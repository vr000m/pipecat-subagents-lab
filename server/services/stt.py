"""Pipecat-native adapter seam for the local websocket STT service.

The sibling ``pipecat-local-stt-server`` reference adapter is not a dependency
of this project, so the wire client is injected. Without one, ``run_stt`` fails
closed instead of claiming that toggling a flag produced live audio.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from typing import Any

from loguru import logger
from pipecat.frames.frames import ErrorFrame, Frame, TranscriptionFrame
from pipecat.services.stt_service import SegmentedSTTService, STTSettings

from .ws_clients import default_stt_client_factory


@dataclass(frozen=True)
class STTEndpoint:
    transport: str
    address: str


class LocalSTT(SegmentedSTTService):
    """SegmentedSTTService backed by an injected local-service client factory."""

    def __init__(
        self,
        endpoint: STTEndpoint,
        on_final: Callable[[str], Any] | None = None,
        *,
        client_factory: Callable[[STTEndpoint], Any] | None = None,
        language: str | None = "en",
        sample_rate: int = 16000,
    ) -> None:
        super().__init__(
            sample_rate=sample_rate,
            ttfs_p99_latency=1.0,
            settings=STTSettings(model=None, language=language),
        )
        self.endpoint, self.on_final, self.language = endpoint, on_final, language
        self.client_factory = client_factory or (
            lambda next_endpoint: default_stt_client_factory(next_endpoint, language=language)
        )
        self._client: Any = None
        self.started = False

    async def start(self, frame: Any = None) -> None:
        if frame is not None:
            await super().start(frame)
        self.started = True

    async def stop(self, frame: Any = None) -> None:
        if frame is not None:
            await super().stop(frame)
        self.started = False

    async def cleanup(self) -> None:
        await self._abort_client()
        await super().cleanup()

    def for_connection(self) -> LocalSTT:
        """Create a client-bound adapter for one promoted browser epoch."""
        return type(self)(
            self.endpoint,
            self.on_final,
            client_factory=self.client_factory,
            language=self.language,
            sample_rate=self.sample_rate,
        )

    def wants_wav_segments(self) -> bool:
        """Send the local server its required raw PCM16 wire format."""
        return False

    async def handle_final_transcript(
        self, text: str, callback: Callable[[str], Any] | None = None
    ) -> Any:
        if not self.started:
            raise RuntimeError("STT service is not started")
        handler = callback if callback is not None else self.on_final
        if handler is not None:
            result = handler(text)
            if inspect.isawaitable(result):
                return await result
            return result
        return text

    async def run_stt(self, audio: bytes) -> AsyncGenerator[Frame | None, None]:
        if not self.started:
            raise RuntimeError("STT service is not started")
        if self.client_factory is None:
            raise RuntimeError(
                "local STT wire client is unavailable; inject the verified "
                "pipecat-local-stt-server client factory before running audio"
            )
        on_final = self.on_final
        client = self._client
        try:
            if client is None:
                client = self.client_factory(self.endpoint)
                self._client = client
                connect = getattr(client, "connect", None)
                if connect is not None:
                    result = connect()
                    if inspect.isawaitable(result):
                        await result
            await client.send_audio(audio)
            await client.commit()
            logger.debug(f"{self}: committed {len(audio)} PCM bytes to local STT")
            async for event in client.events():
                if event.get("type", "").endswith("transcription.completed"):
                    text = event.get("transcript") or ""
                    logger.debug(f"{self}: local STT transcription completed")
                    if text:
                        yield TranscriptionFrame(text, "", "")
                        if on_final is not None:
                            asyncio.create_task(self.handle_final_transcript(text, on_final))
                    return
                if event.get("type") == "error":
                    await self._abort_client(client)
                    yield ErrorFrame(f"local STT error: {event.get('message', event)}")
                    return
            await self._abort_client(client)
            yield ErrorFrame("local STT stream ended before transcription completion")
        except BaseException:
            await self._abort_client(client)
            raise

    async def _abort_client(self, client: Any | None = None) -> None:
        client = self._client if client is None else client
        if client is None:
            return
        if self._client is client:
            self._client = None
        close = getattr(client, "close", None)
        if close is not None:
            result = close()
            if inspect.isawaitable(result):
                await result
