"""Pipecat-native adapter seam for the local websocket STT service.

The sibling ``pipecat-local-stt-server`` reference adapter is not a dependency
of this project, so the wire client is injected. Without one, ``run_stt`` fails
closed instead of claiming that toggling a flag produced live audio.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
import inspect
from dataclasses import dataclass
from typing import Any, Callable

from pipecat.frames.frames import ErrorFrame, Frame, TranscriptionFrame
from pipecat.services.stt_service import SegmentedSTTService

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
        sample_rate: int = 16000,
    ) -> None:
        super().__init__(sample_rate=sample_rate)
        self.endpoint, self.on_final = endpoint, on_final
        self.client_factory = client_factory or default_stt_client_factory
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
        close = getattr(self._client, "close", None)
        if close is not None:
            result = close()
            if inspect.isawaitable(result):
                await result
        self._client = None
        await super().cleanup()

    def for_connection(self) -> LocalSTT:
        """Create a client-bound adapter for one promoted browser epoch."""
        return type(self)(
            self.endpoint,
            client_factory=self.client_factory,
            sample_rate=self.sample_rate,
        )

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
        if self._client is None:
            self._client = self.client_factory(self.endpoint)
            connect = getattr(self._client, "connect", None)
            if connect is not None:
                result = connect()
                if inspect.isawaitable(result):
                    await result
        await self._client.send_audio(audio)
        await self._client.commit()
        async for event in self._client.events():
            if event.get("type", "").endswith("transcription.completed"):
                text = event.get("transcript") or ""
                if text:
                    yield TranscriptionFrame(text, "", "")
                    if on_final is not None:
                        asyncio.create_task(self.handle_final_transcript(text, on_final))
                return
            if event.get("type") == "error":
                yield ErrorFrame(f"local STT error: {event.get('message', event)}")
                return
