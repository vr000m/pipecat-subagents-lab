"""Pipecat-native adapter seam for the local websocket TTS service.

The sibling server's reference client is injected because it is not a project
dependency. The adapter refuses to synthesize without that client rather than
emitting a callback-only fake audio lifecycle.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
import inspect
from dataclasses import dataclass
from typing import Any, Callable

from pipecat.frames.frames import Frame, TTSAudioRawFrame, TTSStartedFrame, TTSStoppedFrame
from pipecat.services.tts_service import TTSService

from .ws_clients import default_tts_client_factory


@dataclass(frozen=True)
class TTSEndpoint:
    transport: str
    address: str


class LocalTTS(TTSService):
    def __init__(
        self,
        endpoint: TTSEndpoint,
        on_event: Callable[[str, str], Any] | None = None,
        *,
        client_factory: Callable[[TTSEndpoint], Any] | None = None,
        sample_rate: int = 24000,
    ) -> None:
        super().__init__(sample_rate=sample_rate)
        self.endpoint, self.on_event = endpoint, on_event
        self.client_factory = client_factory or default_tts_client_factory
        self._client: Any = None
        self.started = False

    async def connect(self) -> int:
        """Connect once and adopt the rate advertised by the local server."""
        if self._client is not None:
            return self.sample_rate
        if self.client_factory is None:
            raise RuntimeError("local TTS wire client is unavailable")
        self._client = self.client_factory(self.endpoint)
        connect = getattr(self._client, "connect", None)
        hello = connect() if connect is not None else None
        if inspect.isawaitable(hello):
            hello = await hello
        rate = (hello or {}).get("audio", {}).get("rate") if isinstance(hello, dict) else None
        if isinstance(rate, int) and rate > 0:
            self._sample_rate = rate
        return self.sample_rate

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

    async def synthesize(self, text: str, utterance_id: str) -> Any:
        if not self.started:
            raise RuntimeError("TTS service is not started")
        if self.on_event is not None:
            result = self.on_event("synthesis_ended", utterance_id)
            if inspect.isawaitable(result):
                return await result
            return result
        return text

    async def run_tts(self, text: str, context_id: str) -> AsyncGenerator[Frame | None, None]:
        if not self.started:
            raise RuntimeError("TTS service is not started")
        if self.client_factory is None:
            raise RuntimeError(
                "local TTS wire client is unavailable; inject the verified "
                "pipecat-local-tts-server client factory before running audio"
            )
        await self.connect()
        # Keep a connection-bound callback stable for this synthesis. A reconnect
        # may install a callback for the replacement scheduler while this
        # generator is still draining.
        on_event = self.on_event
        await self._client.append(text)
        await self._client.commit()
        if on_event is not None:
            result = on_event("synthesis_started", context_id)
            if inspect.isawaitable(result):
                await result
        yield TTSStartedFrame(context_id=context_id)
        async for event in self._client.events():
            kind = event.get("type", "")
            if kind.endswith("audio.delta"):
                import base64

                yield TTSAudioRawFrame(
                    audio=base64.b64decode(event["audio"]),
                    sample_rate=self.sample_rate,
                    num_channels=1,
                )
            elif kind.endswith("audio.done") or kind.endswith("cancelled"):
                if on_event is not None:
                    result = on_event("synthesis_ended", context_id)
                    if inspect.isawaitable(result):
                        await result
                yield TTSStoppedFrame(context_id=context_id)
                return
            elif kind in {"error", "response.failed"}:
                message = str(event.get("message") or event.get("error") or "provider error")
                if on_event is not None:
                    result = on_event("delivery_unknown", context_id)
                    if inspect.isawaitable(result):
                        await result
                raise RuntimeError(f"local TTS error: {message[:256]}")
