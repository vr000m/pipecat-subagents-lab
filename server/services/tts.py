"""Pipecat-native adapter seam for the local websocket TTS service.

The sibling server's reference client is injected because it is not a project
dependency. The adapter refuses to synthesize without that client rather than
emitting a callback-only fake audio lifecycle.
"""

from __future__ import annotations

import inspect
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from typing import Any

from pipecat.frames.frames import (
    Frame,
    TTSAudioRawFrame,
    TTSSpeakFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
)
from pipecat.services.settings import TTSSettings
from pipecat.services.tts_service import TTSService

from .ws_clients import default_tts_client_factory


@dataclass(frozen=True)
class TTSEndpoint:
    transport: str
    address: str


@dataclass
class CorrelatedTTSSpeakFrame(TTSSpeakFrame):
    """A speak request whose Pipecat TTS context is the scheduler utterance."""

    correlation_id: str = ""


class LocalTTS(TTSService):
    def __init__(
        self,
        endpoint: TTSEndpoint,
        on_event: Callable[[str, str], Any] | None = None,
        *,
        client_factory: Callable[[TTSEndpoint], Any] | None = None,
        voice_id: str | None = "azelma",
        sample_rate: int = 24000,
        max_events_per_utterance: int = 4096,
        max_audio_bytes_per_utterance: int = 32 * 1024 * 1024,
    ) -> None:
        super().__init__(
            sample_rate=sample_rate,
            settings=TTSSettings(model=None, voice=voice_id, language=None),
        )
        self.endpoint, self.on_event, self.voice_id = endpoint, on_event, voice_id
        self.max_events_per_utterance = max_events_per_utterance
        self.max_audio_bytes_per_utterance = max_audio_bytes_per_utterance
        self.client_factory = client_factory or (
            lambda next_endpoint: default_tts_client_factory(next_endpoint, voice_id=voice_id)
        )
        self._client: Any = None
        self._context_clients: dict[str, Any] = {}
        self._correlated_context_id: str | None = None
        self.started = False

    def correlated_speak_frame(
        self, text: str, *, correlation_id: str, append_to_context: bool = False
    ) -> CorrelatedTTSSpeakFrame:
        return CorrelatedTTSSpeakFrame(
            text=text,
            append_to_context=append_to_context,
            correlation_id=correlation_id,
        )

    def create_context_id(self) -> str:
        if self._correlated_context_id is not None:
            return self._correlated_context_id
        return super().create_context_id()

    async def process_frame(self, frame: Frame, direction: Any) -> None:
        if not isinstance(frame, CorrelatedTTSSpeakFrame):
            await super().process_frame(frame, direction)
            return
        previous = self._correlated_context_id
        self._correlated_context_id = frame.correlation_id
        try:
            await super().process_frame(frame, direction)
        finally:
            self._correlated_context_id = previous

    async def connect(self) -> int:
        """Connect once and adopt the rate advertised by the local server."""
        if self._client is not None:
            return self.sample_rate
        if self.client_factory is None:
            raise RuntimeError("local TTS wire client is unavailable")
        self._client = self.client_factory(self.endpoint)
        client = self._client
        try:
            connect = getattr(client, "connect", None)
            hello = connect() if connect is not None else None
            if inspect.isawaitable(hello):
                hello = await hello
        except BaseException:
            await self._abort_client(client)
            raise
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
        await self._abort_client()
        await super().cleanup()

    def for_connection(self) -> LocalTTS:
        """Create a client-bound adapter for one promoted browser epoch."""
        return type(self)(
            self.endpoint,
            self.on_event,
            client_factory=self.client_factory,
            voice_id=self.voice_id,
            sample_rate=self.sample_rate,
            max_events_per_utterance=self.max_events_per_utterance,
            max_audio_bytes_per_utterance=self.max_audio_bytes_per_utterance,
        )

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
        client = self._client
        self._context_clients[context_id] = client
        # Keep a connection-bound callback stable for this synthesis. A reconnect
        # may install a callback for the replacement scheduler while this
        # generator is still draining.
        on_event = self.on_event
        completed = False
        try:
            await client.append(text)
            await client.commit()
            if on_event is not None:
                result = on_event("synthesis_started", context_id)
                if inspect.isawaitable(result):
                    await result
            yield TTSStartedFrame(context_id=context_id)
            audio_bytes = 0
            event_count = 0
            async for event in client.events():
                event_count += 1
                if event_count > self.max_events_per_utterance:
                    raise RuntimeError("local TTS event limit exceeded")
                kind = event.get("type", "")
                if kind.endswith("audio.delta"):
                    import base64

                    try:
                        audio = base64.b64decode(event["audio"], validate=True)
                    except (KeyError, TypeError, ValueError) as exc:
                        raise RuntimeError("local TTS returned invalid audio") from exc
                    audio_bytes += len(audio)
                    if audio_bytes > self.max_audio_bytes_per_utterance:
                        raise RuntimeError("local TTS audio limit exceeded")
                    yield TTSAudioRawFrame(
                        audio=audio,
                        sample_rate=self.sample_rate,
                        num_channels=1,
                        context_id=context_id,
                    )
                elif kind.endswith(("audio.done", "cancelled")):
                    completed = True
                    if on_event is not None:
                        result = on_event("synthesis_ended", context_id)
                        if inspect.isawaitable(result):
                            await result
                    yield TTSStoppedFrame(context_id=context_id)
                    return
                elif kind in {"error", "response.failed"}:
                    message = str(event.get("message") or event.get("error") or "provider error")
                    raise RuntimeError(f"local TTS error: {message[:256]}")
            raise RuntimeError("local TTS stream ended before audio completion")
        finally:
            if self._context_clients.get(context_id) is client:
                self._context_clients.pop(context_id, None)
            if not completed:
                await self._abort_client(client)
                await self._notify_delivery_unknown(on_event, context_id)

    async def on_audio_context_interrupted(self, context_id: str) -> None:
        """Fence an interrupted provider stream before Pipecat starts its successor."""
        client = self._context_clients.pop(context_id, None)
        if client is not None:
            await self._abort_client(client)

    async def _abort_client(self, client: Any | None = None) -> None:
        client = self._client if client is None else client
        if client is None:
            return
        if self._client is client:
            self._client = None
        for context_id, context_client in tuple(self._context_clients.items()):
            if context_client is client:
                self._context_clients.pop(context_id, None)
        close = getattr(client, "close", None)
        if close is not None:
            result = close()
            if inspect.isawaitable(result):
                await result

    @staticmethod
    async def _notify_delivery_unknown(
        on_event: Callable[[str, str], Any] | None,
        context_id: str,
    ) -> None:
        if on_event is None:
            return
        result = on_event("delivery_unknown", context_id)
        if inspect.isawaitable(result):
            await result
