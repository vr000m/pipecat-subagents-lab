"""Connection-local Pipecat adapters for hosted speech providers."""

from __future__ import annotations

from typing import Any

from pipecat.frames.frames import Frame
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.deepgram.stt import DeepgramSTTService

from .tts import CorrelatedTTSSpeakFrame


class DeepgramSTT(DeepgramSTTService):
    """Deepgram service that can be cloned for each browser connection."""

    def __init__(self, *, api_key: str, model: str, language: str) -> None:
        self._connection_config = {
            "api_key": api_key,
            "model": model,
            "language": language,
        }
        super().__init__(
            api_key=api_key,
            settings=DeepgramSTTService.Settings(model=model, language=language),
        )

    def for_connection(self) -> DeepgramSTT:
        return type(self)(**self._connection_config)


class CartesiaTTS(CartesiaTTSService):
    """Cartesia service that can be cloned for each browser connection."""

    def __init__(
        self,
        *,
        api_key: str,
        voice_id: str,
        model: str,
        sample_rate: int = 24000,
    ) -> None:
        self._connection_config: dict[str, Any] = {
            "api_key": api_key,
            "voice_id": voice_id,
            "model": model,
            "sample_rate": sample_rate,
        }
        self._correlated_context_id: str | None = None
        super().__init__(
            api_key=api_key,
            sample_rate=sample_rate,
            settings=CartesiaTTSService.Settings(model=model, voice=voice_id),
        )

    def for_connection(self) -> CartesiaTTS:
        return type(self)(**self._connection_config)

    @staticmethod
    def correlated_speak_frame(
        text: str, *, correlation_id: str, append_to_context: bool = False
    ) -> CorrelatedTTSSpeakFrame:
        return CorrelatedTTSSpeakFrame(
            text=text,
            correlation_id=correlation_id,
            append_to_context=append_to_context,
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
