"""Connection-local Pipecat adapters for hosted speech providers."""

from __future__ import annotations

from typing import Any

from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.deepgram.stt import DeepgramSTTService


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
        super().__init__(
            api_key=api_key,
            sample_rate=sample_rate,
            settings=CartesiaTTSService.Settings(model=model, voice=voice_id),
        )

    def for_connection(self) -> CartesiaTTS:
        return type(self)(**self._connection_config)
