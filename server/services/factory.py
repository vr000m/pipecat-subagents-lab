"""Validated speech-provider construction."""

from __future__ import annotations

from typing import Any

from ..config import Config, ConfigError
from .hosted import CartesiaTTS, DeepgramSTT
from .stt import LocalSTT, STTEndpoint
from .tts import LocalTTS, TTSEndpoint


def create_stt(config: Config) -> Any | None:
    if config.stt_provider == "deepgram":
        if not config.deepgram_api_key:
            raise ConfigError("DEEPGRAM_API_KEY is required when stt.provider is deepgram")
        return DeepgramSTT(
            api_key=config.deepgram_api_key,
            model=config.stt_model,
            language=config.stt_language,
        )
    if config.stt_endpoint is None:
        return None
    return LocalSTT(STTEndpoint(*config.stt_endpoint), language=config.stt_language)


def create_tts(config: Config) -> Any | None:
    if config.tts_provider == "cartesia":
        if not config.cartesia_api_key:
            raise ConfigError("CARTESIA_API_KEY is required when tts.provider is cartesia")
        if not config.cartesia_voice_id:
            raise ConfigError("CARTESIA_VOICE_ID is required when tts.provider is cartesia")
        return CartesiaTTS(
            api_key=config.cartesia_api_key,
            voice_id=config.cartesia_voice_id,
            model=config.tts_model,
        )
    if config.tts_endpoint is None:
        return None
    return LocalTTS(TTSEndpoint(*config.tts_endpoint), voice_id=config.tts_voice_id)
