"""Provider selection stays explicit and credential-safe."""

import asyncio

import pytest
from pipecat.services.cartesia.tts import CartesiaTTSService

from server.config import Config, ConfigError
from server.services.factory import create_stt, create_tts
from server.services.hosted import CartesiaTTS, DeepgramSTT
from server.services.stt import LocalSTT
from server.services.tts import CorrelatedTTSSpeakFrame, LocalTTS


def test_factory_preserves_local_provider_defaults() -> None:
    config = Config(
        stt_endpoint=("uds", "/tmp/stt.sock"),
        tts_endpoint=("ws", "127.0.0.1:8965"),
    )

    assert isinstance(create_stt(config), LocalSTT)
    assert isinstance(create_tts(config), LocalTTS)


def test_factory_builds_connection_local_hosted_services() -> None:
    config = Config(
        stt_provider="deepgram",
        stt_model="nova-3-general",
        deepgram_api_key="deepgram-secret",
        tts_provider="cartesia",
        tts_model="sonic-3.5",
        cartesia_api_key="cartesia-secret",
        cartesia_voice_id="voice-uuid",
    )

    stt = create_stt(config)
    tts = create_tts(config)

    assert isinstance(stt, DeepgramSTT)
    assert isinstance(stt.for_connection(), DeepgramSTT)
    assert stt._settings.model == "nova-3-general"
    assert isinstance(tts, CartesiaTTS)
    assert isinstance(tts.for_connection(), CartesiaTTS)
    assert tts._settings.model == "sonic-3.5"
    assert tts._settings.voice == "voice-uuid"
    assert "deepgram-secret" not in repr(config)
    assert "cartesia-secret" not in repr(config)


def test_cartesia_uses_the_scheduler_utterance_as_its_pipecat_context(monkeypatch) -> None:
    async def run() -> None:
        observed: list[str] = []

        async def process(service, _frame, _direction) -> None:
            observed.append(service.create_context_id())

        monkeypatch.setattr(CartesiaTTSService, "process_frame", process)
        service = CartesiaTTS(
            api_key="cartesia-test",
            voice_id="voice-test",
            model="sonic-3.5",
        )
        frame = service.correlated_speak_frame(
            "hello",
            correlation_id="utt-1",
            append_to_context=False,
        )

        assert isinstance(frame, CorrelatedTTSSpeakFrame)
        await service.process_frame(frame, None)
        assert observed == ["utt-1"]

    asyncio.run(run())


@pytest.mark.parametrize(
    ("config", "message"),
    (
        (Config(stt_provider="deepgram"), "DEEPGRAM_API_KEY"),
        (Config(tts_provider="cartesia"), "CARTESIA_API_KEY"),
        (
            Config(tts_provider="cartesia", cartesia_api_key="cartesia-secret"),
            "CARTESIA_VOICE_ID",
        ),
    ),
)
def test_hosted_provider_credentials_fail_closed(config: Config, message: str) -> None:
    factory = create_stt if config.stt_provider == "deepgram" else create_tts

    with pytest.raises(ConfigError, match=message):
        factory(config)
