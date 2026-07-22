"""Async and synchronous callback behavior for local speech service seams."""

import asyncio

import pytest
from pipecat.frames.frames import TTSAudioRawFrame, TranscriptionFrame

from server.services.stt import LocalSTT, STTEndpoint
from server.services.tts import LocalTTS, TTSEndpoint


def test_local_stt_awaits_async_callback() -> None:
    async def run() -> None:
        observed: list[str] = []

        async def on_final(text: str) -> str:
            await asyncio.sleep(0)
            observed.append(text)
            return "handled"

        service = LocalSTT(STTEndpoint("uds", "/tmp/stt.sock"), on_final)
        await service.start()
        assert await service.handle_final_transcript("hello") == "handled"
        assert observed == ["hello"]

    asyncio.run(run())


def test_local_stt_sends_raw_pcm_segments() -> None:
    service = LocalSTT(STTEndpoint("uds", "/tmp/stt.sock"))

    assert service.wants_wav_segments() is False


def test_local_tts_initializes_runtime_settings() -> None:
    service = LocalTTS(TTSEndpoint("tcp", "127.0.0.1:9001"), voice_id="azelma")

    assert service._settings.model is None
    assert service._settings.voice == "azelma"
    assert service._settings.language is None


def test_local_tts_awaits_async_callback() -> None:
    async def run() -> None:
        observed: list[tuple[str, str]] = []

        async def on_event(event: str, utterance_id: str) -> str:
            await asyncio.sleep(0)
            observed.append((event, utterance_id))
            return "handled"

        service = LocalTTS(TTSEndpoint("tcp", "127.0.0.1:9001"), on_event)
        await service.start()
        assert await service.synthesize("hello", "utt-1") == "handled"
        assert observed == [("synthesis_ended", "utt-1")]

    asyncio.run(run())


def test_local_speech_services_preserve_sync_callbacks() -> None:
    async def run() -> None:
        stt = LocalSTT(STTEndpoint("uds", "/tmp/stt.sock"), lambda text: text.upper())
        tts = LocalTTS(TTSEndpoint("tcp", "127.0.0.1:9001"), lambda event, _: event)
        await stt.start()
        await tts.start()
        assert await stt.handle_final_transcript("hello") == "HELLO"
        assert await tts.synthesize("hello", "utt-1") == "synthesis_ended"

    asyncio.run(run())


def test_run_tts_invokes_configured_synthesis_callback() -> None:
    class Client:
        async def append(self, _text: str) -> None:
            pass

        async def commit(self) -> None:
            pass

        async def events(self):
            yield {"type": "audio.done"}

    async def run() -> None:
        events: list[str] = []
        service = LocalTTS(
            TTSEndpoint("tcp", "127.0.0.1:9001"),
            lambda event, _utterance: events.append(event),
            client_factory=lambda _endpoint: Client(),
        )
        await service.start()
        [frame async for frame in service.run_tts("hello", "utt-1")]
        assert events == ["synthesis_started", "synthesis_ended"]

    asyncio.run(run())


def test_local_tts_adopts_server_rate_before_emitting_audio() -> None:
    class Client:
        async def connect(self) -> dict:
            return {"type": "server.hello", "audio": {"rate": 22050}}

        async def append(self, _text: str) -> None:
            pass

        async def commit(self) -> None:
            pass

        async def events(self):
            yield {"type": "response.audio.delta", "audio": "AA=="}
            yield {"type": "response.audio.done"}

    async def run() -> None:
        service = LocalTTS(
            TTSEndpoint("tcp", "127.0.0.1:9001"), client_factory=lambda _endpoint: Client()
        )
        await service.start()
        frames = [frame async for frame in service.run_tts("hello", "utt-1")]

        assert service.sample_rate == 22050
        audio = next(frame for frame in frames if isinstance(frame, TTSAudioRawFrame))
        assert audio.sample_rate == 22050

    asyncio.run(run())


def test_local_tts_preserves_configured_voice_for_connection_client() -> None:
    captured: list[str | None] = []

    class Client:
        async def connect(self) -> dict:
            captured.append(self.voice_id)
            return {"type": "server.hello", "audio": {"rate": 24000}}

        async def append(self, _text: str) -> None:
            pass

        async def commit(self) -> None:
            pass

        async def events(self):
            yield {"type": "response.audio.done"}

        voice_id = "azelma"

    async def run() -> None:
        service = LocalTTS(
            TTSEndpoint("tcp", "127.0.0.1:9001"),
            client_factory=lambda _endpoint: Client(),
            voice_id="azelma",
        )
        await service.start()
        [frame async for frame in service.run_tts("hello", "utt-1")]
        assert service.voice_id == "azelma"

    asyncio.run(run())


def test_local_tts_reports_unknown_delivery_on_provider_error() -> None:
    class Client:
        async def append(self, _text: str) -> None:
            pass

        async def commit(self) -> None:
            pass

        async def events(self):
            yield {"type": "error", "message": "provider unavailable"}

    async def run() -> None:
        events: list[str] = []
        service = LocalTTS(
            TTSEndpoint("tcp", "127.0.0.1:9001"),
            lambda event, _context: events.append(event),
            client_factory=lambda _endpoint: Client(),
        )
        await service.start()
        with pytest.raises(RuntimeError, match="provider unavailable"):
            [frame async for frame in service.run_tts("hello", "utt-1")]

        assert events == ["synthesis_started", "delivery_unknown"]

    asyncio.run(run())


def test_local_stt_dispatches_routing_without_blocking_audio_frame_delivery() -> None:
    class Client:
        async def send_audio(self, _audio: bytes) -> None:
            pass

        async def commit(self) -> None:
            pass

        async def events(self):
            yield {
                "type": "conversation.item.input_audio_transcription.completed",
                "transcript": "hello",
            }

    async def run() -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        async def on_final(_text: str) -> None:
            started.set()
            await release.wait()

        service = LocalSTT(
            STTEndpoint("uds", "/tmp/stt.sock"),
            on_final,
            client_factory=lambda _endpoint: Client(),
        )
        await service.start()
        frames = [frame async for frame in service.run_stt(b"audio")]

        assert isinstance(frames[0], TranscriptionFrame)
        await asyncio.wait_for(started.wait(), timeout=1)
        release.set()
        await asyncio.sleep(0)

    asyncio.run(run())
