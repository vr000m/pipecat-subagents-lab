"""Async and synchronous callback behavior for local speech service seams."""

import asyncio

import pytest
from pipecat.frames.frames import ErrorFrame, TranscriptionFrame, TTSAudioRawFrame

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


def test_run_tts_stamps_context_id_on_audio_frames_matching_started_and_stopped() -> None:
    """TTSAudioRawFrame must carry context_id like its sibling
    TTSStartedFrame/TTSStoppedFrame calls in the same generator, so
    TransportSpeechLifecycleProcessor's `frame.context_id` guard (in
    server/speech_lifecycle.py) actually fires for local-TTS audio chunks --
    otherwise on_tts_audio() is never called (audio_submitted stays False)
    and drop_stale_frame() is never checked for stale/tombstoned local-TTS
    audio, letting it reach transport output after interruption."""

    class Client:
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
        frames = [frame async for frame in service.run_tts("hello", "ctx-1")]

        audio = next(frame for frame in frames if isinstance(frame, TTSAudioRawFrame))
        assert audio.context_id == "ctx-1"

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
        closed = False

        async def append(self, _text: str) -> None:
            pass

        async def commit(self) -> None:
            pass

        async def events(self):
            yield {"type": "error", "message": "provider unavailable"}

        async def close(self) -> None:
            self.closed = True

    async def run() -> None:
        events: list[str] = []
        client = Client()
        service = LocalTTS(
            TTSEndpoint("tcp", "127.0.0.1:9001"),
            lambda event, _context: events.append(event),
            client_factory=lambda _endpoint: client,
        )
        await service.start()
        with pytest.raises(RuntimeError, match="provider unavailable"):
            [frame async for frame in service.run_tts("hello", "utt-1")]

        assert client.closed is True
        assert service._client is None
        assert events == ["synthesis_started", "delivery_unknown"]

    asyncio.run(run())


def test_local_tts_bounds_decoded_audio_per_utterance() -> None:
    class Client:
        closed = False

        async def append(self, _text: str) -> None:
            pass

        async def commit(self) -> None:
            pass

        async def events(self):
            yield {"type": "response.audio.delta", "audio": "AAA="}

        async def close(self) -> None:
            self.closed = True

    async def run() -> None:
        events: list[str] = []
        client = Client()
        service = LocalTTS(
            TTSEndpoint("tcp", "127.0.0.1:9001"),
            lambda event, _context: events.append(event),
            client_factory=lambda _endpoint: client,
            max_audio_bytes_per_utterance=1,
        )
        await service.start()

        with pytest.raises(RuntimeError, match="audio limit"):
            [frame async for frame in service.run_tts("hello", "utt-1")]

        assert client.closed is True
        assert events == ["synthesis_started", "delivery_unknown"]

    asyncio.run(run())


def test_local_tts_closes_premature_stream_and_reconnects_successor() -> None:
    class Client:
        def __init__(self, events: list[dict]) -> None:
            self._events = events
            self.closed = False

        async def append(self, _text: str) -> None:
            pass

        async def commit(self) -> None:
            pass

        async def events(self):
            for event in self._events:
                yield event

        async def close(self) -> None:
            self.closed = True

    async def run() -> None:
        events: list[tuple[str, str]] = []
        clients = [
            Client([{"type": "response.audio.delta", "audio": "AA=="}]),
            Client([{"type": "response.audio.done"}]),
        ]
        service = LocalTTS(
            TTSEndpoint("tcp", "127.0.0.1:9001"),
            lambda event, context: events.append((event, context)),
            client_factory=lambda _endpoint: clients.pop(0),
        )
        await service.start()

        with pytest.raises(RuntimeError, match="ended before audio completion"):
            [frame async for frame in service.run_tts("first", "utt-1")]

        first = service._client
        assert first is None
        frames = [frame async for frame in service.run_tts("second", "utt-2")]

        assert frames[-1].context_id == "utt-2"
        assert events == [
            ("synthesis_started", "utt-1"),
            ("delivery_unknown", "utt-1"),
            ("synthesis_started", "utt-2"),
            ("synthesis_ended", "utt-2"),
        ]

    asyncio.run(run())


def test_local_tts_generator_interruption_fences_stale_stream_from_successor() -> None:
    class Client:
        def __init__(self, completion: asyncio.Event | None = None) -> None:
            self.completion = completion
            self.closed = False

        async def append(self, _text: str) -> None:
            pass

        async def commit(self) -> None:
            pass

        async def events(self):
            if self.completion is not None:
                await self.completion.wait()
            yield {"type": "response.audio.done"}

        async def close(self) -> None:
            self.closed = True

    async def run() -> None:
        events: list[tuple[str, str]] = []
        stale_completion = asyncio.Event()
        first = Client(stale_completion)
        second = Client()
        clients = [first, second]
        service = LocalTTS(
            TTSEndpoint("tcp", "127.0.0.1:9001"),
            lambda event, context: events.append((event, context)),
            client_factory=lambda _endpoint: clients.pop(0),
        )
        await service.start()

        stream = service.run_tts("first", "utt-1")
        started = await anext(stream)
        assert started.context_id == "utt-1"
        await stream.aclose()

        assert first.closed is True
        assert service._client is None

        replacement = [frame async for frame in service.run_tts("second", "utt-2")]
        await service.on_audio_context_interrupted("utt-1")
        stale_completion.set()
        await asyncio.sleep(0)

        assert second.closed is False
        assert replacement[-1].context_id == "utt-2"
        assert events == [
            ("synthesis_started", "utt-1"),
            ("delivery_unknown", "utt-1"),
            ("synthesis_started", "utt-2"),
            ("synthesis_ended", "utt-2"),
        ]

    asyncio.run(run())


def test_local_tts_audio_context_interruption_aborts_provider_client() -> None:
    class Client:
        closed = False

        async def close(self) -> None:
            self.closed = True

    async def run() -> None:
        client = Client()
        service = LocalTTS(
            TTSEndpoint("tcp", "127.0.0.1:9001"),
            client_factory=lambda _endpoint: client,
        )
        service._client = client
        service._context_clients["utt-1"] = client

        await service.on_audio_context_interrupted("utt-1")

        assert client.closed is True
        assert service._client is None

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


def test_local_tts_connect_failure_clears_client_and_reconnects() -> None:
    class Client:
        def __init__(self, *, fails: bool) -> None:
            self.fails = fails
            self.closed = False

        async def connect(self) -> dict:
            if self.fails:
                raise RuntimeError("handshake failed")
            return {"type": "server.hello", "audio": {"rate": 24000}}

        async def append(self, _text: str) -> None:
            pass

        async def commit(self) -> None:
            pass

        async def events(self):
            yield {"type": "response.audio.done"}

        async def close(self) -> None:
            self.closed = True

    async def run() -> None:
        first = Client(fails=True)
        second = Client(fails=False)
        clients = [first, second]
        service = LocalTTS(
            TTSEndpoint("tcp", "127.0.0.1:9001"),
            client_factory=lambda _endpoint: clients.pop(0),
        )
        await service.start()

        with pytest.raises(RuntimeError, match="handshake failed"):
            [frame async for frame in service.run_tts("first", "utt-1")]

        assert first.closed is True
        assert service._client is None

        frames = [frame async for frame in service.run_tts("second", "utt-2")]

        assert frames[-1].context_id == "utt-2"
        assert service._client is second

    asyncio.run(run())


@pytest.mark.parametrize("failure_stage", ["connect", "send", "commit", "events"])
def test_local_stt_failure_closes_client_and_reconnects_next_segment(
    failure_stage: str,
) -> None:
    class Client:
        def __init__(self, *, failure_stage: str | None) -> None:
            self.failure_stage = failure_stage
            self.closed = False
            self.connected = False

        async def connect(self) -> None:
            self.connected = True
            if self.failure_stage == "connect":
                raise RuntimeError("connect failed")

        async def send_audio(self, _audio: bytes) -> None:
            if self.failure_stage == "send":
                raise RuntimeError("send failed")

        async def commit(self) -> None:
            if self.failure_stage == "commit":
                raise RuntimeError("commit failed")

        async def events(self):
            if self.failure_stage == "events":
                raise RuntimeError("events failed")
            yield {
                "type": "conversation.item.input_audio_transcription.completed",
                "transcript": "recovered",
            }

        async def close(self) -> None:
            self.closed = True

    async def run() -> None:
        first = Client(failure_stage=failure_stage)
        second = Client(failure_stage=None)
        clients = [first, second]
        service = LocalSTT(
            STTEndpoint("uds", "/tmp/stt.sock"),
            client_factory=lambda _endpoint: clients.pop(0),
        )
        await service.start()

        with pytest.raises(RuntimeError, match=f"{failure_stage} failed"):
            [frame async for frame in service.run_stt(b"first")]

        assert first.closed is True
        assert service._client is None

        frames = [frame async for frame in service.run_stt(b"second")]

        assert second.connected is True
        assert isinstance(frames[0], TranscriptionFrame)
        assert frames[0].text == "recovered"

    asyncio.run(run())


def test_local_stt_terminal_error_closes_client_before_returning_error_frame() -> None:
    class Client:
        closed = False

        async def send_audio(self, _audio: bytes) -> None:
            pass

        async def commit(self) -> None:
            pass

        async def events(self):
            yield {"type": "error", "message": "decoder failed"}

        async def close(self) -> None:
            self.closed = True

    async def run() -> None:
        client = Client()
        service = LocalSTT(
            STTEndpoint("uds", "/tmp/stt.sock"),
            client_factory=lambda _endpoint: client,
        )
        await service.start()

        frames = [frame async for frame in service.run_stt(b"audio")]

        assert isinstance(frames[0], ErrorFrame)
        assert client.closed is True
        assert service._client is None

    asyncio.run(run())
