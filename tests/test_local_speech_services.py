"""Async and synchronous callback behavior for local speech service seams."""

import asyncio

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
