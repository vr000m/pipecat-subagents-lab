#!/usr/bin/env python3
"""Smoke local speech or compare providers with identical text and audio.

The local-only mode is an opt-in service smoke requiring no hosted credentials.
The comparison mode is a paid benchmark. Both print aggregate timing only and
never print provider payloads or secrets.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import re
import statistics
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from loguru import logger
from websockets.asyncio.client import connect

from server.config import ConfigError, load_config
from server.services.stt import LocalSTT, STTEndpoint
from server.services.tts import LocalTTS, TTSEndpoint


@dataclass(frozen=True)
class TTSMeasurement:
    first_audio_ms: float
    complete_ms: float
    audio: bytes
    sample_rate: int


@dataclass(frozen=True)
class STTMeasurement:
    final_ms: float
    transcript: str


def _percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * fraction)))
    return ordered[index]


def _summary(values: list[float]) -> str:
    return (
        f"median={statistics.median(values):.1f}ms "
        f"p95={_percentile(values, 0.95):.1f}ms "
        f"min={min(values):.1f}ms max={max(values):.1f}ms"
    )


async def _local_tts(service: LocalTTS, text: str) -> TTSMeasurement:
    from pipecat.frames.frames import TTSAudioRawFrame

    started = time.perf_counter()
    first_audio: float | None = None
    audio = bytearray()
    async for frame in service.run_tts(text, f"bench-{time.monotonic_ns()}"):
        if isinstance(frame, TTSAudioRawFrame):
            if first_audio is None:
                first_audio = time.perf_counter()
            audio.extend(frame.audio)
    completed = time.perf_counter()
    if first_audio is None or not audio:
        raise RuntimeError("local TTS returned no audio")
    return TTSMeasurement(
        first_audio_ms=(first_audio - started) * 1000,
        complete_ms=(completed - started) * 1000,
        audio=bytes(audio),
        sample_rate=service.sample_rate,
    )


class CartesiaBenchmarkClient:
    def __init__(self, *, api_key: str, voice_id: str, model: str) -> None:
        self.api_key = api_key
        self.voice_id = voice_id
        self.model = model
        self.ws: Any = None

    async def connect(self) -> float:
        started = time.perf_counter()
        query = urlencode({"api_key": self.api_key, "cartesia_version": "2026-03-01"})
        self.ws = await connect(f"wss://api.cartesia.ai/tts/websocket?{query}")
        return (time.perf_counter() - started) * 1000

    async def synthesize(self, text: str) -> TTSMeasurement:
        context_id = f"bench-{time.monotonic_ns()}"
        message = {
            "transcript": text,
            "continue": False,
            "context_id": context_id,
            "model_id": self.model,
            "voice": {"mode": "id", "id": self.voice_id},
            "output_format": {
                "container": "raw",
                "encoding": "pcm_s16le",
                "sample_rate": 24000,
            },
            "add_timestamps": False,
            "max_buffer_delay_ms": 0,
            "language": "en",
        }
        started = time.perf_counter()
        await self.ws.send(json.dumps(message))
        first_audio: float | None = None
        audio = bytearray()
        async for raw in self.ws:
            event = json.loads(raw)
            if event.get("context_id") != context_id:
                continue
            if event.get("type") == "chunk":
                if first_audio is None:
                    first_audio = time.perf_counter()
                audio.extend(base64.b64decode(event["data"]))
            elif event.get("type") == "done":
                completed = time.perf_counter()
                if first_audio is None or not audio:
                    raise RuntimeError("Cartesia returned no audio")
                return TTSMeasurement(
                    first_audio_ms=(first_audio - started) * 1000,
                    complete_ms=(completed - started) * 1000,
                    audio=bytes(audio),
                    sample_rate=24000,
                )
            elif event.get("type") == "error":
                raise RuntimeError("Cartesia synthesis failed")
        raise RuntimeError("Cartesia stream ended before completion")

    async def close(self) -> None:
        if self.ws is not None:
            await self.ws.close()


async def _local_stt(service: LocalSTT, audio: bytes) -> STTMeasurement:
    from pipecat.frames.frames import TranscriptionFrame

    started = time.perf_counter()
    transcript = ""
    async for frame in service.run_stt(audio):
        if isinstance(frame, TranscriptionFrame):
            transcript = frame.text
    if not transcript:
        raise RuntimeError("local STT returned no final transcript")
    return STTMeasurement((time.perf_counter() - started) * 1000, transcript)


async def _deepgram_stt(
    *, api_key: str, model: str, language: str, audio: bytes
) -> tuple[float, STTMeasurement]:
    query = urlencode(
        {
            "encoding": "linear16",
            "sample_rate": 16000,
            "channels": 1,
            "model": model,
            "language": language,
            "smart_format": "true",
            "interim_results": "true",
        }
    )
    connect_started = time.perf_counter()
    async with connect(
        f"wss://api.deepgram.com/v1/listen?{query}",
        additional_headers={"Authorization": f"Token {api_key}"},
    ) as ws:
        connect_ms = (time.perf_counter() - connect_started) * 1000
        # Match the browser pipeline's 20 ms PCM cadence. Deepgram's live
        # endpoint does not treat one large prerecorded frame as live audio.
        for offset in range(0, len(audio), 640):
            await ws.send(audio[offset : offset + 640])
            await asyncio.sleep(0.02)
        started = time.perf_counter()
        await ws.send(json.dumps({"type": "Finalize"}))
        async for raw in ws:
            event = json.loads(raw)
            if event.get("type") == "Error":
                raise RuntimeError("Deepgram transcription failed")
            if event.get("type") != "Results" or not event.get("is_final"):
                continue
            alternatives = event.get("channel", {}).get("alternatives", [])
            transcript = alternatives[0].get("transcript", "") if alternatives else ""
            if transcript:
                return connect_ms, STTMeasurement(
                    (time.perf_counter() - started) * 1000, transcript
                )
    raise RuntimeError("Deepgram stream ended before a final transcript")


def _resample_to_16khz(audio: bytes, source_rate: int) -> bytes:
    if source_rate == 16000:
        return audio
    import audioop

    converted, _ = audioop.ratecv(audio, 2, 1, source_rate, 16000, None)
    return converted


def _normalized_transcript(value: str) -> str:
    return re.sub(r"[^\w]+", " ", value.casefold()).strip()


async def _measure(
    runs: int,
    operation: Callable[[], Awaitable[Any]],
) -> list[Any]:
    return [await operation() for _ in range(runs)]


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--text", default="What is the capital of India?")
    parser.add_argument("--config", type=Path, default=Path("config.toml"))
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="smoke the configured local TTS -> PCM -> local STT path without hosted services",
    )
    args = parser.parse_args()
    minimum_runs = 1 if args.local_only else 2
    if args.runs < minimum_runs:
        parser.error(f"--runs must be at least {minimum_runs}")

    config = load_config(config_file=args.config)
    if config.stt_endpoint is None or config.tts_endpoint is None:
        raise ConfigError("local STT and TTS endpoints are required for comparison")
    if not args.local_only and not config.deepgram_api_key:
        raise ConfigError("DEEPGRAM_API_KEY is required for the hosted benchmark")
    if not args.local_only and (not config.cartesia_api_key or not config.cartesia_voice_id):
        raise ConfigError(
            "CARTESIA_API_KEY and CARTESIA_VOICE_ID are required for the hosted benchmark"
        )

    local_tts = LocalTTS(TTSEndpoint(*config.tts_endpoint), voice_id=config.tts_voice_id)
    local_stt = LocalSTT(STTEndpoint(*config.stt_endpoint), language=config.stt_language)
    cartesia = (
        None
        if args.local_only
        else CartesiaBenchmarkClient(
            api_key=config.cartesia_api_key,
            voice_id=config.cartesia_voice_id,
            model=config.tts_model,
        )
    )
    await local_tts.start()
    await local_stt.start()
    try:
        local_connect_started = time.perf_counter()
        await local_tts.connect()
        local_tts_connect_ms = (time.perf_counter() - local_connect_started) * 1000
        cartesia_connect_ms = await cartesia.connect() if cartesia is not None else None

        local_tts_runs = await _measure(args.runs, lambda: _local_tts(local_tts, args.text))
        cartesia_runs = (
            await _measure(args.runs, lambda: cartesia.synthesize(args.text))
            if cartesia is not None
            else []
        )

        fixture = _resample_to_16khz(local_tts_runs[-1].audio, local_tts_runs[-1].sample_rate)
        local_stt_runs = await _measure(args.runs, lambda: _local_stt(local_stt, fixture))
        deepgram_runs: list[STTMeasurement] = []
        deepgram_connect: list[float] = []
        if not args.local_only:
            for _ in range(args.runs):
                connect_ms, measurement = await _deepgram_stt(
                    api_key=config.deepgram_api_key,
                    model=config.stt_model,
                    language=config.stt_language,
                    audio=fixture,
                )
                deepgram_connect.append(connect_ms)
                deepgram_runs.append(measurement)

        print(f"runs={args.runs} fixture_pcm_ms={len(fixture) / 32:.1f}")
        print("tts.local.first_audio " + _summary([item.first_audio_ms for item in local_tts_runs]))
        print("tts.local.complete " + _summary([item.complete_ms for item in local_tts_runs]))
        print("tts.local.connect " + _summary([local_tts_connect_ms]))
        print("stt.local.final " + _summary([item.final_ms for item in local_stt_runs]))
        local_match = all(
            _normalized_transcript(item.transcript) == _normalized_transcript(args.text)
            for item in local_stt_runs
        )
        if args.local_only:
            print(f"transcript_match local={local_match}")
            if not local_match:
                raise RuntimeError("local speech smoke transcript did not match the fixture text")
        else:
            print(
                "tts.cartesia.first_audio "
                + _summary([item.first_audio_ms for item in cartesia_runs])
            )
            print("tts.cartesia.complete " + _summary([item.complete_ms for item in cartesia_runs]))
            print("tts.cartesia.connect " + _summary([cartesia_connect_ms]))
            print("stt.deepgram.final " + _summary([item.final_ms for item in deepgram_runs]))
            print("stt.deepgram.connect " + _summary(deepgram_connect))
            deepgram_match = all(
                _normalized_transcript(item.transcript) == _normalized_transcript(args.text)
                for item in deepgram_runs
            )
            print(f"transcript_match local={local_match} deepgram={deepgram_match}")
    finally:
        if cartesia is not None:
            await cartesia.close()
        await local_stt.cleanup()
        await local_tts.cleanup()


if __name__ == "__main__":
    logger.remove()
    asyncio.run(main())
