"""Wire-level settings tests for the local STT/TTS websocket clients."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from server.services import ws_clients


class FakeWebSocket:
    def __init__(self, *events: dict) -> None:
        self.events = [json.dumps(event) for event in events]
        self.sent: list[dict] = []
        self.closed = False

    async def recv(self) -> str:
        return self.events.pop(0)

    async def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw))

    async def close(self) -> None:
        self.closed = True

    def __aiter__(self):
        async def events():
            while self.events:
                yield self.events.pop(0)

        return events()


def test_stt_client_applies_language_after_compatibility_handshake(monkeypatch) -> None:
    async def run() -> None:
        socket = FakeWebSocket(
            {"type": "server.hello"},
            {"type": "session.created"},
            {"type": "transcription_session.created"},
            {"type": "session.updated"},
        )

        async def connect(_endpoint):
            return socket

        monkeypatch.setattr(ws_clients, "_connect", connect)
        client = ws_clients.LocalSTTClient(
            SimpleNamespace(transport="uds", address="/tmp/nemotron.sock"),
            language="en",
        )

        await client.connect()

        assert socket.sent == [
            {
                "type": "session.update",
                "session": {"type": "transcription", "audio": {"input": {"language": "en"}}},
            }
        ]
        await client.close()

    asyncio.run(run())


def test_tts_client_applies_voice_during_session_creation(monkeypatch) -> None:
    async def run() -> None:
        socket = FakeWebSocket(
            {"type": "server.hello", "audio": {"rate": 24_000}},
            {"type": "session.created"},
        )

        async def connect(_endpoint):
            return socket

        monkeypatch.setattr(ws_clients, "_connect", connect)
        client = ws_clients.LocalTTSClient(
            SimpleNamespace(transport="tcp", address="127.0.0.1:8965"),
            voice_id="azelma",
        )

        hello = await client.connect()

        assert hello["audio"]["rate"] == 24_000
        assert socket.sent == [{"type": "session.update", "voice": "azelma"}]
        await client.close()

    asyncio.run(run())


def test_event_stream_closes_when_its_event_budget_is_exceeded(monkeypatch) -> None:
    async def run() -> None:
        socket = FakeWebSocket({"type": "one"}, {"type": "two"})
        monkeypatch.setattr(ws_clients, "_MAX_EVENT_COUNT", 1)

        with pytest.raises(RuntimeError, match="event stream limit"):
            [event async for event in ws_clients._events(socket)]

        assert socket.closed is True

    asyncio.run(run())
