"""Wire-level settings tests for the local STT/TTS websocket clients."""

import asyncio
import json
from types import SimpleNamespace

from server.services import ws_clients


class FakeWebSocket:
    def __init__(self, *events: dict) -> None:
        self.events = [json.dumps(event) for event in events]
        self.sent: list[dict] = []

    async def recv(self) -> str:
        return self.events.pop(0)

    async def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw))

    async def close(self) -> None:
        pass


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
