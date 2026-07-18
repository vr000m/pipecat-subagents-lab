"""Small websocket clients for the sibling local STT/TTS services.

The service adapters keep their factories injectable, but the default browser
host must also have a real wire path when an operator configures an endpoint.
These clients implement only the versioned message seams used by the adapters;
they do not contain model, credential, or browser policy.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import websockets
from websockets.asyncio.client import connect as ws_connect
from websockets.asyncio.client import unix_connect as ws_unix_connect


async def _connect(endpoint: Any) -> Any:
    transport = endpoint.transport
    address = endpoint.address
    if transport == "uds":
        return await ws_unix_connect(address, "ws://localhost/")
    if transport not in {"tcp", "ws", "wss"}:
        raise ValueError(f"unsupported local service transport: {transport}")
    scheme = "wss" if transport == "wss" else "ws"
    return await ws_connect(f"{scheme}://{address}/")


async def _receive_json(ws: Any) -> dict[str, Any]:
    raw = await ws.recv()
    if isinstance(raw, (bytes, bytearray)):
        raise RuntimeError("unexpected binary frame during local service handshake")
    return json.loads(raw)


async def _events(ws: Any) -> AsyncIterator[dict[str, Any]]:
    try:
        async for raw in ws:
            if isinstance(raw, (bytes, bytearray)):
                continue
            try:
                yield json.loads(raw)
            except json.JSONDecodeError:
                continue
    except websockets.exceptions.ConnectionClosed:
        return


class LocalSTTClient:
    def __init__(self, endpoint: Any) -> None:
        self.endpoint = endpoint
        self._ws: Any = None

    async def connect(self) -> None:
        self._ws = await _connect(self.endpoint)
        hello = await _receive_json(self._ws)
        if hello.get("type") != "server.hello":
            raise RuntimeError(f"expected STT server.hello, got {hello.get('type')}")
        created = await _receive_json(self._ws)
        if created.get("type") != "session.created":
            raise RuntimeError(f"expected STT session.created, got {created.get('type')}")

    async def send_audio(self, audio: bytes) -> None:
        await self._ws.send(audio)

    async def commit(self) -> None:
        await self._ws.send(json.dumps({"type": "input_audio_buffer.commit"}))

    def events(self) -> AsyncIterator[dict[str, Any]]:
        return _events(self._ws)

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
            self._ws = None


class LocalTTSClient:
    def __init__(self, endpoint: Any) -> None:
        self.endpoint = endpoint
        self._ws: Any = None

    async def connect(self) -> dict[str, Any]:
        self._ws = await _connect(self.endpoint)
        hello = await _receive_json(self._ws)
        if hello.get("type") != "server.hello":
            raise RuntimeError(f"expected TTS server.hello, got {hello.get('type')}")
        return hello

    async def append(self, text: str) -> None:
        await self._ws.send(json.dumps({"type": "input_text.append", "text": text}))

    async def commit(self) -> None:
        await self._ws.send(json.dumps({"type": "input_text.commit"}))

    def events(self) -> AsyncIterator[dict[str, Any]]:
        return _events(self._ws)

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
            self._ws = None


def default_stt_client_factory(endpoint: Any) -> LocalSTTClient:
    return LocalSTTClient(endpoint)


def default_tts_client_factory(endpoint: Any) -> LocalTTSClient:
    return LocalTTSClient(endpoint)
