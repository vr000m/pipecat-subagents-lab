"""Small websocket clients for the sibling local STT/TTS services.

The service adapters keep their factories injectable, but the default browser
host must also have a real wire path when an operator configures an endpoint.
These clients implement only the versioned message seams used by the adapters;
they do not contain model, credential, or browser policy.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import websockets
from loguru import logger
from websockets.asyncio.client import connect as ws_connect
from websockets.asyncio.client import unix_connect as ws_unix_connect

_CONNECT_TIMEOUT_SECONDS = 10.0
_HANDSHAKE_TIMEOUT_SECONDS = 10.0
_OPERATION_TIMEOUT_SECONDS = 75.0
_CLOSE_TIMEOUT_SECONDS = 5.0
_MAX_HANDSHAKE_EVENTS = 128
_MAX_EVENT_COUNT = 4096
_MAX_MESSAGE_BYTES = 1024 * 1024
_MAX_OPERATION_BYTES = 64 * 1024 * 1024


async def _connect(endpoint: Any) -> Any:
    transport = endpoint.transport
    address = endpoint.address
    if transport == "uds":
        return await asyncio.wait_for(
            ws_unix_connect(address, "ws://localhost/"),
            timeout=_CONNECT_TIMEOUT_SECONDS,
        )
    if transport not in {"tcp", "ws", "wss"}:
        raise ValueError(f"unsupported local service transport: {transport}")
    scheme = "wss" if transport == "wss" else "ws"
    return await asyncio.wait_for(
        ws_connect(f"{scheme}://{address}/"),
        timeout=_CONNECT_TIMEOUT_SECONDS,
    )


async def _receive_json(ws: Any) -> dict[str, Any]:
    raw = await asyncio.wait_for(ws.recv(), timeout=_HANDSHAKE_TIMEOUT_SECONDS)
    if isinstance(raw, (bytes, bytearray)):
        raise TypeError("unexpected binary frame during local service handshake")
    if len(raw.encode("utf-8")) > _MAX_MESSAGE_BYTES:
        raise RuntimeError("local service handshake message limit exceeded")
    return json.loads(raw)


async def _receive_until(ws: Any, expected_types: set[str]) -> dict[str, Any]:
    """Drain compatibility alias events until the required acknowledgement arrives."""
    for _ in range(_MAX_HANDSHAKE_EVENTS):
        event = await _receive_json(ws)
        if event.get("type") in expected_types:
            return event
    raise RuntimeError("local service handshake event limit exceeded")


async def _close_socket(ws: Any) -> None:
    """Bound websocket cleanup so a failed peer cannot retain the caller."""
    try:
        await asyncio.wait_for(ws.close(), timeout=_CLOSE_TIMEOUT_SECONDS)
    except (TimeoutError, websockets.exceptions.ConnectionClosed):
        pass


async def _events(ws: Any) -> AsyncIterator[dict[str, Any]]:
    event_count = 0
    byte_count = 0
    try:
        async with asyncio.timeout(_OPERATION_TIMEOUT_SECONDS):
            async for raw in ws:
                event_count += 1
                raw_size = (
                    len(raw) if isinstance(raw, (bytes, bytearray)) else len(raw.encode("utf-8"))
                )
                byte_count += raw_size
                if event_count > _MAX_EVENT_COUNT or byte_count > _MAX_OPERATION_BYTES:
                    await ws.close()
                    raise RuntimeError("local service event stream limit exceeded")
                if isinstance(raw, (bytes, bytearray)):
                    continue
                if raw_size > _MAX_MESSAGE_BYTES:
                    await ws.close()
                    raise RuntimeError("local service message limit exceeded")
                try:
                    yield json.loads(raw)
                except json.JSONDecodeError:
                    continue
    except TimeoutError:
        await ws.close()
        raise RuntimeError("local service event stream timed out") from None
    except websockets.exceptions.ConnectionClosed as exc:
        yield {
            "type": "error",
            "message": f"local websocket closed before completion (code {exc.code})",
        }


class LocalSTTClient:
    def __init__(self, endpoint: Any, *, language: str | None = None) -> None:
        self.endpoint = endpoint
        self.language = language
        self._ws: Any = None

    async def connect(self) -> None:
        await self.close()
        ws = None
        try:
            async with asyncio.timeout(_HANDSHAKE_TIMEOUT_SECONDS):
                ws = await _connect(self.endpoint)
                self._ws = ws
                hello = await _receive_json(ws)
                if hello.get("type") != "server.hello":
                    raise RuntimeError(f"expected STT server.hello, got {hello.get('type')}")
                created = await _receive_json(ws)
                if created.get("type") != "session.created":
                    raise RuntimeError(f"expected STT session.created, got {created.get('type')}")
                if self.language:
                    await ws.send(
                        json.dumps(
                            {
                                "type": "session.update",
                                "session": {
                                    "type": "transcription",
                                    "audio": {"input": {"language": self.language}},
                                },
                            }
                        )
                    )
                    await _receive_until(
                        ws,
                        {"session.updated", "transcription_session.updated"},
                    )
        except BaseException:
            self._ws = None
            if ws is not None:
                try:
                    await _close_socket(ws)
                except Exception:  # noqa: BLE001  # intentional catch-all: closing the socket during error teardown must not mask the original exception
                    logger.debug("failed to close local service socket during error teardown")
            raise

    async def send_audio(self, audio: bytes) -> None:
        await asyncio.wait_for(self._ws.send(audio), timeout=_OPERATION_TIMEOUT_SECONDS)

    async def commit(self) -> None:
        await asyncio.wait_for(
            self._ws.send(json.dumps({"type": "input_audio_buffer.commit"})),
            timeout=_OPERATION_TIMEOUT_SECONDS,
        )

    def events(self) -> AsyncIterator[dict[str, Any]]:
        return _events(self._ws)

    async def close(self) -> None:
        ws, self._ws = self._ws, None
        if ws is not None:
            await _close_socket(ws)


class LocalTTSClient:
    def __init__(self, endpoint: Any, *, voice_id: str | None = None) -> None:
        self.endpoint = endpoint
        self.voice_id = voice_id
        self._ws: Any = None

    async def connect(self) -> dict[str, Any]:
        await self.close()
        ws = None
        try:
            async with asyncio.timeout(_HANDSHAKE_TIMEOUT_SECONDS):
                ws = await _connect(self.endpoint)
                self._ws = ws
                hello = await _receive_json(ws)
                if hello.get("type") != "server.hello":
                    raise RuntimeError(f"expected TTS server.hello, got {hello.get('type')}")
                if self.voice_id:
                    await ws.send(json.dumps({"type": "session.update", "voice": self.voice_id}))
                    created = await _receive_json(ws)
                    if created.get("type") not in {"session.created", "session.updated"}:
                        raise RuntimeError(
                            f"expected TTS session.created, got {created.get('type')}"
                        )
                return hello
        except BaseException:
            self._ws = None
            if ws is not None:
                try:
                    await _close_socket(ws)
                except Exception:  # noqa: BLE001  # intentional catch-all: closing the socket during error teardown must not mask the original exception
                    logger.debug("failed to close local service socket during error teardown")
            raise

    async def append(self, text: str) -> None:
        await asyncio.wait_for(
            self._ws.send(json.dumps({"type": "input_text.append", "text": text})),
            timeout=_OPERATION_TIMEOUT_SECONDS,
        )

    async def commit(self) -> None:
        await asyncio.wait_for(
            self._ws.send(json.dumps({"type": "input_text.commit"})),
            timeout=_OPERATION_TIMEOUT_SECONDS,
        )

    def events(self) -> AsyncIterator[dict[str, Any]]:
        return _events(self._ws)

    async def close(self) -> None:
        ws, self._ws = self._ws, None
        if ws is not None:
            await _close_socket(ws)


def default_stt_client_factory(endpoint: Any, *, language: str | None = None) -> LocalSTTClient:
    return LocalSTTClient(endpoint, language=language)


def default_tts_client_factory(endpoint: Any, *, voice_id: str | None = None) -> LocalTTSClient:
    return LocalTTSClient(endpoint, voice_id=voice_id)
