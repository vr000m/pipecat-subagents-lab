"""Standalone, values-redacted environment and local-service preflight."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Callable, Protocol

from websockets.sync.client import connect as ws_connect
from websockets.sync.client import unix_connect as ws_unix_connect

from .config import Config


class Probe(Protocol):
    def discover(self, service: str) -> tuple[str, str]: ...
    def healthcheck(self, service: str, transport: str, address: str) -> bool: ...


class ConfiguredServiceProbe:
    """Probe configured speech providers without exposing credential values."""

    def __init__(self, config: Config, *, timeout_seconds: float = 2.0) -> None:
        self.config = config
        self.timeout_seconds = timeout_seconds

    def discover(self, service: str) -> tuple[str, str]:
        provider = getattr(self.config, f"{service}_provider")
        if provider != "local":
            return "hosted", provider
        endpoint = getattr(self.config, f"{service}_endpoint")
        if endpoint is None:
            raise ValueError(f"{service} endpoint is not configured")
        return endpoint

    def healthcheck(self, service: str, transport: str, address: str) -> bool:
        if transport == "hosted":
            if service == "stt":
                return address == "deepgram" and bool(self.config.deepgram_api_key)
            return (
                address == "cartesia"
                and bool(self.config.cartesia_api_key)
                and bool(self.config.cartesia_voice_id)
            )
        if transport == "uds":
            connection = ws_unix_connect(
                address,
                uri="ws://localhost/",
                open_timeout=self.timeout_seconds,
                close_timeout=self.timeout_seconds,
            )
        else:
            scheme = "wss" if transport == "wss" else "ws"
            connection = ws_connect(
                f"{scheme}://{address}/",
                open_timeout=self.timeout_seconds,
                close_timeout=self.timeout_seconds,
            )
        with connection as websocket:
            hello = self._receive_json(websocket)
            if hello.get("type") != "server.hello":
                return False
            if service == "stt":
                created = self._receive_json(websocket)
                if created.get("type") != "session.created":
                    return False
                if self.config.stt_language:
                    websocket.send(
                        json.dumps(
                            {
                                "type": "session.update",
                                "session": {
                                    "type": "transcription",
                                    "audio": {"input": {"language": self.config.stt_language}},
                                },
                            }
                        )
                    )
                    return self._receive_until(
                        websocket,
                        {"session.updated", "transcription_session.updated"},
                    )
                return True
            if self.config.tts_voice_id:
                websocket.send(
                    json.dumps(
                        {
                            "type": "session.update",
                            "voice": self.config.tts_voice_id,
                        }
                    )
                )
                return self._receive_until(
                    websocket,
                    {"session.created", "session.updated"},
                )
            return True

    def _receive_json(self, websocket: object) -> dict[str, object]:
        raw = websocket.recv(timeout=self.timeout_seconds)  # type: ignore[attr-defined]
        if not isinstance(raw, str):
            return {}
        value = json.loads(raw)
        return value if isinstance(value, dict) else {}

    def _receive_until(self, websocket: object, expected_types: set[str]) -> bool:
        for _ in range(128):
            event = self._receive_json(websocket)
            if event.get("type") in expected_types:
                return True
            if event.get("type") in {"error", "response.failed"}:
                return False
        return False


def record_phase_three_import(_: str) -> None:
    """Compatibility seam intentionally unused by this phase."""


@dataclass(frozen=True, repr=False)
class PreflightReport:
    ok: bool
    config: Config
    endpoints: dict[str, dict[str, str]] = field(default_factory=dict)
    failures: dict[str, str] = field(default_factory=dict)
    authenticated_capability: str = "unavailable"
    paid_smoke_required: bool = False
    redacted_text: str = ""

    def __repr__(self) -> str:
        return (
            f"PreflightReport(ok={self.ok!r}, endpoints={self.endpoints!r}, "
            f"failures={self.failures!r}, authenticated_capability={self.authenticated_capability!r}, "
            f"paid_smoke_required={self.paid_smoke_required!r})"
        )


def run_preflight(
    config: Config,
    *,
    probe: Probe,
    authenticated_capability_check: Callable[[Config], str] | None = None,
) -> PreflightReport:
    endpoints: dict[str, dict[str, str]] = {}
    failures: dict[str, str] = {}
    for service in ("stt", "tts"):
        try:
            transport, address = probe.discover(service)
            if not transport or not address:
                raise ValueError("discovery returned an empty transport or address")
            endpoints[service] = {"transport": transport, "address": address}
            if not probe.healthcheck(service, transport, address):
                failures[service] = (
                    f"{service} endpoint is unreachable or health/protocol incompatible"
                )
        except Exception as exc:  # probe errors become actionable, secret-free diagnostics
            failures[service] = f"{service} endpoint preflight failed: {type(exc).__name__}"
    capability = (
        authenticated_capability_check(config) if authenticated_capability_check else "unavailable"
    )
    if capability not in {"available", "unavailable", "unconfirmed"}:
        failures["openai"] = "authenticated capability check returned an invalid status"
        capability = "unconfirmed"
    discovered_config = config
    for service, endpoint in endpoints.items():
        if endpoint["transport"] == "hosted":
            continue
        discovered_config = discovered_config.with_discovered_endpoint(
            service, endpoint["transport"], endpoint["address"]
        )
    return PreflightReport(
        ok=not failures,
        config=discovered_config,
        endpoints=endpoints,
        failures=failures,
        authenticated_capability=capability,
        paid_smoke_required=False,
        redacted_text=f"stt={endpoints.get('stt', 'unavailable')}; tts={endpoints.get('tts', 'unavailable')}; "
        f"openai={capability}",
    )


def main() -> int:
    from .config import load_config

    config = load_config()
    report = run_preflight(
        config,
        probe=ConfiguredServiceProbe(config),
        authenticated_capability_check=lambda value: (
            "available" if value.openai_api_key else "unavailable"
        ),
    )
    print(report.redacted_text)
    for service, failure in sorted(report.failures.items()):
        print(f"{service}: {failure}")
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
