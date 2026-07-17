"""Standalone, values-redacted environment and local-service preflight."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Protocol

from .config import Config


class Probe(Protocol):
    def discover(self, service: str) -> tuple[str, str]: ...
    def healthcheck(self, service: str, transport: str, address: str) -> bool: ...


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
