"""Deterministic, values-redacted preflight tests."""

from dataclasses import dataclass

import pytest

from server.config import Config
from server.preflight import PreflightReport, run_preflight


@dataclass(frozen=True)
class FakeProbe:
    """A probe boundary that cannot access the network or print secrets."""

    stt: tuple[str, str] = ("uds", "/tmp/stt.sock")
    tts: tuple[str, str] = ("tcp", "127.0.0.1:9001")
    healthy: bool = True

    def discover(self, service: str) -> tuple[str, str]:
        return self.stt if service == "stt" else self.tts

    def healthcheck(self, service: str, transport: str, address: str) -> bool:
        assert transport and address
        return self.healthy


def test_preflight_persists_discovered_endpoints_and_redacts_values() -> None:
    secret = "sk-test-secret"
    report = run_preflight(
        Config(openai_api_key=secret),
        probe=FakeProbe(),
        authenticated_capability_check=lambda _config: "unavailable",
    )

    assert isinstance(report, PreflightReport)
    assert report.ok is True
    assert report.endpoints["stt"] == {"transport": "uds", "address": "/tmp/stt.sock"}
    assert report.endpoints["tts"] == {"transport": "tcp", "address": "127.0.0.1:9001"}
    assert report.authenticated_capability == "unavailable"
    assert secret not in repr(report)
    assert secret not in report.redacted_text


def test_preflight_reports_unreachable_or_health_mismatched_services() -> None:
    report = run_preflight(
        Config(),
        probe=FakeProbe(healthy=False),
        authenticated_capability_check=lambda _config: "unavailable",
    )

    assert report.ok is False
    assert {"stt", "tts"} <= set(report.failures)
    assert all("secret" not in failure.lower() for failure in report.failures.values())


def test_preflight_checks_required_variable_names_without_requiring_paid_access() -> None:
    report = run_preflight(
        Config(),
        probe=FakeProbe(),
        authenticated_capability_check=lambda _config: "unavailable",
    )

    assert report.ok is True
    assert report.authenticated_capability == "unavailable"
    assert report.paid_smoke_required is False


def test_preflight_propagates_actionable_probe_failures_without_phase_three_imports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imported: list[str] = []

    def record_import(name: str) -> None:
        imported.append(name)

    monkeypatch.setattr("server.preflight.record_phase_three_import", record_import)
    run_preflight(
        Config(),
        probe=FakeProbe(healthy=False),
        authenticated_capability_check=lambda _config: "unavailable",
    )

    assert imported == []
