"""Tests for ``scripts/validate_v013_evidence.py --write-manifest`` and the
shared status enum in ``scripts/_evidence_common.py``.

Plan: docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md,
Phase 2 bullets 189-192 and the "New files to create" entries for
``scripts/_evidence_common.py`` and the promotion manifest. This module
imports both scripts by path -- neither is a package -- following the
``_load_validator()`` pattern in tests/test_v013_perf_scenarios.py.

The writer mode is invoked as:

    validate_v013_evidence.py --write-manifest --manifest-phase provisional
        --phase0-input <path> --phase1-input <path> --phase2-input <path>
        [--phase4c-input <path>] --source-commit <sha> --source-tree-hash <hash>
        --deployed-at-utc <iso8601> --feature-policy-fingerprint <fp>
        --output <path>

It must revalidate all supplied inputs against their respective schemas and
refuse (nonzero exit, no ``--output`` file written) when any gate is
missing, failed, stale, malformed, or phase-incomplete. A provisional
manifest is always ``promotion_eligible=false``: Phase 2 can only produce a
provisional manifest (Phase 3 stamps the ``final`` completion hash), so the
writer's phase2-transport-eligible / phase2-transport-ineligible split still
matters for the *recorded reason*, even though ``promotion_eligible`` stays
``False`` either way at this phase.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
VALIDATOR_PATH = REPO_ROOT / "scripts" / "validate_v013_evidence.py"
EVIDENCE_COMMON_PATH = REPO_ROOT / "scripts" / "_evidence_common.py"
TRANSPORT_VALIDATOR_PATH = REPO_ROOT / "scripts" / "validate_phase2_transport_browser_contract.py"


def _load(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _validator() -> Any:
    if not VALIDATOR_PATH.exists() or not TRANSPORT_VALIDATOR_PATH.exists():
        pytest.skip(
            "validate_v013_evidence.py / validate_phase2_transport_browser_contract.py "
            "not yet implemented (Phase 2 concurrent implementer)"
        )
    return _load(VALIDATOR_PATH, "validate_v013_evidence")


def _evidence_common() -> Any:
    if not EVIDENCE_COMMON_PATH.exists():
        pytest.skip(
            "scripts/_evidence_common.py not yet implemented (Phase 2 concurrent implementer)"
        )
    return _load(EVIDENCE_COMMON_PATH, "_evidence_common")


UNAVAILABLE = "unavailable"

SOURCE_COMMIT = "a" * 40
SOURCE_TREE_HASH = "b" * 64
FEATURE_POLICY_FINGERPRINT = "fp-" + "c" * 16


def _phase0_record(**overrides: Any) -> dict[str, Any]:
    record = {
        "phase": "phase0",
        "scenario": "direct",
        "turn_id": "turn-1",
        "work_item_id": None,
        "provider": UNAVAILABLE,
        "model": UNAVAILABLE,
        "query_chars": None,
        "context_chars": None,
        "routing_phase_latency_ms": 12.0,
        "outcome": "direct",
        "disposition": "not_applicable",
        "sample_timestamp_ms": 0,
        "run_id": "run-0",
        "sample_index": 0,
        "sample_count": 1,
    }
    record.update(overrides)
    return record


def _phase0_fixture(*, sample_count: int = 6) -> list[dict[str, Any]]:
    """The six named Phase 0 scenarios/outcomes, credential-free stratum."""
    rows = [
        ("direct", "direct", "not_applicable"),
        ("delegated_complete", "completed", "autoplay"),
        ("retained_late", "retained", "display_only"),
        ("cancellation", "cancelled", "suppressed"),
        ("reconnect", "completed", "display_only"),
        ("same_epoch_newer_turn", "cancelled", "suppressed"),
    ]
    return [
        _phase0_record(
            scenario=scenario,
            turn_id=f"turn-{scenario}",
            outcome=outcome,
            disposition=disposition,
            sample_index=index,
            sample_count=sample_count,
        )
        for index, (scenario, outcome, disposition) in enumerate(rows)
    ]


def _phase1_record(**overrides: Any) -> dict[str, Any]:
    record = _phase0_record(
        phase="phase1",
        scenario="exactly_once",
        outcome="completed",
        disposition="autoplay",
        work_item_id="work-1",
        ack_id="ack-1",
        ack_enqueued=True,
        ack_enqueued_ms=5,
        ack_terminal_state="admitted_completed",
    )
    record.update(overrides)
    return record


def _phase1_fixture() -> list[dict[str, Any]]:
    """Covers phase1 scenarios/outcomes plus the queued_discard/
    admitted_completion x2 minimum counts."""
    rows = [
        {"scenario": "ack_enqueued_timing", "outcome": "completed"},
        {"scenario": "exactly_once", "outcome": "completed"},
        {"scenario": "queued_discard", "outcome": "cancelled", "ack_terminal_state": "discarded"},
        {"scenario": "queued_discard", "outcome": "cancelled", "ack_terminal_state": "discarded"},
        {
            "scenario": "admitted_completion",
            "outcome": "completed",
            "ack_terminal_state": "admitted_completed",
        },
        {
            "scenario": "admitted_completion",
            "outcome": "completed",
            "ack_terminal_state": "admitted_completed",
        },
        {"scenario": "no_tts", "outcome": "completed", "ack_terminal_state": "no_tts"},
    ]
    return [
        _phase1_record(
            turn_id=f"turn-p1-{index}", sample_index=index, sample_count=len(rows), **row
        )
        for index, row in enumerate(rows)
    ]


def _write_jsonl(tmp_path: Path, name: str, records: list[dict[str, Any]]) -> Path:
    path = tmp_path / name
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    return path


def _valid_transport_artifact(**overrides: Any) -> dict[str, Any]:
    import re

    bun_lock = (REPO_ROOT / "web" / "bun.lock").read_text(encoding="utf-8")
    match = re.search(
        r'"@pipecat-ai/small-webrtc-transport":\s*\["@pipecat-ai/small-webrtc-transport@([^"]+)"'
        r'[^\]]*"(sha512-[^"]+)"\]',
        bun_lock,
    )
    assert match is not None
    package_version, package_integrity = match.group(1), match.group(2)
    payload = {
        "status": "promotion_eligible",
        "reason": None,
        "promotion_eligible": True,
        "source_commit": SOURCE_COMMIT,
        "source_tree_hash": SOURCE_TREE_HASH,
        "package_version": package_version,
        "package_integrity": package_integrity,
        "source_anchor": "https://github.com/pipecat-ai/small-webrtc-transport/tree/v1.10.6",
        "fake_route_artifact_sha256": "d" * 64,
        "audibility": {
            "state": "audibility_verified",
            "browser_name": "Chrome",
            "browser_version": "128.0.6613.0",
            "os_device_name": "macOS / MacBook Pro",
            "output_route": "default",
            "prior_user_gesture": True,
            "check_method": "manual-named-browser",
            "checked_at_utc": "2026-08-04T00:00:00Z",
            "runner_identity": "varun@varunsingh.net",
            "checked_source_commit": SOURCE_COMMIT,
            "route_artifact_sha256": "d" * 64,
            "package_version": package_version,
            "package_integrity": package_integrity,
            "play_result": "resolved",
        },
    }
    payload.update(overrides)
    return payload


def _write_manifest_argv(tmp_path: Path, *, phase0: Path, phase1: Path, phase2: Path) -> list[str]:
    return [
        "--write-manifest",
        "--manifest-phase",
        "provisional",
        "--phase0-input",
        str(phase0),
        "--phase1-input",
        str(phase1),
        "--phase2-input",
        str(phase2),
        "--source-commit",
        SOURCE_COMMIT,
        "--source-tree-hash",
        SOURCE_TREE_HASH,
        "--deployed-at-utc",
        "2026-08-04T00:00:00Z",
        "--feature-policy-fingerprint",
        FEATURE_POLICY_FINGERPRINT,
        "--output",
        str(tmp_path / "promotion-manifest.json"),
    ]


def _full_valid_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    phase0 = _write_jsonl(tmp_path, "phase0.jsonl", _phase0_fixture())
    phase1 = _write_jsonl(tmp_path, "phase1.jsonl", _phase1_fixture())
    phase2 = tmp_path / "phase2.json"
    phase2.write_text(json.dumps(_valid_transport_artifact()))
    return phase0, phase1, phase2


def test_write_manifest_refuses_when_phase0_input_is_missing(tmp_path: Path) -> None:
    module = _validator()
    phase0 = tmp_path / "missing-phase0.jsonl"
    _, phase1, phase2 = _full_valid_inputs(tmp_path)

    exit_code = module.main(
        _write_manifest_argv(tmp_path, phase0=phase0, phase1=phase1, phase2=phase2)
    )

    assert exit_code != 0
    assert not (tmp_path / "promotion-manifest.json").exists()


def test_write_manifest_refuses_when_phase0_input_is_malformed(tmp_path: Path) -> None:
    module = _validator()
    _, phase1, phase2 = _full_valid_inputs(tmp_path)
    phase0 = tmp_path / "phase0-malformed.jsonl"
    phase0.write_text("{not json")

    exit_code = module.main(
        _write_manifest_argv(tmp_path, phase0=phase0, phase1=phase1, phase2=phase2)
    )

    assert exit_code != 0
    assert not (tmp_path / "promotion-manifest.json").exists()


def test_write_manifest_refuses_when_phase1_input_is_incomplete(tmp_path: Path) -> None:
    """A phase1 artifact missing the queued_discard/admitted_completion x2
    minimum counts (plan bullet 174) must not silently pass into the
    manifest."""
    module = _validator()
    phase0, _, phase2 = _full_valid_inputs(tmp_path)
    incomplete_phase1 = _write_jsonl(tmp_path, "phase1-incomplete.jsonl", [_phase1_record()])

    exit_code = module.main(
        _write_manifest_argv(tmp_path, phase0=phase0, phase1=incomplete_phase1, phase2=phase2)
    )

    assert exit_code != 0
    assert not (tmp_path / "promotion-manifest.json").exists()


def test_write_manifest_records_real_stratum_missing_reason_for_credential_free_only_inputs(
    tmp_path: Path,
) -> None:
    """Plan: 'No v0.1.3 phase mandates paid real-strata collection ... v0.1.3
    ships with autoplay promotion expected ineligible.' All-credential-free
    Phase 0/1 inputs must still let the writer succeed (schema-valid) but
    record why promotion stays ineligible."""
    module = _validator()
    phase0, phase1, phase2 = _full_valid_inputs(tmp_path)

    exit_code = module.main(
        _write_manifest_argv(tmp_path, phase0=phase0, phase1=phase1, phase2=phase2)
    )

    assert exit_code == 0
    manifest = json.loads((tmp_path / "promotion-manifest.json").read_text())
    assert manifest["promotion_eligible"] is False
    assert manifest["reason"] == "real_stratum_missing"


def test_write_manifest_refuses_to_recompute_the_phase2_transport_gate(tmp_path: Path) -> None:
    """Plan: the Phase 2 transport artifact's promotion_eligible/gate result
    is 'an input to this writer, not something this script recomputes.' An
    artifact that misreports promotion_eligible=true for a blocked status is
    schema/consistency-invalid and must be rejected by the transport
    validator this writer delegates to."""
    module = _validator()
    phase0, phase1, _ = _full_valid_inputs(tmp_path)
    phase2 = tmp_path / "phase2-inconsistent.json"
    phase2.write_text(
        json.dumps(_valid_transport_artifact(status="blocked", promotion_eligible=True))
    )

    exit_code = module.main(
        _write_manifest_argv(tmp_path, phase0=phase0, phase1=phase1, phase2=phase2)
    )

    assert exit_code != 0
    assert not (tmp_path / "promotion-manifest.json").exists()


def test_write_manifest_provisional_manifest_is_always_promotion_ineligible(tmp_path: Path) -> None:
    """Plan: 'A provisional manifest is accepted for diagnostics but is
    permanently promotion_eligible=false and display-only.' Even with a
    fully promotion_eligible Phase 2 transport artifact, a provisional
    manifest never reports eligible."""
    module = _validator()
    phase0, phase1, phase2 = _full_valid_inputs(tmp_path)

    exit_code = module.main(
        _write_manifest_argv(tmp_path, phase0=phase0, phase1=phase1, phase2=phase2)
    )

    assert exit_code == 0
    manifest = json.loads((tmp_path / "promotion-manifest.json").read_text())
    assert manifest["manifest_phase"] == "provisional"
    assert manifest["promotion_eligible"] is False


def test_write_manifest_embeds_source_and_policy_fingerprint_identity(tmp_path: Path) -> None:
    module = _validator()
    phase0, phase1, phase2 = _full_valid_inputs(tmp_path)

    exit_code = module.main(
        _write_manifest_argv(tmp_path, phase0=phase0, phase1=phase1, phase2=phase2)
    )

    assert exit_code == 0
    manifest = json.loads((tmp_path / "promotion-manifest.json").read_text())
    assert manifest["source_commit"] == SOURCE_COMMIT
    assert manifest["source_tree_hash"] == SOURCE_TREE_HASH
    assert manifest["feature_policy_fingerprint"] == FEATURE_POLICY_FINGERPRINT
    assert "generated_at_utc" in manifest


def test_write_manifest_embeds_artifact_hashes_for_all_three_inputs(tmp_path: Path) -> None:
    """Plan: 'binds release/source/source-tree/schema/Phase-0/1/2/3-artifact
    hashes.'"""
    module = _validator()
    phase0, phase1, phase2 = _full_valid_inputs(tmp_path)

    exit_code = module.main(
        _write_manifest_argv(tmp_path, phase0=phase0, phase1=phase1, phase2=phase2)
    )

    assert exit_code == 0
    manifest = json.loads((tmp_path / "promotion-manifest.json").read_text())
    inputs = manifest["inputs"]
    for phase_name, input_path in (("phase0", phase0), ("phase1", phase1), ("phase2", phase2)):
        assert inputs[phase_name]["path"] == str(input_path)
        assert len(inputs[phase_name]["sha256"]) == 64  # sha256 hex digest


def test_evidence_common_status_enum_contains_the_documented_values() -> None:
    """Plan: '_evidence_common.py -- shared eligibility/validation module
    owning the blocked/not-run/unavailable/provider_effect_uncontrolled
    status enum ... consumed by validate_v013_evidence.py,
    validate_phase2_transport_browser_contract.py, and
    analyze_query_context_latency.py; the three scripts must not
    re-implement this state machine.'"""
    module = _evidence_common()
    status_values = {member.value for member in module.EvidenceStatus}
    # provider_effect_uncontrolled is a Phase 4 (query-context experiment)
    # addition; Phase 2 only requires the shared vocabulary to already
    # contain blocked/not_run so validate_phase2_transport_browser_contract.py
    # and validate_v013_evidence.py draw from the same enum.
    assert {"blocked", "not_run"} <= status_values
