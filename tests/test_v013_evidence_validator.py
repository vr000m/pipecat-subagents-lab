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
            "checked_source_tree_hash": SOURCE_TREE_HASH,
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


def test_plain_phase_input_cli_cleanly_fails_on_a_missing_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The plain ``--phase``/``--input`` CLI path (``validate_artifact`` ->
    ``load_records``) must still print a clean ``FAIL: ...`` and exit 1 for a
    missing input file -- not a raw traceback. ``load_records`` delegates to
    the shared ``_evidence_common.load_jsonl``, which raises
    ``EvidenceGateError``; that must be translated to
    ``EvidenceValidationError`` since this CLI path's except clause only
    catches the latter."""
    module = _validator()
    missing = tmp_path / "does-not-exist.jsonl"

    exit_code = module.main(["--phase", "phase0", "--input", str(missing)])

    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err.startswith("FAIL:")


def test_plain_phase_input_cli_cleanly_fails_on_a_malformed_json_line(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Same CLI path, but for a malformed JSON line rather than a missing
    file -- both must translate cleanly through ``load_records`` rather than
    raising an uncaught ``EvidenceGateError``."""
    module = _validator()
    malformed = tmp_path / "phase0-malformed.jsonl"
    malformed.write_text("{not json")

    exit_code = module.main(["--phase", "phase0", "--input", str(malformed)])

    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err.startswith("FAIL:")


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


# --- Phase 3: --phase3-input extension and scripts/record_phase3_completion.py -
#
# Plan bullet 245 (Phase 3 closing step): `scripts/record_phase3_completion.py`
# writes a dated completion record binding source-commit/source-tree-hash and
# a sha256 digest of this phase's own Test command; `validate_v013_evidence.py
# --write-manifest --manifest-phase final` then requires a matching
# `--phase3-input` and rejects any mismatch against the earlier phase0/1/2
# artifacts, schema, source identity, or deployment/policy fingerprint.

RECORD_PHASE3_PATH = REPO_ROOT / "scripts" / "record_phase3_completion.py"


def _record_phase3_completion_script() -> Any:
    if not RECORD_PHASE3_PATH.exists():
        pytest.skip(
            "scripts/record_phase3_completion.py not yet implemented "
            "(Phase 3 concurrent implementer)"
        )
    return _load(RECORD_PHASE3_PATH, "record_phase3_completion")


COMMAND_DIGEST = "e" * 64


def _phase3_completion_argv(*, output: Path, command_digest: str = COMMAND_DIGEST) -> list[str]:
    return [
        "--source-commit",
        SOURCE_COMMIT,
        "--source-tree-hash",
        SOURCE_TREE_HASH,
        "--command-digest",
        command_digest,
        "--output",
        str(output),
    ]


def test_record_phase3_completion_writes_a_source_tree_and_command_bound_record(
    tmp_path: Path,
) -> None:
    module = _record_phase3_completion_script()
    output = tmp_path / "v0.1.3-phase3-completion.json"

    exit_code = module.main(_phase3_completion_argv(output=output))

    assert exit_code == 0
    record = json.loads(output.read_text())
    assert record["source_commit"] == SOURCE_COMMIT
    assert record["source_tree_hash"] == SOURCE_TREE_HASH
    assert record["command_digest"] == COMMAND_DIGEST
    assert "generated_at_utc" in record


def test_record_phase3_completion_rejects_an_empty_command_digest(tmp_path: Path) -> None:
    module = _record_phase3_completion_script()
    output = tmp_path / "v0.1.3-phase3-completion.json"

    exit_code = module.main(_phase3_completion_argv(output=output, command_digest=""))

    assert exit_code != 0
    assert not output.exists()


def _write_manifest_final_argv(
    tmp_path: Path,
    *,
    phase0: Path,
    phase1: Path,
    phase2: Path,
    phase3: Path,
) -> list[str]:
    return [
        "--write-manifest",
        "--manifest-phase",
        "final",
        "--phase0-input",
        str(phase0),
        "--phase1-input",
        str(phase1),
        "--phase2-input",
        str(phase2),
        "--phase3-input",
        str(phase3),
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


def _full_valid_inputs_with_phase3(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    phase0, phase1, phase2 = _full_valid_inputs(tmp_path)
    completion_module = _record_phase3_completion_script()
    phase3 = tmp_path / "v0.1.3-phase3-completion.json"
    exit_code = completion_module.main(_phase3_completion_argv(output=phase3))
    assert exit_code == 0
    return phase0, phase1, phase2, phase3


def test_final_manifest_requires_phase3_input_and_binds_its_hash(tmp_path: Path) -> None:
    module = _validator()
    phase0, phase1, phase2, phase3 = _full_valid_inputs_with_phase3(tmp_path)

    exit_code = module.main(
        _write_manifest_final_argv(
            tmp_path, phase0=phase0, phase1=phase1, phase2=phase2, phase3=phase3
        )
    )

    assert exit_code == 0
    manifest = json.loads((tmp_path / "promotion-manifest.json").read_text())
    assert manifest["inputs"]["phase3"]["path"] == str(phase3)
    assert len(manifest["inputs"]["phase3"]["sha256"]) == 64


def test_final_manifest_refuses_when_phase3_input_is_missing(tmp_path: Path) -> None:
    module = _validator()
    phase0, phase1, phase2 = _full_valid_inputs(tmp_path)
    missing_phase3 = tmp_path / "missing-phase3.json"

    exit_code = module.main(
        _write_manifest_final_argv(
            tmp_path, phase0=phase0, phase1=phase1, phase2=phase2, phase3=missing_phase3
        )
    )

    assert exit_code != 0
    assert not (tmp_path / "promotion-manifest.json").exists()


def test_final_manifest_rejects_a_phase3_record_with_mismatched_source_commit(
    tmp_path: Path,
) -> None:
    module = _validator()
    phase0, phase1, phase2 = _full_valid_inputs(tmp_path)
    completion_module = _record_phase3_completion_script()
    phase3 = tmp_path / "v0.1.3-phase3-completion.json"
    completion_module.main(_phase3_completion_argv(output=phase3))
    stale_record = json.loads(phase3.read_text())
    stale_record["source_commit"] = "f" * 40  # does not match --source-commit below
    phase3.write_text(json.dumps(stale_record))

    exit_code = module.main(
        _write_manifest_final_argv(
            tmp_path, phase0=phase0, phase1=phase1, phase2=phase2, phase3=phase3
        )
    )

    assert exit_code != 0
    assert not (tmp_path / "promotion-manifest.json").exists()


def test_final_manifest_binds_the_phase3_command_digest_the_record_declares(tmp_path: Path) -> None:
    """The final manifest must carry forward the exact command_digest the
    Phase 3 completion record declares, so a later loader can compare it
    against the pinned Test command hash rather than trusting the record
    blindly."""
    module = _validator()
    phase0, phase1, phase2 = _full_valid_inputs(tmp_path)
    completion_module = _record_phase3_completion_script()
    phase3 = tmp_path / "v0.1.3-phase3-completion.json"
    distinct_digest = "0" * 64
    completion_module.main(_phase3_completion_argv(output=phase3, command_digest=distinct_digest))

    exit_code = module.main(
        _write_manifest_final_argv(
            tmp_path, phase0=phase0, phase1=phase1, phase2=phase2, phase3=phase3
        )
    )

    assert exit_code == 0
    manifest = json.loads((tmp_path / "promotion-manifest.json").read_text())
    completion_record = json.loads(phase3.read_text())
    assert completion_record["command_digest"] == distinct_digest
    assert manifest["inputs"]["phase3"]["sha256"] == module.sha256_file(phase3)
    # Regression: the validated command binding was computed and then dropped,
    # leaving downstream release verification with only the file hash.
    assert manifest["phase3_command_digest"] == distinct_digest


# --- Regression: phase2 release-identity binding and durable-write safety ---


def test_write_manifest_rejects_a_phase2_artifact_from_a_different_commit(
    tmp_path: Path,
) -> None:
    """Regression: `write_manifest` validated that the Phase 2 evidence was
    eligible but never bound it to the release identity, so a stale-but-valid
    browser check from another build could be stamped into this manifest."""
    module = _validator()
    phase0, phase1, _ = _full_valid_inputs(tmp_path)
    phase2 = tmp_path / "phase2-foreign-commit.json"
    artifact = _valid_transport_artifact(source_commit="c" * 40)
    artifact["audibility"]["checked_source_commit"] = "c" * 40
    phase2.write_text(json.dumps(artifact))

    exit_code = module.main(
        _write_manifest_argv(tmp_path, phase0=phase0, phase1=phase1, phase2=phase2)
    )

    assert exit_code != 0
    assert not (tmp_path / "promotion-manifest.json").exists()


def test_write_manifest_rejects_a_phase2_artifact_from_a_different_tree(tmp_path: Path) -> None:
    module = _validator()
    phase0, phase1, _ = _full_valid_inputs(tmp_path)
    phase2 = tmp_path / "phase2-foreign-tree.json"
    artifact = _valid_transport_artifact(source_tree_hash="c" * 64)
    artifact["audibility"]["checked_source_tree_hash"] = "c" * 64
    phase2.write_text(json.dumps(artifact))

    exit_code = module.main(
        _write_manifest_argv(tmp_path, phase0=phase0, phase1=phase1, phase2=phase2)
    )

    assert exit_code != 0
    assert not (tmp_path / "promotion-manifest.json").exists()


def test_atomic_write_restores_the_previous_manifest_when_the_dir_fsync_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: the parent-directory fsync runs *after* the rename, so a
    failure there left the NEW manifest installed while the exception
    propagated -- breaking the documented old-or-new guarantee."""
    module = _validator()
    output = tmp_path / "manifest.json"
    output.write_text(json.dumps({"manifest_phase": "old"}), encoding="utf-8")
    original = output.read_bytes()

    def _boom(directory: Path) -> None:
        raise OSError("simulated post-rename fsync failure")

    monkeypatch.setattr(module, "_fsync_directory", _boom)
    with pytest.raises(OSError):
        module._atomic_write_manifest(output, {"manifest_phase": "new"})

    assert output.read_bytes() == original, "a failed write must leave the OLD manifest in place"


def test_atomic_write_removes_the_new_manifest_when_there_was_no_previous(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _validator()
    output = tmp_path / "manifest.json"

    def _boom(directory: Path) -> None:
        raise OSError("simulated post-rename fsync failure")

    monkeypatch.setattr(module, "_fsync_directory", _boom)
    with pytest.raises(OSError):
        module._atomic_write_manifest(output, {"manifest_phase": "new"})

    assert not output.exists(), "with no previous manifest, old-or-new means absent"


def test_atomic_write_does_not_write_through_a_planted_previous_symlink(tmp_path: Path) -> None:
    """Regression: the `.previous` backup was opened without O_NOFOLLOW, so a
    symlink planted at that predictable path received the manifest bytes."""
    module = _validator()
    output = tmp_path / "manifest.json"
    output.write_text(json.dumps({"manifest_phase": "old"}), encoding="utf-8")
    victim = tmp_path / "victim.txt"
    victim.write_text("untouched", encoding="utf-8")
    (tmp_path / "manifest.json.previous").symlink_to(victim)

    with pytest.raises(OSError):
        module._atomic_write_manifest(output, {"manifest_phase": "new"})

    assert victim.read_text(encoding="utf-8") == "untouched"


def test_atomic_write_ignores_a_planted_predictable_temp_file(tmp_path: Path) -> None:
    """The temp file is now created via `tempfile.mkstemp` (random name,
    O_EXCL), so a symlink planted at the previously-predictable
    `<output>.tmp` path is never opened at all."""
    module = _validator()
    output = tmp_path / "manifest.json"
    victim = tmp_path / "victim.txt"
    victim.write_text("untouched", encoding="utf-8")
    (tmp_path / "manifest.json.tmp").symlink_to(victim)

    module._atomic_write_manifest(output, {"manifest_phase": "new"})

    assert victim.read_text(encoding="utf-8") == "untouched"
    assert json.loads(output.read_text())["manifest_phase"] == "new"


# --- hex-digest validation and the paid-stratum allowlist -------------------
#
# Both hex patterns were `^...$` matched with `re.match`, and `$` also matches
# immediately before a trailing newline -- so a digest with a trailing `\n`
# validated. And `has_real_provider_stratum` tested `!= "unavailable"`, so any
# self-declared provider name counted as a real paid stratum.


def _phase4c_artifact(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": "promoted",
        "promotion_eligible": True,
        "post_change_source_commit": SOURCE_COMMIT,
        "post_change_source_tree_hash": SOURCE_TREE_HASH,
        "phase3_completion_hash": "1" * 64,
        "phase4b_baseline_input_sha256": "2" * 64,
        "phase4b_normalized_input_sha256": "3" * 64,
        "experiment_command_digest": "4" * 64,
        "analyzer_command_digest": "5" * 64,
        "scorer_hash": "6" * 64,
        "control_fingerprint": "7" * 64,
        "fixture_version": "qcl-test-v1",
        "scorer_version": "scorer-v1",
        "generated_at_utc": "2026-08-05T00:00:00Z",
    }
    payload.update(overrides)
    return payload


def test_require_hex64_rejects_a_digest_with_a_trailing_newline() -> None:
    module = _evidence_common()

    with pytest.raises(module.EvidenceGateError):
        module.require_hex64("a" * 64 + "\n", "some_field")


def test_phase4c_hash_like_field_rejects_a_digest_with_a_trailing_newline() -> None:
    """`_HEX_RE` guards the non-SHA256 hash-like Phase 4C fields; a trailing
    newline must not pass there either."""
    module = _validator()
    payload = _phase4c_artifact(experiment_command_digest="4" * 64 + "\n")

    with pytest.raises(module.EvidenceGateError):
        module._validate_phase4c_artifact(payload)


def test_has_real_provider_stratum_rejects_an_unallowlisted_self_declared_provider() -> None:
    module = _validator()
    records = [_phase0_record(provider="fake-provider-not-unavailable", model="also-fake")]

    assert module.has_real_provider_stratum(records) is False


def test_has_real_provider_stratum_accepts_an_allowlisted_pair() -> None:
    module = _validator()
    provider, model = sorted(module.REAL_PROVIDER_ALLOWLIST)[0]
    records = [_phase0_record(provider=provider, model=model)]

    assert module.has_real_provider_stratum(records) is True
