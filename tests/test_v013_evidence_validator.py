"""Tests for ``scripts/validate_v013_evidence.py --write-manifest`` and the
shared status enum in ``scripts/evidence_common.py``.

Plan: docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md,
Phase 2 bullets 189-192 and the "New files to create" entries for
``scripts/evidence_common.py`` and the promotion manifest. This module
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

import json
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


def _validator() -> Any:
    import scripts.validate_v013_evidence

    return scripts.validate_v013_evidence


@pytest.fixture(autouse=True)
def _confine_manifest_evidence_root_to_tmp_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``write_manifest`` resolves each evidence input relative to its own
    module's ``REPO_ROOT`` and raises ``EvidenceGateError`` for an input that
    doesn't live under it (mirroring ``load_promotion_manifest``'s read-side
    confinement -- see ``_repo_relative_evidence_path``). Point that root at
    each test's own ``tmp_path`` so existing fixtures can keep writing dummy
    evidence files there while still exercising the real relative-path
    resolution, not a bypass of it.

    ``--output`` on both ``validate_v013_evidence.py`` and
    ``record_phase3_completion.py`` is now also confined to each module's own
    ``REPO_ROOT`` via ``confined_output_path`` (round 1 gauntlet security
    finding: evidence writers must apply the same output confinement
    ``eval_model_comparison.py``/``verify_eval_candidates.py`` already do),
    so both modules' roots are patched here the same way
    ``test_eval_model_comparison.py`` patches ``eval_runner.REPO_ROOT``.
    """
    monkeypatch.setattr(_validator(), "REPO_ROOT", tmp_path)
    record_module = pytest.importorskip("scripts.record_phase3_completion")
    monkeypatch.setattr(record_module, "REPO_ROOT", tmp_path)


def _evidence_common() -> Any:
    from scripts import evidence_common as _evidence_common

    return _evidence_common


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


def _write_manifest_argv(
    tmp_path: Path,
    *,
    phase0: Path,
    phase1: Path,
    phase2: Path,
    feature_policy_fingerprint: str = FEATURE_POLICY_FINGERPRINT,
) -> list[str]:
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
        feature_policy_fingerprint,
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
    the shared ``evidence_common.load_jsonl``, which raises
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


def test_write_manifest_ignores_a_real_stratum_record_mislabeled_into_the_phase0_file(
    tmp_path: Path,
) -> None:
    """Regression: `validate_artifact` returns every record in a file
    regardless of that record's own `phase` field -- only
    `check_phase_minimums` filters by phase. A record whose own `phase` is
    `phase4` (valid per PHASES, so it passes `validate_record`) but that
    lives in the phase0 *file* must not count as phase0/phase1 real-stratum
    coverage; otherwise one unrelated record could flip
    `real_stratum_present` with zero real paid-stratum coverage of the phase
    actually under validation."""
    module = _validator()
    provider, model = min(module.REAL_PROVIDER_ALLOWLIST)
    contaminated_phase0 = [
        *_phase0_fixture(),
        _phase0_record(phase="phase4", provider=provider, model=model),
    ]
    phase0 = _write_jsonl(tmp_path, "phase0.jsonl", contaminated_phase0)
    phase1 = _write_jsonl(tmp_path, "phase1.jsonl", _phase1_fixture())
    phase2 = tmp_path / "phase2.json"
    phase2.write_text(json.dumps(_valid_transport_artifact()))

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


def test_write_manifest_stamps_the_effective_configured_release_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: the manifest writer stamped ``Config()``'s package-default
    release version (the installed-package fallback) instead of the
    effective configured one -- ``WEBSEARCH_RELEASE_VERSION``/
    ``[features].release_version`` -- so a configured non-default release
    always mismatched at server load time and fell closed to
    ``source_mismatch``, regardless of the writer's own inputs."""
    module = _validator()
    phase0, phase1, phase2 = _full_valid_inputs(tmp_path)
    monkeypatch.setenv("WEBSEARCH_RELEASE_VERSION", "9.9.9-configured-release")

    exit_code = module.main(
        _write_manifest_argv(tmp_path, phase0=phase0, phase1=phase1, phase2=phase2)
    )

    assert exit_code == 0
    manifest = json.loads((tmp_path / "promotion-manifest.json").read_text())
    assert manifest["release_version"] == "9.9.9-configured-release"


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
        # Repo-relative (here, relative to the confined tmp_path), not the
        # absolute str(input_path) -- load_promotion_manifest rejects an
        # absolute manifest-declared path outright (server/config.py
        # _resolve_confined_evidence_path), so the writer must never emit one.
        assert inputs[phase_name]["path"] == input_path.name
        assert len(inputs[phase_name]["sha256"]) == 64  # sha256 hex digest


def test_evidence_common_status_enum_contains_the_documented_values() -> None:
    """Plan: 'evidence_common.py -- shared eligibility/validation module
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


def _record_phase3_completion_script() -> Any:
    return pytest.importorskip("scripts.record_phase3_completion")


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
    feature_policy_fingerprint: str = FEATURE_POLICY_FINGERPRINT,
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
        feature_policy_fingerprint,
        "--output",
        str(tmp_path / "promotion-manifest.json"),
    ]


def _real_feature_policy_fingerprint() -> str:
    """The fingerprint the *running checkout's* effective feature policy
    produces -- the same expression ``verify_manifest`` re-derives and
    ``server/config.py::load_promotion_manifest`` compares against.

    The write-path tests deliberately stamp the ``fp-cccc...`` placeholder,
    which is fine for asserting the writer copies its argument through. A
    manifest carrying it is one the runtime loader would reject outright as
    ``policy_fingerprint_mismatch``, though, so the verify-path tests must
    write a manifest with the real value or they would only ever be pinning
    the drift they expect to see.
    """
    from server.config import FeaturePolicy, feature_policy_fingerprint, load_config

    return feature_policy_fingerprint(FeaturePolicy.from_config(load_config()))


def _full_valid_inputs_with_phase3(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    phase0, phase1, phase2 = _full_valid_inputs(tmp_path)
    completion_module = _record_phase3_completion_script()
    phase3 = tmp_path / "v0.1.3-phase3-completion.json"
    exit_code = completion_module.main(_phase3_completion_argv(output=phase3))
    assert exit_code == 0
    return phase0, phase1, phase2, phase3


class TestVerifyManifestDriftCheck:
    """Round-3 restart gauntlet, Architecture finding: CI's ``release-metadata``
    job wrote a promotion manifest into an ephemeral workspace and ended there
    -- no commit, no upload-artifact, no deploy, and ``permissions: contents:
    read`` so it could not have pushed one. The consumer,
    ``server/config.py::load_promotion_manifest``, reads the repo-committed
    file, so producer and consumer were never connected and the job proved
    nothing.

    The conservative remedy is a read-only drift check against the *committed*
    manifest, which is what these tests pin. No publish path was added: how a
    manifest gets published is a release-mechanics decision for an operator.
    """

    def _written_manifest(self, tmp_path: Path) -> Path:
        module = _validator()
        phase0, phase1, phase2, phase3 = _full_valid_inputs_with_phase3(tmp_path)
        assert (
            module.main(
                _write_manifest_final_argv(
                    tmp_path,
                    phase0=phase0,
                    phase1=phase1,
                    phase2=phase2,
                    phase3=phase3,
                    feature_policy_fingerprint=_real_feature_policy_fingerprint(),
                )
            )
            == 0
        )
        return tmp_path / "promotion-manifest.json"

    def test_a_freshly_written_manifest_verifies_clean(self, tmp_path: Path) -> None:
        module = _validator()
        manifest_path = self._written_manifest(tmp_path)
        assert module.verify_manifest(manifest_path) == []
        assert module.main(["--verify-manifest", str(manifest_path)]) == 0

    def test_an_edited_evidence_artifact_is_reported_as_digest_drift(self, tmp_path: Path) -> None:
        """The drift that actually matters: someone edits an evidence file
        without regenerating the manifest that vouches for its digest."""
        module = _validator()
        manifest_path = self._written_manifest(tmp_path)
        manifest = json.loads(manifest_path.read_text())

        edited = tmp_path / manifest["inputs"]["phase0"]["path"]
        edited.write_text(edited.read_text() + "\n", encoding="utf-8")

        drift = module.verify_manifest(manifest_path)
        assert any("inputs.phase0 digest drift" in item for item in drift), drift
        assert module.main(["--verify-manifest", str(manifest_path)]) == 1

    def test_a_hand_edited_promotion_eligible_is_reported(self, tmp_path: Path) -> None:
        """The check that stops a stale/forged manifest authorizing
        data-driven tuning the current evidence does not support."""
        module = _validator()
        manifest_path = self._written_manifest(tmp_path)
        manifest = json.loads(manifest_path.read_text())
        assert manifest["promotion_eligible"] is False
        manifest["promotion_eligible"] = True
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        drift = module.verify_manifest(manifest_path)
        assert any("promotion_eligible drift" in item for item in drift), drift

    def test_a_hand_edited_top_level_digest_is_reported(self, tmp_path: Path) -> None:
        module = _validator()
        manifest_path = self._written_manifest(tmp_path)
        manifest = json.loads(manifest_path.read_text())
        manifest["phase3_completion_hash"] = "f" * 64
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        drift = module.verify_manifest(manifest_path)
        assert any("phase3_completion_hash" in item for item in drift), drift

    def test_an_unrecognized_manifest_field_fails_closed(self, tmp_path: Path) -> None:
        """A field added to ``write_manifest`` but not to ``verify_manifest``
        must not be silently unchecked -- that is how the job came to prove
        nothing in the first place."""
        module = _validator()
        manifest_path = self._written_manifest(tmp_path)
        manifest = json.loads(manifest_path.read_text())
        manifest["some_future_binding"] = "abc"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        drift = module.verify_manifest(manifest_path)
        assert any("some_future_binding" in item for item in drift), drift

    def test_release_identity_fields_are_not_compared(self, tmp_path: Path) -> None:
        """A manifest committed at one commit can never match a regeneration at
        a later one, so comparing the identity/timestamp fields would fail the
        job on every push. They are excluded deliberately, and the excluded set
        plus the verified set must together cover the whole document."""
        module = _validator()
        manifest_path = self._written_manifest(tmp_path)
        manifest = json.loads(manifest_path.read_text())

        manifest["source_commit"] = "9" * 40
        manifest["deployed_at_utc"] = "2099-01-01T00:00:00Z"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        assert module.verify_manifest(manifest_path) == []

        assert set(manifest) <= module._MANIFEST_VOLATILE_FIELDS | module._MANIFEST_VERIFIED_FIELDS

    # --- Round-4 confirm pass: the gate proved less than its roster claimed. -

    def test_truncated_inputs_cannot_bypass_the_eligibility_check(self, tmp_path: Path) -> None:
        """Round-4 confirm pass, Logic + Security findings (reproduced before
        the fix): the verdict re-derivation was guarded by ``if
        {"phase0","phase1","phase2"} <= resolved.keys()``, so a manifest that
        simply omitted an ``inputs`` entry never entered the branch. Deleting
        ``inputs.phase2`` and flipping ``promotion_eligible`` to true printed
        "OK: ... still matches the evidence it binds" and exited 0.
        """
        module = _validator()
        manifest_path = self._written_manifest(tmp_path)
        manifest = json.loads(manifest_path.read_text())
        assert manifest["promotion_eligible"] is False
        del manifest["inputs"]["phase2"]
        manifest["promotion_eligible"] = True
        manifest["reason"] = None
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        drift = module.verify_manifest(manifest_path)
        assert any("inputs.phase2 is missing" in item for item in drift), drift
        assert any("could not be re-derived" in item for item in drift), drift
        assert module.main(["--verify-manifest", str(manifest_path)]) == 1

    def test_an_absolute_declared_input_path_is_rejected(self, tmp_path: Path) -> None:
        """Round-4 confirm pass, Architecture + Security findings: the
        confinement here was a hand-rolled ``(REPO_ROOT / declared).resolve()``
        followed by ``is_relative_to``. ``Path(root) / "/abs"`` discards
        ``root`` in pathlib, so an *absolute in-repo* path passed here while
        ``server/config._resolve_confined_evidence_path`` rejects it outright
        -- CI would report clean a manifest the consumer refuses.
        """
        module = _validator()
        manifest_path = self._written_manifest(tmp_path)
        manifest = json.loads(manifest_path.read_text())
        relative = manifest["inputs"]["phase0"]["path"]
        manifest["inputs"]["phase0"]["path"] = str((tmp_path / relative).resolve())
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        drift = module.verify_manifest(manifest_path)
        assert any("inputs.phase0.path is absolute" in item for item in drift), drift

        from server.config import _resolve_confined_evidence_path

        assert _resolve_confined_evidence_path(manifest["inputs"]["phase0"]["path"]) is None

    def test_a_hand_edited_reason_is_reported(self, tmp_path: Path) -> None:
        """``reason`` was listed in ``_MANIFEST_VERIFIED_FIELDS`` and named in
        the docstring, but nothing re-derived it."""
        module = _validator()
        manifest_path = self._written_manifest(tmp_path)
        manifest = json.loads(manifest_path.read_text())
        assert manifest["reason"] == "real_stratum_missing"
        manifest["reason"] = "audibility_unverified"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        drift = module.verify_manifest(manifest_path)
        assert any("reason drift" in item for item in drift), drift

    def test_a_hand_edited_phase3_command_digest_is_reported(self, tmp_path: Path) -> None:
        """``phase3_command_digest`` was unreachable by construction: the
        cross-check loop paired it with ``phase = None`` and its body's first
        statement skipped on exactly that."""
        module = _validator()
        manifest_path = self._written_manifest(tmp_path)
        manifest = json.loads(manifest_path.read_text())
        assert manifest["phase3_command_digest"]
        manifest["phase3_command_digest"] = "f" * 64
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        drift = module.verify_manifest(manifest_path)
        assert any("phase3_command_digest does not match" in item for item in drift), drift

    def test_an_unknown_manifest_phase_is_reported(self, tmp_path: Path) -> None:
        module = _validator()
        manifest_path = self._written_manifest(tmp_path)
        manifest = json.loads(manifest_path.read_text())
        manifest["manifest_phase"] = "definitely-not-a-phase"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        drift = module.verify_manifest(manifest_path)
        assert any("manifest_phase" in item for item in drift), drift

    def test_the_identity_bindings_the_loader_compares_are_verified(self, tmp_path: Path) -> None:
        """``release_version`` and ``feature_policy_fingerprint`` were excluded
        as "derived from whichever checkout the verifier is running in", which
        is not true of either: both are deterministic from the effective config
        and both are compared by ``load_promotion_manifest`` with a fail-closed
        display-only verdict, so CI could not pre-catch two mismatches the
        runtime loader is guaranteed to reject.
        """
        module = _validator()
        for field in ("release_version", "feature_policy_fingerprint"):
            manifest_path = self._written_manifest(tmp_path)
            manifest = json.loads(manifest_path.read_text())
            manifest[field] = "not-what-this-checkout-resolves"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            drift = module.verify_manifest(manifest_path)
            assert any(f"{field} drift" in item for item in drift), (field, drift)

    def test_every_verified_field_is_actually_compared(self, tmp_path: Path) -> None:
        """The invariant behind ``_MANIFEST_VERIFIED_FIELDS``: membership is a
        claim that the field is compared, and the trailing uncovered-field
        guard trusts that claim. Three fields were listed while nothing
        compared them, so the guard asserted a completeness property the
        function did not have. Mutating each field in turn must produce drift.
        """
        module = _validator()
        manifest_path = self._written_manifest(tmp_path)
        original = json.loads(manifest_path.read_text())

        covered = sorted(set(original) & module._MANIFEST_VERIFIED_FIELDS)
        # Guard the guard: a roster this loop never exercises proves nothing.
        assert {"reason", "manifest_phase", "phase3_command_digest"} <= set(covered), covered

        for field in covered:
            mutated = json.loads(json.dumps(original))
            value = mutated[field]
            if field == "inputs":
                del mutated[field]["phase2"]
            elif isinstance(value, bool):
                mutated[field] = not value
            else:
                mutated[field] = "e" * 64 if value == "f" * 64 else "f" * 64
            manifest_path.write_text(json.dumps(mutated), encoding="utf-8")
            assert module.verify_manifest(manifest_path) != [], (
                f"{field} is listed in _MANIFEST_VERIFIED_FIELDS but mutating it is not drift"
            )

    def test_a_digest_drift_message_does_not_echo_a_full_digest(self, tmp_path: Path) -> None:
        """``inputs[*].path`` is manifest-declared, so echoing the full SHA-256
        of whatever it names would make the CI log a general hashing oracle
        over the repo tree (round-4 confirm pass, Security finding). The
        prefix is enough to tell one artifact revision from another.
        """
        module = _validator()
        manifest_path = self._written_manifest(tmp_path)
        manifest = json.loads(manifest_path.read_text())
        edited = tmp_path / manifest["inputs"]["phase0"]["path"]
        edited.write_text(edited.read_text() + "\n", encoding="utf-8")
        actual = module.sha256_file(edited)

        drift = module.verify_manifest(manifest_path)
        digest_drift = next(item for item in drift if "inputs.phase0 digest drift" in item)
        assert actual not in digest_drift
        assert actual[:12] in digest_drift

    def test_a_provisional_manifest_verifies_clean_and_pins_its_own_reason(
        self, tmp_path: Path
    ) -> None:
        """The required-input set is derived from ``manifest_phase``, so the
        provisional path (phase0-2, no phase3) must still verify clean -- and
        its ``reason`` vocabulary differs from the final path's."""
        module = _validator()
        phase0, phase1, phase2 = _full_valid_inputs(tmp_path)
        assert (
            module.main(
                _write_manifest_argv(
                    tmp_path,
                    phase0=phase0,
                    phase1=phase1,
                    phase2=phase2,
                    feature_policy_fingerprint=_real_feature_policy_fingerprint(),
                )
            )
            == 0
        )
        manifest_path = tmp_path / "promotion-manifest.json"
        assert module.verify_manifest(manifest_path) == []

        manifest = json.loads(manifest_path.read_text())
        assert manifest["manifest_phase"] == "provisional"
        manifest["reason"] = "provisional_manifest"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        assert any("reason drift" in item for item in module.verify_manifest(manifest_path))

    def test_verify_mode_writes_nothing(self, tmp_path: Path) -> None:
        """It is a *check*, not a producer -- the whole point of the fix."""
        module = _validator()
        manifest_path = self._written_manifest(tmp_path)
        before = sorted(p.name for p in tmp_path.iterdir())
        digest_before = manifest_path.read_bytes()

        assert module.main(["--verify-manifest", str(manifest_path)]) == 0

        assert sorted(p.name for p in tmp_path.iterdir()) == before
        assert manifest_path.read_bytes() == digest_before

    def test_the_repo_committed_manifest_verifies_clean(self) -> None:
        """The live check the CI job runs. This is the assertion that would
        actually catch a real drift landing on main."""
        module = _validator()
        committed = REPO_ROOT / "docs" / "benchmarks" / "v0.1.3-promotion-manifest.json"
        if not committed.exists():
            pytest.skip("no committed promotion manifest in this checkout")
        # `_confine_manifest_evidence_root_to_tmp_path` repoints the module's
        # REPO_ROOT at tmp_path; this check needs the real tree.
        original_root = module.REPO_ROOT
        module.REPO_ROOT = REPO_ROOT
        try:
            assert module.verify_manifest(committed) == []
        finally:
            module.REPO_ROOT = original_root


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
    # Repo-relative (here, relative to the confined tmp_path), not the
    # absolute str(phase3) -- see the identical note in
    # test_write_manifest_embeds_artifact_hashes_for_all_three_inputs.
    assert manifest["inputs"]["phase3"]["path"] == phase3.name
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


def test_write_manifest_final_without_phase3_input_raises_not_a_reason(tmp_path: Path) -> None:
    """Round-3 restart gauntlet, Logic finding: the ``final`` branch could
    select a ``"phase3_incomplete"`` reason that was unreachable.

    Both entry points already refuse ``--manifest-phase final`` without a
    phase3 input -- ``main()`` rejects the argv combination, and
    ``write_manifest`` itself raises before reaching the reason selection --
    so no manifest could ever carry that string, and a downstream consumer
    keying on it would wait forever. This pins the behaviour that makes the
    branch dead: the missing phase3 input is a *raise*, never a recorded
    reason, so removing the branch cannot have changed an observable outcome.
    """
    module = _validator()
    phase0, phase1, phase2 = _full_valid_inputs(tmp_path)

    with pytest.raises(module.EvidenceGateError, match="requires --phase3-input"):
        module.write_manifest(
            manifest_phase="final",
            phase0_input=phase0,
            phase1_input=phase1,
            phase2_input=phase2,
            phase3_input=None,
            phase4c_input=None,
            source_commit=SOURCE_COMMIT,
            source_tree_hash=SOURCE_TREE_HASH,
            deployed_at_utc="2026-07-28T00:00:00+00:00",
            feature_policy_fingerprint=FEATURE_POLICY_FINGERPRINT,
            output=tmp_path / "promotion-manifest.json",
        )

    assert not (tmp_path / "promotion-manifest.json").exists()


def test_no_written_manifest_can_carry_the_removed_phase3_incomplete_reason() -> None:
    """The reason vocabulary the writer can actually emit no longer contains
    ``phase3_incomplete`` -- guards against it being reintroduced as a dead
    string by a future edit.

    Scans code lines only: the explanatory comment above the removal names the
    string deliberately and must not trip this.
    """
    source = (REPO_ROOT / "scripts" / "validate_v013_evidence.py").read_text(encoding="utf-8")
    code_lines = [
        line for line in source.splitlines() if not line.lstrip().startswith(("#", '"""', "*"))
    ]
    assert not [line for line in code_lines if "phase3_incomplete" in line]


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


def test_atomic_write_does_not_read_the_destination_through_a_symlink(tmp_path: Path) -> None:
    """Regression (#29): the write side was O_NOFOLLOW-hardened but the
    `.previous` copy first *read* the destination via `Path.exists()` /
    `Path.read_bytes()`, both of which follow symlinks. A symlink planted at
    the manifest path -- a predictable, repo-relative location CI also writes
    -- therefore copied the link target's contents into a 0o644
    `<output>.previous` before the rename replaced the link, leaking arbitrary
    file contents. ELOOP is a hard failure, not a "treat as absent"."""
    module = _validator()
    output = tmp_path / "manifest.json"
    secret = tmp_path / "secret.txt"
    secret.write_text("SUPER-SECRET", encoding="utf-8")
    output.symlink_to(secret)

    with pytest.raises(module.EvidenceGateError):
        module._atomic_write_manifest(output, {"manifest_phase": "new"})

    assert not (tmp_path / "manifest.json.previous").exists()
    assert secret.read_text(encoding="utf-8") == "SUPER-SECRET"


def test_atomic_write_refuses_a_fifo_planted_at_the_manifest_path(tmp_path: Path) -> None:
    """Regression (Round 9, #7): round 8's O_NOFOLLOW hardening stops symlinks
    and nothing else. An attacker who can plant a symlink at this predictable,
    repo-relative path can equally ``mkfifo`` it -- O_NOFOLLOW does not cover
    FIFOs, so ``_read_bytes_no_follow``'s open blocked forever, hanging the
    evidence gate. The fix is O_NONBLOCK plus an ``fstat`` on the fd actually
    held (not a pre-open ``stat``, which would be a TOCTOU).

    Run on a daemon thread with a join timeout because the pre-fix failure
    mode is a hang, not an assertion failure: a blocking ``os.open`` on a
    FIFO cannot be interrupted from the test thread.
    """
    import os as _os
    import threading

    module = _validator()
    output = tmp_path / "manifest.json"
    _os.mkfifo(output)
    verdict: list[object] = []

    def _attempt() -> None:
        try:
            module._atomic_write_manifest(output, {"manifest_phase": "new"})
        except BaseException as exc:  # noqa: BLE001 - the type is the assertion
            verdict.append(exc)
        else:
            verdict.append(None)

    worker = threading.Thread(target=_attempt, daemon=True)
    worker.start()
    worker.join(timeout=20)
    assert not worker.is_alive(), "_atomic_write_manifest blocked on a FIFO at the manifest path"
    assert isinstance(verdict[0], module.EvidenceGateError), verdict[0]


def test_read_bytes_no_follow_caps_an_oversized_manifest(tmp_path: Path) -> None:
    """Companion to the FIFO case (#7): the `.previous` read loop had no size
    cap, so a large file planted at the predictable manifest path was read
    unbounded into memory.

    Round-3 restart gauntlet, Architecture finding: the private
    ``_read_bytes_no_follow``/``_MAX_MANIFEST_BYTES`` pair this used to assert
    on were forked copies of ``evidence_common``'s hardened reader and its
    ``_MAX_EVIDENCE_INPUT_BYTES``. The cap is now the shared module's, so the
    property under test is unchanged and there is one implementation of it.
    """
    from scripts.evidence_common import _MAX_EVIDENCE_INPUT_BYTES, read_bytes_if_present

    output = tmp_path / "manifest.json"
    assert _MAX_EVIDENCE_INPUT_BYTES == 8 * 1024 * 1024
    output.write_bytes(b"x" * (_MAX_EVIDENCE_INPUT_BYTES + 1))

    with pytest.raises(_validator().EvidenceGateError):
        read_bytes_if_present(output)


def test_read_bytes_if_present_returns_none_only_for_an_absent_path(tmp_path: Path) -> None:
    """The one behaviour the private copy had that the plain shared reader
    does not: a first run has no manifest to back up, so absent must be
    ``None`` rather than a raised gate error.

    Every *other* failure mode must still raise -- treating a symlink or FIFO
    at the manifest path as "nothing there to preserve" would let the writer
    clobber it, which is exactly what the `.previous` copy exists to prevent.
    """
    import os

    from scripts.evidence_common import EvidenceGateError, read_bytes_if_present

    assert read_bytes_if_present(tmp_path / "absent.json") is None

    real = tmp_path / "real.json"
    real.write_bytes(b"{}")
    assert read_bytes_if_present(real) == b"{}"

    link = tmp_path / "link.json"
    link.symlink_to(real)
    with pytest.raises(EvidenceGateError):
        read_bytes_if_present(link)

    fifo = tmp_path / "fifo.json"
    os.mkfifo(fifo)
    with pytest.raises(EvidenceGateError):
        read_bytes_if_present(fifo)


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
        "fixture_sha256": "8" * 64,
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
    provider, model = min(module.REAL_PROVIDER_ALLOWLIST)
    records = [_phase0_record(provider=provider, model=model)]

    assert module.has_real_provider_stratum(records) is True


def test_relative_output_is_written_under_the_confined_root_not_the_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round-2 confirm pass: all five evidence writers called
    ``confined_output_path`` for its side effect only and then wrote to the
    raw argparse ``Path``.

    ``confined_output_path`` resolves a *relative* candidate against
    ``allowed_root``, but ``os.open()`` resolves the same relative path
    against the process cwd. Run from any cwd outside the repo,
    ``--output report.json`` therefore validated as ``REPO_ROOT/report.json``
    and passed the check, while the write landed in the unrelated cwd --
    the check was adopted, the confinement was not.
    """
    module = pytest.importorskip("scripts.record_phase3_completion")
    elsewhere = tmp_path.parent / f"{tmp_path.name}-cwd"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    exit_code = module.main(
        [
            "--source-commit",
            "a" * 40,
            "--source-tree-hash",
            "b" * 40,
            "--command-digest",
            "c" * 64,
            "--output",
            "reports/phase3.json",
        ]
    )

    assert exit_code == 0
    assert (tmp_path / "reports" / "phase3.json").exists(), (
        "a relative --output must be written under the confined root"
    )
    assert not (elsewhere / "reports").exists(), (
        "nothing may be written relative to the process cwd"
    )
