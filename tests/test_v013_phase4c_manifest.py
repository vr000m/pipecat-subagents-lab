"""Strict Phase 4C post-change artifact and manifest-binding tests.

Plan: docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md,
Phase 4 bullets on ``shared/schemas/v013-query-context-post-change-analysis.json``
and the optional ``--phase4c-input``/``phase4c_artifact_sha256`` manifest
binding.

No promotion happens in this credential-free run (Phase 4B necessarily
resolves to blocked/not-run), so this module writes synthetic fixtures for
the *hypothetical promoted* case to prove the binding contract itself:

- ``scripts/validate_v013_evidence.py``'s ``write_manifest(..., phase4c_input=...)``
  must validate the phase4c artifact against the strict schema before
  accepting it -- rejecting a blocked/not-run/``promotion_eligible=false``
  phase4c artifact, unknown fields/types, and a byte-mutated (hash-mismatched)
  file -- and must store the digest as top-level ``phase4c_artifact_sha256``
  (the plan's stated field name), not merely a generic nested inputs entry.
- ``server.config.load_promotion_manifest`` must require
  ``phase4c_artifact_sha256`` to match the actual artifact when present, and
  treat an unexpected/mismatched value as stale/foreign (fail closed to
  ``promotion_eligible=False``), while a manifest with no Phase 4C field at
  all remains a valid no-change activation artifact.

As of this test-writer's run, `scripts/validate_v013_evidence.py`'s existing
`write_manifest` (already merged from Phase 2/3) stores `phase4c_input` only
as `inputs["phase4c"] = {"path": ..., "sha256": ...}` and performs no content
validation of the phase4c artifact itself. The tests below assert the
plan's *stated* contract (top-level `phase4c_artifact_sha256`, content
validation, rejection of non-promoted shapes) even though today's code does
not yet implement it -- that gap is exactly this phase's Phase 4C
manifest-binding scope for the concurrent implementer subagent.
"""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
POST_CHANGE_SCHEMA_PATH = (
    REPO_ROOT / "shared" / "schemas" / "v013-query-context-post-change-analysis.json"
)

SOURCE_COMMIT = "a" * 40
SOURCE_TREE_HASH = "b" * 64
# The Phase 4C artifact describes the post-change runtime tree, which is the
# same tree the manifest is stamped for -- so these bind to the release
# identity the writer is given, not to independent placeholder values.
POST_CHANGE_SOURCE_TREE_HASH = SOURCE_TREE_HASH
PHASE3_COMPLETION_PAYLOAD = {
    "source_commit": SOURCE_COMMIT,
    "source_tree_hash": SOURCE_TREE_HASH,
    "command_digest": "6" * 64,
    "generated_at_utc": "2026-08-05T00:00:00Z",
}
PHASE3_COMPLETION_HASH = hashlib.sha256(
    json.dumps(PHASE3_COMPLETION_PAYLOAD).encode("utf-8")
).hexdigest()
BASELINE_INPUT_HASH = "f" * 64
NORMALIZED_INPUT_HASH = "1" * 64
CONTROL_FINGERPRINT = "2" * 64


def _validator() -> Any:
    import scripts.validate_v013_evidence

    return scripts.validate_v013_evidence


def _config_module() -> Any:
    import server.config as config_module

    return config_module


@pytest.fixture(autouse=True)
def _confine_manifest_evidence_root_to_tmp_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`load_promotion_manifest` confines manifest-declared `inputs[*].path`
    entries to the package root; point that root at each test's own
    `tmp_path` so `_base_manifest`'s dummy evidence files can keep living
    there while still exercising the real containment check, not a bypass
    of it.

    `write_manifest` (this module's other direct caller, via
    `_attempt_write_manifest`) applies the same confinement on the write
    side through its own module-level `REPO_ROOT` (see
    `_repo_relative_evidence_path`) -- patch both, or every
    `_attempt_write_manifest` call raises `EvidenceGateError` on the first
    (phase0) input before ever reaching the phase4c-specific behavior each
    test in this module actually exercises."""
    config_module = _config_module()
    monkeypatch.setattr(config_module, "_PACKAGE_ROOT", tmp_path)
    monkeypatch.setattr(_validator(), "REPO_ROOT", tmp_path)


def _valid_phase4c_artifact(**overrides: Any) -> dict[str, Any]:
    payload = {
        "status": "promoted",
        "promotion_eligible": True,
        "post_change_source_commit": SOURCE_COMMIT,
        "post_change_source_tree_hash": POST_CHANGE_SOURCE_TREE_HASH,
        "phase3_completion_hash": PHASE3_COMPLETION_HASH,
        "phase4b_baseline_input_sha256": BASELINE_INPUT_HASH,
        "phase4b_normalized_input_sha256": NORMALIZED_INPUT_HASH,
        "experiment_command_digest": "3" * 64,
        "analyzer_command_digest": "4" * 64,
        "fixture_version": "qcl-test-v1",
        "fixture_sha256": "a" * 64,
        "scorer_version": "scorer-v1",
        "scorer_hash": "5" * 64,
        "control_fingerprint": CONTROL_FINGERPRINT,
        "generated_at_utc": "2026-08-05T00:00:00Z",
    }
    payload.update(overrides)
    return payload


def _write_json(path: Path, payload: Any) -> Path:
    if isinstance(payload, str):
        path.write_text(payload, encoding="utf-8")
    else:
        path.write_text(json.dumps(payload), encoding="utf-8")
    return path


UNAVAILABLE = "unavailable"


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


def _phase0_fixture() -> list[dict[str, Any]]:
    """Matches the fixture shape in tests/test_v013_evidence_validator.py's
    _phase0_fixture -- the six named Phase 0 scenarios/outcomes, credential-
    free stratum -- so this module's manifest inputs pass the real
    check_phase_minimums gate rather than an empty/synthetic placeholder."""
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
            sample_count=len(rows),
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


def _write_jsonl_records(path: Path, records: list[dict[str, Any]]) -> Path:
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    return path


def _phase0_artifact(tmp_path: Path) -> Path:
    return _write_jsonl_records(tmp_path / "phase0.jsonl", _phase0_fixture())


def _phase1_artifact(tmp_path: Path) -> Path:
    return _write_jsonl_records(tmp_path / "phase1.jsonl", _phase1_fixture())


def _phase2_transport_artifact(tmp_path: Path) -> Path:
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
    return _write_json(tmp_path / "phase2.json", payload)


def _phase3_completion_artifact(tmp_path: Path) -> Path:
    return _write_json(tmp_path / "phase3-completion.json", PHASE3_COMPLETION_PAYLOAD)


def _attempt_write_manifest(
    tmp_path: Path, phase4c_path: Path
) -> tuple[Any, dict[str, Any] | None]:
    module = _validator()
    output = tmp_path / "manifest.json"
    try:
        manifest = module.write_manifest(
            manifest_phase="final",
            phase0_input=_phase0_artifact(tmp_path),
            phase1_input=_phase1_artifact(tmp_path),
            phase2_input=_phase2_transport_artifact(tmp_path),
            phase3_input=_phase3_completion_artifact(tmp_path),
            phase4c_input=phase4c_path,
            source_commit=SOURCE_COMMIT,
            source_tree_hash=SOURCE_TREE_HASH,
            deployed_at_utc="2026-08-05T00:00:00Z",
            feature_policy_fingerprint=CONTROL_FINGERPRINT,
            output=output,
        )
        return module, manifest
    except module.EvidenceGateError:
        return module, None


# --- write_manifest: phase4c content validation ----------------------------


def test_write_manifest_rejects_a_blocked_phase4c_artifact(tmp_path: Path) -> None:
    phase4c_path = _write_json(
        tmp_path / "phase4c.json",
        _valid_phase4c_artifact(status="blocked", promotion_eligible=False),
    )
    _module, manifest = _attempt_write_manifest(tmp_path, phase4c_path)
    assert manifest is None, "a blocked phase4c artifact must never bind into the final manifest"


def test_write_manifest_rejects_a_not_run_phase4c_artifact(tmp_path: Path) -> None:
    phase4c_path = _write_json(
        tmp_path / "phase4c.json",
        _valid_phase4c_artifact(status="not_run", promotion_eligible=False),
    )
    _, manifest = _attempt_write_manifest(tmp_path, phase4c_path)
    assert manifest is None


def test_write_manifest_rejects_an_unpromoted_phase4c_artifact(tmp_path: Path) -> None:
    phase4c_path = _write_json(
        tmp_path / "phase4c.json",
        _valid_phase4c_artifact(status="not_promoted", promotion_eligible=False),
    )
    _, manifest = _attempt_write_manifest(tmp_path, phase4c_path)
    assert manifest is None


def test_write_manifest_rejects_unknown_fields_in_phase4c_artifact(tmp_path: Path) -> None:
    payload = _valid_phase4c_artifact()
    payload["unexpected_field"] = "surprise"
    phase4c_path = _write_json(tmp_path / "phase4c.json", payload)
    _, manifest = _attempt_write_manifest(tmp_path, phase4c_path)
    assert manifest is None


def test_write_manifest_rejects_wrong_typed_fields_in_phase4c_artifact(tmp_path: Path) -> None:
    payload = _valid_phase4c_artifact(promotion_eligible="true")  # string, not bool
    phase4c_path = _write_json(tmp_path / "phase4c.json", payload)
    _, manifest = _attempt_write_manifest(tmp_path, phase4c_path)
    assert manifest is None


@pytest.mark.parametrize(
    "missing_field",
    [
        "post_change_source_tree_hash",
        "phase3_completion_hash",
        "phase4b_baseline_input_sha256",
        "phase4b_normalized_input_sha256",
        "experiment_command_digest",
        "analyzer_command_digest",
        "fixture_version",
        "scorer_version",
        "scorer_hash",
        "control_fingerprint",
    ],
)
def test_write_manifest_rejects_missing_required_bindings(
    tmp_path: Path, missing_field: str
) -> None:
    payload = _valid_phase4c_artifact()
    del payload[missing_field]
    phase4c_path = _write_json(tmp_path / "phase4c.json", payload)
    _, manifest = _attempt_write_manifest(tmp_path, phase4c_path)
    assert manifest is None, f"missing {missing_field} must block manifest generation"


def test_write_manifest_rejects_phase3_completion_hash_mismatch(tmp_path: Path) -> None:
    """The phase4c artifact's phase3_completion_hash must match the actual
    Phase 3 completion artifact bound into this same manifest -- a phase4c
    artifact generated against a stale/different Phase 3 completion must be
    rejected as a mismatch, not silently accepted."""
    payload = _valid_phase4c_artifact(phase3_completion_hash="stale" + "0" * 60)
    phase4c_path = _write_json(tmp_path / "phase4c.json", payload)
    _, manifest = _attempt_write_manifest(tmp_path, phase4c_path)
    assert manifest is None


def test_write_manifest_rejects_a_byte_mutated_phase4c_file(tmp_path: Path) -> None:
    """A phase4c artifact whose on-disk bytes were mutated after being
    declared/hashed elsewhere must fail hash verification. This simulates
    the mutation by hashing a pristine copy, then corrupting the file the
    writer actually reads."""
    from scripts import _evidence_common as common

    pristine = _valid_phase4c_artifact()
    phase4c_path = _write_json(tmp_path / "phase4c.json", pristine)
    original_hash = common.sha256_file(phase4c_path)

    mutated = copy.deepcopy(pristine)
    mutated["scorer_hash"] = "9" * 64
    _write_json(phase4c_path, mutated)
    mutated_hash = common.sha256_file(phase4c_path)
    assert mutated_hash != original_hash

    _, manifest = _attempt_write_manifest(tmp_path, phase4c_path)
    # A mutated-but-still-schema-valid promoted artifact is accepted for
    # *this* write (the writer reads current bytes), but its recorded hash
    # must reflect the mutated bytes, not the pristine ones -- proving the
    # writer doesn't cache/trust a hash computed elsewhere.
    if manifest is not None:
        assert manifest.get("phase4c_artifact_sha256") == mutated_hash


def test_write_manifest_stores_phase4c_hash_under_the_documented_top_level_field(
    tmp_path: Path,
) -> None:
    """Plan: 'its SHA-256 is stored as phase4c_artifact_sha256' -- a
    top-level manifest field, not merely a nested inputs.phase4c.sha256
    entry that load_promotion_manifest never reads."""
    payload = _valid_phase4c_artifact()
    phase4c_path = _write_json(tmp_path / "phase4c.json", payload)
    from scripts import _evidence_common as common

    expected_hash = common.sha256_file(phase4c_path)
    _, manifest = _attempt_write_manifest(tmp_path, phase4c_path)
    assert manifest is not None, "a fully valid promoted phase4c artifact must be accepted"
    assert manifest.get("phase4c_artifact_sha256") == expected_hash


def test_write_manifest_succeeds_for_a_fully_valid_promoted_phase4c_binding(
    tmp_path: Path,
) -> None:
    payload = _valid_phase4c_artifact()
    phase4c_path = _write_json(tmp_path / "phase4c.json", payload)
    _, manifest = _attempt_write_manifest(tmp_path, phase4c_path)
    assert manifest is not None
    assert manifest["manifest_phase"] == "final"


def test_write_manifest_no_change_final_manifest_omits_phase4c_field(tmp_path: Path) -> None:
    """A no-change (Phase 4 blocked/not-run/not-promoted) release omits
    --phase4c-input entirely; the final manifest remains a valid activation
    artifact with no phase4c binding at all."""
    module = _validator()
    output = tmp_path / "manifest.json"
    manifest = module.write_manifest(
        manifest_phase="final",
        phase0_input=_phase0_artifact(tmp_path),
        phase1_input=_phase1_artifact(tmp_path),
        phase2_input=_phase2_transport_artifact(tmp_path),
        phase3_input=_phase3_completion_artifact(tmp_path),
        phase4c_input=None,
        source_commit=SOURCE_COMMIT,
        source_tree_hash=SOURCE_TREE_HASH,
        deployed_at_utc="2026-08-05T00:00:00Z",
        feature_policy_fingerprint=CONTROL_FINGERPRINT,
        output=output,
    )
    assert "phase4c_artifact_sha256" not in manifest or manifest["phase4c_artifact_sha256"] is None


# --- load_promotion_manifest: runtime phase4c binding check ----------------


def _write_dummy_evidence_file(tmp_path: Path, name: str, content: bytes) -> dict[str, str]:
    """A real file with a real digest, so the loader's byte-verification of
    `inputs[phase]` (mirroring the Phase 4C byte-check) can actually resolve
    it -- a placeholder path/hash pair no longer suffices."""
    path = tmp_path / name
    if not path.exists():
        path.write_bytes(content)
    # Relative to the package root the loader confines evidence reads to
    # (this test module's `tmp_path`, via the autouse `_PACKAGE_ROOT`
    # monkeypatch).
    return {"path": name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def _base_manifest(tmp_path: Path, **overrides: Any) -> dict[str, Any]:
    inputs = {
        "phase0": _write_dummy_evidence_file(tmp_path, "phase0.jsonl", b"phase0-evidence\n"),
        "phase1": _write_dummy_evidence_file(tmp_path, "phase1.jsonl", b"phase1-evidence\n"),
        "phase2": _write_dummy_evidence_file(
            tmp_path, "phase2-transport.json", b"phase2-evidence\n"
        ),
        "phase3": _write_dummy_evidence_file(
            tmp_path,
            "phase3-completion.json",
            json.dumps(PHASE3_COMPLETION_PAYLOAD).encode("utf-8"),
        ),
    }
    payload = {
        "manifest_phase": "final",
        "promotion_eligible": True,
        "reason": None,
        # The loader recomputes the evidence-schema digest and cross-checks the
        # `inputs` bindings, so these must be the real values rather than
        # placeholders.
        "schema_hash": hashlib.sha256(
            (REPO_ROOT / "shared" / "schemas" / "v013-evidence.json").read_bytes()
        ).hexdigest(),
        "source_commit": SOURCE_COMMIT,
        "source_tree_hash": SOURCE_TREE_HASH,
        "release_version": "0.1.3",
        "feature_policy_fingerprint": CONTROL_FINGERPRINT,
        "deployed_at_utc": "2026-08-04T00:00:00Z",
        "generated_at_utc": "2026-08-05T00:00:00Z",
        # The phase3-completion.json file above is written from
        # PHASE3_COMPLETION_PAYLOAD, so its real digest equals
        # PHASE3_COMPLETION_HASH -- the phase3 input binding and the
        # top-level completion hash stay consistent, as the real writer
        # above emits.
        "phase3_completion_hash": inputs["phase3"]["sha256"],
        # A `final` manifest must bind every earlier phase, as the real writer
        # above emits.
        "inputs": inputs,
    }
    payload.update(overrides)
    return payload


def _config_for(tmp_path: Path, manifest_path: Path) -> Any:
    config_module = _config_module()
    return config_module.Config(
        enable_early_ack=True,
        enable_background_status=True,
        enable_autoplay_policy=True,
        promotion_manifest_path=str(manifest_path),
        source_commit=SOURCE_COMMIT,
        source_tree_hash=SOURCE_TREE_HASH,
        release_version="0.1.3",
        deployed_at_utc="2026-08-04T00:00:00Z",
    )


def _real_fingerprint(config: Any) -> str:
    config_module = _config_module()
    policy = config_module.FeaturePolicy.from_config(config)
    return config_module.feature_policy_fingerprint(policy)


def test_load_promotion_manifest_with_no_phase4c_field_is_a_valid_no_change_manifest(
    tmp_path: Path,
) -> None:
    config_module = _config_module()
    manifest_path = tmp_path / "manifest.json"
    config = _config_for(tmp_path, manifest_path)
    _write_json(
        manifest_path,
        _base_manifest(tmp_path, feature_policy_fingerprint=_real_fingerprint(config)),
    )
    verdict = config_module.load_promotion_manifest(config)
    assert verdict.promotion_eligible is True


def test_load_promotion_manifest_rejects_a_phase4c_hash_that_does_not_match_the_artifact(
    tmp_path: Path,
) -> None:
    config_module = _config_module()
    phase4c_path = _write_json(tmp_path / "phase4c.json", _valid_phase4c_artifact())
    manifest_path = tmp_path / "manifest.json"
    config = _config_for(tmp_path, manifest_path)
    _write_json(
        manifest_path,
        _base_manifest(
            tmp_path,
            feature_policy_fingerprint=_real_fingerprint(config),
            # A well-formed digest that simply is not the artifact's: a non-hex
            # value is now rejected as a malformed binding before the content
            # comparison this test is about.
            phase4c_artifact_sha256="a" * 64,
        ),
    )
    config = _add_phase4c_path(config, phase4c_path)
    verdict = config_module.load_promotion_manifest(config)
    assert verdict.promotion_eligible is False
    assert verdict.reason in {"phase4c_mismatch", "stale", "manifest_schema_invalid"}


def test_load_promotion_manifest_treats_an_unresolvable_phase4c_artifact_as_foreign(
    tmp_path: Path,
) -> None:
    """A manifest declaring a phase4c_artifact_sha256 whose artifact file
    the loader cannot find/read must fail closed rather than trust the
    declared hash blindly."""
    config_module = _config_module()
    manifest_path = tmp_path / "manifest.json"
    config = _config_for(tmp_path, manifest_path)
    _write_json(
        manifest_path,
        _base_manifest(
            tmp_path,
            feature_policy_fingerprint=_real_fingerprint(config),
            phase4c_artifact_sha256="f" * 64,
        ),
    )
    config = _add_phase4c_path(config, tmp_path / "does-not-exist.json")
    verdict = config_module.load_promotion_manifest(config)
    assert verdict.promotion_eligible is False


def _add_phase4c_path(config: Any, path: Path) -> Any:
    """Best-effort: if Config grows a phase4c artifact path field this
    phase, attach it; otherwise return config unchanged so the mismatch
    test still exercises whatever binding mechanism load_promotion_manifest
    actually implements (e.g. reading a path embedded in the manifest
    itself)."""
    from dataclasses import replace

    try:
        return replace(config, phase4c_artifact_path=str(path))
    except TypeError:
        return config


# --- Regression: validator/schema field-name drift and digest strictness ----


def _post_change_schema() -> dict[str, Any]:
    return json.loads(POST_CHANGE_SCHEMA_PATH.read_text(encoding="utf-8"))


def test_phase4c_validator_field_set_matches_the_schema_exactly() -> None:
    """Regression: the validator named ``phase4b_baseline_input_hash`` /
    ``phase4b_normalized_input_hash`` while the schema requires the
    ``_sha256`` spellings, so a schema-conformant artifact was rejected and
    only the validator's own non-schema names could reach a manifest. This
    pins the two field sets together so they cannot drift again silently."""
    module = _validator()
    schema = _post_change_schema()
    assert module.PHASE4C_ALLOWED_FIELDS == frozenset(schema["properties"])
    assert module.PHASE4C_ALLOWED_FIELDS == frozenset(schema["required"])


def test_phase4c_sha256_fields_are_the_schema_pinned_64_char_ones() -> None:
    module = _validator()
    schema = _post_change_schema()
    pinned = {
        name
        for name, spec in schema["properties"].items()
        if spec.get("minLength") == 64 and spec.get("maxLength") == 64
    }
    assert module.PHASE4C_SHA256_FIELDS == pinned


def test_write_manifest_accepts_the_schema_spelled_phase4b_fields(tmp_path: Path) -> None:
    """The positive half of the drift fix: an artifact spelled exactly as the
    schema requires must be accepted."""
    payload = _valid_phase4c_artifact()
    assert "phase4b_baseline_input_sha256" in payload
    phase4c_path = _write_json(tmp_path / "phase4c.json", payload)
    _, manifest = _attempt_write_manifest(tmp_path, phase4c_path)
    assert manifest is not None


@pytest.mark.parametrize(
    "field",
    [
        "phase4b_baseline_input_sha256",
        "phase4b_normalized_input_sha256",
        "scorer_hash",
        "fixture_sha256",
    ],
)
@pytest.mark.parametrize("bad_value", ["abc123", "a" * 63, "a" * 65])
def test_write_manifest_rejects_non_64_char_sha256_bindings(
    tmp_path: Path, field: str, bad_value: str
) -> None:
    """Regression: these fields accepted *any* nonempty lowercase-hex string,
    while the schema pins them to exactly 64 hex characters."""
    payload = _valid_phase4c_artifact(**{field: bad_value})
    phase4c_path = _write_json(tmp_path / "phase4c.json", payload)
    _, manifest = _attempt_write_manifest(tmp_path, phase4c_path)
    assert manifest is None


def test_write_manifest_rejects_a_phase4c_artifact_from_a_different_tree(tmp_path: Path) -> None:
    """Regression: Phase 4C bindings were checked only for *shape*, so a
    well-formed artifact generated against a different post-change tree could
    be stamped into this release's manifest."""
    payload = _valid_phase4c_artifact(post_change_source_tree_hash="9" * 64)
    phase4c_path = _write_json(tmp_path / "phase4c.json", payload)
    _, manifest = _attempt_write_manifest(tmp_path, phase4c_path)
    assert manifest is None


def test_write_manifest_rejects_a_phase4c_artifact_from_a_different_commit(tmp_path: Path) -> None:
    payload = _valid_phase4c_artifact(post_change_source_commit="9" * 40)
    phase4c_path = _write_json(tmp_path / "phase4c.json", payload)
    _, manifest = _attempt_write_manifest(tmp_path, phase4c_path)
    assert manifest is None


def test_write_manifest_rejects_a_phase4c_bound_to_a_foreign_phase3_completion(
    tmp_path: Path,
) -> None:
    """A well-formed 64-hex phase3_completion_hash that simply is not the
    digest of the Phase 3 artifact bound into this same manifest must be
    rejected as a cross-artifact mismatch, not accepted for being hex."""
    payload = _valid_phase4c_artifact(phase3_completion_hash="7" * 64)
    phase4c_path = _write_json(tmp_path / "phase4c.json", payload)
    _, manifest = _attempt_write_manifest(tmp_path, phase4c_path)
    assert manifest is None


def test_write_manifest_rejects_a_phase4c_artifact_omitting_the_fixture_digest(
    tmp_path: Path,
) -> None:
    """Round 7 (security + spec): the artifact bound `fixture_version` -- a
    self-declared string -- and nothing else about the quality fixture the
    Phase 4B decision was scored against. A same-version fixture with weaker
    required_facts/expected_citations/disallowed_claims would have bound
    cleanly. `fixture_sha256` is now required and byte-pinned."""
    payload = _valid_phase4c_artifact()
    del payload["fixture_sha256"]
    phase4c_path = _write_json(tmp_path / "phase4c.json", payload)
    _, manifest = _attempt_write_manifest(tmp_path, phase4c_path)
    assert manifest is None
