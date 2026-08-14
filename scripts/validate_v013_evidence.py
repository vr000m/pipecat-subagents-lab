#!/usr/bin/env python3
"""Validate a v0.1.3 evidence JSONL artifact against its schema and phase minimums.

Each line of ``--input`` must be one JSON object matching
``shared/schemas/v013-evidence.json``. This module hand-checks the schema's
required fields, types, and enum vocabularies directly (no ``jsonschema``
dependency) and then enforces phase-specific coverage minimums: one record
per named scenario, one record per outcome exercised by that phase's fixture
matrix, and one record per provider/model stratum (the credential-free
stratum is ``unavailable``/``unavailable``).

``--write-manifest`` mode (added in Phase 2) revalidates the Phase 0/1/2
inputs and, only if every gate passes, writes the durable promotion manifest
``server.config.load_promotion_manifest`` reads at runtime. A provisional
manifest (the only kind this script can produce as of Phase 2) is always
``promotion_eligible=false``: Phase 3 must stamp its completion hash and
rewrite the manifest as ``final`` before autoplay can ever activate.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts._evidence_common import (
    EvidenceGateError,
    load_json,
    load_jsonl,
    require_hex64,
    sha256_file,
)
from scripts.validate_phase2_transport_browser_contract import (
    validate_artifact as validate_transport_browser_artifact,
)
from server.config import Config

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = REPO_ROOT / "shared" / "schemas" / "v013-evidence.json"
TRANSPORT_SCHEMA_PATH = REPO_ROOT / "shared" / "schemas" / "v013-transport-browser-contract.json"

REQUIRED_FIELDS = (
    "phase",
    "scenario",
    "turn_id",
    "provider",
    "model",
    "routing_phase_latency_ms",
    "outcome",
    "disposition",
    "sample_timestamp_ms",
    "run_id",
    "sample_index",
    "sample_count",
)
OPTIONAL_FIELDS = ("work_item_id", "query_chars", "context_chars", "wall_clock_time")
# Phase 1 extends the record shape with the ack-path fields described in the
# dev plan's Phase 1 evidence-artifact bullet. They are optional at the
# schema level (so phase0 records are unaffected) but required-though-nullable
# for phase1 records specifically -- enforced in `validate_record` below.
PHASE1_FIELDS = ("ack_id", "ack_enqueued", "ack_enqueued_ms", "ack_terminal_state")
ALL_FIELDS = frozenset(REQUIRED_FIELDS) | frozenset(OPTIONAL_FIELDS) | frozenset(PHASE1_FIELDS)

PHASES = frozenset({"phase0", "phase1", "phase2", "phase3", "phase4"})
OUTCOMES = frozenset(
    {
        "direct",
        "unsupported",
        "control",
        "clarify",
        "completed",
        "mixed",
        "retained",
        "declined",
        "failed",
        "cancelled",
    }
)
DISPOSITIONS = frozenset({"autoplay", "display_only", "suppressed", "not_applicable"})
ACK_TERMINAL_STATES = frozenset(
    {"discarded", "admitted_completed", "no_tts", "unavailable_transport", "not_applicable"}
)

# Phase-specific fields required in addition to REQUIRED_FIELDS. Values may
# be null (nullable-required); presence is what's enforced here, and the
# per-field type/enum checks below reject a wrong-typed non-null value.
PHASE_EXTRA_REQUIRED_FIELDS: Mapping[str, tuple[str, ...]] = {
    "phase1": ("work_item_id", "ack_id", "ack_enqueued", "ack_enqueued_ms", "ack_terminal_state"),
}

# Phase-specific coverage minimums. Each phase declares the scenario names,
# outcomes, and provider/model strata its fixture matrix must exercise at
# least once. Later phases extend this table; they do not replace phase0's.
PHASE_MINIMUMS: Mapping[str, Mapping[str, frozenset[Any]]] = {
    "phase0": {
        "scenarios": frozenset(
            {
                "direct",
                "delegated_complete",
                "retained_late",
                "cancellation",
                "reconnect",
                "same_epoch_newer_turn",
            }
        ),
        "outcomes": frozenset({"direct", "completed", "retained", "cancelled"}),
        "provider_model_strata": frozenset({("unavailable", "unavailable")}),
    },
    "phase1": {
        "scenarios": frozenset(
            {
                "ack_enqueued_timing",
                "exactly_once",
                "queued_discard",
                "admitted_completion",
                "no_tts",
            }
        ),
        "outcomes": frozenset({"completed", "cancelled"}),
        "provider_model_strata": frozenset({("unavailable", "unavailable")}),
    },
}
# The (provider, model) pairs this release actually supports for paid, real
# evidence collection. `has_real_provider_stratum` checks membership here
# rather than "is not the string 'unavailable'": the negative check let an
# artifact self-declare any provider name (e.g. "fake-provider") into the
# paid stratum. The only real pair the collection tooling can produce is
# `run_query_context_experiment.DEFAULT_PROVIDER`/`DEFAULT_MODEL`; the
# runner's dry-run stratum ("synthetic"/"dry-run-fixture") is deliberately
# absent. Adding a provider/model here is what admits it to promotion.
REAL_PROVIDER_ALLOWLIST: frozenset[tuple[str, str]] = frozenset(
    {
        ("openai", "gpt-4o-search-preview"),
    }
)
# "two records each for the queued-discard and admitted-completion race
# scenarios" -- a minimum *count*, not just presence, so it is checked
# separately from PHASE_MINIMUMS' presence-only sets.
PHASE_SCENARIO_MIN_COUNT: Mapping[str, Mapping[str, int]] = {
    "phase1": {"queued_discard": 2, "admitted_completion": 2},
}


def _require_type(value: Any, kinds: tuple[type, ...], field: str, index: int) -> None:
    if isinstance(value, bool) and bool not in kinds:
        raise EvidenceGateError(f"record {index}: {field} must not be a bool, got {value!r}")
    if not isinstance(value, kinds):
        raise EvidenceGateError(
            f"record {index}: {field} expected {kinds}, got {type(value).__name__}"
        )


def validate_record(record: Mapping[str, Any], index: int) -> None:
    unknown = set(record) - ALL_FIELDS
    if unknown:
        raise EvidenceGateError(f"record {index}: unknown field(s) {sorted(unknown)}")
    missing = [name for name in REQUIRED_FIELDS if name not in record]
    if missing:
        raise EvidenceGateError(f"record {index}: missing required field(s) {missing}")

    if record["phase"] not in PHASES:
        raise EvidenceGateError(f"record {index}: invalid phase {record['phase']!r}")

    phase_extra = PHASE_EXTRA_REQUIRED_FIELDS.get(record["phase"], ())
    missing_extra = [name for name in phase_extra if name not in record]
    if missing_extra:
        raise EvidenceGateError(
            f"record {index}: phase={record['phase']!r} missing required field(s) {missing_extra}"
        )

    for field in ("scenario", "turn_id", "provider", "model", "run_id"):
        _require_type(record[field], (str,), field, index)
        if not record[field]:
            raise EvidenceGateError(f"record {index}: {field} must be non-empty")
    if record["outcome"] not in OUTCOMES:
        raise EvidenceGateError(f"record {index}: invalid outcome {record['outcome']!r}")
    if record["disposition"] not in DISPOSITIONS:
        raise EvidenceGateError(f"record {index}: invalid disposition {record['disposition']!r}")

    latency = record["routing_phase_latency_ms"]
    if latency is not None:
        _require_type(latency, (int, float), "routing_phase_latency_ms", index)
        if latency < 0:
            raise EvidenceGateError(f"record {index}: routing_phase_latency_ms must be >= 0")

    _require_type(record["sample_timestamp_ms"], (int,), "sample_timestamp_ms", index)
    if record["sample_timestamp_ms"] < 0:
        raise EvidenceGateError(f"record {index}: sample_timestamp_ms must be >= 0")
    _require_type(record["sample_index"], (int,), "sample_index", index)
    if record["sample_index"] < 0:
        raise EvidenceGateError(f"record {index}: sample_index must be >= 0")
    _require_type(record["sample_count"], (int,), "sample_count", index)
    if record["sample_count"] < 1:
        raise EvidenceGateError(f"record {index}: sample_count must be >= 1")

    if "work_item_id" in record and record["work_item_id"] is not None:
        _require_type(record["work_item_id"], (str,), "work_item_id", index)
        if not record["work_item_id"]:
            raise EvidenceGateError(f"record {index}: work_item_id must be non-empty")
    for field in ("query_chars", "context_chars"):
        if field in record and record[field] is not None:
            _require_type(record[field], (int,), field, index)
            if record[field] < 0:
                raise EvidenceGateError(f"record {index}: {field} must be >= 0")
    if "wall_clock_time" in record and record["wall_clock_time"] is not None:
        _require_type(record["wall_clock_time"], (str,), "wall_clock_time", index)

    if "ack_id" in record and record["ack_id"] is not None:
        _require_type(record["ack_id"], (str,), "ack_id", index)
        if not record["ack_id"]:
            raise EvidenceGateError(f"record {index}: ack_id must be non-empty")
    if "ack_enqueued" in record and record["ack_enqueued"] is not None:
        _require_type(record["ack_enqueued"], (bool,), "ack_enqueued", index)
    if "ack_enqueued_ms" in record and record["ack_enqueued_ms"] is not None:
        _require_type(record["ack_enqueued_ms"], (int,), "ack_enqueued_ms", index)
        if record["ack_enqueued_ms"] < 0:
            raise EvidenceGateError(f"record {index}: ack_enqueued_ms must be >= 0")
    if (
        "ack_terminal_state" in record
        and record["ack_terminal_state"] is not None
        and record["ack_terminal_state"] not in ACK_TERMINAL_STATES
    ):
        raise EvidenceGateError(
            f"record {index}: invalid ack_terminal_state {record['ack_terminal_state']!r}"
        )


def load_records(input_path: Path) -> list[dict[str, Any]]:
    return load_jsonl(input_path)


def check_phase_minimums(phase: str, records: Sequence[Mapping[str, Any]]) -> None:
    minimums = PHASE_MINIMUMS.get(phase)
    if minimums is None:
        raise EvidenceGateError(f"no coverage minimums declared for phase {phase!r}")

    phase_records = [r for r in records if r["phase"] == phase]
    if not phase_records:
        raise EvidenceGateError(f"no records found for phase {phase!r}")

    seen_scenarios = {r["scenario"] for r in phase_records}
    missing_scenarios = minimums["scenarios"] - seen_scenarios
    if missing_scenarios:
        raise EvidenceGateError(
            f"phase {phase!r}: missing scenario coverage for {sorted(missing_scenarios)}"
        )

    seen_outcomes = {r["outcome"] for r in phase_records}
    missing_outcomes = minimums["outcomes"] - seen_outcomes
    if missing_outcomes:
        raise EvidenceGateError(
            f"phase {phase!r}: missing outcome coverage for {sorted(missing_outcomes)}"
        )

    seen_strata = {(r["provider"], r["model"]) for r in phase_records}
    missing_strata = minimums["provider_model_strata"] - seen_strata
    if missing_strata:
        raise EvidenceGateError(
            f"phase {phase!r}: missing provider/model stratum coverage for {sorted(missing_strata)}"
        )

    min_counts = PHASE_SCENARIO_MIN_COUNT.get(phase, {})
    for scenario, minimum in min_counts.items():
        count = sum(1 for r in phase_records if r["scenario"] == scenario)
        if count < minimum:
            raise EvidenceGateError(
                f"phase {phase!r}: scenario {scenario!r} requires >= {minimum} record(s), got {count}"
            )


def validate_artifact(phase: str, input_path: Path) -> list[dict[str, Any]]:
    records = load_records(input_path)
    for index, record in enumerate(records):
        validate_record(record, index)
    check_phase_minimums(phase, records)
    return records


def has_real_provider_stratum(records: list[Mapping[str, Any]]) -> bool:
    """True if any record uses an allowlisted real provider/model stratum.

    The credential-free ``unavailable``/``unavailable`` stratum can validate
    schema/lifecycle correctness but can never make a manifest
    promotion-eligible; this is the check that enforces that split. Membership
    in ``REAL_PROVIDER_ALLOWLIST`` is required rather than merely differing
    from ``"unavailable"``, so an artifact cannot self-declare an arbitrary
    provider name into the paid stratum.
    """
    return any((r["provider"], r["model"]) in REAL_PROVIDER_ALLOWLIST for r in records)


def _validate_transport_contract(input_path: Path) -> dict[str, Any]:
    """Revalidate the Phase 2 transport/browser contract artifact.

    Delegates to `scripts.validate_phase2_transport_browser_contract` so this
    writer never re-implements or drifts from that gate's own logic.
    """
    record = load_json(input_path)
    if not isinstance(record, dict):
        raise EvidenceGateError("transport contract artifact must be a JSON object")
    validate_transport_browser_artifact(record)
    return record


PHASE3_COMPLETION_REQUIRED_FIELDS = (
    "source_commit",
    "source_tree_hash",
    "command_digest",
    "generated_at_utc",
)


def _validate_phase3_completion(
    input_path: Path, *, source_commit: str, source_tree_hash: str
) -> dict[str, Any]:
    """Revalidate the Phase 3 completion artifact `record_phase3_completion.py` writes."""
    record = load_json(input_path)
    if not isinstance(record, dict):
        raise EvidenceGateError("phase3 completion artifact must be a JSON object")
    missing = [field for field in PHASE3_COMPLETION_REQUIRED_FIELDS if not record.get(field)]
    if missing:
        raise EvidenceGateError(f"phase3 completion artifact missing field(s) {missing}")
    if record["source_commit"] != source_commit:
        raise EvidenceGateError(
            "phase3 completion artifact source_commit "
            f"{record['source_commit']!r} does not match --source-commit {source_commit!r}"
        )
    if record["source_tree_hash"] != source_tree_hash:
        raise EvidenceGateError(
            "phase3 completion artifact source_tree_hash "
            f"{record['source_tree_hash']!r} does not match "
            f"--source-tree-hash {source_tree_hash!r}"
        )
    return record


# Fields that carry a hash/digest/commit identity in the Phase 4C promoted
# post-change artifact: validated as non-empty lowercase hex strings. This is
# what distinguishes a well-formed binding from a corrupted/foreign one (e.g.
# a "stale..." placeholder) without requiring a byte-for-byte recomputation
# against the bound Phase 3 artifact, which the schema does not otherwise tie
# together structurally.
PHASE4C_HASH_LIKE_FIELDS = frozenset(
    {
        "post_change_source_commit",
        "post_change_source_tree_hash",
        "phase3_completion_hash",
        # These two field names must match
        # `shared/schemas/v013-query-context-post-change-analysis.json`
        # exactly. They previously read `..._input_hash`, so a
        # schema-conformant artifact was rejected here and only this
        # validator's own non-schema spellings could ever be bound into a
        # manifest. `test_phase4c_field_set_matches_the_schema` pins them.
        "phase4b_baseline_input_sha256",
        "phase4b_normalized_input_sha256",
        "experiment_command_digest",
        "analyzer_command_digest",
        "scorer_hash",
        "control_fingerprint",
    }
)
# The subset the schema pins to exactly 64 hex characters (SHA-256 digests),
# as opposed to the `minLength: 1` identity fields above.
PHASE4C_SHA256_FIELDS = frozenset(
    {
        "phase4b_baseline_input_sha256",
        "phase4b_normalized_input_sha256",
        "scorer_hash",
    }
)
PHASE4C_STRING_FIELDS = frozenset({"fixture_version", "scorer_version", "generated_at_utc"})
PHASE4C_ALLOWED_FIELDS = (
    frozenset({"status", "promotion_eligible"}) | PHASE4C_HASH_LIKE_FIELDS | PHASE4C_STRING_FIELDS
)
# Unanchored + `fullmatch`, matching `_evidence_common.HEX64_RE` and
# `server.config._is_hex_hash`: `$` also matches before a trailing newline.
_HEX_RE = re.compile(r"[0-9a-f]+")


def _validate_phase4c_artifact(payload: Any) -> None:
    """Validate a Phase 4C promoted post-change analysis artifact.

    Only a ``status=promoted``, ``promotion_eligible=true`` artifact with
    every required binding field present, correctly typed, and (for
    hash/digest/commit fields) valid lowercase hex may be bound into the
    final manifest.
    """
    if not isinstance(payload, dict):
        raise EvidenceGateError("phase4c artifact must be a JSON object")

    unknown = set(payload) - PHASE4C_ALLOWED_FIELDS
    if unknown:
        raise EvidenceGateError(f"phase4c artifact: unknown field(s) {sorted(unknown)}")
    missing = PHASE4C_ALLOWED_FIELDS - set(payload)
    if missing:
        raise EvidenceGateError(f"phase4c artifact: missing required field(s) {sorted(missing)}")

    if not isinstance(payload["status"], str) or payload["status"] != "promoted":
        raise EvidenceGateError(
            f"phase4c artifact: status must be 'promoted', got {payload['status']!r}"
        )
    if (
        not isinstance(payload["promotion_eligible"], bool)
        or payload["promotion_eligible"] is not True
    ):
        raise EvidenceGateError("phase4c artifact: promotion_eligible must be true")

    for name in PHASE4C_HASH_LIKE_FIELDS:
        value = payload[name]
        if name in PHASE4C_SHA256_FIELDS:
            # The schema pins these to exactly 64 hex characters; accepting any
            # hex length here let a truncated or over-long digest bind into a
            # manifest that the schema would have rejected.
            require_hex64(value, f"phase4c artifact: {name}")
            continue
        if not isinstance(value, str) or not value or not _HEX_RE.fullmatch(value):
            raise EvidenceGateError(
                f"phase4c artifact: {name} must be a non-empty lowercase hex string"
            )
    for name in PHASE4C_STRING_FIELDS:
        value = payload[name]
        if not isinstance(value, str) or not value:
            raise EvidenceGateError(f"phase4c artifact: {name} must be a non-empty string")


def _bind_phase4c_artifact(
    payload: Mapping[str, Any],
    *,
    source_commit: str,
    source_tree_hash: str,
    phase3_sha256: str | None,
) -> None:
    """Cross-check a shape-valid Phase 4C artifact against this release.

    Shape validation alone lets a well-formed artifact generated against a
    different tree, or bound to a different Phase 3 completion record, be
    stamped into this manifest. The Phase 4C artifact describes the *post
    -change* runtime tree, which is exactly the tree this manifest is being
    written for, so its source identity must equal the writer's.

    The fixture/scorer/control/command bindings are deliberately not checked
    here: the Phase 4A/4B artifacts they refer to are not inputs to this
    writer, so verifying them would require new writer inputs rather than a
    local cross-check.
    """
    if payload["post_change_source_commit"] != source_commit:
        raise EvidenceGateError(
            "phase4c artifact: post_change_source_commit "
            f"{payload['post_change_source_commit']!r} does not match --source-commit "
            f"{source_commit!r}"
        )
    if payload["post_change_source_tree_hash"] != source_tree_hash:
        raise EvidenceGateError(
            "phase4c artifact: post_change_source_tree_hash "
            f"{payload['post_change_source_tree_hash']!r} does not match --source-tree-hash "
            f"{source_tree_hash!r}"
        )
    if phase3_sha256 is None:
        raise EvidenceGateError("phase4c artifact requires a --phase3-input to bind against")
    if payload["phase3_completion_hash"] != phase3_sha256:
        raise EvidenceGateError(
            "phase4c artifact: phase3_completion_hash "
            f"{payload['phase3_completion_hash']!r} does not match the bound Phase 3 completion "
            f"artifact digest {phase3_sha256!r}"
        )


def _repo_relative_evidence_path(input_path: Path) -> str:
    """Resolve ``input_path`` and express it relative to ``REPO_ROOT`` for
    storage in the manifest's ``inputs[*].path`` field.

    ``server.config.load_promotion_manifest`` treats a manifest-declared
    evidence path as attacker-steerable (the artifact under validation
    declares it about itself) and confines it to the repo tree, rejecting
    every absolute path outright -- so writing an absolute path here (the
    natural CLI form in CI, e.g. ``$GITHUB_WORKSPACE/...``) would silently
    produce a manifest the loader can never accept: `promotion_eligible`
    would resolve to `False`/`evidence_unresolvable` at server boot with no
    error surfaced anywhere. Failing loudly here, at write time -- while a
    human or CI log is watching -- is strictly better than that silent
    degradation.
    """
    resolved = input_path.resolve()
    try:
        return resolved.relative_to(REPO_ROOT).as_posix()
    except ValueError as exc:
        raise EvidenceGateError(
            f"evidence input {input_path} (resolved: {resolved}) must live under "
            f"the repo root ({REPO_ROOT}) -- load_promotion_manifest confines every "
            "manifest-declared evidence path to the repo tree and would otherwise "
            "silently reject this manifest at load time"
        ) from exc


def write_manifest(
    *,
    manifest_phase: str,
    phase0_input: Path,
    phase1_input: Path,
    phase2_input: Path,
    phase3_input: Path | None,
    phase4c_input: Path | None,
    source_commit: str,
    source_tree_hash: str,
    deployed_at_utc: str,
    feature_policy_fingerprint: str,
    output: Path,
) -> dict[str, Any]:
    if manifest_phase not in {"provisional", "final"}:
        raise EvidenceGateError(f"unsupported --manifest-phase {manifest_phase!r}")
    if manifest_phase == "final" and phase3_input is None:
        raise EvidenceGateError("--manifest-phase final requires --phase3-input")

    phase0_records = validate_artifact("phase0", phase0_input)
    phase1_records = validate_artifact("phase1", phase1_input)
    transport_record = _validate_transport_contract(phase2_input)
    # The Phase 2 gate proves the artifact is internally consistent; it says
    # nothing about *which* build it describes. Without this check a
    # stale-but-eligible browser check from a different tree could be stamped
    # into a manifest for the current release identity.
    if transport_record["source_commit"] != source_commit:
        raise EvidenceGateError(
            "phase2 transport artifact source_commit "
            f"{transport_record['source_commit']!r} does not match --source-commit "
            f"{source_commit!r}"
        )
    if transport_record["source_tree_hash"] != source_tree_hash:
        raise EvidenceGateError(
            "phase2 transport artifact source_tree_hash "
            f"{transport_record['source_tree_hash']!r} does not match --source-tree-hash "
            f"{source_tree_hash!r}"
        )
    phase3_record = (
        _validate_phase3_completion(
            phase3_input, source_commit=source_commit, source_tree_hash=source_tree_hash
        )
        if phase3_input is not None
        else None
    )

    inputs: dict[str, dict[str, str]] = {
        "phase0": {
            "path": _repo_relative_evidence_path(phase0_input),
            "sha256": sha256_file(phase0_input),
        },
        "phase1": {
            "path": _repo_relative_evidence_path(phase1_input),
            "sha256": sha256_file(phase1_input),
        },
        "phase2": {
            "path": _repo_relative_evidence_path(phase2_input),
            "sha256": sha256_file(phase2_input),
        },
    }
    if phase3_input is not None:
        inputs["phase3"] = {
            "path": _repo_relative_evidence_path(phase3_input),
            "sha256": sha256_file(phase3_input),
        }

    phase4c_artifact_sha256: str | None = None
    if phase4c_input is not None:
        phase4c_payload = load_json(phase4c_input)
        _validate_phase4c_artifact(phase4c_payload)
        _bind_phase4c_artifact(
            phase4c_payload,
            source_commit=source_commit,
            source_tree_hash=source_tree_hash,
            phase3_sha256=inputs["phase3"]["sha256"] if phase3_input is not None else None,
        )
        phase4c_artifact_sha256 = sha256_file(phase4c_input)
        inputs["phase4c"] = {
            "path": _repo_relative_evidence_path(phase4c_input),
            "sha256": phase4c_artifact_sha256,
        }

    real_stratum_present = has_real_provider_stratum([*phase0_records, *phase1_records])
    transport_eligible = bool(transport_record.get("promotion_eligible"))
    phase3_complete = phase3_record is not None

    reason: str | None
    if manifest_phase == "provisional":
        # A provisional manifest is diagnostic-only and is permanently
        # ineligible; Phase 3 replaces it with a "final" manifest that can
        # actually pass `load_promotion_manifest`'s eligibility gate.
        promotion_eligible = False
        if not real_stratum_present:
            reason = "real_stratum_missing"
        elif not transport_eligible:
            reason = "audibility_unverified"
        else:
            reason = "provisional_manifest"
    else:
        promotion_eligible = real_stratum_present and transport_eligible and phase3_complete
        reason = (
            None
            if promotion_eligible
            else (
                "real_stratum_missing"
                if not real_stratum_present
                else "audibility_unverified"
                if not transport_eligible
                else "phase3_incomplete"
            )
        )

    if not SCHEMA_PATH.exists():
        # Stamping an empty digest here would still print "OK: ...
        # promotion_eligible=True" even though `server/config.py`'s reader
        # recomputes this same schema and fails closed on it downstream --
        # better to fail the writer now than mislead the operator running it.
        raise EvidenceGateError(f"evidence schema missing, cannot stamp manifest: {SCHEMA_PATH}")
    schema_hash = sha256_file(SCHEMA_PATH)
    manifest = {
        "manifest_phase": manifest_phase,
        "promotion_eligible": promotion_eligible,
        "reason": reason,
        "schema_hash": schema_hash,
        "source_commit": source_commit,
        "source_tree_hash": source_tree_hash,
        "release_version": Config().release_version,
        "feature_policy_fingerprint": feature_policy_fingerprint,
        "deployed_at_utc": deployed_at_utc,
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "inputs": inputs,
    }
    if phase3_input is not None:
        manifest["phase3_completion_hash"] = inputs["phase3"]["sha256"]
        # `_validate_phase3_completion` verifies the recorded command digest is
        # present, then the manifest used to drop it -- so downstream release
        # verification had no way to re-check which command produced Phase 3.
        assert phase3_record is not None
        manifest["phase3_command_digest"] = phase3_record["command_digest"]
    if phase4c_artifact_sha256 is not None:
        manifest["phase4c_artifact_sha256"] = phase4c_artifact_sha256

    _atomic_write_manifest(output, manifest)
    return manifest


def _write_bytes_no_follow(path: Path, payload: bytes) -> None:
    """Write `payload` to `path`, refusing to write *through* a symlink.

    ``O_NOFOLLOW`` makes an attacker-planted symlink at this predictable path
    fail with ``ELOOP`` instead of silently redirecting manifest bytes to the
    link's target. The path may legitimately already exist as a regular file
    (a `.previous` copy from an earlier run), so ``O_EXCL`` is not usable
    here -- ``O_NOFOLLOW`` is the check that matters.
    """
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o644)
    try:
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_directory(directory: Path) -> None:
    dir_fd = os.open(str(directory), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _atomic_write_manifest(output: Path, manifest: dict[str, Any]) -> None:
    """Durable atomic-rename write with fsync, preserving a `.previous` copy.

    Writes a same-directory temp file, fsyncs it, atomically renames it over
    the destination, then fsyncs the parent directory. A pre-existing
    manifest is preserved as `<output>.previous` until the rename is
    durable; on any failure before the rename completes, the previous
    manifest (if any) is left untouched.

    Two properties this function must hold, both of which it previously did
    not:

    * **old-or-new, always.** A failure of the post-rename directory fsync
      used to propagate with the *new* file already installed, so a caller
      that saw the exception could still be left reading the new manifest.
      That failure now rolls the previous manifest back (or removes the new
      one when there was no previous) before re-raising.
    * **no symlink follow-through.** The temp file used a predictable
      `<output>.tmp` path opened without ``O_EXCL``/``O_NOFOLLOW``, so a
      planted symlink there would have received the manifest bytes. The temp
      file is now created by `tempfile.mkstemp` (random name, ``O_EXCL``) and
      the `.previous` copy is opened ``O_NOFOLLOW``.
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    previous_path = output.with_suffix(output.suffix + ".previous")
    previous_bytes: bytes | None = None
    if output.exists():
        previous_bytes = output.read_bytes()
        _write_bytes_no_follow(previous_path, previous_bytes)

    fd, tmp_name = tempfile.mkstemp(
        dir=str(output.parent), prefix=f".{output.name}.", suffix=".tmp"
    )
    tmp_path = Path(tmp_name)
    try:
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    try:
        os.chmod(tmp_path, 0o644)
        os.replace(tmp_path, output)
    except BaseException:
        # Nothing was installed: drop the temp file and leave any previous
        # manifest exactly as it was.
        tmp_path.unlink(missing_ok=True)
        raise

    try:
        _fsync_directory(output.parent)
    except BaseException:
        _rollback_manifest(output, previous_bytes)
        raise


def _rollback_manifest(output: Path, previous_bytes: bytes | None) -> None:
    """Undo an installed-but-not-durable manifest replacement.

    Restores the previous manifest bytes, or removes the newly installed file
    when there was no previous manifest, so a caller that sees the write fail
    never finds a half-committed new manifest in place. A failure *during*
    the restore is deliberately suppressed: the original write failure is the
    error worth reporting, and re-raising from here would mask it.
    """
    try:
        if previous_bytes is None:
            output.unlink(missing_ok=True)
        else:
            _write_bytes_no_follow(output, previous_bytes)
    except OSError:
        pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=sorted(PHASE_MINIMUMS))
    parser.add_argument("--input", type=Path)
    parser.add_argument("--write-manifest", action="store_true")
    parser.add_argument("--manifest-phase", default="provisional", choices=("provisional", "final"))
    parser.add_argument("--phase0-input", type=Path)
    parser.add_argument("--phase1-input", type=Path)
    parser.add_argument("--phase2-input", type=Path)
    parser.add_argument("--phase3-input", type=Path)
    parser.add_argument("--phase4c-input", type=Path)
    parser.add_argument("--source-commit")
    parser.add_argument("--source-tree-hash")
    parser.add_argument("--deployed-at-utc")
    parser.add_argument("--feature-policy-fingerprint")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)

    if args.write_manifest:
        required = {
            "--phase0-input": args.phase0_input,
            "--phase1-input": args.phase1_input,
            "--phase2-input": args.phase2_input,
            "--source-commit": args.source_commit,
            "--source-tree-hash": args.source_tree_hash,
            "--deployed-at-utc": args.deployed_at_utc,
            "--feature-policy-fingerprint": args.feature_policy_fingerprint,
            "--output": args.output,
        }
        missing = [name for name, value in required.items() if not value]
        if args.manifest_phase == "final" and not args.phase3_input:
            missing.append("--phase3-input")
        if missing:
            print(f"FAIL: --write-manifest requires {missing}", file=sys.stderr)
            return 1
        try:
            manifest = write_manifest(
                manifest_phase=args.manifest_phase,
                phase0_input=args.phase0_input,
                phase1_input=args.phase1_input,
                phase2_input=args.phase2_input,
                phase3_input=args.phase3_input,
                phase4c_input=args.phase4c_input,
                source_commit=args.source_commit,
                source_tree_hash=args.source_tree_hash,
                deployed_at_utc=args.deployed_at_utc,
                feature_policy_fingerprint=args.feature_policy_fingerprint,
                output=args.output,
            )
        except (EvidenceGateError, OSError) as exc:
            print(f"FAIL: {exc}", file=sys.stderr)
            return 1
        print(
            f"OK: wrote {args.output} (manifest_phase={manifest['manifest_phase']}, "
            f"promotion_eligible={manifest['promotion_eligible']})"
        )
        return 0

    if not args.phase or not args.input:
        parser.error("--phase and --input are required unless --write-manifest is given")

    try:
        records = validate_artifact(args.phase, args.input)
    except EvidenceGateError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    print(f"OK: {len(records)} record(s) validated for phase={args.phase}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
