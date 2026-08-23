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
from pathlib import Path
from typing import Any

from scripts.evidence_common import (
    REPO_ROOT,
    EvidenceGateError,
    confine_output_arg,
    confined_evidence_input_path,
    load_json,
    load_jsonl,
    now_utc,
    read_bytes_if_present,
    require_hex64,
    require_type,
    sha256_file,
    write_bytes_no_follow,
)
from scripts.validate_phase2_transport_browser_contract import (
    validate_artifact as validate_transport_browser_artifact,
)
from server.config import (
    _MANIFEST_REQUIRED_FIELDS,
    _MANIFEST_REQUIRED_FINAL_INPUTS,
    _MANIFEST_STRING_FIELDS,
    effective_feature_policy_fingerprint,
    load_config,
)

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
    """``evidence_common.require_type`` with this file's per-record prefix.

    Round-3 restart gauntlet, Architecture finding: this was a forked copy of
    the shared check (same bool-is-not-an-int rule, same isinstance test),
    differing only in prefixing the message with the record index. Two copies
    of a type predicate can harden independently, so the predicate now lives
    in one place and only the *message* is local -- which is the actual
    difference.
    """
    try:
        require_type(value, kinds, field)
    except EvidenceGateError as exc:
        raise EvidenceGateError(f"record {index}: {exc}") from exc


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


def _read_phase3_completion(input_path: Path) -> dict[str, Any]:
    """Shape-validate the Phase 3 completion artifact, without binding it to a
    source identity.

    Split out of :func:`_validate_phase3_completion` for
    :func:`verify_manifest`, which needs `command_digest` off this record but
    has no trustworthy ``--source-commit``/``--source-tree-hash`` to bind it
    against: those live in the manifest under scrutiny and are deliberately
    *not* re-derived (see ``_MANIFEST_VOLATILE_FIELDS``), so comparing the
    artifact to them would only prove the forged manifest is self-consistent.
    """
    record = load_json(input_path)
    if not isinstance(record, dict):
        raise EvidenceGateError("phase3 completion artifact must be a JSON object")
    missing = [field for field in PHASE3_COMPLETION_REQUIRED_FIELDS if not record.get(field)]
    if missing:
        raise EvidenceGateError(f"phase3 completion artifact missing field(s) {missing}")
    return record


def _validate_phase3_completion(
    input_path: Path, *, source_commit: str, source_tree_hash: str
) -> dict[str, Any]:
    """Revalidate the Phase 3 completion artifact `record_phase3_completion.py` writes."""
    record = _read_phase3_completion(input_path)
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
        # The sha256 of the exact quality-fixture bytes the Phase 4B
        # decision was scored against. `fixture_version` alone is a
        # self-declared string, so it binds nothing: a same-version fixture
        # with weaker required_facts/expected_citations/disallowed_claims
        # would resolve forged match IDs cleanly.
        "fixture_sha256",
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
        "fixture_sha256",
    }
)
PHASE4C_STRING_FIELDS = frozenset({"fixture_version", "scorer_version", "generated_at_utc"})
PHASE4C_ALLOWED_FIELDS = (
    frozenset({"status", "promotion_eligible"}) | PHASE4C_HASH_LIKE_FIELDS | PHASE4C_STRING_FIELDS
)
# Unanchored + `fullmatch`, matching `evidence_common.HEX64_RE` and
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


def _promotion_verdict(
    *, manifest_phase: str, real_stratum_present: bool, transport_eligible: bool
) -> tuple[bool, str | None]:
    """The ``(promotion_eligible, reason)`` pair the current evidence supports.

    Called by :func:`write_manifest` to *stamp* the verdict and by
    :func:`verify_manifest` to *re-derive* it. One function rather than two
    copies: ``verify_manifest`` re-derived only ``promotion_eligible`` while
    listing ``reason`` as covered, so hand-editing ``reason`` to a different
    string passed the CI gate clean (round-4 confirm pass, Logic + Security +
    Architecture findings). Sharing the selection makes the two structurally
    unable to disagree about what the reason for a verdict is.
    """
    if manifest_phase == "provisional":
        # A provisional manifest is diagnostic-only and is permanently
        # ineligible; Phase 3 replaces it with a "final" manifest that can
        # actually pass `load_promotion_manifest`'s eligibility gate.
        if not real_stratum_present:
            return False, "real_stratum_missing"
        if not transport_eligible:
            return False, "audibility_unverified"
        return False, "provisional_manifest"
    promotion_eligible = real_stratum_present and transport_eligible
    if promotion_eligible:
        return True, None
    return False, ("real_stratum_missing" if not real_stratum_present else "audibility_unverified")


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

    # `validate_artifact` returns every record in the file regardless of that
    # record's own `phase` field (only `check_phase_minimums` filters by
    # phase). Filter here too, so a record mislabeled with an unrelated phase
    # can't satisfy the paid-stratum gate without real phase0/phase1
    # coverage.
    real_stratum_present = has_real_provider_stratum(
        [r for r in phase0_records if r["phase"] == "phase0"]
        + [r for r in phase1_records if r["phase"] == "phase1"]
    )
    transport_eligible = bool(transport_record.get("promotion_eligible"))

    if manifest_phase == "final":
        # Phase 3 completeness is not a *reason* `_promotion_verdict` can
        # report: the guard at the top of this function refuses
        # `--manifest-phase final` without `--phase3-input` outright, so
        # `phase3_record` is never None here. The old `"phase3_incomplete"`
        # reason string was therefore dead (round-3 restart gauntlet, Logic
        # finding) -- a downstream consumer keying on it would have waited
        # forever for a value the writer could not emit.
        #
        # Removed rather than made reachable: turning the hard refusal into a
        # soft reason would mean writing an ineligible-but-well-formed final
        # manifest where the writer currently declines to write one at all.
        # That is a promotion-gate policy change, not a dead-code cleanup, and
        # it is not one this file gets to make on its own.
        assert phase3_record is not None, (
            "--manifest-phase final without --phase3-input must have been refused above"
        )
    promotion_eligible, reason = _promotion_verdict(
        manifest_phase=manifest_phase,
        real_stratum_present=real_stratum_present,
        transport_eligible=transport_eligible,
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
        # `Config()` alone only ever yields the package-installed default --
        # it never reads `WEBSEARCH_RELEASE_VERSION`/`[features].release_version`,
        # so a configured non-default release was silently overwritten with
        # the default here and then failed closed as a source mismatch when
        # `load_promotion_manifest` compared it against the running server's
        # own `load_config()`-resolved release version. Resolve the same
        # effective config the server itself loads at boot.
        "release_version": load_config().release_version,
        "feature_policy_fingerprint": feature_policy_fingerprint,
        "deployed_at_utc": deployed_at_utc,
        "generated_at_utc": now_utc(),
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


# The manifest's own no-follow read/write primitives used to live here as
# private copies of ``scripts/evidence_common``'s (round-3 restart gauntlet,
# Architecture finding). They were the security-critical half of this file --
# ``O_NOFOLLOW``/``O_NONBLOCK``/``fstat``-``S_ISREG``/byte-cap -- duplicated
# beside the five helpers this file already imported from that module, so the
# two copies could (and did) harden independently: the shared writer had
# gained an output byte cap and a short-write loop that the copy here never
# got, meaning a payload larger than one ``write(2)`` could return was
# silently truncated into the manifest with no error raised.
#
# ``_MAX_MANIFEST_BYTES`` is likewise ``evidence_common``'s
# ``_MAX_EVIDENCE_INPUT_BYTES`` under a local name; the cap is now expressed
# as that module's default rather than as a second constant to keep in step.


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
      the `.previous` copy is both read (`read_bytes_if_present`) and written
      (`write_bytes_no_follow`) ``O_NOFOLLOW``. The read side matters as much
      as the write side: it is what stops a symlink at the manifest path from
      leaking its target's contents into `<output>.previous`.

    ``read_bytes_if_present`` is the shared reader's "absent is fine, every
    other failure is not" variant: a first run has no manifest to back up, but
    a symlink, FIFO, device node or oversized file at the manifest path still
    raises rather than being read as "nothing there to preserve".
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    previous_path = output.with_suffix(output.suffix + ".previous")
    previous_bytes = read_bytes_if_present(output)
    if previous_bytes is not None:
        write_bytes_no_follow(previous_path, previous_bytes)

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

    The handler catches ``EvidenceGateError`` as well as ``OSError`` (round-3
    restart gauntlet, Logic finding). ``write_bytes_no_follow`` raises both --
    ``OSError`` for ``ELOOP`` and the ordinary IO failures, but
    ``EvidenceGateError`` for the non-regular-file and over-cap checks -- so
    suppressing only ``OSError`` let exactly those cases propagate out of the
    rollback and replace the original write failure, which is the outcome the
    paragraph above says must not happen.
    """
    try:
        if previous_bytes is None:
            output.unlink(missing_ok=True)
        else:
            write_bytes_no_follow(output, previous_bytes)
    except (OSError, EvidenceGateError):
        pass


#: Manifest fields that describe *when and where* the manifest was produced
#: rather than *what evidence it binds*. Excluded from the drift check in
#: :func:`verify_manifest`, because a manifest committed at one commit can
#: never match a regeneration at a later one: ``deployed_at_utc`` and
#: ``generated_at_utc`` are wall-clock stamps, and ``source_commit`` /
#: ``source_tree_hash`` name the checkout the manifest was written *from*,
#: which every subsequent commit on the branch changes.
#:
#: ``release_version`` and ``feature_policy_fingerprint`` used to be listed
#: here too, on the same "derived from whichever checkout the verifier happens
#: to be running in" rationale -- which does not apply to them (round-4 confirm
#: pass, Architecture finding). Both are deterministic from the effective
#: config, both are re-derived by ``verify_manifest`` now, and both are
#: compared by ``server/config.py::load_promotion_manifest`` with a fail-closed
#: display-only verdict, so excluding them meant CI could not pre-catch two
#: mismatches the runtime loader is guaranteed to reject.
#:
#: Everything *not* listed here is evidence binding, and must match.
_MANIFEST_VOLATILE_FIELDS = frozenset(
    {
        "deployed_at_utc",
        "generated_at_utc",
        "source_commit",
        "source_tree_hash",
    }
)

#: The evidence-binding fields :func:`verify_manifest` knows how to re-derive.
#: Together with ``_MANIFEST_VOLATILE_FIELDS`` this must cover every top-level
#: manifest key -- ``verify_manifest`` reports anything left over as drift, so
#: a field added to ``write_manifest`` cannot slip through unverified.
#:
#: **Membership here is a claim that the field is actually compared**, and
#: ``tests/test_v013_evidence_validator.py`` pins that claim per field by
#: mutating each one and asserting drift is reported. Three names were listed
#: here while nothing in ``verify_manifest`` compared them (round-4 confirm
#: pass, Architecture + Security + Logic findings): ``reason`` (never
#: re-derived), ``manifest_phase`` (read as an input, never checked), and
#: ``phase3_command_digest`` (unreachable by construction -- the cross-check
#: loop paired it with ``phase = None`` and skipped it on the first
#: statement). The trailing uncovered-field guard counted all three as
#: covered, so it asserted a completeness property the function did not have.
_MANIFEST_VERIFIED_FIELDS = frozenset(
    {
        "inputs",
        "schema_hash",
        "manifest_phase",
        "promotion_eligible",
        "reason",
        "release_version",
        "feature_policy_fingerprint",
        "phase3_completion_hash",
        "phase3_command_digest",
        "phase4c_artifact_sha256",
    }
)


def verify_manifest(manifest_path: Path) -> list[str]:
    """Re-derive the committed manifest's evidence bindings and report drift.

    The read-only counterpart to :func:`write_manifest`, for CI. The
    ``release-metadata`` job used to *write* a manifest into an ephemeral
    workspace and end there -- no commit, no artifact upload, no deploy step --
    while the actual consumer (``server/config.py``'s
    ``load_promotion_manifest``) reads the repo-committed file. Producer and
    consumer were never connected, so the job proved nothing (round-3 restart
    gauntlet, Architecture finding).

    Verifying instead of writing is the conservative fix: it makes the job
    prove something real about the file the consumer actually reads, without
    introducing a publish path (which is an operator decision about release
    mechanics, not something a review fix gets to add).

    What is checked, and why each one is drift worth failing on:

    * every field ``server.config._MANIFEST_REQUIRED_FIELDS`` requires is
      present, and every field ``_MANIFEST_STRING_FIELDS`` names is a JSON
      string -- the completeness guard at the end of this function only ever
      caught an *extra* field CI didn't recognize, never a required field
      that was missing or wrongly typed, which let this gate report clean on
      a manifest ``load_promotion_manifest`` rejects outright;
    * every declared ``inputs[*].sha256`` still matches the bytes at
      ``inputs[*].path`` -- i.e. nobody edited an evidence artifact without
      regenerating the manifest that vouches for it;
    * ``schema_hash`` still matches ``shared/schemas/v013-evidence.json`` --
      ``server/config.py`` recomputes this at boot and fails closed on a
      mismatch, so CI should catch it first;
    * ``release_version`` and ``feature_policy_fingerprint`` still match the
      effective config, the two identity bindings ``load_promotion_manifest``
      itself compares and fails closed on;
    * ``manifest_phase`` is one of the two known values and the ``inputs``
      *it* implies are all declared -- a ``final`` manifest must carry
      phase0-3 and a ``phase3_completion_hash``. It is a writer input, not
      something re-derivable from evidence, so this presence-and-consistency
      check plus its role in the verdict below is the whole of its coverage;
    * every *required* declared input still resolves and *passes its gate* --
      a manifest must not outlive the validity of the evidence it points at;
    * ``promotion_eligible`` **and** ``reason`` still equal what those gates
      now conclude, re-derived through the same :func:`_promotion_verdict`
      the writer stamps them with -- the check that stops a stale manifest
      from authorizing data-driven tuning the current evidence no longer
      supports;
    * ``phase3_command_digest`` still matches the ``command_digest`` inside
      the resolved Phase 3 completion artifact, the provenance binding
      ``write_manifest`` copies up so release verification can re-check which
      command produced Phase 3.

    **The empty return is the security property.** An empty list must mean
    every one of the above was actually evaluated, never that some of them
    were skipped: the pre-fix guard was ``if {"phase0","phase1","phase2"} <=
    resolved.keys()``, so a manifest that simply *omitted* an ``inputs`` entry
    never entered the branch and a forged ``promotion_eligible=true`` passed
    with exit 0 (round-4 confirm pass, Logic + Security findings). Every
    "cannot check this" path below therefore appends drift rather than
    falling through.

    Returns a list of human-readable drift descriptions; empty means clean.
    Raises ``EvidenceGateError`` only when the manifest itself cannot be read
    or is structurally unusable.
    """
    manifest = load_json(manifest_path)
    if not isinstance(manifest, dict):
        raise EvidenceGateError(f"{manifest_path}: manifest must be a JSON object")

    drift: list[str] = []

    # Presence + type of every field `load_promotion_manifest` requires. The
    # uncovered-field guard at the end of this function only ever caught an
    # *extra*, unrecognized field -- never a required field that was absent
    # or wrongly typed -- so this gate could report clean (drift == []) on a
    # manifest `server/config.py`'s loader rejects outright as
    # `manifest_schema_invalid`: delete `deployed_at_utc`, or set
    # `source_commit` to a non-string, and this function had nothing that
    # would notice (round-5 restart, Architecture finding -- the exact
    # asymmetry the round-4 `confined_evidence_input_path` fix closed for the
    # declared-input-path rule, now closed here for the top-level field
    # roster). Reusing `server.config._MANIFEST_REQUIRED_FIELDS`/
    # `_MANIFEST_STRING_FIELDS` rather than a second hand-copied roster means
    # this check and the runtime loader's own check cannot independently
    # drift.
    missing_required = _MANIFEST_REQUIRED_FIELDS - set(manifest)
    if missing_required:
        drift.append(
            f"manifest is missing required field(s) {sorted(missing_required)} -- "
            "load_promotion_manifest rejects this manifest outright as manifest_schema_invalid"
        )
    wrongly_typed_fields = sorted(
        name
        for name in _MANIFEST_STRING_FIELDS
        if name in manifest and not isinstance(manifest[name], str)
    )
    if wrongly_typed_fields:
        drift.append(
            f"manifest field(s) {wrongly_typed_fields} must be JSON strings -- "
            "load_promotion_manifest rejects this manifest outright as manifest_schema_invalid"
        )

    manifest_phase = manifest.get("manifest_phase")
    if manifest_phase not in ("provisional", "final"):
        drift.append(
            f"manifest_phase {manifest_phase!r} is neither 'provisional' nor 'final'; "
            "the verdict it governs cannot be re-derived"
        )
        manifest_phase = None

    inputs = manifest.get("inputs")
    if not isinstance(inputs, dict) or not inputs:
        raise EvidenceGateError(f"{manifest_path}: manifest declares no inputs")

    # The input set the manifest's own `manifest_phase` implies. `write_manifest`
    # requires phase0/1/2 for either phase and additionally phase3 for `final`;
    # phase3-on-a-provisional and phase4c are both optional extras. Derived
    # from `server.config._MANIFEST_REQUIRED_FINAL_INPUTS` (the same roster
    # `load_promotion_manifest` itself requires a `final` manifest's `inputs`
    # to cover) rather than a hand-copied `{"phase0", "phase1", "phase2"}`
    # literal, so this gate and the runtime consumer it speaks for cannot
    # silently drift apart on which phases a manifest must declare (round-5
    # restart, Architecture finding).
    required_phases = (
        set(_MANIFEST_REQUIRED_FINAL_INPUTS)
        if manifest_phase == "final"
        else set(_MANIFEST_REQUIRED_FINAL_INPUTS) - {"phase3"}
    )

    resolved: dict[str, Path] = {}
    for phase, entry in sorted(inputs.items()):
        if not isinstance(entry, dict):
            drift.append(f"inputs.{phase} is not an object")
            continue
        declared_path = entry.get("path")
        declared_sha = entry.get("sha256")
        if not isinstance(declared_path, str) or not isinstance(declared_sha, str):
            drift.append(f"inputs.{phase} is missing a string path/sha256")
            continue
        # Confined the same way `load_promotion_manifest` confines it -- by
        # calling the shared primitive rather than by re-implementing the rule,
        # which is how the copy that used to live here came to accept absolute
        # in-repo paths the runtime loader rejects outright (round-4 confirm
        # pass, Architecture + Security findings). The path comes from the
        # artifact under scrutiny, so it is not a trustworthy read target on
        # its own.
        candidate = confined_evidence_input_path(declared_path, allowed_root=REPO_ROOT)
        if candidate is None:
            drift.append(
                f"inputs.{phase}.path is absolute or escapes the repo tree: {declared_path!r} "
                "-- load_promotion_manifest refuses it too"
            )
            continue
        try:
            actual_sha = sha256_file(candidate)
        except EvidenceGateError as exc:
            drift.append(f"inputs.{phase} is unreadable: {exc}")
            continue
        if actual_sha != declared_sha:
            # Only the digest prefix is echoed: `declared_path` is
            # manifest-declared and confined to the repo tree but otherwise
            # free, so printing the full digest of whatever it names would make
            # the CI log a general hashing oracle over the tree. A prefix is
            # enough to tell one artifact revision from another, which is all a
            # human debugging drift needs.
            drift.append(
                f"inputs.{phase} digest drift: manifest says {declared_sha}, "
                f"{declared_path} now hashes to {actual_sha[:12]}..."
            )
            continue
        resolved[phase] = candidate

    for phase in sorted(required_phases - set(inputs)):
        drift.append(
            f"inputs.{phase} is missing; a {manifest_phase!r} manifest must declare it "
            "and promotion_eligible cannot be re-derived without it"
        )

    if SCHEMA_PATH.exists():
        actual_schema_hash = sha256_file(SCHEMA_PATH)
        if manifest.get("schema_hash") != actual_schema_hash:
            drift.append(
                f"schema_hash drift: manifest says {manifest.get('schema_hash')}, "
                f"{SCHEMA_PATH.name} now hashes to {actual_schema_hash}"
            )
    else:
        drift.append(f"evidence schema missing, cannot verify schema_hash: {SCHEMA_PATH}")

    config = load_config()
    if manifest.get("release_version") != config.release_version:
        drift.append(
            f"release_version drift: manifest says {manifest.get('release_version')!r}, "
            f"the effective config resolves {config.release_version!r}"
        )
    expected_fingerprint = effective_feature_policy_fingerprint(config)
    if manifest.get("feature_policy_fingerprint") != expected_fingerprint:
        drift.append(
            "feature_policy_fingerprint drift: manifest says "
            f"{manifest.get('feature_policy_fingerprint')!r}, the effective feature policy "
            f"fingerprints to {expected_fingerprint!r}"
        )

    if manifest_phase == "final" and "phase3_completion_hash" not in manifest:
        drift.append("a 'final' manifest must carry phase3_completion_hash")

    # Re-run the gates over the still-matching inputs and re-derive the
    # verdict. `unresolved` collapses "never declared" and "declared but the
    # digest/gate check above rejected it" -- either way the verdict is not
    # re-derivable, and that must be reported, not skipped.
    unresolved = sorted(required_phases - resolved.keys())
    phase3_command_digest_checked = False
    if unresolved:
        drift.append(
            "promotion_eligible/reason could not be re-derived: required input(s) "
            f"{unresolved} are missing or did not resolve"
        )
    elif manifest_phase is None:
        drift.append(
            "promotion_eligible/reason could not be re-derived: manifest_phase is unusable"
        )
    else:
        try:
            phase0_records = validate_artifact("phase0", resolved["phase0"])
            phase1_records = validate_artifact("phase1", resolved["phase1"])
            transport_record = _validate_transport_contract(resolved["phase2"])
            phase3_record = (
                _read_phase3_completion(resolved["phase3"]) if "phase3" in resolved else None
            )
        except EvidenceGateError as exc:
            drift.append(f"a declared input no longer passes its gate: {exc}")
        else:
            real_stratum_present = has_real_provider_stratum(
                [r for r in phase0_records if r["phase"] == "phase0"]
                + [r for r in phase1_records if r["phase"] == "phase1"]
            )
            expected_eligible, expected_reason = _promotion_verdict(
                manifest_phase=manifest_phase,
                real_stratum_present=real_stratum_present,
                transport_eligible=bool(transport_record.get("promotion_eligible")),
            )
            if bool(manifest.get("promotion_eligible")) != expected_eligible:
                drift.append(
                    f"promotion_eligible drift: manifest says "
                    f"{manifest.get('promotion_eligible')!r}, the current evidence supports "
                    f"{expected_eligible!r}"
                )
            if manifest.get("reason") != expected_reason:
                drift.append(
                    f"reason drift: manifest says {manifest.get('reason')!r}, "
                    f"the current evidence supports {expected_reason!r}"
                )
            if phase3_record is not None:
                phase3_command_digest_checked = True
                if (
                    "phase3_command_digest" in manifest
                    and manifest["phase3_command_digest"] != phase3_record["command_digest"]
                ):
                    drift.append(
                        "phase3_command_digest does not match the resolved phase3 artifact's "
                        f"command_digest ({manifest['phase3_command_digest']!r} vs "
                        f"{phase3_record['command_digest']!r})"
                    )

    if "phase3_command_digest" in manifest and not phase3_command_digest_checked:
        drift.append(
            "phase3_command_digest could not be checked: the phase3 completion artifact it "
            "is copied from did not resolve"
        )

    # The digests the manifest repeats at top level must agree with the
    # `inputs` entries they are copied from -- a hand-edited manifest could
    # otherwise pass every check above while its top-level binding pointed at
    # different bytes than the input it claims to summarize.
    for field, phase in (
        ("phase3_completion_hash", "phase3"),
        ("phase4c_artifact_sha256", "phase4c"),
    ):
        if field not in manifest:
            continue
        entry = inputs.get(phase)
        declared = entry.get("sha256") if isinstance(entry, dict) else None
        if manifest[field] != declared:
            drift.append(
                f"{field} does not match inputs.{phase}.sha256 "
                f"({manifest[field]!r} vs {declared!r})"
            )

    # Fail closed on a manifest field nothing above knows how to verify: a
    # future field added to `write_manifest` but not here would otherwise be
    # silently unchecked, which is how this job came to prove nothing in the
    # first place.
    uncovered = set(manifest) - _MANIFEST_VOLATILE_FIELDS - _MANIFEST_VERIFIED_FIELDS
    if uncovered:
        drift.append(
            f"manifest field(s) {sorted(uncovered)} are covered by neither the drift check "
            "nor the documented volatile set -- extend verify_manifest()"
        )

    return drift


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=sorted(PHASE_MINIMUMS))
    parser.add_argument("--input", type=Path)
    parser.add_argument("--write-manifest", action="store_true")
    parser.add_argument(
        "--verify-manifest",
        type=Path,
        help=(
            "Read-only drift check on a committed promotion manifest: re-derive its "
            "evidence bindings and exit non-zero if they no longer hold. Does not write."
        ),
    )
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

    if args.verify_manifest is not None:
        try:
            drift = verify_manifest(args.verify_manifest)
        except (EvidenceGateError, OSError) as exc:
            print(f"FAIL: {exc}", file=sys.stderr)
            return 1
        if drift:
            print(
                f"FAIL: {args.verify_manifest} has drifted from the evidence it binds:",
                file=sys.stderr,
            )
            for item in drift:
                print(f"  - {item}", file=sys.stderr)
            print(
                "  Regenerate it with --write-manifest and commit the result.",
                file=sys.stderr,
            )
            return 1
        print(f"OK: {args.verify_manifest} still matches the evidence it binds")
        return 0

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
            # confine_output_arg (scripts/evidence_common.py): an
            # operator-supplied --output is still attacker-influenced surface
            # (a credentialed run could be invoked with a scripted or
            # copy-pasted value), so it is confined to the repo tree -- and
            # rejected as EvidenceGateError, not a bare ValueError, so this
            # one call folds into the same FAIL/exit-1 gate-error handling as
            # everything else below rather than needing its own earlier
            # try/except block.
            args.output = confine_output_arg(args.output, allowed_root=REPO_ROOT)
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
