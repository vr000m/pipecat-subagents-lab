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
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from _evidence_common import EvidenceGateError, load_json, load_jsonl, sha256_file

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
# "two records each for the queued-discard and admitted-completion race
# scenarios" -- a minimum *count*, not just presence, so it is checked
# separately from PHASE_MINIMUMS' presence-only sets.
PHASE_SCENARIO_MIN_COUNT: Mapping[str, Mapping[str, int]] = {
    "phase1": {"queued_discard": 2, "admitted_completion": 2},
}


class EvidenceValidationError(ValueError):
    """Raised when a record or artifact fails the evidence contract."""


def _require_type(value: Any, kinds: tuple[type, ...], field: str, index: int) -> None:
    if isinstance(value, bool) and bool not in kinds:
        raise EvidenceValidationError(f"record {index}: {field} must not be a bool, got {value!r}")
    if not isinstance(value, kinds):
        raise EvidenceValidationError(
            f"record {index}: {field} expected {kinds}, got {type(value).__name__}"
        )


def validate_record(record: Mapping[str, Any], index: int) -> None:
    unknown = set(record) - ALL_FIELDS
    if unknown:
        raise EvidenceValidationError(f"record {index}: unknown field(s) {sorted(unknown)}")
    missing = [name for name in REQUIRED_FIELDS if name not in record]
    if missing:
        raise EvidenceValidationError(f"record {index}: missing required field(s) {missing}")

    if record["phase"] not in PHASES:
        raise EvidenceValidationError(f"record {index}: invalid phase {record['phase']!r}")

    phase_extra = PHASE_EXTRA_REQUIRED_FIELDS.get(record["phase"], ())
    missing_extra = [name for name in phase_extra if name not in record]
    if missing_extra:
        raise EvidenceValidationError(
            f"record {index}: phase={record['phase']!r} missing required field(s) {missing_extra}"
        )

    for field in ("scenario", "turn_id", "provider", "model", "run_id"):
        _require_type(record[field], (str,), field, index)
        if not record[field]:
            raise EvidenceValidationError(f"record {index}: {field} must be non-empty")
    if record["outcome"] not in OUTCOMES:
        raise EvidenceValidationError(f"record {index}: invalid outcome {record['outcome']!r}")
    if record["disposition"] not in DISPOSITIONS:
        raise EvidenceValidationError(
            f"record {index}: invalid disposition {record['disposition']!r}"
        )

    latency = record["routing_phase_latency_ms"]
    if latency is not None:
        _require_type(latency, (int, float), "routing_phase_latency_ms", index)
        if latency < 0:
            raise EvidenceValidationError(f"record {index}: routing_phase_latency_ms must be >= 0")

    _require_type(record["sample_timestamp_ms"], (int,), "sample_timestamp_ms", index)
    if record["sample_timestamp_ms"] < 0:
        raise EvidenceValidationError(f"record {index}: sample_timestamp_ms must be >= 0")
    _require_type(record["sample_index"], (int,), "sample_index", index)
    if record["sample_index"] < 0:
        raise EvidenceValidationError(f"record {index}: sample_index must be >= 0")
    _require_type(record["sample_count"], (int,), "sample_count", index)
    if record["sample_count"] < 1:
        raise EvidenceValidationError(f"record {index}: sample_count must be >= 1")

    if "work_item_id" in record and record["work_item_id"] is not None:
        _require_type(record["work_item_id"], (str,), "work_item_id", index)
        if not record["work_item_id"]:
            raise EvidenceValidationError(f"record {index}: work_item_id must be non-empty")
    for field in ("query_chars", "context_chars"):
        if field in record and record[field] is not None:
            _require_type(record[field], (int,), field, index)
            if record[field] < 0:
                raise EvidenceValidationError(f"record {index}: {field} must be >= 0")
    if "wall_clock_time" in record and record["wall_clock_time"] is not None:
        _require_type(record["wall_clock_time"], (str,), "wall_clock_time", index)

    if "ack_id" in record and record["ack_id"] is not None:
        _require_type(record["ack_id"], (str,), "ack_id", index)
        if not record["ack_id"]:
            raise EvidenceValidationError(f"record {index}: ack_id must be non-empty")
    if "ack_enqueued" in record and record["ack_enqueued"] is not None:
        _require_type(record["ack_enqueued"], (bool,), "ack_enqueued", index)
    if "ack_enqueued_ms" in record and record["ack_enqueued_ms"] is not None:
        _require_type(record["ack_enqueued_ms"], (int,), "ack_enqueued_ms", index)
        if record["ack_enqueued_ms"] < 0:
            raise EvidenceValidationError(f"record {index}: ack_enqueued_ms must be >= 0")
    if (
        "ack_terminal_state" in record
        and record["ack_terminal_state"] is not None
        and record["ack_terminal_state"] not in ACK_TERMINAL_STATES
    ):
        raise EvidenceValidationError(
            f"record {index}: invalid ack_terminal_state {record['ack_terminal_state']!r}"
        )


def load_records(input_path: Path) -> list[dict[str, Any]]:
    try:
        return load_jsonl(input_path)
    except EvidenceGateError as exc:
        raise EvidenceValidationError(str(exc)) from exc


def check_phase_minimums(phase: str, records: Sequence[Mapping[str, Any]]) -> None:
    minimums = PHASE_MINIMUMS.get(phase)
    if minimums is None:
        raise EvidenceValidationError(f"no coverage minimums declared for phase {phase!r}")

    phase_records = [r for r in records if r["phase"] == phase]
    if not phase_records:
        raise EvidenceValidationError(f"no records found for phase {phase!r}")

    seen_scenarios = {r["scenario"] for r in phase_records}
    missing_scenarios = minimums["scenarios"] - seen_scenarios
    if missing_scenarios:
        raise EvidenceValidationError(
            f"phase {phase!r}: missing scenario coverage for {sorted(missing_scenarios)}"
        )

    seen_outcomes = {r["outcome"] for r in phase_records}
    missing_outcomes = minimums["outcomes"] - seen_outcomes
    if missing_outcomes:
        raise EvidenceValidationError(
            f"phase {phase!r}: missing outcome coverage for {sorted(missing_outcomes)}"
        )

    seen_strata = {(r["provider"], r["model"]) for r in phase_records}
    missing_strata = minimums["provider_model_strata"] - seen_strata
    if missing_strata:
        raise EvidenceValidationError(
            f"phase {phase!r}: missing provider/model stratum coverage for {sorted(missing_strata)}"
        )

    min_counts = PHASE_SCENARIO_MIN_COUNT.get(phase, {})
    for scenario, minimum in min_counts.items():
        count = sum(1 for r in phase_records if r["scenario"] == scenario)
        if count < minimum:
            raise EvidenceValidationError(
                f"phase {phase!r}: scenario {scenario!r} requires >= {minimum} record(s), got {count}"
            )


def validate_artifact(phase: str, input_path: Path) -> list[dict[str, Any]]:
    records = load_records(input_path)
    for index, record in enumerate(records):
        validate_record(record, index)
    check_phase_minimums(phase, records)
    return records


def has_real_provider_stratum(records: list[Mapping[str, Any]]) -> bool:
    """True if any record uses a non-credential-free provider/model stratum.

    The credential-free ``unavailable``/``unavailable`` stratum can validate
    schema/lifecycle correctness but can never make a manifest
    promotion-eligible; this is the check that enforces that split.
    """
    return any(r["provider"] != "unavailable" or r["model"] != "unavailable" for r in records)


def _validate_transport_contract(input_path: Path) -> dict[str, Any]:
    """Revalidate the Phase 2 transport/browser contract artifact.

    Delegates to `validate_phase2_transport_browser_contract.py` (loaded by
    path, since `scripts/` is not a package) so this writer never
    re-implements or drifts from that gate's own logic.
    """
    spec_path = Path(__file__).resolve().parent / "validate_phase2_transport_browser_contract.py"
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "validate_phase2_transport_browser_contract", spec_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    record = load_json(input_path)
    if not isinstance(record, dict):
        raise EvidenceGateError("transport contract artifact must be a JSON object")
    module.validate_artifact(record)
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
        "phase4b_baseline_input_hash",
        "phase4b_normalized_input_hash",
        "experiment_command_digest",
        "analyzer_command_digest",
        "scorer_hash",
        "control_fingerprint",
    }
)
PHASE4C_STRING_FIELDS = frozenset({"fixture_version", "scorer_version", "generated_at_utc"})
PHASE4C_ALLOWED_FIELDS = (
    frozenset({"status", "promotion_eligible"}) | PHASE4C_HASH_LIKE_FIELDS | PHASE4C_STRING_FIELDS
)
_HEX_RE = re.compile(r"^[0-9a-f]+$")


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
        if not isinstance(value, str) or not value or not _HEX_RE.match(value):
            raise EvidenceGateError(
                f"phase4c artifact: {name} must be a non-empty lowercase hex string"
            )
    for name in PHASE4C_STRING_FIELDS:
        value = payload[name]
        if not isinstance(value, str) or not value:
            raise EvidenceGateError(f"phase4c artifact: {name} must be a non-empty string")


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
    phase3_record = (
        _validate_phase3_completion(
            phase3_input, source_commit=source_commit, source_tree_hash=source_tree_hash
        )
        if phase3_input is not None
        else None
    )

    inputs: dict[str, dict[str, str]] = {
        "phase0": {"path": str(phase0_input), "sha256": sha256_file(phase0_input)},
        "phase1": {"path": str(phase1_input), "sha256": sha256_file(phase1_input)},
        "phase2": {"path": str(phase2_input), "sha256": sha256_file(phase2_input)},
    }
    if phase3_input is not None:
        inputs["phase3"] = {"path": str(phase3_input), "sha256": sha256_file(phase3_input)}

    phase4c_artifact_sha256: str | None = None
    if phase4c_input is not None:
        phase4c_payload = load_json(phase4c_input)
        _validate_phase4c_artifact(phase4c_payload)
        phase4c_artifact_sha256 = sha256_file(phase4c_input)
        inputs["phase4c"] = {"path": str(phase4c_input), "sha256": phase4c_artifact_sha256}

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

    schema_hash = sha256_file(SCHEMA_PATH) if SCHEMA_PATH.exists() else ""
    manifest = {
        "manifest_phase": manifest_phase,
        "promotion_eligible": promotion_eligible,
        "reason": reason,
        "schema_hash": schema_hash,
        "source_commit": source_commit,
        "source_tree_hash": source_tree_hash,
        "release_version": "0.1.3",
        "feature_policy_fingerprint": feature_policy_fingerprint,
        "deployed_at_utc": deployed_at_utc,
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "inputs": inputs,
    }
    if phase3_input is not None:
        manifest["phase3_completion_hash"] = inputs["phase3"]["sha256"]
    if phase4c_artifact_sha256 is not None:
        manifest["phase4c_artifact_sha256"] = phase4c_artifact_sha256

    _atomic_write_manifest(output, manifest)
    return manifest


def _atomic_write_manifest(output: Path, manifest: dict[str, Any]) -> None:
    """Durable atomic-rename write with fsync, preserving a `.previous` copy.

    Writes a same-directory temp file, fsyncs it, atomically renames it over
    the destination, then fsyncs the parent directory. A pre-existing
    manifest is preserved as `<output>.previous` until the rename is
    durable; on any failure before the rename completes, the previous
    manifest (if any) is left untouched.
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    previous_path = output.with_suffix(output.suffix + ".previous")
    if output.exists():
        previous_path.write_bytes(output.read_bytes())

    tmp_path = output.with_suffix(output.suffix + ".tmp")
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp_path, output)
    dir_fd = os.open(str(output.parent), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


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
        except (EvidenceValidationError, EvidenceGateError, OSError) as exc:
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
    except EvidenceValidationError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    print(f"OK: {len(records)} record(s) validated for phase={args.phase}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
