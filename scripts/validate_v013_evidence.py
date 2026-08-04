#!/usr/bin/env python3
"""Validate a v0.1.3 evidence JSONL artifact against its schema and phase minimums.

Each line of ``--input`` must be one JSON object matching
``shared/schemas/v013-evidence.json``. This module hand-checks the schema's
required fields, types, and enum vocabularies directly (no ``jsonschema``
dependency) and then enforces phase-specific coverage minimums: one record
per named scenario, one record per outcome exercised by that phase's fixture
matrix, and one record per provider/model stratum (the credential-free
stratum is ``unavailable``/``unavailable``).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = REPO_ROOT / "shared" / "schemas" / "v013-evidence.json"

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
ALL_FIELDS = frozenset(REQUIRED_FIELDS) | frozenset(OPTIONAL_FIELDS)

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


def load_records(input_path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with input_path.open("r", encoding="utf-8") as handle:
        for line_no, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise EvidenceValidationError(f"line {line_no}: invalid JSON ({exc})") from exc
            if not isinstance(record, dict):
                raise EvidenceValidationError(f"line {line_no}: expected a JSON object")
            records.append(record)
    return records


def check_phase_minimums(phase: str, records: list[Mapping[str, Any]]) -> None:
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


def validate_artifact(phase: str, input_path: Path) -> list[dict[str, Any]]:
    records = load_records(input_path)
    for index, record in enumerate(records):
        validate_record(record, index)
    check_phase_minimums(phase, records)
    return records


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True, choices=sorted(PHASE_MINIMUMS))
    parser.add_argument("--input", required=True, type=Path)
    args = parser.parse_args(argv)

    try:
        records = validate_artifact(args.phase, args.input)
    except EvidenceValidationError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    print(f"OK: {len(records)} record(s) validated for phase={args.phase}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
