#!/usr/bin/env python3
"""Phase 4A collector: transform raw query-context samples into the
normalized artifact Phase 4B's analyzer consumes.

See the "Phase 4: Query-context narrowing experiment" section of
``docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md``.

Input is the strict-allowlist raw JSONL produced by
``scripts/run_query_context_experiment.py`` in live mode
(``docs/benchmarks/v0.1.3-query-context-raw.jsonl``; never fabricated by this
script). Output is ``docs/benchmarks/v0.1.3-query-context.jsonl``: either the
validated, normalized data records, or -- when the required paid input does
not exist or is undersized -- a single deterministic status line so the
named artifact path is never merely absent. This script never fabricates
samples; a missing or undersized raw capture is recorded as a status, not
invented data.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from _evidence_common import EvidenceGateError, EvidenceStatus, closed_object, require_type
from run_query_context_experiment import CONDITIONS, RAW_ALLOWED, RAW_REQUIRED, scorer_hash

MIN_PAIRED_SAMPLES_PER_CELL = 30


def _now_utc() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _status_record(*, status: str, reason: str, record_count: int = 0) -> dict[str, Any]:
    return {
        "status": status,
        "reason": reason,
        "promotion_eligible": False,
        "record_count": record_count,
        "generated_at_utc": _now_utc(),
    }


def _validate_raw_record(record: dict[str, Any], *, line_no: int) -> None:
    if "status" in record:
        raise EvidenceGateError(f"line {line_no}: raw records must not carry a 'status' field")
    closed_object(record, required=RAW_REQUIRED, allowed=RAW_ALLOWED)
    if record["condition"] not in CONDITIONS:
        raise EvidenceGateError(f"line {line_no}: invalid condition {record['condition']!r}")
    if record["outcome"] not in {"success", "error", "timeout"}:
        raise EvidenceGateError(f"line {line_no}: invalid outcome {record['outcome']!r}")
    require_type(record["quality_score"], (int, float), "quality_score")
    if not (0.0 <= float(record["quality_score"]) <= 1.0):
        raise EvidenceGateError(f"line {line_no}: quality_score must be within [0, 1]")
    for field in ("matched_fact_ids", "matched_citation_ids", "matched_disallowed_claim_ids"):
        require_type(record[field], (list,), field)
    expected_hash = scorer_hash(
        record["fixture_version"],
        record["fixture_turn_id"],
        matched_fact_ids=record["matched_fact_ids"],
        matched_citation_ids=record["matched_citation_ids"],
        matched_disallowed_claim_ids=record["matched_disallowed_claim_ids"],
        quality_score=record["quality_score"],
    )
    if record["scorer_hash"] != expected_hash:
        raise EvidenceGateError(
            f"line {line_no}: scorer_hash does not match its record's matched IDs/quality_score "
            "-- forged, edited, or mismatched scorer provenance"
        )


def _cell_key(record: dict[str, Any]) -> tuple[str, str, str]:
    return (record["provider"], record["model"], record["condition"])


def load_and_validate_raw(input_path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with input_path.open("r", encoding="utf-8") as handle:
        for line_no, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise EvidenceGateError(f"line {line_no}: invalid JSON ({exc})") from exc
            if not isinstance(record, dict):
                raise EvidenceGateError(f"line {line_no}: expected a JSON object")
            _validate_raw_record(record, line_no=line_no)
            records.append(record)
    return records


def normalize(
    records: list[dict[str, Any]],
) -> tuple[str, dict[str, Any] | None, list[dict[str, Any]]]:
    """Return (status, status_record_or_none, data_records).

    ``status`` is ``"ok"`` when every provider/model/condition cell meets
    ``MIN_PAIRED_SAMPLES_PER_CELL``; otherwise ``"undersized"`` and the
    caller must emit a blocked status line instead of the raw data.
    """
    if not records:
        return (
            EvidenceStatus.NOT_RUN.value,
            _status_record(status=EvidenceStatus.NOT_RUN.value, reason="no_paid_samples"),
            [],
        )

    counts: Counter[tuple[str, str, str]] = Counter(_cell_key(r) for r in records)
    undersized = {key: n for key, n in counts.items() if n < MIN_PAIRED_SAMPLES_PER_CELL}
    if undersized:
        detail = ", ".join(f"{key}={n}" for key, n in sorted(undersized.items()))
        return (
            EvidenceStatus.BLOCKED.value,
            _status_record(
                status=EvidenceStatus.BLOCKED.value,
                reason="undersized_cell",
                record_count=len(records),
            )
            | {"undersized_cells": detail},
            [],
        )
    return "ok", None, records


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    try:
        if not args.input.exists():
            _status, status_record, data_records = (
                EvidenceStatus.NOT_RUN.value,
                _status_record(status=EvidenceStatus.NOT_RUN.value, reason="no_paid_samples"),
                [],
            )
        else:
            raw_records = load_and_validate_raw(args.input)
            _status, status_record, data_records = normalize(raw_records)
    except (EvidenceGateError, OSError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        if status_record is not None:
            handle.write(json.dumps(status_record, sort_keys=True) + "\n")
        for record in data_records:
            handle.write(json.dumps(record, sort_keys=True) + "\n")

    if status_record is not None:
        print(
            f"OK: wrote status={status_record['status']} reason={status_record['reason']} to {args.output}"
        )
    else:
        print(f"OK: wrote {len(data_records)} normalized record(s) to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
