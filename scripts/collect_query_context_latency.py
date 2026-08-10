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

from _evidence_common import EvidenceGateError, EvidenceStatus, require_nonempty_str
from run_query_context_experiment import load_fixture, scorer_hash, validate_raw_record

MIN_PAIRED_SAMPLES_PER_CELL = 30
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "query-context-quality-v1.json"


def _now_utc() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _status_record(
    *,
    status: str,
    reason: str,
    record_count: int = 0,
    source_commit: str | None = None,
    source_tree_hash: str | None = None,
) -> dict[str, Any]:
    """Build the deterministic status line written in place of data records.

    Carries the same ``source_commit``/``source_tree_hash`` identity the other
    evidence artifacts record (see ``scripts/record_phase3_completion.py``): a
    blocked or not-run outcome is itself evidence a manifest cites, and
    without the identity binding it could not be tied to the checkout it was
    produced from -- any later run could be passed off as this one. They stay
    ``None`` when the caller supplies no identity, so the field set is stable
    and an unbound record is visibly unbound rather than merely silent.
    """
    return {
        "status": status,
        "reason": reason,
        "promotion_eligible": False,
        "record_count": record_count,
        "source_commit": source_commit,
        "source_tree_hash": source_tree_hash,
        "generated_at_utc": _now_utc(),
    }


class _FixtureIndex:
    """The versioned fixture's own view of what any record may legally claim.

    The collector cannot re-run `score_response`: raw records deliberately
    carry no response text (that is the point of the strict allowlist). What
    it *can* do -- and previously did not -- is refuse to take the record's
    self-reported matches on trust. Recomputing `scorer_hash` from fields
    the record itself supplies proves only internal consistency: an editor
    who invents unknown or duplicate matches and recomputes the hash from
    those fabricated values passes that check. Every match ID is therefore
    resolved against the versioned fixture, and `quality_score` is
    recomputed from the fixture's own denominator.
    """

    def __init__(self, fixture: dict[str, Any]) -> None:
        self.version: str = fixture["fixture_version"]
        self.turns: dict[str, dict[str, Any]] = {t["turn_id"]: t for t in fixture["turns"]}

    def turn_for(self, fixture_turn_id: str) -> dict[str, Any]:
        # The runner pairs repeats as "<turn_id>#<repeat_index>".
        base_id = fixture_turn_id.split("#", 1)[0]
        turn = self.turns.get(base_id)
        if turn is None:
            raise EvidenceGateError(
                f"fixture_turn_id {fixture_turn_id!r} does not resolve to a turn in fixture "
                f"version {self.version!r}"
            )
        return turn


def _check_ids_against_fixture(
    claimed: list[str], known: list[str], *, field: str, line_no: int
) -> None:
    unknown = [value for value in claimed if value not in set(known)]
    if unknown:
        raise EvidenceGateError(
            f"line {line_no}: {field} contains ID(s) {sorted(unknown)} that the versioned "
            "fixture does not declare -- forged or fabricated match"
        )
    if len(claimed) != len(set(claimed)):
        raise EvidenceGateError(f"line {line_no}: {field} contains duplicate IDs")


def _validate_against_fixture(
    record: dict[str, Any], *, index: _FixtureIndex, line_no: int
) -> None:
    if record["fixture_version"] != index.version:
        raise EvidenceGateError(
            f"line {line_no}: fixture_version {record['fixture_version']!r} does not match the "
            f"loaded fixture {index.version!r}"
        )
    turn = index.turn_for(record["fixture_turn_id"])
    _check_ids_against_fixture(
        record["matched_fact_ids"],
        [fact["id"] for fact in turn["required_facts"]],
        field="matched_fact_ids",
        line_no=line_no,
    )
    _check_ids_against_fixture(
        record["matched_citation_ids"],
        [cite["id"] for cite in turn["expected_citations"]],
        field="matched_citation_ids",
        line_no=line_no,
    )
    _check_ids_against_fixture(
        record["matched_disallowed_claim_ids"],
        [claim["id"] for claim in turn["disallowed_claims"]],
        field="matched_disallowed_claim_ids",
        line_no=line_no,
    )
    # A citation is valid only when the fixture-expected fact it maps to was
    # also matched -- the same binding `score_response` applies.
    matched_facts = set(record["matched_fact_ids"])
    for cite in turn["expected_citations"]:
        if cite["id"] not in record["matched_citation_ids"]:
            continue
        expected_fact_id = cite.get("fact_id")
        if expected_fact_id is not None and expected_fact_id not in matched_facts:
            raise EvidenceGateError(
                f"line {line_no}: citation {cite['id']!r} is claimed without its "
                f"fixture-expected fact {expected_fact_id!r}"
            )

    denominator = len(turn["required_facts"]) + len(turn["expected_citations"])
    if denominator == 0:
        raise EvidenceGateError(
            f"line {line_no}: fixture turn {turn['turn_id']!r} has a zero quality-score denominator"
        )
    numerator = (
        len(record["matched_fact_ids"])
        + len(record["matched_citation_ids"])
        - len(record["matched_disallowed_claim_ids"])
    )
    expected_score = max(0.0, min(1.0, numerator / denominator))
    if abs(float(record["quality_score"]) - expected_score) > 1e-9:
        raise EvidenceGateError(
            f"line {line_no}: quality_score {record['quality_score']!r} is not the fixture-derived "
            f"score {expected_score!r} for its matched IDs"
        )


def _validate_raw_record(
    record: dict[str, Any], *, line_no: int, fixture_index: _FixtureIndex
) -> None:
    if "status" in record:
        raise EvidenceGateError(f"line {line_no}: raw records must not carry a 'status' field")
    validate_raw_record(record, where=f"line {line_no}")
    _validate_against_fixture(record, index=fixture_index, line_no=line_no)
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


def load_and_validate_raw(input_path: Path, *, fixture_path: Path) -> list[dict[str, Any]]:
    fixture_index = _FixtureIndex(load_fixture(fixture_path))
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
            _validate_raw_record(record, line_no=line_no, fixture_index=fixture_index)
            records.append(record)
    return records


def normalize(
    records: list[dict[str, Any]],
    *,
    source_commit: str | None = None,
    source_tree_hash: str | None = None,
) -> tuple[str, dict[str, Any] | None, list[dict[str, Any]]]:
    """Return (status, status_record_or_none, data_records).

    ``status`` is ``"ok"`` when every provider/model/condition cell meets
    ``MIN_PAIRED_SAMPLES_PER_CELL``; otherwise ``"undersized"`` and the
    caller must emit a blocked status line instead of the raw data.
    """
    if not records:
        return (
            EvidenceStatus.NOT_RUN.value,
            _status_record(
                status=EvidenceStatus.NOT_RUN.value,
                reason="no_paid_samples",
                source_commit=source_commit,
                source_tree_hash=source_tree_hash,
            ),
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
                source_commit=source_commit,
                source_tree_hash=source_tree_hash,
            )
            | {"undersized_cells": detail},
            [],
        )
    return "ok", None, records


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--fixture",
        type=Path,
        default=DEFAULT_FIXTURE_PATH,
        help=(
            "Versioned quality fixture every record's match IDs and quality_score are "
            "resolved against. Defaults to the committed Phase 4A fixture, so the plan's "
            "documented --input/--output invocation is unchanged."
        ),
    )
    parser.add_argument(
        "--source-commit",
        default=None,
        help="Commit the run was produced from; recorded in a blocked/not-run status line.",
    )
    parser.add_argument(
        "--source-tree-hash",
        default=None,
        help="Tree hash the run was produced from; recorded in a blocked/not-run status line.",
    )
    args = parser.parse_args(argv)

    try:
        if args.source_commit is not None:
            require_nonempty_str(args.source_commit, "--source-commit")
        if args.source_tree_hash is not None:
            require_nonempty_str(args.source_tree_hash, "--source-tree-hash")
        if not args.input.exists():
            _status, status_record, data_records = (
                EvidenceStatus.NOT_RUN.value,
                _status_record(
                    status=EvidenceStatus.NOT_RUN.value,
                    reason="no_paid_samples",
                    source_commit=args.source_commit,
                    source_tree_hash=args.source_tree_hash,
                ),
                [],
            )
        else:
            raw_records = load_and_validate_raw(args.input, fixture_path=args.fixture)
            _status, status_record, data_records = normalize(
                raw_records,
                source_commit=args.source_commit,
                source_tree_hash=args.source_tree_hash,
            )
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
