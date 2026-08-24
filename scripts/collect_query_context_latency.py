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
from pathlib import Path
from typing import Any

from scripts.evidence_common import (
    MIN_PAIRED_SAMPLES_PER_CELL,
    REPO_ROOT,
    EvidenceGateError,
    EvidenceStatus,
    FixtureIndex,
    confine_output_arg,
    load_jsonl,
    now_utc,
    require_nonempty_str,
    sha256_file,
    write_bytes_no_follow,
)
from scripts.query_context_common import (
    DEFAULT_FIXTURE_PATH,
    load_fixture,
    validate_scored_record,
)


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
        "generated_at_utc": now_utc(),
    }


def _cell_key(record: dict[str, Any]) -> tuple[str, str, str]:
    return (record["provider"], record["model"], record["condition"])


def load_and_validate_raw(input_path: Path, *, fixture_path: Path) -> list[dict[str, Any]]:
    fixture_index = FixtureIndex(load_fixture(fixture_path))

    def _validate(line_no: int, record: dict[str, Any]) -> None:
        # query_context_common.validate_scored_record (round-5 restart,
        # Architecture finding): the same shape -> scorer_hash re-derivation
        # -> fixture cross-check gate the analyzer applies, with
        # reject_status_field=True reproducing this script's stricter input
        # contract -- a raw record must never carry a 'status' field, which
        # belongs to the whole-file status-line records this gate never sees.
        validate_scored_record(
            record,
            index=fixture_index,
            where=f"line {line_no}",
            reject_status_field=True,
        )

    return load_jsonl(input_path, validate_record=_validate)


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
        # confine_output_arg (scripts/evidence_common.py): an operator-supplied
        # --output is still attacker-influenced surface (a credentialed run
        # could be invoked with a scripted or copy-pasted value), so it is
        # confined to the repo tree -- and rejected as EvidenceGateError, not
        # a bare ValueError, so this one call folds into the same FAIL/exit-1
        # gate-error handling as everything else below rather than needing
        # its own earlier try/except block.
        args.output = confine_output_arg(args.output, allowed_root=REPO_ROOT)
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
            if data_records:
                # Bind each emitted record to the exact fixture bytes its
                # matched IDs/quality_score were resolved against, not just
                # the fixture's self-declared version string, so downstream
                # Phase 4C binding can eventually tie the artifact to those
                # bytes.
                fixture_digest = sha256_file(args.fixture)
                data_records = [
                    {**record, "fixture_sha256": fixture_digest} for record in data_records
                ]

        args.output.parent.mkdir(parents=True, exist_ok=True)
        lines: list[str] = []
        if status_record is not None:
            lines.append(json.dumps(status_record, sort_keys=True) + "\n")
        for record in data_records:
            lines.append(json.dumps(record, sort_keys=True) + "\n")
        # write_bytes_no_follow (scripts/evidence_common.py): this
        # predictable, repo-relative output path gets the same symlink/FIFO
        # hardening as the promotion-manifest writer, instead of a plain
        # open("w") that would follow a planted symlink. Kept inside this
        # try block, not after it, so a symlinked --output fails closed with
        # the same FAIL/exit-1 contract as every other gate error here,
        # instead of an uncaught traceback.
        write_bytes_no_follow(args.output, "".join(lines).encode("utf-8"))
    except (EvidenceGateError, OSError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    if status_record is not None:
        print(
            f"OK: wrote status={status_record['status']} reason={status_record['reason']} to {args.output}"
        )
    else:
        print(f"OK: wrote {len(data_records)} normalized record(s) to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
