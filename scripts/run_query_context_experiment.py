#!/usr/bin/env python3
"""Bounded Phase 4A experiment runner for query-context narrowing.

See the "Phase 4: Query-context narrowing experiment" section of
``docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md``.

This runner selects exactly one named history-count condition
(``selected_dimension="history_count"``, baseline value 4 matching
production ``history[-4:]``, narrowed candidate value 2) and injects it only
into experiment runs -- it never changes ``server/workers/web_search.py``'s
production defaults.

Two modes:

``--dry-run``
    Fully credential-free. Synthesizes deterministic seeded responses from
    the versioned fixture (no live provider call), scores them with the
    same scorer used against real data, and writes a single JSON artifact
    (not JSONL) to ``--output`` for schema/shape verification. Always
    available in this environment.

``--live``
    Requires real search-provider credentials (``OPENAI_API_KEY``) and a
    caller-supplied responses client factory; appends strict-allowlist raw
    records to the JSONL ``--output`` path. Missing credentials is a
    blocked outcome, not a fabricated sample: the runner writes nothing and
    exits non-zero with a ``BLOCKED: provider_unavailable`` message so the
    caller does not mistake an empty run for an empty-but-successful one.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from _evidence_common import EvidenceGateError, closed_object, require_type

REPO_ROOT = Path(__file__).resolve().parent.parent

SCORER_VERSION = "v1"
SELECTED_DIMENSION = "history_count"
SELECTABLE_DIMENSIONS = frozenset({"history_count", "answer_chars"})
BASELINE_VALUE = 4  # matches production server/workers/web_search.py history[-4:]
NARROWED_VALUE = 2  # the one named candidate condition this experiment evaluates
DEFAULT_ANSWER_CHAR_LIMIT = 1200  # matches production _contextual_input's per-entry truncation
CONDITIONS = ("baseline", "narrowed")
DEFAULT_BASELINE_REPEATS = 10
DEFAULT_PROVIDER = "openai"
DEFAULT_MODEL = "gpt-4o-search-preview"

RAW_REQUIRED = frozenset(
    {
        "run_id",
        "run_block",
        "run_order",
        "fixture_version",
        "fixture_turn_id",
        "condition",
        "selected_dimension",
        "selected_value",
        "context_chars",
        "query_chars",
        "provider",
        "model",
        "latency_ms",
        "outcome",
        "quality_score",
        "matched_fact_ids",
        "matched_citation_ids",
        "matched_disallowed_claim_ids",
        "scorer_version",
        "scorer_hash",
        "attempt_count",
        "retry_count",
        "rate_limit_count",
        "cache_status",
        "retrieval_snapshot_id",
        "recorded_at_utc",
    }
)
RAW_ALLOWED = RAW_REQUIRED


def scorer_hash(
    fixture_version: str,
    fixture_turn_id: str | None = None,
    *,
    matched_fact_ids: list[str] | None = None,
    matched_citation_ids: list[str] | None = None,
    matched_disallowed_claim_ids: list[str] | None = None,
    quality_score: float | None = None,
) -> str:
    """Provenance hash binding a scorer identity to a fixture (and, when the
    per-record fields are supplied, to one record's exact matched-ID sets and
    quality_score).

    Called two ways: ``scorer_hash(fixture_version)`` is the coarse
    scorer/fixture identity binding -- it changes whenever the scorer version
    or fixture version changes. The fully-parameterized form additionally
    binds one record's sorted matched-ID sets and quality_score, so a caller
    who forges or edits a matched ID after scoring -- without re-running the
    scorer -- changes this hash's expected value, letting
    ``collect_query_context_latency.py`` detect the forgery by recomputing
    and comparing, with no fixture file of its own to consult.
    """
    fixture_turn_id = fixture_turn_id or ""
    matched_fact_ids = matched_fact_ids or []
    matched_citation_ids = matched_citation_ids or []
    matched_disallowed_claim_ids = matched_disallowed_claim_ids or []
    payload = json.dumps(
        {
            "scorer_version": SCORER_VERSION,
            "fixture_version": fixture_version,
            "fixture_turn_id": fixture_turn_id,
            "matched_fact_ids": sorted(matched_fact_ids),
            "matched_citation_ids": sorted(matched_citation_ids),
            "matched_disallowed_claim_ids": sorted(matched_disallowed_claim_ids),
            "quality_score": round(float(quality_score), 6) if quality_score is not None else None,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def load_fixture(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        fixture = json.load(handle)
    if not isinstance(fixture, dict) or "fixture_version" not in fixture or "turns" not in fixture:
        raise EvidenceGateError(f"{path}: fixture must be an object with fixture_version/turns")
    if not fixture["turns"]:
        raise EvidenceGateError(f"{path}: fixture must declare at least one turn")
    return fixture


def score_response(
    turn: dict[str, Any], response_text: str
) -> tuple[float, list[str], list[str], list[str]]:
    """Score a response deterministically against one fixture turn.

    quality_score = (required_facts_present + valid_citations - disallowed_claims)
                    / (required_facts + expected_citations), clamped to [0, 1].
    A fact/citation/claim only counts as matched when its normalized ID's
    match_pattern is found in the response text -- the scorer derives IDs
    from the fixture and the response, never from caller-supplied input.
    """
    text = response_text.lower()
    matched_facts = [
        fact["id"] for fact in turn["required_facts"] if fact["match_pattern"].lower() in text
    ]
    matched_citations = [
        cite["id"] for cite in turn["expected_citations"] if cite["domain"].lower() in text
    ]
    matched_disallowed = [
        claim["id"] for claim in turn["disallowed_claims"] if claim["match_pattern"].lower() in text
    ]
    denominator = len(turn["required_facts"]) + len(turn["expected_citations"])
    if denominator == 0:
        raise EvidenceGateError(f"turn {turn['turn_id']!r} has a zero quality-score denominator")
    numerator = len(matched_facts) + len(matched_citations) - len(matched_disallowed)
    score = max(0.0, min(1.0, numerator / denominator))
    return score, matched_facts, matched_citations, matched_disallowed


def _synthesize_response_text(turn: dict[str, Any], condition: str, rng: random.Random) -> str:
    """Deterministic seeded synthetic response for dry-run scoring only.

    Never used for a promotion decision -- 4B always treats dry-run/synthetic
    input as ineligible; this exists solely to exercise the schema, scorer,
    and runner shape without live-provider credentials.
    """
    parts = [f"Regarding {turn['query']}"]
    for fact in turn["required_facts"]:
        parts.append(fact["match_pattern"])
    for cite in turn["expected_citations"]:
        parts.append(f"see {cite['domain']} for details")
    if condition == "narrowed" and rng.random() < 0.5:
        # Deterministic minor variation so narrowed/baseline cells are not
        # byte-identical, without ever injecting a disallowed claim.
        parts.append("additional context omitted")
    return ". ".join(parts)


def _context_chars_for(
    turn: dict[str, Any], condition: str, *, dimension: str, narrowed_value: int
) -> int:
    history_count = BASELINE_VALUE
    char_limit = DEFAULT_ANSWER_CHAR_LIMIT
    if condition == "narrowed":
        if dimension == "history_count":
            history_count = narrowed_value
        else:
            char_limit = narrowed_value
    prior = turn.get("prior_queries", [])[-history_count:]
    # Mirrors _contextual_input's per-entry answer-truncation budget.
    return sum(min(len(p) + 40, char_limit) for p in prior) if prior else 0


def build_dry_run_artifact(
    fixture: dict[str, Any],
    *,
    seed: int,
    baseline_repeats: int,
    dimension: str = SELECTED_DIMENSION,
    narrowed_value: int = NARROWED_VALUE,
) -> dict[str, Any]:
    if dimension not in SELECTABLE_DIMENSIONS:
        raise EvidenceGateError(
            f"unknown --dimension {dimension!r}; must be one of {sorted(SELECTABLE_DIMENSIONS)}"
        )
    fixture_version = fixture["fixture_version"]
    generated_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    run_id = f"dry-run-{seed}"
    baseline_value = BASELINE_VALUE if dimension == "history_count" else DEFAULT_ANSWER_CHAR_LIMIT

    records: list[dict[str, Any]] = []
    run_order = 0
    for turn in fixture["turns"]:
        # Each repeat gets its own fixture-turn identity (turn_id#repeat_index)
        # so baseline/narrowed records pair 1:1 by identity, per the plan's
        # "paired fixture-turn identity" pairing unit -- a fixed turn scored
        # once is not enough samples to satisfy the per-cell minimum.
        for repeat_index in range(baseline_repeats):
            fixture_turn_id = f"{turn['turn_id']}#{repeat_index}"
            for condition in CONDITIONS:
                rng = random.Random(f"{seed}:{turn['turn_id']}:{condition}:{repeat_index}")
                response_text = _synthesize_response_text(turn, condition, rng)
                quality_score, facts, cites, disallowed = score_response(turn, response_text)
                selected_value = baseline_value if condition == "baseline" else narrowed_value
                context_chars = _context_chars_for(
                    turn, condition, dimension=dimension, narrowed_value=narrowed_value
                )
                query_chars = len(turn["query"])
                # Deterministic synthetic latency: proportional to context size
                # plus small seeded jitter, purely to exercise the analyzer's
                # Spearman/bootstrap machinery -- never treated as real evidence.
                base_latency = 400.0 + 0.05 * context_chars
                jitter = rng.uniform(-15.0, 15.0)
                latency_ms = round(base_latency + jitter, 3)
                scorer_digest = scorer_hash(
                    fixture_version,
                    fixture_turn_id,
                    matched_fact_ids=facts,
                    matched_citation_ids=cites,
                    matched_disallowed_claim_ids=disallowed,
                    quality_score=quality_score,
                )
                record = {
                    "run_id": run_id,
                    "run_block": run_order // len(CONDITIONS),
                    "run_order": run_order,
                    "fixture_version": fixture_version,
                    "fixture_turn_id": fixture_turn_id,
                    "condition": condition,
                    "selected_dimension": dimension,
                    "selected_value": selected_value,
                    "context_chars": context_chars,
                    "query_chars": query_chars,
                    "provider": "synthetic",
                    "model": "dry-run-fixture",
                    "latency_ms": latency_ms,
                    "outcome": "success",
                    "quality_score": quality_score,
                    "matched_fact_ids": facts,
                    "matched_citation_ids": cites,
                    "matched_disallowed_claim_ids": disallowed,
                    "scorer_version": SCORER_VERSION,
                    "scorer_hash": scorer_digest,
                    "attempt_count": 1,
                    "retry_count": 0,
                    "rate_limit_count": 0,
                    "cache_status": "unknown",
                    "retrieval_snapshot_id": None,
                    "recorded_at_utc": generated_at,
                }
                closed_object(record, required=RAW_REQUIRED, allowed=RAW_ALLOWED)
                records.append(record)
                run_order += 1

    return {
        "status": "dry_run",
        "run_id": run_id,
        "fixture_version": fixture_version,
        "selected_dimension": dimension,
        "conditions": {
            "baseline": {"selected_value": baseline_value},
            "narrowed": {"selected_value": narrowed_value},
        },
        "scorer_version": SCORER_VERSION,
        "record_count": len(records),
        "generated_at_utc": generated_at,
        "records": records,
    }


def run_live(*, output: Path) -> int:
    if not os.environ.get("OPENAI_API_KEY"):
        print(
            "BLOCKED: provider_unavailable -- OPENAI_API_KEY is not set; writing no "
            f"fabricated records to {output}",
            file=sys.stderr,
        )
        return 1
    print(
        "BLOCKED: live query-context collection is not wired to a responses client "
        "in this runner; supply credentials and extend run_live() before use.",
        file=sys.stderr,
    )
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--live", action="store_true")
    parser.add_argument("--fixture", type=Path, required=False)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--baseline-repeats", type=int, default=DEFAULT_BASELINE_REPEATS)
    parser.add_argument("--dimension", type=str, default=SELECTED_DIMENSION)
    parser.add_argument("--value", type=int, action="append", default=None)
    args = parser.parse_args(argv)

    if args.live:
        return run_live(output=args.output)

    if args.fixture is None:
        print("FAIL: --fixture is required for --dry-run", file=sys.stderr)
        return 1

    try:
        if args.dimension not in SELECTABLE_DIMENSIONS:
            raise EvidenceGateError(
                f"unknown --dimension {args.dimension!r}; must be one of "
                f"{sorted(SELECTABLE_DIMENSIONS)}"
            )
        values = args.value if args.value is not None else [NARROWED_VALUE]
        if len(values) != 1:
            raise EvidenceGateError(
                f"exactly one --value must be selected per named condition, got {len(values)}"
            )
        fixture = load_fixture(args.fixture)
        require_type(args.baseline_repeats, (int,), "--baseline-repeats")
        if args.baseline_repeats < 1:
            raise EvidenceGateError("--baseline-repeats must be >= 1")
        artifact = build_dry_run_artifact(
            fixture,
            seed=args.seed,
            baseline_repeats=args.baseline_repeats,
            dimension=args.dimension,
            narrowed_value=values[0],
        )
    except (EvidenceGateError, OSError, json.JSONDecodeError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"OK: wrote {len(artifact['records'])} dry-run record(s) to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
