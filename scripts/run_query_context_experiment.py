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

Control-field source audit (Phase 4A, required before freezing
``shared/schemas/v013-query-context-raw.json``). For each control field, where
the value comes from:

``cache_status``
    Client-side collector bookkeeping. The OpenAI Responses API exposes no
    per-request search-cache indicator, so no provider response metadata can
    populate this. The dry run hardcodes ``"unknown"``; a live collection can
    only ever set ``hit``/``miss`` from a collector-owned cache it operates
    itself. Treat a non-``unknown`` value as a claim about the *collector's*
    cache, never the provider's.
``retrieval_snapshot_id``
    Client-side collector bookkeeping. There is no provider-returned
    retrieval/corpus snapshot identifier on the Responses API, so this is an
    operator-supplied label identifying the retrieval corpus state a batch was
    collected against. The dry run leaves it ``None``. Because it is
    collector-declared, the analyzer must not treat its presence as evidence
    of a real provider call -- that is what the ``SYNTHETIC_STRATA``
    provider/model gate is for.
``attempt_count`` / ``retry_count``
    Client-side collector bookkeeping. Counted by the collector's own retry
    loop; the provider reports nothing equivalent.
``rate_limit_count``
    Client-side collector bookkeeping, *derived from* provider signals: the
    collector increments it when a request returns HTTP 429. The count is the
    collector's; the trigger is the provider's.

All five are therefore collector-owned. None is provider response metadata,
so none is independently attestable, which is precisely why ``scorer_hash``
binds the whole record (see :func:`scorer_hash`) rather than trusting these
fields to be self-describing.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.evidence_common import (
    REPO_ROOT,
    EvidenceGateError,
    closed_object,
    confined_output_path,
    require_type,
    write_bytes_no_follow,
)
from scripts.query_context_common import (
    CACHE_STATUSES,
    CONDITIONS,
    OUTCOMES,
    RAW_ALLOWED,
    RAW_REQUIRED,
    SCORER_VERSION,
    SELECTABLE_DIMENSIONS,
    load_fixture,
    scorer_hash,
    validate_raw_record,
)

# Re-exported for existing callers (in this module and in tests reaching them
# via ``scripts.run_query_context_experiment``) that pull these names from
# this module rather than from ``scripts.query_context_common`` directly --
# see ``scripts/query_context_common.py``'s module docstring for why they
# moved. Listed explicitly so ruff's unused-import check does not flag names
# this module re-exports but does not itself reference.
__all__ = [
    "CACHE_STATUSES",
    "CONDITIONS",
    "OUTCOMES",
    "RAW_ALLOWED",
    "RAW_REQUIRED",
    "SCORER_VERSION",
    "SELECTABLE_DIMENSIONS",
    "load_fixture",
    "main",
    "scorer_hash",
    "validate_raw_record",
]

SELECTED_DIMENSION = "history_count"
BASELINE_VALUE = 4  # matches production server/workers/web_search.py history[-4:]
NARROWED_VALUE = 2  # the one named candidate condition this experiment evaluates
DEFAULT_ANSWER_CHAR_LIMIT = 1200  # matches production _contextual_input's per-entry truncation
NARROWED_ANSWER_CHARS = 600  # the answer_chars candidate condition: half the production limit

#: The narrowed value ``--value`` defaults to, *per dimension* (round-3
#: restart gauntlet, Logic finding). The default used to be ``NARROWED_VALUE``
#: unconditionally -- but that constant is a *history turn count*. Running
#: ``--dimension answer_chars`` without an explicit ``--value`` therefore swept
#: an answer budget of 2 characters against the 1200-character baseline, and
#: ``build_dry_run_artifact`` baked that 2 into the artifact as the narrowed
#: answer_chars value. The run completed and emitted a well-formed artifact,
#: so the wrong-dimension default was entirely silent. Keyed by dimension so a
#: new dimension cannot inherit another dimension's units by omission --
#: ``_default_narrowed_value`` raises for a dimension missing from this map.
NARROWED_VALUE_BY_DIMENSION = {
    "history_count": NARROWED_VALUE,
    "answer_chars": NARROWED_ANSWER_CHARS,
}
DEFAULT_BASELINE_REPEATS = 10
DEFAULT_PROVIDER = "openai"
DEFAULT_MODEL = "gpt-4o-search-preview"


def _default_narrowed_value(dimension: str) -> int:
    """The ``--value`` default for `dimension`, in that dimension's own units.

    Raises rather than falling back, so a dimension added to
    ``SELECTABLE_DIMENSIONS`` without a narrowed default here fails loudly
    instead of silently inheriting another dimension's units -- the exact
    failure this map replaced.
    """
    try:
        return NARROWED_VALUE_BY_DIMENSION[dimension]
    except KeyError:
        raise EvidenceGateError(
            f"--dimension {dimension!r} has no narrowed default; pass --value explicitly"
        ) from None


def score_response(
    turn: dict[str, Any], response_text: str
) -> tuple[float, list[str], list[str], list[str]]:
    """Score a response deterministically against one fixture turn.

    quality_score = (required_facts_present + valid_citations - disallowed_claims)
                    / (required_facts + expected_citations), clamped to [0, 1].
    A fact/citation/claim only counts as matched when its normalized ID's
    match_pattern is found in the response text -- the scorer derives IDs
    from the fixture and the response, never from caller-supplied input.

    A citation is *valid*, not merely mentioned: the plan requires "its
    canonical URL/domain and fixture-expected fact mapping match". Domain
    presence alone credited a response that name-dropped e.g. "github.com"
    without supporting the fact the fixture maps that citation to, so the
    citation's `fact_id` must also be among the matched facts, and its
    canonical `url` (when the fixture pins one) must appear in the response.
    """
    text = response_text.lower()
    matched_facts = [
        fact["id"] for fact in turn["required_facts"] if fact["match_pattern"].lower() in text
    ]
    matched_fact_ids = set(matched_facts)
    matched_citations = []
    for cite in turn["expected_citations"]:
        if cite["domain"].lower() not in text:
            continue
        canonical_url = cite.get("url")
        if canonical_url and canonical_url.lower() not in text:
            continue
        expected_fact_id = cite.get("fact_id")
        if expected_fact_id is not None and expected_fact_id not in matched_fact_ids:
            continue
        matched_citations.append(cite["id"])
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
    history = turn.get("prior_queries", [])
    # `history[-0:]` is the FULL list, not an empty one -- so a schema-valid
    # `--value 0` "zero history" condition silently included every prior
    # query. Zero has to be handled before the slice.
    prior = history[-history_count:] if history_count > 0 else []
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
    condition_orders: list[dict[str, Any]] = []
    run_order = 0
    for turn in fixture["turns"]:
        # Each repeat gets its own fixture-turn identity (turn_id#repeat_index)
        # so baseline/narrowed records pair 1:1 by identity, per the plan's
        # "paired fixture-turn identity" pairing unit -- a fixed turn scored
        # once is not enough samples to satisfy the per-cell minimum.
        for repeat_index in range(baseline_repeats):
            fixture_turn_id = f"{turn['turn_id']}#{repeat_index}"
            # Assumption (d): run order must not be confounded with condition.
            # Iterating CONDITIONS in fixed baseline-then-narrowed order meant
            # every narrowed sample ran second within its block, so any
            # provider/cache/time drift loaded entirely onto the narrowed arm.
            # The order is shuffled from the run seed (so it stays exactly
            # reproducible) and recorded in the artifact.
            order_rng = random.Random(f"{seed}:order:{turn['turn_id']}:{repeat_index}")
            block_conditions = list(CONDITIONS)
            order_rng.shuffle(block_conditions)
            condition_orders.append(
                {
                    "run_block": run_order // len(CONDITIONS),
                    "fixture_turn_id": fixture_turn_id,
                    "order": block_conditions,
                }
            )
            for condition in block_conditions:
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
                    "attempt_count": 1,
                    "retry_count": 0,
                    "rate_limit_count": 0,
                    "cache_status": "unknown",
                    "retrieval_snapshot_id": None,
                    "recorded_at_utc": generated_at,
                }
                # Stamped after the record is fully built: the digest binds
                # every measurement and stratum-identity field, not just the
                # scoring fields, so `scorer_hash` cannot be present in the
                # payload it authenticates.
                record["scorer_hash"] = scorer_hash(
                    fixture_version,
                    fixture_turn_id,
                    matched_fact_ids=facts,
                    matched_citation_ids=cites,
                    matched_disallowed_claim_ids=disallowed,
                    quality_score=quality_score,
                    record=record,
                )
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
        "seed": seed,
        "condition_orders": condition_orders,
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

    # Sibling eval scripts (eval_model_comparison.py, verify_eval_candidates.py)
    # confine every operator-supplied --out/--output to the repo tree before
    # writing; this evidence writer previously skipped that, so --output
    # could point at an arbitrary destination such as .github/workflows/ci.yml
    # despite write_bytes_no_follow already blocking symlink/FIFO redirection
    # at the resolved path.
    # The confined result is bound back onto ``args.output`` and is what
    # every write below uses: the check resolves a relative --output against
    # ``allowed_root``, but the raw argparse Path an os.open() would see
    # resolves against the process cwd instead -- so dropping the return
    # value validates one path and writes another, which is no confinement
    # at all.
    try:
        args.output = confined_output_path(args.output, allowed_root=REPO_ROOT)
    except ValueError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

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
        values = args.value if args.value is not None else [_default_narrowed_value(args.dimension)]
        if len(values) != 1:
            raise EvidenceGateError(
                f"exactly one --value must be selected per named condition, got {len(values)}"
            )
        # Only the *count* of values was checked, so `--value -1` reached
        # artifact construction and produced negative selected_value/context
        # fields that violate the raw schema's documented minimum of zero.
        if values[0] < 0:
            raise EvidenceGateError(f"--value must be >= 0, got {values[0]}")
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
        args.output.parent.mkdir(parents=True, exist_ok=True)
        # write_bytes_no_follow (scripts/evidence_common.py): this predictable,
        # repo-relative output path gets the same symlink/FIFO hardening as the
        # promotion-manifest writer, instead of a plain write_text that would
        # follow a planted symlink. Kept inside this try block, not after it,
        # so a symlinked --output fails closed with the same FAIL/exit-1
        # contract as every other gate error here, instead of an uncaught
        # traceback.
        write_bytes_no_follow(
            args.output, (json.dumps(artifact, indent=2, sort_keys=True) + "\n").encode("utf-8")
        )
    except (EvidenceGateError, OSError, json.JSONDecodeError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    print(f"OK: wrote {len(artifact['records'])} dry-run record(s) to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
