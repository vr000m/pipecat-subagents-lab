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
import hashlib
import json
import os
import random
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts._evidence_common import (
    EvidenceGateError,
    closed_object,
    require_type,
    write_bytes_no_follow,
)
from scripts.eval_common import confined_output_path

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
# `fixture_sha256` is populated by the collector (a sha256 of the exact
# fixture file bytes each record's matched IDs/quality_score were resolved
# against), not by the runner's raw output, so it is allowed but not
# required -- an artifact predating this field is still valid raw shape.
RAW_ALLOWED = RAW_REQUIRED | frozenset({"fixture_sha256"})

# Fields deliberately excluded from `scorer_hash`'s whole-record binding.
# `scorer_hash` is the digest itself; `fixture_sha256` is stamped by the
# collector *after* the runner has scored and hashed the record, and has its
# own binding path (compared against `sha256_file(fixture_path)`).
_UNBOUND_RECORD_FIELDS = frozenset({"scorer_hash", "fixture_sha256"})


OUTCOMES = frozenset({"success", "error", "timeout"})
CACHE_STATUSES = frozenset({"hit", "miss", "unknown"})

# (field, python types, minimum) for the numeric raw fields, mirroring
# `shared/schemas/v013-query-context-raw.json`'s type/minimum constraints.
_RAW_NUMERIC_FIELDS: tuple[tuple[str, tuple[type, ...], float], ...] = (
    ("run_block", (int,), 0),
    ("run_order", (int,), 0),
    ("selected_value", (int,), 0),
    ("context_chars", (int,), 0),
    ("query_chars", (int,), 0),
    ("latency_ms", (int, float), 0),
    ("attempt_count", (int,), 1),
    ("retry_count", (int,), 0),
    ("rate_limit_count", (int,), 0),
)
_RAW_STRING_FIELDS = (
    "run_id",
    "fixture_version",
    "fixture_turn_id",
    "provider",
    "model",
    "scorer_version",
    "recorded_at_utc",
)
_RAW_ID_LIST_FIELDS = (
    "matched_fact_ids",
    "matched_citation_ids",
    "matched_disallowed_claim_ids",
)


def validate_raw_record(record: dict[str, Any], *, where: str = "record") -> None:
    """Full type/range/enum validation of one raw record against the schema.

    Both the collector and the analyzer need this: a hand-edited or
    externally produced JSONL can reach either one, and a key-set
    (`closed_object`) check alone lets wrong numeric types, negative values,
    and invalid enums through to skew statistics or raise downstream. It
    lives here because this module already owns `RAW_REQUIRED`/`RAW_ALLOWED`,
    so the allowlist and the field contract cannot drift apart.
    """
    closed_object(record, required=RAW_REQUIRED, allowed=RAW_ALLOWED)
    for field in _RAW_STRING_FIELDS:
        value = record[field]
        require_type(value, (str,), f"{where}: {field}")
        if not value:
            raise EvidenceGateError(f"{where}: {field} must be non-empty")
    if record["condition"] not in CONDITIONS:
        raise EvidenceGateError(f"{where}: invalid condition {record['condition']!r}")
    if record["selected_dimension"] not in SELECTABLE_DIMENSIONS:
        raise EvidenceGateError(
            f"{where}: invalid selected_dimension {record['selected_dimension']!r}"
        )
    if record["outcome"] not in OUTCOMES:
        raise EvidenceGateError(f"{where}: invalid outcome {record['outcome']!r}")
    if record["cache_status"] not in CACHE_STATUSES:
        raise EvidenceGateError(f"{where}: invalid cache_status {record['cache_status']!r}")
    # A record's declared `scorer_version` was previously never checked against
    # anything -- `scorer_hash` bound the module constant `SCORER_VERSION`
    # regardless of what a record claimed, so a uniformly forged
    # `scorer_version` across an entire batch produced an identical, correctly
    # matching digest and passed every gate. Rejecting any declared value that
    # is not the actual current scorer version closes that gap independent of
    # hash binding.
    if record["scorer_version"] != SCORER_VERSION:
        raise EvidenceGateError(
            f"{where}: scorer_version {record['scorer_version']!r} does not match the "
            f"current scorer version {SCORER_VERSION!r}"
        )

    for field, kinds, minimum in _RAW_NUMERIC_FIELDS:
        value = record[field]
        require_type(value, kinds, f"{where}: {field}")
        if value < minimum:
            raise EvidenceGateError(f"{where}: {field} must be >= {minimum}, got {value!r}")

    require_type(record["quality_score"], (int, float), f"{where}: quality_score")
    if not (0.0 <= float(record["quality_score"]) <= 1.0):
        raise EvidenceGateError(f"{where}: quality_score must be within [0, 1]")

    for field in _RAW_ID_LIST_FIELDS:
        value = record[field]
        require_type(value, (list,), f"{where}: {field}")
        for item in value:
            if not isinstance(item, str) or not item:
                raise EvidenceGateError(f"{where}: {field} entries must be non-empty strings")

    digest = record["scorer_hash"]
    require_type(digest, (str,), f"{where}: scorer_hash")
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise EvidenceGateError(f"{where}: scorer_hash must be a 64-character lowercase hex digest")

    snapshot = record["retrieval_snapshot_id"]
    if snapshot is not None:
        require_type(snapshot, (str,), f"{where}: retrieval_snapshot_id")
        if not snapshot:
            raise EvidenceGateError(f"{where}: retrieval_snapshot_id must be non-empty or null")

    if "fixture_sha256" in record:
        fixture_digest = record["fixture_sha256"]
        require_type(fixture_digest, (str,), f"{where}: fixture_sha256")
        if len(fixture_digest) != 64 or any(c not in "0123456789abcdef" for c in fixture_digest):
            raise EvidenceGateError(
                f"{where}: fixture_sha256 must be a 64-character lowercase hex digest"
            )


def scorer_hash(
    fixture_version: str,
    fixture_turn_id: str | None = None,
    *,
    matched_fact_ids: list[str] | None = None,
    matched_citation_ids: list[str] | None = None,
    matched_disallowed_claim_ids: list[str] | None = None,
    quality_score: float | None = None,
    scorer_version: str = SCORER_VERSION,
    record: Mapping[str, Any] | None = None,
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

    ``scorer_version`` defaults to the module constant for internal callers
    stamping a hash for a record they are about to emit (there is no
    record yet to read a declared version from). A verifier recomputing this
    hash to check an *existing* record must instead pass
    ``record["scorer_version"]`` explicitly -- otherwise recomputing with the
    constant regardless of what the record declares lets a uniformly forged
    ``scorer_version`` produce a matching digest for every record in a batch.

    ``record`` binds the *whole* raw record, not just the scoring fields. The
    scoring-field-only payload authenticated exactly the values the scorer
    produced and nothing else, which left the measurement and stratum-identity
    fields the promotion decision actually rests on -- ``latency_ms``,
    ``condition``, ``selected_value``, ``provider``, ``model``, ``outcome``,
    ``cache_status``, ``retrieval_snapshot_id``, the attempt/retry/rate-limit
    counters -- entirely unauthenticated. Since the analyzer's synthetic guard
    is a name denylist over those same unbound strings
    (``SYNTHETIC_STRATA``), a dry-run artifact could be relabelled into the
    paid stratum with any chosen latency and still satisfy every ID, quality,
    fixture-digest, and hash check. Binding the full record closes that: any
    post-scoring edit to any field invalidates the digest.

    ``scorer_hash`` itself is excluded (it is the digest being computed) and
    so is ``fixture_sha256``, which the *collector* stamps onto a record after
    the runner has already scored and hashed it; including it would make the
    runner's stamp and every verifier's recomputation disagree by
    construction. ``fixture_sha256`` has its own binding path -- it is
    compared against ``sha256_file(fixture_path)``.

    This remains a keyless self-hash: it detects edits to a runner-produced
    artifact, not a wholly regenerated one. Fixture resolution
    (``validate_against_fixture``) is the anti-fabrication layer on top. A
    threat model that includes regeneration needs an HMAC stamped with a
    collection-time key, or a signed artifact, not a bare SHA-256.
    """
    fixture_turn_id = fixture_turn_id or ""
    matched_fact_ids = matched_fact_ids or []
    matched_citation_ids = matched_citation_ids or []
    matched_disallowed_claim_ids = matched_disallowed_claim_ids or []
    bound_record = (
        {key: value for key, value in sorted(record.items()) if key not in _UNBOUND_RECORD_FIELDS}
        if record is not None
        else None
    )
    payload = json.dumps(
        {
            "scorer_version": scorer_version,
            "fixture_version": fixture_version,
            "fixture_turn_id": fixture_turn_id,
            "matched_fact_ids": sorted(matched_fact_ids),
            "matched_citation_ids": sorted(matched_citation_ids),
            "matched_disallowed_claim_ids": sorted(matched_disallowed_claim_ids),
            "quality_score": round(float(quality_score), 6) if quality_score is not None else None,
            "record": bound_record,
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
    try:
        confined_output_path(args.output, allowed_root=REPO_ROOT)
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
        values = args.value if args.value is not None else [NARROWED_VALUE]
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
        # write_bytes_no_follow (scripts/_evidence_common.py): this predictable,
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
