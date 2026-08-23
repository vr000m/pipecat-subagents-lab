#!/usr/bin/env python3
"""Phase 4B analyzer: deterministic, credential-free promotion decision for
the query-context narrowing experiment.

See the "Phase 4: Query-context narrowing experiment" section of
``docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md``.

Consumes ``scripts/evidence_common.py``'s shared
``blocked``/``not-run``/``provider_effect_uncontrolled`` status vocabulary
rather than re-implementing it. Always emits
``docs/benchmarks/v0.1.3-query-context-analysis.json`` -- for a blocked,
not-run, or not-promoted outcome ``analysis`` is ``null`` and
``promotion_eligible`` is ``false``; promotion additionally requires every
named assumption (a)-(d) to hold.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import statistics
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.evidence_common import (
    MIN_PAIRED_SAMPLES_PER_CELL,
    EvidenceGateError,
    EvidenceStatus,
    FixtureIndex,
    confined_output_path,
    load_jsonl,
    sha256_file,
    validate_against_fixture,
    write_bytes_no_follow,
)
from scripts.run_query_context_experiment import load_fixture, scorer_hash, validate_raw_record

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "query-context-quality-v1.json"

BOOTSTRAP_ITERATIONS = 10_000
BOOTSTRAP_SEED = 0
PROMOTION_MEDIAN_IMPROVEMENT = 0.10
PROMOTION_BOOTSTRAP_LOWER_BOUND = 0.05
QUALITY_FLOOR = 0.90
QUALITY_DROP_BUDGET = 0.02
BASELINE_NOISE_SD_THRESHOLD = 0.01
EPSILON = 1e-9  # float round-off tolerance for exact-boundary threshold comparisons

CONTAMINATION_FIELDS = ("attempt_count", "retry_count", "rate_limit_count")


def _now_utc() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _terminal(
    *,
    status: str,
    reason: str,
    promotion_eligible: bool = False,
    analysis: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "reason": reason,
        "promotion_eligible": promotion_eligible,
        "analysis": analysis,
        "generated_at_utc": _now_utc(),
    }


def _is_contaminated(record: dict[str, Any], *, retrieval_snapshot_exposed: bool) -> bool:
    # A failed call's latency/quality_score are not a measurement of the
    # narrowing effect -- `outcome` in {"error", "timeout"} means the
    # fast-return/failure path produced these values, not a completed
    # provider call, so such a record must never reach the latency/quality
    # statistics regardless of its other fields.
    if record["outcome"] != "success":
        return True
    if record["attempt_count"] > 1 or record["retry_count"] > 0 or record["rate_limit_count"] > 0:
        return True
    if record["cache_status"] == "unknown":
        return True
    return retrieval_snapshot_exposed and record["retrieval_snapshot_id"] is None


def _spearman(xs: list[float], ys: list[float]) -> float | None:
    """Spearman's rank correlation, average-rank tie handling, no scipy/numpy."""
    n = len(xs)
    if n < 2:
        return None

    def _ranks(values: list[float]) -> list[float]:
        order = sorted(range(len(values)), key=lambda i: values[i])
        ranks = [0.0] * len(values)
        i = 0
        while i < len(order):
            j = i
            while j + 1 < len(order) and abs(values[order[j + 1]] - values[order[i]]) <= EPSILON:
                j += 1
            avg_rank = (i + j) / 2.0 + 1.0
            for k in range(i, j + 1):
                ranks[order[k]] = avg_rank
            i = j + 1
        return ranks

    rx = _ranks(xs)
    ry = _ranks(ys)
    mean_rx = sum(rx) / n
    mean_ry = sum(ry) / n
    cov = sum((rx[i] - mean_rx) * (ry[i] - mean_ry) for i in range(n))
    var_x = sum((r - mean_rx) ** 2 for r in rx)
    var_y = sum((r - mean_ry) ** 2 for r in ry)
    denom = (var_x * var_y) ** 0.5
    if denom == 0:
        return None
    return cov / denom


def _median(values: list[float]) -> float:
    return statistics.median(values)


# Provider/model pairs that `run_query_context_experiment.py` stamps on
# synthetic dry-run records. A dry-run row must never reach the promotion
# rubric; until now it was only rejected as an incidental side effect of
# carrying `retrieval_snapshot_id=None` (routing its stratum into
# `provider_effect_uncontrolled`), so a synthetic row with a snapshot id and a
# hit/miss cache status would have promoted.
SYNTHETIC_STRATA = frozenset({("synthetic", "dry-run-fixture")})


def build_analysis(
    records: list[dict[str, Any]],
    *,
    fixture_index: FixtureIndex,
    fixture_sha256: str,
) -> dict[str, Any]:
    """Apply the full Phase 4B promotion rubric to a clean set of raw records.

    Raises ``EvidenceGateError`` for input-contract violations (missing
    dimensions, forbidden/unknown fields, duplicate or unpaired fixture-turn
    identities, mixed fixture/scorer versions, mixed selected dimensions, a
    scorer_hash that does not bind its own record, or matched IDs/quality_score
    that do not resolve against the versioned fixture). Returns a terminal
    result dict for every other outcome, including halts required by named
    assumptions (a)-(d).

    Re-validates every record against the strict raw allowlist and the
    versioned fixture even though ``collect_query_context_latency.py``
    already did so: the analyzer must not silently trust its input file's
    provenance, since nothing prevents a hand-edited or externally produced
    JSONL from reaching this script. The key-set check alone was not enough
    -- wrong numeric types, negative values, invalid enums, and invented
    fixture IDs (with a correctly self-recomputed ``scorer_hash``, which
    proves only internal consistency, not forgery-resistance) all passed
    straight through into the statistics -- so this runs the same full
    type/range validator and fixture-binding check the collector uses.
    """
    # Shape is validated first, unconditionally, so neither this gate's own
    # key reads below nor the promotion rubric ever run against an
    # unvalidated record: a malformed record missing `provider`/`model`
    # would otherwise raise a raw KeyError here instead of the documented
    # EvidenceGateError, which main()'s except clause does not catch.
    for index, record in enumerate(records):
        validate_raw_record(record, where=f"record {index}")

    # Assumption: a dry-run/synthetic row is never promotion input. Until now
    # that held only as an incidental side effect of dry-run records carrying
    # `retrieval_snapshot_id=None`, which routed their stratum into
    # `provider_effect_uncontrolled`; a synthetic row with a snapshot id and a
    # hit/miss cache status would have reached the promotion rubric. This is
    # the explicit gate.
    synthetic_strata = sorted(
        {
            f"{r['provider']}/{r['model']}"
            for r in records
            if (r["provider"], r["model"]) in SYNTHETIC_STRATA
        }
    )
    if synthetic_strata:
        return _terminal(
            status=EvidenceStatus.BLOCKED.value,
            reason="synthetic_dry_run_input",
            analysis={"synthetic_strata": synthetic_strata},
        )

    for index, record in enumerate(records):
        where = f"record {index}"
        # Shape validation alone accepted a record whose scorer_hash was
        # unrelated to the matched IDs and quality_score it claims, so an edit
        # made after scoring (or a wholly invented row) reached the promotion
        # rubric. Recomputing the same provenance hash the runner emitted
        # binds each row back to its own scored values, the way the collector
        # already does before it ever writes the analyzer's input.
        expected_scorer_hash = scorer_hash(
            record["fixture_version"],
            record["fixture_turn_id"],
            matched_fact_ids=record["matched_fact_ids"],
            matched_citation_ids=record["matched_citation_ids"],
            matched_disallowed_claim_ids=record["matched_disallowed_claim_ids"],
            quality_score=record["quality_score"],
            scorer_version=record["scorer_version"],
            record=record,
        )
        if record["scorer_hash"] != expected_scorer_hash:
            raise EvidenceGateError(
                f"{where}: scorer_hash does not match this record's own fields "
                "-- internally inconsistent scorer provenance (the digest binds every "
                "field except scorer_hash/fixture_sha256, so any post-scoring edit "
                "invalidates it)"
            )
        # A keyless self-hash proves only that scorer_hash agrees with the
        # record's OWN fields -- it cannot detect an editor who invents
        # unknown fixture IDs and recomputes the hash from those fabricated
        # values. Every match ID and the quality_score are therefore also
        # resolved against the versioned fixture, exactly as the collector
        # already does before it ever writes this script's input.
        validate_against_fixture(record, index=fixture_index, where=where)
        # `fixture_version` is a string the record declares about itself, so it
        # binds nothing: a `--fixture` file carrying the same version string
        # but weaker required_facts/expected_citations/disallowed_claims would
        # resolve forged match IDs and an inflated quality_score cleanly. The
        # collector stamps the digest of the exact fixture bytes it scored
        # against precisely so this check can exist; without comparing it, the
        # digest was only ever shape-checked as 64 hex characters.
        declared_digest = record.get("fixture_sha256")
        if declared_digest is None:
            raise EvidenceGateError(
                f"{where}: fixture_sha256 is required -- a record that does not bind the exact "
                "fixture bytes it was scored against cannot support a promotion decision"
            )
        if declared_digest != fixture_sha256:
            raise EvidenceGateError(
                f"{where}: fixture_sha256 {declared_digest} does not match the analysis fixture "
                f"{fixture_sha256} -- the record was scored against different fixture bytes"
            )

    fixture_versions = {r["fixture_version"] for r in records}
    if len(fixture_versions) > 1:
        raise EvidenceGateError(
            f"mixed fixture_version values in input: {sorted(fixture_versions)}"
        )
    scorer_versions = {r["scorer_version"] for r in records}
    if len(scorer_versions) > 1:
        raise EvidenceGateError(f"mixed scorer_version values in input: {sorted(scorer_versions)}")
    selected_dimensions = {r["selected_dimension"] for r in records}
    if len(selected_dimensions) > 1:
        raise EvidenceGateError(
            f"mixed selected_dimension values in input: {sorted(selected_dimensions)} -- the "
            "experiment selects exactly one dimension, so records from different dimensions "
            "describe different experiments and must not be pooled"
        )

    by_stratum: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        by_stratum[(record["provider"], record["model"])].append(record)

    # Assumption (d): control-field contamination. A stratum is
    # provider_effect_uncontrolled if none of its records expose a retrieval
    # snapshot id at all (no discoverable control), otherwise samples missing
    # it while siblings have it are excluded as contaminated.
    clean_by_stratum: dict[tuple[str, str], list[dict[str, Any]]] = {}
    excluded_contaminated_count: dict[str, int] = {}
    uncontrolled_strata: list[str] = []
    for stratum in sorted(by_stratum):
        stratum_records = by_stratum[stratum]
        stratum_key = f"{stratum[0]}/{stratum[1]}"
        exposes_snapshot = any(r["retrieval_snapshot_id"] is not None for r in stratum_records)
        if not exposes_snapshot:
            uncontrolled_strata.append(stratum_key)
            continue
        clean = [
            r for r in stratum_records if not _is_contaminated(r, retrieval_snapshot_exposed=True)
        ]
        clean_by_stratum[stratum] = clean
        excluded_contaminated_count[stratum_key] = len(stratum_records) - len(clean)

    if not clean_by_stratum:
        return _terminal(
            status=EvidenceStatus.BLOCKED.value,
            reason="provider_effect_uncontrolled",
            analysis={"uncontrolled_strata": uncontrolled_strata},
        )

    per_stratum_report: dict[str, Any] = {}
    per_stratum_pair_improvements: dict[str, list[float]] = {}
    undersized: list[str] = []

    # Halt reasons are collected across every stratum before any terminal
    # result is returned (instead of returning from inside this loop), and
    # strata are visited in sorted key order, so the emitted report -- and
    # which reason takes priority when strata disagree -- no longer depends
    # on the order providers happened to first appear in the input file.
    noisy_strata: list[str] = []
    below_floor_strata: list[str] = []
    quality_drop_strata: list[str] = []

    for provider, model in sorted(clean_by_stratum):
        stratum_records = clean_by_stratum[(provider, model)]
        stratum_key = f"{provider}/{model}"
        baseline = [r for r in stratum_records if r["condition"] == "baseline"]
        narrowed = [r for r in stratum_records if r["condition"] == "narrowed"]

        for group, label in ((baseline, "baseline"), (narrowed, "narrowed")):
            ids = [r["fixture_turn_id"] for r in group]
            if len(ids) != len(set(ids)):
                raise EvidenceGateError(
                    f"duplicate fixture_turn_id in {stratum_key}/{label}: input is not one sample "
                    "per fixture-turn identity"
                )

        baseline_by_id = {r["fixture_turn_id"]: r for r in baseline}
        narrowed_by_id = {r["fixture_turn_id"]: r for r in narrowed}
        paired_ids = sorted(set(baseline_by_id) & set(narrowed_by_id))

        if len(paired_ids) < MIN_PAIRED_SAMPLES_PER_CELL:
            undersized.append(f"{stratum_key}={len(paired_ids)}")
            continue

        # Assumption (a): baseline run-to-run noise. Repeated baseline
        # quality scores must be stable enough that the score measures the
        # context-window effect, not provider variance. `fixture_turn_id` is
        # `"<base_turn_id>#<repeat_index>"` (run_query_context_experiment.py),
        # so pooling every paired id's quality_score together (as before)
        # measured variance ACROSS distinct base turns -- which legitimately
        # differ -- rather than variance across repeats of the SAME base
        # turn. Grouping by base turn id and taking the worst within-group SD
        # isolates true run-to-run noise. A base turn with only one repeat
        # contributes no signal (pstdev of a single value is 0) and is
        # skipped. An epsilon absorbs float round-off so an exact-SD-0.01
        # fixture is not misclassified as "too noisy" by a value like
        # 0.010000000000000009.
        baseline_quality = [baseline_by_id[i]["quality_score"] for i in paired_ids]
        narrowed_quality = [narrowed_by_id[i]["quality_score"] for i in paired_ids]
        baseline_quality_by_base_turn: dict[str, list[float]] = defaultdict(list)
        for i in paired_ids:
            baseline_quality_by_base_turn[i.split("#", 1)[0]].append(
                baseline_by_id[i]["quality_score"]
            )
        within_turn_sds = [
            statistics.pstdev(scores)
            for scores in baseline_quality_by_base_turn.values()
            if len(scores) > 1
        ]
        baseline_quality_sd = max(within_turn_sds) if within_turn_sds else 0.0
        baseline_too_noisy = baseline_quality_sd > BASELINE_NOISE_SD_THRESHOLD + EPSILON

        baseline_quality_mean = statistics.fmean(baseline_quality)
        narrowed_quality_mean = statistics.fmean(narrowed_quality)
        quality_drop = baseline_quality_mean - narrowed_quality_mean
        baseline_below_floor = baseline_quality_mean < QUALITY_FLOOR - EPSILON
        quality_ok = (
            narrowed_quality_mean >= QUALITY_FLOOR - EPSILON
            and quality_drop <= QUALITY_DROP_BUDGET + EPSILON
        )

        pair_latencies_baseline = [baseline_by_id[i]["latency_ms"] for i in paired_ids]
        pair_latencies_narrowed = [narrowed_by_id[i]["latency_ms"] for i in paired_ids]
        for b in pair_latencies_baseline:
            if not b:
                raise EvidenceGateError(
                    f"{stratum_key}: a paired baseline record has latency_ms == 0 -- a relative "
                    "improvement cannot be computed against a zero baseline latency"
                )
        rel_improvements = [
            (b - n) / b for b, n in zip(pair_latencies_baseline, pair_latencies_narrowed)
        ]

        # Every other statistic in this stratum is computed over the paired
        # baseline/narrowed records (`paired_ids`), not every clean record --
        # an unpaired baseline-only row published a correlation over a
        # different, larger population than `paired_sample_count` reports.
        paired_records = [baseline_by_id[i] for i in paired_ids] + [
            narrowed_by_id[i] for i in paired_ids
        ]
        paired_context = [r["context_chars"] for r in paired_records]
        paired_latency = [r["latency_ms"] for r in paired_records]
        spearman = _spearman(paired_context, paired_latency)

        per_stratum_pair_improvements[stratum_key] = rel_improvements
        per_stratum_report[stratum_key] = {
            "paired_sample_count": len(paired_ids),
            "excluded_contaminated_count": excluded_contaminated_count.get(stratum_key, 0),
            "spearman_context_vs_latency": spearman,
            "baseline_quality_mean": baseline_quality_mean,
            "narrowed_quality_mean": narrowed_quality_mean,
            "baseline_quality_sd": baseline_quality_sd,
            "quality_drop": quality_drop,
            "quality_ok": quality_ok,
            "median_relative_improvement": _median(rel_improvements),
        }

        if baseline_too_noisy:
            noisy_strata.append(stratum_key)
        if baseline_below_floor:
            below_floor_strata.append(stratum_key)
        if not quality_ok:
            quality_drop_strata.append(stratum_key)

    # Named-assumption halts (a)/(b) take priority over the ordinary
    # promotion rubric, in the order the plan names them, evaluated across
    # every stratum's result rather than stopping at the first stratum
    # processed.
    if noisy_strata:
        return _terminal(
            status="not_promoted",
            reason="baseline_too_noisy",
            analysis={
                "strata": per_stratum_report,
                "undersized_strata": undersized,
                "noisy_strata": noisy_strata,
            },
        )
    if below_floor_strata:
        return _terminal(
            status="not_promoted",
            reason="baseline_below_quality_floor",
            analysis={
                "strata": per_stratum_report,
                "undersized_strata": undersized,
                "below_quality_floor_strata": below_floor_strata,
            },
        )
    if quality_drop_strata:
        return _terminal(
            status="not_promoted",
            reason="quality_drop_exceeded",
            analysis={
                "strata": per_stratum_report,
                "undersized_strata": undersized,
                "quality_drop_strata": quality_drop_strata,
            },
        )

    if not per_stratum_pair_improvements:
        return _terminal(
            status=EvidenceStatus.BLOCKED.value,
            reason="undersized_cell",
            analysis={"undersized_strata": undersized},
        )

    # A stratum that was dropped for too few pairs or for having no
    # retrieval-snapshot control was only excluded from the pairwise analysis,
    # so a run where one provider/model collected cleanly and another did not
    # still promoted -- on the surviving stratum alone, silently narrowing the
    # claim from "this holds across the collected strata" to "this holds
    # somewhere". Any dropped stratum means the collected evidence does not
    # cover the strata it purports to, so the whole decision blocks.
    if undersized or uncontrolled_strata:
        return _terminal(
            status=EvidenceStatus.BLOCKED.value,
            reason="incomplete_stratum_coverage",
            analysis={
                "strata": per_stratum_report,
                "undersized_strata": undersized,
                "uncontrolled_strata": uncontrolled_strata,
            },
        )

    strata = sorted(per_stratum_pair_improvements)
    stratum_medians = [_median(per_stratum_pair_improvements[s]) for s in strata]
    equal_weight_median = _median(stratum_medians)

    rng = random.Random(BOOTSTRAP_SEED)
    bootstrap_overall_medians: list[float] = []
    for _ in range(BOOTSTRAP_ITERATIONS):
        resample_medians = []
        for stratum in strata:
            pool = per_stratum_pair_improvements[stratum]
            resample = [pool[rng.randrange(len(pool))] for _ in range(len(pool))]
            resample_medians.append(_median(resample))
        bootstrap_overall_medians.append(_median(resample_medians))
    bootstrap_overall_medians.sort()
    # `int(0.05 * N)` truncates towards zero, so for N=10_000 it picked index
    # 500 -- the 501st order statistic, one past the true 5th-percentile
    # index -- biasing the lower bound slightly toward easier promotion.
    # `ceil(0.05 * N) - 1` is the correct (0-indexed) percentile-index
    # formula; `max(0, ...)` guards a degenerate N where the ceiling is 0.
    lower_bound_index = max(0, math.ceil(0.05 * BOOTSTRAP_ITERATIONS) - 1)
    bootstrap_lower_bound = bootstrap_overall_medians[lower_bound_index]

    # Quality gating already happened per-stratum above (halts with
    # quality_drop_exceeded before reaching this point), so promotion here
    # depends only on the latency thresholds.
    promote = (
        equal_weight_median >= PROMOTION_MEDIAN_IMPROVEMENT - EPSILON
        and bootstrap_lower_bound >= PROMOTION_BOOTSTRAP_LOWER_BOUND - EPSILON
    )

    analysis = {
        "strata": per_stratum_report,
        # Published so a downstream evidence binder can verify the promotion
        # decision against the exact fixture bytes it was made under, rather
        # than against a self-declared fixture_version string.
        "fixture_sha256": fixture_sha256,
        "median_relative_latency_improvement": equal_weight_median,
        "bootstrap_lower_bound_95": bootstrap_lower_bound,
        "bootstrap_iterations": BOOTSTRAP_ITERATIONS,
        "bootstrap_seed": BOOTSTRAP_SEED,
        "thresholds": {
            "median_improvement": PROMOTION_MEDIAN_IMPROVEMENT,
            "bootstrap_lower_bound": PROMOTION_BOOTSTRAP_LOWER_BOUND,
            "quality_floor": QUALITY_FLOOR,
            "quality_drop_budget": QUALITY_DROP_BUDGET,
        },
        "undersized_strata": undersized,
    }

    if promote:
        return _terminal(status="promoted", reason=None, promotion_eligible=True, analysis=analysis)
    return _terminal(
        status="not_promoted", reason="data_did_not_support_promotion", analysis=analysis
    )


def analyze(
    records: list[dict[str, Any]], *, fixture_path: Path = DEFAULT_FIXTURE_PATH
) -> dict[str, Any]:
    if len(records) == 1 and "status" in records[0] and "quality_score" not in records[0]:
        status_record = records[0]
        status = status_record["status"]
        if status not in {member.value for member in EvidenceStatus}:
            raise EvidenceGateError(f"status record: invalid status {status!r}")
        reason = status_record.get("reason")
        if reason is not None and (not isinstance(reason, str) or not reason or len(reason) > 200):
            raise EvidenceGateError(
                "status record: reason must be null or a bounded non-empty string"
            )
        return _terminal(
            status=status,
            reason=reason,
            promotion_eligible=False,
            analysis=None,
        )
    if not records:
        return _terminal(status=EvidenceStatus.NOT_RUN.value, reason="no_paid_samples")
    fixture_index = FixtureIndex(load_fixture(fixture_path))
    return build_analysis(
        records, fixture_index=fixture_index, fixture_sha256=sha256_file(fixture_path)
    )


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
            "resolved against. Defaults to the committed Phase 4A fixture, matching "
            "collect_query_context_latency.py's default."
        ),
    )
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

    try:
        records = load_jsonl(args.input)
        result = analyze(records, fixture_path=args.fixture)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        # `write_bytes_no_follow` (scripts/evidence_common.py) rather than
        # `Path.write_text` -- this predictable, repo-relative output path is
        # exactly the kind of target the promotion-manifest writer's symlink
        # hardening exists for, and every evidence-gate writer routes through
        # the same shared primitive so none of them silently regresses. Kept
        # inside this try block, not after it, so a symlinked --output fails
        # closed with the same FAIL/exit-1 contract as every other gate error
        # here, instead of an uncaught traceback.
        write_bytes_no_follow(
            args.output, (json.dumps(result, indent=2, sort_keys=True) + "\n").encode("utf-8")
        )
    except (EvidenceGateError, OSError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    print(
        f"OK: wrote status={result['status']} promotion_eligible={result['promotion_eligible']} "
        f"to {args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
