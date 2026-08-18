#!/usr/bin/env python3
"""Router/worker model-comparison eval-suite runner.

Drives ``SessionHost._handle_transcript()`` directly against the live, paid
OpenAI provider (no live ``pipecat eval`` transport exists in this repo -- see
the dev plan's Context section) to compare candidate router/worker
model+reasoning-effort combinations, scoring semantic correctness via
``pipecat.evals.judge.EvalJudge`` and tracking per-turn latency.

Every (model, effort) combination this runner will actually call must first
be present and accepted in Phase 0's verification manifest
(``docs/dev_plans/artifacts/eval-candidates-manifest.json`` by default) --
this script never probes a candidate combination itself; see
``scripts/verify_eval_candidates.py`` for that gate.

Usage::

    uv run python scripts/eval_model_comparison.py --dry-run
    uv run python scripts/eval_model_comparison.py --router baseline --worker baseline --scenario single-turn-default
    uv run python scripts/eval_model_comparison.py --full-matrix --max-calls 40 --max-cost 5.0
"""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from evals.scenarios import SCENARIOS, SCENARIOS_BY_NAME, Scenario
from scripts._eval_common import (
    CollectingMeasurementSink,
    _latest_turn_stage_metrics,
    build_session_for_run,
)
from scripts.smoke_conversation import SAFE_FALLBACKS
from server.config import Config, load_config

_REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST_PATH = _REPO_ROOT / "docs/dev_plans/artifacts/eval-candidates-manifest.json"
DEFAULT_JUDGE_MODEL = "gpt-5-mini"
DEFAULT_MAX_ROUTING_SECONDS = 15.0
DEFAULT_MAX_LATENCY_SECONDS = 60.0

# Rough, deliberately conservative per-call dollar estimates for the
# --dry-run cost preview. These are not billed-price lookups -- just enough
# to give an operator an order-of-magnitude sense of spend before confirming
# a live run. The worker multiplier accounts for tool_choice="required"
# potentially driving multiple internal web_search tool invocations inside a
# single Responses API call (the call count itself is unaffected -- it is
# still one HTTP call per turn -- but the token/tool cost of that one call is
# not flat, so the estimate applies the multiplier to cost only, not to call
# count).
_ROUGH_COST_PER_CALL = {"router": 0.01, "worker": 0.03, "judge": 0.002}
_WORKER_TOOL_CALL_COST_MULTIPLIER = 2.0


# --------------------------------------------------------------------------
# Candidate matrix (Objective's candidate matrix, verified live by Phase 0).
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Candidate:
    label: str
    role: str  # "router" | "worker"
    model: str
    effort: str | None  # None means "unset" (today's model-conditional default)


ROUTER_BASELINE = Candidate(label="baseline", role="router", model="gpt-5-mini", effort=None)
ROUTER_CANDIDATES = (
    Candidate(label="luna-high", role="router", model="gpt-5.6-luna", effort="high"),
    Candidate(label="terra-low", role="router", model="gpt-5.6-terra", effort="low"),
)
ROUTER_CANDIDATES_BY_LABEL = {c.label: c for c in (ROUTER_BASELINE, *ROUTER_CANDIDATES)}

WORKER_BASELINE = Candidate(label="baseline", role="worker", model="gpt-5", effort=None)
WORKER_CANDIDATES = (
    Candidate(label="terra-medium", role="worker", model="gpt-5.6-terra", effort="medium"),
    Candidate(label="sol-low", role="worker", model="gpt-5.6-sol", effort="low"),
)
WORKER_CANDIDATES_BY_LABEL = {c.label: c for c in (WORKER_BASELINE, *WORKER_CANDIDATES)}


@dataclass(frozen=True)
class RunPair:
    router: Candidate
    worker: Candidate

    @property
    def label(self) -> str:
        return f"router={self.router.label}/worker={self.worker.label}"

    @property
    def is_baseline(self) -> bool:
        return self.router.label == "baseline" and self.worker.label == "baseline"


def default_sweep_pairs() -> tuple[RunPair, ...]:
    """baseline x baseline, plus one-role-varied sweeps.

    Not the full cross product -- proportionate to what "compare Codex's
    per-role recommendation" actually needs. See Requirements.
    """
    pairs = [RunPair(ROUTER_BASELINE, WORKER_BASELINE)]
    pairs += [RunPair(candidate, WORKER_BASELINE) for candidate in ROUTER_CANDIDATES]
    pairs += [RunPair(ROUTER_BASELINE, candidate) for candidate in WORKER_CANDIDATES]
    return tuple(pairs)


def full_matrix_pairs() -> tuple[RunPair, ...]:
    routers = (ROUTER_BASELINE, *ROUTER_CANDIDATES)
    workers = (WORKER_BASELINE, *WORKER_CANDIDATES)
    return tuple(RunPair(r, w) for r in routers for w in workers)


# --------------------------------------------------------------------------
# Phase 0 manifest consultation.
# --------------------------------------------------------------------------


class ManifestError(RuntimeError):
    """The manifest is missing, malformed, or doesn't cover a needed combination."""


@dataclass(frozen=True)
class ManifestStatus:
    path: Path
    exists: bool
    source_commit: str | None
    current_commit: str | None
    stale: bool
    accepted: frozenset[tuple[str, str, str | None]]


def _current_source_commit() -> str | None:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip() or None


def load_manifest_status(manifest_path: Path) -> ManifestStatus:
    """Load Phase 0's manifest and index the (kind, model, effort) tuples it accepted.

    Never raises: a missing/malformed manifest is reported via
    ``ManifestStatus.exists=False``/an empty accepted set, and it is the
    caller's job to decide whether that is fatal (it is, for any live cell;
    it is a printed warning for ``--dry-run``).
    """
    current_commit = _current_source_commit()
    if not manifest_path.exists():
        return ManifestStatus(
            path=manifest_path,
            exists=False,
            source_commit=None,
            current_commit=current_commit,
            stale=True,
            accepted=frozenset(),
        )
    try:
        manifest = json.loads(manifest_path.read_text())
    except (OSError, json.JSONDecodeError):
        return ManifestStatus(
            path=manifest_path,
            exists=True,
            source_commit=None,
            current_commit=current_commit,
            stale=True,
            accepted=frozenset(),
        )
    if not isinstance(manifest, dict) or not isinstance(manifest.get("results"), list):
        return ManifestStatus(
            path=manifest_path,
            exists=True,
            source_commit=None,
            current_commit=current_commit,
            stale=True,
            accepted=frozenset(),
        )
    source_commit = manifest.get("source_commit")
    stale = bool(
        current_commit and isinstance(source_commit, str) and source_commit != current_commit
    )
    accepted: set[tuple[str, str, str | None]] = set()
    for entry in manifest["results"]:
        if not isinstance(entry, dict) or entry.get("accepted") is not True:
            continue
        kind = entry.get("kind")
        model = entry.get("model")
        effort = entry.get("effort")
        if isinstance(kind, str) and isinstance(model, str):
            accepted.add((kind, model, effort if isinstance(effort, str) else None))
    return ManifestStatus(
        path=manifest_path,
        exists=True,
        source_commit=source_commit if isinstance(source_commit, str) else None,
        current_commit=current_commit,
        stale=stale,
        accepted=frozenset(accepted),
    )


def candidate_manifest_kind(candidate: Candidate) -> str:
    return candidate.role


def _effective_effort_for_manifest_lookup(candidate: Candidate) -> str | None:
    """The effort this candidate's request will actually carry on the wire.

    Mirrors ``LazyRouterProvider.__call__``'s conditional exactly: an unset
    effort-policy (``candidate.effort is None``) is not "no reasoning key" for
    a ``gpt-5*`` router model -- it falls back to today's
    ``if model.startswith("gpt-5"): kwargs["reasoning"] = {"effort": "minimal"}``
    default. Phase 0's manifest recorded the *effective* request shape for the
    baseline (``gpt-5-mini`` @ ``minimal``), not the policy-label state, so the
    lookup has to resolve the same way or the router baseline would spuriously
    read as "absent from the manifest". The worker has no such conditional --
    an unset worker effort genuinely omits the ``reasoning`` key.
    """
    if candidate.effort is not None:
        return candidate.effort
    if candidate.role == "router" and candidate.model.startswith("gpt-5"):
        return "minimal"
    return None


def candidate_accepted(candidate: Candidate, status: ManifestStatus) -> bool:
    effort = _effective_effort_for_manifest_lookup(candidate)
    return (candidate_manifest_kind(candidate), candidate.model, effort) in status.accepted


def judge_accepted(judge_model: str, status: ManifestStatus) -> bool:
    return ("judge", judge_model, None) in status.accepted


def require_manifest_ok_for_live_run(
    status: ManifestStatus,
    *,
    allow_stale: bool,
    candidates: tuple[Candidate, ...],
    judge_model: str,
) -> None:
    """Fatal, pre-flight manifest check for a live run. Never called under --dry-run."""
    if not status.exists:
        raise ManifestError(
            f"Phase 0 manifest not found at {status.path} -- run "
            "scripts/verify_eval_candidates.py before a live eval run"
        )
    if status.stale and not allow_stale:
        raise ManifestError(
            f"manifest source_commit={status.source_commit!r} does not match the current "
            f"checkout ({status.current_commit!r}); pass --i-know-the-manifest-is-stale to "
            "proceed anyway"
        )
    missing = [c for c in candidates if not candidate_accepted(c, status)]
    if missing:
        described = ", ".join(f"{c.role}:{c.model}@{c.effort}" for c in missing)
        raise ManifestError(
            f"the following (model, effort) combinations are absent from the manifest and "
            f"cannot be run: {described}"
        )
    if not judge_accepted(judge_model, status):
        raise ManifestError(f"judge model {judge_model!r} is absent from the manifest")


# --------------------------------------------------------------------------
# Call/cost accounting.
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class CallAccounting:
    router_calls: int
    worker_calls: int
    judge_calls: int

    @property
    def total_calls(self) -> int:
        return self.router_calls + self.worker_calls + self.judge_calls

    @property
    def estimated_cost_usd(self) -> float:
        return (
            self.router_calls * _ROUGH_COST_PER_CALL["router"]
            + self.worker_calls * _ROUGH_COST_PER_CALL["worker"] * _WORKER_TOOL_CALL_COST_MULTIPLIER
            + self.judge_calls * _ROUGH_COST_PER_CALL["judge"]
        )


def scenario_call_counts(scenario: Scenario) -> tuple[int, int, int]:
    router_calls = len(scenario.turns)
    worker_calls = sum(1 for turn in scenario.turns if turn.expect_delegated)
    judge_calls = sum(1 for turn in scenario.turns if turn.judge_criterion)
    return router_calls, worker_calls, judge_calls


def matrix_call_accounting(
    pairs: tuple[RunPair, ...], scenarios: tuple[Scenario, ...]
) -> CallAccounting:
    router_calls = worker_calls = judge_calls = 0
    for _pair in pairs:
        for scenario in scenarios:
            r, w, j = scenario_call_counts(scenario)
            router_calls += r
            worker_calls += w
            judge_calls += j
    return CallAccounting(
        router_calls=router_calls, worker_calls=worker_calls, judge_calls=judge_calls
    )


# --------------------------------------------------------------------------
# Dry-run / matrix preview. Zero live calls, zero connections, from here down
# to the "Live execution" section below.
# --------------------------------------------------------------------------


def print_matrix_preview(
    pairs: tuple[RunPair, ...],
    scenarios: tuple[Scenario, ...],
    *,
    judge_model: str,
    status: ManifestStatus,
) -> None:
    print(f"Manifest: {status.path}")
    if not status.exists:
        print("  WARNING: manifest not found -- a live run would refuse to proceed")
    else:
        print(f"  source_commit={status.source_commit!r} current={status.current_commit!r}")
        if status.stale:
            print("  WARNING: manifest is stale relative to the current checkout")
    print()
    print(
        f"Judge model: {judge_model} ({'OK' if judge_accepted(judge_model, status) else 'NOT in manifest'})"
    )
    print()
    print(
        f"Matrix: {len(pairs)} config pair(s) x {len(scenarios)} scenario(s) = {len(pairs) * len(scenarios)} cell(s)"
    )
    for pair in pairs:
        router_ok = candidate_accepted(pair.router, status)
        worker_ok = candidate_accepted(pair.worker, status)
        flag = "" if router_ok and worker_ok else "  <-- NOT in manifest, would refuse to run"
        print(
            f"  {pair.label}  "
            f"[router={pair.router.model}@{pair.router.effort}, "
            f"worker={pair.worker.model}@{pair.worker.effort}]{flag}"
        )
    print()
    for scenario in scenarios:
        r, w, j = scenario_call_counts(scenario)
        print(
            f"  scenario={scenario.name}: {len(scenario.turns)} turn(s), router={r} worker(worst-case)={w} judge={j}"
        )
    print()
    accounting = matrix_call_accounting(pairs, scenarios)
    print(
        f"Total calls: router={accounting.router_calls} worker={accounting.worker_calls} "
        f"judge={accounting.judge_calls} total={accounting.total_calls}"
    )
    print(f"Rough cost estimate: ${accounting.estimated_cost_usd:.2f} (order-of-magnitude only)")


# --------------------------------------------------------------------------
# Live execution.
# --------------------------------------------------------------------------


# Reason-text prefixes pipecat's EvalJudge.evaluate() uses to signal an infra
# failure via verdict="no" rather than a distinct verdict value (see
# pipecat/evals/judge.py: the inference-exception path, the empty-response
# path, and _parse_verdict's unparsable-response fallback).
_JUDGE_INFRA_ERROR_REASON_PREFIXES = (
    "judge call failed",
    "judge returned empty response",
    "could not parse judge response",
)


@dataclass
class TurnOutcome:
    query: str
    status: str  # "ok" | "provider-error" | "timeout" | "setup-error"
    judge_verdict: str | None = None  # "yes" | "no" | "continue" | "judge-error" | None
    judge_reason: str | None = None
    deterministic_action_pass: bool | None = None
    citations_pass: bool | None = None
    routing_ms: float | None = None
    search_ms: float | None = None
    total_ms: float | None = None
    latency_budget_exceeded: bool | None = None
    latency_budget_enforced: bool = False


@dataclass
class CellOutcome:
    pair_label: str
    scenario_name: str
    status: str  # "ok" | "setup-error" | "manifest-rejected"
    error: str | None = None
    turns: list[TurnOutcome] | None = None
    router_timeout_seconds: float | None = None
    foreground_search_timeout_seconds: float | None = None


def _connect_handshake(host: Any) -> dict[str, Any]:
    return {
        "session_id": host.state.session_id,
        "resume_token": host.state.resume_token,
        "proposed_epoch": 1,
        "snapshot_sequence": 0,
    }


def _per_run_config(
    base_config: Config,
    pair: RunPair,
    *,
    max_routing_seconds: float,
    max_latency_seconds: float,
) -> Config:
    router_effort_policy = {"fast": pair.router.effort} if pair.router.effort is not None else {}
    worker_effort_policy = {"deep": pair.worker.effort} if pair.worker.effort is not None else {}
    foreground_search_timeout_seconds = max_latency_seconds
    provider_timeout_seconds = max(
        base_config.provider_timeout_seconds, foreground_search_timeout_seconds + 15
    )
    router_timeout_seconds = min(
        max(base_config.router_timeout_seconds, max_routing_seconds + 5),
        provider_timeout_seconds,
    )
    return replace(
        base_config,
        router_model_policy={"fast": pair.router.model},
        worker_model_policy={"deep": pair.worker.model},
        router_reasoning_effort_policy=router_effort_policy,
        worker_reasoning_effort_policy=worker_effort_policy,
        router_timeout_seconds=router_timeout_seconds,
        foreground_search_timeout_seconds=foreground_search_timeout_seconds,
        provider_timeout_seconds=provider_timeout_seconds,
    )


async def run_cell(
    pair: RunPair,
    scenario: Scenario,
    base_config: Config,
    *,
    judge_model: str,
    max_routing_seconds: float,
    max_latency_seconds: float,
) -> CellOutcome:
    from pipecat.evals.judge import EvalJudge
    from pipecat.evals.services import openai_service

    config = _per_run_config(
        base_config,
        pair,
        max_routing_seconds=max_routing_seconds,
        max_latency_seconds=max_latency_seconds,
    )
    # Pre-call assertion: the per-run Config resolves to exactly this cell's
    # candidate model/effort -- checkable before any paid call happens.
    resolved_router_model = config.resolve_router_model("fast")
    resolved_router_effort = config.resolve_router_reasoning_effort("fast")
    if resolved_router_model != pair.router.model or resolved_router_effort != pair.router.effort:
        return CellOutcome(
            pair_label=pair.label,
            scenario_name=scenario.name,
            status="setup-error",
            error=(
                f"router config did not resolve to the candidate: "
                f"got {resolved_router_model}@{resolved_router_effort}, "
                f"wanted {pair.router.model}@{pair.router.effort}"
            ),
        )

    sink = CollectingMeasurementSink()
    host = build_session_for_run(config, measurement_sink=sink)
    judge = EvalJudge(openai_service({"model": judge_model}))
    turns: list[TurnOutcome] = []
    checked_worker_ids: set[str] = set()

    await host.start()
    try:
        connection = await host.connect(_connect_handshake(host))
        for turn in scenario.turns:
            outcome = TurnOutcome(query=turn.query, status="ok")
            started = time.perf_counter()
            try:
                value = await asyncio.wait_for(
                    host._handle_transcript(turn.query, origin=connection),
                    timeout=config.provider_timeout_seconds + 5,
                )
            except TimeoutError:
                outcome.status = "timeout"
                turns.append(outcome)
                break
            except Exception as exc:  # noqa: BLE001 -- classify, don't crash the matrix
                outcome.status = "provider-error"
                outcome.judge_reason = f"{exc.__class__.__name__}: {exc}"
                turns.append(outcome)
                break
            elapsed_ms = (time.perf_counter() - started) * 1000
            results = value if isinstance(value, tuple) else (value,)
            if len(results) != 1:
                outcome.status = "provider-error"
                outcome.judge_reason = f"expected one result, received {len(results)}"
                turns.append(outcome)
                break
            result = results[0]
            if result.ui_text in SAFE_FALLBACKS:
                outcome.status = "provider-error"
                outcome.judge_reason = "host returned a safe fallback"
                turns.append(outcome)
                break

            # Post-first-delegation worker model/effort assertion: check every
            # worker the registry has not already checked, since distinct
            # topics (e.g. Riga vs Helsinki weather) can each spawn a new
            # worker within the same scenario.
            for registered in host.registry.workers:
                if registered.worker_id in checked_worker_ids:
                    continue
                checked_worker_ids.add(registered.worker_id)
                worker_model = getattr(registered.worker, "model", None)
                worker_effort = getattr(registered.worker, "reasoning_effort", None)
                if worker_model != pair.worker.model or worker_effort != pair.worker.effort:
                    outcome.status = "setup-error"
                    outcome.judge_reason = (
                        f"worker config did not resolve to the candidate: "
                        f"got {worker_model}@{worker_effort}, "
                        f"wanted {pair.worker.model}@{pair.worker.effort}"
                    )
                    turns.append(outcome)
                    return CellOutcome(
                        pair_label=pair.label,
                        scenario_name=scenario.name,
                        status="setup-error",
                        error=outcome.judge_reason,
                        turns=turns,
                    )

            if turn.expect_action is not None:
                routing = getattr(host.state, "routing", None)
                action = getattr(routing, "action", None)
                outcome.deterministic_action_pass = action == turn.expect_action

            if turn.expect_delegated:
                outcome.citations_pass = bool(result.citations)

            stage_metrics = _latest_turn_stage_metrics(sink, elapsed_ms, result.turn_id)
            outcome.routing_ms = round(stage_metrics["routing_ms"], 1)
            outcome.search_ms = round(stage_metrics["search_ms"], 1)
            outcome.total_ms = round(stage_metrics["total_ms"], 1)
            outcome.latency_budget_enforced = pair.is_baseline
            exceeded = (
                stage_metrics["routing_ms"] > max_routing_seconds * 1000
                or stage_metrics["total_ms"] > max_latency_seconds * 1000
            )
            outcome.latency_budget_exceeded = exceeded

            judge.add_user_message(turn.query)
            # result.ui_text is the display projection (GroundedResult.text,
            # populated from the worker's structured display_text field, or
            # the router's own direct-response prose) -- never spoken_text,
            # whose contract forbids citation markers/URLs the judge would
            # otherwise be evaluating against.
            judge.add_assistant_message(result.ui_text)
            if turn.judge_criterion:
                verdict = await judge.evaluate(turn.judge_criterion)
                # pipecat's EvalJudge.evaluate() (pipecat/evals/judge.py) signals
                # infra-style failures through `reason`, not a distinct verdict
                # value -- a raised inference exception ("judge call failed: ..."),
                # an empty LLM response ("judge returned empty response"), and an
                # unparsable response ("could not parse judge response: ...") all
                # come back as verdict="no", indistinguishable from a genuine
                # semantic "no" unless the reason text is checked. Any of these
                # three prefixes -- or an out-of-enum verdict value -- is an
                # infrastructure failure, not a real assertion result.
                if verdict.reason.startswith(_JUDGE_INFRA_ERROR_REASON_PREFIXES) or (
                    verdict.verdict not in {"yes", "no", "continue"}
                ):
                    outcome.judge_verdict = "judge-error"
                else:
                    outcome.judge_verdict = verdict.verdict
                outcome.judge_reason = verdict.reason

            turns.append(outcome)
    finally:
        await host.shutdown()

    return CellOutcome(
        pair_label=pair.label,
        scenario_name=scenario.name,
        status="ok",
        turns=turns,
        router_timeout_seconds=config.router_timeout_seconds,
        foreground_search_timeout_seconds=config.foreground_search_timeout_seconds,
    )


async def run_matrix(
    pairs: tuple[RunPair, ...],
    scenarios: tuple[Scenario, ...],
    base_config: Config,
    *,
    judge_model: str,
    max_routing_seconds: float,
    max_latency_seconds: float,
    manifest_status: ManifestStatus,
) -> list[CellOutcome]:
    outcomes: list[CellOutcome] = []
    for pair in pairs:
        for scenario in scenarios:
            if not candidate_accepted(pair.router, manifest_status) or not candidate_accepted(
                pair.worker, manifest_status
            ):
                outcomes.append(
                    CellOutcome(
                        pair_label=pair.label,
                        scenario_name=scenario.name,
                        status="manifest-rejected",
                        error="one or both candidates are absent from the manifest",
                    )
                )
                continue
            outcomes.append(
                await run_cell(
                    pair,
                    scenario,
                    base_config,
                    judge_model=judge_model,
                    max_routing_seconds=max_routing_seconds,
                    max_latency_seconds=max_latency_seconds,
                )
            )
    return outcomes


# --------------------------------------------------------------------------
# Report aggregation.
# --------------------------------------------------------------------------


_CELL_INFRA_FAILURE_STATUSES = {"provider-error", "timeout", "setup-error", "manifest-rejected"}


def compute_pass_fail(report: dict[str, Any]) -> tuple[str, list[str]]:
    """Aggregate the report's cells/turns into one pass/fail verdict.

    Distinguishes infrastructure failure (a cell/turn never produced a real
    result -- provider error, timeout, judge-error) from semantic failure (a
    real result the judge or a deterministic assertion scored as wrong, or an
    enforced baseline latency budget breach). Both are reported as reasons so
    the caller can tell FAIL-infra from FAIL-semantic apart, but either kind
    fails the run -- a judge-error is not evidence of correctness.
    """
    reasons: list[str] = []
    for cell in report["cells"]:
        label = f"{cell['pair']}/{cell['scenario']}"
        if cell["status"] in _CELL_INFRA_FAILURE_STATUSES:
            reasons.append(f"infra: {label} cell status={cell['status']!r}")
        for turn in cell["turns"]:
            query = turn["query"]
            if turn["judge_verdict"] == "judge-error":
                reasons.append(f"infra: {label} {query!r} judge-error ({turn['judge_reason']})")
            elif turn["judge_verdict"] == "no":
                reasons.append(f"semantic: {label} {query!r} judge verdict=no")
            elif turn["judge_verdict"] == "continue":
                reasons.append(f"semantic: {label} {query!r} judge verdict=continue")
            if turn["deterministic_action_pass"] is False:
                reasons.append(f"semantic: {label} {query!r} deterministic action assertion failed")
            if turn["citations_pass"] is False:
                reasons.append(f"semantic: {label} {query!r} citations assertion failed")
            if turn["latency_budget_enforced"] and turn["latency_budget_exceeded"]:
                reasons.append(f"semantic: {label} {query!r} enforced latency budget exceeded")
    return ("FAIL" if reasons else "PASS"), reasons


def build_report(outcomes: list[CellOutcome], *, judge_model: str) -> dict[str, Any]:
    report: dict[str, Any] = {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "judge_model": judge_model,
        "repetition_count_per_cell": 1,
        "repetition_note": "n=1 per cell -- exploratory/noise-dominated given cost constraints",
        "cells": [
            {
                "pair": outcome.pair_label,
                "scenario": outcome.scenario_name,
                "status": outcome.status,
                "error": outcome.error,
                "router_timeout_seconds": outcome.router_timeout_seconds,
                "foreground_search_timeout_seconds": outcome.foreground_search_timeout_seconds,
                "turns": [
                    {
                        "query": turn.query,
                        "status": turn.status,
                        "judge_verdict": turn.judge_verdict,
                        "judge_reason": turn.judge_reason,
                        "deterministic_action_pass": turn.deterministic_action_pass,
                        "citations_pass": turn.citations_pass,
                        "routing_ms": turn.routing_ms,
                        "search_ms": turn.search_ms,
                        "total_ms": turn.total_ms,
                        "latency_budget_exceeded": turn.latency_budget_exceeded,
                        "latency_budget_enforced": turn.latency_budget_enforced,
                    }
                    for turn in (outcome.turns or [])
                ],
            }
            for outcome in outcomes
        ],
    }
    overall_status, failure_reasons = compute_pass_fail(report)
    report["overall_status"] = overall_status
    report["failure_reasons"] = failure_reasons
    return report


def print_report_summary(report: dict[str, Any]) -> None:
    for cell in report["cells"]:
        print(f"[{cell['status']}] {cell['pair']} / {cell['scenario']}")
        if cell["error"]:
            print(f"    error: {cell['error']}")
        for turn in cell["turns"]:
            bits = [f"status={turn['status']}"]
            if turn["judge_verdict"] is not None:
                bits.append(f"judge={turn['judge_verdict']}")
            if turn["deterministic_action_pass"] is not None:
                bits.append(f"action_pass={turn['deterministic_action_pass']}")
            if turn["citations_pass"] is not None:
                bits.append(f"citations_pass={turn['citations_pass']}")
            if turn["total_ms"] is not None:
                budget = "blocking" if turn["latency_budget_enforced"] else "report-only"
                bits.append(
                    f"total_ms={turn['total_ms']} (budget={budget}, exceeded={turn['latency_budget_exceeded']})"
                )
            print(f"    - {turn['query']!r}: {' '.join(bits)}")
    print(f"\noverall: {report['overall_status']}")
    for reason in report["failure_reasons"]:
        print(f"  - {reason}")


# --------------------------------------------------------------------------
# CLI.
# --------------------------------------------------------------------------


def _resolve_pairs(args: argparse.Namespace) -> tuple[RunPair, ...]:
    if args.router or args.worker:
        router = ROUTER_CANDIDATES_BY_LABEL[args.router or "baseline"]
        worker = WORKER_CANDIDATES_BY_LABEL[args.worker or "baseline"]
        return (RunPair(router, worker),)
    return full_matrix_pairs() if args.full_matrix else default_sweep_pairs()


def _resolve_scenarios(args: argparse.Namespace) -> tuple[Scenario, ...]:
    if args.scenario:
        return (SCENARIOS_BY_NAME[args.scenario],)
    return SCENARIOS


def _confirm_spend(
    accounting: CallAccounting, *, max_calls: int | None, max_cost: float | None, assume_yes: bool
) -> bool:
    exceeds = (max_calls is not None and accounting.total_calls > max_calls) or (
        max_cost is not None and accounting.estimated_cost_usd > max_cost
    )
    if not exceeds:
        return True
    print(
        f"WARNING: this run would make {accounting.total_calls} live paid calls "
        f"(~${accounting.estimated_cost_usd:.2f}), exceeding --max-calls={max_calls}/--max-cost={max_cost}."
    )
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        print(
            "refusing to proceed without confirmation (not an interactive terminal); pass --yes to override"
        )
        return False
    reply = input("Proceed anyway? [y/N] ").strip().lower()
    return reply in {"y", "yes"}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the matrix/cost estimate; make zero live calls",
    )
    parser.add_argument(
        "--full-matrix", action="store_true", help="run the full router x worker cross product"
    )
    parser.add_argument("--router", choices=sorted(ROUTER_CANDIDATES_BY_LABEL), default=None)
    parser.add_argument("--worker", choices=sorted(WORKER_CANDIDATES_BY_LABEL), default=None)
    parser.add_argument("--scenario", choices=sorted(SCENARIOS_BY_NAME), default=None)
    parser.add_argument("--max-calls", type=int, default=None)
    parser.add_argument("--max-cost", type=float, default=None)
    parser.add_argument("--yes", action="store_true", help="skip the spend-confirmation prompt")
    parser.add_argument("--i-know-the-manifest-is-stale", action="store_true", dest="allow_stale")
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--judge-model", default=DEFAULT_JUDGE_MODEL)
    parser.add_argument("--max-routing-seconds", type=float, default=DEFAULT_MAX_ROUTING_SECONDS)
    parser.add_argument("--max-latency-seconds", type=float, default=DEFAULT_MAX_LATENCY_SECONDS)
    parser.add_argument(
        "--out", type=Path, default=None, help="report output path (default: printed only)"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    pairs = _resolve_pairs(args)
    scenarios = _resolve_scenarios(args)
    manifest_status = load_manifest_status(args.manifest_path)

    if args.dry_run:
        print_matrix_preview(pairs, scenarios, judge_model=args.judge_model, status=manifest_status)
        return 0

    candidates = tuple({c for pair in pairs for c in (pair.router, pair.worker)})
    try:
        require_manifest_ok_for_live_run(
            manifest_status,
            allow_stale=args.allow_stale,
            candidates=candidates,
            judge_model=args.judge_model,
        )
    except ManifestError as exc:
        print(f"refusing to run: {exc}", file=sys.stderr)
        return 1

    base_config = load_config()
    if not base_config.openai_api_key:
        print(
            "refusing to run: no OPENAI_API_KEY credential resolved by load_config()",
            file=sys.stderr,
        )
        return 1

    accounting = matrix_call_accounting(pairs, scenarios)
    if not _confirm_spend(
        accounting, max_calls=args.max_calls, max_cost=args.max_cost, assume_yes=args.yes
    ):
        return 1

    outcomes = asyncio.run(
        run_matrix(
            pairs,
            scenarios,
            base_config,
            judge_model=args.judge_model,
            max_routing_seconds=args.max_routing_seconds,
            max_latency_seconds=args.max_latency_seconds,
            manifest_status=manifest_status,
        )
    )
    report = build_report(outcomes, judge_model=args.judge_model)
    print_report_summary(report)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2, sort_keys=True))
        print(f"\nreport written to {args.out}")
    return 0 if report["overall_status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
