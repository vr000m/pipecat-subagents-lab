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
import math
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from evals.scenarios import SCENARIOS, SCENARIOS_BY_NAME, Scenario
from scripts.eval_common import (
    DEFAULT_JUDGE_MODEL,
    DEFAULT_MANIFEST_RELATIVE_PATH,
    MANIFEST_VERSION,
    REPO_ROOT,
    ROUTER_BASELINE,
    ROUTER_CANDIDATES,
    ROUTER_MANIFEST_TOOLS,
    SAFE_FALLBACKS,
    TIMEOUT_FALLBACKS,
    WORKER_BASELINE,
    WORKER_CANDIDATES,
    WORKER_MANIFEST_TOOLS,
    Candidate,
    CollectingMeasurementSink,
    _latest_turn_stage_metrics,
    build_judge_llm_service,
    build_session_for_run,
    confined_output_path,
    effective_effort_for_manifest_lookup,
    error_text,
    git_head,
    sanitize_reason,
    strip_control_chars,
    write_no_follow,
)
from server.config import Config, load_config

# REPO_ROOT/DEFAULT_JUDGE_MODEL/DEFAULT_MANIFEST_RELATIVE_PATH/MANIFEST_VERSION
# hoisted from scripts/eval_common.py (round 5, Architecture lens finding 2;
# REPO_ROOT hoisted in round 7, Architecture finding 12): this consumer and
# scripts/verify_eval_candidates.py's producer reference the same objects,
# not two independently-maintained "must match" copies.
_REPO_ROOT = REPO_ROOT
DEFAULT_MANIFEST_PATH = _REPO_ROOT / DEFAULT_MANIFEST_RELATIVE_PATH
DEFAULT_MAX_ROUTING_SECONDS = 15.0
DEFAULT_MAX_LATENCY_SECONDS = 60.0
# Additive margin between the report-only latency budget and the internal
# provider-level foreground-search timeout, so a slow-but-successful
# candidate isn't truncated into a safe fallback before it can be scored.
_FOREGROUND_TIMEOUT_HEADROOM_SECONDS = 30.0

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

# The OpenAI SDK clients this runner drives (via build_session_for_run() ->
# LazyRouterProvider / WebSearchWorker, and the judge's OpenAILLMService) all
# leave the SDK's client-level max_retries=2 default in place: a transient
# 429/5xx/timeout is silently retried up to twice more, so a single logical
# turn can issue up to 3 real HTTP requests to the provider while this
# runner's own call-count/cost estimate -- and therefore the
# --max-calls/--max-cost confirmation gate -- would otherwise only see 1.
# Applied at the matrix-accounting layer (matrix_call_accounting()), not
# inside scenario_call_counts() itself, which stays a pure per-scenario
# nominal count. See round-5 gauntlet finding 11.
_OPENAI_SDK_DEFAULT_MAX_RETRIES = 2
_RETRY_WORST_CASE_MULTIPLIER = 1 + _OPENAI_SDK_DEFAULT_MAX_RETRIES


# --------------------------------------------------------------------------
# Candidate matrix (Objective's candidate matrix, verified live by Phase 0).
#
# Candidate/ROUTER_BASELINE/ROUTER_CANDIDATES/WORKER_BASELINE/WORKER_CANDIDATES
# and effective_effort_for_manifest_lookup() are hoisted to
# scripts/eval_common.py (round 7, Architecture finding 11) -- this runner and
# scripts/verify_eval_candidates.py's Phase 0 verifier both need the same
# candidate matrix, and previously kept it as two independently-shaped
# copies. Only the *_SELECTABLE_BY_LABEL dicts below are specific to this
# runner's --router/--worker label lookup, so they stay local.
# --------------------------------------------------------------------------

# Includes the baseline, unlike ROUTER_CANDIDATES/WORKER_CANDIDATES -- the
# stem-only naming difference (CANDIDATES vs CANDIDATES_BY_LABEL, same
# "CANDIDATES" stem) was a load-bearing-but-undocumented membership
# difference (round 7 gauntlet, Architecture finding 18); SELECTABLE names
# what --router/--worker actually accept (any label a candidate is
# registered under, baseline included), not just the non-baseline sweep
# candidates.
ROUTER_SELECTABLE_BY_LABEL = {c.label: c for c in (ROUTER_BASELINE, *ROUTER_CANDIDATES)}
WORKER_SELECTABLE_BY_LABEL = {c.label: c for c in (WORKER_BASELINE, *WORKER_CANDIDATES)}


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


# scripts/eval_common.py's MANIFEST_VERSION -- the manifest producer
# (scripts/verify_eval_candidates.py) and this consumer both import the same
# object, an identity relationship a test enforces (see
# tests/test_eval_model_comparison.py), rather than two independently
# hand-maintained "must match" copies of the schema version constant.
_SUPPORTED_MANIFEST_VERSION = MANIFEST_VERSION


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


# _current_source_commit() hoisted to scripts/eval_common.py's git_head()
# (round 7 gauntlet finding 12) -- was a near-identical copy of
# scripts/verify_eval_candidates.py's own _git_head(), cross-referencing it in
# comments instead of sharing code. Call sites below use git_head() directly.


# The manifest's source_commit attests to the request shape these specific
# files produce (router policy, worker tool wiring, config defaults, router
# structured-output schema, worker/registry construction) -- the files
# scripts/verify_eval_candidates.py's probes actually exercise, directly or
# via what they import. A commit match alone is not enough: if any of these
# carry uncommitted edits, `git rev-parse HEAD` still matches the manifest
# even though the tree it describes is no longer the tree in front of the
# runner.
#
# server/registry.py and server/structured_outputs.py added in round 7
# (Codex adversarial gate finding 2): registry.py threads the resolved
# worker model/effort through WebSearchWorker construction (the shape
# _build_worker_kwargs()/_build_router_kwargs() in
# scripts/verify_eval_candidates.py reproduce), and structured_outputs.py
# builds the router/worker's `text` structured-output format -- editing
# either after Phase 0 verified a request shape would silently invalidate
# that verification without tripping staleness.
_MANIFEST_ATTESTED_PATHS = (
    "server/router.py",
    "server/workers/web_search.py",
    "server/config.py",
    "server/registry.py",
    "server/structured_outputs.py",
)


def _source_tree_dirty() -> bool | None:
    """True if any manifest-attested source file has uncommitted changes.

    Returns ``None`` (unverifiable) rather than ``False`` when the git
    invocation itself fails -- mirrors ``git_head()``'s fail-closed contract,
    so a broken git toolchain can't be read as "clean".
    """
    try:
        completed = subprocess.run(
            ["git", "status", "--porcelain", "--", *_MANIFEST_ATTESTED_PATHS],
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
    return bool(completed.stdout.strip())


def _request_kwargs_shape_ok(kind: str, request_kwargs: dict[str, Any]) -> bool:
    """Confirm ``request_kwargs`` actually carries the production-equivalent
    shape Phase 0's probe for this role established -- not just that *some*
    dict is present (round 7 gauntlet, Codex adversarial gate finding 1).

    An accepted manifest entry only proves what it can be shown to have
    probed: a hand-edited or faulty-verifier-produced entry could carry
    ``request_kwargs={"model": model}`` and pass the pre-existing
    ``isinstance(..., dict)``-only check while never having exercised
    ``store=False``, the structured-output ``text`` format, the worker's
    ``tool_choice``, or the judge's message shape at all. Mirrors
    ``scripts/verify_eval_candidates.py``'s ``_build_router_kwargs``/
    ``_build_worker_kwargs``/``_judge_kwargs`` -- checking presence and
    load-bearing values of the keys those builders always set, not every key
    (``timeout``/``instructions``/``include`` vary in ways that don't change
    what "this candidate is callable" means).
    """
    if kind == "router":
        return (
            request_kwargs.get("store") is False
            and isinstance(request_kwargs.get("text"), dict)
            and isinstance(request_kwargs.get("input"), str)
            and bool(request_kwargs.get("input"))
        )
    if kind == "worker":
        tools = request_kwargs.get("tools")
        return (
            request_kwargs.get("store") is False
            and request_kwargs.get("tool_choice") == "required"
            and isinstance(request_kwargs.get("text"), dict)
            and isinstance(tools, list)
            and any(isinstance(t, dict) and t.get("type") == "web_search" for t in tools)
        )
    if kind == "judge":
        messages = request_kwargs.get("messages")
        return (
            isinstance(messages, list)
            and len(messages) >= 1
            and all(isinstance(m, dict) and "role" in m and "content" in m for m in messages)
        )
    # "model_existence" (and any future kind) carries no request-shape
    # contract to validate -- scripts/verify_eval_candidates.py's own
    # build_plan() never attaches request_kwargs beyond {"model": model} for
    # it.
    return True


def load_manifest_status(manifest_path: Path) -> ManifestStatus:
    """Load Phase 0's manifest and index the (kind, model, effort) tuples it accepted.

    Never raises: a missing/malformed manifest is reported via
    ``ManifestStatus.exists=False``/an empty accepted set, and it is the
    caller's job to decide whether that is fatal (it is, for any live cell;
    it is a printed warning for ``--dry-run``).
    """
    current_commit = git_head()
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
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
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
    # A manifest whose declared schema version this loader doesn't recognize
    # is treated the same as malformed: a future/hand-edited schema could
    # change what "accepted" means for a given entry shape, and silently
    # reading it under today's assumptions would be worse than refusing.
    if manifest.get("manifest_version") != _SUPPORTED_MANIFEST_VERSION:
        return ManifestStatus(
            path=manifest_path,
            exists=True,
            source_commit=None,
            current_commit=current_commit,
            stale=True,
            accepted=frozenset(),
        )
    source_commit = manifest.get("source_commit")
    # Fail closed, not open: an unverifiable identity -- source_commit is
    # missing from the manifest, or the current-commit lookup itself failed
    # (e.g. git unavailable) -- must be treated as stale/rejected for a live
    # run, not silently accepted just because there was nothing to compare
    # against. Only a same-valued, both-resolved comparison counts as fresh.
    identity_verifiable = current_commit is not None and isinstance(source_commit, str)
    # A HEAD match alone doesn't mean the manifest still describes the
    # checkout: uncommitted edits to the files it attests (router policy,
    # worker tool wiring, config defaults) leave `git rev-parse HEAD`
    # unchanged while the actual request shape has moved. Fail closed here
    # too -- an unverifiable dirty-check (git failure) is treated as dirty,
    # not clean.
    tree_dirty = _source_tree_dirty()
    stale = not identity_verifiable or source_commit != current_commit or tree_dirty is not False
    accepted: set[tuple[str, str, str | None]] = set()
    for entry in manifest["results"]:
        if not isinstance(entry, dict) or entry.get("accepted") is not True:
            continue
        kind = entry.get("kind")
        model = entry.get("model")
        effort = entry.get("effort")
        if not isinstance(kind, str) or not isinstance(model, str):
            continue
        # A present-but-malformed `effort` (e.g. 0, [], a non-null non-string
        # value) is rejected outright, not silently coerced to None -- a
        # coercion here would make a manifest entry that never actually
        # verified an unset-effort request read as if it had (round 5,
        # Security lens finding 9). Fail-closed matches every other shape
        # violation in this loop: reject just this entry, not the whole
        # manifest.
        if effort is not None and not isinstance(effort, str):
            continue
        # request_kwargs is Phase 0's actual evidence that a given request
        # shape was probed at all -- an entry missing it, or carrying a
        # non-dict value, asserts "accepted" with nothing behind it (a
        # hand-edited or faulty-verifier-produced manifest could omit it
        # entirely). Reject rather than accept-with-no-evidence.
        if not isinstance(entry.get("request_kwargs"), dict):
            continue
        # Beyond "some dict is present": the dict's actual keys/values must
        # match the production request shape Phase 0's probe for this role
        # established (round 7 gauntlet, Codex adversarial gate finding 1).
        if not _request_kwargs_shape_ok(kind, entry["request_kwargs"]):
            continue
        # Each role's manifest entry must declare the specific request-shape
        # element Phase 0's probe for that role actually established --
        # kind/model/effort alone would authorize a shape Phase 0 never
        # verified for a hand-edited or faulty-verifier-produced manifest.
        # Mirrors scripts/verify_eval_candidates.py's per-role probe, using
        # the same ROUTER_MANIFEST_TOOLS/WORKER_MANIFEST_TOOLS vocabulary the
        # producer emits (scripts/eval_common.py -- round 5, Architecture
        # lens finding 2):
        # - worker: probed with the `web_search` tool attached
        #   (`_build_worker_kwargs`);
        # - router: probed with the structured-output `text` format attached
        #   (`_build_router_kwargs`) -- recorded as `tools=["text"]`;
        # - judge: probed via the Chat Completions path with no tools
        #   attached at all (`_judge_kwargs`) -- kind/model already fully
        #   describe what was verified, so no additional tools check applies.
        if kind == "worker":
            tools = entry.get("tools")
            if not isinstance(tools, list) or not set(WORKER_MANIFEST_TOOLS).issubset(tools):
                continue
        elif kind == "router":
            tools = entry.get("tools")
            if not isinstance(tools, list) or not set(ROUTER_MANIFEST_TOOLS).issubset(tools):
                continue
        accepted.add((kind, model, effort))
    return ManifestStatus(
        path=manifest_path,
        exists=True,
        source_commit=source_commit if isinstance(source_commit, str) else None,
        current_commit=current_commit,
        stale=stale,
        accepted=frozenset(accepted),
    )


def candidate_accepted(candidate: Candidate, status: ManifestStatus) -> bool:
    effort = effective_effort_for_manifest_lookup(candidate)
    # candidate.role IS the manifest's "kind" vocabulary ("router"/"worker")
    # -- candidate_manifest_kind() was a one-line, single-call-site
    # indirection over that fact (round 7 gauntlet, Architecture finding 18).
    return (candidate.role, candidate.model, effort) in status.accepted


def judge_accepted(judge_model: str, status: ManifestStatus) -> bool:
    return ("judge", judge_model, None) in status.accepted


def require_manifest_ok_for_live_run(
    status: ManifestStatus,
    *,
    allow_stale: bool,
    candidates: tuple[Candidate, ...],
    judge_model: str,
) -> None:
    """Fatal, pre-flight manifest check for a live run. Never called under --dry-run.

    ``main()`` calls this once, for every candidate in the resolved matrix,
    before ``run_matrix()`` starts -- so under the CLI's normal control flow
    ``run_matrix()``'s own per-cell ``candidate_accepted()`` check (see its
    docstring) can never actually trigger. That per-cell check is kept
    anyway as defense-in-depth for any caller that invokes ``run_matrix()``
    directly without going through this preflight (tests, or a future
    embedding) -- both checks call the same ``candidate_accepted()``
    predicate, so they can never disagree about what's accepted.
    """
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
        # Prints the effective (resolved) effort, not the raw candidate.effort
        # -- for the baseline router candidate, .effort is None but the
        # manifest lookup (candidate_accepted(), just above) actually checked
        # the resolved "minimal" value; printing the raw None here would show
        # an operator a combination that isn't the one actually looked up.
        described = ", ".join(
            f"{c.role}:{c.model}@{effective_effort_for_manifest_lookup(c)}" for c in missing
        )
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
    """Router/worker/judge call counts for one scenario's spend-confirmation estimate.

    ``worker_calls`` is a *worst case*, not the expected count: it counts
    every turn as a potential worker call, not just the turns the scenario
    definition marks ``expect_delegated=True``. A turn marked
    ``expect_delegated=False`` (e.g. a greeting) can still be misrouted to
    the worker by a routing regression -- exactly the failure mode this
    scenario type exists to catch -- and if the estimate only counted
    ``expect_delegated=True`` turns, a run that hits that regression would
    silently exceed an operator's ``--max-calls``/``--max-cost`` limit
    without ever triggering the confirmation gate. This is a pre-run
    estimate only; it does not change the actual runtime calls made.
    """
    router_calls = len(scenario.turns)
    worker_calls = len(scenario.turns)
    judge_calls = sum(1 for turn in scenario.turns if turn.judge_criterion)
    return router_calls, worker_calls, judge_calls


def matrix_call_accounting(
    pairs: tuple[RunPair, ...], scenarios: tuple[Scenario, ...]
) -> CallAccounting:
    """Worst-case call/cost accounting for the spend-confirmation gate.

    Includes the SDK's default-retry worst case (``_RETRY_WORST_CASE_MULTIPLIER``)
    on top of ``scenario_call_counts()``'s nominal one-request-per-turn count,
    so an operator confirming ``--max-calls``/``--max-cost`` sees the true
    worst-case exposure, not just the happy-path call count.
    """
    router_calls = worker_calls = judge_calls = 0
    for _pair in pairs:
        for scenario in scenarios:
            r, w, j = scenario_call_counts(scenario)
            router_calls += r
            worker_calls += w
            judge_calls += j
    return CallAccounting(
        router_calls=router_calls * _RETRY_WORST_CASE_MULTIPLIER,
        worker_calls=worker_calls * _RETRY_WORST_CASE_MULTIPLIER,
        judge_calls=judge_calls * _RETRY_WORST_CASE_MULTIPLIER,
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
        # Effective (resolved) effort, not the raw candidate.effort -- see
        # require_manifest_ok_for_live_run()'s matching fix for why.
        print(
            f"  {pair.label}  "
            f"[router={pair.router.model}@{effective_effort_for_manifest_lookup(pair.router)}, "
            f"worker={pair.worker.model}@{effective_effort_for_manifest_lookup(pair.worker)}]"
            f"{flag}"
        )
    print()
    for scenario in scenarios:
        r, w, j = scenario_call_counts(scenario)
        print(
            f"  scenario={scenario.name}: {len(scenario.turns)} turn(s), router={r} "
            f"worker(worst-case, assumes every turn could route to a worker)={w} judge={j}"
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
    status: str  # "ok" | "provider-error" | "timeout" | "setup-error" | "turn-error" | "skipped"
    judge_verdict: str | None = None  # "yes" | "no" | "continue" | "judge-error" | None
    judge_reason: str | None = None
    deterministic_action_pass: bool | None = None
    citations_pass: bool | None = None
    # Round 5, Architecture lens finding 12: whether the router's routing
    # action for this turn (new_worker/existing_worker vs
    # direct/unsupported/clarify) matched the scenario's own
    # Turn.expect_delegated declaration -- a routing regression that produces
    # a superficially valid final reply but takes the wrong path (e.g.
    # answers directly instead of delegating, or vice versa) is caught here
    # even when the reply text and judge verdict would otherwise look fine.
    worker_presence_pass: bool | None = None
    routing_ms: float | None = None
    search_ms: float | None = None
    total_ms: float | None = None
    latency_budget_exceeded: bool | None = None
    latency_budget_enforced: bool = False


@dataclass
class CellOutcome:
    pair_label: str
    scenario_name: str
    status: str  # "ok" | "provider-error" | "timeout" | "setup-error" | "turn-error" | "manifest-rejected"
    error: str | None = None
    turns: list[TurnOutcome] | None = None
    router_timeout_seconds: float | None = None
    foreground_search_timeout_seconds: float | None = None


def _skipped_turn_outcomes(scenario: Scenario, *, already_run: int) -> list[TurnOutcome]:
    """``TurnOutcome`` placeholders, by index, for every turn a cell never attempted.

    Shared by every "cell never ran (all or some of) its turns" producer --
    ``run_cell()``'s pre-call config-mismatch return, ``run_matrix()``'s
    manifest-rejected cell, and ``run_cell()``'s own end-of-function backfill
    for a cell that raised partway through -- so a reader comparing two
    "never ran" cells sees the same ``[skipped, skipped, ...]`` shape
    regardless of which producer built them, instead of one producer
    returning ``[]`` and another backfilling. Backfilled by index, not by
    deduping on query text, so a scenario with a repeated query string is
    still backfilled correctly.
    """
    return [TurnOutcome(query=t.query, status="skipped") for t in scenario.turns[already_run:]]


def _never_ran_cell(
    pair: RunPair,
    scenario: Scenario,
    status: str,
    error: str,
    *,
    config: Config | None = None,
) -> CellOutcome:
    """Build a ``CellOutcome`` for a cell that never attempted any turn.

    Applies the same turn-backfill logic as a cell that started but never
    finished (``_skipped_turn_outcomes``), and threads through the per-run
    provider timeouts when a resolved ``config`` is available (it is for a
    router-config mismatch, caught after ``_per_run_config()`` has already
    run; it isn't for a manifest rejection, caught before any per-run
    ``Config`` exists).
    """
    return CellOutcome(
        pair_label=pair.label,
        scenario_name=scenario.name,
        status=status,
        error=error,
        turns=_skipped_turn_outcomes(scenario, already_run=0),
        router_timeout_seconds=config.router_timeout_seconds if config is not None else None,
        foreground_search_timeout_seconds=(
            config.foreground_search_timeout_seconds if config is not None else None
        ),
    )


def _sanitize_for_judge(text: str, *, max_len: int = 4000) -> str:
    """Light defense-in-depth before feeding worker-sourced (hosted web
    search) text into the judge LLM's context: strips ASCII control
    characters (other than newline/tab, via ``strip_control_chars`` -- the
    same filter ``error_text`` uses) and caps length. This is not a
    prompt-injection defense -- the judge's own verdict schema constrains its
    output regardless of what's in its context -- it just bounds how much raw,
    externally-sourced content reaches the judge unmodified.
    """
    return strip_control_chars(text)[:max_len]


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
    # Headroom beyond the report-only latency budget, not equal to it: the
    # foreground timeout is an internal provider-level cutoff, not the
    # scoring budget. Setting them equal means a non-baseline worker that
    # completes successfully just after max_latency_seconds gets hard-killed
    # by this timeout before its result/citations arrive, instead of
    # completing and being scored as a (separately, report-only) latency
    # miss -- collapsing "too slow" and "never got an answer" into the same
    # outcome for a candidate that was actually working.
    foreground_search_timeout_seconds = max_latency_seconds + _FOREGROUND_TIMEOUT_HEADROOM_SECONDS
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
    turns: list[TurnOutcome] = []
    # Worker IDs are minted as f"worker-{len(self._workers)+1}"
    # (server/registry.py) and can only repeat if a worker is evicted --
    # WorkerRegistry.remove() unconditionally raises RuntimeError under this
    # registry's first-slice no-eviction policy, so within one cell's
    # lifetime every worker_id this loop sees is guaranteed unique. Tracked
    # by raw ID rather than a synthetic identity for that reason.
    checked_worker_ids: set[str] = set()
    # Tracks the cell-level status a turn-level break should propagate as --
    # compute_pass_fail() also checks each turn's own status, so this exists
    # so a reader of just the cell's top-level status (without inspecting
    # every turn) still sees that the cell didn't complete cleanly.
    cell_status = "ok"
    cell_error: str | None = None
    # None until build_session_for_run() actually succeeds -- the finally
    # block below only shuts a host down that was actually constructed, so an
    # exception raised during setup (config resolution, judge construction)
    # before the host exists can't call shutdown() on nothing.
    host: Any | None = None
    config: Config | None = None
    # Set True immediately before the current turn's paid call, and stays
    # True until that turn's TurnOutcome is appended to `turns` (every
    # explicit break path below appends before breaking; the only way to
    # reach the outer except handler with turn_started True is an exception
    # that escaped from *after* the paid call but *before* that append --
    # e.g. _latest_turn_stage_metrics() raising, or a malformed result's
    # attribute access failing). Distinguishes "a turn actually ran (and was
    # billed) but the code around it blew up" from "nothing was attempted
    # yet", which `len(turns)` alone can't do for the turn that's mid-flight
    # when the exception hits.
    turn_started = False
    in_flight_query: str | None = None

    try:
        from pipecat.evals.judge import EvalJudge

        config = _per_run_config(
            base_config,
            pair,
            max_routing_seconds=max_routing_seconds,
            max_latency_seconds=max_latency_seconds,
        )
        # Pre-call assertion: the per-run Config resolves to exactly this
        # cell's candidate model/effort -- checkable before any paid call
        # happens.
        resolved_router_model = config.resolve_router_model("fast")
        resolved_router_effort = config.resolve_router_reasoning_effort("fast")
        if (
            resolved_router_model != pair.router.model
            or resolved_router_effort != pair.router.effort
        ):
            return _never_ran_cell(
                pair,
                scenario,
                "setup-error",
                (
                    f"router config did not resolve to the candidate: "
                    f"got {resolved_router_model}@{resolved_router_effort}, "
                    f"wanted {pair.router.model}@{pair.router.effort}"
                ),
                config=config,
            )

        sink = CollectingMeasurementSink()
        # Assigned to the outer `host` (not a local) as soon as it's
        # constructed, before judge construction can raise and leak it.
        host = build_session_for_run(config, measurement_sink=sink)
        judge = EvalJudge(build_judge_llm_service(judge_model, config.openai_api_key))

        await host.start()
        connection = await host.connect(_connect_handshake(host))
        for turn in scenario.turns:
            outcome = TurnOutcome(query=turn.query, status="ok")
            started = time.perf_counter()
            turn_started = True
            in_flight_query = turn.query
            try:
                value = await asyncio.wait_for(
                    host._handle_transcript(turn.query, origin=connection),
                    timeout=config.provider_timeout_seconds + 5,
                )
            except TimeoutError:
                outcome.status = "timeout"
                turns.append(outcome)
                cell_status, cell_error = "timeout", f"turn {turn.query!r} timed out"
                break
            except Exception as exc:  # noqa: BLE001 -- classify, don't crash the matrix
                outcome.status = "provider-error"
                outcome.judge_reason = error_text(exc, credential=config.openai_api_key)
                turns.append(outcome)
                cell_status, cell_error = "provider-error", outcome.judge_reason
                break
            elapsed_ms = (time.perf_counter() - started) * 1000
            results = value if isinstance(value, tuple) else (value,)
            # A multi-item tuple is a genuine, successful outcome for a
            # multi_intent turn (server/pipeline.py's `_handle_multi_intent`
            # returns `tuple(committed)`, one entry per work item) -- not an
            # infra failure. `len(results) != 1` previously misclassified
            # every multi-intent turn as "provider-error" and aborted the
            # cell (round 7 gauntlet, Logic lens finding 6). Only a truly
            # empty result (nothing committed at all) has no result to score.
            if not results:
                outcome.status = "provider-error"
                outcome.judge_reason = "expected at least one result, received none"
                turns.append(outcome)
                cell_status, cell_error = "provider-error", outcome.judge_reason
                break
            # Judge/citations scoring below is fed from the first committed
            # result only -- a deliberate simplification, not full multi-item
            # scoring (this eval suite's own scenarios never route to
            # multi_intent; see evals/scenarios.py). worker_presence_pass
            # below still inspects every committed result's worker_id, so a
            # routing regression that fans a should-be-direct turn out to
            # several workers is still caught even though only one worker's
            # reply text reaches the judge.
            result = results[0]
            # _handle_transcript() has documented return paths that return
            # None (server/pipeline.py, when a capable connection's retained
            # work item goes background-status-only) or a bare str transcript
            # -- neither carries a .ui_text attribute, and a bare
            # AttributeError from the access below would escape to the outer
            # except handler as an undiagnostic "turn-error". Check
            # explicitly so the report names what actually came back instead.
            if result is None or not hasattr(result, "ui_text"):
                outcome.status = "provider-error"
                outcome.judge_reason = (
                    f"_handle_transcript returned {type(result).__name__} instead of a "
                    "result with .ui_text (e.g. the background-status no-op path, or a "
                    "bare str transcript)"
                )
                turns.append(outcome)
                cell_status, cell_error = "provider-error", outcome.judge_reason
                break
            if result.ui_text in TIMEOUT_FALLBACKS:
                outcome.status = "timeout"
                outcome.judge_reason = "host returned the foreground-search-timeout placeholder"
                turns.append(outcome)
                cell_status, cell_error = "timeout", outcome.judge_reason
                break
            routing = getattr(host.state, "routing", None)
            # RoutingState (server/contracts.py) carries the turn_id it was
            # decided for. A stale read is possible: if this turn's own
            # routing/dispatch call fails before ever assigning a new
            # decision, host.state.routing still holds the PRIOR turn's
            # object. Without this check, a genuine new infra failure on
            # this turn could be misread as the previous turn's
            # action="unsupported", misclassifying a real provider error as
            # a semantic outcome and skipping infra-failure handling below.
            # Only trust routing_action when it was actually decided for
            # *this* turn's result.
            routing_action = (
                getattr(routing, "action", None)
                if getattr(routing, "turn_id", None) == result.turn_id
                else None
            )
            # A genuine, on-topic `action="unsupported"` routing decision (the
            # model correctly reached the router but decided -- rightly or
            # wrongly -- that the request needed a capability it doesn't have)
            # renders through the exact same _CAPABILITY_UNAVAILABLE_TEXT
            # fallback as a true infrastructure failure (RoutingValidationError/
            # UnsupportedWorkerType raised during dispatch, server/pipeline.py).
            # Text alone can't tell them apart, but the routing action can: the
            # infra-failure path never sets action="unsupported" (it's only
            # reached after a new_worker/existing_worker decision whose dispatch
            # then failed). Round 5, Security/Architecture lens finding 10 --
            # without this check, a genuine semantic routing miss (the model
            # wrongly decided a real weather/release request was unsupported)
            # was misattributed to "provider-error" and never reached judge
            # scoring. When routing_action == "unsupported", fall through to
            # the deterministic/judge scoring below instead of short-circuiting
            # as infrastructure failure.
            if result.ui_text in SAFE_FALLBACKS and routing_action != "unsupported":
                outcome.status = "provider-error"
                outcome.judge_reason = "host returned a safe fallback"
                turns.append(outcome)
                cell_status, cell_error = "provider-error", outcome.judge_reason
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
                    cell_status, cell_error = "setup-error", outcome.judge_reason
                    break
            if cell_status != "ok":
                break

            # routing_action is None on two return paths that never reach
            # server/pipeline.py's `isinstance(outcome.decision,
            # RoutingDecision)` branch at all: multi_intent and
            # continue_pending both `return` earlier, so `host.state.routing`
            # is never set for these turns and the staleness guard above
            # correctly resolves to None -- not "no delegation happened"
            # (round 7 gauntlet, Logic lens finding 6). A
            # routing_action-dependent assertion must therefore stay
            # unevaluated (None), not silently score False, whenever
            # routing_action is None.
            if turn.expect_action is not None:
                outcome.deterministic_action_pass = (
                    None if routing_action is None else routing_action == turn.expect_action
                )

            # Worker presence/absence assertion (round 5, Architecture lens
            # finding 12): a routing regression can produce a superficially
            # valid final reply while taking the wrong path -- e.g. a
            # "should be direct" turn that actually delegates, or vice versa
            # -- and neither the reply text nor (for a delegated turn) the
            # citations check alone would catch that. `new_worker`/
            # `existing_worker` are the only actions that select/create a
            # worker (server/pipeline.py's `_dispatch`); every other action
            # (direct/unsupported/clarify) never touches the registry.
            #
            # Unlike deterministic_action_pass above, this assertion does NOT
            # need routing_action to be non-None to evaluate: `result.worker_id`
            # is a signal valid on every _handle_transcript() return path,
            # including multi_intent/continue_pending -- the same reason
            # scripts/smoke_conversation.py:368's `result.worker_id == "main"`
            # check exists. Folding it into `delegated_action` via `or` means
            # a multi_intent/continue_pending turn that genuinely delegated
            # (any committed result's worker_id != "main") is still correctly
            # scored, instead of routing_action's None collapsing this
            # assertion to a spurious False (round 7 gauntlet, Logic lens
            # finding 6).
            delegated_action = routing_action in {"new_worker", "existing_worker"} or any(
                r.worker_id != "main" for r in results
            )
            outcome.worker_presence_pass = delegated_action == turn.expect_delegated

            if turn.expect_delegated:
                outcome.citations_pass = any(r.citations for r in results)

            try:
                stage_metrics: dict[str, float] | None = _latest_turn_stage_metrics(
                    sink, elapsed_ms, result.turn_id
                )
            except RuntimeError:
                # A multi-intent commit labels each item's result with an
                # item-suffixed turn_id (f"{turn_id}-{index}",
                # server/pipeline.py's `_handle_multi_intent`), which never
                # matches the parent `app_turn_foreground` metric record
                # (keyed by the original turn_id) -- per-item latency is
                # genuinely unavailable for a multi-intent turn's first
                # result. Report as such rather than let a metrics-lookup
                # mismatch escape to the outer handler and misclassify an
                # otherwise-successful turn as "turn-error".
                stage_metrics = None
            if stage_metrics is not None:
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
            # otherwise be evaluating against. Sanitized before it reaches the
            # judge's context since it can carry raw hosted-web-search content.
            judge.add_assistant_message(_sanitize_for_judge(result.ui_text))
            if turn.judge_criterion:
                try:
                    verdict = await judge.evaluate(turn.judge_criterion)
                except Exception as exc:  # noqa: BLE001 -- classify, don't crash the matrix
                    outcome.judge_verdict = "judge-error"
                    outcome.judge_reason = (
                        f"judge.evaluate() raised: "
                        f"{error_text(exc, credential=config.openai_api_key)}"
                    )
                else:
                    # pipecat's EvalJudge.evaluate() (pipecat/evals/judge.py) signals
                    # infra-style failures through `reason`, not a distinct verdict
                    # value -- a raised inference exception ("judge call failed: ..."),
                    # an empty LLM response ("judge returned empty response"), and an
                    # unparsable response ("could not parse judge response: ...") all
                    # come back as verdict="no", indistinguishable from a genuine
                    # semantic "no" unless the reason text is checked. Any of these
                    # three prefixes is an infrastructure failure, not a real
                    # assertion result.
                    #
                    # No `verdict.verdict not in {"yes", "no", "continue"}` guard
                    # here (round 5 added one; round 7 gauntlet, Logic lens finding
                    # 9 removed it): pipecat's own `_parse_verdict` unconditionally
                    # coerces any out-of-enum value to "no" before ever returning a
                    # JudgeVerdict (verified against the installed pipecat==1.6.0
                    # source), so that comparison can never be true -- dead code
                    # that read as a real safety net. `JudgeVerdict.raw_response`
                    # does exist, but distinguishing "genuinely judged no" from
                    # "coerced from a nonsense verdict value" from it would mean
                    # re-parsing `raw_response` with a second copy of pipecat's own
                    # tolerant-JSON/keyword-fallback parsing logic -- a
                    # disproportionate, fragile coupling to pipecat internals for a
                    # gap this eval suite's own reason-prefix check already covers
                    # for every infra failure pipecat's `_parse_verdict` actually
                    # signals distinctly.
                    if verdict.reason.startswith(_JUDGE_INFRA_ERROR_REASON_PREFIXES):
                        outcome.judge_verdict = "judge-error"
                    else:
                        outcome.judge_verdict = verdict.verdict
                    # Bounded/redacted before persisting to the report, matching
                    # every other provider/judge-sourced text this runner stores
                    # (error_text()'s callers) -- verdict.reason is
                    # provider-controlled free text with no exception object
                    # wrapping it, previously stored verbatim (round 7 gauntlet,
                    # Codex adversarial gate finding 5).
                    outcome.judge_reason = sanitize_reason(
                        verdict.reason, credential=config.openai_api_key
                    )

            turns.append(outcome)
    except Exception as exc:  # noqa: BLE001 -- classify, don't crash the matrix
        # Setup work (config resolution, host/judge construction) or
        # host.start()/host.connect() (or any other unexpected exception that
        # escaped the per-turn handling above) must not abort run_matrix
        # uncaught -- that would discard every already-completed and
        # already-billed cell with no report ever written, contradicting this
        # function's own "classify, don't crash the matrix" contract.
        #
        # turn_started (not len(turns)) decides "setup-error" vs
        # "turn-error": a TurnOutcome is only appended to `turns` at the END
        # of a turn's iteration, so if the FIRST turn fails mid-execution
        # (e.g. _latest_turn_stage_metrics() raising, or a malformed
        # _handle_transcript return value's attribute access failing), `turns`
        # is still `[]` here even though that turn actually ran and was
        # billed. Append its in-flight outcome now, before the backfill below
        # runs, so it isn't mislabeled "skipped".
        if turn_started:
            # in_flight_query is only ever None before the first turn starts
            # (turn_started gates this branch to "a turn's query was already
            # assigned"); the `or ""` is a type-narrowing fallback for mypy,
            # not a reachable runtime case.
            turns.append(TurnOutcome(query=in_flight_query or "", status="turn-error"))
        cell_status = "turn-error" if turns else "setup-error"
        cell_error = error_text(exc, credential=config.openai_api_key if config else None)
    finally:
        if host is not None:
            try:
                await host.shutdown()
            except Exception as shutdown_exc:  # noqa: BLE001 -- never mask the original outcome
                # A shutdown failure must not crash run_matrix or overwrite a
                # real cell outcome/error above -- surface it only if nothing
                # else already explains why this cell isn't "ok". Reported as
                # "turn-error", not "setup-error": this branch is only
                # reachable when cell_status == "ok", i.e. every turn already
                # ran and succeeded -- shutdown runs strictly after setup, so
                # "setup-error" would be self-contradictory alongside N "ok"
                # turns. Both statuses are already in
                # _CELL_INFRA_FAILURE_STATUSES, so this is a reporting-only
                # change, not a behavior change to the pass/fail gate.
                if cell_status == "ok":
                    cell_status = "turn-error"
                    cell_error = (
                        "host.shutdown() raised: "
                        f"{error_text(shutdown_exc, credential=config.openai_api_key if config else None)}"
                    )

    if cell_status != "ok":
        # Record every turn the scenario defines but this cell never
        # attempted, so a reader of the report can tell "ran and failed" from
        # "never got here" instead of the turn silently being absent.
        turns.extend(_skipped_turn_outcomes(scenario, already_run=len(turns)))

    return CellOutcome(
        pair_label=pair.label,
        scenario_name=scenario.name,
        status=cell_status,
        error=cell_error,
        turns=turns,
        router_timeout_seconds=config.router_timeout_seconds if config is not None else None,
        foreground_search_timeout_seconds=(
            config.foreground_search_timeout_seconds if config is not None else None
        ),
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
            # Defense-in-depth, not the primary gate: main()'s call to
            # require_manifest_ok_for_live_run() (see its docstring) already
            # rejects any of these candidates before run_matrix() is ever
            # invoked from the CLI, so under normal use this branch is
            # unreachable. It stays reachable for a caller that invokes
            # run_matrix() directly without that preflight -- see
            # TestManifestGate.test_run_matrix_rejects_uncovered_candidate_directly.
            if not candidate_accepted(pair.router, manifest_status) or not candidate_accepted(
                pair.worker, manifest_status
            ):
                outcomes.append(
                    _never_ran_cell(
                        pair,
                        scenario,
                        "manifest-rejected",
                        "one or both candidates are absent from the manifest",
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


_CELL_INFRA_FAILURE_STATUSES = {
    "provider-error",
    "timeout",
    "setup-error",
    "turn-error",
    "manifest-rejected",
}


def compute_pass_fail(outcomes: list[CellOutcome]) -> tuple[str, list[str]]:
    """Aggregate the run's cells/turns into one pass/fail verdict.

    Takes the typed ``list[CellOutcome]`` ``run_cell()``/``run_matrix()``
    actually produce, not the ``dict[str, Any]`` ``build_report()`` later
    serializes them into -- round 5's Architecture lens (finding 7) flagged
    that re-reading the serialized dict by string key (``cell["status"]``,
    ``turn["judge_verdict"]``, ...) meant a field rename in ``build_report()``
    would surface as a runtime ``KeyError`` here with no static check
    catching it first. Operating on ``CellOutcome``/``TurnOutcome`` attributes
    directly closes that gap: a rename is now a mypy error at this call site,
    not a runtime one.

    Distinguishes infrastructure failure (a cell/turn never produced a real
    result -- provider error, timeout, judge-error) from semantic failure (a
    real result the judge or a deterministic assertion scored as wrong, or an
    enforced baseline latency budget breach). Both are reported as reasons so
    the caller can tell FAIL-infra from FAIL-semantic apart, but either kind
    fails the run -- a judge-error is not evidence of correctness.
    """
    reasons: list[str] = []
    for cell in outcomes:
        label = f"{cell.pair_label}/{cell.scenario_name}"
        if cell.status in _CELL_INFRA_FAILURE_STATUSES:
            reasons.append(f"infra: {label} cell status={cell.status!r}")
        for turn in cell.turns or []:
            query = turn.query
            # A turn whose own status isn't "ok" (provider-error, timeout,
            # setup-error, skipped) never produced a real result -- its
            # judge_verdict/deterministic_action_pass/citations_pass/latency
            # fields are all None (see build_report), so nothing below this
            # branch has a real signal to check. Without this check a fully
            # failed, fully-billed run whose every turn timed out or errored
            # had no per-turn reason recorded here at all (only the cell's
            # own status, which run_cell previously always reported "ok"
            # regardless of a turn breaking early -- see run_cell's
            # cell_status tracking) and could silently read as passing.
            if turn.status != "ok":
                reasons.append(f"infra: {label} {query!r} turn status={turn.status!r}")
                continue
            if turn.judge_verdict == "judge-error":
                reasons.append(f"infra: {label} {query!r} judge-error ({turn.judge_reason!r})")
            elif turn.judge_verdict == "no":
                reasons.append(f"semantic: {label} {query!r} judge verdict=no")
            elif turn.judge_verdict == "continue":
                reasons.append(f"semantic: {label} {query!r} judge verdict=continue")
            if turn.deterministic_action_pass is False:
                reasons.append(f"semantic: {label} {query!r} deterministic action assertion failed")
            if turn.worker_presence_pass is False:
                reasons.append(
                    f"semantic: {label} {query!r} worker presence/absence assertion failed"
                )
            if turn.citations_pass is False:
                reasons.append(f"semantic: {label} {query!r} citations assertion failed")
            if turn.latency_budget_enforced and turn.latency_budget_exceeded:
                reasons.append(f"semantic: {label} {query!r} enforced latency budget exceeded")
    return ("FAIL" if reasons else "PASS"), reasons


def build_report(outcomes: list[CellOutcome], *, judge_model: str) -> dict[str, Any]:
    overall_status, failure_reasons = compute_pass_fail(outcomes)
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
                        "worker_presence_pass": turn.worker_presence_pass,
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
            if turn["worker_presence_pass"] is not None:
                bits.append(f"worker_presence_pass={turn['worker_presence_pass']}")
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
        router = ROUTER_SELECTABLE_BY_LABEL[args.router or "baseline"]
        worker = WORKER_SELECTABLE_BY_LABEL[args.worker or "baseline"]
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
    try:
        reply = input("Proceed anyway? [y/N] ").strip().lower()
    except EOFError:
        # A tty that receives EOF (Ctrl-D) rather than piped input isn't
        # caught by the isatty() check above -- treat it the same as an
        # explicit decline rather than letting the exception propagate as an
        # uncaught traceback.
        return False
    return reply in {"y", "yes"}


def _finite_nonnegative_int(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--max-calls must be an integer: {raw!r}") from exc
    if value < 0:
        raise argparse.ArgumentTypeError(f"--max-calls must be non-negative: {raw!r}")
    return value


def _finite_nonnegative_float(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--max-cost must be a number: {raw!r}") from exc
    # float("nan")/float("inf") both parse without raising -- and a NaN
    # spend limit makes every comparison in _confirm_spend's `exceeds` check
    # false (NaN never compares > anything), so an operator who mistypes or
    # scripts a NaN/inf --max-cost value would silently get an *unbounded*
    # budget treated as "always within budget" instead of the intended cap.
    if not math.isfinite(value) or value < 0:
        raise argparse.ArgumentTypeError(f"--max-cost must be finite and non-negative: {raw!r}")
    return value


def _finite_positive_float(flag: str) -> Callable[[str], float]:
    """Build an argparse ``type=`` validator for a finite, positive float flag.

    Parameterized by ``flag`` (rather than hardcoding one flag's name in the
    error text) so the same validator backs both ``--max-routing-seconds``
    and ``--max-latency-seconds`` -- each budget flag's blocking-check
    divides/compares against its own value (``routing_ms > max_routing_seconds
    * 1000``, ``total_ms > max_latency_seconds * 1000``), and nan/inf both
    parse via bare ``float()`` without raising, silently disabling that
    budget's enforcement instead of rejecting the bad input up front. A
    non-positive-but-finite value (0 or negative) is rejected too: it would
    make every measured result exceed the configured limit. See round-4
    gauntlet finding 1.
    """

    def _validate(raw: str) -> float:
        try:
            value = float(raw)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"{flag} must be a number: {raw!r}") from exc
        if not math.isfinite(value) or value <= 0:
            raise argparse.ArgumentTypeError(f"{flag} must be finite and positive: {raw!r}")
        return value

    return _validate


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
    parser.add_argument("--router", choices=sorted(ROUTER_SELECTABLE_BY_LABEL), default=None)
    parser.add_argument("--worker", choices=sorted(WORKER_SELECTABLE_BY_LABEL), default=None)
    parser.add_argument("--scenario", choices=sorted(SCENARIOS_BY_NAME), default=None)
    parser.add_argument("--max-calls", type=_finite_nonnegative_int, default=None)
    parser.add_argument("--max-cost", type=_finite_nonnegative_float, default=None)
    parser.add_argument("--yes", action="store_true", help="skip the spend-confirmation prompt")
    parser.add_argument("--i-know-the-manifest-is-stale", action="store_true", dest="allow_stale")
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--judge-model", default=DEFAULT_JUDGE_MODEL)
    parser.add_argument(
        "--max-routing-seconds",
        type=_finite_positive_float("--max-routing-seconds"),
        default=DEFAULT_MAX_ROUTING_SECONDS,
    )
    parser.add_argument(
        "--max-latency-seconds",
        type=_finite_positive_float("--max-latency-seconds"),
        default=DEFAULT_MAX_LATENCY_SECONDS,
    )
    parser.add_argument(
        "--out", type=Path, default=None, help="report output path (default: printed only)"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.full_matrix and (args.router or args.worker):
        # --router/--worker already select a single pair; --full-matrix
        # asks for the entire cross product. Silently letting --router/
        # --worker win (the prior behavior) means a typo'd or copy-pasted
        # --full-matrix flag looks accepted but has no effect -- error out
        # explicitly instead, per the dev plan's "read the plan for
        # guidance; if ambiguous, error out explicitly" fallback.
        parser.error(
            "--full-matrix conflicts with --router/--worker (which already select one pair)"
        )

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
    # Always shown before a live call is made -- not only when a limit would
    # be exceeded -- per the dev plan's documented contract: "no live call
    # happens without the operator seeing the total call count, cost
    # estimate...". _confirm_spend() itself returns silently when under
    # budget, so without this the operator could start paid cells having
    # never seen the estimate.
    print_matrix_preview(pairs, scenarios, judge_model=args.judge_model, status=manifest_status)
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
        try:
            out_path = confined_output_path(args.out, allowed_root=_REPO_ROOT)
            write_no_follow(out_path, json.dumps(report, indent=2, sort_keys=True))
        except (ValueError, OSError) as exc:
            print(f"refusing to write report: {exc}", file=sys.stderr)
            return 1
        print(f"\nreport written to {out_path}")
    return 0 if report["overall_status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
