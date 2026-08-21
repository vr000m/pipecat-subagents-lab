"""Offline, mocked tests for the eval-suite runner (plan Phase 2, dev plan
``docs/dev_plans/20260817-feature-router-worker-model-eval-suite.md``).

Design intent under test (not just surface behaviour): a single command runs
the default sweep matrix (or the full cross product under ``--full-matrix``)
against the live provider, refuses to run any (model, effort) combination
Phase 0's manifest didn't confirm (or a manifest stale relative to the current
commit), drives each turn through a real connected ``SessionHost``, and
produces one aggregate pass/fail + latency report distinguishing semantic
failure from infrastructure failure -- and no live call happens without the
operator seeing the total call count, cost estimate, and (if
``--max-calls``/``--max-cost`` would be exceeded) an explicit confirmation
prompt.

This file is OFFLINE-ONLY: no real network access, no real OpenAI client, no
real ``SessionHost`` connection lifecycle. Everything that could reach the
network is mocked or monkeypatched, and the dry-run tests actively assert
that touching those mocked seams raises rather than silently succeeding, so a
live call sneaking into ``--dry-run`` (or into the offline unit tests
generally) fails loudly instead of passing quietly.

Reconciled (round 1) against the real implementation
(``scripts/eval_model_comparison.py``, ``scripts/eval_common.py``,
``evals/scenarios.py``): the real API differs from the pre-implementation
guesses this file originally shipped with (e.g. ``default_sweep_pairs()``/
``full_matrix_pairs()`` instead of a guessed ``build_matrix()``,
``load_manifest_status()``/``candidate_accepted()``/
``require_manifest_ok_for_live_run()`` instead of a guessed
``load_manifest()``/``require_manifest_coverage()``/
``require_manifest_freshness()``, and no standalone
``check_latency_budget()``/``check_citations()``/``feed_turn_to_judge()``/
``classify_judge_verdict()``/``aggregate_report()`` helpers -- that logic
lives inline in ``run_cell()``/``build_report()``). Tests below exercise the
real names/shapes directly, driving ``run_cell()`` end-to-end with the
``SessionHost``/``EvalJudge``/latency-metrics seams faked out where the
behaviour under test only lives inside that function.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, ClassVar

import pytest

pytest.importorskip(
    "scripts.eval_model_comparison",
    reason="Phase 2 implementation not yet landed (offline test written against the plan's contract)",
)

import scripts.eval_model_comparison as eval_runner
from evals import scenarios as eval_scenarios
from evals.scenarios import Scenario, Turn
from scripts import eval_common

# ---------------------------------------------------------------------------
# Shared fixtures / fakes
# ---------------------------------------------------------------------------


def _write_manifest(tmp_path: Path, *, source_commit: str, results: list[dict[str, Any]]) -> Path:
    manifest = {
        "manifest_version": 1,
        "source_commit": source_commit,
        "verified_at_utc": "2026-08-17T22:53:47Z",
        "results": results,
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    return path


def _accepted_router_entry(model: str, effort: str | None) -> dict[str, Any]:
    # request_kwargs mirrors the shape _request_kwargs_shape_ok() (round 7
    # gauntlet, Codex finding 1) requires for a router entry -- store=False,
    # a structured-output `text` format, and a non-empty `input` -- so this
    # fixture keeps passing the manifest's own shape gate, not just its
    # (kind, model, effort) membership check. `model` and `reasoning` (round
    # 8 gauntlet, Codex P1 finding 2) mirror the entry's own recorded
    # (model, effort) -- using the EFFECTIVE effort
    # (effective_router_reasoning_effort), matching what
    # LazyRouterProvider.__call__ actually sends on the wire.
    effective_effort = eval_runner.effective_router_reasoning_effort(model, effort)
    request_kwargs: dict[str, Any] = {
        "model": model,
        "store": False,
        "text": {"format": {"type": "json_schema"}},
        "input": "probe transcript",
    }
    if effective_effort is not None:
        request_kwargs["reasoning"] = {"effort": effective_effort}
    return {
        "kind": "router",
        "model": model,
        "effort": effort,
        "tools": ["text"],
        "request_kwargs": request_kwargs,
        "accepted": True,
        "error": None,
        "response_id": "resp-1",
    }


def _accepted_worker_entry(model: str, effort: str | None) -> dict[str, Any]:
    # See _accepted_router_entry(): shape must also satisfy
    # _request_kwargs_shape_ok()'s worker branch -- store=False,
    # tool_choice="required", a `text` format, and a `web_search` tool.
    # `model`/`reasoning` (round 8 gauntlet, Codex P1 finding 2) mirror the
    # entry's own recorded (model, effort) directly -- unlike the router, the
    # worker has no "unset effort defaults to minimal" resolution rule.
    request_kwargs: dict[str, Any] = {
        "model": model,
        "store": False,
        "tool_choice": "required",
        "text": {"format": {"type": "json_schema"}},
        "tools": [{"type": "web_search"}],
    }
    if effort is not None:
        request_kwargs["reasoning"] = {"effort": effort}
    return {
        "kind": "worker",
        "model": model,
        "effort": effort,
        "tools": ["web_search"],
        "request_kwargs": request_kwargs,
        "accepted": True,
        "error": None,
        "response_id": "resp-2",
    }


def _accepted_judge_entry(model: str) -> dict[str, Any]:
    # See _accepted_router_entry(): shape must also satisfy
    # _request_kwargs_shape_ok()'s judge branch -- a non-empty `messages`
    # list of role/content dicts, plus whatever reasoning_effort production
    # actually sends for this model (round 3 confirming pass, Codex P2
    # finding: the judge branch didn't used to cross-check this at all).
    request_kwargs = {
        "model": model,
        "messages": [{"role": "user", "content": "probe"}],
        **eval_runner._judge_extra_kwargs(model),
    }
    return {
        "kind": "judge",
        "model": model,
        "effort": None,
        "tools": [],
        "request_kwargs": request_kwargs,
        "accepted": True,
        "error": None,
    }


class _NetworkAccessError(AssertionError):
    """Raised by a mocked seam that must never be reached in these tests."""


class _RaisingSessionHost:
    """Stands in for ``server.pipeline.SessionHost``: any lifecycle method
    touching a live connection raises, so a test that accidentally drives a
    real turn fails loudly instead of hanging or reaching the network.
    """

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        raise _NetworkAccessError("SessionHost must not be constructed in an offline/dry-run test")


def _raise_network_access(*_args: Any, **_kwargs: Any) -> Any:
    raise _NetworkAccessError("no live call is permitted in this test")


# ---------------------------------------------------------------------------
# ``run_cell`` harness: fakes the SessionHost lifecycle, the per-turn stage
# metrics, and the EvalJudge, so the latency-budget and judge-scoring logic
# that lives inline inside ``run_cell`` can be exercised end-to-end without
# any network access or real connection lifecycle.
# ---------------------------------------------------------------------------


class _FakeRouting:
    def __init__(self, action: str | None, turn_id: str = "turn-1") -> None:
        self.action = action
        # Mirrors server/contracts.py's RoutingState, which is turn-scoped:
        # run_cell()'s stale-routing-read guard compares this against the
        # current turn's result.turn_id (also "turn-1" by default in
        # _run_cell's _result_factory), so a fixture that omits it would
        # always read as "prior turn's stale decision" and silently defeat
        # every routing_action-driven assertion below.
        self.turn_id = turn_id


class _FakeState:
    def __init__(self, routing_action: str | None = None, routing_turn_id: str = "turn-1") -> None:
        self.session_id = "session-1"
        self.resume_token = "resume-1"
        # None reproduces the pre-finding-12 default (no `routing` signal at
        # all -- getattr(host.state, "routing", None) falls back to None
        # either way); a caller exercising the worker-presence assertion
        # passes the routing action it wants run_cell to observe.
        self.routing = (
            _FakeRouting(routing_action, routing_turn_id) if routing_action is not None else None
        )


class _FakeRegistry:
    workers: ClassVar[list[Any]] = []


@dataclass
class _FakeResult:
    ui_text: str
    spoken_text: str
    citations: list[Any]
    turn_id: str
    # Defaults to "main" (the direct-answer path's worker_id) so every
    # existing single-result fixture is unaffected; a multi-intent test
    # overrides this to exercise delegated_action's worker_id fallback
    # (round 7 gauntlet, Logic lens finding 6).
    worker_id: str = "main"


class _FakeHost:
    def __init__(
        self,
        result_factory: Any,
        *,
        routing_action: str | None = None,
        routing_turn_id: str = "turn-1",
    ) -> None:
        self.state = _FakeState(routing_action, routing_turn_id)
        self.registry = _FakeRegistry()
        self._result_factory = result_factory

    async def start(self) -> None:
        pass

    async def connect(self, _handshake: dict[str, Any]) -> str:
        return "connection-1"

    async def _handle_transcript(self, query: str, *, origin: Any) -> Any:
        del origin
        return self._result_factory(query)

    async def shutdown(self) -> None:
        pass


def _make_stub_judge_class(verdicts: list[Any], recorder: list[Any] | None) -> type:
    """Builds a fresh stand-in for ``pipecat.evals.judge.EvalJudge`` bound to
    a caller-supplied verdict queue, so ``run_cell``'s ``judge.evaluate()``
    call never reaches a real LLM.
    """

    class _StubEvalJudge:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            self.user_messages: list[str] = []
            self.assistant_messages: list[str] = []
            self.init_kwargs = _kwargs
            if recorder is not None:
                recorder.append(self)

        def add_user_message(self, text: str) -> None:
            self.user_messages.append(text)

        def add_assistant_message(self, text: str) -> None:
            self.assistant_messages.append(text)

        async def evaluate(self, _criterion: str) -> Any:
            if not verdicts:
                raise AssertionError("evaluate() called with no canned verdict queued")
            return verdicts.pop(0)

    return _StubEvalJudge


def _run_cell(
    monkeypatch: pytest.MonkeyPatch,
    *,
    pair: Any,
    turns: tuple[Turn, ...],
    ui_text: str = "a distinctly non-fallback reply",
    spoken_text: str = "a distinctly non-fallback spoken reply",
    citations: list[Any] | None = None,
    routing_ms: float = 100.0,
    total_ms: float = 100.0,
    verdicts: list[Any] | None = None,
    judge_recorder: list[Any] | None = None,
    routing_action: str | None = None,
    routing_turn_id: str = "turn-1",
    result_factory: Any | None = None,
) -> Any:
    from server.config import Config

    result_citations = citations if citations is not None else []

    def _default_result_factory(_query: str) -> Any:
        return _FakeResult(
            ui_text=ui_text, spoken_text=spoken_text, citations=result_citations, turn_id="turn-1"
        )

    # result_factory lets a caller return a tuple of _FakeResult (mirroring
    # server/pipeline.py's `_handle_multi_intent` -> `tuple(committed)` shape)
    # instead of the default single-result stand-in, so multi-intent
    # regressions (round 7 gauntlet, Logic lens finding 6) can be exercised
    # without a new, parallel harness.
    active_result_factory = (
        result_factory if result_factory is not None else _default_result_factory
    )

    def _fake_build_session_for_run(_config: Any, *, measurement_sink: Any = None) -> Any:
        del measurement_sink
        return _FakeHost(
            active_result_factory, routing_action=routing_action, routing_turn_id=routing_turn_id
        )

    def _fake_stage_metrics(_sink: Any, _elapsed_ms: float, _turn_id: str) -> dict[str, float]:
        return {"routing_ms": routing_ms, "search_ms": 0.0, "total_ms": total_ms}

    monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
    monkeypatch.setattr(eval_runner, "latest_turn_stage_metrics", _fake_stage_metrics)
    monkeypatch.setattr(
        "pipecat.evals.judge.EvalJudge", _make_stub_judge_class(verdicts or [], judge_recorder)
    )
    # run_cell() builds the judge's LLM service via build_judge_llm_service()
    # (threading the resolved credential through explicitly -- see its
    # docstring), not the library's openai_service() factory -- patch the
    # former so no real OpenAILLMService/credential check is reached.
    monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)

    scenario = Scenario(name="fixture-scenario", turns=turns)
    config = Config()

    return asyncio.run(
        eval_runner.run_cell(
            pair,
            scenario,
            config,
            judge_model="gpt-5-mini",
            max_routing_seconds=15.0,
            max_latency_seconds=15.0,
        )
    )


# ---------------------------------------------------------------------------
# Matrix-building
# ---------------------------------------------------------------------------


class TestMatrixBuilding:
    """Default sweep set vs --full-matrix must produce the documented set of
    (router_config, worker_config) pairs: baseline x baseline, each router
    candidate x baseline worker, baseline router x each worker candidate --
    NOT the full cross product unless --full-matrix is passed.
    """

    def test_default_sweep_is_not_the_full_cross_product(self) -> None:
        pairs = eval_runner.default_sweep_pairs()
        # baseline x baseline (1) + N router candidates x baseline worker
        # + baseline router x M worker candidates -- no shipped x shipped
        # cell (round-4 restart, Architecture finding 2: build_report()
        # annotates the shipped cells instead of running a live joint
        # cell) -- not the (N+1)*(M+1) full cross product full_matrix_pairs()
        # would produce.
        expected = 1 + len(eval_runner.ROUTER_CANDIDATES) + len(eval_runner.WORKER_CANDIDATES)
        assert len(pairs) == expected
        full_matrix_size = (1 + len(eval_runner.ROUTER_CANDIDATES)) * (
            1 + len(eval_runner.WORKER_CANDIDATES)
        )
        assert len(pairs) < full_matrix_size

    def test_default_sweep_contains_baseline_by_baseline(self) -> None:
        pairs = eval_runner.default_sweep_pairs()
        baseline_keys = {eval_runner._pair_cell_key(p) for p in pairs}
        assert (
            eval_runner._pair_cell_key(
                eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
            )
            in baseline_keys
        )

    def test_default_sweep_varies_router_only_against_baseline_worker(self) -> None:
        pairs = eval_runner.default_sweep_pairs()
        keys = {eval_runner._pair_cell_key(p) for p in pairs}
        for candidate in eval_runner.ROUTER_CANDIDATES:
            assert (
                eval_runner._pair_cell_key(
                    eval_runner.RunPair(candidate, eval_runner.WORKER_BASELINE)
                )
                in keys
            )
            # A router candidate must never be paired with a worker candidate
            # in the default (non-full-matrix) sweep.
            for worker_candidate in eval_runner.WORKER_CANDIDATES:
                assert (
                    eval_runner._pair_cell_key(eval_runner.RunPair(candidate, worker_candidate))
                    not in keys
                )

    def test_default_sweep_varies_worker_only_against_baseline_router(self) -> None:
        pairs = eval_runner.default_sweep_pairs()
        keys = {eval_runner._pair_cell_key(p) for p in pairs}
        for candidate in eval_runner.WORKER_CANDIDATES:
            assert (
                eval_runner._pair_cell_key(
                    eval_runner.RunPair(eval_runner.ROUTER_BASELINE, candidate)
                )
                in keys
            )

    def test_full_matrix_is_the_full_cross_product(self) -> None:
        pairs = eval_runner.full_matrix_pairs()
        routers = (eval_runner.ROUTER_BASELINE, *eval_runner.ROUTER_CANDIDATES)
        workers = (eval_runner.WORKER_BASELINE, *eval_runner.WORKER_CANDIDATES)
        assert len(pairs) == len(routers) * len(workers)
        keys = {eval_runner._pair_cell_key(p) for p in pairs}
        for router in routers:
            for worker in workers:
                assert eval_runner._pair_cell_key(eval_runner.RunPair(router, worker)) in keys

    def test_full_matrix_is_a_strict_superset_of_the_default_sweep(self) -> None:
        # No shipped anchor to exclude any more (round-4 restart, Architecture
        # finding 2) -- the default sweep's cells are exactly a subset of the
        # full matrix's cells.
        default_keys = {eval_runner._pair_cell_key(p) for p in eval_runner.default_sweep_pairs()}
        full_keys = {eval_runner._pair_cell_key(p) for p in eval_runner.full_matrix_pairs()}
        assert default_keys <= full_keys
        assert default_keys != full_keys


class TestLatencyEnforcementIsExplicitNotLabelDerived:
    """Regression for round-4 restart, Architecture finding 1 / Logic finding
    2: RunPair.enforce_latency_budget is an explicit field, not derived from
    Candidate.label (the old `is_baseline` property compared
    router.label == "baseline" and worker.label == "baseline"). Only the
    historical baseline x baseline cell is enforced, regardless of what
    config.toml happens to ship.
    """

    def test_only_the_historical_baseline_cell_is_enforced(self) -> None:
        pairs = eval_runner.default_sweep_pairs()
        enforced = [p for p in pairs if p.enforce_latency_budget]
        assert len(enforced) == 1
        assert enforced[0].router is eval_runner.ROUTER_BASELINE
        assert enforced[0].worker is eval_runner.WORKER_BASELINE

    def test_enforcement_does_not_depend_on_config_toml_coincidence(self, monkeypatch: Any) -> None:
        # The bug reproduction: pre-fix, enforcement was derived from
        # Candidate.label == "baseline", so a shipped-config coincidence with
        # the historical baseline silently changed which cells got enforced.
        # build_report()'s shipped-cell annotation (F2) reads shipped_candidates()
        # independently of default_sweep_pairs()'s enforcement, so monkeypatching
        # it here exercises the same "coincidence must not matter" property
        # the old is_baseline property broke.
        coincident = (
            replace(eval_runner.ROUTER_BASELINE, label="shipped"),
            replace(eval_runner.WORKER_BASELINE, label="shipped"),
        )
        divergent = (
            replace(eval_runner.ROUTER_CANDIDATES[0], label="shipped"),
            replace(eval_runner.WORKER_CANDIDATES[0], label="shipped"),
        )

        def _enforced_keys() -> set[tuple[str, str | None, str, str | None]]:
            return {
                eval_runner._pair_cell_key(p)
                for p in eval_runner.default_sweep_pairs()
                if p.enforce_latency_budget
            }

        monkeypatch.setattr(eval_runner, "shipped_candidates", lambda: coincident)
        coincident_enforced = _enforced_keys()
        monkeypatch.setattr(eval_runner, "shipped_candidates", lambda: divergent)
        divergent_enforced = _enforced_keys()

        assert coincident_enforced == divergent_enforced

    def test_selected_baseline_pair_is_enforced(self) -> None:
        from argparse import Namespace

        baseline_args = Namespace(router="baseline", worker="baseline")
        (pair,) = eval_runner._resolve_pairs(baseline_args)
        assert pair.enforce_latency_budget is True

        candidate_args = Namespace(router=eval_runner.ROUTER_CANDIDATES[0].label, worker="baseline")
        (pair,) = eval_runner._resolve_pairs(candidate_args)
        assert pair.enforce_latency_budget is False


class TestPairCellKeyUsesEffectiveEffort:
    """Regression for round-4 restart, Logic finding 1: _pair_cell_key() must
    key on effective_effort_for_manifest_lookup(), not the raw declared
    effort -- ("gpt-5-mini", None) and ("gpt-5-mini", "minimal") are ONE wire
    request under two spellings.
    """

    def test_unset_and_explicit_minimal_dedupe_to_one_cell(self) -> None:
        router_a = eval_runner.Candidate(label="a", role="router", model="gpt-5-mini", effort=None)
        router_b = eval_runner.Candidate(
            label="b", role="router", model="gpt-5-mini", effort="minimal"
        )
        worker = eval_runner.WORKER_BASELINE
        pairs = [eval_runner.RunPair(router_a, worker), eval_runner.RunPair(router_b, worker)]

        assert len(eval_runner._dedupe_pairs(pairs)) == 1

    def test_genuinely_different_efforts_stay_distinct(self) -> None:
        router_a = eval_runner.Candidate(
            label="a", role="router", model="gpt-5-mini", effort="minimal"
        )
        router_b = eval_runner.Candidate(
            label="b", role="router", model="gpt-5-mini", effort="high"
        )
        worker = eval_runner.WORKER_BASELINE
        pairs = [eval_runner.RunPair(router_a, worker), eval_runner.RunPair(router_b, worker)]

        assert len(eval_runner._dedupe_pairs(pairs)) == 2

    def test_non_gpt5_model_unset_vs_minimal_effort_stays_distinct(self) -> None:
        # Pins that the fix delegates to the shared resolver rather than
        # hardcoding "unset defaults to minimal" for every model.
        router_a = eval_runner.Candidate(
            label="a", role="router", model="not-a-gpt-5-model", effort=None
        )
        router_b = eval_runner.Candidate(
            label="b", role="router", model="not-a-gpt-5-model", effort="minimal"
        )
        worker = eval_runner.WORKER_BASELINE
        pairs = [eval_runner.RunPair(router_a, worker), eval_runner.RunPair(router_b, worker)]

        assert len(eval_runner._dedupe_pairs(pairs)) == 2


class TestDedupePairsGuardsEnforcementConsistency:
    """Regression for round-4 restart verification follow-up: two RunPairs
    that collide on wire identity but disagree on enforce_latency_budget
    must be caught loudly, not silently first-wins resolved. Unreachable
    today by construction (only one caller ever sets True), but a future
    construction site could reintroduce F1's original bug in a new shape."""

    def test_agreeing_enforcement_dedupes_silently(self) -> None:
        router_a = eval_runner.Candidate(label="a", role="router", model="gpt-5-mini", effort=None)
        router_b = eval_runner.Candidate(
            label="b", role="router", model="gpt-5-mini", effort="minimal"
        )
        worker = eval_runner.WORKER_BASELINE
        pairs = [
            eval_runner.RunPair(router_a, worker, enforce_latency_budget=True),
            eval_runner.RunPair(router_b, worker, enforce_latency_budget=True),
        ]

        assert len(eval_runner._dedupe_pairs(pairs)) == 1

    def test_disagreeing_enforcement_raises(self) -> None:
        router_a = eval_runner.Candidate(label="a", role="router", model="gpt-5-mini", effort=None)
        router_b = eval_runner.Candidate(
            label="b", role="router", model="gpt-5-mini", effort="minimal"
        )
        worker = eval_runner.WORKER_BASELINE
        pairs = [
            eval_runner.RunPair(router_a, worker, enforce_latency_budget=True),
            eval_runner.RunPair(router_b, worker, enforce_latency_budget=False),
        ]

        with pytest.raises(AssertionError, match="disagrees on enforce_latency_budget"):
            eval_runner._dedupe_pairs(pairs)


class TestSpendEstimateIsWorstCase:
    """Regression: scenario_call_counts() (the spend-confirmation gate's
    estimate function) must count every turn as a potential worker call, not
    just turns marked expect_delegated=True. ROUTING_REGRESSION's first turn
    is expect_delegated=False (a greeting), but if the router actually
    misroutes it -- a routing regression, the exact failure mode this
    scenario exists to catch -- the run makes a billed worker call anyway.
    An estimate that only counted expect_delegated=True turns would
    under-count and could silently let a run exceed --max-calls/--max-cost
    without triggering the confirmation prompt.
    """

    def test_worker_estimate_counts_every_turn_not_just_expect_delegated(self) -> None:
        scenario = eval_scenarios.ROUTING_REGRESSION
        assert scenario.turns[0].expect_delegated is False
        router_calls, worker_calls, _judge_calls = eval_runner.scenario_call_counts(scenario)
        assert router_calls == len(scenario.turns)
        # Worst case: every turn, including the non-expect_delegated one,
        # counts as a potential worker call.
        assert worker_calls == len(scenario.turns)

    def test_worker_estimate_exceeds_the_expect_delegated_only_count(self) -> None:
        scenario = eval_scenarios.ROUTING_REGRESSION
        expect_delegated_only = sum(1 for turn in scenario.turns if turn.expect_delegated)
        _router_calls, worker_calls, _judge_calls = eval_runner.scenario_call_counts(scenario)
        assert worker_calls > expect_delegated_only


class TestMatrixAccountingIncludesProviderRetryWorstCase:
    """Regression for round-5 gauntlet finding 11: the OpenAI SDK clients
    this runner drives leave the SDK's client-level max_retries=2 default in
    place, so a transient 429/5xx/timeout can issue up to 3 real requests
    for what this runner counts as 1 nominal call. matrix_call_accounting()
    (not scenario_call_counts(), which stays a pure per-scenario nominal
    count) must inflate the --max-calls/--max-cost estimate by the SDK's
    retry worst case, or the confirmation gate can authorize a run that ends
    up costing more than approved.
    """

    def test_accounting_multiplies_nominal_counts_by_the_retry_worst_case(self) -> None:
        pairs = (eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE),)
        scenarios = (eval_scenarios.SINGLE_TURN_DEFAULT,)
        nominal_router, nominal_worker, nominal_judge = eval_runner.scenario_call_counts(
            scenarios[0]
        )

        accounting = eval_runner.matrix_call_accounting(pairs, scenarios)

        assert accounting.router_calls == nominal_router * eval_runner._RETRY_WORST_CASE_MULTIPLIER
        assert accounting.worker_calls == nominal_worker * eval_runner._RETRY_WORST_CASE_MULTIPLIER
        assert accounting.judge_calls == nominal_judge * eval_runner._RETRY_WORST_CASE_MULTIPLIER

    def test_retry_worst_case_multiplier_reflects_the_sdk_default(self) -> None:
        assert (
            eval_runner._RETRY_WORST_CASE_MULTIPLIER
            == 1 + eval_runner._OPENAI_SDK_DEFAULT_MAX_RETRIES
        )
        assert eval_runner._RETRY_WORST_CASE_MULTIPLIER > 1


class TestWaitForBudgetExceedsTheHostsOwnWorstCaseTimeout:
    """Regression for round 8 gauntlet, Logic lens finding 3: run_cell()'s
    per-turn ``asyncio.wait_for`` budget previously matched
    ``config.provider_timeout_seconds + 5`` exactly (110s at defaults, the
    same as the host's own worst-case internal timeout), so a boundary race
    could abort the whole cell via this wait_for's ``TimeoutError`` instead
    of letting the host's own graceful per-turn timeout fire first. The
    budget is now derived from ``router_timeout_seconds +
    foreground_search_timeout_seconds + margin``, which must always exceed
    ``provider_timeout_seconds``.
    """

    def test_wait_for_timeout_strictly_exceeds_provider_timeout_seconds_at_cli_defaults(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, float] = {}
        real_wait_for = asyncio.wait_for

        async def _capturing_wait_for(coro: Any, *, timeout: float) -> Any:
            captured["timeout"] = timeout
            return await real_wait_for(coro, timeout=timeout)

        def _fake_build_session_for_run(_config: Any, *, measurement_sink: Any = None) -> Any:
            del measurement_sink
            return _FakeHost(
                lambda _q: _FakeResult(
                    ui_text="a distinctly non-fallback reply",
                    spoken_text="a distinctly non-fallback spoken reply",
                    citations=[],
                    turn_id="turn-1",
                )
            )

        monkeypatch.setattr(eval_runner.asyncio, "wait_for", _capturing_wait_for)
        monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
        monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)
        monkeypatch.setattr("pipecat.evals.judge.EvalJudge", _make_stub_judge_class([], None))
        monkeypatch.setattr(
            eval_runner,
            "latest_turn_stage_metrics",
            lambda *_a, **_k: {"routing_ms": 10.0, "search_ms": 0.0, "total_ms": 10.0},
        )

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        asyncio.run(
            eval_runner.run_cell(
                pair,
                Scenario(name="s", turns=(Turn(query="hi"),)),
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=eval_runner.DEFAULT_MAX_ROUTING_SECONDS,
                max_latency_seconds=eval_runner.DEFAULT_MAX_LATENCY_SECONDS,
            )
        )

        config = eval_runner._per_run_config(
            eval_runner.Config(),
            pair,
            max_routing_seconds=eval_runner.DEFAULT_MAX_ROUTING_SECONDS,
            max_latency_seconds=eval_runner.DEFAULT_MAX_LATENCY_SECONDS,
        )
        assert "timeout" in captured
        assert captured["timeout"] > config.provider_timeout_seconds


# ---------------------------------------------------------------------------
# Budget-checking
# ---------------------------------------------------------------------------


class TestLatencyBudgetChecking:
    """Baseline latency budget is blocking (``latency_budget_enforced``);
    candidate latency budget is report-only (not enforced). There is no
    standalone ``check_latency_budget()`` helper in the real implementation
    -- this logic lives inline in ``run_cell()`` -- so these tests drive
    ``run_cell()`` end-to-end with the ``SessionHost``/judge/stage-metrics
    seams faked out.
    """

    def test_baseline_over_budget_is_enforced_and_flagged_exceeded(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(
            eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE, enforce_latency_budget=True
        )
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="hi"),),
            routing_ms=20_000.0,
            total_ms=20_000.0,
        )
        assert outcome.status == "ok"
        turn = outcome.turns[0]
        assert turn.latency_budget_enforced is True
        assert turn.latency_budget_exceeded is True

    def test_baseline_within_budget_passes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        pair = eval_runner.RunPair(
            eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE, enforce_latency_budget=True
        )
        outcome = _run_cell(
            monkeypatch, pair=pair, turns=(Turn(query="hi"),), routing_ms=100.0, total_ms=100.0
        )
        turn = outcome.turns[0]
        assert turn.latency_budget_enforced is True
        assert turn.latency_budget_exceeded is False

    def test_candidate_over_budget_is_report_only_not_blocking(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_CANDIDATES[0], eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="hi"),),
            routing_ms=20_000.0,
            total_ms=20_000.0,
        )
        turn = outcome.turns[0]
        # A slow candidate is annotated as over-budget for reporting, but
        # latency_budget_enforced must be False -- higher reasoning effort
        # structurally increases latency, so this budget is not a fair
        # pass/fail gate for non-baseline configs.
        assert turn.latency_budget_exceeded is True
        assert turn.latency_budget_enforced is False

    def test_candidate_within_budget_passes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_CANDIDATES[0], eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch, pair=pair, turns=(Turn(query="hi"),), routing_ms=100.0, total_ms=100.0
        )
        turn = outcome.turns[0]
        assert turn.latency_budget_exceeded is False
        assert turn.latency_budget_enforced is False


# ---------------------------------------------------------------------------
# Report-aggregation logic
# ---------------------------------------------------------------------------


class TestReportAggregation:
    """``build_report()`` -- the real report-shaping function -- keeps
    ``TurnOutcome.status`` (ok/provider-error/timeout/setup-error) and
    ``judge_verdict`` (yes/no/judge-error/None) as separate fields per turn,
    so a downstream reader can never conflate "the model gave a bad answer"
    with "the run broke", and the report is labeled with the n=1-per-cell
    repetition count. There is no standalone ``aggregate_report()`` helper or
    pre-computed ``outcome_counts`` dict in the real implementation -- the
    report is the full per-turn breakdown; the plan's checklist (line 169)
    calls for exactly that, not summary counts.
    """

    def _turn(self, **overrides: Any) -> Any:
        base: dict[str, Any] = {"query": "q", "status": "ok", "judge_verdict": None}
        base.update(overrides)
        return eval_runner.TurnOutcome(**base)

    def test_semantic_and_infra_outcomes_are_distinct_fields_not_folded(self) -> None:
        cell = eval_runner.CellOutcome(
            pair_label="router=baseline/worker=baseline",
            scenario_name="s",
            status="ok",
            turns=[
                self._turn(status="ok", judge_verdict="yes"),
                self._turn(status="ok", judge_verdict="no"),
                self._turn(status="ok", judge_verdict="judge-error"),
                self._turn(status="provider-error"),
                self._turn(status="timeout"),
                self._turn(status="setup-error"),
            ],
        )
        report = eval_runner.build_report([cell], judge_model="gpt-5-mini")
        turns = report["cells"][0]["turns"]
        assert [t["status"] for t in turns] == [
            "ok",
            "ok",
            "ok",
            "provider-error",
            "timeout",
            "setup-error",
        ]
        assert [t["judge_verdict"] for t in turns] == ["yes", "no", "judge-error", None, None, None]

    def test_infrastructure_failures_carry_no_semantic_judge_verdict(self) -> None:
        cell = eval_runner.CellOutcome(
            pair_label="p",
            scenario_name="s",
            status="setup-error",
            turns=[
                self._turn(status="provider-error"),
                self._turn(status="timeout"),
                self._turn(status="setup-error"),
            ],
        )
        report = eval_runner.build_report([cell], judge_model="gpt-5-mini")
        # None of the infrastructure/error statuses should carry a semantic
        # judge_verdict -- a provider/timeout/setup problem is not evidence
        # the model gave a bad answer.
        for turn in report["cells"][0]["turns"]:
            assert turn["judge_verdict"] is None

    def test_report_labels_n_equals_one_repetition_per_cell(self) -> None:
        report = eval_runner.build_report([], judge_model="gpt-5-mini")
        # The plan requires the report to be "explicitly labeled with the
        # per-cell repetition count -- n=1 per cell in v1".
        assert report["repetition_count_per_cell"] == 1
        assert "repetition_note" in report

    def test_call_accounting_is_persisted_into_the_report(self) -> None:
        # Regression for round 9 gauntlet, Codex P2 finding 3: the
        # spend-confirmation estimate was only printed to the console,
        # unavailable to a caller auditing the persisted report file.
        accounting = eval_runner.CallAccounting(router_calls=3, worker_calls=2, judge_calls=1)
        report = eval_runner.build_report([], judge_model="gpt-5-mini", call_accounting=accounting)
        assert report["call_accounting"] == {
            "router_calls": 3,
            "worker_calls": 2,
            "judge_calls": 1,
            "total_calls": 6,
            "estimated_cost_usd": accounting.estimated_cost_usd,
        }

    def test_call_accounting_defaults_to_none_when_omitted(self) -> None:
        report = eval_runner.build_report([], judge_model="gpt-5-mini")
        assert report["call_accounting"] is None


# ---------------------------------------------------------------------------
# --repeat: majority-vote aggregation across independent repetitions.
# ---------------------------------------------------------------------------


class TestMatrixCallAccountingRepeatMultiplier:
    """matrix_call_accounting()'s repeat_count must multiply the whole
    worst-case estimate -- run_matrix() calls run_cell() repeat_count times
    per (pair, scenario), and every one of those is a real, separately
    billed live call. The spend-confirmation gate reads this estimate before
    any call is made, so an under-counted --repeat run could blow past an
    operator's --max-calls/--max-cost without ever tripping the gate.
    """

    def test_repeat_count_one_is_unchanged_from_the_pre_repeat_estimate(self) -> None:
        pairs = eval_runner.default_sweep_pairs()
        scenarios = eval_runner.SCENARIOS
        default = eval_runner.matrix_call_accounting(pairs, scenarios)
        explicit = eval_runner.matrix_call_accounting(pairs, scenarios, repeat_count=1)
        assert explicit.total_calls == default.total_calls
        assert explicit.estimated_cost_usd == default.estimated_cost_usd

    def test_repeat_count_three_triples_every_call_count(self) -> None:
        pairs = eval_runner.default_sweep_pairs()
        scenarios = eval_runner.SCENARIOS
        base = eval_runner.matrix_call_accounting(pairs, scenarios, repeat_count=1)
        repeated = eval_runner.matrix_call_accounting(pairs, scenarios, repeat_count=3)
        assert repeated.router_calls == base.router_calls * 3
        assert repeated.worker_calls == base.worker_calls * 3
        assert repeated.judge_calls == base.judge_calls * 3
        assert repeated.estimated_cost_usd == pytest.approx(base.estimated_cost_usd * 3)


class TestMajorityBool:
    def test_strict_majority_true(self) -> None:
        assert eval_runner._majority_bool([True, True, False]) is True

    def test_strict_majority_false(self) -> None:
        assert eval_runner._majority_bool([True, False, False]) is False

    def test_tie_resolves_to_false(self) -> None:
        assert eval_runner._majority_bool([True, False]) is False

    def test_none_entries_are_ignored_not_counted(self) -> None:
        # 2 real votes (both True) plus a never-evaluated repeat -- the
        # never-evaluated repeat must not count as a "no" vote.
        assert eval_runner._majority_bool([True, True, None]) is True

    def test_all_none_is_none(self) -> None:
        assert eval_runner._majority_bool([None, None]) is None


class TestCleanOutcomeRequiresStrictMajority:
    """Regression for round-4 restart, Codex P2: _majority_with_tiebreak()'s
    `clean` outcome must require a STRICT majority, not merely a plurality --
    with --repeat 4, (ok, ok, timeout, provider-error) is a 2/4 plurality for
    "ok", but half the repetitions did not complete cleanly.
    """

    def test_codexs_exact_case(self) -> None:
        # Pre-fix returns "ok".
        result = eval_runner._majority_with_tiebreak(
            ["ok", "ok", "timeout", "provider-error"],
            eval_runner._TURN_STATUS_TIE_PRIORITY,
            "ok",
        )
        assert result == "provider-error"

    def test_judge_case(self) -> None:
        # Pre-fix returns "yes".
        result = eval_runner._majority_with_tiebreak(
            ["yes", "yes", "no", "judge-error"],
            eval_runner._JUDGE_VERDICT_TIE_PRIORITY,
            "yes",
        )
        assert result == "judge-error"

    def test_strict_majority_still_passes(self) -> None:
        assert (
            eval_runner._majority_with_tiebreak(
                ["ok", "ok", "timeout"], eval_runner._TURN_STATUS_TIE_PRIORITY, "ok"
            )
            == "ok"
        )
        assert (
            eval_runner._majority_with_tiebreak(
                ["yes", "yes", "yes", "no"], eval_runner._JUDGE_VERDICT_TIE_PRIORITY, "yes"
            )
            == "yes"
        )

    def test_single_value(self) -> None:
        assert (
            eval_runner._majority_with_tiebreak(["ok"], eval_runner._TURN_STATUS_TIE_PRIORITY, "ok")
            == "ok"
        )

    def test_two_two_clean_dirty_tie_is_unchanged(self) -> None:
        # Already correct pre-fix (a genuine tie, not a plurality) -- pins no
        # regression.
        assert (
            eval_runner._majority_with_tiebreak(
                ["ok", "ok", "timeout", "timeout"], eval_runner._TURN_STATUS_TIE_PRIORITY, "ok"
            )
            == "timeout"
        )

    def test_end_to_end_repeat_4_cell_fails(self) -> None:
        """The finding as actually stated: the cell can pass with only half
        of its repetitions completing cleanly."""
        turn_repeats = [
            eval_runner.TurnOutcome(query="q", status="ok", judge_verdict="yes"),
            eval_runner.TurnOutcome(query="q", status="ok", judge_verdict="yes"),
            eval_runner.TurnOutcome(query="q", status="timeout"),
            eval_runner.TurnOutcome(query="q", status="provider-error"),
        ]
        aggregated_turn = eval_runner._aggregate_turn_repeats(
            turn_repeats,
            max_routing_seconds=eval_runner.DEFAULT_MAX_ROUTING_SECONDS,
            max_latency_seconds=eval_runner.DEFAULT_MAX_LATENCY_SECONDS,
        )
        assert aggregated_turn.status == "provider-error"

        cell_repeats = [
            eval_runner.CellOutcome(
                pair_label="p", scenario_name="s", status="ok", turns=[turn_repeats[0]]
            ),
            eval_runner.CellOutcome(
                pair_label="p", scenario_name="s", status="ok", turns=[turn_repeats[1]]
            ),
            eval_runner.CellOutcome(
                pair_label="p", scenario_name="s", status="timeout", turns=[turn_repeats[2]]
            ),
            eval_runner.CellOutcome(
                pair_label="p", scenario_name="s", status="provider-error", turns=[turn_repeats[3]]
            ),
        ]
        aggregated_cell = eval_runner._aggregate_cell_repeats(
            "p",
            "s",
            cell_repeats,
            max_routing_seconds=eval_runner.DEFAULT_MAX_ROUTING_SECONDS,
            max_latency_seconds=eval_runner.DEFAULT_MAX_LATENCY_SECONDS,
        )
        overall_status, _reasons = eval_runner.compute_pass_fail([aggregated_cell])
        assert overall_status == "FAIL"


class TestAggregateCellRepeats:
    """_aggregate_cell_repeats() majority-votes N independent run_cell()
    results for the SAME (pair, scenario) into one summary CellOutcome. The
    len==1 case is a strict identity (round-trips the exact same object) so
    the --repeat 1 default reproduces the pre-existing single-run report
    shape byte-for-byte.
    """

    def _turn(self, **overrides: Any) -> Any:
        base: dict[str, Any] = {"query": "q", "status": "ok", "judge_verdict": None}
        base.update(overrides)
        return eval_runner.TurnOutcome(**base)

    def _cell(
        self, *, status: eval_runner.CellStatus = "ok", turns: list[Any] | None = None
    ) -> Any:
        return eval_runner.CellOutcome(
            pair_label="router=baseline/worker=baseline",
            scenario_name="s",
            status=status,
            turns=turns if turns is not None else [self._turn()],
        )

    def _aggregate(
        self,
        cells: list[Any],
        *,
        max_routing_seconds: float = eval_runner.DEFAULT_MAX_ROUTING_SECONDS,
        max_latency_seconds: float = eval_runner.DEFAULT_MAX_LATENCY_SECONDS,
    ) -> Any:
        return eval_runner._aggregate_cell_repeats(
            "p",
            "s",
            cells,
            max_routing_seconds=max_routing_seconds,
            max_latency_seconds=max_latency_seconds,
        )

    def test_single_repeat_returns_the_same_object_unchanged(self) -> None:
        cell = self._cell()
        aggregated = self._aggregate([cell])
        assert aggregated is cell
        assert aggregated.repeats is None

    def test_raw_repeats_are_attached_for_audit(self) -> None:
        cells = [self._cell(), self._cell(), self._cell()]
        aggregated = self._aggregate(cells)
        assert aggregated.repeats == cells

    def test_majority_yes_verdict_wins(self) -> None:
        cells = [
            self._cell(turns=[self._turn(judge_verdict="yes")]),
            self._cell(turns=[self._turn(judge_verdict="yes")]),
            self._cell(turns=[self._turn(judge_verdict="no")]),
        ]
        aggregated = self._aggregate(cells)
        assert aggregated.turns is not None
        assert aggregated.turns[0].judge_verdict == "yes"
        assert "2/3" in (aggregated.turns[0].judge_reason or "")

    def test_tied_verdict_resolves_toward_the_failure_outcome(self) -> None:
        cells = [
            self._cell(turns=[self._turn(judge_verdict="yes")]),
            self._cell(turns=[self._turn(judge_verdict="no")]),
        ]
        aggregated = self._aggregate(cells)
        assert aggregated.turns is not None
        assert aggregated.turns[0].judge_verdict == "no"

    def test_majority_ok_status_reports_ok(self) -> None:
        cells = [self._cell(status="ok"), self._cell(status="ok"), self._cell(status="timeout")]
        aggregated = self._aggregate(cells)
        assert aggregated.status == "ok"
        # A minority infra failure still surfaces in `error` even though the
        # majority-voted status is "ok" -- see
        # test_minority_infra_failure_surfaces_in_error_without_flipping_status
        # (round 10 gauntlet, Logic finding 8). This test's own concern is the
        # STATUS vote, not error content.
        assert aggregated.error is not None
        assert "1/3" in aggregated.error

    def test_majority_infra_failure_reports_the_failure_with_a_summary(self) -> None:
        cells = [
            self._cell(status="timeout"),
            self._cell(status="timeout"),
            self._cell(status="ok"),
        ]
        aggregated = self._aggregate(cells)
        assert aggregated.status == "timeout"
        assert aggregated.error is not None
        assert "2/3" in aggregated.error

    def test_minority_infra_failure_surfaces_in_error_without_flipping_status(self) -> None:
        """Round 10 gauntlet, Logic finding 8: a minority provider-error
        repeat is real evidence about a live paid run and must survive into
        the aggregate's `error` field even when the majority voted "ok" --
        previously agg_error was gated on agg_status == "ok" and silently
        dropped it. Purely a reporting change: compute_pass_fail() gates on
        status, not error, so the verdict must stay PASS."""
        cells = [
            self._cell(status="ok"),
            self._cell(status="ok"),
            self._cell(status="provider-error"),
        ]
        aggregated = self._aggregate(cells)
        assert aggregated.status == "ok"
        assert aggregated.error is not None
        assert "1/3" in aggregated.error
        assert "provider-error" in aggregated.error
        overall_status, _reasons = eval_runner.compute_pass_fail([aggregated])
        assert overall_status == "PASS"

    def test_infra_failed_repeats_do_not_pollute_the_semantic_vote(self) -> None:
        # A judge=no from a genuinely-ok repeat outvotes... but an
        # infra-failed repeat's None judge_verdict must not count as a "no"
        # vote at all -- only the 2 ok repeats' real verdicts matter.
        cells = [
            self._cell(turns=[self._turn(status="ok", judge_verdict="yes")]),
            self._cell(turns=[self._turn(status="ok", judge_verdict="yes")]),
            self._cell(status="provider-error", turns=[self._turn(status="provider-error")]),
        ]
        aggregated = self._aggregate(cells)
        assert aggregated.turns is not None
        assert aggregated.turns[0].judge_verdict == "yes"
        assert "2/2" in (aggregated.turns[0].judge_reason or "")

    def test_latency_ms_fields_are_averaged_across_ok_repeats(self) -> None:
        cells = [
            self._cell(turns=[self._turn(total_ms=100.0)]),
            self._cell(turns=[self._turn(total_ms=200.0)]),
            self._cell(turns=[self._turn(total_ms=300.0)]),
        ]
        aggregated = self._aggregate(cells)
        assert aggregated.turns is not None
        assert aggregated.turns[0].total_ms == pytest.approx(200.0)

    # --- Round 10 gauntlet, Logic findings 5 and 6: latency_budget_exceeded
    # is recomputed from the aggregated means, not majority-voted, so it can
    # never disagree with the total_ms/routing_ms the same report publishes.

    def test_tied_latency_budget_exceeded_resolves_to_exceeded(self) -> None:
        """Finding 5: a 1-1 split, where a hand-voted tie-break would have
        applied, must not resolve to "not exceeded" via _majority_bool's
        tie-to-False rule -- the aggregate is instead recomputed from the
        mean, which this case is deliberately over budget."""
        cells = [
            self._cell(
                turns=[
                    self._turn(
                        total_ms=100_000.0,
                        latency_budget_exceeded=True,
                        latency_budget_enforced=True,
                    )
                ]
            ),
            self._cell(
                turns=[
                    self._turn(
                        total_ms=50_000.0,
                        latency_budget_exceeded=False,
                        latency_budget_enforced=True,
                    )
                ]
            ),
        ]
        # mean = 75,000 ms, over the 60s budget.
        aggregated = self._aggregate(cells, max_latency_seconds=60.0)
        assert aggregated.turns is not None
        assert aggregated.turns[0].latency_budget_exceeded is True
        overall_status, _reasons = eval_runner.compute_pass_fail([aggregated])
        assert overall_status == "FAIL"

    def test_majority_not_exceeded_but_mean_over_budget_is_still_exceeded(self) -> None:
        """Finding 6: a 2/3 majority voting 'not exceeded' must not outvote
        a mean that is provably over budget -- the aggregate's
        latency_budget_exceeded and its own total_ms must stay consistent."""
        cells = [
            self._cell(
                turns=[
                    self._turn(
                        total_ms=200_000.0,
                        latency_budget_exceeded=True,
                        latency_budget_enforced=True,
                    )
                ]
            ),
            self._cell(
                turns=[
                    self._turn(
                        total_ms=5_000.0,
                        latency_budget_exceeded=False,
                        latency_budget_enforced=True,
                    )
                ]
            ),
            self._cell(
                turns=[
                    self._turn(
                        total_ms=5_000.0,
                        latency_budget_exceeded=False,
                        latency_budget_enforced=True,
                    )
                ]
            ),
        ]
        # 2/3 repeats voted "not exceeded" -- the OLD majority-vote behavior
        # would have reported False here. The mean (70,000 ms) is over the
        # 60s budget, so the recomputed field must be True and must agree
        # with the total_ms this same aggregate publishes.
        aggregated = self._aggregate(cells, max_latency_seconds=60.0)
        assert aggregated.turns is not None
        turn = aggregated.turns[0]
        assert turn.latency_budget_exceeded is True
        assert turn.total_ms is not None and turn.total_ms > 60.0 * 1000

    def test_a_failed_repeat_zero_does_not_disable_the_enforced_budget_gate(self) -> None:
        """Round 11 gauntlet, Logic finding 1: latency_budget_enforced is only
        written on run_cell()'s measured path, so a repeat that failed before
        measuring carries the False default. Sampling repeat 0 let that False
        outvote two surviving repeats that measured a real baseline breach --
        compute_pass_fail()'s `enforced and exceeded` gate never fired and the
        cell reported PASS."""
        cells = [
            self._cell(
                status="provider-error",
                turns=[self._turn(status="provider-error")],  # enforced defaults False
            ),
            self._cell(
                turns=[
                    self._turn(
                        total_ms=200_000.0,
                        latency_budget_exceeded=True,
                        latency_budget_enforced=True,
                    )
                ]
            ),
            self._cell(
                turns=[
                    self._turn(
                        total_ms=200_000.0,
                        latency_budget_exceeded=True,
                        latency_budget_enforced=True,
                    )
                ]
            ),
        ]
        aggregated = self._aggregate(cells, max_latency_seconds=60.0)
        assert aggregated.turns is not None
        turn = aggregated.turns[0]
        assert turn.status == "ok"  # 2/3 majority
        assert turn.latency_budget_exceeded is True
        assert turn.latency_budget_enforced is True
        overall_status, reasons = eval_runner.compute_pass_fail([aggregated])
        assert overall_status == "FAIL"
        assert any("enforced latency budget exceeded" in r for r in reasons)

    def test_a_non_baseline_pairs_budget_stays_report_only(self) -> None:
        """any() must not manufacture enforcement: no repeat of a non-baseline
        pair ever sets latency_budget_enforced, so the aggregate stays False and
        an over-budget mean is reported without failing the run."""
        cells = [
            self._cell(turns=[self._turn(total_ms=200_000.0, latency_budget_exceeded=True)]),
            self._cell(turns=[self._turn(total_ms=200_000.0, latency_budget_exceeded=True)]),
        ]
        aggregated = self._aggregate(cells, max_latency_seconds=60.0)
        assert aggregated.turns is not None
        assert aggregated.turns[0].latency_budget_enforced is False
        assert eval_runner.compute_pass_fail([aggregated])[0] == "PASS"

    def test_routing_leg_over_budget_is_exceeded_even_when_total_ms_is_fine(self) -> None:
        cells = [
            self._cell(turns=[self._turn(routing_ms=20_000.0, total_ms=10.0)]),
        ]
        aggregated = self._aggregate(
            [cells[0], cells[0]], max_routing_seconds=15.0, max_latency_seconds=60.0
        )
        assert aggregated.turns is not None
        assert aggregated.turns[0].latency_budget_exceeded is True

    def test_no_latency_metrics_stays_none_not_false(self) -> None:
        cells = [
            self._cell(turns=[self._turn(routing_ms=None, total_ms=None)]),
            self._cell(turns=[self._turn(routing_ms=None, total_ms=None)]),
        ]
        aggregated = self._aggregate(cells)
        assert aggregated.turns is not None
        assert aggregated.turns[0].latency_budget_exceeded is None

    # --- Round 10 gauntlet, Logic finding 7: deterministic_action_unevaluated
    # _reason is majority-gated, matching its sibling deterministic_action_pass,
    # instead of propagating on any single unevaluated repeat.

    def test_minority_unevaluated_repeat_does_not_fail_a_clean_majority(self) -> None:
        cells = [
            self._cell(
                turns=[
                    self._turn(
                        deterministic_action_pass=True, deterministic_action_unevaluated_reason=None
                    )
                ]
            ),
            self._cell(
                turns=[
                    self._turn(
                        deterministic_action_pass=True, deterministic_action_unevaluated_reason=None
                    )
                ]
            ),
            self._cell(
                turns=[
                    self._turn(
                        deterministic_action_pass=None,
                        deterministic_action_unevaluated_reason="routing_action was unavailable",
                    )
                ]
            ),
        ]
        aggregated = self._aggregate(cells)
        assert aggregated.turns is not None
        turn = aggregated.turns[0]
        assert turn.deterministic_action_unevaluated_reason is None
        overall_status, _reasons = eval_runner.compute_pass_fail([aggregated])
        assert overall_status == "PASS"
        assert turn.error is not None
        assert "routing_action was unavailable" in turn.error

    def test_majority_unevaluated_repeats_fail_the_run(self) -> None:
        cells = [
            self._cell(
                turns=[
                    self._turn(
                        deterministic_action_pass=None,
                        deterministic_action_unevaluated_reason="routing_action was unavailable",
                    )
                ]
            ),
            self._cell(
                turns=[
                    self._turn(
                        deterministic_action_pass=None,
                        deterministic_action_unevaluated_reason="routing_action was unavailable",
                    )
                ]
            ),
            self._cell(
                turns=[
                    self._turn(
                        deterministic_action_pass=True, deterministic_action_unevaluated_reason=None
                    )
                ]
            ),
        ]
        aggregated = self._aggregate(cells)
        assert aggregated.turns is not None
        turn = aggregated.turns[0]
        assert turn.deterministic_action_unevaluated_reason is not None
        overall_status, reasons = eval_runner.compute_pass_fail([aggregated])
        assert overall_status == "FAIL"
        assert any(reason.startswith("infra:") and "unevaluated" in reason for reason in reasons)

    def test_exact_tie_is_not_a_strict_majority(self) -> None:
        cells = [
            self._cell(
                turns=[
                    self._turn(
                        deterministic_action_pass=None,
                        deterministic_action_unevaluated_reason="routing_action was unavailable",
                    )
                ]
            ),
            self._cell(
                turns=[
                    self._turn(
                        deterministic_action_pass=True, deterministic_action_unevaluated_reason=None
                    )
                ]
            ),
        ]
        aggregated = self._aggregate(cells)
        assert aggregated.turns is not None
        assert aggregated.turns[0].deterministic_action_unevaluated_reason is None

    # --- Round 10 gauntlet, Logic finding 10: CellOutcome.repeats is
    # depth-1 -- an already-aggregated cell must never be fed back in as a
    # raw input repeat.

    def test_rejects_an_already_aggregated_cell_as_an_input_repeat(self) -> None:
        already_aggregated = self._aggregate([self._cell(), self._cell()])
        assert already_aggregated.repeats is not None

        with pytest.raises(ValueError, match="depth-1"):
            self._aggregate([already_aggregated, self._cell()])

    def test_a_normal_aggregates_repeats_are_all_depth_one(self) -> None:
        aggregated = self._aggregate([self._cell(), self._cell(), self._cell()])
        assert aggregated.repeats is not None
        assert all(repeat.repeats is None for repeat in aggregated.repeats)

    def test_an_already_aggregated_lone_cell_is_rejected_not_round_tripped(self) -> None:
        """Round 11 gauntlet, Minor A: the depth-1 guard ran after the len==1
        identity return, so a single already-aggregated cell was returned
        unchanged and _serialize_cell would emit depth-2 `repeats`."""
        nested = self._aggregate([self._cell(), self._cell(), self._cell()])
        assert nested.repeats is not None
        with pytest.raises(ValueError, match="depth-1"):
            self._aggregate([nested])


class TestRunMatrixRepeatWiring:
    """run_matrix()'s repeat_count param must call run_cell() that many
    times per (pair, scenario) and feed the results through
    _aggregate_cell_repeats() -- not just the first result.
    """

    def test_repeat_count_default_calls_run_cell_once_per_cell(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[Any] = []

        async def _fake_run_cell(pair: Any, scenario: Any, *_a: Any, **_k: Any) -> Any:
            calls.append((pair, scenario))
            return eval_runner.CellOutcome(
                pair_label=pair.label, scenario_name=scenario.name, status="ok", turns=[]
            )

        monkeypatch.setattr(eval_runner, "run_cell", _fake_run_cell)
        monkeypatch.setattr(eval_runner, "candidate_accepted", lambda *_a, **_k: True)

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        scenario = Scenario(name="s", turns=(Turn(query="hi"),))
        status = eval_runner.ManifestStatus(
            path=Path("/nonexistent"),
            exists=True,
            source_commit="deadbeef",
            current_commit="deadbeef",
            stale=False,
            accepted=frozenset(),
        )

        outcomes = asyncio.run(
            eval_runner.run_matrix(
                (pair,),
                (scenario,),
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
                manifest_status=status,
            )
        )

        assert len(calls) == 1
        assert outcomes[0].repeats is None

    def test_repeat_count_three_calls_run_cell_three_times_and_aggregates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        call_count = 0

        async def _fake_run_cell(pair: Any, scenario: Any, *_a: Any, **_k: Any) -> Any:
            nonlocal call_count
            call_count += 1
            status: eval_runner.CellStatus = "ok" if call_count != 2 else "timeout"
            return eval_runner.CellOutcome(
                pair_label=pair.label, scenario_name=scenario.name, status=status, turns=[]
            )

        monkeypatch.setattr(eval_runner, "run_cell", _fake_run_cell)
        monkeypatch.setattr(eval_runner, "candidate_accepted", lambda *_a, **_k: True)

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        scenario = Scenario(name="s", turns=(Turn(query="hi"),))
        status = eval_runner.ManifestStatus(
            path=Path("/nonexistent"),
            exists=True,
            source_commit="deadbeef",
            current_commit="deadbeef",
            stale=False,
            accepted=frozenset(),
        )

        outcomes = asyncio.run(
            eval_runner.run_matrix(
                (pair,),
                (scenario,),
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
                manifest_status=status,
                repeat_count=3,
            )
        )

        assert call_count == 3
        assert len(outcomes) == 1
        # 2/3 ok -> majority is "ok" despite one timeout repeat.
        assert outcomes[0].status == "ok"
        assert outcomes[0].repeats is not None
        assert len(outcomes[0].repeats) == 3


class TestRunMatrixRejectsANonPositiveRepeatCount:
    """Round 10 gauntlet, Logic finding 9: run_matrix(repeat_count=0) built
    repeats=[], skipped _aggregate_cell_repeats' len==1 shortcut, and crashed
    inside _majority_with_tiebreak's max() on an empty Counter with a bare,
    unphraseable ValueError. run_matrix() is a public coroutine callable
    directly, bypassing the CLI's --repeat _positive_int() guard -- as the
    other tests in this file already do -- so it must validate itself.
    """

    def _run(self, monkeypatch: pytest.MonkeyPatch, *, repeat_count: int) -> list[Any]:
        calls: list[Any] = []

        async def _fake_run_cell(pair: Any, scenario: Any, *_a: Any, **_k: Any) -> Any:
            calls.append((pair, scenario))
            return eval_runner.CellOutcome(
                pair_label=pair.label, scenario_name=scenario.name, status="ok", turns=[]
            )

        monkeypatch.setattr(eval_runner, "run_cell", _fake_run_cell)
        monkeypatch.setattr(eval_runner, "candidate_accepted", lambda *_a, **_k: True)

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        scenario = Scenario(name="s", turns=(Turn(query="hi"),))
        status = eval_runner.ManifestStatus(
            path=Path("/nonexistent"),
            exists=True,
            source_commit="deadbeef",
            current_commit="deadbeef",
            stale=False,
            accepted=frozenset(),
        )

        with pytest.raises(ValueError, match="repeat_count must be at least 1"):
            asyncio.run(
                eval_runner.run_matrix(
                    (pair,),
                    (scenario,),
                    eval_runner.Config(),
                    judge_model="gpt-5-mini",
                    max_routing_seconds=15.0,
                    max_latency_seconds=15.0,
                    manifest_status=status,
                    repeat_count=repeat_count,
                )
            )
        return calls

    def test_zero_repeat_count_raises_before_any_call(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = self._run(monkeypatch, repeat_count=0)
        assert calls == []

    def test_negative_repeat_count_raises_before_any_call(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = self._run(monkeypatch, repeat_count=-1)
        assert calls == []

    def test_majority_with_tiebreak_rejects_an_empty_sequence(self) -> None:
        with pytest.raises(ValueError):
            eval_runner._majority_with_tiebreak([], eval_runner._CELL_STATUS_TIE_PRIORITY, "ok")


class TestBuildReportRepeatCount:
    def test_repeat_count_one_matches_the_pre_repeat_note(self) -> None:
        report = eval_runner.build_report([], judge_model="gpt-5-mini")
        assert report["repetition_count_per_cell"] == 1
        assert "n=1 per cell" in report["repetition_note"]

    def test_repeat_count_three_is_labeled_and_serializes_raw_repeats(self) -> None:
        cell = eval_runner.CellOutcome(
            pair_label="p",
            scenario_name="s",
            status="ok",
            turns=[eval_runner.TurnOutcome(query="q", status="ok")],
            repeats=[
                eval_runner.CellOutcome(pair_label="p", scenario_name="s", status="ok", turns=[]),
                eval_runner.CellOutcome(pair_label="p", scenario_name="s", status="ok", turns=[]),
                eval_runner.CellOutcome(
                    pair_label="p", scenario_name="s", status="timeout", turns=[]
                ),
            ],
        )
        report = eval_runner.build_report([cell], judge_model="gpt-5-mini", repeat_count=3)
        assert report["repetition_count_per_cell"] == 3
        assert "majority vote" in report["repetition_note"]
        serialized_repeats = report["cells"][0]["repeats"]
        assert serialized_repeats is not None
        assert len(serialized_repeats) == 3
        assert [r["status"] for r in serialized_repeats] == ["ok", "ok", "timeout"]

    def test_a_cell_with_no_repeats_omits_the_repeats_key(self) -> None:
        # Round-10 gauntlet confirming pass, Codex P2: a single-run
        # (--repeat 1, the default) cell must not gain a "repeats" key at
        # all -- this predates the --repeat feature entirely, so a consumer
        # doing strict schema/key-set comparison against the pre-feature
        # shape must see no new key, not a new key holding None.
        cell = eval_runner.CellOutcome(
            pair_label="p",
            scenario_name="s",
            status="ok",
            turns=[eval_runner.TurnOutcome(query="q", status="ok")],
        )
        report = eval_runner.build_report([cell], judge_model="gpt-5-mini")
        assert "repeats" not in report["cells"][0]


# ---------------------------------------------------------------------------
# Dry-run: zero live calls invariant
# ---------------------------------------------------------------------------


class TestDryRunMakesZeroLiveCalls:
    """A live call sneaking into --dry-run must fail the test loudly, not
    silently pass. The OpenAI client construction and SessionHost lifecycle
    methods are patched to raise if invoked at all.
    """

    def test_dry_run_never_constructs_a_session_host(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(eval_common, "build_session_for_run", _raise_network_access)
        monkeypatch.setattr(
            eval_runner, "build_session_for_run", _raise_network_access, raising=False
        )

        eval_runner.main(["--dry-run"])

        out = capsys.readouterr().out
        assert out  # something was printed -- the matrix/cost preview

    def test_dry_run_never_touches_the_openai_client(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import openai

        monkeypatch.setattr(openai, "OpenAI", _raise_network_access)
        monkeypatch.setattr(openai, "AsyncOpenAI", _raise_network_access)

        # Must not raise _NetworkAccessError: dry-run must never reach an
        # OpenAI client constructor.
        eval_runner.main(["--dry-run"])

    def test_dry_run_prints_total_call_count_and_cost_estimate(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        eval_runner.main(["--dry-run"])
        out = capsys.readouterr().out.lower()
        assert "call" in out
        assert "cost" in out or "$" in out


# ---------------------------------------------------------------------------
# --max-calls / --max-cost confirmation gate
# ---------------------------------------------------------------------------


class TestSpendConfirmationGate:
    """A run that would exceed --max-calls or --max-cost must prompt/block
    without explicit confirmation, and must not proceed to any live call
    without it.
    """

    def _patch_for_spend_gate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # main() itself returns an int (never raises SystemExit -- only the
        # __main__ guard does that), so these tests assert on the return
        # code directly. Bypass the two gates that run *before* the spend
        # check -- manifest freshness (the committed manifest's
        # source_commit predates the current checkout in this environment)
        # and the OPENAI_API_KEY credential check -- so a --max-calls/
        # --max-cost refusal isn't confounded with an unrelated earlier gate.
        monkeypatch.setattr(eval_common, "build_session_for_run", _raise_network_access)
        monkeypatch.setattr(
            eval_runner, "build_session_for_run", _raise_network_access, raising=False
        )
        monkeypatch.setattr("builtins.input", lambda *_a, **_k: "n")
        monkeypatch.setattr(
            eval_runner, "load_config", lambda: eval_runner.Config(openai_api_key="test-key")
        )

    def test_exceeding_max_calls_without_confirmation_blocks(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # An absurdly low --max-calls guarantees the estimated matrix
        # exceeds it; stdin is not a tty and no --yes/--confirm is passed,
        # so the runner must refuse rather than silently proceeding to a
        # live call.
        self._patch_for_spend_gate(monkeypatch)

        exit_code = eval_runner.main(["--max-calls", "1", "--i-know-the-manifest-is-stale"])
        assert exit_code != 0

    def test_exceeding_max_cost_without_confirmation_blocks(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._patch_for_spend_gate(monkeypatch)

        exit_code = eval_runner.main(["--max-cost", "0.0001", "--i-know-the-manifest-is-stale"])
        assert exit_code != 0

    def test_confirmation_gate_is_bypassed_by_dry_run(self) -> None:
        # --dry-run makes zero live calls regardless of --max-calls, so a
        # tiny --max-calls under --dry-run must not need confirmation at all.
        eval_runner.main(["--dry-run", "--max-calls", "1"])


class TestSpendEstimateShownBeforeLiveRun:
    """Regression: on a normal live invocation where no spend limit is
    exceeded, the operator must still see the total-call-count/cost estimate
    before any live call happens -- _confirm_spend() itself returns silently
    when under budget, so main() must print the preview unconditionally on
    the live-run path, not only when a limit would be exceeded.
    """

    def test_preview_is_printed_on_a_normal_live_run_within_limits(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        monkeypatch.setattr(eval_common, "build_session_for_run", _raise_network_access)
        monkeypatch.setattr(
            eval_runner, "build_session_for_run", _raise_network_access, raising=False
        )
        monkeypatch.setattr(
            eval_runner, "load_config", lambda: eval_runner.Config(openai_api_key="test-key")
        )
        # This run completes the full flow (through report persistence) --
        # redirect the default report location into tmp_path so the test
        # doesn't write into the real repo tree's .review-plan/.
        monkeypatch.setattr(eval_runner, "DEFAULT_REPORT_DIR", tmp_path / "eval-reports")

        # No --max-calls/--max-cost passed at all -- nothing to exceed, so
        # _confirm_spend() alone would print nothing.
        eval_runner.main(
            [
                "--router",
                "baseline",
                "--worker",
                "baseline",
                "--scenario",
                "single-turn-default",
                "--i-know-the-manifest-is-stale",
            ]
        )

        out = capsys.readouterr().out.lower()
        assert "call" in out
        assert "cost" in out or "$" in out


# ---------------------------------------------------------------------------
# Manifest-staleness / absence refusal
# ---------------------------------------------------------------------------


class TestManifestGate:
    """A (model, effort) combination absent from Phase 0's manifest -- or a
    manifest whose source_commit is stale relative to the current commit --
    is refused, not silently run. The real API is
    ``load_manifest_status()``/``candidate_accepted()``/
    ``require_manifest_ok_for_live_run()`` (never raises on load; the
    presence/freshness/acceptance checks are combined into one preflight
    gate), not the guessed ``load_manifest()``/``require_manifest_coverage()``/
    ``require_manifest_freshness()`` split.
    """

    def _full_manifest_results(self) -> list[dict[str, Any]]:
        results = [_accepted_router_entry(eval_runner.ROUTER_BASELINE.model, "minimal")]
        results += [
            _accepted_router_entry(c.model, c.effort) for c in eval_runner.ROUTER_CANDIDATES
        ]
        results.append(
            _accepted_worker_entry(
                eval_runner.WORKER_BASELINE.model, eval_runner.WORKER_BASELINE.effort
            )
        )
        results += [
            _accepted_worker_entry(c.model, c.effort) for c in eval_runner.WORKER_CANDIDATES
        ]
        results.append(_accepted_judge_entry(eval_runner.DEFAULT_JUDGE_MODEL))
        return results

    def test_manifest_missing_file_is_refused(self, tmp_path: Path) -> None:
        missing_path = tmp_path / "does-not-exist.json"
        status = eval_runner.load_manifest_status(missing_path)
        assert status.exists is False
        with pytest.raises(eval_runner.ManifestError):
            eval_runner.require_manifest_ok_for_live_run(
                status,
                allow_stale=False,
                candidates=(eval_runner.ROUTER_BASELINE,),
                judge_model=eval_runner.DEFAULT_JUDGE_MODEL,
            )

    def test_undecodable_manifest_is_treated_as_malformed_not_raised(self, tmp_path: Path) -> None:
        # Path.read_text() raises UnicodeDecodeError for a non-UTF-8 byte
        # sequence -- this is NOT a subclass of json.JSONDecodeError, so a
        # guard written as `except (OSError, json.JSONDecodeError)` misses
        # it. main() calls load_manifest_status() before the guarded
        # live-run path, so an uncaught UnicodeDecodeError here would
        # surface as a raw traceback instead of the documented fail-closed
        # status.
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_bytes(b"\xff\xfe\x00\x81not valid utf-8")

        status = eval_runner.load_manifest_status(manifest_path)

        assert status.exists is True
        assert status.stale is True
        assert status.accepted == frozenset()
        with pytest.raises(eval_runner.ManifestError):
            eval_runner.require_manifest_ok_for_live_run(
                status,
                allow_stale=False,
                candidates=(eval_runner.ROUTER_BASELINE,),
                judge_model=eval_runner.DEFAULT_JUDGE_MODEL,
            )

    def test_combination_absent_from_manifest_is_refused(self, tmp_path: Path) -> None:
        # Manifest only covers the baseline, not the router candidates.
        manifest_path = _write_manifest(
            tmp_path,
            source_commit="deadbeef",
            results=[_accepted_router_entry(eval_runner.ROUTER_BASELINE.model, "minimal")],
        )
        status = eval_runner.load_manifest_status(manifest_path)
        assert eval_runner.candidate_accepted(eval_runner.ROUTER_CANDIDATES[0], status) is False

        with pytest.raises(eval_runner.ManifestError):
            eval_runner.require_manifest_ok_for_live_run(
                status,
                allow_stale=True,  # isolate the missing-combination failure from staleness
                candidates=(eval_runner.ROUTER_CANDIDATES[0], eval_runner.WORKER_BASELINE),
                judge_model=eval_runner.DEFAULT_JUDGE_MODEL,
            )

    def test_combination_present_and_accepted_is_allowed(self, tmp_path: Path) -> None:
        manifest_path = _write_manifest(
            tmp_path, source_commit="deadbeef", results=self._full_manifest_results()
        )
        status = eval_runner.load_manifest_status(manifest_path)

        # Must not raise: every candidate named here is present and accepted.
        eval_runner.require_manifest_ok_for_live_run(
            status,
            allow_stale=True,
            candidates=(eval_runner.ROUTER_CANDIDATES[0], eval_runner.WORKER_CANDIDATES[0]),
            judge_model=eval_runner.DEFAULT_JUDGE_MODEL,
        )

    def test_rejected_combination_is_refused_even_if_present(self, tmp_path: Path) -> None:
        rejected = _accepted_router_entry(
            eval_runner.ROUTER_CANDIDATES[0].model, eval_runner.ROUTER_CANDIDATES[0].effort
        )
        rejected["accepted"] = False
        rejected["error"] = "model rejected this effort level"
        manifest_path = _write_manifest(
            tmp_path,
            source_commit="deadbeef",
            results=[
                _accepted_router_entry(eval_runner.ROUTER_BASELINE.model, "minimal"),
                rejected,
            ],
        )
        status = eval_runner.load_manifest_status(manifest_path)
        assert eval_runner.candidate_accepted(eval_runner.ROUTER_CANDIDATES[0], status) is False

        with pytest.raises(eval_runner.ManifestError):
            eval_runner.require_manifest_ok_for_live_run(
                status,
                allow_stale=True,
                candidates=(eval_runner.ROUTER_CANDIDATES[0], eval_runner.WORKER_BASELINE),
                judge_model=eval_runner.DEFAULT_JUDGE_MODEL,
            )

    def test_stale_manifest_source_commit_is_refused_without_override(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            eval_runner,
            "git_head",
            lambda: "ffffffffffffffffffffffffffffffffffffffff",
        )
        manifest_path = _write_manifest(
            tmp_path,
            source_commit="0000000000000000000000000000000000000000",
            results=self._full_manifest_results(),
        )
        status = eval_runner.load_manifest_status(manifest_path)
        assert status.stale is True

        with pytest.raises(eval_runner.ManifestError):
            eval_runner.require_manifest_ok_for_live_run(
                status,
                allow_stale=False,
                candidates=(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE),
                judge_model=eval_runner.DEFAULT_JUDGE_MODEL,
            )

    def test_stale_manifest_is_allowed_with_explicit_override(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            eval_runner,
            "git_head",
            lambda: "ffffffffffffffffffffffffffffffffffffffff",
        )
        manifest_path = _write_manifest(
            tmp_path,
            source_commit="0000000000000000000000000000000000000000",
            results=self._full_manifest_results(),
        )
        status = eval_runner.load_manifest_status(manifest_path)
        assert status.stale is True

        # Must not raise when the operator has explicitly acknowledged staleness.
        eval_runner.require_manifest_ok_for_live_run(
            status,
            allow_stale=True,
            candidates=(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE),
            judge_model=eval_runner.DEFAULT_JUDGE_MODEL,
        )

    def test_real_phase0_manifest_shape_is_readable(self) -> None:
        """Sanity check against Phase 0's real, committed manifest -- confirms
        the loader accepts the actual on-disk shape, not just a synthetic
        fixture. No network access; this is a static file read.
        """
        real_manifest_path = Path("docs/dev_plans/artifacts/eval-candidates-manifest.json")
        assert real_manifest_path.exists(), "Phase 0's manifest must be committed to the repo"
        status = eval_runner.load_manifest_status(real_manifest_path)
        assert status.exists is True
        assert status.source_commit is not None
        assert status.accepted


# ---------------------------------------------------------------------------
# Judge scoring semantics
# ---------------------------------------------------------------------------


class TestJudgeScoringSemantics:
    """Citations are checked deterministically (never via a judge criterion,
    since spoken_text forbids citation markers); display_text (``ui_text``)
    -- not spoken_text -- is what gets fed to the judge as the assistant
    reply; and judge-error classification collapses a call-failure reason
    into ``"judge-error"`` while a genuine yes/no verdict passes through
    as-is. This logic lives inline in ``run_cell()`` (no standalone
    ``check_citations()``/``feed_turn_to_judge()``/``classify_judge_verdict()``
    helpers exist), so these tests drive ``run_cell()`` end-to-end with the
    ``SessionHost``/judge seams faked out.
    """

    def test_citation_pass_flag_is_deterministic_not_a_judge_call(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?", expect_delegated=True, expect_citations=True),),
            citations=["citation-1"],
            verdicts=[],
        )
        assert outcome.turns[0].citations_pass is True

    def test_no_citations_fails_the_deterministic_check(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?", expect_delegated=True, expect_citations=True),),
            citations=[],
            verdicts=[],
        )
        assert outcome.turns[0].citations_pass is False

    def test_delegated_turn_without_expect_citations_leaves_the_check_unevaluated(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression: citations_pass used to be driven by expect_delegated
        alone, so a genuinely delegated turn whose hosted worker cannot
        yield a citable URL (e.g. a weather query answered via the
        web_search tool's internal oai-weather sub-tool, confirmed via a
        live probe) always failed this check regardless of the reply's
        actual quality. expect_citations now gates it independently --
        expect_delegated=True with expect_citations left at its False
        default must leave citations_pass unevaluated (None), not compute a
        spurious False from empty citations.
        """
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?", expect_delegated=True),),
            citations=[],
            verdicts=[],
        )
        assert outcome.turns[0].citations_pass is None

    def test_judge_is_fed_display_text_not_spoken_text(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        recorder: list[Any] = []
        _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?"),),
            ui_text="display projection",
            spoken_text="spoken projection",
            verdicts=[],
            judge_recorder=recorder,
        )
        judge = recorder[0]
        assert judge.assistant_messages == ["display projection"]
        assert "spoken projection" not in judge.assistant_messages

    def test_judge_max_tokens_comes_from_eval_common_judge_max_tokens(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Round 10 gauntlet, Logic finding 3: the EvalJudge cap and
        ``build_judge_request_kwargs``'s default must be the same constant,
        not two independently-maintained literals."""
        from scripts import eval_common

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        recorder: list[Any] = []
        _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?"),),
            verdicts=[],
            judge_recorder=recorder,
        )
        judge = recorder[0]
        assert judge.init_kwargs["max_tokens"] == eval_common.JUDGE_MAX_TOKENS

    def test_judge_call_failed_reason_is_classified_as_judge_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from pipecat.evals.judge import JudgeVerdict

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        verdict = JudgeVerdict(
            verdict="no", reason="judge call failed: RateLimitError", raw_response=""
        )
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?", judge_criterion="names a temperature"),),
            verdicts=[verdict],
        )
        assert outcome.turns[0].judge_verdict == "judge-error"

    def test_empty_judge_response_is_classified_as_judge_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from pipecat.evals.judge import JudgeVerdict

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        verdict = JudgeVerdict(
            verdict="no", reason="judge returned empty response", raw_response=""
        )
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?", judge_criterion="names a temperature"),),
            verdicts=[verdict],
        )
        assert outcome.turns[0].judge_verdict == "judge-error"

    def test_unparsable_judge_response_is_classified_as_judge_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from pipecat.evals.judge import JudgeVerdict

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        verdict = JudgeVerdict(
            verdict="no",
            reason="could not parse judge response: 'garbage, not json'",
            raw_response="garbage, not json",
        )
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?", judge_criterion="names a temperature"),),
            verdicts=[verdict],
        )
        assert outcome.turns[0].judge_verdict == "judge-error"

    def test_genuine_yes_verdict_passes_through_as_yes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from pipecat.evals.judge import JudgeVerdict

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        verdict = JudgeVerdict(
            verdict="yes", reason="names a specific temperature for Riga", raw_response="{}"
        )
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?", judge_criterion="names a temperature"),),
            verdicts=[verdict],
        )
        assert outcome.turns[0].judge_verdict == "yes"

    def test_genuine_no_verdict_is_a_semantic_fail_not_an_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from pipecat.evals.judge import JudgeVerdict

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        verdict = JudgeVerdict(
            verdict="no", reason="does not mention a temperature or condition", raw_response="{}"
        )
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?", judge_criterion="names a temperature"),),
            verdicts=[verdict],
        )
        assert outcome.turns[0].judge_verdict == "no"

    def test_continue_verdict_passes_through_uncollapsed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The plan's checklist vocabulary for judge_verdict is
        # yes/no/judge-error (plan line 169); "continue" is preserved as-is
        # rather than collapsed into a pass/fail bucket. There is no
        # aggregate pass/fail rollup anywhere in this runner --
        # build_report()/print_report_summary() print the raw per-turn
        # verdict for a human to read -- so "continue" is not reclassified
        # as "fail" by run_cell().
        from pipecat.evals.judge import JudgeVerdict

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        verdict = JudgeVerdict(verdict="continue", reason="interim filler reply", raw_response="{}")
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?", judge_criterion="names a temperature"),),
            verdicts=[verdict],
        )
        assert outcome.turns[0].judge_verdict == "continue"


class TestGenuineUnsupportedIsDistinctFromInfraFailure:
    """Regression for round-5 gauntlet finding 10: a genuine, on-topic
    `action="unsupported"` routing decision renders through the exact same
    SAFE_FALLBACKS text (`_CAPABILITY_UNAVAILABLE_TEXT`) as a true
    infrastructure failure (dispatch raising RoutingValidationError/
    UnsupportedWorkerType) -- text alone can't tell them apart. Only the
    latter should short-circuit as `provider-error`; the former must reach
    judge scoring so the model is held accountable for the misrouting.
    """

    _CAPABILITY_UNAVAILABLE_TEXT = "I cannot access that capability here."

    def test_genuine_unsupported_action_reaches_judge_scoring(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from pipecat.evals.judge import JudgeVerdict

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        verdict = JudgeVerdict(
            verdict="no", reason="wrongly declined an answerable request", raw_response="{}"
        )
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(
                Turn(
                    query="what's the weather in Riga?",
                    judge_criterion="names a specific weather condition or temperature",
                ),
            ),
            ui_text=self._CAPABILITY_UNAVAILABLE_TEXT,
            routing_action="unsupported",
            verdicts=[verdict],
        )
        # Reached judge scoring (a real, non-infra outcome) rather than
        # being short-circuited as a provider-error before the judge ever
        # saw it.
        assert outcome.status == "ok"
        assert outcome.turns[0].status == "ok"
        assert outcome.turns[0].judge_verdict == "no"

    def test_capability_unavailable_text_without_unsupported_action_is_still_provider_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="what's the weather in Riga?", expect_delegated=True),),
            ui_text=self._CAPABILITY_UNAVAILABLE_TEXT,
            # A dispatch failure after a new_worker/existing_worker decision
            # -- not "unsupported" -- still renders the same fallback text,
            # and must still short-circuit as an infra failure.
            routing_action="new_worker",
            verdicts=[],
        )
        assert outcome.status == "provider-error"
        assert outcome.turns[0].status == "provider-error"

    def test_prior_turn_stale_unsupported_routing_is_not_trusted_for_this_turn(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression for round-6 gauntlet finding: host.state.routing can
        still hold a PRIOR turn's decision if this turn's own routing/
        dispatch call fails before ever assigning a new one (server/
        pipeline.py only calls state.set_routing() after a successful
        routed outcome). A stale action="unsupported" read from turn N-1
        must not be trusted for turn N's genuine infra failure -- it must
        still short-circuit as provider-error, not be misread as the
        prior turn's semantic "unsupported" outcome.
        """
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="what's the weather in Riga?", expect_delegated=True),),
            ui_text=self._CAPABILITY_UNAVAILABLE_TEXT,
            # host.state.routing carries action="unsupported", but for
            # "turn-0" -- a stale read left over from a prior turn, not the
            # current turn's result ("turn-1", _run_cell's default).
            routing_action="unsupported",
            routing_turn_id="turn-0",
            verdicts=[],
        )
        assert outcome.status == "provider-error"
        assert outcome.turns[0].status == "provider-error"


class TestInfraFailureReasonIsSeparateFromJudgeReason:
    """Regression for round 8 gauntlet, Architecture finding 9:
    ``TurnOutcome.judge_reason`` previously doubled as a catch-all for every
    non-judge infra failure (provider error, safe-fallback text, ...), so a
    reader couldn't tell "the judge said this" from "the judge never ran,
    this is why". Non-judge infra failures now write into the dedicated
    ``error`` field, leaving ``judge_reason`` populated only by an actual
    judge call.
    """

    def test_a_provider_error_populates_error_and_leaves_judge_reason_unset(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="what's the weather in Riga?", expect_delegated=True),),
            ui_text="I cannot access that capability here.",
            routing_action="new_worker",
            verdicts=[],
        )
        turn = outcome.turns[0]
        assert turn.error == "host returned a safe fallback"
        assert turn.judge_reason is None

    def test_a_judge_error_populates_judge_reason_and_leaves_error_unset(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from pipecat.evals.judge import JudgeVerdict

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        verdict = JudgeVerdict(
            verdict="no", reason="judge call failed: RateLimitError", raw_response=""
        )
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?", judge_criterion="names a temperature"),),
            verdicts=[verdict],
        )
        turn = outcome.turns[0]
        assert turn.judge_verdict == "judge-error"
        assert "judge call failed" in (turn.judge_reason or "")
        assert turn.error is None


class TestWorkerPresenceAssertionCatchesRoutingRegressions:
    """Regression for round-5 gauntlet finding 12: the routing-regression
    scenario previously only asserted `action == expect_action` for the
    greeting turn and citations for delegated turns -- a turn that
    delegates when it should have answered directly (or vice versa) could
    still pass with a superficially valid reply. `worker_presence_pass` now
    catches that directly from the routing action, independent of reply
    text or judge verdict.
    """

    def test_should_be_direct_turn_that_actually_delegates_is_caught(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="Hi.", expect_action="direct", expect_delegated=False),),
            citations=["citation-1"],
            # Routing regression: the router misrouted a plain greeting to
            # the worker instead of answering directly.
            routing_action="new_worker",
            verdicts=[],
        )
        assert outcome.turns[0].worker_presence_pass is False

    def test_delegated_turn_that_actually_stays_direct_is_caught(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?", expect_delegated=True),),
            routing_action="direct",
            verdicts=[],
        )
        assert outcome.turns[0].worker_presence_pass is False

    def test_correctly_routed_turns_pass(self, monkeypatch: pytest.MonkeyPatch) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        direct_outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="Hi.", expect_action="direct", expect_delegated=False),),
            routing_action="direct",
            verdicts=[],
        )
        assert direct_outcome.turns[0].worker_presence_pass is True

        delegated_outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?", expect_delegated=True),),
            citations=["citation-1"],
            routing_action="new_worker",
            verdicts=[],
        )
        assert delegated_outcome.turns[0].worker_presence_pass is True

    def test_worker_presence_failure_fails_the_matrix_via_compute_pass_fail(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="Hi.", expect_action="direct", expect_delegated=False),),
            citations=["citation-1"],
            routing_action="new_worker",
            verdicts=[],
        )
        overall_status, reasons = eval_runner.compute_pass_fail([outcome])
        assert overall_status == "FAIL"
        assert any("worker presence/absence assertion failed" in reason for reason in reasons)


class TestMultiIntentRoutingSignalGap:
    """Regression for round-7 gauntlet, Logic lens finding 6: multi_intent and
    continue_pending turns return via server/pipeline.py's `_handle_multi_intent`/
    `_handle_pending` and never reach the `isinstance(outcome.decision,
    RoutingDecision)` branch that sets `host.state.routing` -- so `routing_action`
    reads as None for these turns even though real delegation happened. Three
    distinct bugs stemmed from that gap: (a) a multi-item result tuple was
    misclassified as a provider error, (b) worker_presence_pass fell back to
    routing_action alone and always failed a delegated multi-intent turn, and
    (c) deterministic_action_pass was scored False (not left unevaluated) when
    routing_action was None and turn.expect_action was set.
    """

    def test_multi_item_result_tuple_is_not_misclassified_as_provider_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)

        def _multi_intent_result_factory(_query: str) -> Any:
            return (
                _FakeResult(
                    ui_text="reply one",
                    spoken_text="reply one",
                    citations=["citation-1"],
                    turn_id="turn-1-0",
                    worker_id="worker-a",
                ),
                _FakeResult(
                    ui_text="reply two",
                    spoken_text="reply two",
                    citations=["citation-2"],
                    turn_id="turn-1-1",
                    worker_id="worker-b",
                ),
            )

        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga and Helsinki?", expect_delegated=True),),
            verdicts=["yes"],
            result_factory=_multi_intent_result_factory,
        )

        assert outcome.turns[0].status == "ok"

    def test_empty_result_tuple_is_still_a_provider_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?", expect_delegated=True),),
            result_factory=lambda _query: (),
        )
        assert outcome.turns[0].status == "provider-error"

    def test_worker_presence_pass_reflects_delegation_via_worker_id_when_routing_action_is_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)

        def _multi_intent_result_factory(_query: str) -> Any:
            return (
                _FakeResult(
                    ui_text="reply one",
                    spoken_text="reply one",
                    citations=["citation-1"],
                    turn_id="turn-1-0",
                    worker_id="worker-a",
                ),
            )

        # routing_action left at its default (None) -- host.state.routing is
        # never set on the multi_intent/continue_pending return paths.
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?", expect_delegated=True),),
            verdicts=["yes"],
            result_factory=_multi_intent_result_factory,
        )

        assert outcome.turns[0].worker_presence_pass is True

    def test_deterministic_action_pass_is_none_not_false_when_routing_action_is_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="Hi.", expect_action="direct", expect_delegated=False),),
            verdicts=[],
            # routing_action left at its default (None).
        )
        assert outcome.turns[0].deterministic_action_pass is None

    def test_unevaluated_reason_is_recorded_and_surfaced_distinctly_from_not_applicable(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Regression for round 8 gauntlet, Logic lens finding 2:
        ``deterministic_action_pass=None`` alone can't distinguish "no
        assertion was requested" from "the assertion was requested but never
        ran" -- the latter must be visible in the report/summary.
        """
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="Hi.", expect_action="direct", expect_delegated=False),),
            verdicts=[],
            # routing_action left at its default (None).
        )
        turn = outcome.turns[0]
        assert turn.deterministic_action_pass is None
        assert turn.deterministic_action_unevaluated_reason is not None

        report = eval_runner.build_report([outcome], judge_model="gpt-5-mini")
        report_turn = report["cells"][0]["turns"][0]
        assert report_turn["deterministic_action_pass"] is None
        assert report_turn["deterministic_action_unevaluated_reason"] is not None

        eval_runner.print_report_summary(report)
        captured = capsys.readouterr().out
        assert "UNEVALUATED" in captured

    def test_no_unevaluated_reason_when_no_action_assertion_was_requested(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="Hi."),),  # no expect_action at all
            verdicts=[],
        )
        turn = outcome.turns[0]
        assert turn.deterministic_action_pass is None
        assert turn.deterministic_action_unevaluated_reason is None


class TestUnevaluatedDeterministicActionFailsTheRun:
    """Regression for round 9 gauntlet, merged Codex P1 + Logic lens
    finding 3: ``deterministic_action_unevaluated_reason`` (round 8) made a
    requested-but-never-run assertion visible in the per-turn report, but
    ``compute_pass_fail()`` only rejected ``deterministic_action_pass is
    False`` -- a ``None`` value (whether "not applicable" or "requested but
    unevaluated") still aggregated to PASS, the exact class of gap round 8
    fixed for the latency-budget gate.
    """

    def _cell_with_unevaluated_action(self) -> Any:
        turn = eval_runner.TurnOutcome(
            query="Hi.",
            status="ok",
            deterministic_action_pass=None,
            deterministic_action_unevaluated_reason=(
                "routing_action was unavailable for this turn"
            ),
        )
        return eval_runner.CellOutcome(
            pair_label="router=baseline/worker=baseline",
            scenario_name="s",
            status="ok",
            turns=[turn],
        )

    def test_a_requested_but_unevaluated_action_assertion_fails_the_run(self) -> None:
        overall_status, reasons = eval_runner.compute_pass_fail(
            [self._cell_with_unevaluated_action()]
        )
        assert overall_status == "FAIL"
        assert any("unevaluated" in reason for reason in reasons)

    def test_reason_is_classified_infra_not_semantic(self) -> None:
        _overall_status, reasons = eval_runner.compute_pass_fail(
            [self._cell_with_unevaluated_action()]
        )
        assert any(reason.startswith("infra:") and "unevaluated" in reason for reason in reasons)

    def test_a_not_applicable_none_action_still_passes(self) -> None:
        # deterministic_action_pass=None with NO unevaluated_reason means "no
        # assertion was requested" -- must still pass, not regress into a
        # false failure for every turn without an expect_action.
        turn = eval_runner.TurnOutcome(
            query="Hi.",
            status="ok",
            deterministic_action_pass=None,
            deterministic_action_unevaluated_reason=None,
        )
        cell = eval_runner.CellOutcome(pair_label="p", scenario_name="s", status="ok", turns=[turn])
        overall_status, _reasons = eval_runner.compute_pass_fail([cell])
        assert overall_status == "PASS"


class TestMissingTurnMetricsIsNotSilentlySwallowed:
    """Regression for round 8 gauntlet, merged Codex P1 + Logic lens finding 4:
    round 7 added ``except RuntimeError: stage_metrics = None`` around
    ``latest_turn_stage_metrics()`` to handle a multi-intent item's
    suffixed ``turn_id`` (``f"{turn_id}-{index}"``, which never matches the
    parent ``app_turn_foreground`` record). But that same catch also
    swallowed a genuinely-missing metrics record on an ordinary turn --
    ``latency_budget_enforced``/``latency_budget_exceeded`` silently stayed
    at their False/None defaults, so a baseline cell could report PASS with
    no latency evidence at all. Fixed by positively identifying the
    multi-intent item-suffix shape (``_is_multi_intent_item_turn_id``)
    before swallowing; any other RuntimeError now re-raises and surfaces as
    ``turn-error``.
    """

    def test_genuinely_missing_metrics_on_an_ordinary_turn_is_a_turn_error_not_a_silent_pass(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _fake_build_session_for_run(_config: Any, *, measurement_sink: Any = None) -> Any:
            del measurement_sink
            return _FakeHost(
                lambda _q: _FakeResult(
                    ui_text="a distinctly non-fallback reply",
                    spoken_text="a distinctly non-fallback spoken reply",
                    citations=[],
                    turn_id="turn-1",
                )
            )

        def _stage_metrics_never_recorded(
            _sink: Any, _elapsed_ms: float, turn_id: str
        ) -> dict[str, float]:
            raise RuntimeError(f"no app_turn_foreground metric was emitted for turn_id={turn_id!r}")

        monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
        monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)
        monkeypatch.setattr("pipecat.evals.judge.EvalJudge", _make_stub_judge_class([], None))
        monkeypatch.setattr(eval_runner, "latest_turn_stage_metrics", _stage_metrics_never_recorded)

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        scenario = Scenario(name="s", turns=(Turn(query="hi"),))
        outcome = asyncio.run(
            eval_runner.run_cell(
                pair,
                scenario,
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

        # Must NOT be a silent "ok" turn with latency_budget_enforced left
        # False -- that would let compute_pass_fail() report PASS with no
        # latency evidence at all. THIS turn is classified turn-error; the
        # cell itself stays "ok" (round 9 gauntlet, Logic lens finding 7 --
        # see TestMissingMetricsDoesNotAbortTheCell below for why the cell
        # must not abort over a single ordinary turn's metrics gap).
        assert outcome.turns[0].status == "turn-error"
        assert outcome.turns[0].error is not None
        assert "app_turn_foreground" in outcome.turns[0].error
        # The cell itself completes ("ok") rather than aborting -- a single
        # ordinary turn's metrics gap must not discard the rest of a paid
        # scenario's turns (round 9 gauntlet, Logic lens finding 7).
        assert outcome.status == "ok"

    def test_multi_intent_item_suffixed_turn_id_is_still_a_recognized_gap_not_a_turn_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from server.perf_metrics import MeasurementRecord

        def _fake_build_session_for_run(_config: Any, *, measurement_sink: Any) -> Any:
            # Simulate the parent app_turn_foreground record production
            # actually emits for the (unsuffixed) turn -- the shape
            # _is_multi_intent_item_turn_id positively checks for. Appended
            # directly to the sink's internal list (not via emit()/
            # build_record(), which validate the full production field set
            # this synthetic record doesn't need) since only .event/.fields
            # matter to the helper under test.
            measurement_sink._records.append(
                MeasurementRecord(
                    event="app_turn_foreground", fields={"turn_id": "turn-1"}, line=""
                )
            )
            return _FakeHost(
                lambda _q: (
                    _FakeResult(
                        ui_text="reply one",
                        spoken_text="reply one",
                        citations=["citation-1"],
                        turn_id="turn-1-0",
                        worker_id="worker-a",
                    ),
                )
            )

        def _stage_metrics_fails_for_item_suffix(
            _sink: Any, _elapsed_ms: float, turn_id: str
        ) -> dict[str, float]:
            raise RuntimeError(f"no app_turn_foreground metric was emitted for turn_id={turn_id!r}")

        monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
        monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)
        monkeypatch.setattr("pipecat.evals.judge.EvalJudge", _make_stub_judge_class(["yes"], None))
        monkeypatch.setattr(
            eval_runner, "latest_turn_stage_metrics", _stage_metrics_fails_for_item_suffix
        )

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        scenario = Scenario(
            name="s", turns=(Turn(query="weather in Riga and Helsinki?", expect_delegated=True),)
        )
        outcome = asyncio.run(
            eval_runner.run_cell(
                pair,
                scenario,
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

        assert outcome.status == "ok"
        assert outcome.turns[0].status == "ok"
        assert outcome.turns[0].routing_ms is None
        assert outcome.turns[0].latency_budget_enforced is False


class TestMissingMetricsDoesNotAbortTheCell:
    """Regression for round 9 gauntlet, Logic lens finding 7: round 8's
    ``raise`` (on a genuinely-missing, non-multi-intent metrics gap) escaped
    the whole ``for turn in scenario.turns`` loop -- discarding the
    already-computed ``outcome`` fields for that turn (worker_presence_pass/
    citations_pass/routing data, overwritten by a fresh ``TurnOutcome`` in
    the outer handler) and aborting the entire cell, marking every remaining
    turn "skipped" even though this is an ordinary single-turn metrics gap,
    not a real infrastructure failure. The fix marks THIS turn turn-error
    and continues to the next turn.
    """

    def test_a_turn_after_the_missing_metrics_turn_still_runs(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[str] = []

        def _fake_build_session_for_run(_config: Any, *, measurement_sink: Any = None) -> Any:
            del measurement_sink

            def _result_factory(query: str) -> Any:
                calls.append(query)
                return _FakeResult(
                    ui_text="a distinctly non-fallback reply",
                    spoken_text="a distinctly non-fallback spoken reply",
                    citations=[],
                    turn_id="turn-1",
                )

            return _FakeHost(_result_factory)

        def _stage_metrics_fails_once(
            _sink: Any, _elapsed_ms: float, turn_id: str
        ) -> dict[str, float]:
            if len(calls) == 1:
                raise RuntimeError(
                    f"no app_turn_foreground metric was emitted for turn_id={turn_id!r}"
                )
            return {"routing_ms": 10.0, "search_ms": 0.0, "total_ms": 10.0}

        monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
        monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)
        monkeypatch.setattr("pipecat.evals.judge.EvalJudge", _make_stub_judge_class([], None))
        monkeypatch.setattr(eval_runner, "latest_turn_stage_metrics", _stage_metrics_fails_once)

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        scenario = Scenario(name="s", turns=(Turn(query="first"), Turn(query="second")))
        outcome = asyncio.run(
            eval_runner.run_cell(
                pair,
                scenario,
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

        # Both turns actually ran (were billed) -- the second was not
        # skipped, and the cell completed rather than aborting.
        assert calls == ["first", "second"]
        assert outcome.status == "ok"
        assert len(outcome.turns) == 2
        assert outcome.turns[0].status == "turn-error"
        assert outcome.turns[1].status == "ok"
        # The second turn's own outcome data was not discarded/overwritten.
        assert outcome.turns[1].routing_ms == 10.0


class TestIsMultiIntentItemTurnId:
    """Unit coverage for the positive-identification helper itself."""

    def test_true_for_a_suffixed_id_with_a_matching_parent_record(self) -> None:
        from server.perf_metrics import CollectingMeasurementSink, MeasurementRecord

        sink = CollectingMeasurementSink()
        sink._records.append(
            MeasurementRecord(event="app_turn_foreground", fields={"turn_id": "turn-1"}, line="")
        )
        assert eval_runner._is_multi_intent_item_turn_id(sink, "turn-1-0") is True

    def test_false_when_no_matching_parent_record_exists(self) -> None:
        from server.perf_metrics import CollectingMeasurementSink

        sink = CollectingMeasurementSink()
        assert eval_runner._is_multi_intent_item_turn_id(sink, "turn-1-0") is False

    def test_false_for_a_non_digit_suffix(self) -> None:
        from server.perf_metrics import CollectingMeasurementSink, MeasurementRecord

        sink = CollectingMeasurementSink()
        sink._records.append(
            MeasurementRecord(event="app_turn_foreground", fields={"turn_id": "turn-abc"}, line="")
        )
        assert eval_runner._is_multi_intent_item_turn_id(sink, "turn-abc-x") is False

    def test_false_when_there_is_no_hyphen_at_all(self) -> None:
        from server.perf_metrics import CollectingMeasurementSink

        sink = CollectingMeasurementSink()
        assert eval_runner._is_multi_intent_item_turn_id(sink, "turn1") is False


# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------


class TestScenarioDefinitions:
    """2 in-scope scenarios, each an ordered list of turns with per-turn
    judge criteria where applicable; routing-regression reuses
    ROUTING_REGRESSION_QUERIES verbatim (3 turns, not 2) with a deterministic
    assertion on turn 1 and time-robust judge criteria on turns 2/3; the
    ack-ordering scenario is explicitly excluded from this matrix.
    """

    def test_two_scenarios_are_defined(self) -> None:
        assert len(eval_scenarios.SCENARIOS) == 2

    def test_evals_package_does_not_import_scripts(self) -> None:
        """Regression for round-2 gauntlet finding 8: evals/scenarios.py used
        to import its query constants from scripts/eval_common.py, making
        scripts <-> evals bidirectionally coupled at the package level (since
        scripts/eval_model_comparison.py already imports evals.scenarios).
        The constants now live in evals/queries.py, so evals/ has no
        dependency on scripts/ at all.
        """
        import ast
        from pathlib import Path

        evals_dir = Path(eval_scenarios.__file__).parent
        for py_file in evals_dir.glob("*.py"):
            tree = ast.parse(py_file.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module:
                    assert not node.module.startswith("scripts"), (
                        f"{py_file.name} imports from {node.module!r} -- "
                        "evals/ must not depend on scripts/"
                    )
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        assert not alias.name.startswith("scripts"), (
                            f"{py_file.name} imports {alias.name!r} -- "
                            "evals/ must not depend on scripts/"
                        )

    def test_ack_ordering_scenario_is_excluded(self) -> None:
        names = [s.name for s in eval_scenarios.SCENARIOS]
        assert not any("ack" in name.lower() for name in names)

    def test_routing_regression_scenario_has_three_turns_not_two(self) -> None:
        from scripts.smoke_conversation import ROUTING_REGRESSION_QUERIES

        assert len(ROUTING_REGRESSION_QUERIES) == 3
        routing_regression = next(
            s for s in eval_scenarios.SCENARIOS if "routing" in s.name.lower()
        )
        assert len(routing_regression.turns) == 3
        assert [t.query for t in routing_regression.turns] == list(ROUTING_REGRESSION_QUERIES)

    def test_routing_regression_first_turn_is_deterministic_not_judge_scored(self) -> None:
        routing_regression = next(
            s for s in eval_scenarios.SCENARIOS if "routing" in s.name.lower()
        )
        first_turn = routing_regression.turns[0]
        # Turn 1 ("Hi.") is a deterministic action==direct/no-worker-created
        # assertion, not a judge criterion.
        assert first_turn.judge_criterion is None

    def test_routing_regression_weather_turns_have_judge_criteria(self) -> None:
        routing_regression = next(
            s for s in eval_scenarios.SCENARIOS if "routing" in s.name.lower()
        )
        for turn in routing_regression.turns[1:]:
            assert turn.judge_criterion
            assert isinstance(turn.judge_criterion, str)
            # Judge criteria must be time-robust ("names a specific weather
            # condition or temperature"), never asserting the *correct*
            # current weather -- ground truth would drift and isn't checkable.
            assert "correct current weather" not in turn.judge_criterion.lower()

    def test_all_scenario_criteria_are_written_verbatim_not_placeholders(self) -> None:
        placeholder_markers = {"todo", "tbd", "placeholder", "fixme", "xxx"}
        for scenario in eval_scenarios.SCENARIOS:
            for turn in scenario.turns:
                if turn.judge_criterion:
                    lowered = turn.judge_criterion.lower()
                    assert not any(marker in lowered for marker in placeholder_markers)


# ---------------------------------------------------------------------------
# Round-1 review-gauntlet regression tests.
# ---------------------------------------------------------------------------


class _RaisingOnFirstTurnHost:
    """A ``SessionHost`` stand-in whose first turn times out (or errors) so a
    cell that never gets a real result must not still report ``status="ok"``.
    """

    def __init__(self, *, failure: BaseException) -> None:
        self.state = _FakeState()
        self.registry = _FakeRegistry()
        self._failure = failure
        self.calls = 0

    async def start(self) -> None:
        pass

    async def connect(self, _handshake: dict[str, Any]) -> str:
        return "connection-1"

    async def _handle_transcript(self, query: str, *, origin: Any) -> Any:
        del origin, query
        self.calls += 1
        raise self._failure

    async def shutdown(self) -> None:
        pass


class TestTurnFailurePropagatesToCellAndReport:
    """Regression for finding 1: a turn that times out or raises must not
    leave the cell reporting ``status="ok"`` -- and ``compute_pass_fail()``
    must itself notice a non-"ok" turn status, not just a non-"ok" cell
    status, since a fully-failed run's every symptom lives on its turns.
    """

    def _run(self, monkeypatch: pytest.MonkeyPatch, *, failure: BaseException) -> Any:
        host = _RaisingOnFirstTurnHost(failure=failure)

        def _fake_build_session_for_run(_config: Any, *, measurement_sink: Any = None) -> Any:
            del measurement_sink
            return host

        monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
        monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)
        monkeypatch.setattr("pipecat.evals.judge.EvalJudge", _make_stub_judge_class([], None))

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        scenario = Scenario(
            name="two-turn-fixture",
            turns=(Turn(query="turn one"), Turn(query="turn two")),
        )
        config = eval_runner.Config()
        return asyncio.run(
            eval_runner.run_cell(
                pair,
                scenario,
                config,
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

    def test_a_timed_out_turn_marks_the_cell_status_non_ok(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        outcome = self._run(monkeypatch, failure=TimeoutError())
        assert outcome.status == "timeout"
        assert outcome.turns[0].status == "timeout"
        # Regression for round 9 gauntlet, Logic lens finding 8: every
        # sibling infra-failure break path sets outcome.error; the
        # wait_for-TimeoutError path previously only set cell_error, leaving
        # this turn's own `error` detail blank.
        assert outcome.turns[0].error is not None
        assert "wait_for budget" in outcome.turns[0].error

    def test_a_provider_error_turn_marks_the_cell_status_non_ok(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        outcome = self._run(monkeypatch, failure=RuntimeError("boom"))
        assert outcome.status == "provider-error"
        assert outcome.turns[0].status == "provider-error"

    def test_the_unattempted_second_turn_is_recorded_as_skipped_not_absent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        outcome = self._run(monkeypatch, failure=TimeoutError())
        assert len(outcome.turns) == 2
        assert outcome.turns[1].query == "turn two"
        assert outcome.turns[1].status == "skipped"

    def test_compute_pass_fail_fails_the_run_and_names_the_turn_status(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        outcome = self._run(monkeypatch, failure=RuntimeError("boom"))
        report = eval_runner.build_report([outcome], judge_model="gpt-5-mini")

        assert report["overall_status"] == "FAIL"
        assert any(
            "turn one" in reason and "status='provider-error'" in reason
            for reason in report["failure_reasons"]
        )

    def test_a_fully_failed_billed_run_never_reports_pass(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The exact scenario finding 1 named: every turn errors, and the
        report must not read PASS / exit 0 as a result."""
        outcome = self._run(monkeypatch, failure=RuntimeError("every turn fails"))
        report = eval_runner.build_report([outcome], judge_model="gpt-5-mini")
        assert report["overall_status"] == "FAIL"


class TestMidTurnFailureIsNotMisclassifiedAsSetupError:
    """Regression for round-2 gauntlet finding 12: an exception raised mid-loop
    AFTER at least one turn already completed and was billed previously
    landed in the outer handler that unconditionally set
    ``cell_status = "setup-error"`` -- misreporting a mid-turn failure as if
    nothing had run yet. Fixed by checking whether ``turns`` already has
    entries: "turn-error" if so, "setup-error" only if the exception happened
    before any turn completed.

    The failure is injected via ``latest_turn_stage_metrics()`` raising, but
    NOT via ``RuntimeError`` -- round 7 gauntlet, Logic lens finding 6 made a
    ``RuntimeError`` from that call site an expected, non-fatal "per-item
    latency genuinely unavailable" signal (a multi-intent commit's item-
    suffixed turn_id never matches the parent metric record), so these tests
    use ``ValueError`` instead to keep exercising a genuine uncaught
    mid-execution failure.
    """

    def test_failure_after_a_completed_turn_is_turn_error_not_setup_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _fake_build_session_for_run(_config: Any, *, measurement_sink: Any = None) -> Any:
            del measurement_sink
            return _FakeHost(
                lambda _q: _FakeResult(
                    ui_text="a distinctly non-fallback reply",
                    spoken_text="a distinctly non-fallback spoken reply",
                    citations=[],
                    turn_id="turn-1",
                )
            )

        calls = {"n": 0}

        def _stage_metrics_fails_on_second_turn(
            _sink: Any, _elapsed_ms: float, _turn_id: str
        ) -> dict[str, float]:
            calls["n"] += 1
            if calls["n"] == 1:
                return {"routing_ms": 10.0, "search_ms": 0.0, "total_ms": 10.0}
            raise ValueError("stage metrics lookup broke in a way that must not be swallowed")

        monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
        monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)
        monkeypatch.setattr("pipecat.evals.judge.EvalJudge", _make_stub_judge_class([], None))
        monkeypatch.setattr(
            eval_runner, "latest_turn_stage_metrics", _stage_metrics_fails_on_second_turn
        )

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        scenario = Scenario(name="s", turns=(Turn(query="turn one"), Turn(query="turn two")))
        outcome = asyncio.run(
            eval_runner.run_cell(
                pair,
                scenario,
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

        assert outcome.status == "turn-error"
        # The first turn actually ran and was billed -- it must be recorded
        # as "ok", not swallowed into the failure.
        assert outcome.turns[0].status == "ok"
        assert outcome.turns[0].query == "turn one"

    def test_failure_before_any_turn_completes_is_still_setup_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            eval_runner,
            "build_session_for_run",
            lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("no host for you")),
        )

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = asyncio.run(
            eval_runner.run_cell(
                pair,
                Scenario(name="s", turns=(Turn(query="hi"),)),
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

        assert outcome.status == "setup-error"

    def test_failure_mid_execution_of_the_first_turn_is_turn_error_not_setup_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression for round-3 gauntlet finding 1: ``cell_status = "turn-error"
        if turns else "setup-error"`` previously inferred "did any turn
        complete" from ``len(turns)`` -- but a ``TurnOutcome`` is only
        appended at the END of a turn's iteration, so a turn that fails mid-
        execution (paid call succeeded, something after it raised) on the
        FIRST turn left ``turns == []`` even though that turn actually ran
        and was billed. It was misclassified "setup-error" (implying nothing
        was attempted) and the skipped-turn backfill mislabeled the
        already-billed, already-run turn as "skipped". Fixed by a
        ``turn_started`` flag set immediately before the paid call, used to
        append an in-flight ``TurnOutcome(status="turn-error")`` for the turn
        that was mid-execution before the backfill runs.

        Uses ``ValueError`` (not ``RuntimeError``) to inject the mid-execution
        failure -- see class docstring: round 7 gauntlet, Logic lens finding 6
        made a ``RuntimeError`` from ``latest_turn_stage_metrics()`` an
        expected, non-fatal signal.
        """

        def _fake_build_session_for_run(_config: Any, *, measurement_sink: Any = None) -> Any:
            del measurement_sink
            return _FakeHost(
                lambda _q: _FakeResult(
                    ui_text="a distinctly non-fallback reply",
                    spoken_text="a distinctly non-fallback spoken reply",
                    citations=[],
                    turn_id="turn-1",
                )
            )

        def _stage_metrics_always_fails(
            _sink: Any, _elapsed_ms: float, _turn_id: str
        ) -> dict[str, float]:
            raise ValueError("stage metrics lookup broke in a way that must not be swallowed")

        monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
        monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)
        monkeypatch.setattr("pipecat.evals.judge.EvalJudge", _make_stub_judge_class([], None))
        monkeypatch.setattr(eval_runner, "latest_turn_stage_metrics", _stage_metrics_always_fails)

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        scenario = Scenario(name="s", turns=(Turn(query="turn one"), Turn(query="turn two")))
        outcome = asyncio.run(
            eval_runner.run_cell(
                pair,
                scenario,
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

        assert outcome.status == "turn-error"
        # The first turn actually ran (and was billed) -- it must be recorded
        # as "turn-error", not as "setup-error" and not silently dropped into
        # the "skipped" backfill.
        assert len(outcome.turns) == 2
        assert outcome.turns[0].query == "turn one"
        assert outcome.turns[0].status == "turn-error"
        assert outcome.turns[1].query == "turn two"
        assert outcome.turns[1].status == "skipped"


class TestSkippedTurnBackfillDedupesByIndexNotQueryText:
    """Regression for round-2 gauntlet finding 11: the skipped-turn backfill
    previously deduped on query TEXT (``{t.query for t in turns}``), so a
    scenario with a repeated query string under-reported which turns were
    skipped -- a trailing duplicate query was silently absent from the
    backfill instead of being marked "skipped". Fixed by backfilling by
    INDEX (``scenario.turns[len(turns):]``) since ``turns`` is appended
    strictly in scenario order.
    """

    def test_a_repeated_query_after_the_failure_is_still_backfilled_as_skipped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class _FailsOnSecondCallHost:
            state = _FakeState()
            registry = _FakeRegistry()

            def __init__(self) -> None:
                self.calls = 0

            async def start(self) -> None:
                pass

            async def connect(self, _handshake: dict[str, Any]) -> str:
                return "connection-1"

            async def _handle_transcript(self, query: str, *, origin: Any) -> Any:
                del origin, query
                self.calls += 1
                if self.calls == 1:
                    return _FakeResult(
                        ui_text="a distinctly non-fallback reply",
                        spoken_text="a distinctly non-fallback spoken reply",
                        citations=[],
                        turn_id="turn-1",
                    )
                raise RuntimeError("second turn fails")

            async def shutdown(self) -> None:
                pass

        def _fake_build_session_for_run(_config: Any, *, measurement_sink: Any = None) -> Any:
            del measurement_sink
            return _FailsOnSecondCallHost()

        monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
        monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)
        monkeypatch.setattr("pipecat.evals.judge.EvalJudge", _make_stub_judge_class([], None))
        monkeypatch.setattr(
            eval_runner,
            "latest_turn_stage_metrics",
            lambda *_a, **_k: {"routing_ms": 10.0, "search_ms": 0.0, "total_ms": 10.0},
        )

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        # Turns 1 and 3 share the same query text -- a text-based dedup would
        # wrongly treat turn 3 as "already attempted" (because turn 1's query
        # matches it) and silently drop it from the backfill.
        scenario = Scenario(
            name="repeated-query-fixture",
            turns=(
                Turn(query="same query"),
                Turn(query="a different query"),
                Turn(query="same query"),
            ),
        )
        outcome = asyncio.run(
            eval_runner.run_cell(
                pair,
                scenario,
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

        assert len(outcome.turns) == 3
        assert outcome.turns[0].status == "ok"
        assert outcome.turns[1].status == "provider-error"
        assert outcome.turns[2].query == "same query"
        assert outcome.turns[2].status == "skipped"


class TestHostLifecycleExceptionsDontCrashTheMatrix:
    """Regression for finding 2: an exception from host.start()/connect(), or
    from judge.evaluate(), must be classified into a CellOutcome rather than
    escaping run_cell() uncaught (which would abort run_matrix() and discard
    every already-billed cell with no report ever written).
    """

    def test_host_start_failure_still_shuts_down_and_returns_a_cell_outcome(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        shutdown_calls: list[bool] = []

        class _FailingStartHost:
            state = _FakeState()
            registry = _FakeRegistry()

            async def start(self) -> None:
                raise RuntimeError("startup exploded")

            async def connect(self, _h: Any) -> str:
                raise AssertionError("connect() must not be reached if start() failed")

            async def _handle_transcript(self, *_a: Any, **_k: Any) -> Any:
                raise AssertionError("a turn must not run if start() failed")

            async def shutdown(self) -> None:
                shutdown_calls.append(True)

        def _fake_build_session_for_run(_config: Any, *, measurement_sink: Any = None) -> Any:
            del measurement_sink
            return _FailingStartHost()

        monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
        monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)
        monkeypatch.setattr("pipecat.evals.judge.EvalJudge", _make_stub_judge_class([], None))

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        scenario = Scenario(name="s", turns=(Turn(query="hi"),))
        outcome = asyncio.run(
            eval_runner.run_cell(
                pair,
                scenario,
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

        assert outcome.status == "setup-error"
        assert "startup exploded" in (outcome.error or "")
        # The whole point of finding 2: shutdown() still runs even though
        # start() never succeeded, so cached provider clients are released.
        assert shutdown_calls == [True]

    def test_judge_evaluate_raising_is_classified_as_judge_error_not_a_crash(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class _RaisingJudge:
            def __init__(self, *_a: Any, **_k: Any) -> None:
                pass

            def add_user_message(self, _text: str) -> None:
                pass

            def add_assistant_message(self, _text: str) -> None:
                pass

            async def evaluate(self, _criterion: str) -> Any:
                raise RuntimeError("judge backend unreachable")

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather in Riga?", judge_criterion="names a temperature"),),
        )
        del outcome  # the _run_cell helper already queues a stub judge class

        # Re-run with a judge whose evaluate() itself raises, proving
        # run_cell() classifies that as judge-error for the turn instead of
        # letting the exception propagate out of run_cell()/run_matrix().
        def _fake_build_session_for_run(_config: Any, *, measurement_sink: Any = None) -> Any:
            del measurement_sink

            def _result_factory(_q: str) -> Any:
                return _FakeResult(
                    ui_text="a distinctly non-fallback reply",
                    spoken_text="a distinctly non-fallback spoken reply",
                    citations=[],
                    turn_id="turn-1",
                )

            return _FakeHost(_result_factory)

        monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
        monkeypatch.setattr(
            eval_runner,
            "latest_turn_stage_metrics",
            lambda *_a, **_k: {"routing_ms": 10.0, "search_ms": 0.0, "total_ms": 10.0},
        )
        monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)
        monkeypatch.setattr("pipecat.evals.judge.EvalJudge", _RaisingJudge)

        result = asyncio.run(
            eval_runner.run_cell(
                pair,
                Scenario(name="s", turns=(Turn(query="weather in Riga?", judge_criterion="x"),)),
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

        # The cell completed (run_cell did not raise) and the turn is
        # classified as judge-error, not silently absent from the report.
        assert result.status == "ok"
        assert result.turns[0].judge_verdict == "judge-error"
        assert "judge backend unreachable" in (result.turns[0].judge_reason or "")


class TestJudgeEvaluateIsBoundedByATimeout:
    """Regression for round 9 gauntlet, Codex P2 finding 1: a stalled/
    unreachable judge endpoint previously had no bound at all and could hold
    the whole matrix for minutes.
    """

    def test_a_hanging_judge_call_is_classified_as_judge_error_not_hung_forever(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class _HangingJudge:
            def __init__(self, *_a: Any, **_k: Any) -> None:
                pass

            def add_user_message(self, _text: str) -> None:
                pass

            def add_assistant_message(self, _text: str) -> None:
                pass

            async def evaluate(self, _criterion: str) -> Any:
                await asyncio.sleep(10)
                raise AssertionError("should have timed out before completing")

        def _fake_build_session_for_run(_config: Any, *, measurement_sink: Any = None) -> Any:
            del measurement_sink
            return _FakeHost(
                lambda _q: _FakeResult(
                    ui_text="a distinctly non-fallback reply",
                    spoken_text="a distinctly non-fallback spoken reply",
                    citations=[],
                    turn_id="turn-1",
                )
            )

        monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
        monkeypatch.setattr(
            eval_runner,
            "latest_turn_stage_metrics",
            lambda *_a, **_k: {"routing_ms": 10.0, "search_ms": 0.0, "total_ms": 10.0},
        )
        monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)
        monkeypatch.setattr("pipecat.evals.judge.EvalJudge", _HangingJudge)
        # Keep the test fast: the timeout mechanism, not the specific bound,
        # is under test here.
        monkeypatch.setattr(eval_runner, "_JUDGE_EVALUATE_TIMEOUT_SECONDS", 0.05)

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        result = asyncio.run(
            eval_runner.run_cell(
                pair,
                Scenario(name="s", turns=(Turn(query="weather in Riga?", judge_criterion="x"),)),
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

        assert result.status == "ok"
        assert result.turns[0].judge_verdict == "judge-error"
        assert "timeout" in (result.turns[0].judge_reason or "").lower()


class TestManifestStalenessFailsClosed:
    """Regression for finding 5: an unverifiable identity -- a manifest
    missing source_commit, or a current-commit lookup that itself failed --
    must be treated as stale (fail closed), not silently accepted as fresh.
    """

    def test_missing_source_commit_is_treated_as_stale(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(eval_runner, "git_head", lambda: "abc123")
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "manifest_version": 1,
                    "source_commit": None,
                    "results": [],
                }
            )
        )

        status = eval_runner.load_manifest_status(manifest_path)

        assert status.stale is True

    def test_unresolvable_current_commit_is_treated_as_stale(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(eval_runner, "git_head", lambda: None)
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[])

        status = eval_runner.load_manifest_status(manifest_path)

        assert status.stale is True

    def test_matching_verifiable_identity_is_not_stale(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(eval_runner, "git_head", lambda: "deadbeef")
        monkeypatch.setattr(eval_runner, "_source_tree_dirty", lambda: False)
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[])

        status = eval_runner.load_manifest_status(manifest_path)

        assert status.stale is False

    def test_dirty_source_tree_is_treated_as_stale_despite_matching_commit(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression: a matching HEAD alone must not clear staleness if the
        manifest-attested source files (server/router.py,
        server/workers/web_search.py, server/config.py) carry uncommitted
        edits -- ``git rev-parse HEAD`` would still match the manifest even
        though the tree it describes is no longer the tree in front of the
        runner.
        """
        monkeypatch.setattr(eval_runner, "git_head", lambda: "deadbeef")
        monkeypatch.setattr(eval_runner, "_source_tree_dirty", lambda: True)
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[])

        status = eval_runner.load_manifest_status(manifest_path)

        assert status.stale is True

    def test_unverifiable_dirty_check_is_treated_as_stale(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Fail-closed: a broken git invocation for the dirty-tree check
        (``_source_tree_dirty()`` returning ``None``) must not be read as
        "clean", mirroring ``git_head()`` returning ``None``.
        """
        monkeypatch.setattr(eval_runner, "git_head", lambda: "deadbeef")
        monkeypatch.setattr(eval_runner, "_source_tree_dirty", lambda: None)
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[])

        status = eval_runner.load_manifest_status(manifest_path)

        assert status.stale is True


class TestSourceTreeDirtyCheckIsWholeTreeAttested:
    """Regression for round 9 gauntlet, Architecture lens finding 15: round
    7, 8, and 9 of the review gauntlet each independently found one more
    file a hand-enumerated ``_MANIFEST_ATTESTED_PATHS`` tuple was missing.
    Replaced with whole-tree attestation over ``server/``,
    ``scripts/eval_common.py``, and ``evals/`` -- a dirty file under any of
    those, not just a previously-enumerated subset, must now be detected.
    """

    def test_attested_paths_cover_the_whole_server_tree_not_an_enumerated_subset(self) -> None:
        assert eval_runner._MANIFEST_ATTESTED_PATHS == (
            "server/",
            "scripts/eval_common.py",
            "evals/",
        )

    def test_git_status_is_invoked_with_the_broadened_attested_paths(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, list[str]] = {}
        real_run = eval_runner.subprocess.run

        def _capturing_run(cmd, **kwargs):
            captured["cmd"] = cmd
            return real_run(["git", "status", "--porcelain"], **kwargs)

        monkeypatch.setattr(eval_runner.subprocess, "run", _capturing_run)

        eval_runner._source_tree_dirty()

        for attested_path in eval_runner._MANIFEST_ATTESTED_PATHS:
            assert attested_path in captured["cmd"]
        # Previously-enumerated single files (e.g. server/registry.py) are no
        # longer named individually -- the whole "server/" directory covers
        # them instead, which is exactly the point: a file this list never
        # enumerated (e.g. server/app.py) is covered too.
        assert "server/registry.py" not in captured["cmd"]


class TestManifestWorkerEntryRequiresWebSearchTool:
    """Regression for finding 10: a worker candidate's manifest entry must
    declare the web_search tool it was actually probed with -- a
    hand-edited/future-schema manifest that marks a worker tuple accepted
    without it must not be treated as covering a live worker run.
    """

    def test_worker_entry_missing_web_search_tool_is_not_accepted(self, tmp_path: Path) -> None:
        entry = _accepted_worker_entry(
            eval_runner.WORKER_CANDIDATES[0].model, eval_runner.WORKER_CANDIDATES[0].effort
        )
        entry["tools"] = []  # hand-edited: declares no tools at all
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[entry])

        status = eval_runner.load_manifest_status(manifest_path)

        assert eval_runner.candidate_accepted(eval_runner.WORKER_CANDIDATES[0], status) is False

    def test_wrong_manifest_version_is_rejected_wholesale(self, tmp_path: Path) -> None:
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "manifest_version": 999,
                    "source_commit": "deadbeef",
                    "results": [
                        _accepted_router_entry(eval_runner.ROUTER_BASELINE.model, "minimal")
                    ],
                }
            )
        )

        status = eval_runner.load_manifest_status(manifest_path)

        assert status.accepted == frozenset()


class TestManifestEntryRejectsMalformedEffortAndMissingRequestKwargs:
    """Regression for round-5 gauntlet finding 9: a manifest entry whose
    `effort` field is present but not a string/null must be rejected
    outright, not silently coerced to None (which would make it read as the
    verified unset-effort baseline); an entry missing `request_kwargs` (or
    carrying a non-dict value) has no evidence Phase 0 actually probed it and
    must also be rejected.
    """

    def test_malformed_effort_value_is_rejected_not_coerced_to_none(self, tmp_path: Path) -> None:
        entry = _accepted_worker_entry(eval_runner.WORKER_CANDIDATES[0].model, None)
        entry["effort"] = 0  # hand-edited: not a string, not null
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[entry])

        status = eval_runner.load_manifest_status(manifest_path)

        # Must NOT be readable as the unset-effort (None) baseline entry.
        assert ("worker", eval_runner.WORKER_CANDIDATES[0].model, None) not in status.accepted
        assert status.accepted == frozenset()

    def test_missing_request_kwargs_is_rejected(self, tmp_path: Path) -> None:
        entry = _accepted_router_entry(eval_runner.ROUTER_BASELINE.model, "minimal")
        del entry["request_kwargs"]
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[entry])

        status = eval_runner.load_manifest_status(manifest_path)

        assert eval_runner.candidate_accepted(eval_runner.ROUTER_BASELINE, status) is False

    def test_non_dict_request_kwargs_is_rejected(self, tmp_path: Path) -> None:
        entry = _accepted_router_entry(eval_runner.ROUTER_BASELINE.model, "minimal")
        entry["request_kwargs"] = "not-a-dict"
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[entry])

        status = eval_runner.load_manifest_status(manifest_path)

        assert eval_runner.candidate_accepted(eval_runner.ROUTER_BASELINE, status) is False


class TestRequestKwargsRequiredKeysAreDerivedFromProductionBuilders:
    """Regression for round 9 gauntlet, Architecture lens finding 16: the
    required-key set ``_request_kwargs_shape_ok()`` enforces must be DERIVED
    from ``build_router_request_kwargs``/``build_worker_request_kwargs``
    (the production builders), not a hand-written literal -- so a new
    load-bearing kwarg added to a builder is automatically required here
    too, instead of this validator silently staying behind the builder it's
    supposed to mirror.
    """

    def test_a_new_key_added_to_the_router_builder_is_now_required(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        real_builder = eval_runner.build_router_request_kwargs

        def _builder_with_extra_key(*args: Any, **kwargs: Any) -> dict[str, Any]:
            kwargs_out = real_builder(*args, **kwargs)
            kwargs_out["a_new_load_bearing_kwarg"] = "x"
            return kwargs_out

        monkeypatch.setattr(eval_runner, "build_router_request_kwargs", _builder_with_extra_key)

        entry = _accepted_router_entry(eval_runner.ROUTER_BASELINE.model, "minimal")
        # A previously-fully-valid entry, missing only the NEW key the
        # builder now emits.
        assert (
            eval_runner._request_kwargs_shape_ok(
                "router", entry["model"], entry["effort"], entry["request_kwargs"]
            )
            is False
        )

        entry["request_kwargs"]["a_new_load_bearing_kwarg"] = "x"
        assert (
            eval_runner._request_kwargs_shape_ok(
                "router", entry["model"], entry["effort"], entry["request_kwargs"]
            )
            is True
        )

    def test_a_new_key_added_to_the_worker_builder_is_now_required(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        real_builder = eval_runner.build_worker_request_kwargs

        def _builder_with_extra_key(*args: Any, **kwargs: Any) -> dict[str, Any]:
            kwargs_out = real_builder(*args, **kwargs)
            kwargs_out["a_new_load_bearing_kwarg"] = "x"
            return kwargs_out

        monkeypatch.setattr(eval_runner, "build_worker_request_kwargs", _builder_with_extra_key)

        entry = _accepted_worker_entry(eval_runner.WORKER_CANDIDATES[0].model, "medium")
        assert (
            eval_runner._request_kwargs_shape_ok(
                "worker", entry["model"], entry["effort"], entry["request_kwargs"]
            )
            is False
        )

        entry["request_kwargs"]["a_new_load_bearing_kwarg"] = "x"
        assert (
            eval_runner._request_kwargs_shape_ok(
                "worker", entry["model"], entry["effort"], entry["request_kwargs"]
            )
            is True
        )

    def test_judge_required_keys_track_the_builder(self) -> None:
        """Round-4 restart, Architecture Minor #6: the judge branch used to
        hand-list model/reasoning_effort/messages instead of deriving them
        from build_judge_request_kwargs(), unlike the router/worker branches
        above -- so a new key added to the builder silently went unchecked.
        """
        entry = _accepted_judge_entry(eval_runner.DEFAULT_JUDGE_MODEL)
        required = set(
            eval_runner.build_judge_request_kwargs(
                entry["model"], messages=[{"role": "user", "content": "p"}]
            )
        ) - {"max_completion_tokens"}
        assert required, "the judge builder produced no required keys -- fixture likely broken"

        for key in required:
            entry_missing_key = dict(entry["request_kwargs"])
            del entry_missing_key[key]
            assert (
                eval_runner._request_kwargs_shape_ok(
                    "judge", entry["model"], entry["effort"], entry_missing_key
                )
                is False
            ), f"removing {key!r} should have failed the judge shape check"

    def test_existing_fixtures_still_pass_with_the_derived_required_set(self) -> None:
        # The router/worker fixtures already used throughout this file must
        # not regress -- confirms the derived required set didn't
        # accidentally widen to include "timeout"/"instructions"/"include",
        # which this function's own docstring documents as non-load-bearing.
        router_entry = _accepted_router_entry(eval_runner.ROUTER_BASELINE.model, "minimal")
        assert (
            eval_runner._request_kwargs_shape_ok(
                "router",
                router_entry["model"],
                router_entry["effort"],
                router_entry["request_kwargs"],
            )
            is True
        )
        worker_entry = _accepted_worker_entry(eval_runner.WORKER_CANDIDATES[0].model, "medium")
        assert (
            eval_runner._request_kwargs_shape_ok(
                "worker",
                worker_entry["model"],
                worker_entry["effort"],
                worker_entry["request_kwargs"],
            )
            is True
        )


class TestManifestEntryRequestKwargsMustMatchItsOwnModelAndEffort:
    """Regression for round 8 gauntlet, Codex P1 finding 2: an entry's
    ``request_kwargs`` must actually name the same ``model``/effective
    ``reasoning.effort`` as the entry's own recorded ``(model, effort)``
    fields -- not just satisfy the shape check with some other candidate's
    values. Without this, a malformed/stale entry could claim acceptance for
    one (model, effort) pair while its ``request_kwargs`` evidence actually
    describes a different one (or omits ``reasoning`` entirely), authorizing
    a live paid run against a request shape Phase 0 never actually probed
    for that candidate.
    """

    def test_router_entry_with_a_disagreeing_request_kwargs_model_is_rejected(
        self, tmp_path: Path
    ) -> None:
        entry = _accepted_router_entry(
            eval_runner.ROUTER_CANDIDATES[0].model, eval_runner.ROUTER_CANDIDATES[0].effort
        )
        entry["request_kwargs"]["model"] = "some-other-model"
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[entry])

        status = eval_runner.load_manifest_status(manifest_path)

        assert eval_runner.candidate_accepted(eval_runner.ROUTER_CANDIDATES[0], status) is False

    def test_router_entry_missing_reasoning_for_a_non_minimal_effort_is_rejected(
        self, tmp_path: Path
    ) -> None:
        entry = _accepted_router_entry(
            eval_runner.ROUTER_CANDIDATES[0].model, eval_runner.ROUTER_CANDIDATES[0].effort
        )
        del entry["request_kwargs"]["reasoning"]
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[entry])

        status = eval_runner.load_manifest_status(manifest_path)

        assert eval_runner.candidate_accepted(eval_runner.ROUTER_CANDIDATES[0], status) is False

    def test_worker_entry_with_a_disagreeing_request_kwargs_effort_is_rejected(
        self, tmp_path: Path
    ) -> None:
        entry = _accepted_worker_entry(
            eval_runner.WORKER_CANDIDATES[0].model, eval_runner.WORKER_CANDIDATES[0].effort
        )
        entry["request_kwargs"]["reasoning"] = {"effort": "an-unrelated-effort"}
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[entry])

        status = eval_runner.load_manifest_status(manifest_path)

        assert eval_runner.candidate_accepted(eval_runner.WORKER_CANDIDATES[0], status) is False

    def test_worker_entry_with_an_unset_effort_but_a_reasoning_kwarg_is_rejected(
        self, tmp_path: Path
    ) -> None:
        entry = _accepted_worker_entry(eval_runner.WORKER_BASELINE.model, None)
        entry["request_kwargs"]["reasoning"] = {"effort": "high"}
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[entry])

        status = eval_runner.load_manifest_status(manifest_path)

        assert eval_runner.candidate_accepted(eval_runner.WORKER_BASELINE, status) is False

    def test_a_correctly_matching_entry_is_still_accepted(self, tmp_path: Path) -> None:
        entry = _accepted_router_entry(
            eval_runner.ROUTER_CANDIDATES[0].model, eval_runner.ROUTER_CANDIDATES[0].effort
        )
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[entry])

        status = eval_runner.load_manifest_status(manifest_path)

        assert eval_runner.candidate_accepted(eval_runner.ROUTER_CANDIDATES[0], status) is True

    def test_judge_entry_with_a_disagreeing_request_kwargs_model_is_rejected(
        self, tmp_path: Path
    ) -> None:
        """Regression for round 9 gauntlet, Codex P2 finding 2: the router
        and worker branches already cross-checked request_kwargs["model"]
        against the entry's own recorded model (round 8); the judge branch
        didn't, so a malformed/stale judge entry could carry a different
        model's request_kwargs and still pass validation.
        """
        entry = _accepted_judge_entry(eval_runner.DEFAULT_JUDGE_MODEL)
        entry["request_kwargs"]["model"] = "some-other-judge-model"
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[entry])

        status = eval_runner.load_manifest_status(manifest_path)

        assert eval_runner.judge_accepted(eval_runner.DEFAULT_JUDGE_MODEL, status) is False

    def test_a_correctly_matching_judge_entry_is_still_accepted(self, tmp_path: Path) -> None:
        entry = _accepted_judge_entry(eval_runner.DEFAULT_JUDGE_MODEL)
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[entry])

        status = eval_runner.load_manifest_status(manifest_path)

        assert eval_runner.judge_accepted(eval_runner.DEFAULT_JUDGE_MODEL, status) is True

    def test_judge_entry_missing_reasoning_effort_is_rejected(self, tmp_path: Path) -> None:
        """Regression for round 3 confirming pass, Codex P2 finding: the judge
        branch never cross-checked request_kwargs["reasoning_effort"] against
        what production actually sends for this model, so a hand-edited or
        stale-overridden manifest entry could authorize a live run against a
        gpt-5* judge request that was never probed with an effort pin at all
        -- the exact request-shape gap the round-3 reasoning_effort fix
        introduced without a manifest-side check for it.
        """
        entry = _accepted_judge_entry(eval_runner.DEFAULT_JUDGE_MODEL)
        del entry["request_kwargs"]["reasoning_effort"]
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[entry])

        status = eval_runner.load_manifest_status(manifest_path)

        assert eval_runner.judge_accepted(eval_runner.DEFAULT_JUDGE_MODEL, status) is False

    def test_judge_entry_with_a_disagreeing_reasoning_effort_is_rejected(
        self, tmp_path: Path
    ) -> None:
        entry = _accepted_judge_entry(eval_runner.DEFAULT_JUDGE_MODEL)
        entry["request_kwargs"]["reasoning_effort"] = "high"
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[entry])

        status = eval_runner.load_manifest_status(manifest_path)

        assert eval_runner.judge_accepted(eval_runner.DEFAULT_JUDGE_MODEL, status) is False


class TestManifestGateRunMatrixDirectly:
    """Regression for finding 15: run_matrix()'s own per-cell manifest check
    is reachable independently of main()'s preflight -- a direct caller that
    skips require_manifest_ok_for_live_run() still gets a manifest-rejected
    cell instead of an unguarded live call.
    """

    def test_run_matrix_rejects_an_uncovered_candidate_without_a_preflight_call(
        self, tmp_path: Path
    ) -> None:
        # Manifest only covers the baseline -- no preflight call was made.
        manifest_path = _write_manifest(
            tmp_path,
            source_commit="deadbeef",
            results=[_accepted_router_entry(eval_runner.ROUTER_BASELINE.model, "minimal")],
        )
        status = eval_runner.load_manifest_status(manifest_path)
        pair = eval_runner.RunPair(eval_runner.ROUTER_CANDIDATES[0], eval_runner.WORKER_BASELINE)

        outcomes = asyncio.run(
            eval_runner.run_matrix(
                (pair,),
                (Scenario(name="s", turns=(Turn(query="hi"),)),),
                eval_runner.Config(),
                judge_model=eval_runner.DEFAULT_JUDGE_MODEL,
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
                manifest_status=status,
            )
        )

        assert len(outcomes) == 1
        assert outcomes[0].status == "manifest-rejected"


class TestOutputPathConfinement:
    """Regression for finding 6: a ``--out`` path must not escape the repo
    tree via ``..`` traversal, and must not follow an existing symlink.
    """

    def test_traversal_outside_the_repo_root_is_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            eval_runner.confined_output_path("../../etc/passwd", allowed_root=tmp_path)

    def test_absolute_path_outside_the_repo_root_is_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            eval_runner.confined_output_path(Path("/etc/passwd"), allowed_root=tmp_path)

    def test_a_path_within_the_root_is_accepted(self, tmp_path: Path) -> None:
        resolved = eval_runner.confined_output_path("reports/out.json", allowed_root=tmp_path)
        assert resolved == (tmp_path / "reports/out.json").resolve()

    def test_an_existing_symlink_at_the_target_is_rejected(self, tmp_path: Path) -> None:
        real_target = tmp_path / "real-secret.txt"
        real_target.write_text("do not overwrite me")
        symlink_path = tmp_path / "out.json"
        symlink_path.symlink_to(real_target)

        with pytest.raises(ValueError):
            eval_runner.confined_output_path(symlink_path, allowed_root=tmp_path)

    def test_write_no_follow_refuses_an_existing_symlink(self, tmp_path: Path) -> None:
        real_target = tmp_path / "real-secret.txt"
        real_target.write_text("do not overwrite me")
        symlink_path = tmp_path / "out.json"
        symlink_path.symlink_to(real_target)

        with pytest.raises(OSError):
            eval_runner.write_no_follow(symlink_path, "clobbered")
        assert real_target.read_text() == "do not overwrite me"


class TestReportIsPersistedByDefault:
    """Regression for round 8 gauntlet, Codex P2 finding 2: the dev plan's
    Architecture & Call Flow table requires the aggregate report to persist
    to a file, not just print a summary. A run with no ``--out`` must still
    write a report file (to a timestamped default path), not silently
    discard it.
    """

    def _run_to_completion(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, extra_args: list[str]
    ) -> tuple[int, Path]:
        monkeypatch.setattr(eval_common, "build_session_for_run", _raise_network_access)
        monkeypatch.setattr(
            eval_runner, "build_session_for_run", _raise_network_access, raising=False
        )
        monkeypatch.setattr(
            eval_runner, "load_config", lambda: eval_runner.Config(openai_api_key="test-key")
        )
        # confined_output_path() confines the write to REPO_ROOT -- patch it
        # to tmp_path too, alongside DEFAULT_REPORT_DIR, so the default
        # timestamped path passes confinement without writing into the real
        # repo tree.
        monkeypatch.setattr(eval_runner, "REPO_ROOT", tmp_path)
        report_dir = tmp_path / "eval-reports"
        monkeypatch.setattr(eval_runner, "DEFAULT_REPORT_DIR", report_dir)
        exit_code = eval_runner.main(
            [
                "--router",
                "baseline",
                "--worker",
                "baseline",
                "--scenario",
                "single-turn-default",
                "--i-know-the-manifest-is-stale",
                *extra_args,
            ]
        )
        return exit_code, report_dir

    def test_a_run_with_no_out_flag_still_writes_a_report_file(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        _exit_code, report_dir = self._run_to_completion(monkeypatch, tmp_path, extra_args=[])

        written = list(report_dir.glob("eval-report-*.json"))
        assert len(written) == 1
        report = json.loads(written[0].read_text())
        assert "overall_status" in report

    def test_an_explicit_out_flag_still_wins_over_the_default(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        # confined_output_path() confines --out to REPO_ROOT -- patch it to
        # tmp_path too so an explicit tmp_path-relative --out passes
        # confinement without writing into the real repo tree.
        monkeypatch.setattr(eval_runner, "REPO_ROOT", tmp_path)
        explicit_path = tmp_path / "my-report.json"
        _exit_code, report_dir = self._run_to_completion(
            monkeypatch, tmp_path, extra_args=["--out", str(explicit_path)]
        )

        assert explicit_path.exists()
        assert not report_dir.exists()

    def test_two_runs_within_the_same_second_do_not_collide(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Regression for round 9 gauntlet, Codex P2 finding 4: a bare
        seconds-resolution default filename let a second run finishing
        within the same UTC second silently overwrite the first run's
        report via write_no_follow()'s O_TRUNC.
        """

        class _FrozenDatetime(eval_runner.datetime):
            @classmethod
            def now(cls, tz=None):  # type: ignore[override]
                return cls(2026, 8, 19, 12, 0, 0, tzinfo=tz)

        monkeypatch.setattr(eval_runner, "datetime", _FrozenDatetime)

        self._run_to_completion(monkeypatch, tmp_path, extra_args=[])
        self._run_to_completion(monkeypatch, tmp_path, extra_args=[])

        report_dir = tmp_path / "eval-reports"
        written = list(report_dir.glob("eval-report-*.json"))
        assert len(written) == 2
        assert written[0].name != written[1].name


class TestSpendLimitValidation:
    """Regression for finding 12: --max-cost must reject non-finite (NaN/inf)
    values -- a NaN spend limit makes every comparison in _confirm_spend's
    `exceeds` check false, so it would otherwise be silently treated as an
    unbounded budget instead of the intended cap. --max-calls must reject
    negative values too.
    """

    def test_max_cost_rejects_nan(self) -> None:
        parser = eval_runner.build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--max-cost", "nan"])

    def test_max_cost_rejects_infinity(self) -> None:
        parser = eval_runner.build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--max-cost", "inf"])

    def test_max_cost_rejects_negative(self) -> None:
        parser = eval_runner.build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--max-cost", "-1.0"])

    def test_max_calls_rejects_negative(self) -> None:
        parser = eval_runner.build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--max-calls", "-1"])

    def test_max_cost_accepts_a_normal_value(self) -> None:
        parser = eval_runner.build_arg_parser()
        args = parser.parse_args(["--max-cost", "5.0"])
        assert args.max_cost == 5.0


class TestRoutingBudgetRejectsNonFinite:
    """Regression: --max-routing-seconds must reject NaN/inf the same way
    --max-cost/--max-calls do. run_cell()'s blocking-budget check
    (`routing_ms > max_routing_seconds * 1000`) is false for both nan and
    inf, so an unvalidated NaN/inf value silently disables routing-budget
    enforcement instead of erroring on bad input.
    """

    def test_rejects_nan(self) -> None:
        parser = eval_runner.build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--max-routing-seconds", "nan"])

    def test_rejects_infinity(self) -> None:
        parser = eval_runner.build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--max-routing-seconds", "inf"])

    def test_rejects_zero_or_negative(self) -> None:
        parser = eval_runner.build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--max-routing-seconds", "0"])
        with pytest.raises(SystemExit):
            parser.parse_args(["--max-routing-seconds", "-1.0"])

    def test_accepts_a_normal_value(self) -> None:
        parser = eval_runner.build_arg_parser()
        args = parser.parse_args(["--max-routing-seconds", "15.0"])
        assert args.max_routing_seconds == 15.0


class TestFullMatrixConflictsWithSingleCellSelection:
    """Regression for finding 14: --full-matrix silently losing to --router/
    --worker (rather than erroring) would make a typo'd or copy-pasted
    --full-matrix flag look accepted but have no effect."""

    def test_full_matrix_with_router_flag_errors_explicitly(self) -> None:
        with pytest.raises(SystemExit):
            eval_runner.main(["--full-matrix", "--router", "baseline", "--dry-run"])

    def test_full_matrix_with_worker_flag_errors_explicitly(self) -> None:
        with pytest.raises(SystemExit):
            eval_runner.main(["--full-matrix", "--worker", "baseline", "--dry-run"])

    def test_full_matrix_alone_is_still_allowed(self) -> None:
        # Must not raise: no --router/--worker present.
        exit_code = eval_runner.main(["--full-matrix", "--dry-run"])
        assert exit_code == 0


class TestJudgeInputSanitization:
    """Regression for finding 17: light defense-in-depth sanitization before
    worker-sourced text reaches the judge's context."""

    def test_control_characters_are_stripped(self) -> None:
        dirty = "hello\x00\x07world"
        assert eval_runner._sanitize_for_judge(dirty) == "helloworld"

    def test_newlines_and_tabs_are_preserved(self) -> None:
        text = "line one\nline two\ttabbed"
        assert eval_runner._sanitize_for_judge(text) == text

    def test_length_is_capped(self) -> None:
        long_text = "a" * 10_000
        assert len(eval_runner._sanitize_for_judge(long_text, max_len=100)) == 100


class TestRunCellSetupExceptionsDontCrashTheMatrix:
    """Regression for round-2 gauntlet finding 2: run_cell()'s own setup work
    (config resolution, host/judge construction) previously sat OUTSIDE the
    function's try/finally -- an exception there would propagate out of
    run_cell() uncaught, and run_matrix() doesn't catch it either, so it
    would abort the whole matrix and discard every already-billed cell with
    no report ever written. Setup work is now inside the try, with a `host =
    None` sentinel so the finally block only shuts down a host that was
    actually constructed.
    """

    def test_per_run_config_raising_is_classified_setup_error_not_a_crash(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            eval_runner,
            "_per_run_config",
            lambda *_a, **_k: (_ for _ in ()).throw(ValueError("bad config")),
        )
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)

        outcome = asyncio.run(
            eval_runner.run_cell(
                pair,
                Scenario(name="s", turns=(Turn(query="hi"),)),
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

        assert outcome.status == "setup-error"
        assert "bad config" in (outcome.error or "")

    def test_judge_construction_raising_after_host_built_still_shuts_down_host(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Secondary bug in the same finding: host is constructed BEFORE the
        # judge, so an exception during judge construction must not leak the
        # already-constructed host -- shutdown() must still run.
        shutdown_calls: list[bool] = []

        class _TrackedHost:
            state = _FakeState()
            registry = _FakeRegistry()

            async def start(self) -> None:
                raise AssertionError("start() must not be reached if judge construction failed")

            async def connect(self, _h: Any) -> str:
                raise AssertionError("connect() must not be reached")

            async def _handle_transcript(self, *_a: Any, **_k: Any) -> Any:
                raise AssertionError("a turn must not run")

            async def shutdown(self) -> None:
                shutdown_calls.append(True)

        monkeypatch.setattr(eval_runner, "build_session_for_run", lambda *_a, **_k: _TrackedHost())
        monkeypatch.setattr(
            eval_runner,
            "build_judge_llm_service",
            lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("judge service unavailable")),
        )

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = asyncio.run(
            eval_runner.run_cell(
                pair,
                Scenario(name="s", turns=(Turn(query="hi"),)),
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

        assert outcome.status == "setup-error"
        assert "judge service unavailable" in (outcome.error or "")
        assert shutdown_calls == [True]

    def test_shutdown_failure_is_reported_not_raised(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # A shutdown() failure itself must not crash run_matrix or mask a
        # genuinely "ok" cell's outcome by raising out of run_cell().
        #
        # Regression for round-3 gauntlet finding 3: this branch is only
        # reachable when every turn already ran and succeeded (cell_status ==
        # "ok" going into the finally block) -- shutdown() runs strictly
        # after every turn, so "setup-error" was self-contradictory alongside
        # N "ok" turns. Fixed to report "turn-error" instead (both are
        # already infra-failure statuses, so no pass/fail behavior change).
        class _FailingShutdownHost(_FakeHost):
            async def shutdown(self) -> None:
                raise RuntimeError("shutdown exploded")

        def _fake_build_session_for_run(_config: Any, *, measurement_sink: Any = None) -> Any:
            del measurement_sink
            return _FailingShutdownHost(
                lambda _q: _FakeResult(
                    ui_text="a distinctly non-fallback reply",
                    spoken_text="a distinctly non-fallback spoken reply",
                    citations=[],
                    turn_id="turn-1",
                )
            )

        monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
        monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)
        monkeypatch.setattr("pipecat.evals.judge.EvalJudge", _make_stub_judge_class([], None))
        monkeypatch.setattr(
            eval_runner,
            "latest_turn_stage_metrics",
            lambda *_a, **_k: {"routing_ms": 10.0, "search_ms": 0.0, "total_ms": 10.0},
        )

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = asyncio.run(
            eval_runner.run_cell(
                pair,
                Scenario(name="s", turns=(Turn(query="hi"),)),
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

        assert outcome.status == "turn-error"
        assert "shutdown exploded" in (outcome.error or "")


class TestNeverRanCellsAreBackfilledConsistently:
    """Regression for round-3 gauntlet finding 2: cells that never ran any
    turn were reported inconsistently depending on which code path produced
    them. The router-config-mismatch early return and the manifest-rejected
    early return in ``run_matrix()`` both left ``turns`` at its default
    (rendering as ``[]`` in the report), while the exception-path fallback
    backfilled ``N`` "skipped" ``TurnOutcome``s. Fixed by routing every
    "never ran" producer through the shared ``_never_ran_cell()``/
    ``_skipped_turn_outcomes()`` helpers.
    """

    def test_router_config_mismatch_backfills_all_turns_as_skipped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Force the pre-call router assertion to fail by making the resolved
        # model diverge from what the candidate claims -- no real Config
        # subclassing needed, just monkeypatch resolve_router_model.
        monkeypatch.setattr(
            eval_runner.Config,
            "resolve_router_model",
            lambda self, _label: "some-other-model",
        )

        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        scenario = Scenario(name="s", turns=(Turn(query="turn one"), Turn(query="turn two")))
        outcome = asyncio.run(
            eval_runner.run_cell(
                pair,
                scenario,
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

        assert outcome.status == "setup-error"
        assert len(outcome.turns) == 2
        assert [t.status for t in outcome.turns] == ["skipped", "skipped"]
        assert [t.query for t in outcome.turns] == ["turn one", "turn two"]
        # The per-run Config had already been resolved by the time the
        # mismatch was caught -- its provider timeouts must still be threaded
        # into the CellOutcome, not dropped.
        assert outcome.router_timeout_seconds is not None
        assert outcome.foreground_search_timeout_seconds is not None

    def test_manifest_rejected_cell_backfills_all_turns_as_skipped(self) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        scenario = Scenario(name="s", turns=(Turn(query="turn one"), Turn(query="turn two")))
        empty_status = eval_runner.ManifestStatus(
            path=Path("/nonexistent"),
            exists=True,
            source_commit="deadbeef",
            current_commit="deadbeef",
            stale=False,
            accepted=frozenset(),
        )

        outcomes = asyncio.run(
            eval_runner.run_matrix(
                (pair,),
                (scenario,),
                eval_runner.Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
                manifest_status=empty_status,
            )
        )

        assert len(outcomes) == 1
        outcome = outcomes[0]
        assert outcome.status == "manifest-rejected"
        assert len(outcome.turns) == 2
        assert [t.status for t in outcome.turns] == ["skipped", "skipped"]
        assert [t.query for t in outcome.turns] == ["turn one", "turn two"]


class TestManifestDiagnosticsPrintEffectiveEffort:
    """Regression for round-2 gauntlet finding 13: the "absent from manifest"
    error and the dry-run matrix preview must print the EFFECTIVE (resolved)
    effort, not the raw candidate.effort -- for the router baseline,
    candidate.effort is None but the manifest lookup actually resolved and
    checked "minimal" (server.router.effective_router_reasoning_effort's
    gpt-5* conditional). Printing the raw None would show an operator a
    combination that isn't the one actually looked up.
    """

    def test_missing_baseline_error_names_the_effective_effort_not_none(
        self, tmp_path: Path
    ) -> None:
        # Manifest exists but doesn't cover the router baseline at all.
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[])
        status = eval_runner.load_manifest_status(manifest_path)

        with pytest.raises(eval_runner.ManifestError) as excinfo:
            eval_runner.require_manifest_ok_for_live_run(
                status,
                allow_stale=True,
                candidates=(eval_runner.ROUTER_BASELINE,),
                judge_model=eval_runner.DEFAULT_JUDGE_MODEL,
            )

        message = str(excinfo.value)
        assert "@minimal" in message
        assert "@None" not in message

    def test_dry_run_preview_prints_effective_effort_for_the_baseline_pair(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[])
        status = eval_runner.load_manifest_status(manifest_path)
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)

        eval_runner.print_matrix_preview(
            (pair,),
            (Scenario(name="s", turns=(Turn(query="hi"),)),),
            judge_model=eval_runner.DEFAULT_JUDGE_MODEL,
            status=status,
        )

        out = capsys.readouterr().out
        assert "router=gpt-5-mini@minimal" in out
        assert "router=gpt-5-mini@None" not in out


# ---------------------------------------------------------------------------
# Round 4 gauntlet regressions.
# ---------------------------------------------------------------------------


class TestLatencyBudgetRejectsNonFinite:
    """Regression for round-4 finding 1: --max-latency-seconds must reject
    NaN/inf/non-positive the same way --max-routing-seconds does. run_cell()'s
    blocking-budget check (`total_ms > max_latency_seconds * 1000`) is false
    for nan/inf, and even 0/-1 make every measured result exceed the limit.
    """

    def test_rejects_nan(self) -> None:
        parser = eval_runner.build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--max-latency-seconds", "nan"])

    def test_rejects_infinity(self) -> None:
        parser = eval_runner.build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--max-latency-seconds", "inf"])

    def test_rejects_zero_or_negative(self) -> None:
        parser = eval_runner.build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--max-latency-seconds", "0"])
        with pytest.raises(SystemExit):
            parser.parse_args(["--max-latency-seconds", "-1000"])

    def test_accepts_a_normal_value(self) -> None:
        parser = eval_runner.build_arg_parser()
        args = parser.parse_args(["--max-latency-seconds", "60.0"])
        assert args.max_latency_seconds == 60.0

    def test_error_message_names_the_correct_flag(self) -> None:
        parser = eval_runner.build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--max-latency-seconds", "not-a-number"])
        with pytest.raises(SystemExit):
            parser.parse_args(["--max-routing-seconds", "not-a-number"])


class TestForegroundTimeoutIsClassifiedAsTimeoutNotSemanticFailure:
    """Regression for round-4 finding 2: the foreground-search-timeout
    placeholder text must be classified as a "timeout" infra outcome, not
    scored as a semantic judge/citations failure -- previously only 3 of 7
    degraded/failure texts were covered by SAFE_FALLBACK_TEXTS.
    """

    def test_timeout_placeholder_marks_the_turn_status_timeout(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather?", judge_criterion="names a temperature"),),
            ui_text=("That is taking longer than expected; I will continue in the background."),
        )

        assert outcome.status == "timeout"
        assert outcome.turns[0].status == "timeout"

    def test_timeout_placeholder_never_reaches_the_judge(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        recorder: list[Any] = []
        _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather?", judge_criterion="names a temperature"),),
            ui_text=("That is taking longer than expected; I will continue in the background."),
            judge_recorder=recorder,
        )

        assert all(not judge.assistant_messages for judge in recorder)

    def test_other_safe_fallback_texts_are_still_provider_error_not_timeout(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        outcome = _run_cell(
            monkeypatch,
            pair=pair,
            turns=(Turn(query="weather?", judge_criterion="names a temperature"),),
            ui_text="The search service is busy; please try again shortly.",
        )

        assert outcome.status == "provider-error"

    def test_safe_fallbacks_and_timeout_fallbacks_are_re_exported_and_disjoint(self) -> None:
        assert eval_common.SAFE_FALLBACKS.isdisjoint(eval_common.TIMEOUT_FALLBACKS)
        assert (
            "That is taking longer than expected; I will continue in the background."
            in eval_common.TIMEOUT_FALLBACKS
        )


class TestManifestRouterEntryRequiresTextTool:
    """Regression for round-4 finding 4: a router candidate's manifest entry
    must declare the structured-output `text` shape it was actually probed
    with -- kind/model/effort alone would authorize a request shape Phase 0
    never verified for a hand-edited or faulty-verifier-produced manifest.
    """

    def test_router_entry_missing_text_tool_is_not_accepted(self, tmp_path: Path) -> None:
        entry = _accepted_router_entry(eval_runner.ROUTER_BASELINE.model, "minimal")
        entry["tools"] = []  # hand-edited: declares no tools at all
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[entry])

        status = eval_runner.load_manifest_status(manifest_path)

        assert eval_runner.candidate_accepted(eval_runner.ROUTER_BASELINE, status) is False

    def test_router_entry_with_text_tool_is_accepted(self, tmp_path: Path) -> None:
        entry = _accepted_router_entry(eval_runner.ROUTER_BASELINE.model, "minimal")
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[entry])

        status = eval_runner.load_manifest_status(manifest_path)

        assert eval_runner.candidate_accepted(eval_runner.ROUTER_BASELINE, status) is True


class TestNoneOrBareStrResultIsDiagnosedNotAnUncaughtAttributeError:
    """Regression for round-4 finding 5: _handle_transcript() has documented
    return paths that return None (background-status-only retained work item)
    or a bare str transcript -- neither carries .ui_text, and previously the
    bare AttributeError from accessing it escaped to the outer handler as an
    undiagnostic generic "turn-error".
    """

    def test_none_result_produces_a_diagnostic_provider_error_not_a_crash(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)

        def _fake_build_session_for_run(_config: Any, *, measurement_sink: Any = None) -> Any:
            del measurement_sink
            return _FakeHost(lambda _query: None)

        monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
        monkeypatch.setattr("pipecat.evals.judge.EvalJudge", _make_stub_judge_class([], None))
        monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)

        from server.config import Config

        outcome = asyncio.run(
            eval_runner.run_cell(
                pair,
                Scenario(name="s", turns=(Turn(query="hi"),)),
                Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

        assert outcome.status == "provider-error"
        assert outcome.turns[0].status == "provider-error"
        assert "NoneType" in outcome.error

    def test_bare_str_result_produces_a_diagnostic_provider_error_not_a_crash(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)

        def _fake_build_session_for_run(_config: Any, *, measurement_sink: Any = None) -> Any:
            del measurement_sink
            return _FakeHost(lambda _query: "a bare str transcript")

        monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
        monkeypatch.setattr("pipecat.evals.judge.EvalJudge", _make_stub_judge_class([], None))
        monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)

        from server.config import Config

        outcome = asyncio.run(
            eval_runner.run_cell(
                pair,
                Scenario(name="s", turns=(Turn(query="hi"),)),
                Config(),
                judge_model="gpt-5-mini",
                max_routing_seconds=15.0,
                max_latency_seconds=15.0,
            )
        )

        assert outcome.status == "provider-error"
        assert "str" in outcome.error


class TestConfirmSpendHandlesEOFError:
    """Regression for round-4 finding 6: a tty that receives EOF (Ctrl-D)
    raises EOFError from input(), uncaught -- sys.stdin.isatty() doesn't rule
    this out (it's a real tty, just one that got EOF). Must be treated as an
    explicit decline, matching the "n" answer's return value.
    """

    def test_eof_error_is_treated_as_decline(self, monkeypatch: pytest.MonkeyPatch) -> None:
        accounting = eval_runner.CallAccounting(router_calls=100, worker_calls=100, judge_calls=100)
        monkeypatch.setattr(eval_runner.sys.stdin, "isatty", lambda: True)

        def _raise_eof(_prompt: str) -> str:
            raise EOFError

        monkeypatch.setattr("builtins.input", _raise_eof)

        result = eval_runner._confirm_spend(
            accounting, max_calls=1, max_cost=None, assume_yes=False
        )

        assert result is False

    def test_explicit_no_answer_returns_the_same_value_as_eof(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        accounting = eval_runner.CallAccounting(router_calls=100, worker_calls=100, judge_calls=100)
        monkeypatch.setattr(eval_runner.sys.stdin, "isatty", lambda: True)
        monkeypatch.setattr("builtins.input", lambda _prompt: "n")

        result = eval_runner._confirm_spend(
            accounting, max_calls=1, max_cost=None, assume_yes=False
        )

        assert result is False


class TestErrorTextStripsControlCharacters:
    """Regression for round-4 finding 7: error_text() must strip ASCII
    control characters (e.g. ANSI escape sequences a provider-exception body
    could carry) before the text reaches a raw print() sink -- previously
    only redaction/truncation were applied, and this filter existed only as
    eval_model_comparison.py's local _sanitize_for_judge(), unused by
    error_text()'s own raw-print call sites.
    """

    def test_ansi_escape_sequence_is_stripped(self) -> None:
        exc = ValueError("bad response \x1b[31mRED\x1b[0m text")

        text = eval_common.error_text(exc)

        assert "\x1b" not in text
        assert "RED" in text

    def test_newline_and_tab_are_preserved(self) -> None:
        exc = ValueError("line one\nline two\ttabbed")

        text = eval_common.error_text(exc)

        assert "\n" in text
        assert "\t" in text

    def test_strip_control_chars_is_shared_with_sanitize_for_judge(self) -> None:
        raw = "clean\x07bell\x1b[2Jclear"

        assert eval_common.strip_control_chars(raw) == eval_runner._sanitize_for_judge(raw)

    def test_c1_control_range_is_stripped(self) -> None:
        # Regression for round-5 gauntlet finding 8: the original pattern
        # only covered ASCII C0 + DEL, not the C1 range (U+0080-U+009F) --
        # mainstream terminals honor some of these too.
        raw = "before\x9bmiddle\x85after"

        text = eval_common.strip_control_chars(raw)

        assert "\x9b" not in text
        assert "\x85" not in text
        assert "before" in text and "middle" in text and "after" in text

    def test_bidi_override_characters_are_stripped(self) -> None:
        # Regression for round-5 gauntlet finding 8: Unicode bidirectional
        # overrides (U+202A-U+202E, U+2066-U+2069) could visually reorder an
        # operator-facing error/report line in a terminal that honors them.
        # Expressed as \N{...} escapes, not literal characters, so this
        # source file never itself embeds an invisible/control code point.
        rle = "\N{RIGHT-TO-LEFT EMBEDDING}"
        pdf = "\N{POP DIRECTIONAL FORMATTING}"
        lri = "\N{LEFT-TO-RIGHT ISOLATE}"
        pdi = "\N{POP DIRECTIONAL ISOLATE}"
        raw = f"safe{rle}evil{pdf}reversed{lri}iso{pdi}end"

        text = eval_common.strip_control_chars(raw)

        for bidi_char in (rle, pdf, lri, pdi):
            assert bidi_char not in text
        assert "safe" in text and "evil" in text and "reversed" in text


# ---------------------------------------------------------------------------
# Round 3 confirming pass regressions.
# ---------------------------------------------------------------------------


class TestMatrixPreviewLabelsPerScenarioScope:
    """Regression for round-3 confirming pass, Logic finding 2:
    print_matrix_preview()'s per-scenario breakdown printed raw per-cell,
    pre-retry counts right above the grand "Total calls:" line with nothing
    naming the scope difference -- an operator approving spend from this
    output could read the per-scenario numbers as summing to the total.
    matrix_call_accounting() actually multiplies by len(pairs), by
    repeat_count, AND by _RETRY_WORST_CASE_MULTIPLIER, so no single-factor
    rescale would have made them line up; the fix relabels rather than
    rescaling.
    """

    def test_per_scenario_breakdown_is_labelled_with_its_scope(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[])
        status = eval_runner.load_manifest_status(manifest_path)
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        scenario = Scenario(name="s", turns=(Turn(query="hi"), Turn(query="there")))

        eval_runner.print_matrix_preview(
            (pair,),
            (scenario,),
            judge_model=eval_runner.DEFAULT_JUDGE_MODEL,
            status=status,
            repeat_count=3,
        )

        out = capsys.readouterr().out
        assert "one config pair, one repeat, before retry worst case" in out

    def test_per_scenario_router_count_is_unscaled(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[])
        status = eval_runner.load_manifest_status(manifest_path)
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        scenario = Scenario(name="s", turns=(Turn(query="hi"), Turn(query="there")))

        eval_runner.print_matrix_preview(
            (pair,),
            (scenario,),
            judge_model=eval_runner.DEFAULT_JUDGE_MODEL,
            status=status,
            repeat_count=3,
        )

        out = capsys.readouterr().out
        router_calls, _worker_calls, _judge_calls = eval_runner.scenario_call_counts(scenario)
        assert f"router={router_calls} " in out
        assert router_calls == len(scenario.turns)

    def test_total_calls_line_still_matches_matrix_call_accounting(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[])
        status = eval_runner.load_manifest_status(manifest_path)
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
        scenario = Scenario(name="s", turns=(Turn(query="hi"), Turn(query="there")))

        eval_runner.print_matrix_preview(
            (pair,),
            (scenario,),
            judge_model=eval_runner.DEFAULT_JUDGE_MODEL,
            status=status,
            repeat_count=3,
        )

        out = capsys.readouterr().out
        accounting = eval_runner.matrix_call_accounting((pair,), (scenario,), repeat_count=3)
        assert f"total={accounting.total_calls}" in out


class TestLatencyBudgetExceededPredicate:
    """Regression for round-3 confirming pass, Architecture finding 5:
    run_cell()'s per-turn live budget check and _aggregate_turn_repeats()'
    recomputation from the aggregated means duplicated the same boolean
    expression at two call sites -- a future edit to one (e.g. a `>=` vs `>`
    fix) could silently desync from the other, contradicting round 10's
    Logic finding 6 invariant that both must agree.
    """

    def test_returns_none_when_nothing_measured(self) -> None:
        result = eval_runner._latency_budget_exceeded(
            None, None, max_routing_seconds=15.0, max_latency_seconds=60.0
        )

        assert result is None

    def test_returns_false_when_only_one_side_measured_and_within_budget(self) -> None:
        result = eval_runner._latency_budget_exceeded(
            None, 100.0, max_routing_seconds=15.0, max_latency_seconds=60.0
        )

        assert result is False

    def test_boundary_is_strictly_greater(self) -> None:
        at_budget = eval_runner._latency_budget_exceeded(
            None, 60_000.0, max_routing_seconds=15.0, max_latency_seconds=60.0
        )
        over_budget = eval_runner._latency_budget_exceeded(
            None, 60_000.1, max_routing_seconds=15.0, max_latency_seconds=60.0
        )

        assert at_budget is False
        assert over_budget is True

    def test_routing_alone_can_exceed(self) -> None:
        result = eval_runner._latency_budget_exceeded(
            20_000.0, 100.0, max_routing_seconds=15.0, max_latency_seconds=60.0
        )

        assert result is True

    def test_run_cell_and_aggregate_use_the_same_predicate(self, monkeypatch: Any) -> None:
        sentinel = object()
        monkeypatch.setattr(
            eval_runner, "_latency_budget_exceeded", lambda *args, **kwargs: sentinel
        )
        turn = eval_runner.TurnOutcome(
            query="q",
            status="ok",
            routing_ms=10.0,
            total_ms=20.0,
        )

        aggregated = eval_runner._aggregate_turn_repeats(
            [turn, turn], max_routing_seconds=15.0, max_latency_seconds=60.0
        )

        assert aggregated.latency_budget_exceeded is sentinel


class TestDefaultSweepAnchorsOnShippedConfig:
    """Regression for round-4 restart, Architecture finding 2: the sweep
    varies one role at a time, so the production anchor for a router
    candidate is (shipped_router x WORKER_BASELINE) -- already guaranteed
    present by TestShippedConfigHasAnEvalCandidateCell -- not a live joint
    shipped x shipped cell. default_sweep_pairs() no longer runs that joint
    cell; build_report() names which already-present cells carry the shipped
    router/worker instead.
    """

    @staticmethod
    def _shipped_pair() -> tuple[eval_runner.Candidate, eval_runner.Candidate]:
        # Same construction as tests/test_eval_common.py's
        # TestShippedConfigHasAnEvalCandidateCell -- the repo-tracked
        # config.toml, loaded with env={} so the anchor is the same on every
        # machine.
        return eval_runner.shipped_candidates()

    def test_default_sweep_has_no_joint_shipped_cell(self) -> None:
        shipped_router, shipped_worker = self._shipped_pair()
        router_key = (
            shipped_router.model,
            eval_runner.effective_effort_for_manifest_lookup(shipped_router),
        )
        worker_key = (
            shipped_worker.model,
            eval_runner.effective_effort_for_manifest_lookup(shipped_worker),
        )
        baseline_router_key = (
            eval_runner.ROUTER_BASELINE.model,
            eval_runner.effective_effort_for_manifest_lookup(eval_runner.ROUTER_BASELINE),
        )
        baseline_worker_key = (
            eval_runner.WORKER_BASELINE.model,
            eval_runner.effective_effort_for_manifest_lookup(eval_runner.WORKER_BASELINE),
        )
        if router_key == baseline_router_key and worker_key == baseline_worker_key:
            pytest.skip("shipped pair coincides with the historical baseline on this checkout")

        pairs = eval_runner.default_sweep_pairs()

        for pair in pairs:
            pair_router_key = (
                pair.router.model,
                eval_runner.effective_effort_for_manifest_lookup(pair.router),
            )
            pair_worker_key = (
                pair.worker.model,
                eval_runner.effective_effort_for_manifest_lookup(pair.worker),
            )
            assert not (pair_router_key == router_key and pair_worker_key == worker_key)

    def test_shipped_router_and_worker_each_have_a_one_role_varied_cell(self) -> None:
        shipped_router, shipped_worker = self._shipped_pair()
        router_key = (
            shipped_router.model,
            eval_runner.effective_effort_for_manifest_lookup(shipped_router),
        )
        worker_key = (
            shipped_worker.model,
            eval_runner.effective_effort_for_manifest_lookup(shipped_worker),
        )
        baseline_worker_key = (
            eval_runner.WORKER_BASELINE.model,
            eval_runner.effective_effort_for_manifest_lookup(eval_runner.WORKER_BASELINE),
        )
        baseline_router_key = (
            eval_runner.ROUTER_BASELINE.model,
            eval_runner.effective_effort_for_manifest_lookup(eval_runner.ROUTER_BASELINE),
        )

        pairs = eval_runner.default_sweep_pairs()

        assert any(
            (pair.router.model, eval_runner.effective_effort_for_manifest_lookup(pair.router))
            == router_key
            and (pair.worker.model, eval_runner.effective_effort_for_manifest_lookup(pair.worker))
            == baseline_worker_key
            for pair in pairs
        )
        assert any(
            (pair.router.model, eval_runner.effective_effort_for_manifest_lookup(pair.router))
            == baseline_router_key
            and (pair.worker.model, eval_runner.effective_effort_for_manifest_lookup(pair.worker))
            == worker_key
            for pair in pairs
        )

    def test_report_annotates_the_shipped_cells(self) -> None:
        shipped_router, shipped_worker = self._shipped_pair()
        pairs = eval_runner.default_sweep_pairs()
        outcomes = [
            eval_runner.CellOutcome(pair_label=pair.label, scenario_name="s", status="ok")
            for pair in pairs
        ]

        report = eval_runner.build_report(
            outcomes, judge_model="judge", shipped=(shipped_router, shipped_worker)
        )

        shipped_cells = report["shipped_config_cells"]
        assert shipped_cells is not None
        router_key = (
            shipped_router.model,
            eval_runner.effective_effort_for_manifest_lookup(shipped_router),
        )
        expected_router_cells = [
            pair.label
            for pair in pairs
            if (pair.router.model, eval_runner.effective_effort_for_manifest_lookup(pair.router))
            == router_key
        ]
        assert sorted(shipped_cells["router"]["cells"]) == sorted(expected_router_cells)
        assert expected_router_cells  # sanity: the guaranteeing invariant actually holds here

    def test_default_sweep_pairs_does_no_file_io(self, monkeypatch: Any) -> None:
        # The dry-run contract reproduction: default_sweep_pairs() must not
        # call load_config() (directly or via shipped_candidates()) -- that
        # call now lives on the live-run-only path in main().
        def _raise(*_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("default_sweep_pairs() must not read config.toml")

        monkeypatch.setattr(eval_common, "load_config", _raise)

        eval_runner.default_sweep_pairs()

    def test_shipped_anchor_does_not_disturb_the_existing_sweep(self) -> None:
        pairs = eval_runner.default_sweep_pairs()
        expected_prefix_len = (
            1 + len(eval_runner.ROUTER_CANDIDATES) + len(eval_runner.WORKER_CANDIDATES)
        )

        expected_prefix = [
            eval_runner.RunPair(
                eval_runner.ROUTER_BASELINE,
                eval_runner.WORKER_BASELINE,
                enforce_latency_budget=True,
            )
        ]
        expected_prefix += [
            eval_runner.RunPair(candidate, eval_runner.WORKER_BASELINE)
            for candidate in eval_runner.ROUTER_CANDIDATES
        ]
        expected_prefix += [
            eval_runner.RunPair(eval_runner.ROUTER_BASELINE, candidate)
            for candidate in eval_runner.WORKER_CANDIDATES
        ]

        assert list(pairs[:expected_prefix_len]) == expected_prefix
