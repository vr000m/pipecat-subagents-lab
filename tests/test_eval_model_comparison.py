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
(``scripts/eval_model_comparison.py``, ``scripts/_eval_common.py``,
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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import pytest

pytest.importorskip(
    "scripts.eval_model_comparison",
    reason="Phase 2 implementation not yet landed (offline test written against the plan's contract)",
)

import scripts._eval_common as eval_common
import scripts.eval_model_comparison as eval_runner
from evals import scenarios as eval_scenarios
from evals.scenarios import Scenario, Turn

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
    return {
        "kind": "router",
        "model": model,
        "effort": effort,
        "tools": ["text"],
        "request_kwargs": {"model": model},
        "accepted": True,
        "error": None,
        "response_id": "resp-1",
    }


def _accepted_worker_entry(model: str, effort: str | None) -> dict[str, Any]:
    return {
        "kind": "worker",
        "model": model,
        "effort": effort,
        "tools": ["web_search"],
        "request_kwargs": {"model": model},
        "accepted": True,
        "error": None,
        "response_id": "resp-2",
    }


def _accepted_judge_entry(model: str) -> dict[str, Any]:
    return {"kind": "judge", "model": model, "effort": None, "accepted": True, "error": None}


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


class _FakeState:
    def __init__(self) -> None:
        self.session_id = "session-1"
        self.resume_token = "resume-1"


class _FakeRegistry:
    workers: ClassVar[list[Any]] = []


@dataclass
class _FakeResult:
    ui_text: str
    spoken_text: str
    citations: list[Any]
    turn_id: str


class _FakeHost:
    def __init__(self, result_factory: Any) -> None:
        self.state = _FakeState()
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
) -> Any:
    from server.config import Config

    result_citations = citations if citations is not None else []

    def _result_factory(_query: str) -> Any:
        return _FakeResult(
            ui_text=ui_text, spoken_text=spoken_text, citations=result_citations, turn_id="turn-1"
        )

    def _fake_build_session_for_run(_config: Any, *, measurement_sink: Any = None) -> Any:
        del measurement_sink
        return _FakeHost(_result_factory)

    def _fake_stage_metrics(_sink: Any, _elapsed_ms: float, _turn_id: str) -> dict[str, float]:
        return {"routing_ms": routing_ms, "search_ms": 0.0, "total_ms": total_ms}

    monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
    monkeypatch.setattr(eval_runner, "_latest_turn_stage_metrics", _fake_stage_metrics)
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
        # baseline x baseline (1) + 2 router candidates x baseline worker (2)
        # + baseline router x 2 worker candidates (2) == 5, not 3*3==9.
        assert len(pairs) == 5

    def test_default_sweep_contains_baseline_by_baseline(self) -> None:
        pairs = eval_runner.default_sweep_pairs()
        assert (
            eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE) in pairs
        )

    def test_default_sweep_varies_router_only_against_baseline_worker(self) -> None:
        pairs = eval_runner.default_sweep_pairs()
        for candidate in eval_runner.ROUTER_CANDIDATES:
            assert eval_runner.RunPair(candidate, eval_runner.WORKER_BASELINE) in pairs
            # A router candidate must never be paired with a worker candidate
            # in the default (non-full-matrix) sweep.
            for worker_candidate in eval_runner.WORKER_CANDIDATES:
                assert eval_runner.RunPair(candidate, worker_candidate) not in pairs

    def test_default_sweep_varies_worker_only_against_baseline_router(self) -> None:
        pairs = eval_runner.default_sweep_pairs()
        for candidate in eval_runner.WORKER_CANDIDATES:
            assert eval_runner.RunPair(eval_runner.ROUTER_BASELINE, candidate) in pairs

    def test_full_matrix_is_the_full_cross_product(self) -> None:
        pairs = eval_runner.full_matrix_pairs()
        assert len(pairs) == 3 * 3
        routers = (eval_runner.ROUTER_BASELINE, *eval_runner.ROUTER_CANDIDATES)
        workers = (eval_runner.WORKER_BASELINE, *eval_runner.WORKER_CANDIDATES)
        for router in routers:
            for worker in workers:
                assert eval_runner.RunPair(router, worker) in pairs

    def test_full_matrix_is_a_strict_superset_of_the_default_sweep(self) -> None:
        default_pairs = set(eval_runner.default_sweep_pairs())
        full_pairs = set(eval_runner.full_matrix_pairs())
        assert default_pairs <= full_pairs
        assert default_pairs != full_pairs


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
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
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
        pair = eval_runner.RunPair(eval_runner.ROUTER_BASELINE, eval_runner.WORKER_BASELINE)
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
            "_current_source_commit",
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
            "_current_source_commit",
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
            turns=(Turn(query="weather in Riga?", expect_delegated=True),),
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
            turns=(Turn(query="weather in Riga?", expect_delegated=True),),
            citations=[],
            verdicts=[],
        )
        assert outcome.turns[0].citations_pass is False

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
        to import its query constants from scripts/_eval_common.py, making
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
    AFTER at least one turn already completed and was billed (e.g.
    ``_latest_turn_stage_metrics()`` raising because no
    ``app_turn_foreground`` record was emitted) previously landed in the
    outer handler that unconditionally set ``cell_status = "setup-error"`` --
    misreporting a mid-turn failure as if nothing had run yet. Fixed by
    checking whether ``turns`` already has entries: "turn-error" if so,
    "setup-error" only if the exception happened before any turn completed.
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
            raise RuntimeError("no app_turn_foreground metric was emitted for turn_id='turn-1'")

        monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
        monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)
        monkeypatch.setattr("pipecat.evals.judge.EvalJudge", _make_stub_judge_class([], None))
        monkeypatch.setattr(
            eval_runner, "_latest_turn_stage_metrics", _stage_metrics_fails_on_second_turn
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
            raise RuntimeError("no app_turn_foreground metric was emitted for turn_id='turn-1'")

        monkeypatch.setattr(eval_runner, "build_session_for_run", _fake_build_session_for_run)
        monkeypatch.setattr(eval_runner, "build_judge_llm_service", lambda *_a, **_k: None)
        monkeypatch.setattr("pipecat.evals.judge.EvalJudge", _make_stub_judge_class([], None))
        monkeypatch.setattr(eval_runner, "_latest_turn_stage_metrics", _stage_metrics_always_fails)

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
            "_latest_turn_stage_metrics",
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
            "_latest_turn_stage_metrics",
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


class TestManifestStalenessFailsClosed:
    """Regression for finding 5: an unverifiable identity -- a manifest
    missing source_commit, or a current-commit lookup that itself failed --
    must be treated as stale (fail closed), not silently accepted as fresh.
    """

    def test_missing_source_commit_is_treated_as_stale(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(eval_runner, "_current_source_commit", lambda: "abc123")
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
        monkeypatch.setattr(eval_runner, "_current_source_commit", lambda: None)
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[])

        status = eval_runner.load_manifest_status(manifest_path)

        assert status.stale is True

    def test_matching_verifiable_identity_is_not_stale(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(eval_runner, "_current_source_commit", lambda: "deadbeef")
        manifest_path = _write_manifest(tmp_path, source_commit="deadbeef", results=[])

        status = eval_runner.load_manifest_status(manifest_path)

        assert status.stale is False


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
            "_latest_turn_stage_metrics",
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
