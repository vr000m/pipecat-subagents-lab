"""Regression tests for scripts/eval_common.py's build_session_for_run().

Covers gauntlet round-2 finding 1 (promotion_manifest was never threaded
through, silently disabling promotion eligibility for every host this helper
builds) and finding 7 (stt/tts were bound post-hoc instead of at
construction, leaving SessionHost._tts_on_event stale for a caller that
reassigns host.tts after the fact).
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.eval_common as eval_common_module
import server.app as app_module
import server.composition as composition_module
from scripts.eval_common import (
    JUDGE_MAX_TOKENS,
    SAFE_FALLBACKS,
    _redact,
    build_judge_llm_service,
    build_judge_request_kwargs,
    build_session_for_run,
    confined_output_path,
    turn_correlated_routing_action,
)
from server.config import Config, PromotionManifest, load_config
from server.pipeline import SAFE_FALLBACK_TEXTS


def test_build_session_for_run_delegates_to_the_one_composition_root(
    monkeypatch,
) -> None:
    """Regression for round-5 gauntlet finding 1: scripts/eval_common.py's
    build_session_for_run() and server/app.py's _default_session_host() used
    to independently wire WorkerRegistry/Router/WorkItemCoordinator/
    SessionHost, with nothing detecting divergence between the two. Both now
    delegate to server.composition.build_session_host() -- asserted here by
    identity (calls the same object), not just by re-verifying equal output,
    so a future change that re-introduces a second, independent wiring call
    site fails this test rather than silently drifting again.
    """
    calls: list[dict[str, object]] = []
    real_build = composition_module.build_session_host

    def _recording_build(config, **kwargs):
        calls.append({"config": config, **kwargs})
        return real_build(config, **kwargs)

    monkeypatch.setattr(eval_common_module, "build_session_host", _recording_build)
    monkeypatch.setattr(app_module, "build_session_host", _recording_build)
    monkeypatch.setattr(app_module, "load_config", lambda: Config())

    build_session_for_run(Config())
    app_module._default_session_host()

    assert len(calls) == 2


def test_safe_fallbacks_is_the_same_object_as_pipelines_shared_constant() -> None:
    # Regression for round-2 gauntlet finding 9: SAFE_FALLBACKS used to be a
    # hand-typed duplicate of the three literal strings server/pipeline.py
    # sends -- a wording change there would silently desync this guard into
    # a no-op false PASS. Now re-exported (not re-typed), so this identity
    # check would fail loudly instead if that ever regresses.
    assert SAFE_FALLBACKS is SAFE_FALLBACK_TEXTS


def test_promotion_manifest_flows_into_promotion_eligible() -> None:
    host = build_session_for_run(
        Config(), promotion_manifest=PromotionManifest(promotion_eligible=True)
    )
    assert host._promotion_eligible is True


def test_omitted_promotion_manifest_still_defaults_to_ineligible() -> None:
    # Documents the default, deliberately-fail-closed behavior for a caller
    # (like the eval-suite matrix runner) that has no manifest to give --
    # distinct from finding 1's bug, which was that a caller that *does* have
    # one (the ack-ordering smoke) could never get it applied.
    host = build_session_for_run(Config())
    assert host._promotion_eligible is False


def test_tts_bound_at_construction_is_not_stale() -> None:
    class _StubTTS:
        on_event = None

    tts = _StubTTS()
    host = build_session_for_run(Config(), tts=tts)
    assert host.tts is tts
    # SessionHost derives _tts_on_event from the constructor's tts argument
    # only -- a post-hoc `host.tts = ...` reassignment leaves this stale.
    # Passing tts= through the constructor keyword must not exhibit that.
    assert host._tts_on_event is tts.on_event


def test_stt_bound_at_construction() -> None:
    stt = object()
    host = build_session_for_run(Config(), stt=stt)
    assert host.stt is stt


def test_stt_tts_default_to_none() -> None:
    host = build_session_for_run(Config())
    assert host.stt is None
    assert host.tts is None


def test_router_kwarg_is_forwarded(monkeypatch) -> None:
    """Regression for round 9 gauntlet, Architecture lens finding 19:
    ``router=`` was silently narrowed out of this wrapper's signature
    relative to ``build_session_host``'s own -- confirm it's actually
    forwarded, by identity, rather than a fresh router being built.
    """

    captured: dict[str, object] = {}
    real_build = composition_module.build_session_host

    def _recording_build(config, **kwargs):
        captured.update(kwargs)
        return real_build(config, **kwargs)

    monkeypatch.setattr(eval_common_module, "build_session_host", _recording_build)
    sentinel_router = object()

    build_session_for_run(Config(), router=sentinel_router)

    assert captured["router"] is sentinel_router


class TestRedactEmptyAfterStripCredential:
    """Regression for round 9 gauntlet, Logic lens finding 5 + Security lens
    finding 4: an unstripped-truthy-but-stripped-empty (or short) credential
    must not be used as a ``str.replace`` needle -- an empty needle shreds
    the entire output by inserting the replacement between every character.
    """

    def test_all_control_char_credential_does_not_shred_output(self) -> None:
        # Strips to "" -- the old `if credential:` guard tested the
        # unstripped (truthy) value and would have shredded this.
        credential = "\x00\x01\x02"
        text = "some perfectly normal log line"
        assert _redact(text, credential) == text

    def test_short_stripped_credential_below_the_floor_is_not_redacted(self) -> None:
        # Stripped length 4, below the 8-char floor -- too short to safely
        # use as an exact-substring needle without risking coincidental
        # matches elsewhere in the text.
        credential = "abcd"
        text = "the word abcd appears here normally"
        assert _redact(text, credential) == text

    def test_real_length_credential_is_still_redacted(self) -> None:
        credential = "a" * 32
        text = f"leaked: {credential} in the output"
        redacted = _redact(text, credential)
        assert credential not in redacted
        assert "***REDACTED***" in redacted


class TestRedactZeroWidthCharacters:
    """Regression for round 9 gauntlet, Security lens finding 12: a
    credential with an embedded zero-width/invisible character must still be
    fully redacted, and legitimate text containing these characters is
    otherwise just stripped (matching the existing control-char contract),
    not treated specially.
    """

    @pytest.mark.parametrize(
        "invisible_char",
        ["­", "\u200b", "‌", "‍", "‎", "‏", "⁠", "﻿"],
    )
    def test_credential_with_embedded_zero_width_char_is_fully_redacted(
        self, invisible_char: str
    ) -> None:
        raw_credential = "sk-testcredential1234567890"
        credential_with_zw = raw_credential[:10] + invisible_char + raw_credential[10:]
        text = f"error talking to provider: {credential_with_zw}"
        redacted = _redact(text, credential_with_zw)
        assert raw_credential[:10] not in redacted or "***REDACTED***" in redacted
        assert invisible_char not in redacted


class TestConfinedOutputPathDenylist:
    """Regression for round 9 gauntlet, Security lens findings 10 and 11."""

    def test_mixed_case_git_directory_is_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            confined_output_path(".GIT/hooks/pre-commit", allowed_root=tmp_path)

    def test_lowercase_git_directory_is_still_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            confined_output_path(".git/config", allowed_root=tmp_path)

    def test_github_directory_is_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            confined_output_path(".github/workflows/pwn.yml", allowed_root=tmp_path)

    def test_mixed_case_github_directory_is_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            confined_output_path(".GitHub/workflows/pwn.yml", allowed_root=tmp_path)

    def test_ordinary_path_is_still_accepted(self, tmp_path: Path) -> None:
        resolved = confined_output_path("reports/out.json", allowed_root=tmp_path)
        assert resolved == (tmp_path / "reports/out.json").resolve()


class TestTurnCorrelatedRoutingAction:
    """Regression for round 9 gauntlet, Architecture lens finding 17: shared
    implementation between scripts/eval_model_comparison.py and
    scripts/smoke_conversation.py.
    """

    def test_matching_turn_id_returns_the_action(self) -> None:
        routing = SimpleNamespace(action="existing_worker", turn_id="turn-1")
        assert turn_correlated_routing_action(routing, "turn-1") == "existing_worker"

    def test_stale_turn_id_returns_none(self) -> None:
        routing = SimpleNamespace(action="direct", turn_id="turn-1")
        assert turn_correlated_routing_action(routing, "turn-2") is None

    def test_none_routing_returns_none(self) -> None:
        assert turn_correlated_routing_action(None, "turn-1") is None


class TestBuildJudgeLlmServicePinsReasoningEffort:
    """Regression: a live --full-matrix run's judge calls all returned
    'judge returned empty response' -- gpt-5-mini (a reasoning model) had no
    reasoning_effort override, so EvalJudge's 200-token completion cap could
    be exhausted by hidden reasoning tokens before any visible JSON verdict
    was emitted. build_judge_llm_service() now pins a gpt-5* judge model to
    minimal reasoning effort via Settings.extra, the only way to set it on
    pipecat's Chat Completions path (see the function's own docstring).
    """

    def test_gpt5_judge_model_gets_minimal_reasoning_effort(self) -> None:
        service = build_judge_llm_service("gpt-5-mini", api_key="sk-test")
        assert service._settings.extra == {"reasoning_effort": "minimal"}

    def test_non_gpt5_judge_model_gets_no_reasoning_effort_override(self) -> None:
        service = build_judge_llm_service("gpt-4.1-mini", api_key="sk-test")
        assert service._settings.extra == {}

    def test_credential_is_still_threaded_through(self) -> None:
        service = build_judge_llm_service("gpt-5-mini", api_key="sk-test-credential")
        assert service._client.api_key == "sk-test-credential"


class TestBuildJudgeRequestKwargs:
    """Round 10 gauntlet, Logic finding 3: build_judge_request_kwargs() is
    the single source of truth for the judge request shape, shared with
    build_judge_llm_service() via judge_extra_kwargs(). Previously
    scripts/verify_eval_candidates.py hand-listed its own kwargs, which could
    silently drift from what build_judge_llm_service() actually sends.
    """

    def test_default_max_completion_tokens_is_judge_max_tokens(self) -> None:
        kwargs = build_judge_request_kwargs(
            "gpt-4.1-mini", messages=[{"role": "user", "content": "hi"}]
        )
        assert kwargs["max_completion_tokens"] == JUDGE_MAX_TOKENS

    def test_explicit_max_completion_tokens_overrides_the_default(self) -> None:
        kwargs = build_judge_request_kwargs(
            "gpt-5-mini", messages=[{"role": "user", "content": "hi"}], max_completion_tokens=16
        )
        assert kwargs["max_completion_tokens"] == 16

    def test_gpt5_model_carries_reasoning_effort(self) -> None:
        kwargs = build_judge_request_kwargs(
            "gpt-5-mini", messages=[{"role": "user", "content": "hi"}]
        )
        assert kwargs["reasoning_effort"] == "minimal"

    def test_non_gpt5_model_has_no_reasoning_effort_key(self) -> None:
        kwargs = build_judge_request_kwargs(
            "gpt-4.1-mini", messages=[{"role": "user", "content": "hi"}]
        )
        assert "reasoning_effort" not in kwargs

    def test_parity_with_build_judge_llm_service(self) -> None:
        """The actual regression guard: the probe's declared shape and the
        real judge client's request shape must never disagree."""
        for model in ("gpt-5-mini", "gpt-4.1-mini"):
            probe_kwargs = build_judge_request_kwargs(
                model, messages=[{"role": "user", "content": "hi"}]
            )
            service = build_judge_llm_service(model, "sk-test")
            assert probe_kwargs.get("reasoning_effort") == service._settings.extra.get(
                "reasoning_effort"
            )


class TestJudgeEffortDecoupledFromRouterPolicy:
    """Round 10 gauntlet, Architecture finding 4: the judge's reasoning
    effort must not move when the router's own resolved-config policy is
    retuned -- it shares only the gpt-5* naming rule, not the router's
    contract.
    """

    def test_retuning_router_policy_does_not_move_the_judge(self, monkeypatch) -> None:
        # Patched on eval_common_module, not server.router: build_judge_llm_service()
        # no longer calls effective_router_reasoning_effort at all (it calls
        # default_reasoning_effort_for_model via judge_extra_kwargs), so this
        # proves the decoupling regardless of which name is patched.
        monkeypatch.setattr(
            eval_common_module,
            "effective_router_reasoning_effort",
            lambda model, effort: "high",
        )

        service = build_judge_llm_service("gpt-5-mini", "sk-test")

        assert service._settings.extra["reasoning_effort"] == "minimal"


class TestShippedConfigHasAnEvalCandidateCell:
    """Round 10 gauntlet confirming pass, Architecture finding: ROUTER_BASELINE/
    WORKER_BASELINE are a fixed historical anchor, not "whatever config.toml
    ships today" -- those diverged when config.toml's [models] defaults moved
    to the luna-medium/terra-medium shortlist. Nothing previously asserted the
    two stay related, so a future default change could silently leave the
    comparison matrix without a cell for what's actually in production: a
    candidate that regresses against *current* production would read as a
    pass, since the matrix would only be measuring against the stale
    baseline and older candidates.

    Round-4 restart, Architecture finding 2: this is now the ENABLING
    invariant for scripts/eval_model_comparison.py's default sweep too --
    default_sweep_pairs() no longer runs a live shipped x shipped cell;
    instead it relies on this test's guarantee that a one-role-varied cell
    for each shipped candidate already exists, and build_report() annotates
    those cells by name. If this test starts failing, that annotation would
    silently name nothing rather than loudly erroring (see
    tests/test_eval_model_comparison.py's TestDefaultSweepAnchorsOnShippedConfig
    for the annotation's own coverage).

    Round 9 gauntlet, Architecture F4 -- considered and declined: this test
    makes CI go red on a config.toml router/worker change unless
    ROUTER_CANDIDATES/WORKER_CANDIDATES (eval_common.py) is updated to match,
    which inverts the usual "eval tooling depends on production config"
    direction. That coupling is the INTENDED tripwire, not an accidental
    inversion -- shipping a production router/worker the comparison matrix
    cannot measure is exactly the condition this test exists to make loud.
    Now that F1 (round 9) makes `unmatched_roles` a true registry-gap
    detector rather than an inference from this run's `pairs`, a later round
    may choose to demote this to a runner pre-flight warning instead of a
    repo-wide failing test -- but that would be a deliberate loosening of a
    CI guard, not a cleanup, and should be a decision made on its own merits
    rather than folded into an unrelated round.
    """

    def test_shipped_router_and_worker_models_have_a_candidate_cell(self) -> None:
        from server.config import load_config

        repo_config_toml = Path(__file__).resolve().parents[1] / "config.toml"
        config = load_config(env={}, config_file=repo_config_toml)

        shipped_router = (
            config.resolve_router_model("fast"),
            config.resolve_router_reasoning_effort("fast"),
        )
        shipped_worker = (
            config.resolve_worker_model("deep"),
            config.resolve_worker_reasoning_effort("deep"),
        )

        router_pairs = {
            (c.model, c.effort)
            for c in (eval_common_module.ROUTER_BASELINE, *eval_common_module.ROUTER_CANDIDATES)
        }
        worker_pairs = {
            (c.model, c.effort)
            for c in (eval_common_module.WORKER_BASELINE, *eval_common_module.WORKER_CANDIDATES)
        }

        assert shipped_router in router_pairs, (
            f"config.toml's shipped router (model, effort) {shipped_router} has no "
            "cell in ROUTER_BASELINE/ROUTER_CANDIDATES -- a comparison run no longer "
            "measures against current production; add a candidate for it."
        )
        assert shipped_worker in worker_pairs, (
            f"config.toml's shipped worker (model, effort) {shipped_worker} has no "
            "cell in WORKER_BASELINE/WORKER_CANDIDATES -- a comparison run no longer "
            "measures against current production; add a candidate for it."
        )


class TestShippedCandidatesCarryARegisteredLabel:
    """Round 5 restart2, Architecture A5: shipped_candidates() must return
    Candidates whose `label` is a real registered selector/report-identity
    string, not a synthesized "shipped" label that overloads
    Candidate.label's documented dual contract (CLI selector key + report
    identity, see RunPair.label's docstring in eval_model_comparison.py).
    TestShippedConfigHasAnEvalCandidateCell guarantees a matching registered
    candidate always exists on this checkout, so the "shipped" fallback
    should never fire here -- but .model/.effort must still be the
    config-resolved values, not the matched candidate's own declared values
    (fidelity: config.toml may ship effort="minimal" for a candidate whose
    registry entry declares effort=None)."""

    def test_shipped_candidates_carry_a_registered_selectable_label(self) -> None:
        from server.config import load_config

        repo_config_toml = Path(__file__).resolve().parents[1] / "config.toml"
        config = load_config(env={}, config_file=repo_config_toml)

        router, worker = eval_common_module.shipped_candidates()

        router_labels = {
            c.label
            for c in (eval_common_module.ROUTER_BASELINE, *eval_common_module.ROUTER_CANDIDATES)
        }
        worker_labels = {
            c.label
            for c in (eval_common_module.WORKER_BASELINE, *eval_common_module.WORKER_CANDIDATES)
        }
        assert router.label in router_labels
        assert worker.label in worker_labels

        # Fidelity half of the decision: .model/.effort stay the
        # config-resolved values, not the matched registry candidate's own.
        assert router.model == config.resolve_router_model("fast")
        assert router.effort == config.resolve_router_reasoning_effort("fast")
        assert worker.model == config.resolve_worker_model("deep")
        assert worker.effort == config.resolve_worker_reasoning_effort("deep")


class TestShippedCandidatesAcceptsAnInjectedConfigFile:
    """Round 7 F5 hardcoded load_config(env={}, config_file=REPO_ROOT /
    "config.toml"), coupling this module to the repo layout even for a
    caller (mainly tests) that wants to point it at a different file --
    breaking this module's "config comes in, it is never loaded here"
    convention (cf. build_session_for_run). Round 7 F5's first attempt took
    a pre-resolved Config parameter instead, but round 8's Architecture lens
    found that a live footgun: Config carries no provenance of which layers
    were applied, so nothing stops a future caller from passing a
    process-env-derived Config and silently breaking the env={} anchoring
    invariant this function exists to guarantee. Narrowed to config_file --
    the function still owns calling load_config(env={}, ...) itself, so the
    anchoring invariant cannot be bypassed by construction (round 8 gauntlet,
    Architecture finding 1)."""

    def test_shipped_candidates_reads_the_injected_config_file(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[models]\nrouter_model = "gpt-9-router"\nworker_model = "gpt-9-worker"\n'
        )

        router, worker = eval_common_module.shipped_candidates(config_file)

        expected = load_config(env={}, config_file=config_file)
        assert router.model == expected.resolve_router_model("fast") == "gpt-9-router"
        assert worker.model == expected.resolve_worker_model("deep") == "gpt-9-worker"

    def test_shipped_candidates_defaults_to_the_repo_tracked_config_toml(self) -> None:
        default_result = eval_common_module.shipped_candidates()
        explicit_result = eval_common_module.shipped_candidates(
            eval_common_module.REPO_ROOT / "config.toml"
        )

        assert default_result == explicit_result
