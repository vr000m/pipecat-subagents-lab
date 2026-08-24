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
from typing import Any

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
    close_judge_llm_service,
    close_session_provider_clients,
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


class TestCloseJudgeLlmService:
    """Round 10 gauntlet, Logic finding 2: close_judge_llm_service() is the
    single close authority for a build_judge_llm_service() result's
    AsyncOpenAI/httpx pool, shared between run_cell() and
    verify_eval_candidates.probe_judge().
    """

    def test_none_service_is_a_noop(self) -> None:
        import asyncio

        asyncio.run(close_judge_llm_service(None))  # must not raise

    def test_service_with_no_client_attribute_is_a_noop(self) -> None:
        import asyncio

        asyncio.run(close_judge_llm_service(SimpleNamespace()))  # must not raise

    def test_real_service_client_is_closed(self) -> None:
        import asyncio

        service = build_judge_llm_service("gpt-5-mini", api_key="sk-test")
        asyncio.run(close_judge_llm_service(service))
        assert service._client.is_closed()

    def test_sync_close_is_not_awaited(self) -> None:
        """Round-3 restart gauntlet, Logic finding: the helper did a bare
        ``await close()``.

        A provider exposing a *synchronous* ``close()`` -- the shape the
        sibling ``close_session_provider_clients._close_hop`` already handles
        for the sync router client -- returns ``None``, so ``await close()``
        raised ``TypeError: object NoneType can't be used in 'await'
        expression``. Because this runs in ``run_cell``'s ``finally``, that
        TypeError replaced an otherwise-successful cell result and rewrote it
        to ``turn-error``, corrupting the matrix over a cleanup detail.
        """
        import asyncio

        client = _SyncRecordingClient()
        service = SimpleNamespace(_client=client)
        asyncio.run(close_judge_llm_service(service))  # must not raise
        assert client.close_calls == 1

    def test_async_close_is_still_awaited(self) -> None:
        """The awaitable probe must not silently *stop* awaiting the async
        client -- that would reinstate the round-10 httpx-pool leak."""
        import asyncio

        client = _RecordingClient()
        service = SimpleNamespace(_client=client)
        asyncio.run(close_judge_llm_service(service))
        assert client.close_calls == 1


class _RecordingClient:
    def __init__(self, *, raise_on_close: bool = False) -> None:
        self.close_calls = 0
        self._raise_on_close = raise_on_close

    async def close(self) -> None:
        self.close_calls += 1
        if self._raise_on_close:
            raise RuntimeError("client close exploded")


class _SyncRecordingClient:
    """A sync ``OpenAI.close()`` stand-in -- close_session_provider_clients()
    must handle both the async worker client and the sync router client
    through the same probe."""

    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


def _fake_provider_host(*, worker_client: Any = None, router_client: Any = None) -> SimpleNamespace:
    registry = SimpleNamespace(responses=SimpleNamespace(_client=worker_client))
    call = SimpleNamespace(_responses=SimpleNamespace(_client=router_client))
    router = SimpleNamespace(_call=call)
    coordinator = SimpleNamespace(router=router)
    return SimpleNamespace(registry=registry, coordinator=coordinator)


class TestCloseSessionProviderClients:
    """Round 11 gauntlet, Codex F1: run_cell() closed the judge client
    (round 10 F2) but never the router/worker Responses clients
    build_session_for_run() constructs per cell -- SessionHost.shutdown()
    never touches them either. close_session_provider_clients() is the
    single close authority for both, reached through the same private-
    attribute convention close_judge_llm_service() already documents.
    """

    def test_none_host_is_a_noop(self) -> None:
        import asyncio

        asyncio.run(close_session_provider_clients(None))  # must not raise

    def test_host_missing_registry_and_coordinator_is_a_noop(self) -> None:
        import asyncio

        asyncio.run(close_session_provider_clients(SimpleNamespace()))  # must not raise

    def test_worker_client_still_none_is_a_noop(self) -> None:
        import asyncio

        # registry.responses is None until a worker is actually built --
        # must not raise reaching for ._client on a None responses object.
        host = _fake_provider_host()
        asyncio.run(close_session_provider_clients(host))

    def test_router_with_a_non_lazy_call_is_a_noop_for_the_router_hop(self) -> None:
        import asyncio

        # Many eval/smoke tests inject a plain callable (not a
        # LazyRouterProvider) as Router._call -- it has no ._responses hop.
        worker_client = _RecordingClient()
        registry = SimpleNamespace(responses=SimpleNamespace(_client=worker_client))
        router = SimpleNamespace(_call=lambda prompt: {})
        coordinator = SimpleNamespace(router=router)
        host = SimpleNamespace(registry=registry, coordinator=coordinator)

        asyncio.run(close_session_provider_clients(host))

        assert worker_client.close_calls == 1

    def test_both_worker_and_router_clients_are_closed_exactly_once(self) -> None:
        import asyncio

        worker_client = _RecordingClient()
        router_client = _SyncRecordingClient()
        host = _fake_provider_host(worker_client=worker_client, router_client=router_client)

        asyncio.run(close_session_provider_clients(host))

        assert worker_client.close_calls == 1
        assert router_client.close_calls == 1

    def test_a_router_close_failure_propagates_to_the_caller(self) -> None:
        import asyncio

        # close_session_provider_clients() itself does not swallow a close
        # failure -- run_cell()'s finally block is responsible for the
        # never-mask-the-outcome guard around this call, not this helper.
        router_client = _RecordingClient(raise_on_close=True)
        host = _fake_provider_host(router_client=router_client)

        with pytest.raises(RuntimeError, match="client close exploded"):
            asyncio.run(close_session_provider_clients(host))


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


class TestAllCandidateRegistries:
    """Round 10 gauntlet, Architecture finding 4: ALL_ROUTER_CANDIDATES/
    ALL_WORKER_CANDIDATES are the single named abstraction for "the full
    candidate registry, baseline included" -- previously re-spelled as
    `(ROUTER_BASELINE, *ROUTER_CANDIDATES)`/`(WORKER_BASELINE,
    *WORKER_CANDIDATES)` at 5 production call sites across 3 modules.
    """

    def test_registries_match_the_selection_maps(self) -> None:
        from scripts.eval_model_comparison import (
            ROUTER_SELECTABLE_BY_LABEL,
            WORKER_SELECTABLE_BY_LABEL,
        )

        assert {c.label for c in eval_common_module.ALL_ROUTER_CANDIDATES} == set(
            ROUTER_SELECTABLE_BY_LABEL
        )
        assert {c.label for c in eval_common_module.ALL_WORKER_CANDIDATES} == set(
            WORKER_SELECTABLE_BY_LABEL
        )

    def test_no_module_under_scripts_re_spells_the_registry_tuple(self) -> None:
        """Source-introspection guard, same idiom as
        tests/test_justfile_ci_parity.py -- without this, the set-equality
        test above is tautological: nothing stops a new call site from
        re-spelling `(ROUTER_BASELINE, *ROUTER_CANDIDATES)` instead of using
        the constant, and both would still agree by construction.
        """
        import re

        repo_root = Path(__file__).resolve().parents[1]
        scripts_dir = repo_root / "scripts"
        pattern = re.compile(r"\(ROUTER_BASELINE,\s*\*|\(WORKER_BASELINE,\s*\*")
        offenders: list[str] = []
        for path in sorted(scripts_dir.glob("*.py")):
            if path.name == "eval_common.py":
                # The one place these are legitimately spelled out: the
                # ALL_ROUTER_CANDIDATES/ALL_WORKER_CANDIDATES definitions
                # themselves.
                continue
            text = path.read_text(encoding="utf-8")
            if pattern.search(text):
                offenders.append(path.name)
        assert offenders == []


class TestCandidateWireKey:
    """Round 11 gauntlet, Architecture finding 5: candidate_wire_key() moved
    here from scripts/eval_model_comparison.py (as the private
    `_candidate_wire_key`) and is now the shared, exported single source
    for "same candidate" at the wire level.
    """

    def test_unset_and_explicit_minimal_router_effort_collapse_to_the_same_key(self) -> None:
        # The round-4 restart collision this function exists to prevent:
        # ("gpt-5-mini", None) and ("gpt-5-mini", "minimal") are one wire
        # request under two spellings for the router role.
        from scripts.eval_common import Candidate, candidate_wire_key
        from server.router import effective_router_reasoning_effort

        model = "gpt-5-mini"
        minimal = effective_router_reasoning_effort(model, None)
        assert minimal == "minimal"

        unset = Candidate(label="a", role="router", model=model, effort=None)
        explicit_minimal = Candidate(label="b", role="router", model=model, effort=minimal)

        assert candidate_wire_key(unset) == candidate_wire_key(explicit_minimal)

    def test_different_models_never_collide(self) -> None:
        from scripts.eval_common import Candidate, candidate_wire_key

        a = Candidate(label="a", role="worker", model="gpt-5.6-sol", effort="low")
        b = Candidate(label="b", role="worker", model="gpt-5.6-luna", effort="low")

        assert candidate_wire_key(a) != candidate_wire_key(b)
