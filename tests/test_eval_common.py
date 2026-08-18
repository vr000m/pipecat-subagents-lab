"""Regression tests for scripts/eval_common.py's build_session_for_run().

Covers gauntlet round-2 finding 1 (promotion_manifest was never threaded
through, silently disabling promotion eligibility for every host this helper
builds) and finding 7 (stt/tts were bound post-hoc instead of at
construction, leaving SessionHost._tts_on_event stale for a caller that
reassigns host.tts after the fact).
"""

from __future__ import annotations

import server.app as app_module
import server.composition as composition_module
from scripts.eval_common import SAFE_FALLBACKS, build_session_for_run
from server.config import Config, PromotionManifest
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

    import scripts.eval_common as eval_common_module

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
