"""Regression tests for scripts/_eval_common.py's build_session_for_run().

Covers gauntlet round-2 finding 1 (promotion_manifest was never threaded
through, silently disabling promotion eligibility for every host this helper
builds) and finding 7 (stt/tts were bound post-hoc instead of at
construction, leaving SessionHost._tts_on_event stale for a caller that
reassigns host.tts after the fact).
"""

from __future__ import annotations

from scripts._eval_common import SAFE_FALLBACKS, build_session_for_run
from server.config import Config, PromotionManifest
from server.pipeline import SAFE_FALLBACK_TEXTS


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
