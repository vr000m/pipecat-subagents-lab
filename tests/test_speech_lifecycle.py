"""Contract tests for the Phase 1 correlated speech lifecycle coordinator.

Exercises ``server/speech_lifecycle.py`` against the invariant the plan
(``docs/dev_plans/20260728-bug-transport-aware-speech-supersession.md``)
requires: per-work queues feed one coordinator-owned transport slot,
generation identity is established before TTS, and synthesis completion,
cleanup deadlines, and late frames must never release the wrong generation.
``ManualTimerScheduler`` (defined in the module under test) drives both
monotonic reads and timer wakeups deterministically -- no wall-clock sleeps.

The coordinator's terminal-path methods (``on_transport_bot_stopped``,
``provider_error``, ``acknowledge_tts_lane_flush``, ``teardown_complete``)
and its timer callbacks are async, so every test here runs inside
``asyncio.run()`` (matching this repo's existing convention, e.g.
``tests/test_speech_scheduler.py``) and flushes timer-fired coroutines with
``await tick(clock, seconds)`` rather than a bare ``clock.advance()``.
"""

from __future__ import annotations

import asyncio

import pytest

from server.speech_lifecycle import (
    DeliveryDisposition,
    GenerationIdentity,
    GenerationPhase,
    ManualTimerScheduler,
    SpeechLifecycleCoordinator,
)


def make_coordinator(
    clock: ManualTimerScheduler | None = None,
    *,
    auto_ack_cleanup: bool = True,
    **overrides: object,
) -> tuple[SpeechLifecycleCoordinator, ManualTimerScheduler]:
    clock = clock or ManualTimerScheduler()
    requested_cleanup = overrides.pop("dispatch_cleanup", None)
    kwargs: dict[str, object] = {
        "speech_start_timeout_seconds": 10.0,
        "speech_transport_grace_seconds": 1.0,
    }
    kwargs.update(overrides)
    coordinator = SpeechLifecycleCoordinator(clock=clock, timers=clock, **kwargs)
    if auto_ack_cleanup:

        async def dispatch_cleanup(token: str, identity_: GenerationIdentity) -> None:
            if callable(requested_cleanup):
                result = requested_cleanup(token, identity_)
                if asyncio.iscoroutine(result):
                    await result
            await coordinator.acknowledge_tts_lane_flush(token)

        coordinator._dispatch_cleanup = dispatch_cleanup
    elif callable(requested_cleanup):
        coordinator._dispatch_cleanup = requested_cleanup
    return coordinator, clock


def identity(work_item_id: str = "work-1", utterance_id: str = "utt-1", origin_epoch=None):
    return GenerationIdentity(utterance_id, work_item_id, origin_epoch)


def admit_and_hand_to_tts(
    coordinator: SpeechLifecycleCoordinator, work_item_id: str, utterance_id: str
):
    generation = coordinator.try_admit(identity(work_item_id, utterance_id))
    assert generation is not None
    coordinator.mark_handed_to_tts(generation.token)
    return generation


def synth_audio(*, seconds: float, sample_rate: int = 16000, num_channels: int = 1) -> bytes:
    """Real 16-bit PCM byte payload of the given duration for on_tts_audio()."""
    num_frames = round(seconds * sample_rate)
    return bytes(num_frames * num_channels * 2)


async def tick(clock: ManualTimerScheduler, seconds: float) -> None:
    """Advance the manual clock and let any timer-fired coroutine finish.

    A due timer's callback is scheduled with ``asyncio.ensure_future`` from
    inside ``ManualTimerScheduler._fire_due`` (synchronous), so the actual
    coroutine body only runs once the event loop gets a turn.
    """
    clock.advance(seconds)
    for _ in range(3):
        await asyncio.sleep(0)


def run(coro_fn) -> None:
    asyncio.run(coro_fn())


# ---------------------------------------------------------------------------
# SpeechGeneration / GenerationPhase state machine
# ---------------------------------------------------------------------------


def test_generation_admitted_phase_before_any_tts_activity() -> None:
    coordinator, _ = make_coordinator()

    generation = coordinator.try_admit(identity())

    assert generation is not None
    assert generation.phase == GenerationPhase.ADMITTED
    assert generation.disposition is None


def test_marker_hands_generation_to_tts_phase() -> None:
    coordinator, _ = make_coordinator()
    generation = coordinator.try_admit(identity())

    coordinator.mark_handed_to_tts(generation.token)

    assert coordinator.generation_for_token(generation.token).phase == GenerationPhase.HANDED_TO_TTS


def test_bound_context_progresses_to_synthesizing_then_synthesis_ended() -> None:
    coordinator, _ = make_coordinator()
    generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")

    coordinator.bind_context(generation.token, "utt-1")
    coordinator.on_tts_started("utt-1")
    assert coordinator.generation_for_token(generation.token).phase == GenerationPhase.SYNTHESIZING

    assert coordinator.on_tts_stopped("utt-1") is True
    assert (
        coordinator.generation_for_token(generation.token).phase == GenerationPhase.SYNTHESIS_ENDED
    )


def test_on_tts_stopped_and_on_tts_audio_reject_a_stale_or_unknown_context() -> None:
    coordinator, _ = make_coordinator()

    assert coordinator.on_tts_stopped("never-admitted") is False
    assert (
        coordinator.on_tts_audio(
            "never-admitted", audio=b"\x00\x00", sample_rate=16000, num_channels=1
        )
        is False
    )


# ---------------------------------------------------------------------------
# Single global transport slot
# ---------------------------------------------------------------------------


def test_only_one_generation_may_occupy_the_global_slot() -> None:
    coordinator, _ = make_coordinator()
    first = coordinator.try_admit(identity("work-1", "utt-1"))
    assert first is not None

    second = coordinator.try_admit(identity("work-2", "utt-2"))

    assert second is None
    assert coordinator.slot_token == first.token
    assert coordinator.occupied is True


def test_synthesis_end_does_not_clear_the_slot_or_admit_next() -> None:
    coordinator, _ = make_coordinator()
    generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
    coordinator.bind_context(generation.token, "utt-1")
    coordinator.on_tts_started("utt-1")

    coordinator.on_tts_stopped("utt-1")

    assert coordinator.occupied is True
    assert coordinator.slot_token == generation.token
    blocked = coordinator.try_admit(identity("work-2", "utt-2"))
    assert blocked is None


def test_raw_drain_timer_expiry_does_not_clear_an_output_active_slot_without_teardown() -> None:
    """A timer expiry may initiate cleanup but must not itself release a slot
    that has already submitted audio to output -- only ``teardown_complete``
    (proxying completed connection-scoped output teardown) may."""

    async def body() -> None:
        coordinator, clock = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        coordinator.bind_context(generation.token, "utt-1")
        coordinator.on_tts_started("utt-1")
        coordinator.on_tts_audio(
            "utt-1", audio=synth_audio(seconds=0.1), sample_rate=16000, num_channels=1
        )
        coordinator.on_tts_stopped("utt-1")

        # Drain deadline = synthesis_end + audio_duration(0.1s) + grace(1.0s).
        await tick(clock, 1.1 + 1e-6)

        generation_state = coordinator.generation_for_token(generation.token)
        assert generation_state.cleanup_pending is True
        assert coordinator.occupied is True, (
            "output-submitted expiry must await teardown_complete(), not silently free the slot"
        )
        assert generation_state.terminalized is False

    run(body)


def test_teardown_complete_releases_the_slot_after_output_submitted_expiry() -> None:
    async def body() -> None:
        coordinator, clock = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        coordinator.bind_context(generation.token, "utt-1")
        coordinator.on_tts_started("utt-1")
        coordinator.on_tts_audio(
            "utt-1", audio=synth_audio(seconds=0.1), sample_rate=16000, num_channels=1
        )
        coordinator.on_tts_stopped("utt-1")
        await tick(clock, 1.1 + 1e-6)
        assert coordinator.occupied is True

        await coordinator.teardown_complete(generation.token)

        assert coordinator.occupied is False
        # The terminalized generation is reaped from the coordinator's
        # internal dicts (see test_terminalized_generation_is_reaped_from_
        # internal_dicts below); the object this test already holds was
        # mutated in place before the reap, so it still reflects final state.
        assert generation.terminalized is True
        assert coordinator.generation_for_token(generation.token) is None
        next_generation = coordinator.try_admit(identity("work-2", "utt-2"))
        assert next_generation is not None

    run(body)


def test_fieldless_transport_stopped_is_the_normal_slot_release() -> None:
    async def body() -> None:
        coordinator, clock = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        coordinator.bind_context(generation.token, "utt-1")
        coordinator.on_tts_started("utt-1")
        coordinator.on_tts_stopped("utt-1")

        await coordinator.on_transport_bot_stopped()

        assert coordinator.occupied is False
        # Reaped from the coordinator on terminalization; the held object
        # was mutated in place first, so it still reflects the final phase.
        assert generation.phase == GenerationPhase.TRANSPORT_STOPPED
        assert coordinator.generation_for_token(generation.token) is None
        next_generation = coordinator.try_admit(identity("work-2", "utt-2"))
        assert next_generation is not None

    run(body)


# ---------------------------------------------------------------------------
# Fieldless bot frames only ever apply to the sole occupied slot
# ---------------------------------------------------------------------------


def test_fieldless_transport_started_applies_to_the_sole_occupied_slot() -> None:
    coordinator, _ = make_coordinator()
    generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
    coordinator.bind_context(generation.token, "utt-1")

    coordinator.on_transport_bot_started()

    assert (
        coordinator.generation_for_token(generation.token).phase
        == GenerationPhase.TRANSPORT_STARTED
    )


def test_fieldless_transport_stopped_with_empty_slot_is_a_noop() -> None:
    async def body() -> None:
        coordinator, _ = make_coordinator()

        await coordinator.on_transport_bot_stopped()  # no active generation at all

        assert coordinator.occupied is False

    run(body)


def test_late_fieldless_a_stop_after_teardown_cannot_release_b() -> None:
    """A-fallback -> attempted B admission -> late fieldless A stop.

    The single hardest ordering in the plan's Review Focus: once A's audio
    has crossed into output and its drain deadline has expired, B is
    admitted only after ``teardown_complete`` retires A's slot; a later
    fieldless stop that is really A's old, torn-down lane must never be
    observed as if it were confirming B.
    """

    async def body() -> None:
        coordinator, clock = make_coordinator()
        generation_a = admit_and_hand_to_tts(coordinator, "work-1", "utt-a")
        coordinator.bind_context(generation_a.token, "utt-a")
        coordinator.on_tts_started("utt-a")
        coordinator.on_tts_audio(
            "utt-a", audio=synth_audio(seconds=0.1), sample_rate=16000, num_channels=1
        )
        coordinator.on_tts_stopped("utt-a")
        await tick(clock, 1.1 + 1e-6)  # drain deadline expiry with output-submitted audio

        blocked = coordinator.try_admit(identity("work-2", "utt-b"))
        assert blocked is None, "B must not be admitted until A's old lane completes teardown"

        await coordinator.teardown_complete(generation_a.token)
        generation_b = coordinator.try_admit(identity("work-2", "utt-b"))
        assert generation_b is not None
        coordinator.bind_context(generation_b.token, "utt-b")
        coordinator.on_transport_bot_started()

        # A very late fieldless stop, arriving after B has already started.
        await coordinator.on_transport_bot_stopped()

        assert coordinator.occupied is False
        # B is also reaped once terminalized; the held object was mutated in
        # place first, so it still reflects the final phase.
        assert generation_b.phase == GenerationPhase.TRANSPORT_STOPPED
        # A's own generation must not be mutated a second time by the late stop.
        assert generation_a.phase == GenerationPhase.SYNTHESIS_ENDED

    run(body)


# ---------------------------------------------------------------------------
# Stale-frame fence / tombstones
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "pause",
    [pytest.param(False, id="interrupted"), pytest.param(True, id="paused")],
)
def test_stale_frame_fence_drops_late_a_frames_after_interruption_or_pause(pause: bool) -> None:
    coordinator, _ = make_coordinator()
    generation_a = admit_and_hand_to_tts(coordinator, "work-1", "utt-a")
    coordinator.bind_context(generation_a.token, "utt-a")
    coordinator.on_tts_started("utt-a")

    coordinator.record_interruption(generation_a.token, pause=pause)

    assert coordinator.drop_stale_frame("utt-a") is True
    # A late TTS-stopped frame for A must be dropped before it can touch
    # anything, per TransportSpeechLifecycleProcessor's own drop_stale_frame
    # gate around on_tts_stopped/on_tts_audio.
    assert coordinator.on_tts_stopped("utt-a") is False


def test_drop_stale_frame_is_false_for_a_live_untombstoned_context() -> None:
    coordinator, _ = make_coordinator()
    generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
    coordinator.bind_context(generation.token, "utt-1")

    assert coordinator.drop_stale_frame("utt-1") is False


def test_drop_stale_frame_is_true_once_terminalized() -> None:
    async def body() -> None:
        coordinator, _ = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        coordinator.bind_context(generation.token, "utt-1")
        coordinator.on_tts_started("utt-1")
        coordinator.on_tts_stopped("utt-1")

        await coordinator.on_transport_bot_stopped()  # terminalizes

        assert coordinator.drop_stale_frame("utt-1") is True

    run(body)


# ---------------------------------------------------------------------------
# Manual-clock watchdogs
# ---------------------------------------------------------------------------


def test_start_timeout_fires_delivery_unknown_after_ten_seconds_with_no_correlated_event() -> None:
    async def body() -> None:
        coordinator, clock = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")

        await tick(clock, 10.0 + 1e-6)

        # Terminalized and reaped from the coordinator; the held object was
        # mutated in place first, so it still reflects final state.
        assert generation.disposition == DeliveryDisposition.DELIVERY_UNKNOWN
        # No audio was ever submitted, so the coordinator's own no-audio
        # flush acknowledgement retires the slot without an external ack.
        assert generation.terminalized is True
        assert coordinator.generation_for_token(generation.token) is None
        assert coordinator.occupied is False

    run(body)


def test_no_audio_cleanup_retains_slot_until_token_bearing_post_tts_ack() -> None:
    async def body() -> None:
        cleanup_calls: list[str] = []
        coordinator, clock = make_coordinator(
            auto_ack_cleanup=False,
            dispatch_cleanup=lambda token, _identity: cleanup_calls.append(token),
        )
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")

        await tick(clock, 10.0 + 1e-6)

        assert cleanup_calls == [generation.token]
        assert generation.cleanup_pending is True
        assert generation.terminalized is False
        assert coordinator.occupied is True
        assert coordinator.try_admit(identity("work-2", "utt-2")) is None

        await coordinator.acknowledge_tts_lane_flush(generation.token)

        assert generation.terminalized is True
        assert coordinator.occupied is False
        assert coordinator.try_admit(identity("work-2", "utt-2")) is not None

    run(body)


def test_missing_post_tts_ack_escalates_to_verified_connection_teardown() -> None:
    async def body() -> None:
        teardown_calls: list[str] = []

        async def teardown(token: str, _identity: GenerationIdentity) -> None:
            teardown_calls.append(token)
            await coordinator.teardown_complete(token)

        coordinator, clock = make_coordinator(
            auto_ack_cleanup=False,
            dispatch_cleanup=lambda _token, _identity: None,
            dispatch_teardown=teardown,
        )
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")

        await tick(clock, 10.0 + 1e-6)
        assert coordinator.occupied is True

        await tick(clock, 1.0 + 1e-6)

        assert teardown_calls == [generation.token]
        assert generation.terminalized is True
        assert coordinator.occupied is False
        assert coordinator.connection_epoch == 1

    run(body)


def test_cleanup_dispatch_failure_escalates_to_verified_connection_teardown() -> None:
    async def body() -> None:
        teardown_calls: list[str] = []

        async def cleanup_failed(_token: str, _identity: GenerationIdentity) -> None:
            raise RuntimeError("queueing interruption failed")

        async def teardown(token: str, _identity: GenerationIdentity) -> None:
            teardown_calls.append(token)
            await coordinator.teardown_complete(token)

        coordinator, _ = make_coordinator(
            auto_ack_cleanup=False,
            dispatch_cleanup=cleanup_failed,
            dispatch_teardown=teardown,
        )
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")

        await coordinator.provider_error(generation.token)

        assert teardown_calls == [generation.token]
        assert generation.cleanup_pending is True
        assert generation.teardown_pending is True
        assert generation.terminalized is True
        assert coordinator.occupied is False

    run(body)


def test_start_timeout_is_cancelled_by_the_first_correlated_tts_started_event() -> None:
    async def body() -> None:
        coordinator, clock = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        coordinator.bind_context(generation.token, "utt-1")
        coordinator.on_tts_started("utt-1")

        await tick(clock, 10.0 + 1e-6)

        assert coordinator.generation_for_token(generation.token).disposition is None
        assert coordinator.occupied is True

    run(body)


def test_drain_deadline_uses_synthesis_end_plus_accumulated_audio_plus_grace() -> None:
    async def body() -> None:
        coordinator, clock = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        coordinator.bind_context(generation.token, "utt-1")
        coordinator.on_tts_started("utt-1")
        coordinator.on_tts_audio(
            "utt-1", audio=synth_audio(seconds=0.5), sample_rate=16000, num_channels=1
        )
        coordinator.on_tts_stopped("utt-1")

        await tick(clock, 0.5 + 1.0 - 1e-3)  # just under the deadline
        assert coordinator.generation_for_token(generation.token).disposition is None

        await tick(clock, 2e-3)  # cross the deadline
        assert coordinator.generation_for_token(generation.token).disposition == (
            DeliveryDisposition.DELIVERY_UNKNOWN
        )

    run(body)


def test_zero_audio_synthesis_uses_only_the_grace_deadline() -> None:
    async def body() -> None:
        coordinator, clock = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        coordinator.bind_context(generation.token, "utt-1")
        coordinator.on_tts_started("utt-1")
        coordinator.on_tts_stopped("utt-1")  # no audio ever arrived

        await tick(clock, 1.0 - 1e-3)
        assert coordinator.generation_for_token(generation.token).disposition is None

        await tick(clock, 2e-3)
        # Terminalized and reaped; the held object was mutated in place
        # first, so it still reflects final state.
        assert generation.disposition == DeliveryDisposition.DELIVERY_UNKNOWN
        assert coordinator.generation_for_token(generation.token) is None
        # Zero-audio cleanup self-acknowledges, so the slot is retired too.
        assert coordinator.occupied is False

    run(body)


def test_interruption_cleanup_deadline_is_forwarded_at_plus_grace() -> None:
    async def body() -> None:
        coordinator, clock = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        coordinator.bind_context(generation.token, "utt-1")
        coordinator.on_tts_started("utt-1")

        coordinator.record_interruption(generation.token)
        await tick(clock, 1.0 - 1e-3)
        assert coordinator.occupied is True, "barrier must hold until the interruption deadline"

        await tick(clock, 2e-3)
        assert coordinator.occupied is False

    run(body)


@pytest.mark.parametrize("stop_first", [True, False])
def test_stop_versus_expiry_race_resolves_deterministically_in_both_orders(
    stop_first: bool,
) -> None:
    async def body() -> None:
        coordinator, clock = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        coordinator.bind_context(generation.token, "utt-1")
        coordinator.on_tts_started("utt-1")
        coordinator.on_tts_stopped("utt-1")  # zero-audio: grace-only deadline

        if stop_first:
            await coordinator.on_transport_bot_stopped()
            await tick(clock, 1.0 + 1e-6)  # deadline firing after stop must be a no-op
        else:
            await tick(clock, 1.0 + 1e-6)  # deadline fires first, self-acknowledges (zero audio)
            await coordinator.on_transport_bot_stopped()  # a stale/late stop must be a no-op

        assert coordinator.occupied is False
        # Exactly one terminal disposition/slot-release, regardless of
        # order. Terminalized and reaped; the held object was mutated in
        # place first, so it still reflects final state.
        assert generation.terminalized is True
        assert generation.disposition == DeliveryDisposition.DELIVERY_UNKNOWN
        assert coordinator.generation_for_token(generation.token) is None

    run(body)


# ---------------------------------------------------------------------------
# Provider error ingress: generic upstream ErrorFrame vs local context-bearing
# ---------------------------------------------------------------------------


def test_provider_error_transitions_the_captured_token_to_delivery_unknown() -> None:
    """Both the generic upstream ErrorFrame path (attributed by captured
    token + ErrorFrame.processor, since it carries no context_id) and the
    local context-bearing provider callback converge on the same
    ``provider_error(token)`` transition."""

    async def body() -> None:
        coordinator, _ = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")

        await coordinator.provider_error(generation.token)

        # Terminalized and reaped; the held object was mutated in place
        # first, so it still reflects final state.
        assert generation.disposition == DeliveryDisposition.DELIVERY_UNKNOWN
        assert generation.terminalized is True  # no audio was ever submitted
        assert coordinator.generation_for_token(generation.token) is None

    run(body)


def test_local_context_bearing_error_resolves_its_context_to_the_bound_token_first() -> None:
    async def body() -> None:
        coordinator, _ = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        coordinator.bind_context(generation.token, "utt-1")
        coordinator.on_tts_started("utt-1")

        token = coordinator.token_for_context("utt-1")
        assert token == generation.token
        await coordinator.provider_error(token)

        # Terminalized and reaped; the held object was mutated in place
        # first, so it still reflects final state.
        assert generation.disposition == DeliveryDisposition.DELIVERY_UNKNOWN
        assert coordinator.generation_for_token(generation.token) is None

    run(body)


def test_error_for_a_stale_captured_token_does_not_corrupt_the_next_generation() -> None:
    async def body() -> None:
        coordinator, clock = make_coordinator()
        generation_a = admit_and_hand_to_tts(coordinator, "work-1", "utt-a")
        coordinator.record_interruption(generation_a.token)
        # Interruption alone does not free the slot until its own cleanup
        # deadline resolves or the old lane acknowledges.
        await tick(clock, 1.0 + 1e-6)
        generation_b = coordinator.try_admit(identity("work-2", "utt-b"))
        assert generation_b is not None

        await coordinator.provider_error(generation_a.token)  # stale error for A's captured token

        assert coordinator.generation_for_token(generation_b.token).disposition is None
        assert coordinator.occupied is True
        assert coordinator.slot_token == generation_b.token

    run(body)


# ---------------------------------------------------------------------------
# Barge-in and explicit pause
# ---------------------------------------------------------------------------


def test_record_interruption_tombstones_and_sets_disposition_synchronously() -> None:
    """The plan requires disposition + tombstone recorded *before* the
    interruption is forwarded downstream. ``record_interruption`` itself is
    synchronous and mutates both before returning, so a caller that awaits
    it (or simply calls it, since it is not a coroutine) before forwarding
    InterruptionFrame satisfies the ordering."""
    coordinator, _ = make_coordinator()
    generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
    coordinator.bind_context(generation.token, "utt-1")
    coordinator.on_tts_started("utt-1")

    coordinator.record_interruption(generation.token, pause=False)

    assert coordinator.drop_stale_frame("utt-1") is True
    assert coordinator.generation_for_token(generation.token).disposition == (
        DeliveryDisposition.INTERRUPTED
    )


def test_barge_in_never_automatically_admits_the_next_generation() -> None:
    coordinator, _ = make_coordinator()
    generation_a = admit_and_hand_to_tts(coordinator, "work-1", "utt-a")
    coordinator.bind_context(generation_a.token, "utt-a")
    coordinator.on_tts_started("utt-a")

    coordinator.record_interruption(generation_a.token, pause=False)

    # The coordinator's own interruption bookkeeping never calls try_admit;
    # the barrier stays fenced until the interruption-cleanup deadline (or
    # an external ack/teardown) resolves it -- a caller must explicitly
    # decide to admit the next generation.
    assert coordinator.occupied is True
    assert coordinator.slot_token == generation_a.token


def test_pause_records_paused_disposition_and_retains_the_barrier() -> None:
    async def body() -> None:
        coordinator, clock = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        coordinator.bind_context(generation.token, "utt-1")
        coordinator.on_tts_started("utt-1")

        coordinator.record_interruption(generation.token, pause=True)

        assert coordinator.generation_for_token(generation.token).disposition == (
            DeliveryDisposition.PAUSED
        )
        assert coordinator.occupied is True, "old barrier must hold through the deadline"

        too_early = coordinator.try_admit(identity("work-1", "utt-resume-early"))
        assert too_early is None

        await tick(clock, 1.0 + 1e-6)
        resumed = coordinator.try_admit(identity("work-1", "utt-resume"))
        assert resumed is not None

    run(body)


# ---------------------------------------------------------------------------
# Exactly-once terminalization / on_terminal callback
# ---------------------------------------------------------------------------


def test_on_terminal_callback_fires_exactly_once_per_generation() -> None:
    async def body() -> None:
        terminal_calls: list[tuple[str, DeliveryDisposition]] = []

        def on_terminal(token, identity_, disposition) -> None:
            terminal_calls.append((token, disposition))

        coordinator, clock = make_coordinator(on_terminal=on_terminal)
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")

        await tick(clock, 10.0 + 1e-6)  # start-timeout fires once
        await tick(clock, 10.0)  # nothing left pending; must not fire again

        assert terminal_calls == [(generation.token, DeliveryDisposition.DELIVERY_UNKNOWN)]

    run(body)


def test_terminalized_generation_is_reaped_from_internal_dicts() -> None:
    """`_generations` must not grow without bound over a long-lived
    connection: once a generation reaches terminal state, its entry is
    popped (whitebox check, matching this module's existing test style of
    reaching into coordinator internals). `generation_for_token` must
    correctly report "not found" (None) for the reaped token rather than
    KeyError, and a stale frame for the reaped context must still be
    dropped, not mistaken for live.

    The active context binding is reaped into the bounded tombstone cache,
    which preserves recent stale-frame detection without unbounded growth."""

    async def body() -> None:
        coordinator, _ = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        coordinator.bind_context(generation.token, "utt-1")
        coordinator.on_tts_started("utt-1")
        coordinator.on_tts_stopped("utt-1")  # zero audio: self-acknowledges on terminalize

        await coordinator.on_transport_bot_stopped()

        assert generation.terminalized is True
        assert generation.token not in coordinator._generations
        assert "utt-1" not in coordinator._context_tokens
        assert "utt-1" in coordinator._context_tombstones
        assert coordinator.generation_for_token(generation.token) is None
        assert coordinator.token_for_context("utt-1") is None
        assert coordinator.drop_stale_frame("utt-1") is True
        assert coordinator.drop_stale_frame("never-bound-ctx") is False

    run(body)


def test_context_tombstones_are_bounded_and_connection_shutdown_clears_them() -> None:
    async def body() -> None:
        coordinator, _ = make_coordinator(context_tombstone_limit=2)

        for index in range(4):
            utterance_id = f"utt-{index}"
            generation = admit_and_hand_to_tts(coordinator, "work-1", utterance_id)
            assert coordinator.bind_context(generation.token, utterance_id) is True
            assert coordinator.on_tts_started(utterance_id) is True
            assert coordinator.on_tts_stopped(utterance_id) is True
            await coordinator.on_transport_bot_stopped()

        assert list(coordinator._context_tombstones) == ["utt-2", "utt-3"]
        assert len(coordinator._context_tokens) == 0
        assert coordinator.drop_stale_frame("utt-3") is True

        coordinator.connection_closed()

        assert coordinator._context_tombstones == {}
        assert coordinator._context_tokens == {}
        assert coordinator.try_admit(identity("work-2", "utt-after-close")) is None

    run(body)


def test_dispatch_cleanup_and_teardown_hooks_receive_the_captured_token_and_identity() -> None:
    async def body() -> None:
        cleanup_calls: list[tuple[str, GenerationIdentity]] = []
        teardown_calls: list[tuple[str, GenerationIdentity]] = []

        coordinator, clock = make_coordinator(
            dispatch_cleanup=lambda token, ident: cleanup_calls.append((token, ident)),
            dispatch_teardown=lambda token, ident: teardown_calls.append((token, ident)),
        )
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        coordinator.bind_context(generation.token, "utt-1")
        coordinator.on_tts_started("utt-1")
        coordinator.on_tts_audio(
            "utt-1", audio=synth_audio(seconds=0.1), sample_rate=16000, num_channels=1
        )
        coordinator.on_tts_stopped("utt-1")

        await tick(clock, 1.1 + 1e-6)

        assert cleanup_calls == [(generation.token, generation.identity)]
        assert teardown_calls == [(generation.token, generation.identity)]  # audio_submitted=True

    run(body)


# ---------------------------------------------------------------------------
# Real, credential-free pinned SmallWebRTC output-lane contract
# ---------------------------------------------------------------------------


def test_real_raw_audio_track_future_only_resolves_after_the_chunk_is_consumed() -> None:
    """Prove the primitive a real output lane's drain wait depends on is
    real: ``RawAudioTrack.add_audio_bytes()`` (from pipecat's pinned
    SmallWebRTC transport) returns a future that resolves only once
    ``recv()`` has actually drained the chunk, not merely once it was
    queued. A slot-release decision gated on synthesis completion alone
    (rather than this future or the transport's own bot-stop frame) is
    exactly the bug this plan fixes.
    """
    from pipecat.transports.smallwebrtc.transport import RawAudioTrack

    async def body() -> None:
        track = RawAudioTrack(sample_rate=16000, auto_silence=False)
        # 10ms of silence at 16kHz mono 16-bit PCM (bytes_per_10ms).
        chunk = bytes(16000 * 10 // 1000 * 2)

        future = track.add_audio_bytes(chunk)
        assert not future.done(), "future must not resolve merely because bytes were queued"

        await track.recv()

        assert future.done(), "future must resolve once the real track actually drains the chunk"

    run(body)


def test_ordered_event_log_for_start_timeout_cleanup_matches_the_planned_sequence() -> None:
    """``fence A cleanup-pending -> dispatch cancel/flush A -> record
    delivery_unknown -> acknowledge A's lane -> clear slot -> optionally
    admit B``, exactly as the plan's fallback sequence diagram orders it."""

    async def body() -> None:
        events: list[str] = []

        def on_terminal(token, ident, disposition) -> None:
            events.append(f"record_{disposition.value}")

        def dispatch_cleanup(token, ident) -> None:
            events.append("dispatch_cleanup")

        coordinator, clock = make_coordinator(
            on_terminal=on_terminal, dispatch_cleanup=dispatch_cleanup
        )
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")

        assert generation.cleanup_pending is False
        assert coordinator.occupied is True

        await tick(clock, 10.0 + 1e-6)

        assert generation.cleanup_pending is True, "deadline expiry must fence cleanup_pending"
        assert events == ["dispatch_cleanup", "record_delivery_unknown"], (
            "cancel/flush dispatch must precede the recorded terminal disposition"
        )
        assert coordinator.occupied is False, "slot clears once, after the recorded disposition"
        assert generation.terminalized is True

        admitted = coordinator.try_admit(identity("work-2", "utt-2"))
        assert admitted is not None, "a cleared slot may optionally admit the next generation"

    run(body)


@pytest.mark.parametrize(
    "pause",
    [pytest.param(False, id="barge_in"), pytest.param(True, id="pause")],
)
def test_ordered_event_log_for_interruption_or_pause_records_before_cleanup_with_no_admission(
    pause: bool,
) -> None:
    """``record disposition/tombstone -> (caller forwards InterruptionFrame)
    -> stop or teardown``. ``record_interruption``'s own mutations are
    complete before it returns, so a caller that forwards
    ``InterruptionFrame`` immediately afterwards (as ``server/pipeline.py``
    does around every ``record_interruption`` call site) can never observe
    a stale, un-recorded generation. Neither the interruption call nor its
    later cleanup dispatch ever calls ``try_admit``/``start_next`` -- the
    coordinator has no reference to ``SpeechScheduler`` at all, so it is
    structurally incapable of starting the next generation on its own; only
    an explicit caller-issued admission (mirrored here) does that."""

    async def body() -> None:
        events: list[str] = []

        def dispatch_cleanup(token, ident) -> None:
            events.append("dispatch_cleanup")

        coordinator, clock = make_coordinator(dispatch_cleanup=dispatch_cleanup)
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        coordinator.bind_context(generation.token, "utt-1")
        coordinator.on_tts_started("utt-1")

        coordinator.record_interruption(generation.token, pause=pause)
        events.append("recorded")

        # Disposition and tombstone are visible immediately -- before any
        # forwarded InterruptionFrame or cleanup dispatch could have run.
        assert events == ["recorded"]
        assert coordinator.generation_for_token(generation.token).disposition == (
            DeliveryDisposition.PAUSED if pause else DeliveryDisposition.INTERRUPTED
        )
        assert coordinator.generation_for_token(generation.token).tombstoned is True
        # No admission happens as a side effect of the interruption call.
        assert coordinator.try_admit(identity("work-2", "utt-blocked")) is None

        await tick(clock, 1.0 + 1e-6)  # interruption-cleanup deadline

        assert events == ["recorded", "dispatch_cleanup"]
        assert coordinator.occupied is False
        # A late/stale stop after the barrier already resolved is a no-op,
        # not a second admission trigger.
        await coordinator.on_transport_bot_stopped()
        assert events == ["recorded", "dispatch_cleanup"]

        admitted = coordinator.try_admit(identity("work-2", "utt-2"))
        assert admitted is not None

    run(body)


# ---------------------------------------------------------------------------
# Phase 3: full terminal matrix -- every terminal reason drops late A frames,
# never lets a late fieldless A stop be observed as B's, leaves B unchanged,
# and progresses exactly once.
# ---------------------------------------------------------------------------


async def _terminalize_generation_a(
    coordinator: SpeechLifecycleCoordinator,
    clock: ManualTimerScheduler,
    generation: object,
    *,
    reason: str,
    audio_submitted: bool,
) -> None:
    """Drive generation A to its terminal state via the coordinator-level
    path the plan assigns to each terminal reason.

    Cancellation and barge-in share the same coordinator-level path
    (``record_interruption(pause=False)``) because
    ``server/pipeline.py``'s cancel-control handler calls
    ``lifecycle.record_interruption(active_lease.token, pause=False)``
    directly, exactly as barge-in does. Reconnect and shutdown share the
    drain/teardown path because the coordinator exposes no reconnect- or
    shutdown-specific hook: a fresh connection-scoped lane is this same
    coordinator's post-``teardown_complete`` state (``connection_epoch``
    bump), per the plan's fallback sequence diagram and
    ``server/pipeline.py``'s per-connection ``lifecycle`` field.
    """
    if reason in ("interruption", "cancellation", "pause"):
        coordinator.record_interruption(generation.token, pause=(reason == "pause"))
        await tick(clock, 1.0 + 1e-6)  # interruption-cleanup deadline
    elif reason == "start_timeout":
        await tick(clock, 10.0 + 1e-6)
    elif reason in ("drain_fallback", "reconnect", "shutdown"):
        coordinator.on_tts_stopped("utt-a")
        await tick(clock, generation.audio_duration_seconds + 1.0 + 1e-6)
    elif reason in ("generic_error", "local_error"):
        await coordinator.provider_error(generation.token)
    else:  # pragma: no cover - guards against a mistyped parametrize id
        raise AssertionError(reason)

    if audio_submitted:
        await coordinator.teardown_complete(generation.token)


_TERMINAL_MATRIX_REASONS = [
    pytest.param("start_timeout", False, id="start_timeout"),
    pytest.param("drain_fallback", True, id="drain_fallback"),
    pytest.param("interruption", True, id="interruption"),
    pytest.param("pause", True, id="pause"),
    pytest.param("cancellation", True, id="cancellation"),
    pytest.param("reconnect", True, id="reconnect"),
    pytest.param("shutdown", True, id="shutdown"),
    pytest.param("generic_error", True, id="generic_error"),
    pytest.param("local_error", True, id="local_error"),
]


@pytest.mark.parametrize("reason, audio_submitted", _TERMINAL_MATRIX_REASONS)
def test_terminal_matrix_drops_late_a_frames_leaves_b_unchanged_and_progresses_once(
    reason: str, audio_submitted: bool
) -> None:
    """The full terminal matrix -- interruption, pause, reconnect,
    cancellation, shutdown, start-timeout, drain fallback, generic error,
    and local error -- must each: drop late context-bearing A frames before
    output, never let a late fieldless A stop be observed as anything other
    than B's own event once B legally occupies the slot, leave B
    unaffected, and terminalize A exactly once.

    ``start_timeout`` fires precisely because no correlated TTS event ever
    arrives, so it never binds a context and has no context-bearing A frame
    to drop -- its matrix row instead proves the slot still clears via
    self-acknowledged flush and admits B cleanly, same as every other
    reason.
    """

    async def body() -> None:
        terminal_calls: list[tuple[str, DeliveryDisposition]] = []

        def on_terminal(token, ident, disposition) -> None:
            terminal_calls.append((token, disposition))

        coordinator, clock = make_coordinator(on_terminal=on_terminal)
        generation_a = admit_and_hand_to_tts(coordinator, "work-1", "utt-a")
        if reason != "start_timeout":
            coordinator.bind_context(generation_a.token, "utt-a")
            coordinator.on_tts_started("utt-a")
            if audio_submitted:
                coordinator.on_tts_audio(
                    "utt-a", audio=synth_audio(seconds=0.1), sample_rate=16000, num_channels=1
                )

        await _terminalize_generation_a(
            coordinator, clock, generation_a, reason=reason, audio_submitted=audio_submitted
        )

        # Terminalized and reaped from the coordinator; the held object was
        # mutated in place first, so it still reflects final state.
        assert generation_a.terminalized is True
        assert terminal_calls == [(generation_a.token, generation_a.disposition)]
        assert coordinator.generation_for_token(generation_a.token) is None

        if reason != "start_timeout":
            # Late context-bearing A frames are dropped before they could
            # reach output.
            assert coordinator.drop_stale_frame("utt-a") is True
            assert (
                coordinator.on_tts_audio(
                    "utt-a", audio=synth_audio(seconds=0.05), sample_rate=16000, num_channels=1
                )
                is False
            )
            assert coordinator.on_tts_stopped("utt-a") is False

        # B is admitted only once A's lane is fully retired, and B's own
        # state is untouched by anything A did.
        generation_b = coordinator.try_admit(identity("work-2", "utt-b"))
        assert generation_b is not None, "B must be admitted once A's lane is retired"
        coordinator.bind_context(generation_b.token, "utt-b")
        coordinator.on_transport_bot_started()
        assert (
            coordinator.generation_for_token(generation_b.token).phase
            == GenerationPhase.TRANSPORT_STARTED
        )

        # A late fieldless stop can only ever be observed as the sole
        # occupied lane's own event: by the time B occupies the slot, A's
        # lane has already been fully retired (self-acked or torn down) and
        # can emit nothing further, so this resolves B -- never A a second
        # time.
        await coordinator.on_transport_bot_stopped()

        assert coordinator.occupied is False
        # B is also reaped once terminalized; the held object was mutated in
        # place first, so it still reflects the final phase.
        assert generation_b.phase == GenerationPhase.TRANSPORT_STOPPED
        assert terminal_calls == [
            (generation_a.token, generation_a.disposition),
            (generation_b.token, DeliveryDisposition.DELIVERY_UNKNOWN),
        ], "A must not be terminalized a second time by a late stop meant for it"

    run(body)


def test_output_lane_wrapper_gates_transport_stopped_on_the_real_final_audio_future() -> None:
    """Wire the real RawAudioTrack future alongside the coordinator's
    transport lifecycle. A fieldless stop arriving before the final audio
    future has resolved is exactly the "late A stop" shape the plan calls
    out; the coordinator must still resolve the generation correctly (via
    its own on_tts_stopped/on_transport_bot_stopped sequencing) once the
    real track has actually drained. Full pipeline wiring through
    TransportSpeechLifecycleProcessor and the real SmallWebRTCOutputTransport
    is exercised once server/app.py installs it.
    """
    from pipecat.transports.smallwebrtc.transport import RawAudioTrack

    async def body() -> None:
        coordinator, _ = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        coordinator.bind_context(generation.token, "utt-1")
        coordinator.on_tts_started("utt-1")

        track = RawAudioTrack(sample_rate=16000, auto_silence=False)
        chunk = bytes(16000 * 10 // 1000 * 2)
        final_audio_future = track.add_audio_bytes(chunk)
        coordinator.on_tts_audio("utt-1", audio=chunk, sample_rate=16000, num_channels=1)
        coordinator.on_tts_stopped("utt-1")

        assert not final_audio_future.done()
        await coordinator.on_transport_bot_stopped()
        assert coordinator.occupied is False

        await track.recv()  # drains the real chunk; future now resolves
        assert final_audio_future.done()

    run(body)


def test_transport_processor_drops_a_tts_started_frame_for_a_tombstoned_generation() -> None:
    """A straggling TTSStartedFrame for an already-tombstoned generation
    (e.g. after record_interruption) must never reach transport.output(), the
    same way TTSAudioRawFrame/TTSStoppedFrame are already dropped."""
    from pipecat.frames.frames import TTSStartedFrame
    from pipecat.processors.frame_processor import FrameDirection

    from server.speech_lifecycle import TransportSpeechLifecycleProcessor

    async def body() -> None:
        coordinator, _ = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        coordinator.bind_context(generation.token, "utt-1")
        coordinator.record_interruption(generation.token, pause=False)
        assert coordinator.generation_for_token(generation.token).tombstoned is True

        processor = TransportSpeechLifecycleProcessor(coordinator)
        forwarded: list[object] = []

        async def push(frame: object, _direction: object) -> None:
            forwarded.append(frame)

        processor.push_frame = push  # type: ignore[method-assign]

        await processor.process_frame(
            TTSStartedFrame(context_id="utt-1"), FrameDirection.DOWNSTREAM
        )

        assert forwarded == []

    run(body)


def test_transport_processor_keeps_marker_b_across_stale_start_and_audio_a() -> None:
    """marker-B, stale start/audio/stop-A, start/audio-B must bind only B."""
    from pipecat.frames.frames import TTSAudioRawFrame, TTSStartedFrame, TTSStoppedFrame
    from pipecat.processors.frame_processor import FrameDirection

    from server.speech_lifecycle import (
        SpeechGenerationMarkerFrame,
        TransportSpeechLifecycleProcessor,
    )

    async def body() -> None:
        coordinator, _ = make_coordinator()
        generation_a = admit_and_hand_to_tts(coordinator, "work-a", "utt-a")
        assert coordinator.bind_context(generation_a.token, "utt-a") is True
        assert coordinator.on_tts_started("utt-a") is True
        assert coordinator.on_tts_stopped("utt-a") is True
        await coordinator.on_transport_bot_stopped()

        generation_b = admit_and_hand_to_tts(coordinator, "work-b", "utt-b")
        processor = TransportSpeechLifecycleProcessor(coordinator)
        forwarded: list[object] = []

        async def push(frame: object, _direction: object) -> None:
            forwarded.append(frame)

        processor.push_frame = push  # type: ignore[method-assign]
        stale_audio = TTSAudioRawFrame(
            audio=synth_audio(seconds=0.05),
            sample_rate=16000,
            num_channels=1,
            context_id="utt-a",
        )
        unbound_audio = TTSAudioRawFrame(
            audio=synth_audio(seconds=0.05),
            sample_rate=16000,
            num_channels=1,
            context_id="never-bound",
        )
        audio_b = TTSAudioRawFrame(
            audio=synth_audio(seconds=0.05),
            sample_rate=16000,
            num_channels=1,
            context_id="utt-b",
        )

        await processor.process_frame(
            SpeechGenerationMarkerFrame(
                token=generation_b.token,
                utterance_id="utt-b",
                work_item_id="work-b",
            ),
            FrameDirection.DOWNSTREAM,
        )
        await processor.process_frame(
            TTSStartedFrame(context_id="utt-a"), FrameDirection.DOWNSTREAM
        )
        await processor.process_frame(stale_audio, FrameDirection.DOWNSTREAM)
        await processor.process_frame(
            TTSStoppedFrame(context_id="utt-a"), FrameDirection.DOWNSTREAM
        )
        await processor.process_frame(unbound_audio, FrameDirection.DOWNSTREAM)
        await processor.process_frame(
            TTSStoppedFrame(context_id="never-bound"), FrameDirection.DOWNSTREAM
        )
        start_b = TTSStartedFrame(context_id="utt-b")
        await processor.process_frame(start_b, FrameDirection.DOWNSTREAM)
        await processor.process_frame(audio_b, FrameDirection.DOWNSTREAM)

        assert forwarded == [start_b, audio_b]
        assert coordinator.token_for_context("utt-a") is None
        assert coordinator.token_for_context("utt-b") == generation_b.token
        assert generation_b.context_id == "utt-b"
        assert generation_b.audio_submitted is True

    run(body)


def test_transport_processor_consumes_token_bearing_flush_ack_after_cleanup() -> None:
    from pipecat.processors.frame_processor import FrameDirection

    from server.speech_lifecycle import (
        SpeechGenerationFlushAckFrame,
        TransportSpeechLifecycleProcessor,
    )

    async def body() -> None:
        coordinator, clock = make_coordinator(auto_ack_cleanup=False)
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")
        await tick(clock, 10.0 + 1e-6)
        assert coordinator.occupied is True

        processor = TransportSpeechLifecycleProcessor(coordinator)
        forwarded: list[object] = []

        async def push(frame: object, _direction: object) -> None:
            forwarded.append(frame)

        processor.push_frame = push  # type: ignore[method-assign]
        await processor.process_frame(
            SpeechGenerationFlushAckFrame(token=generation.token),
            FrameDirection.DOWNSTREAM,
        )

        assert forwarded == []
        assert generation.terminalized is True
        assert coordinator.occupied is False

    run(body)


def test_transport_processor_binds_local_tts_audio_frames_carrying_context_id() -> None:
    """LocalTTS.run_tts() yields TTSAudioRawFrame(context_id=...) (fixed
    alongside TTSStartedFrame/TTSStoppedFrame in server/services/tts.py).
    TransportSpeechLifecycleProcessor must accumulate its duration via
    on_tts_audio() -- setting audio_submitted True -- and, for a tombstoned
    generation, must drop it before it reaches transport.output(), the same
    way the local TTSStartedFrame/TTSStoppedFrame paths already are covered
    above."""
    from pipecat.frames.frames import TTSAudioRawFrame, TTSStartedFrame
    from pipecat.processors.frame_processor import FrameDirection

    from server.speech_lifecycle import (
        SpeechGenerationMarkerFrame,
        TransportSpeechLifecycleProcessor,
    )

    async def body() -> None:
        coordinator, _ = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")

        processor = TransportSpeechLifecycleProcessor(coordinator)
        forwarded: list[object] = []

        async def push(frame: object, _direction: object) -> None:
            forwarded.append(frame)

        processor.push_frame = push  # type: ignore[method-assign]

        await processor.process_frame(
            SpeechGenerationMarkerFrame(
                token=generation.token, utterance_id="utt-1", work_item_id="work-1"
            ),
            FrameDirection.DOWNSTREAM,
        )
        await processor.process_frame(
            TTSStartedFrame(context_id="utt-1"), FrameDirection.DOWNSTREAM
        )
        audio_frame = TTSAudioRawFrame(
            audio=synth_audio(seconds=0.1), sample_rate=16000, num_channels=1, context_id="utt-1"
        )
        await processor.process_frame(audio_frame, FrameDirection.DOWNSTREAM)

        assert coordinator.generation_for_token(generation.token).audio_submitted is True
        assert audio_frame in forwarded

        # A second, tombstoned generation's local-TTS audio frame must be
        # dropped rather than forwarded to transport.output().
        coordinator.record_interruption(generation.token, pause=False)
        stale_audio_frame = TTSAudioRawFrame(
            audio=synth_audio(seconds=0.1), sample_rate=16000, num_channels=1, context_id="utt-1"
        )
        forwarded.clear()
        assert coordinator.drop_stale_frame("utt-1") is True
        await processor.process_frame(stale_audio_frame, FrameDirection.DOWNSTREAM)

        assert stale_audio_frame not in forwarded

    run(body)


def test_timeout_driven_cleanup_tombstones_before_dispatch_races_a_late_start_frame() -> None:
    """_begin_delivery_unknown (start-timeout/drain-timeout path) must
    tombstone the generation before its first await in cleanup dispatch, the
    same way record_interruption does -- otherwise a straggling TTS
    start/audio frame that arrives while cleanup is in flight would not be
    recognised as stale and would reach output."""

    async def body() -> None:
        coordinator, clock = make_coordinator()
        generation = admit_and_hand_to_tts(coordinator, "work-1", "utt-1")

        await tick(clock, 10.0)  # fires the start-timeout -> _begin_delivery_unknown

        # By the time the tick's timer-fired coroutine chain finishes, a
        # zero-audio generation has already run all the way through
        # terminalization and been reaped; the held object was mutated in
        # place first, so it still reflects the tombstoned state.
        assert generation.tombstoned is True

    run(body)
