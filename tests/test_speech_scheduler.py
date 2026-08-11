"""Speech scheduling owns leases, queues, and frame ownership.

Phase 1 makes ``lifecycle`` an unconditionally required ``SpeechScheduler``
constructor argument (the dev plan's "Normative build order" step 2): every
construction below -- including the pre-existing no-lifecycle cases -- goes
through ``_scheduler()``, which always injects a real, ``ManualTimerScheduler``-
backed ``SpeechLifecycleCoordinator`` rather than relying on the removed
``lifecycle is None`` fallback branches.
"""

import asyncio

from server.contracts import DeliveryState
from server.session_state import SessionState
from server.speech_lifecycle import (
    DeliveryDisposition,
    GenerationIdentity,
    ManualTimerScheduler,
    SpeechLifecycleCoordinator,
)
from server.speech_scheduler import ROLE_ACK, ROLE_RESULT, ROLE_TIMEOUT_NOTICE, SpeechScheduler


def _lifecycle(**kwargs) -> SpeechLifecycleCoordinator:
    timers = ManualTimerScheduler()
    return SpeechLifecycleCoordinator(clock=timers, timers=timers, **kwargs)


def _scheduler(
    state: SessionState | None = None,
    *,
    lifecycle: SpeechLifecycleCoordinator | None = None,
    **kwargs,
) -> SpeechScheduler:
    return SpeechScheduler(state or SessionState(), lifecycle=lifecycle or _lifecycle(), **kwargs)


def enqueue(scheduler: SpeechScheduler, work_item_id: str, text: str):
    return scheduler.enqueue(
        work_item_id=work_item_id,
        run_id=f"run-{work_item_id}",
        result_id=f"result-{work_item_id}",
        text=text,
    )


def enqueue_ack(scheduler: SpeechScheduler, *, turn_id: str, text: str = "One moment."):
    """Enqueue an ephemeral ack under the plan's synthetic queue key
    (``ack-{turn_id}``): ack items require ``ack_id`` and carry no
    ``result_id``."""
    ack_work_item_id = f"ack-{turn_id}"
    return scheduler.enqueue(
        work_item_id=ack_work_item_id,
        run_id=f"run-{ack_work_item_id}",
        result_id=None,
        text=text,
        role=ROLE_ACK,
        ack_id=ack_work_item_id,
    )


def test_speech_scheduler_requires_a_lifecycle_coordinator() -> None:
    """Plan: the ``lifecycle is None`` fallback branches -- ``_release()``
    clearing ``_active`` directly and the ``uuid4().hex`` token fallback in
    ``start_next()`` -- are deleted entirely, and ``lifecycle`` becomes a
    non-optional constructor argument with no default."""
    try:
        SpeechScheduler(SessionState())
    except TypeError:
        pass
    else:
        raise AssertionError("SpeechScheduler no longer requires lifecycle")


def test_one_active_utterance_lease_and_per_work_item_queue_isolation() -> None:
    scheduler = _scheduler()
    first = enqueue(scheduler, "work-1", "first")
    second = enqueue(scheduler, "work-2", "second")

    lease = asyncio.run(scheduler.start_next())
    assert lease is not None and lease.utterance_id == first.utterance_id
    assert asyncio.run(scheduler.start_next()) is None

    scheduler.interrupt()
    next_item = asyncio.run(scheduler.start_next())
    assert next_item is not None and next_item.utterance_id == second.utterance_id
    assert scheduler.state.speech[first.utterance_id].state == DeliveryState.INTERRUPTED


def test_replay_gets_a_new_utterance_id() -> None:
    scheduler = _scheduler()
    first = enqueue(scheduler, "work-1", "answer")
    assert asyncio.run(scheduler.start_next()) is not None
    scheduler.interrupt()

    replay = scheduler.resume(first)
    assert replay.utterance_id != first.utterance_id
    assert replay.text == "answer"


def test_pause_preserves_paused_state_and_resume_records_resumed_transition() -> None:
    scheduler = _scheduler()
    first = enqueue(scheduler, "work-1", "answer")
    asyncio.run(scheduler.start_next())

    scheduler.pause("work-1")
    assert scheduler.active is None
    assert scheduler.paused("work-1") == first
    assert scheduler.state.speech[first.utterance_id].state == DeliveryState.PAUSED

    replay = scheduler.resume("work-1")
    assert replay is not None
    assert scheduler.state.speech[replay.utterance_id].state == DeliveryState.RESUMED


def test_resume_terminalizes_the_old_paused_utterance_instead_of_stranding_it() -> None:
    """``resume()`` replays a paused item under a NEW utterance_id (minted by
    ``enqueue()``), so nothing else would ever terminalize the OLD id's
    PAUSED record in ``SessionState.speech`` -- it would otherwise ship in
    every future RuntimeSnapshot for the rest of the process lifetime."""
    scheduler = _scheduler()
    first = enqueue(scheduler, "work-1", "answer")
    asyncio.run(scheduler.start_next())

    scheduler.pause("work-1")
    assert scheduler.state.speech[first.utterance_id].state == DeliveryState.PAUSED

    replay = scheduler.resume("work-1")
    assert replay is not None
    assert replay.utterance_id != first.utterance_id
    assert scheduler.state.speech[first.utterance_id].state == DeliveryState.INTERRUPTED

    snapshot_states = [progress.state for progress in scheduler.state.snapshot().speech_progress]
    assert DeliveryState.PAUSED not in snapshot_states


def test_reconnect_interrupt_terminalizes_paused_items_instead_of_stranding_them() -> None:
    """A paused utterance is only resumable through this scheduler, which a
    reconnect discards -- so ``interrupt(reconnect=True)`` must terminalize it
    rather than leave a non-terminal PAUSED record shipping in every future
    snapshot."""
    state = SessionState()
    scheduler = _scheduler(state)
    first = enqueue(scheduler, "work-1", "answer")
    asyncio.run(scheduler.start_next())
    scheduler.pause("work-1")
    assert state.speech[first.utterance_id].state == DeliveryState.PAUSED

    scheduler.interrupt(reconnect=True)

    assert scheduler.paused("work-1") is None
    assert state.speech[first.utterance_id].state == DeliveryState.INTERRUPTED_BY_RECONNECT
    snapshot_states = [progress.state for progress in state.snapshot().speech_progress]
    assert DeliveryState.PAUSED not in snapshot_states


def test_scheduler_stop_is_task_local_when_other_work_is_queued() -> None:
    async def run() -> None:
        scheduler = _scheduler()
        task_one = enqueue(scheduler, "work-1", "one")
        task_two = enqueue(scheduler, "work-2", "two")
        await scheduler.start_next()
        scheduler.interrupt()

        assert scheduler.state.speech[task_one.utterance_id].state == DeliveryState.INTERRUPTED
        assert scheduler._queues["work-2"][0].utterance_id == task_two.utterance_id

    asyncio.run(run())


def test_discard_queued_notice_removes_only_timeout_notice_role_and_preserves_order() -> None:
    scheduler = _scheduler()
    before = scheduler.enqueue(
        work_item_id="work-b",
        run_id="run-before",
        result_id="result-before",
        text="Still checking on that.",
        role=ROLE_RESULT,
    )
    notice = scheduler.enqueue(
        work_item_id="work-b",
        run_id="run-notice",
        result_id="result-notice",
        text="That is taking longer than expected.",
        role=ROLE_TIMEOUT_NOTICE,
    )
    after = scheduler.enqueue(
        work_item_id="work-b",
        run_id="run-after",
        result_id="result-after",
        text="One more update.",
        role=ROLE_RESULT,
    )

    discarded = scheduler.discard_queued_notice("work-b")

    assert discarded == (notice,)
    assert scheduler._queues["work-b"] == [before, after]
    assert scheduler.state.speech[notice.utterance_id].state == DeliveryState.INTERRUPTED
    assert scheduler.state.speech[before.utterance_id].state != DeliveryState.INTERRUPTED
    assert scheduler.state.speech[after.utterance_id].state != DeliveryState.INTERRUPTED


def test_discard_queued_notice_is_a_noop_when_no_notice_is_queued() -> None:
    scheduler = _scheduler()
    item = enqueue(scheduler, "work-b", "just a result")

    discarded = scheduler.discard_queued_notice("work-b")

    assert discarded == ()
    assert scheduler._queues["work-b"] == [item]


def test_discard_queued_notice_is_a_noop_for_an_unknown_work_item() -> None:
    scheduler = _scheduler()

    assert scheduler.discard_queued_notice("no-such-work-item") == ()


def test_discard_queued_notice_does_not_touch_other_work_item_queues() -> None:
    scheduler = _scheduler()
    notice = scheduler.enqueue(
        work_item_id="work-b",
        run_id="run-notice",
        result_id="result-notice",
        text="That is taking longer than expected.",
        role=ROLE_TIMEOUT_NOTICE,
    )
    other = enqueue(scheduler, "work-other", "unrelated")

    discarded = scheduler.discard_queued_notice("work-b")

    assert discarded == (notice,)
    assert scheduler._queues["work-other"] == [other]


def test_discard_queued_notice_cannot_remove_a_notice_already_admitted_to_the_transport_slot() -> (
    None
):
    scheduler = _scheduler()
    scheduler.enqueue(
        work_item_id="work-b",
        run_id="run-notice",
        result_id="result-notice",
        text="That is taking longer than expected.",
        role=ROLE_TIMEOUT_NOTICE,
    )
    admitted = asyncio.run(scheduler.start_next())
    assert admitted is not None and admitted.role == ROLE_TIMEOUT_NOTICE

    discarded = scheduler.discard_queued_notice("work-b")

    assert discarded == ()
    assert scheduler.active is not None
    assert scheduler.active.item == admitted


def test_interrupt_signals_provider_stop_before_releasing_active_lease() -> None:
    stopped: list[str] = []
    scheduler = _scheduler(stop=lambda item: stopped.append(item.utterance_id))
    item = enqueue(scheduler, "work-1", "one")
    asyncio.run(scheduler.start_next())

    scheduler.interrupt()

    assert stopped == [item.utterance_id]
    assert scheduler.active is None


def test_synthesis_end_is_not_completion_and_unknown_delivery_is_terminal() -> None:
    scheduler = _scheduler()
    item = enqueue(scheduler, "work-1", "answer")
    asyncio.run(scheduler.start_next())

    scheduler.synthesis_ended(item.utterance_id)
    assert scheduler.state.speech[item.utterance_id].state == DeliveryState.SYNTHESIS_ENDED
    assert scheduler.active is not None

    scheduler.delivery_unknown(item.utterance_id)
    assert scheduler.state.speech[item.utterance_id].state == DeliveryState.DELIVERY_UNKNOWN
    assert scheduler.active is None


def test_synthesis_failure_releases_lease_and_allows_next_item() -> None:
    async def run() -> None:
        calls = 0

        async def speak(_item):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("provider unavailable")

        scheduler = _scheduler(speak=speak)
        first = enqueue(scheduler, "work-1", "first")
        second = enqueue(scheduler, "work-2", "second")

        try:
            await scheduler.start_next("work-1")
        except RuntimeError as exc:
            assert str(exc) == "provider unavailable"
        else:
            raise AssertionError("speech provider failure was swallowed")

        # The lease is released synchronously with the failure; the next
        # item is admitted by the scheduler's own deferred re-probe.
        assert scheduler.active is None
        assert scheduler.state.speech[first.utterance_id].state == DeliveryState.DELIVERY_UNKNOWN

        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert scheduler.active is not None
        assert scheduler.active.item.utterance_id == second.utterance_id

    asyncio.run(run())


def test_speech_submission_and_cleanup_failure_release_only_after_teardown() -> None:
    async def run() -> None:
        teardown_calls: list[str] = []

        async def cleanup_failed(*_args) -> None:
            raise RuntimeError("cleanup unavailable")

        async def teardown(token, _identity) -> None:
            teardown_calls.append(token)
            await lifecycle.teardown_complete(token)

        lifecycle = _lifecycle(
            dispatch_cleanup=cleanup_failed,
            dispatch_teardown=teardown,
        )

        async def speak(_item):
            raise RuntimeError("provider unavailable")

        scheduler = _scheduler(speak=speak, lifecycle=lifecycle)
        item = enqueue(scheduler, "work-1", "first")

        try:
            await scheduler.start_next()
        except RuntimeError as exc:
            assert str(exc) == "provider unavailable"
        else:
            raise AssertionError("speech provider failure was swallowed")

        assert scheduler.active is None
        assert lifecycle.occupied is False
        assert len(teardown_calls) == 1
        assert scheduler.state.speech[item.utterance_id].state == DeliveryState.DELIVERY_UNKNOWN

    asyncio.run(run())


def test_reconnect_terminally_cancels_active_and_queued_old_epoch_items() -> None:
    scheduler = _scheduler()
    first = enqueue(scheduler, "work-1", "one")
    second = enqueue(scheduler, "work-2", "two")
    third = enqueue(scheduler, "work-3", "three")
    asyncio.run(scheduler.start_next())

    scheduler.interrupt(epoch=7, reconnect=True)

    assert (
        scheduler.state.speech[first.utterance_id].state == DeliveryState.INTERRUPTED_BY_RECONNECT
    )
    assert scheduler._queues == {}
    assert (
        scheduler.state.speech[second.utterance_id].state == DeliveryState.INTERRUPTED_BY_RECONNECT
    )
    assert (
        scheduler.state.speech[third.utterance_id].state == DeliveryState.INTERRUPTED_BY_RECONNECT
    )


def test_full_stop_terminalizes_active_queued_and_paused_items_without_reconnect() -> None:
    """Regression for round-4 deep-review logic finding #7: on a same-epoch,
    non-reconnect teardown (``ConnectionPipeline.shutdown``'s "speech output
    teardown"/"session shutdown" paths), only the active lease used to be
    terminalized -- queued and paused items on other work-item keys were
    left as ``QUEUED``/``PAUSED`` forever, leaking into every later
    connection's snapshot because the scheduler carrying them is discarded
    with no other terminalizer. ``full_stop=True`` (what
    ``ConnectionPipeline.deactivate`` now always passes) sweeps them too,
    while recording plain ``INTERRUPTED`` (not ``INTERRUPTED_BY_RECONNECT``)
    and without bypassing the active-epoch fence -- unlike a genuine
    reconnect, this teardown never advanced ``active_epoch``.

    A bare ``interrupt()`` (``full_stop=False``, the default) must still
    behave task-locally, per
    ``test_scheduler_stop_is_task_local_when_other_work_is_queued``.
    """
    scheduler = _scheduler()
    paused = enqueue(scheduler, "work-1", "to-be-paused")
    asyncio.run(scheduler.start_next())
    scheduler.pause("work-1")
    assert scheduler.state.speech[paused.utterance_id].state == DeliveryState.PAUSED

    active = enqueue(scheduler, "work-2", "active")
    asyncio.run(scheduler.start_next())
    queued = enqueue(scheduler, "work-3", "queued")

    scheduler.interrupt(full_stop=True)

    assert scheduler.state.speech[active.utterance_id].state == DeliveryState.INTERRUPTED
    assert scheduler.state.speech[queued.utterance_id].state == DeliveryState.INTERRUPTED
    assert scheduler.state.speech[paused.utterance_id].state == DeliveryState.INTERRUPTED
    assert scheduler._queues == {}
    assert scheduler._paused == {}


def test_full_stop_without_reconnect_still_emits_when_active_epoch_already_advanced() -> None:
    """Regression for round-5 review-gauntlet Theme B: ``full_stop=True,
    reconnect=False`` used to pass ``allow_stale_reconnect=reconnect`` (i.e.
    ``False``), so if ``active_epoch`` had already advanced past these
    items' origin epoch by the time the sweep ran -- e.g. a promotion
    landing in the async gap between ``dispatch_lifecycle_teardown``
    scheduling ``shutdown(reconnect=False)`` as a separate task and that
    task actually running ``deactivate()`` -- ``SessionState.speech_progress``
    silently dropped the terminal record instead of storing it, defeating
    the exact leak ``full_stop`` was added to fix. The bypass must track
    "this scheduler is being discarded" (``full_stop``), not "was this
    literally a reconnect".
    """
    scheduler = _scheduler()
    paused = enqueue(scheduler, "work-1", "to-be-paused")
    asyncio.run(scheduler.start_next())
    scheduler.pause("work-1")

    active = enqueue(scheduler, "work-2", "active")
    asyncio.run(scheduler.start_next())
    queued = enqueue(scheduler, "work-3", "queued")

    # Simulate a promotion advancing active_epoch during the async gap
    # before this same-epoch teardown's sweep runs.
    scheduler.state.active_epoch = 999

    scheduler.interrupt(full_stop=True, reconnect=False)

    assert scheduler.state.speech[active.utterance_id].state == DeliveryState.INTERRUPTED
    assert scheduler.state.speech[queued.utterance_id].state == DeliveryState.INTERRUPTED
    assert scheduler.state.speech[paused.utterance_id].state == DeliveryState.INTERRUPTED
    # Disposition must stay plain INTERRUPTED, not INTERRUPTED_BY_RECONNECT,
    # since reconnect=False -- only the fence-bypass tracks full_stop.
    assert scheduler.state.speech[active.utterance_id].state != (
        DeliveryState.INTERRUPTED_BY_RECONNECT
    )


def test_delayed_callbacks_from_reconnected_utterance_are_ignored() -> None:
    scheduler = _scheduler()
    old_item = enqueue(scheduler, "work-1", "old")
    asyncio.run(scheduler.start_next())

    scheduler.interrupt(epoch=2, reconnect=True)
    new_item = scheduler.enqueue(
        result_id="result-work-1",
        work_item_id="work-1",
        run_id="run-work-1-new",
        text="new",
        origin_epoch=2,
    )
    asyncio.run(scheduler.start_next())
    events_before_callbacks = scheduler.state.events

    scheduler.synthesis_ended(old_item.utterance_id)
    scheduler.delivery_completed(old_item.utterance_id)
    scheduler.delivery_unknown(old_item.utterance_id)

    assert scheduler.active is not None
    assert scheduler.active.item.utterance_id == new_item.utterance_id
    assert scheduler.state.events == events_before_callbacks
    assert scheduler.state.speech[new_item.utterance_id].state == DeliveryState.STARTED


def test_start_next_defers_admission_to_an_injected_lifecycle_coordinator() -> None:
    """Phase 1 makes SpeechLifecycleCoordinator the sole admission authority:
    the scheduler owns queue selection but must ask the coordinator before
    starting speech, and must not start when the coordinator's global slot
    is occupied (matches the plan's Integration Seams entry "Work queues ->
    global slot" -> ``SpeechLifecycleCoordinator.try_admit()``)."""
    coordinator = _lifecycle()
    scheduler = _scheduler(lifecycle=coordinator)
    item = enqueue(scheduler, "work-1", "one")

    result = asyncio.run(scheduler.start_next())

    assert result is not None
    assert coordinator.occupied is True
    assert coordinator.generation_for_token(scheduler.active.token) is not None
    assert coordinator.generation_for_token(scheduler.active.token).identity.utterance_id == (
        item.utterance_id
    )


def test_start_next_does_not_start_when_the_coordinator_slot_is_already_occupied() -> None:
    coordinator = _lifecycle()
    coordinator.try_admit(GenerationIdentity("occupied-by-someone-else", "work-other"))
    scheduler = _scheduler(lifecycle=coordinator)
    enqueue(scheduler, "work-1", "one")

    result = asyncio.run(scheduler.start_next())

    assert result is None
    assert scheduler.active is None


def test_dropped_prestart_context_cannot_claim_replacement_utterance() -> None:
    scheduler = _scheduler(speak=lambda _item: None)
    old_item = enqueue(scheduler, "work-1", "old")
    asyncio.run(scheduler.start_next())
    scheduler.interrupt()

    new_item = enqueue(scheduler, "work-2", "new")
    asyncio.run(scheduler.start_next())

    scheduler.provider_started(old_item.utterance_id)
    assert scheduler.provider_synthesis_ended(old_item.utterance_id) is False
    assert scheduler.active is not None
    assert scheduler.active.item == new_item

    scheduler.provider_started(new_item.utterance_id)
    assert scheduler.provider_synthesis_ended(new_item.utterance_id) is True
    assert scheduler.provider_delivery_unknown(new_item.utterance_id) is True
    assert scheduler.active is None


# -- Phase 1: ephemeral ack items ---------------------------------------


def test_ack_item_carries_a_non_null_ack_id_and_a_null_result_id() -> None:
    scheduler = _scheduler()
    ack = enqueue_ack(scheduler, turn_id="turn-1")

    assert ack.role == ROLE_ACK
    assert ack.ack_id is not None
    assert ack.result_id is None


def test_result_item_still_requires_a_result_id() -> None:
    scheduler = _scheduler()
    result = enqueue(scheduler, "work-1", "answer")

    assert result.role == ROLE_RESULT
    assert result.result_id is not None
    assert result.ack_id is None


def test_ack_items_never_reach_session_state_speech_progress() -> None:
    """Plan: 'route every speech_progress-shaped emission ... through a
    single internal _emit_progress(item, state) helper that no-ops for
    item.role == "ack"'. Sweep enqueue, admission, interruption, cancellation,
    pause/resume, and terminalization and assert zero SessionState events are
    recorded for the ack item's utterance."""
    scheduler = _scheduler(speak=lambda _item: None)
    ack = enqueue_ack(scheduler, turn_id="turn-1")
    assert ack.utterance_id not in scheduler.state.speech

    asyncio.run(scheduler.start_next())
    assert ack.utterance_id not in scheduler.state.speech

    scheduler.synthesis_ended(ack.utterance_id)
    assert ack.utterance_id not in scheduler.state.speech

    scheduler.pause("ack-turn-1")
    assert ack.utterance_id not in scheduler.state.speech

    resumed = scheduler.resume("ack-turn-1")
    assert resumed is not None
    assert resumed.utterance_id not in scheduler.state.speech

    scheduler.cancel("ack-turn-1")
    assert resumed.utterance_id not in scheduler.state.speech


def test_discard_queued_ack_removes_only_the_named_ack_and_leaves_other_queues_untouched() -> None:
    scheduler = _scheduler()
    ack = enqueue_ack(scheduler, turn_id="turn-1")
    other = enqueue(scheduler, "work-other", "unrelated")

    discarded = scheduler.discard_queued_ack(ack.ack_id)

    assert discarded is not None
    assert "ack-turn-1" not in scheduler._queues
    assert scheduler._queues["work-other"] == [other]


def test_discard_queued_ack_cannot_remove_an_admitted_ack() -> None:
    """Plan: 'an admitted ack may finish but never blocks a result from
    being committed' -- discard only ever affects a still-queued ack."""
    scheduler = _scheduler()
    ack = enqueue_ack(scheduler, turn_id="turn-1")
    admitted = asyncio.run(scheduler.start_next())
    assert admitted is not None and admitted.role == ROLE_ACK

    discarded = scheduler.discard_queued_ack(ack.ack_id)

    assert not discarded
    assert scheduler.active is not None
    assert scheduler.active.item.ack_id == ack.ack_id


def test_normal_admission_clears_the_ack_index_entry() -> None:
    """The index only ever resolves still-queued acks, so a mapping left
    behind by normal admission is a per-ack leak on a long-lived connection."""
    scheduler = _scheduler()
    for index in range(3):
        ack = enqueue_ack(scheduler, turn_id=f"turn-{index}")
        admitted = asyncio.run(scheduler.start_next())
        assert admitted is not None and admitted.ack_id == ack.ack_id
        scheduler.delivery_unknown(admitted.utterance_id)

    assert scheduler._ack_index == {}


def test_cancel_with_the_ack_work_item_id_removes_only_the_parent_ack() -> None:
    """Plan: 'cancel(work_item_id) therefore only ever removes the ack when
    called with ack_work_item_id (i.e. a whole-turn cancel); cancelling a
    single delegated child's work_item_id does not touch the parent ack.'"""
    scheduler = _scheduler()
    ack = enqueue_ack(scheduler, turn_id="turn-1")
    child = enqueue(scheduler, "work-1-0", "child")

    cancelled = scheduler.cancel("work-1-0")

    assert [item.utterance_id for item in cancelled] == [child.utterance_id]
    assert "ack-turn-1" in scheduler._queues
    assert scheduler._queues["ack-turn-1"] == [ack]


def test_ack_admission_and_completion_do_not_block_a_ready_result_from_committing() -> None:
    """The ack is ephemeral, non-canonical delivery; its presence in the
    scheduler must never prevent a real result's canonical commit path (that
    commit happens above the scheduler, in SessionState/SessionHost -- this
    asserts only that the scheduler keeps the ack and the result item fully
    independent in its own bookkeeping)."""
    scheduler = _scheduler()
    enqueue_ack(scheduler, turn_id="turn-1")
    admitted_ack = asyncio.run(scheduler.start_next())
    assert admitted_ack is not None and admitted_ack.role == ROLE_ACK

    result = scheduler.enqueue(
        work_item_id="work-1-0",
        run_id="run-1-0",
        result_id="result-1-0",
        text="the real answer",
    )
    assert result.utterance_id in {item.utterance_id for item in scheduler._queues["work-1-0"]}

    scheduler.delivery_completed(admitted_ack.utterance_id)
    assert scheduler.active is None
    started = asyncio.run(scheduler.start_next("work-1-0"))
    assert started is not None and started.utterance_id == result.utterance_id


def test_start_next_drops_the_queue_key_once_its_last_item_is_admitted() -> None:
    """An admitted item must not leave an empty ``[]`` behind under its queue
    key: callers that ask "is any work still pending?" by looking at the queue
    keys would otherwise see a permanently truthy stale key."""
    scheduler = _scheduler()
    enqueue(scheduler, "work-1-0", "only item")

    admitted = asyncio.run(scheduler.start_next())

    assert admitted is not None
    assert "work-1-0" not in scheduler._queues
    assert scheduler.pending_work_item_ids() == frozenset()


def test_pending_work_item_ids_reports_only_non_empty_queues_and_honours_exclude() -> None:
    scheduler = _scheduler()
    enqueue_ack(scheduler, turn_id="turn-1")
    enqueue(scheduler, "work-1-0", "child")
    # Defence in depth: even a hand-planted empty queue must not be reported.
    scheduler._queues["work-1-1"] = []

    assert scheduler.pending_work_item_ids() == frozenset({"ack-turn-1", "work-1-0"})
    assert scheduler.pending_work_item_ids(exclude="ack-turn-1") == frozenset({"work-1-0"})


def test_no_tts_result_never_occupies_the_slot_waiting_for_the_start_timeout() -> None:
    """I2: ``_commit_and_speak(..., require_tts=False)`` enqueues the
    direct/unsupported/clarify replies on a connection with no TTS lane, so
    ``speak`` is ``None``. Admitting such an item would arm
    ``speech_start_timeout_seconds`` on a generation that structurally cannot
    be handed to a provider; it must terminalize before admission instead."""
    lifecycle = _lifecycle(tts_available=False)
    scheduler = _scheduler(lifecycle=lifecycle, speak=None)
    first = enqueue(scheduler, "work-1", "direct answer")
    second = enqueue(scheduler, "work-2", "clarify")

    assert asyncio.run(scheduler.start_next("work-1")) is None
    assert lifecycle.occupied is False
    assert scheduler.active is None
    assert lifecycle._timer_handles == {}
    assert scheduler.state.speech[first.utterance_id].state == DeliveryState.DELIVERY_UNKNOWN
    assert scheduler.pending_work_item_ids() == frozenset({"work-2"})

    # The next item is handled in the same way, immediately: nothing had to
    # wait for a start-timeout to expire before the slot could be reused.
    assert asyncio.run(scheduler.start_next("work-2")) is None
    assert scheduler.state.speech[second.utterance_id].state == DeliveryState.DELIVERY_UNKNOWN
    assert scheduler.pending_work_item_ids() == frozenset()


def test_start_next_with_no_key_probes_every_candidate_after_a_terminal_disposition() -> None:
    """Regression for round-4 deep-review logic finding #8 (minor,
    hardening): a probe-any-key ``start_next()`` call used to discard only
    the first candidate key's head item on a terminal pre-admission
    disposition and return ``None`` immediately, even though the transport
    slot was still provably free -- leaving every other queued key's item
    stranded until an unrelated caller happened to call ``start_next``
    again. It must keep probing remaining candidate keys in the same call.
    """
    lifecycle = _lifecycle(tts_available=False)
    scheduler = _scheduler(lifecycle=lifecycle, speak=None)
    first = enqueue(scheduler, "work-1", "direct answer")
    second = enqueue(scheduler, "work-2", "clarify")

    assert asyncio.run(scheduler.start_next()) is None

    assert scheduler.state.speech[first.utterance_id].state == DeliveryState.DELIVERY_UNKNOWN
    assert scheduler.state.speech[second.utterance_id].state == DeliveryState.DELIVERY_UNKNOWN
    assert scheduler.pending_work_item_ids() == frozenset()


def test_start_next_probes_the_same_keys_newly_exposed_second_item_after_a_terminal_head() -> None:
    """Regression for round-5 review-gauntlet Theme I: on a terminal
    pre-admission disposition, ``start_next`` discarded the candidate and
    moved on to the *next key* in its probe list without re-checking the
    same key's newly-exposed second item -- not a correctness break (the
    sibling is swept on the next call), but incomplete relative to the
    finding's own goal of clearing everything the transport slot's
    provable freedom allows in one call. Two items queued under the same
    work-item key must both be cleared by a single ``start_next()`` call.
    """
    lifecycle = _lifecycle(tts_available=False)
    scheduler = _scheduler(lifecycle=lifecycle, speak=None)
    first = scheduler.enqueue(
        result_id="result-first",
        work_item_id="work-1",
        run_id="run-first",
        text="first",
    )
    second = scheduler.enqueue(
        result_id="result-second",
        work_item_id="work-1",
        run_id="run-second",
        text="second",
    )

    assert asyncio.run(scheduler.start_next()) is None

    assert scheduler.state.speech[first.utterance_id].state == DeliveryState.DELIVERY_UNKNOWN
    assert scheduler.state.speech[second.utterance_id].state == DeliveryState.DELIVERY_UNKNOWN
    assert scheduler.pending_work_item_ids() == frozenset()


def test_submission_failure_advances_other_pending_work_without_a_further_start_next() -> None:
    """I10: the submission-failure path frees the coordinator slot with
    ``notify=False``, so the coordinator's ``on_terminal`` -- the only other
    reactive re-probe of the queue when the slot frees -- never fires. Items
    queued under another work-item key would otherwise stall until some
    unrelated caller happened to call ``start_next`` again."""

    async def run() -> None:
        calls = 0

        async def speak(_item) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("provider unavailable")

        scheduler = _scheduler(speak=speak)
        first = enqueue(scheduler, "work-1", "first")
        second = enqueue(scheduler, "work-2", "second")

        try:
            await scheduler.start_next("work-1")
        except RuntimeError as exc:
            assert str(exc) == "provider unavailable"
        else:
            raise AssertionError("speech provider failure was swallowed")

        # No further explicit start_next: the scheduler's own deferred
        # re-probe is the only thing that may advance the queue here.
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert scheduler.state.speech[first.utterance_id].state == DeliveryState.DELIVERY_UNKNOWN
        assert scheduler.active is not None
        assert scheduler.active.item.utterance_id == second.utterance_id

    asyncio.run(run())


def test_submission_failure_leaves_its_own_work_item_key_for_its_owner_to_retry() -> None:
    """The failed item's own key is excluded from the deferred re-probe: its
    owner catches the propagating exception and decides whether to re-queue
    (the early-ack retry path in ``pipeline._schedule_ack_admission``), so a
    replacement item queued under that key must still be there afterwards."""

    async def run() -> None:
        async def speak(_item) -> None:
            raise RuntimeError("provider unavailable")

        scheduler = _scheduler(speak=speak)
        enqueue(scheduler, "work-1", "first")

        try:
            await scheduler.start_next("work-1")
        except RuntimeError:
            retry = enqueue(scheduler, "work-1", "retry")
        else:
            raise AssertionError("speech provider failure was swallowed")

        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert scheduler.active is None
        assert scheduler.pending_work_item_ids() == frozenset({"work-1"})
        assert scheduler._queues["work-1"][0].utterance_id == retry.utterance_id

    asyncio.run(run())


def test_clean_delivery_reports_a_completed_terminal_disposition() -> None:
    """M1: the coordinator-internal disposition for a completed delivery must
    not be recorded as DELIVERY_UNKNOWN."""

    async def run() -> None:
        dispositions: list[DeliveryDisposition] = []

        def on_terminal(_token, _identity, disposition) -> None:
            dispositions.append(disposition)

        lifecycle = _lifecycle(on_terminal=on_terminal)
        scheduler = _scheduler(lifecycle=lifecycle)
        item = enqueue(scheduler, "work-1", "answer")
        await scheduler.start_next()

        scheduler.delivery_completed(item.utterance_id)
        await asyncio.sleep(0)

        assert dispositions == [DeliveryDisposition.DELIVERY_COMPLETED]
        assert scheduler.state.speech[item.utterance_id].state == DeliveryState.DELIVERY_COMPLETED

    asyncio.run(run())
