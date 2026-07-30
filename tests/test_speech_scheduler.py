"""Speech scheduling owns leases, queues, and frame ownership."""

import asyncio

from server.contracts import DeliveryState
from server.session_state import SessionState
from server.speech_lifecycle import GenerationIdentity, SpeechLifecycleCoordinator
from server.speech_scheduler import ROLE_RESULT, ROLE_TIMEOUT_NOTICE, SpeechScheduler


def enqueue(scheduler: SpeechScheduler, work_item_id: str, text: str):
    return scheduler.enqueue(
        work_item_id=work_item_id,
        run_id=f"run-{work_item_id}",
        result_id=f"result-{work_item_id}",
        text=text,
    )


def test_one_active_utterance_lease_and_per_work_item_queue_isolation() -> None:
    scheduler = SpeechScheduler(SessionState())
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
    scheduler = SpeechScheduler(SessionState())
    first = enqueue(scheduler, "work-1", "answer")
    assert asyncio.run(scheduler.start_next()) is not None
    scheduler.interrupt()

    replay = scheduler.resume(first)
    assert replay.utterance_id != first.utterance_id
    assert replay.text == "answer"


def test_pause_preserves_paused_state_and_resume_records_resumed_transition() -> None:
    scheduler = SpeechScheduler(SessionState())
    first = enqueue(scheduler, "work-1", "answer")
    asyncio.run(scheduler.start_next())

    scheduler.pause("work-1")
    assert scheduler.active is None
    assert scheduler.paused("work-1") == first
    assert scheduler.state.speech[first.utterance_id].state == DeliveryState.PAUSED

    replay = scheduler.resume("work-1")
    assert replay is not None
    assert scheduler.state.speech[replay.utterance_id].state == DeliveryState.RESUMED


def test_scheduler_stop_is_task_local_when_other_work_is_queued() -> None:
    async def run() -> None:
        scheduler = SpeechScheduler(SessionState())
        task_one = enqueue(scheduler, "work-1", "one")
        task_two = enqueue(scheduler, "work-2", "two")
        await scheduler.start_next()
        scheduler.interrupt()

        assert scheduler.state.speech[task_one.utterance_id].state == DeliveryState.INTERRUPTED
        assert scheduler._queues["work-2"][0].utterance_id == task_two.utterance_id

    asyncio.run(run())


def test_discard_queued_notice_removes_only_timeout_notice_role_and_preserves_order() -> None:
    scheduler = SpeechScheduler(SessionState())
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
    scheduler = SpeechScheduler(SessionState())
    item = enqueue(scheduler, "work-b", "just a result")

    discarded = scheduler.discard_queued_notice("work-b")

    assert discarded == ()
    assert scheduler._queues["work-b"] == [item]


def test_discard_queued_notice_is_a_noop_for_an_unknown_work_item() -> None:
    scheduler = SpeechScheduler(SessionState())

    assert scheduler.discard_queued_notice("no-such-work-item") == ()


def test_discard_queued_notice_does_not_touch_other_work_item_queues() -> None:
    scheduler = SpeechScheduler(SessionState())
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
    scheduler = SpeechScheduler(SessionState())
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
    scheduler = SpeechScheduler(
        SessionState(),
        stop=lambda item: stopped.append(item.utterance_id),
    )
    item = enqueue(scheduler, "work-1", "one")
    asyncio.run(scheduler.start_next())

    scheduler.interrupt()

    assert stopped == [item.utterance_id]
    assert scheduler.active is None


def test_synthesis_end_is_not_completion_and_unknown_delivery_is_terminal() -> None:
    scheduler = SpeechScheduler(SessionState())
    item = enqueue(scheduler, "work-1", "answer")
    asyncio.run(scheduler.start_next())

    scheduler.synthesis_ended(item.utterance_id)
    assert scheduler.state.speech[item.utterance_id].state == DeliveryState.SYNTHESIS_ENDED
    assert scheduler.active is not None

    scheduler.delivery_unknown(item.utterance_id)
    assert scheduler.state.speech[item.utterance_id].state == DeliveryState.DELIVERY_UNKNOWN
    assert scheduler.active is None


def test_synthesis_failure_releases_lease_and_allows_next_item() -> None:
    calls = 0

    async def speak(_item):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("provider unavailable")

    scheduler = SpeechScheduler(SessionState(), speak=speak)
    first = enqueue(scheduler, "work-1", "first")
    second = enqueue(scheduler, "work-2", "second")

    try:
        asyncio.run(scheduler.start_next())
    except RuntimeError as exc:
        assert str(exc) == "provider unavailable"
    else:
        raise AssertionError("speech provider failure was swallowed")

    assert scheduler.active is None
    assert scheduler.state.speech[first.utterance_id].state == DeliveryState.DELIVERY_UNKNOWN
    assert asyncio.run(scheduler.start_next()).utterance_id == second.utterance_id


def test_speech_submission_failure_releases_lifecycle_generation() -> None:
    async def run() -> None:
        async def cleanup_failed(*_args) -> None:
            raise RuntimeError("cleanup unavailable")

        lifecycle = SpeechLifecycleCoordinator(dispatch_cleanup=cleanup_failed)

        async def speak(_item):
            raise RuntimeError("provider unavailable")

        scheduler = SpeechScheduler(SessionState(), speak=speak, lifecycle=lifecycle)
        item = enqueue(scheduler, "work-1", "first")

        try:
            await scheduler.start_next()
        except RuntimeError as exc:
            assert str(exc) == "provider unavailable"
        else:
            raise AssertionError("speech provider failure was swallowed")

        assert scheduler.active is None
        assert lifecycle.occupied is False
        assert scheduler.state.speech[item.utterance_id].state == DeliveryState.DELIVERY_UNKNOWN

    asyncio.run(run())


def test_reconnect_terminally_cancels_active_and_queued_old_epoch_items() -> None:
    scheduler = SpeechScheduler(SessionState())
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


def test_delayed_callbacks_from_reconnected_utterance_are_ignored() -> None:
    scheduler = SpeechScheduler(SessionState())
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
    coordinator = SpeechLifecycleCoordinator()
    scheduler = SpeechScheduler(SessionState(), lifecycle=coordinator)
    item = enqueue(scheduler, "work-1", "one")

    result = asyncio.run(scheduler.start_next())

    assert result is not None
    assert coordinator.occupied is True
    assert coordinator.generation_for_token(scheduler.active.token) is not None
    assert coordinator.generation_for_token(scheduler.active.token).identity.utterance_id == (
        item.utterance_id
    )


def test_start_next_does_not_start_when_the_coordinator_slot_is_already_occupied() -> None:
    coordinator = SpeechLifecycleCoordinator()
    coordinator.try_admit(GenerationIdentity("occupied-by-someone-else", "work-other"))
    scheduler = SpeechScheduler(SessionState(), lifecycle=coordinator)
    enqueue(scheduler, "work-1", "one")

    result = asyncio.run(scheduler.start_next())

    assert result is None
    assert scheduler.active is None


def test_dropped_prestart_context_cannot_claim_replacement_utterance() -> None:
    scheduler = SpeechScheduler(SessionState(), speak=lambda _item: None)
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
