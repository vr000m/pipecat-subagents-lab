"""Speech scheduling owns leases, queues, and frame ownership."""

import asyncio

from server.session_state import SessionState
from server.speech_scheduler import SpeechScheduler
from server.contracts import DeliveryState


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
