"""Bounded work items preserve accepted order and isolate worker contexts."""

import asyncio

from server.work_item_coordinator import PendingDialogue, WorkItemCoordinator


class FakeSpeechScheduler:
    def __init__(self) -> None:
        self.spoken: list[str] = []

    async def speak(self, text: str, **_: object) -> None:
        self.spoken.append(text)


async def completed_worker(text: str) -> dict:
    return {"text": text, "citations": []}


def test_compound_pending_reply_is_classified_as_multi_intent() -> None:
    coordinator = WorkItemCoordinator(max_work_items_per_turn=2, clock=lambda: 0)
    coordinator.add_pending(PendingDialogue("session", "worker", "worker-1", "turn", "result", 10))
    outcome = coordinator.arbitrate("session", "yes, and also search for news")
    assert outcome.kind == "multi_intent"


def test_control_and_consent_outcomes_preserve_the_requested_action() -> None:
    coordinator = WorkItemCoordinator(max_work_items_per_turn=2, clock=lambda: 0)
    coordinator.add_pending(PendingDialogue("session", "worker", "worker-1", "turn", "result", 10))

    assert coordinator.arbitrate("session", "cancel").control_action == "cancel"
    assert coordinator.arbitrate("session", "consent").kind == "continue_pending"


def test_same_worker_turns_are_causal_but_different_workers_can_run_concurrently() -> None:
    observed: list[str] = []

    async def worker(worker_id: str, text: str) -> dict:
        observed.append(f"start:{worker_id}:{text}")
        await asyncio.sleep(0)
        observed.append(f"end:{worker_id}:{text}")
        return await completed_worker(text)

    async def run() -> None:
        coordinator = WorkItemCoordinator(
            max_work_items_per_turn=2,
            speech_scheduler=FakeSpeechScheduler(),
        )
        await coordinator.submit("turn-a", [("worker-weather", "A")], worker)
        await coordinator.submit("turn-b", [("worker-weather", "B")], worker)
        await coordinator.submit("turn-c", [("worker-news", "C")], worker)

    asyncio.run(run())
    assert observed.index("end:worker-weather:A") < observed.index("start:worker-weather:B")


def test_multi_intent_is_bounded_and_timeout_keeps_completed_results() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(
            max_work_items_per_turn=2,
            wait_timeout_ms=1,
            speech_scheduler=FakeSpeechScheduler(),
        )

        async def worker(_worker_id: str, text: str) -> dict:
            if text == "slow":
                await asyncio.sleep(0.02)
            return await completed_worker(text)

        outcome = await coordinator.submit(
            "turn-1",
            [("worker-weather", "fast"), ("worker-news", "slow"), ("worker-extra", "ignored")],
            worker,
        )
        assert len(outcome.work_items) == 2
        assert outcome.results[0].text == "fast"
        assert outcome.pending_work_item_ids

    asyncio.run(run())


def test_timeout_boundary_completion_is_retained_after_wait_returns_stale_done_set(
    monkeypatch,
) -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(max_work_items_per_turn=2, wait_timeout_ms=1)
        real_wait = asyncio.wait

        async def stale_wait(tasks, *, timeout):
            await real_wait(tasks, timeout=timeout)
            # Model the event-loop boundary where the task has completed but
            # asyncio.wait returned before its completion was observed.
            await asyncio.gather(*tasks)
            return set(), set(tasks)

        monkeypatch.setattr(asyncio, "wait", stale_wait)

        async def worker(_worker_id: str, text: str) -> dict:
            return await completed_worker(text)

        outcome = await coordinator.submit("turn-boundary", [("worker", "done")], worker)
        assert [result.text for result in outcome.results] == ["done"]
        assert outcome.pending_work_item_ids == ()

    asyncio.run(run())


def test_immediate_worker_failure_is_retained_without_reordering_successes() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(max_work_items_per_turn=3, wait_timeout_ms=100)

        async def worker(worker_id: str, text: str) -> dict:
            if text == "fail":
                raise ValueError("provider detail should not be exposed")
            return {"text": text, "citations": []}

        outcome = await coordinator.submit(
            "turn-failure",
            [("worker-a", "first"), ("worker-b", "fail"), ("worker-c", "last")],
            worker,
        )

        assert [result.text for result in outcome.results] == ["first", "last"]
        assert len(outcome.failures) == 1
        assert outcome.failures[0].work_item_id == "turn-failure-1"
        assert outcome.failures[0].worker_id == "worker-b"
        assert outcome.failures[0].error_type == "ValueError"
        assert outcome.failures[0].error_message == "worker execution failed"

    asyncio.run(run())


def test_timed_out_result_is_drained_once_after_pending_worker_finishes() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(max_work_items_per_turn=2, wait_timeout_ms=1)
        release = asyncio.Event()

        async def worker(worker_id: str, text: str) -> dict:
            assert worker_id == "worker-late"
            await release.wait()
            return {"text": text, "citations": []}

        outcome = await coordinator.submit("turn-late", [("worker-late", "answer")], worker)
        assert outcome.pending_work_item_ids == ("turn-late-0",)
        assert coordinator.drain_late_results() == ()

        release.set()
        for _ in range(4):
            await asyncio.sleep(0)

        late = coordinator.drain_late_results()
        assert len(late) == 1
        assert late[0].work_item_id == "turn-late-0"
        assert late[0].worker_id == "worker-late"
        assert late[0].result.text == "answer"
        assert late[0].error is None
        assert coordinator.drain_late_results() == ()

    asyncio.run(run())
