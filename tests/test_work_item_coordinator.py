"""Bounded work items preserve accepted order and isolate worker contexts."""

import asyncio

from server.work_item_coordinator import WorkItemCoordinator


class FakeSpeechScheduler:
    def __init__(self) -> None:
        self.spoken: list[str] = []

    async def speak(self, text: str, **_: object) -> None:
        self.spoken.append(text)


async def completed_worker(text: str) -> dict:
    return {"text": text, "citations": []}


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
