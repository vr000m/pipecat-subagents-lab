"""Unit tests for ``WorkTaskLedger``, in particular ``live_ids()`` -- the
one shared "which work items are live" computation ``SessionHost``'s three
consumers (``_cancel_work``, ``cancel_turn_or_child``, ``shutdown``) are
meant to agree on (Step 5 of the SessionHost decomposition plan, closing
deep-review Architecture finding #3)."""

from __future__ import annotations

import asyncio

from server.work_task_ledger import WorkTaskLedger


async def _noop() -> None:
    await asyncio.sleep(3600)


def test_live_ids_is_the_union_of_turn_tasks_work_tasks_and_known_ids() -> None:
    """Property-style: however the three containers are populated,
    ``live_ids()`` is exactly their union -- no source is silently dropped,
    and no id is fabricated that isn't backed by one of the three."""

    async def run() -> None:
        ledger = WorkTaskLedger()

        # Empty ledger: empty union.
        assert ledger.live_ids() == set()

        turn_task = asyncio.create_task(_noop())
        work_task = asyncio.create_task(_noop())
        try:
            ledger.register_turn_task("turn-only", turn_task)
            ledger.register_work_task("work-only", work_task)
            ledger.register_known("known-only")

            assert ledger.live_ids() == {"turn-only", "work-only", "known-only"}

            # An id can be simultaneously turn-tracked, work-tracked, and
            # known -- still counted exactly once.
            ledger.register_known("turn-only")
            assert ledger.live_ids() == {"turn-only", "work-only", "known-only"}
        finally:
            turn_task.cancel()
            work_task.cancel()
            await asyncio.gather(turn_task, work_task, return_exceptions=True)

    asyncio.run(run())


def test_pending_tasks_returns_every_locally_tracked_task_not_just_turn_tasks() -> None:
    """Regression: ``shutdown`` used to gather only ``turn_tasks``, leaving
    delegated ``work_tasks`` with no local turn task above them (e.g. a
    retained background child) uncancelled and unawaited past shutdown."""

    async def run() -> None:
        ledger = WorkTaskLedger()
        turn_task = asyncio.create_task(_noop())
        work_task_a = asyncio.create_task(_noop())
        work_task_b = asyncio.create_task(_noop())
        try:
            ledger.register_turn_task("turn-item", turn_task)
            ledger.register_work_task("work-item", work_task_a)
            ledger.register_work_task("work-item", work_task_b)

            pending = set(ledger.pending_tasks())
            assert pending == {turn_task, work_task_a, work_task_b}
        finally:
            turn_task.cancel()
            work_task_a.cancel()
            work_task_b.cancel()
            await asyncio.gather(turn_task, work_task_a, work_task_b, return_exceptions=True)

    asyncio.run(run())
