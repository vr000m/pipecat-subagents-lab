"""Unit tests for ``server.task_retention.retain_until_done`` -- the shared
add/discard fire-and-forget idiom (SessionHost decomposition plan, Phase 5).
"""

from __future__ import annotations

import asyncio
import contextlib

from server.task_retention import retain_until_done


async def _sleep_then_return(seconds: float, value: object = None) -> object:
    await asyncio.sleep(seconds)
    return value


async def _sleep_then_raise(seconds: float, exc: Exception) -> None:
    await asyncio.sleep(seconds)
    raise exc


def test_retains_the_task_until_it_completes() -> None:
    """The task stays in the tracking set for as long as it is running."""

    async def run() -> None:
        tracking: set[asyncio.Task[object]] = set()
        task = retain_until_done(asyncio.create_task(_sleep_then_return(0.05)), tracking)

        # Still pending: retained.
        assert task in tracking

        await task

        # Done: discarded.
        assert task not in tracking

    asyncio.run(run())


def test_discards_after_done_without_anyone_awaiting_the_task() -> None:
    """Discard happens off the completion callback, not off an explicit await
    at the call site -- retention must not depend on anyone awaiting the
    fire-and-forget task."""

    async def run() -> None:
        tracking: set[asyncio.Task[object]] = set()
        task = retain_until_done(asyncio.create_task(_sleep_then_return(0.01, "done")), tracking)

        # Give the loop a chance to run the task to completion and fire the
        # done-callback, without anyone awaiting `task` directly.
        await asyncio.sleep(0.05)

        assert task not in tracking
        assert task.result() == "done"

    asyncio.run(run())


def test_exception_handling_task_still_discarded_and_on_done_observes_it() -> None:
    """A task that raises is still discarded exactly like a successful one,
    and an ``on_done`` callback can retrieve the exception -- preserving the
    exception-observation semantics some call sites need without leaking
    an "exception was never retrieved" warning."""

    async def run() -> None:
        tracking: set[asyncio.Task[object]] = set()
        observed: list[BaseException | None] = []

        def on_done(completed: asyncio.Task[object]) -> None:
            observed.append(completed.exception())

        boom = ValueError("boom")
        task = retain_until_done(
            asyncio.create_task(_sleep_then_raise(0.01, boom)),
            tracking,
            on_done=on_done,
        )

        await asyncio.sleep(0.05)

        assert task not in tracking
        assert observed == [boom]

    asyncio.run(run())


def test_on_done_runs_after_discard_not_before() -> None:
    """Ordering guarantee: by the time ``on_done`` runs, the task has
    already been removed from the tracking set."""

    async def run() -> None:
        tracking: set[asyncio.Task[object]] = set()
        seen_in_tracking_during_on_done: list[bool] = []

        def on_done(completed: asyncio.Task[object]) -> None:
            seen_in_tracking_during_on_done.append(completed in tracking)

        task = retain_until_done(
            asyncio.create_task(_sleep_then_return(0.01)),
            tracking,
            on_done=on_done,
        )
        await asyncio.sleep(0.05)

        assert seen_in_tracking_during_on_done == [False]
        assert task not in tracking

    asyncio.run(run())


def test_accepts_a_bare_future_not_just_a_task() -> None:
    """The helper works for ``asyncio.Future`` inputs too (e.g. from
    ``asyncio.ensure_future`` on something that isn't a coroutine, or a
    hand-created ``Future``), not only ``asyncio.Task``."""

    async def run() -> None:
        loop = asyncio.get_running_loop()
        tracking: set[asyncio.Future[object]] = set()
        future: asyncio.Future[object] = loop.create_future()

        retained = retain_until_done(future, tracking)
        assert retained is future
        assert future in tracking

        future.set_result("value")
        # Done-callbacks scheduled via call_soon; yield once for it to run.
        await asyncio.sleep(0)

        assert future not in tracking

    asyncio.run(run())


def test_ensure_future_on_a_coroutine_is_also_accepted() -> None:
    """``asyncio.ensure_future`` on a coroutine returns a ``Task``, the
    other shape every hand-rolled call site actually retains today."""

    async def run() -> None:
        tracking: set[asyncio.Future[object]] = set()
        future = retain_until_done(asyncio.ensure_future(_sleep_then_return(0.01, 42)), tracking)

        assert future in tracking
        result = await future
        assert result == 42
        assert future not in tracking

    asyncio.run(run())


def test_cancellation_still_discards_and_on_done_observes_it() -> None:
    """A cancelled task is discarded exactly like any other terminal task,
    and an ``on_done`` callback can observe the cancellation without
    ``exception()`` raising ``CancelledError`` out of the callback --
    mirroring ``work_item_coordinator._consume_task_exception``."""

    async def run() -> None:
        tracking: set[asyncio.Task[object]] = set()
        observed: list[bool] = []

        def on_done(completed: asyncio.Task[object]) -> None:
            if completed.cancelled():
                observed.append(True)
                return
            try:
                completed.exception()
            except asyncio.CancelledError:
                pass

        task = retain_until_done(
            asyncio.create_task(_sleep_then_return(0.05)),
            tracking,
            on_done=on_done,
        )

        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert task not in tracking
        assert task.cancelled() is True
        assert observed == [True]

    asyncio.run(run())


def test_discard_is_safe_if_the_task_was_already_removed() -> None:
    """``set.discard`` never raises for a missing element -- retention must
    tolerate a caller (e.g. a shutdown sweep) removing the task first."""

    async def run() -> None:
        tracking: set[asyncio.Task[object]] = set()
        task = retain_until_done(asyncio.create_task(_sleep_then_return(0.01)), tracking)

        tracking.discard(task)  # simulate an external removal
        await task
        await asyncio.sleep(0)  # let the done-callback run

        assert task not in tracking

    asyncio.run(run())
