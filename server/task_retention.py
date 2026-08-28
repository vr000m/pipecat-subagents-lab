"""Shared fire-and-forget task/future retention idiom.

A coroutine scheduled with ``asyncio.create_task``/``asyncio.ensure_future``
and never awaited is held only by a weak reference from the event loop --
nothing stops it from being garbage-collected mid-flight. The idiom used
throughout ``server/`` to prevent that is: add the task to a caller-owned
set for the duration of its run, then discard it once it completes::

    self._x_tasks.add(task)
    task.add_done_callback(self._x_tasks.discard)

This module gives that idiom a single, tested implementation so the next
fire-and-forget site reuses it instead of hand-rolling a subtly different
copy. It is a leaf utility: no imports from anywhere else in ``server/``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, MutableSet
from typing import Any, TypeVar

_TaskOrFuture = TypeVar("_TaskOrFuture", bound="asyncio.Future[Any]")


def retain_until_done(
    task: _TaskOrFuture,
    tracking: MutableSet[_TaskOrFuture],
    *,
    on_done: Callable[[_TaskOrFuture], None] | None = None,
) -> _TaskOrFuture:
    """Retain ``task`` in ``tracking`` until it finishes, then discard it.

    Accepts either an ``asyncio.Task`` or a bare ``asyncio.Future`` -- both
    expose ``add_done_callback``, and the retention idiom does not care
    which one it is holding a reference to.

    ``tracking`` is a caller-owned ``MutableSet`` (almost always a plain
    ``set``); ``task`` is added to it immediately, and removed by the
    completion callback regardless of how the task ended (success,
    exception, or cancellation) -- ``discard`` never raises if the task
    was already removed some other way.

    ``on_done``, if given, runs *after* the discard, once, with the
    completed task/future -- for callers that also need to observe the
    outcome (e.g. retrieve and log an exception) without re-implementing
    the add/discard bookkeeping themselves. It does not change what is
    retained or when it is discarded; it only lets a call site keep its
    existing exception-observation behavior while sharing this idiom.

    Returns ``task`` unchanged, so a call site can do
    ``self._x = retain_until_done(asyncio.create_task(...), self._x_tasks)``
    in one line where that reads better.
    """
    tracking.add(task)

    if on_done is None:
        task.add_done_callback(tracking.discard)
    else:

        def _completed(completed: _TaskOrFuture) -> None:
            tracking.discard(completed)
            on_done(completed)

        task.add_done_callback(_completed)

    return task
