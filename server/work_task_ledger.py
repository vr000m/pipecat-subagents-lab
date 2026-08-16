"""Work-item/turn task bookkeeping owned by a single SessionHost.

Extracted from SessionHost (deep-review Architecture finding #3): SessionHost
was carrying four bookkeeping fields for the tasks *it* creates and awaits
(turn handler tasks and delegated work-item tasks), manipulated by three
methods scattered through a much larger class.

This registry is deliberately separate from ``WorkItemCoordinator``'s own
``_work_tasks`` registry in ``server/work_item_coordinator.py``. That split is
load-bearing, not incidental duplication:

- Ownership follows creation. SessionHost has no handle to tasks the
  coordinator creates internally in ``submit()``, and the coordinator has no
  handle to SessionHost's turn/work tasks. Merging the two registries would
  require threading task handles across an ownership boundary that doesn't
  otherwise exist.
- Lifetimes differ. The coordinator's ``_work_tasks`` entries deliberately
  outlive the turn handler into a background ``retain_late_task`` phase;
  this ledger's entries do not.

Do not merge this class with ``WorkItemCoordinator``'s task registry.

This module is a pure extraction of SessionHost's existing storage and
tracking/cancellation logic -- semantics are unchanged from the code that
previously lived directly on SessionHost. SessionHost still owns most of the
*read* call sites for now (via property forwarding to this ledger's
containers); reconciling the three inconsistent "which work items are live"
computations spread across ``_cancel_work``, ``cancel_turn_or_child``, and
``shutdown`` is a later step, not this one.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from typing import Any


class WorkTaskLedger:
    """Tracks a SessionHost's own in-flight turn/work-item tasks.

    ``turn_tasks`` holds at most one active turn-handler task per work item
    (the ack-latched, top-level task for a turn). ``work_tasks`` holds the
    set of delegated child work-item tasks a turn has spawned. ``known_ids``
    is the superset of every work item this ledger has ever tracked and not
    yet retired -- it is what lets a whole-connection cancel reach a work
    item whose task already completed. ``cancelled_ids`` remembers which
    known ids were explicitly cancelled, independent of whether their task
    is still running, so late callbacks and status reconciliation can still
    see the cancellation after the task itself is gone.
    """

    def __init__(self) -> None:
        self.turn_tasks: dict[str, asyncio.Task[Any]] = {}
        self.work_tasks: dict[str, set[asyncio.Task[Any]]] = {}
        self.known_ids: set[str] = set()
        self.cancelled_ids: set[str] = set()

    def register_work_task(self, work_item_id: str, task: asyncio.Task[Any]) -> None:
        """Track a delegated child work-item task."""
        self.known_ids.add(work_item_id)
        self.work_tasks.setdefault(work_item_id, set()).add(task)

        def completed(completed_task: asyncio.Task[Any]) -> None:
            tasks = self.work_tasks.get(work_item_id)
            if tasks is None:
                return
            tasks.discard(completed_task)
            if not tasks:
                self.work_tasks.pop(work_item_id, None)

        task.add_done_callback(completed)

    def register_turn_task(self, work_item_id: str, task: asyncio.Task[Any]) -> None:
        """Track a turn-handler task."""
        self.known_ids.add(work_item_id)
        self.turn_tasks[work_item_id] = task

        def completed(completed_task: asyncio.Task[Any]) -> None:
            if self.turn_tasks.get(work_item_id) is not completed_task:
                return
            self.turn_tasks.pop(work_item_id, None)
            if work_item_id not in self.work_tasks:
                self.known_ids.discard(work_item_id)
                self.cancelled_ids.discard(work_item_id)

        task.add_done_callback(completed)

    def register_known(self, work_item_id: str) -> None:
        """Record a work item as known without an accompanying local task."""
        self.known_ids.add(work_item_id)

    def retire(self, work_item_id: str) -> None:
        """Drop a work item from the known set once its fate is settled."""
        self.known_ids.discard(work_item_id)

    def mark_cancelled(self, work_item_id: str) -> None:
        self.cancelled_ids.add(work_item_id)

    def is_cancelled(self, work_item_id: str) -> bool:
        return work_item_id in self.cancelled_ids

    def live_ids(self) -> set[str]:
        """Union of every work item this ledger currently knows is live or known."""
        return set(self.turn_tasks) | set(self.work_tasks) | self.known_ids

    def known_ids_snapshot(self) -> frozenset[str]:
        return frozenset(self.known_ids)

    def local_ids(self) -> tuple[str, ...]:
        """Ids with a locally tracked turn and/or work task, dedup-ordered."""
        return tuple(dict.fromkeys((*self.turn_tasks, *self.work_tasks)))

    def cancel_selected(self, selected_work_item_ids: Iterable[str]) -> None:
        """Mark the given ids cancelled and cancel any locally tracked tasks.

        The caller (``SessionHost._cancel_work``) determines the full
        selection -- which may include ids sourced from the coordinator or
        from ``known_ids`` that have no locally tracked task at all -- this
        method just applies the bookkeeping and cancels whatever local tasks
        exist for the selected ids.
        """
        ids = tuple(selected_work_item_ids)
        self.cancelled_ids.update(ids)
        for item_id in ids:
            turn_task = self.turn_tasks.get(item_id)
            if turn_task is not None:
                turn_task.cancel()
            for task in self.work_tasks.get(item_id, ()):
                task.cancel()
