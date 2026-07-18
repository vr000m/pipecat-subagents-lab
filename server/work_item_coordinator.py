"""Bounded work-item dispatch and pending-dialogue arbitration."""

from __future__ import annotations

import re
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable

from .config import Config
from .contracts import RoutingDecision
from .registry import WorkerRegistry
from .router import Router, WorkerCatalogue, validate_decision


@dataclass(frozen=True)
class PendingDialogue:
    session_id: str
    owner_kind: str
    owner_id: str
    turn_id: str
    result_id: str
    expires_at: float


@dataclass(frozen=True)
class DispatchOutcome:
    kind: str
    transcript: str
    decision: RoutingDecision | None = None
    work_items: tuple[str, ...] = ()
    control_action: str | None = None


@dataclass(frozen=True)
class SubmittedOutcome:
    work_items: tuple[Any, ...]
    results: tuple[Any, ...]
    pending_work_item_ids: tuple[str, ...] = ()
    failures: tuple["WorkItemFailure", ...] = ()


@dataclass(frozen=True)
class WorkItemFailure:
    work_item_id: str
    worker_id: str
    error_type: str
    error_message: str


@dataclass(frozen=True)
class LateResult:
    work_item_id: str
    worker_id: str
    result: Any = None
    error: str | None = None


class WorkItemCoordinator:
    def __init__(
        self,
        registry: WorkerRegistry | None = None,
        router: Router | None = None,
        config: Config | None = None,
        clock: Callable[[], float] = time.monotonic,
        max_work_items: int | None = None,
        max_work_items_per_turn: int | None = None,
        wait_timeout_ms: int | None = None,
        speech_scheduler: Any = None,
    ) -> None:
        self.registry, self.router, self.config, self.clock = (
            registry,
            router,
            config or (registry.config if registry else Config()),
            clock,
        )
        max_work_items = max_work_items if max_work_items is not None else max_work_items_per_turn
        if max_work_items is not None:
            self.config = Config(
                max_work_items_per_turn=max_work_items,
                multi_intent_wait_timeout_ms=wait_timeout_ms or 10_000,
            )
        self.speech_scheduler = speech_scheduler
        self._pending: dict[str, PendingDialogue] = {}
        self._tails: dict[str, Any] = {}
        self._late_results: deque[LateResult] = deque()

    def drain_late_results(self) -> tuple[LateResult, ...]:
        """Return and remove results from timed-out work items in completion order."""
        results = tuple(self._late_results)
        self._late_results.clear()
        return results

    def add_pending(self, candidate: PendingDialogue) -> None:
        self._pending[candidate.session_id] = candidate

    def pending(self, session_id: str) -> PendingDialogue | None:
        candidate = self._pending.get(session_id)
        if candidate and candidate.expires_at <= self.clock():
            self._pending.pop(session_id, None)
            return None
        return candidate

    @staticmethod
    def control_intent(transcript: str) -> tuple[str, str | None] | None:
        match = re.match(
            r"^\s*(pause|resume|cancel|stop|consent)\b(?:\s+(work[-_ ]item[-_ ]\w+))?",
            transcript,
            re.I,
        )
        if not match:
            return None
        action = "cancel" if match.group(1).lower() == "stop" else match.group(1).lower()
        target = match.group(2).replace(" ", "_") if match.group(2) else None
        return action, target

    def arbitrate(self, session_id: str, transcript: str) -> DispatchOutcome:
        control = self.control_intent(transcript)
        if control:
            pending = self.pending(session_id)
            if control[0] == "consent" and pending:
                return DispatchOutcome(
                    "continue_pending",
                    transcript,
                    work_items=(pending.owner_id,),
                    control_action="consent",
                )
            return DispatchOutcome(
                "control",
                transcript,
                work_items=(control[1],) if control[1] else (),
                control_action=control[0],
            )
        pending = self.pending(session_id)
        if pending and re.search(r"\b(and|also)\b", transcript, re.I):
            parts = tuple(
                part.strip()
                for part in re.split(r"\band\b|\balso\b", transcript, flags=re.I)
                if part.strip()
            )
            if len(parts) > self.config.max_work_items_per_turn:
                parts = parts[: self.config.max_work_items_per_turn]
            return DispatchOutcome("multi_intent", transcript, work_items=parts)
        if pending and transcript.lower().startswith(("yes", "continue", "that", "please search")):
            return DispatchOutcome("continue_pending", transcript, work_items=(pending.owner_id,))
        if self.registry is None or self.router is None:
            raise RuntimeError("routing arbitration requires a registry and router")
        catalogue = self.registry.catalogue()
        decision = self.router.route(transcript, catalogue)
        return DispatchOutcome("routed", transcript, decision=decision)

    def dispatch(
        self,
        decision: RoutingDecision,
        operation: Callable[[Any], Any] | None = None,
        catalogue: WorkerCatalogue | None = None,
    ) -> Any:
        if self.registry is None:
            raise RuntimeError("dispatch requires a worker registry")
        validate_decision(decision, catalogue or self.registry.catalogue())
        if decision.action == "new_worker":
            worker = self.registry.get_or_create(
                topic=decision.topic or "",
                worker_type=decision.worker_type or "",
                model_policy=decision.model_policy or "",
            )
        else:
            worker = self.registry.get(decision.worker_id) if decision.worker_id else None
        if worker is None:
            return None
        if operation is None:
            return worker.worker
        return operation(worker.worker)

    async def submit(
        self, turn_id: str, items: list[tuple[str, str]], worker: Callable[[str, str], Any]
    ) -> SubmittedOutcome:
        import asyncio

        selected = items[: self.config.max_work_items_per_turn]
        work = tuple(
            type("WorkItem", (), {"work_item_id": f"{turn_id}-{i}"})()
            for i, _ in enumerate(selected)
        )

        def materialize_result(value: Any) -> Any:
            return type("Result", (), value)() if isinstance(value, dict) else value

        def retain_late_result(task: asyncio.Task[Any], work_item_id: str, worker_id: str) -> None:
            try:
                result = materialize_result(task.result())
            except BaseException as exc:
                self._late_results.append(
                    LateResult(
                        work_item_id=work_item_id,
                        worker_id=worker_id,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )
            else:
                self._late_results.append(
                    LateResult(
                        work_item_id=work_item_id,
                        worker_id=worker_id,
                        result=result,
                    )
                )

        async def one(worker_id: str, text: str) -> Any:
            previous = self._tails.get(worker_id)
            if previous is not None:
                await previous
            task = asyncio.create_task(worker(worker_id, text))
            self._tails[worker_id] = task
            try:
                return await task
            finally:
                if self._tails.get(worker_id) is task:
                    self._tails.pop(worker_id, None)

        tasks = [asyncio.create_task(one(worker_id, text)) for worker_id, text in selected]
        await asyncio.wait(tasks, timeout=self.config.multi_intent_wait_timeout_ms / 1000)
        # asyncio.wait can return its done set before a completion callback that
        # crossed the timeout boundary has run. Give those callbacks one turn,
        # then classify from the authoritative task state.
        await asyncio.sleep(0)
        done = {task for task in tasks if task.done()}
        raw_results = tuple(
            task.result() for task in tasks if task in done and task.exception() is None
        )
        results = tuple(materialize_result(value) for value in raw_results)
        failures = tuple(
            WorkItemFailure(
                work_item_id=work[i].work_item_id,
                worker_id=selected[i][0],
                error_type=type(task.exception()).__name__,
                error_message="worker execution failed",
            )
            for i, task in enumerate(tasks)
            if task in done and task.exception() is not None
        )
        pending_ids = tuple(
            work[i].work_item_id for i, task in enumerate(tasks) if task not in done
        )
        for i, task in enumerate(tasks):
            if task not in done:
                task.add_done_callback(
                    lambda completed, i=i: retain_late_result(
                        completed, work[i].work_item_id, selected[i][0]
                    )
                )
        return SubmittedOutcome(work, results, pending_ids, failures)
