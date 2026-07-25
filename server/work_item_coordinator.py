"""Bounded work-item dispatch and pending-dialogue arbitration."""

from __future__ import annotations

import asyncio
import inspect
import re
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any

from .config import Config
from .contracts import RoutingDecision
from .registry import WorkerRegistry
from .router import Router, WorkerCatalogue, validate_decision


_SEPARATE_REQUEST_VERBS = (
    r"search|find|look\s+up|check|show\s+me|tell\s+me|get|open|create|write|summarize"
)
_MULTI_INTENT_SEPARATOR = re.compile(
    rf"\s*(?:,\s*)?\b(?:and(?:\s+(?:also|then))?|also)\s+"
    rf"(?=(?:please\s+)?(?:{_SEPARATE_REQUEST_VERBS})\b)",
    re.IGNORECASE,
)
_IMPERATIVE_NEW_TOPIC = re.compile(
    rf"^(?:(?:please|also)\s+)?(?:{_SEPARATE_REQUEST_VERBS})\b",
    re.IGNORECASE,
)
_POLITE_NEW_TOPIC = re.compile(
    rf"^(?:please\s+)?(?:can|could|would|will)\s+you\s+"
    rf"(?:please\s+)?(?:{_SEPARATE_REQUEST_VERBS})\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class PendingDialogue:
    session_id: str
    owner_kind: str
    owner_id: str
    turn_id: str
    result_id: str
    expires_at: float
    original_query: str = ""
    question: str = ""


@dataclass(frozen=True)
class DispatchOutcome:
    kind: str
    transcript: str
    decision: RoutingDecision | None = None
    catalogue: WorkerCatalogue | None = None
    work_items: tuple[str, ...] = ()
    control_action: str | None = None
    prose: str | None = None
    pending_dialogue: PendingDialogue | None = None


@dataclass(frozen=True)
class SubmittedOutcome:
    work_items: tuple[Any, ...]
    results: tuple[Any, ...]
    pending_work_item_ids: tuple[str, ...] = ()
    failures: tuple[WorkItemFailure, ...] = ()


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


@dataclass(frozen=True)
class WorkItem:
    work_item_id: str


@dataclass(frozen=True)
class Result:
    outcome: str = ""
    text: str = ""
    citations: tuple[Any, ...] = ()


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
        max_background_tasks: int | None = None,
    ) -> None:
        self.registry, self.router, self.config, self.clock = (
            registry,
            router,
            config or (registry.config if registry else Config()),
            clock,
        )
        max_work_items = max_work_items if max_work_items is not None else max_work_items_per_turn
        if max_work_items is not None:
            self.config = replace(
                self.config,
                max_work_items_per_turn=max_work_items,
                multi_intent_wait_timeout_ms=wait_timeout_ms or 10_000,
            )
        self.speech_scheduler = speech_scheduler
        self._pending: dict[str, PendingDialogue] = {}
        self._tails: dict[str, Any] = {}
        self._max_background_tasks = (
            max(4, self.config.max_work_items_per_turn * 2)
            if max_background_tasks is None
            else max_background_tasks
        )
        if self._max_background_tasks <= 0:
            raise ValueError("max_background_tasks must be positive")
        self._late_results: deque[LateResult] = deque(maxlen=self._max_background_tasks)
        self._late_tasks: set[asyncio.Task[Any]] = set()
        self._callback_tasks: set[asyncio.Task[Any]] = set()
        self._cancelling_tasks: set[asyncio.Task[Any]] = set()
        self._owned_tasks: set[asyncio.Task[Any]] = set()
        self._submission_tasks: set[asyncio.Task[Any]] = set()
        self._submit_tasks: set[asyncio.Task[Any]] = set()
        self._provider_tasks: set[asyncio.Task[Any]] = set()
        self._background_task_order: deque[asyncio.Task[Any]] = deque()
        self._shutdown = False

    def _ensure_open(self) -> None:
        if self._shutdown:
            raise RuntimeError("work item coordinator is shut down")

    def _has_background_capacity(self) -> bool:
        return len(self._owned_tasks) < self._max_background_tasks

    @staticmethod
    def _consume_task_exception(task: asyncio.Task[Any]) -> None:
        if task.cancelled():
            return
        try:
            task.exception()
        except asyncio.CancelledError:
            pass

    def _adopt_task(self, task: asyncio.Task[Any], *, force: bool = False) -> bool:
        """Own a task until terminal completion.

        Internal callers reserve ownership before scheduling provider work.
        ``force`` exists only for already-started compatibility seams: such a
        task must remain owned while cancellation takes effect.
        """
        if task in self._owned_tasks:
            return True
        if not force and not self._has_background_capacity():
            return False
        self._owned_tasks.add(task)

        def completed(completed_task: asyncio.Task[Any]) -> None:
            self._owned_tasks.discard(completed_task)

        task.add_done_callback(completed)
        return True

    def start_task(self, operation: Any) -> asyncio.Task[Any] | None:
        """Start an awaitable only after reserving bounded coordinator capacity."""
        if self._shutdown or not self._has_background_capacity():
            close = getattr(operation, "close", None)
            if close is not None:
                close()
            return None
        task = asyncio.create_task(operation)
        self._adopt_task(task, force=True)
        return task

    def _track_cancelling_task(self, task: asyncio.Task[Any]) -> None:
        self._adopt_task(task, force=True)
        self._cancelling_tasks.add(task)

        def completed(completed_task: asyncio.Task[Any]) -> None:
            self._cancelling_tasks.discard(completed_task)
            self._consume_task_exception(completed_task)

        task.add_done_callback(completed)
        task.cancel()

    @staticmethod
    def _discard_ordered_task(
        task: asyncio.Task[Any],
        tasks: set[asyncio.Task[Any]],
        order: deque[asyncio.Task[Any]],
    ) -> None:
        tasks.discard(task)
        try:
            order.remove(task)
        except ValueError:
            pass

    def _track_callback_task(self, task: asyncio.Task[Any]) -> None:
        if not self._adopt_task(task):
            self._track_cancelling_task(task)
            return
        self._callback_tasks.add(task)
        self._background_task_order.append(task)

        def completed(completed_task: asyncio.Task[Any]) -> None:
            self._discard_ordered_task(
                completed_task,
                self._callback_tasks,
                self._background_task_order,
            )
            if completed_task.cancelled():
                return
            exception = completed_task.exception()
            if exception is not None and not self._shutdown:
                asyncio.get_running_loop().call_exception_handler(
                    {
                        "message": "late-result completion callback failed",
                        "exception": exception,
                        "task": completed_task,
                    }
                )

        task.add_done_callback(completed)

    def drain_late_results(self) -> tuple[LateResult, ...]:
        """Return and remove results from timed-out work items in completion order."""
        results = tuple(self._late_results)
        self._late_results.clear()
        return results

    def retain_late_task(
        self,
        task: asyncio.Task[Any],
        *,
        work_item_id: str,
        worker_id: str,
        on_complete: Callable[[LateResult], Any] | None = None,
    ) -> bool:
        """Retain a timed-out task, returning false when shutdown rejects ownership."""
        if self._shutdown:
            self._track_cancelling_task(task)
            return False
        if not self._adopt_task(task):
            self._track_cancelling_task(task)
            return False
        self._late_tasks.add(task)
        self._background_task_order.append(task)

        def completed(completed_task: asyncio.Task[Any]) -> None:
            self._discard_ordered_task(
                completed_task,
                self._late_tasks,
                self._background_task_order,
            )
            if self._shutdown:
                return
            try:
                value = completed_task.result()
            except asyncio.CancelledError:
                late = LateResult(
                    work_item_id=work_item_id,
                    worker_id=worker_id,
                    error="CancelledError: worker task was cancelled",
                )
            except Exception as exc:
                late = LateResult(
                    work_item_id=work_item_id,
                    worker_id=worker_id,
                    error=f"{type(exc).__name__}: {exc}",
                )
            else:
                late = LateResult(
                    work_item_id=work_item_id,
                    worker_id=worker_id,
                    result=Result(**value) if isinstance(value, dict) else value,
                )
            if on_complete is None:
                self._late_results.append(late)
                return
            callback_result = on_complete(late)
            if inspect.isawaitable(callback_result):
                callback_task = self.start_task(callback_result)
                if callback_task is not None:
                    self._track_callback_task(callback_task)

        task.add_done_callback(completed)
        return True

    async def shutdown(self) -> None:
        """Cancel retained background work and reject future mutation."""
        self._shutdown = True
        self._pending.clear()
        self._late_results.clear()
        current_task = asyncio.current_task()
        owned_tasks = self._owned_tasks | self._submission_tasks | self._provider_tasks
        tasks = tuple(owned_tasks - ({current_task} if current_task is not None else set()))
        for task in tasks:
            task.cancel()
        if tasks:
            done, pending = await asyncio.wait(
                tasks,
                timeout=self.config.shutdown_grace_seconds,
            )
            for task in done:
                self._consume_task_exception(task)
            for task in pending:
                task.add_done_callback(self._consume_task_exception)
        self._late_tasks.clear()
        self._callback_tasks.clear()
        self._cancelling_tasks.clear()
        self._owned_tasks.clear()
        self._submission_tasks.clear()
        self._submit_tasks.clear()
        self._provider_tasks.clear()
        self._background_task_order.clear()
        self._tails.clear()

    def add_pending(self, candidate: PendingDialogue) -> None:
        self._ensure_open()
        self._pending[candidate.session_id] = candidate

    def add_worker_clarification(
        self,
        *,
        session_id: str,
        worker_id: str,
        turn_id: str,
        result_id: str,
        original_query: str,
        question: str,
    ) -> None:
        """Record a worker's clarifying question as the next turn's pending candidate."""
        self.add_pending(
            PendingDialogue(
                session_id=session_id,
                owner_kind="worker",
                owner_id=worker_id,
                turn_id=turn_id,
                result_id=result_id,
                expires_at=self.clock() + self.config.pending_dialogue_timeout_seconds,
                original_query=original_query,
                question=question,
            )
        )

    def pending(self, session_id: str) -> PendingDialogue | None:
        candidate = self._pending.get(session_id)
        if candidate and candidate.expires_at <= self.clock():
            self._pending.pop(session_id, None)
            return None
        return candidate

    @staticmethod
    def control_intent(transcript: str) -> tuple[str, str | None] | None:
        match = re.match(
            r"^\s*(pause|resume|cancel|stop|consent)\b(?:\s+(work(?:[-_ ]item)?[-_ ][\w-]+))?",
            transcript,
            re.IGNORECASE,
        )
        if not match:
            return None
        action = "cancel" if match.group(1).lower() == "stop" else match.group(1).lower()
        target = match.group(2).replace(" ", "_") if match.group(2) else None
        return action, target

    @staticmethod
    def pending_intent(transcript: str) -> str:
        """Classify a reply while a worker clarification is pending."""
        normalized = " ".join(transcript.strip().split())
        lowered = normalized.casefold()
        if lowered.startswith(("new question", "unrelated", "forget that")):
            return "new_topic"
        separator = _MULTI_INTENT_SEPARATOR.search(normalized)
        if separator and normalized[: separator.start()].strip(" ,"):
            return "multi_intent"
        if lowered.startswith(("what about", "how about", "instead", "change that")):
            return "steer_same_topic"
        if lowered.startswith(("who ", "what ", "when ", "where ", "why ", "how ")):
            return "new_topic"
        if _IMPERATIVE_NEW_TOPIC.match(normalized) or _POLITE_NEW_TOPIC.match(normalized):
            return "new_topic"
        return "continue_pending"

    def arbitrate(self, session_id: str, transcript: str) -> DispatchOutcome:
        self._ensure_open()
        control = self.control_intent(transcript)
        if control:
            pending = self.pending(session_id)
            if control[0] == "consent" and pending:
                self._pending.pop(session_id, None)
                return DispatchOutcome(
                    "continue_pending",
                    transcript,
                    work_items=(pending.owner_id,),
                    control_action="consent",
                    pending_dialogue=pending,
                )
            return DispatchOutcome(
                "control",
                transcript,
                work_items=(control[1],) if control[1] else (),
                control_action=control[0],
            )
        pending = self.pending(session_id)
        pending_intent = self.pending_intent(transcript) if pending else None
        if pending and pending_intent == "multi_intent":
            parts = tuple(
                part.strip() for part in _MULTI_INTENT_SEPARATOR.split(transcript) if part.strip()
            )
            if len(parts) > self.config.max_work_items_per_turn:
                parts = parts[: self.config.max_work_items_per_turn]
            self._pending.pop(session_id, None)
            return DispatchOutcome(
                "multi_intent",
                transcript,
                work_items=parts,
                pending_dialogue=pending,
            )
        if pending and pending_intent in {"continue_pending", "steer_same_topic"}:
            self._pending.pop(session_id, None)
            return DispatchOutcome(
                "continue_pending",
                transcript,
                work_items=(pending.owner_id,),
                pending_dialogue=pending,
            )
        if self.registry is None or self.router is None:
            raise RuntimeError("routing arbitration requires a registry and router")
        catalogue = self.registry.catalogue()
        envelope = self.router.route_envelope(transcript, catalogue)
        return DispatchOutcome(
            "routed",
            transcript,
            decision=envelope.decision,
            catalogue=catalogue,
            prose=envelope.prose,
        )

    def dispatch(
        self,
        decision: RoutingDecision,
        operation: Callable[[Any], Any] | None = None,
        catalogue: WorkerCatalogue | None = None,
    ) -> Any:
        self._ensure_open()
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
        self._ensure_open()
        submission_task = asyncio.current_task()
        if submission_task is not None:
            self._submission_tasks.add(submission_task)
        try:
            selected = items[: self.config.max_work_items_per_turn]
            work = tuple(WorkItem(work_item_id=f"{turn_id}-{i}") for i, _ in enumerate(selected))

            def materialize_result(value: Any) -> Any:
                return Result(**value) if isinstance(value, dict) else value

            async def one(worker_id: str, text: str) -> Any:
                previous = self._tails.get(worker_id)
                if previous is not None:
                    try:
                        await asyncio.shield(previous)
                    except asyncio.CancelledError:
                        current = asyncio.current_task()
                        if current is not None and current.cancelling():
                            raise
                    except Exception:
                        pass
                task = asyncio.create_task(worker(worker_id, text))
                self._provider_tasks.add(task)
                self._tails[worker_id] = task
                try:
                    return await task
                finally:
                    self._provider_tasks.discard(task)
                    if self._tails.get(worker_id) is task:
                        self._tails.pop(worker_id, None)

            indexed_tasks: list[tuple[int, asyncio.Task[Any]]] = []
            capacity_failures: list[WorkItemFailure] = []
            for index, (worker_id, text) in enumerate(selected):
                task = self.start_task(one(worker_id, text))
                if task is None:
                    capacity_failures.append(
                        WorkItemFailure(
                            work_item_id=work[index].work_item_id,
                            worker_id=worker_id,
                            error_type="CapacityError",
                            error_message="worker execution capacity is exhausted",
                        )
                    )
                    continue
                indexed_tasks.append((index, task))
                self._submit_tasks.add(task)
                task.add_done_callback(self._submit_tasks.discard)
            tasks = [task for _, task in indexed_tasks]
            try:
                if tasks:
                    await asyncio.wait(
                        tasks,
                        timeout=self.config.multi_intent_wait_timeout_ms / 1000,
                    )
            except asyncio.CancelledError:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise
            await asyncio.sleep(0)
            done = {task for task in tasks if task.done()}
            raw_results = tuple(
                task.result() for task in tasks if task in done and task.exception() is None
            )
            results = tuple(materialize_result(value) for value in raw_results)
            failures = tuple(capacity_failures) + tuple(
                WorkItemFailure(
                    work_item_id=work[index].work_item_id,
                    worker_id=selected[index][0],
                    error_type=type(task.exception()).__name__,
                    error_message="worker execution failed",
                )
                for index, task in indexed_tasks
                if task in done and task.exception() is not None
            )
            pending_ids: list[str] = []
            for index, task in indexed_tasks:
                if task in done:
                    continue
                accepted = self.retain_late_task(
                    task,
                    work_item_id=work[index].work_item_id,
                    worker_id=selected[index][0],
                )
                if accepted:
                    pending_ids.append(work[index].work_item_id)
                else:
                    failures += (
                        WorkItemFailure(
                            work_item_id=work[index].work_item_id,
                            worker_id=selected[index][0],
                            error_type="CapacityError",
                            error_message="worker execution could not continue in background",
                        ),
                    )
            return SubmittedOutcome(work, results, tuple(pending_ids), failures)
        finally:
            if submission_task is not None:
                self._submission_tasks.discard(submission_task)
