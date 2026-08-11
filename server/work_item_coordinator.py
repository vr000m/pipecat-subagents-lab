"""Bounded work-item dispatch and pending-dialogue arbitration."""

from __future__ import annotations

import asyncio
import inspect
import re
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, ClassVar, Literal, Protocol, get_args

from loguru import logger

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


FailureKind = Literal["capacity_rejected", "retention_rejected", "cancelled", "failed"]
FAILURE_KINDS: frozenset[str] = frozenset(get_args(FailureKind))

TerminalKind = Literal["completed", "failed", "cancelled"]
"""How a late background task finished, classified from the task itself.

Deliberately declared here rather than imported from ``perf_metrics``: the
coordinator knows nothing about telemetry vocabularies, and importing one
would invert the dependency. A test pins this as a subset of the telemetry
side's ``WORK_OUTCOMES``.
"""
TERMINAL_KINDS: frozenset[str] = frozenset(get_args(TerminalKind))


@dataclass(frozen=True)
class WorkItemFailure:
    work_item_id: str
    worker_id: str
    error_type: str
    error_message: str
    failure_kind: FailureKind
    """Structured classification, set explicitly at each raise/catch site.

    ``error_type`` remains free-text diagnostic (worker exception class name
    or a hand-written sentinel) for anything else that consumes it, but
    telemetry classification must read this field instead of pattern-matching
    on ``error_type``, so renaming a worker exception class cannot silently
    reclassify its outcome.
    """


@dataclass(frozen=True)
class LateResult:
    work_item_id: str
    worker_id: str
    result: Any = None
    error: str | None = None
    terminal_kind: TerminalKind | None = None
    """Structured completion class: ``completed``, ``failed``, or ``cancelled``.

    Classified directly from the completed task, independent of ``error``'s
    free-text content, so telemetry never has to infer cancellation from an
    error string.
    """


@dataclass(frozen=True)
class WorkItem:
    work_item_id: str


@dataclass(frozen=True)
class Result:
    outcome: str = ""
    text: str = ""
    citations: tuple[Any, ...] = ()


class Coordinator(Protocol):
    """The SessionHost <-> coordinator boundary, declared explicitly.

    ``SessionHost`` previously typed its ``coordinator`` field as ``Any``
    and duck-typed it via separate ``getattr(coordinator, ...)`` probes,
    with production code defaulting to this concrete class's own
    attributes to accommodate test doubles. This Protocol is the single
    declaration of what a coordinator must provide, matching
    ``WorkItemCoordinator``'s actual public surface as used by
    ``server/pipeline.py``. Every member declared here is required and is
    accessed directly (via ``_require_coordinator() -> Coordinator`` or an
    explicit ``self.coordinator is not None`` guard), not through
    ``getattr``, everywhere the coordinator's identity is already settled.
    The one remaining exception is ``SessionHost.__init__`` itself, where
    ``registry``, ``config``, and ``OWNED_CONFIG_FIELDS`` all stay behind
    ``getattr`` fallbacks on purpose: a large number of duck-typed
    test-double coordinators across the suite construct without declaring
    those members at all, and requiring full Protocol conformance from
    every test double is out of scope for this pass.
    """

    registry: WorkerRegistry | None
    router: Router | None
    config: Config
    OWNED_CONFIG_FIELDS: ClassVar[frozenset[str]]

    def arbitrate(self, session_id: str, transcript: str) -> DispatchOutcome: ...

    def dispatch(
        self,
        decision: RoutingDecision,
        operation: Callable[[Any], Any] | None = None,
        catalogue: WorkerCatalogue | None = None,
    ) -> Any: ...

    def start_task(self, operation: Any) -> asyncio.Task[Any] | None: ...

    async def submit(
        self,
        turn_id: str,
        items: list[tuple[str, str]],
        worker: Callable[[str, str], Any],
        *,
        on_late_complete: Callable[[LateResult], Any] | None = None,
        work_item_ids: list[str] | None = None,
        on_late_terminal: Callable[[str, TerminalKind], Any] | None = None,
    ) -> SubmittedOutcome: ...

    def retain_late_task(
        self,
        task: asyncio.Task[Any],
        *,
        work_item_id: str,
        worker_id: str,
        on_complete: Callable[[LateResult], Any] | None = None,
        on_late_terminal: Callable[[str, TerminalKind], Any] | None = None,
    ) -> bool: ...

    def cancel(self, work_item_id: str | None = None) -> tuple[str, ...]: ...

    async def shutdown(self) -> None: ...

    def add_worker_clarification(
        self,
        *,
        session_id: str,
        worker_id: str,
        turn_id: str,
        result_id: str,
        original_query: str,
        question: str,
    ) -> None: ...


class WorkItemCoordinator:
    #: The ``Config`` fields this constructor is allowed to override onto
    #: whatever ``Config`` it was handed (see ``__init__``'s ``replace`` call
    #: below). ``SessionHost`` reconciles its own ``Config`` against a
    #: coordinator's by excluding exactly these fields, so the two components
    #: read one declaration instead of duplicating a literal list that can
    #: silently drift when a field is added or removed here.
    OWNED_CONFIG_FIELDS: ClassVar[frozenset[str]] = frozenset(
        {"max_work_items_per_turn", "multi_intent_wait_timeout_ms"}
    )

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
        self._pending_lock = threading.RLock()
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
        self._work_tasks: dict[str, asyncio.Task[Any]] = {}
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
        on_late_terminal: Callable[[str, TerminalKind], Any] | None = None,
    ) -> bool:
        """Retain a timed-out task, returning false when shutdown rejects ownership.

        ``on_late_terminal`` is a synchronous, telemetry-only hook invoked with
        ``(work_item_id, terminal_kind)`` as the very first action inside the
        completion callback, before the shutdown guard below can suppress
        ``on_complete``. This lets a host-owned recorder observe every retained
        task's terminal classification even when shutdown suppresses the normal
        completion path.
        """
        if self._shutdown:
            self._track_cancelling_task(task)
            return False
        if not self._adopt_task(task):
            self._track_cancelling_task(task)
            return False
        self._late_tasks.add(task)
        self._background_task_order.append(task)

        def completed(completed_task: asyncio.Task[Any]) -> None:
            terminal_kind: TerminalKind
            if completed_task.cancelled():
                terminal_kind = "cancelled"
            elif completed_task.exception() is not None:
                terminal_kind = "failed"
            else:
                terminal_kind = "completed"
            if on_late_terminal is not None:
                try:
                    on_late_terminal(work_item_id, terminal_kind)
                except Exception:  # noqa: BLE001  # telemetry-only hook must never block task cleanup or result delivery
                    logger.exception(
                        f"on_late_terminal hook raised for {work_item_id} ({terminal_kind})"
                    )
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
                    terminal_kind=terminal_kind,
                )
            except Exception as exc:  # noqa: BLE001  # intentional catch-all: any worker task failure must surface as a LateResult, not crash the coordinator
                late = LateResult(
                    work_item_id=work_item_id,
                    worker_id=worker_id,
                    error=f"{type(exc).__name__}: {exc}",
                    terminal_kind=terminal_kind,
                )
            else:
                late = LateResult(
                    work_item_id=work_item_id,
                    worker_id=worker_id,
                    result=Result(**value) if isinstance(value, dict) else value,
                    terminal_kind=terminal_kind,
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
        self._work_tasks.clear()
        self._background_task_order.clear()
        self._tails.clear()

    def add_pending(self, candidate: PendingDialogue) -> None:
        self._ensure_open()
        with self._pending_lock:
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
        with self._pending_lock:
            candidate = self._pending.get(session_id)
            if candidate and candidate.expires_at <= self.clock():
                self._pending.pop(session_id, None)
                return None
            return candidate

    @staticmethod
    def control_intent(transcript: str) -> tuple[str, str | None] | None:
        match = re.fullmatch(
            r"\s*(pause|resume|cancel|stop|consent)"
            r"(?:\s+(work(?:[-_ ]item)?[-_ ][\w-]{1,128}))?\s*[.!]?\s*",
            transcript,
            re.IGNORECASE,
        )
        if not match:
            return None
        action = match.group(1).lower()
        target = match.group(2).replace(" ", "_") if match.group(2) else None
        return action, target

    def cancel(self, work_item_id: str | None = None) -> tuple[str, ...]:
        """Cancel accepted computation for one work item, or all active work."""
        selected = tuple(
            item_id
            for item_id in self._work_tasks
            if work_item_id is None or item_id == work_item_id
        )
        for item_id in selected:
            self._work_tasks[item_id].cancel()
        return selected

    def live_work_item_ids(self) -> frozenset[str]:
        """Work items the coordinator still owns a live task for.

        Covers coordinator-retained background work (``retain_late_task``)
        that a caller's own local turn/work-task bookkeeping does not track
        once a turn handler has returned -- see ``SessionHost.
        cancel_turn_or_child``'s sole-child liveness check. Deliberately not
        declared on the ``Coordinator`` Protocol: it is brand new and no
        existing duck-typed test-double coordinator implements it, so the
        one call site probes it with ``getattr`` and tolerates its absence,
        same as any other genuinely optional member.
        """
        return frozenset(self._work_tasks)

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
            with self._pending_lock:
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
        with self._pending_lock:
            pending = self.pending(session_id)
            pending_intent = self.pending_intent(transcript) if pending else None
            if pending and pending_intent == "multi_intent":
                parts = tuple(
                    part.strip()
                    for part in _MULTI_INTENT_SEPARATOR.split(transcript)
                    if part.strip()
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
        worker = (
            self.registry.get_or_create(
                topic=decision.topic or "",
                worker_type=decision.worker_type or "",
                model_policy=decision.model_policy or "",
            )
            if decision.action == "new_worker"
            else (self.registry.get(decision.worker_id) if decision.worker_id else None)
        )
        if worker is None:
            return None
        if operation is None:
            return worker.worker
        return operation(worker.worker)

    def _work_task_cleanup(self, item_id: str) -> Callable[[asyncio.Task[Any]], None]:
        def completed(completed_task: asyncio.Task[Any]) -> None:
            if self._work_tasks.get(item_id) is completed_task:
                self._work_tasks.pop(item_id, None)

        return completed

    async def submit(
        self,
        turn_id: str,
        items: list[tuple[str, str]],
        worker: Callable[[str, str], Any],
        *,
        on_late_complete: Callable[[LateResult], Any] | None = None,
        work_item_ids: list[str] | None = None,
        on_late_terminal: Callable[[str, TerminalKind], Any] | None = None,
    ) -> SubmittedOutcome:
        self._ensure_open()
        submission_task = asyncio.current_task()
        if submission_task is not None:
            self._submission_tasks.add(submission_task)
        try:
            selected = items[: self.config.max_work_items_per_turn]
            selected_ids = (
                work_item_ids[: len(selected)]
                if work_item_ids is not None
                else [f"{turn_id}-{i}" for i, _ in enumerate(selected)]
            )
            if len(selected_ids) != len(selected):
                raise ValueError("work_item_ids must cover every selected item")
            work = tuple(WorkItem(work_item_id=item_id) for item_id in selected_ids)

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
                    except Exception:  # noqa: BLE001  # intentional catch-all: a failed prior tail task must not block the next queued task for this worker
                        logger.debug(
                            f"prior tail task for {worker_id} raised while awaiting shield"
                        )
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
                            failure_kind="capacity_rejected",
                        )
                    )
                    continue
                indexed_tasks.append((index, task))
                self._work_tasks[work[index].work_item_id] = task
                self._submit_tasks.add(task)
                task.add_done_callback(self._submit_tasks.discard)
                task.add_done_callback(self._work_task_cleanup(work[index].work_item_id))
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
                task.result()
                for task in tasks
                if task in done and not task.cancelled() and task.exception() is None
            )
            results = tuple(materialize_result(value) for value in raw_results)
            execution_failures: list[WorkItemFailure] = []
            for index, task in indexed_tasks:
                if task not in done:
                    continue
                if task.cancelled():
                    execution_failures.append(
                        WorkItemFailure(
                            work_item_id=work[index].work_item_id,
                            worker_id=selected[index][0],
                            error_type="CancelledError",
                            error_message="worker execution was cancelled",
                            failure_kind="cancelled",
                        )
                    )
                    continue
                exception = task.exception()
                if exception is not None:
                    execution_failures.append(
                        WorkItemFailure(
                            work_item_id=work[index].work_item_id,
                            worker_id=selected[index][0],
                            error_type=type(exception).__name__,
                            error_message="worker execution failed",
                            failure_kind="failed",
                        )
                    )
            failures = tuple(capacity_failures) + tuple(execution_failures)
            pending_ids: list[str] = []
            for index, task in indexed_tasks:
                if task in done:
                    continue
                accepted = self.retain_late_task(
                    task,
                    work_item_id=work[index].work_item_id,
                    worker_id=selected[index][0],
                    on_complete=on_late_complete,
                    on_late_terminal=on_late_terminal,
                )
                if accepted:
                    pending_ids.append(work[index].work_item_id)
                else:
                    failures += (
                        WorkItemFailure(
                            work_item_id=work[index].work_item_id,
                            worker_id=selected[index][0],
                            error_type="RetentionCapacityError",
                            error_message="worker execution could not continue in background",
                            failure_kind="retention_rejected",
                        ),
                    )
            return SubmittedOutcome(work, results, tuple(pending_ids), failures)
        finally:
            if submission_task is not None:
                self._submission_tasks.discard(submission_task)
