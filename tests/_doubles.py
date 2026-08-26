"""Canonical Coordinator test double, contract-checked against the real interface.

Phase 0 (test-double modernization) inventory found 77 duck-typed
``class *Coordinator`` doubles across ``tests/`` (``rg -n 'class \\w*Coordinator'
tests/``: 71 in ``tests/test_pipeline.py``, 3 in ``tests/test_smoke_conversation.py``,
2 in ``tests/test_work_item_coordinator.py``, 1 in ``tests/test_session_host.py``).
This module is the single shared replacement those doubles migrate onto in later
Phase 0 sub-steps (one migration commit per test module); this sub-step only
introduces the double and its contract check, and migrates nothing yet.

Protocol vs ABC
----------------
``server.work_item_coordinator`` already declares ``Coordinator`` as a
``typing.Protocol`` -- the one explicit statement of the SessionHost <->
coordinator boundary (see that class's docstring). This module reuses that
Protocol as the sole source of truth rather than inventing a second,
competing interface. An ABC was considered and rejected: an ABC would force
every test double to inherit from one shared base to satisfy ``isinstance``,
but a survey of the existing 71 doubles in ``tests/test_pipeline.py`` found
most are small, standalone classes (no shared base at all today) that
override exactly one or two methods and lean on ``SessionHost``'s
``getattr`` fallback for everything else. A Protocol lets ``FakeCoordinator``
below, its per-test subclasses, and the real ``WorkItemCoordinator`` all
satisfy the same interface structurally, with zero coupling between the
double and the production class.

``FakeCoordinator`` implements ``Coordinator``'s full required surface,
using ``CoordinatorDefaults`` (already the canonical fallback for the 7
*optional* members: ``registry``, ``config``, ``OWNED_CONFIG_FIELDS``,
``live_work_item_ids``, ``start_task``, ``cancel``, ``shutdown``) as a base,
plus concrete, minimal implementations for the remaining required members
(``router``, ``arbitrate``, ``dispatch``, ``submit``, ``retain_late_task``,
``add_worker_clarification``). Each of those is overridable two ways, to
mirror how the existing doubles are actually used:

- a constructor hook (``FakeCoordinator(arbitrate=lambda session_id,
  transcript: ...)``) for the common case of swapping one method's behaviour
  inline in a test function, and
- subclassing (``class ControlCoordinator(FakeCoordinator): def
  arbitrate(self, ...): ...``) for the common case of a small per-test class,
  which is how the majority of today's 71 doubles in ``tests/test_pipeline.py``
  are already written.

Conformance checking
---------------------
``assert_conforms_to_coordinator`` structurally validates any object against
``Coordinator``: every attribute in ``Coordinator.__annotations__`` must be
present, and every method declared on ``Coordinator`` must be present with a
matching signature (the Protocol's ``self`` parameter is stripped before
comparison, since the check runs against bound methods on instances). This
makes the check import-and-call, not isinstance-based, because ``Coordinator``
is not decorated ``@runtime_checkable`` -- adding that decorator would be a
change to a carefully-documented boundary declaration this phase is
explicitly scoped not to touch (see Requirement 1's re-litigation, deferred
to Phase 3).

``tests/test_doubles.py`` calls this against both a default-constructed
``FakeCoordinator`` and a default-constructed ``WorkItemCoordinator``, so a
member added to (or renamed on) ``Coordinator`` without a matching
``FakeCoordinator`` update fails the suite loudly, instead of every migrated
test silently falling back to a stale double.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from typing import Any

from server.config import Config
from server.contracts import RoutingDecision
from server.registry import WorkerRegistry
from server.router import Router, WorkerCatalogue
from server.work_item_coordinator import (
    Coordinator,
    CoordinatorDefaults,
    DispatchOutcome,
    LateResult,
    SubmittedOutcome,
    TerminalKind,
)


def conformance_problems(obj: Any) -> list[str]:
    """Return every way ``obj`` fails to structurally satisfy ``Coordinator``.

    Empty list means ``obj`` conforms. Kept separate from
    ``assert_conforms_to_coordinator`` so a caller that wants the full list
    (rather than the first ``AssertionError``) can get it directly.
    """
    problems: list[str] = []

    for name in Coordinator.__annotations__:
        if not hasattr(obj, name):
            problems.append(f"missing attribute: {name!r}")

    for name, member in vars(Coordinator).items():
        if name.startswith("_") or not callable(member):
            continue
        if not hasattr(obj, name):
            problems.append(f"missing method: {name!r}")
            continue
        candidate = getattr(obj, name)
        if not callable(candidate):
            problems.append(f"{name!r} is not callable on {obj!r}")
            continue
        try:
            expected = inspect.signature(member)
            actual = inspect.signature(candidate)
        except (TypeError, ValueError):
            continue
        # ``member`` is the unbound function pulled off the Protocol's class
        # body; drop its leading ``self`` so it compares against the bound
        # method's signature on ``obj``.
        expected = expected.replace(parameters=list(expected.parameters.values())[1:])
        if expected != actual:
            problems.append(f"signature mismatch on {name!r}: expected {expected}, got {actual}")

    return problems


def assert_conforms_to_coordinator(obj: Any, *, label: str = "object") -> None:
    """Raise ``AssertionError`` with every mismatch if ``obj`` doesn't satisfy ``Coordinator``."""
    problems = conformance_problems(obj)
    if problems:
        joined = "\n".join(f"  - {problem}" for problem in problems)
        raise AssertionError(f"{label} does not conform to Coordinator:\n{joined}")


ArbitrateHook = Callable[[str, str], Any]
DispatchHook = Callable[..., Any]
SubmitHook = Callable[..., Any]
RetainLateTaskHook = Callable[..., bool]
AddWorkerClarificationHook = Callable[..., None]


class FakeCoordinator(CoordinatorDefaults):
    """Canonical, contract-checked ``Coordinator`` double for tests/.

    Provides every required ``Coordinator`` member. The 7 optional members
    (``registry``, ``config``, ``OWNED_CONFIG_FIELDS``, ``live_work_item_ids``,
    ``start_task``, ``cancel``, ``shutdown``) come from ``CoordinatorDefaults``
    except ``config``, which defaults to a real ``Config()`` here (matching
    how the existing doubles already construct one) rather than ``None``.

    Override a behaviour by passing a constructor hook, or by subclassing and
    overriding the method directly -- both are first-class; see the module
    docstring for when each fits.
    """

    def __init__(
        self,
        *,
        registry: WorkerRegistry | None = None,
        router: Router | None = None,
        config: Config | None = None,
        arbitrate: ArbitrateHook | None = None,
        dispatch: DispatchHook | None = None,
        submit: SubmitHook | None = None,
        retain_late_task: RetainLateTaskHook | None = None,
        add_worker_clarification: AddWorkerClarificationHook | None = None,
    ) -> None:
        self.registry = registry
        self.router = router
        self.config = config if config is not None else Config()
        self._arbitrate_hook = arbitrate
        self._dispatch_hook = dispatch
        self._submit_hook = submit
        self._retain_late_task_hook = retain_late_task
        self._add_worker_clarification_hook = add_worker_clarification
        self.clarifications: list[dict[str, str]] = []

    def arbitrate(self, session_id: str, transcript: str) -> DispatchOutcome:
        if self._arbitrate_hook is not None:
            return self._arbitrate_hook(session_id, transcript)
        # Safe no-op default: a "control" outcome with no action falls
        # through SessionHost's control-turn handling to its generic
        # "Control request noted." acknowledgement, so a test that never
        # cares about routing does not have to supply one.
        return DispatchOutcome(kind="control", transcript=transcript, control_action=None)

    def dispatch(
        self,
        decision: RoutingDecision,
        operation: Callable[[Any], Any] | None = None,
        catalogue: WorkerCatalogue | None = None,
    ) -> Any:
        if self._dispatch_hook is not None:
            return self._dispatch_hook(decision, operation, catalogue)
        raise NotImplementedError(
            "FakeCoordinator.dispatch: default arbitrate() never returns a "
            "'routed' outcome, so dispatch() is unreached unless you override "
            "arbitrate. Pass dispatch=... or override both in a subclass."
        )

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
        if self._submit_hook is not None:
            result = self._submit_hook(
                turn_id,
                items,
                worker,
                on_late_complete=on_late_complete,
                work_item_ids=work_item_ids,
                on_late_terminal=on_late_terminal,
            )
            if inspect.isawaitable(result):
                return await result
            return result
        raise NotImplementedError(
            "FakeCoordinator.submit: no submit=... hook was supplied. Pass "
            "submit=... or override submit() in a subclass."
        )

    def retain_late_task(
        self,
        task: asyncio.Task[Any],
        *,
        work_item_id: str,
        worker_id: str,
        on_complete: Callable[[LateResult], Any] | None = None,
        on_late_terminal: Callable[[str, TerminalKind], Any] | None = None,
    ) -> bool:
        if self._retain_late_task_hook is not None:
            return self._retain_late_task_hook(
                task,
                work_item_id=work_item_id,
                worker_id=worker_id,
                on_complete=on_complete,
                on_late_terminal=on_late_terminal,
            )
        raise NotImplementedError(
            "FakeCoordinator.retain_late_task: no retain_late_task=... hook "
            "was supplied. Pass retain_late_task=... or override "
            "retain_late_task() in a subclass."
        )

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
        if self._add_worker_clarification_hook is not None:
            self._add_worker_clarification_hook(
                session_id=session_id,
                worker_id=worker_id,
                turn_id=turn_id,
                result_id=result_id,
                original_query=original_query,
                question=question,
            )
            return
        self.clarifications.append(
            {
                "session_id": session_id,
                "worker_id": worker_id,
                "turn_id": turn_id,
                "result_id": result_id,
                "original_query": original_query,
                "question": question,
            }
        )
