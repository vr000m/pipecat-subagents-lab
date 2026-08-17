"""Delegated-child work-status publishing owned by a SessionHost.

Extracted from SessionHost (deep-review Architecture finding #1: SessionHost
god class) as the "work-status publishing" slice: recording one delegated
child's coarse status and sweeping a turn's still-non-terminal children to a
final state, plus the pure outcome -> status derivation helpers those two
operations (and many turn-handling call sites elsewhere in ``pipeline.py``)
share.

``state`` (the session's ``SessionState``, which owns the work-status
ledger) and ``feature_policy`` (the ``enable_background_status`` gate) stay
owned by SessionHost -- this class holds *references* to the same objects,
not copies, since both are read by many other SessionHost slices too. This
mirrors ``HandshakeGate``/``RunnerSupervisor``: a focused API surface over
state SessionHost still keeps for itself.

This module is a pure extraction of SessionHost's existing work-status
logic -- semantics, including transition/gating/precedence rules, are
unchanged from the code that previously lived directly on SessionHost and
at module scope in ``pipeline.py``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from .contracts import WORK_STATUS_TERMINAL, TerminalReason, WorkStatusState

if TYPE_CHECKING:
    from .config import FeaturePolicy
    from .session_state import SessionState


def _work_status_for_outcome(
    outcome_label: str | None,
    *,
    cancelled: bool = False,
    committed: bool = True,
    terminal_kind: str | None = None,
) -> tuple[WorkStatusState, TerminalReason | None] | None:
    """The single outcome -> coarse work-status derivation for both sites.

    Used by the foreground delegated-child finalization path and by
    ``SessionHost.commit_late_result_once``'s finalization block, so the two
    can never drift apart. Precedence mirrors the commit contract:
    cancellation classifies the work item regardless of whether its result
    additionally cleared the commit fences; otherwise an outcome that both
    committed and delivered a real committed result -- ``completed``, or a
    worker-raised ``clarify``/``declined`` (both commit and speak a canonical
    result of their own, so the work item is just as done as a completed
    search) -- is ``result_ready``, and everything else terminal is
    ``failed``. ``None`` means "no status to emit" (no outcome was ever
    assigned). ``terminal_reason`` is carried only for a retention-rejected
    terminal, the sole reason either site can name.
    """
    if cancelled:
        return "cancelled", None
    if outcome_label is None:
        return None
    if outcome_label in {"completed", "clarify", "declined"} and committed:
        return "result_ready", None
    return "failed", ("retention_rejected" if terminal_kind == "retention_rejected" else None)


def _work_status_after_commit_failure(
    derived: tuple[WorkStatusState, TerminalReason | None] | None,
) -> tuple[WorkStatusState, TerminalReason | None] | None:
    """The status to publish when the canonical commit itself raised.

    ``None`` means "publish nothing": either no status was derived at all, or
    the derived state is non-terminal (a still-legitimately-running
    ``background`` child, which must stay reachable by its own late result).
    A cancellation still classifies the work item regardless of the commit
    outcome -- the same precedence ``_work_status_for_outcome`` documents --
    so it is published as ``cancelled``, not overridden to ``failed``.
    """
    if derived is None or derived[0] not in WORK_STATUS_TERMINAL:
        return None
    if derived[0] == "cancelled":
        return derived
    return "failed", None


def _child_work_status_after_dispatch(
    outcome_label: str | None,
    *,
    cancelled: bool,
    terminal_kind: str | None = None,
) -> tuple[WorkStatusState, TerminalReason | None] | None:
    """Retained-and-not-cancelled work is not terminal: 'background' is its
    truthful status until the late result terminalizes it. Every other case
    defers to ``_work_status_for_outcome``.
    """
    if outcome_label == "retained" and not cancelled:
        return "background", None
    return _work_status_for_outcome(outcome_label, cancelled=cancelled, terminal_kind=terminal_kind)


class WorkStatusPublisher:
    """Records and sweeps delegated-child work-status for one SessionHost.

    Holds references to the owning SessionHost's ``state`` (the work-status
    ledger) and ``feature_policy`` (the ``enable_background_status`` gate) --
    not copies -- so this class stays in lockstep with the same objects
    SessionHost and its other slices read and mutate.
    """

    def __init__(self, *, state: SessionState, feature_policy: FeaturePolicy) -> None:
        self.state = state
        self.feature_policy = feature_policy

    def emit(
        self,
        *,
        turn_id: str,
        work_item_id: str,
        parent_work_item_id: str | None = None,
        worker_id: str | None = None,
        state: WorkStatusState,
        origin_epoch: int,
        terminal_reason: TerminalReason | None = None,
    ) -> None:
        """Record one delegated child's coarse status (Phase 3).

        A no-op unless ``enable_background_status`` is on; the wire
        emission itself is further capability-gated per connection inside
        the observer, so this call site does not need to branch on client
        capability -- only on whether the feature is enabled at all.
        """
        if not self.feature_policy.enable_background_status:
            return
        self.state.set_child_work_status(
            turn_id=turn_id,
            work_item_id=work_item_id,
            parent_work_item_id=parent_work_item_id,
            worker_id=worker_id,
            state=state,
            origin_epoch=origin_epoch,
            terminal_reason=terminal_reason,
        )

    def terminalize_children(
        self,
        *,
        turn_id: str,
        origin_epoch: int,
        children: Mapping[str, str | None],
        state: WorkStatusState,
        parent_work_item_id: str | None = None,
        terminal_reason: TerminalReason | None = None,
    ) -> None:
        """Terminalize every still-non-terminal delegated child as ``state``.

        A turn that ends mid-flight -- cancelled *or* failed on an unexpected
        exception after ``routing``/``searching`` was already emitted --
        would otherwise strand a capable client on a non-terminal record
        forever. ``children`` maps each delegated child's ``work_item_id`` to
        its worker id.

        Idempotent against a child that already settled:
        ``legal_work_status_transition`` rejects any move out of a terminal
        child state, and ``_reaggregate_parent`` never regresses an
        already-terminal parent. It does *not* reject every ``state`` as a
        cold start -- only ``cancelled`` is excluded from
        ``WORK_STATUS_COLD_START``; ``failed`` (used by
        ``_finalize_turn_exception``'s non-cancelled branch) is legally
        cold-startable. Safety for a child that never had a status therefore
        rests on caller discipline (every ``children`` entry already has a
        ``routing`` status by the time this runs), not on this mechanism.
        """
        for work_item_id, worker_id in children.items():
            self.emit(
                turn_id=turn_id,
                work_item_id=work_item_id,
                parent_work_item_id=parent_work_item_id,
                worker_id=worker_id,
                state=state,
                origin_epoch=origin_epoch,
                terminal_reason=terminal_reason,
            )
