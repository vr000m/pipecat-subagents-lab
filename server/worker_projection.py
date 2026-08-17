"""Worker-state projection and clarification bookkeeping owned by a SessionHost.

Extracted from SessionHost (deep-review Architecture finding #1: SessionHost
god class) as the "worker projection / clarification" slice: projecting a
delegated worker's metadata into the session's ``WorkerState`` ledger,
recording a worker's clarifying question as the next turn's pending
candidate, and deriving the ``ClarificationContext`` a pending dialogue's
answer resumes with.

``state`` (the session's ``SessionState``, which owns the ``workers``
ledger) stays owned by SessionHost -- this class holds a *reference* to the
same object, not a copy, since it is read by many other SessionHost slices
too. This mirrors ``WorkStatusPublisher``/``RecorderFactory``: a focused API
surface over state SessionHost still keeps for itself.

This module is a pure extraction of SessionHost's existing worker-projection
and clarification logic -- semantics, including the metadata-completeness
guard and the ``latest_result_id`` carry-forward, are unchanged from the code
that previously lived directly on SessionHost.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from .contracts import GroundedResult, WorkerState
from .results import canonical_result
from .workers.web_search import ClarificationContext

if TYPE_CHECKING:
    from .session_state import SessionState


class WorkerProjection:
    """Projects worker metadata into a SessionHost's worker ledger and tracks
    clarification candidates awaiting the next turn's answer.

    Holds a reference to the owning SessionHost's ``state`` (the worker
    ledger) -- not a copy -- so this class stays in lockstep with the same
    object SessionHost and its other slices read and mutate.
    """

    #: Cap on retained clarification candidates. A candidate is normally
    #: consumed by ``pop_clarification_candidate`` on the commit path, but a
    #: clarification whose turn is cancelled, whose connection drops, or whose
    #: result never reaches ``_commit_result_state`` is never popped, so the
    #: map needs a bound of its own rather than relying on every turn ending
    #: in a commit. Mirrors the handshake-token cap in ``HandshakeGate`` and
    #: the work-status key cap in ``SessionState``: evict oldest first.
    MAX_CLARIFICATION_CANDIDATES = 32

    def __init__(self, *, state: SessionState) -> None:
        self.state = state
        self._clarification_candidates: OrderedDict[str, dict[str, str]] = OrderedDict()

    def project(
        self,
        worker: Any,
        *,
        origin_epoch: int,
        status: str,
        latest_result_id: str | None = None,
    ) -> None:
        metadata = getattr(worker, "metadata", None)
        if metadata is None:
            return
        worker_id = getattr(metadata, "worker_id", None)
        topic = getattr(metadata, "topic", None)
        model_policy = getattr(metadata, "model_policy", None)
        if (
            not (isinstance(worker_id, str) and worker_id)
            or not (isinstance(topic, str) and topic)
            or not (isinstance(model_policy, str) and model_policy)
        ):
            return
        previous = self.state.workers.get(worker_id)
        self.state.set_worker(
            WorkerState(
                worker_id=worker_id,
                topic=topic,
                model_policy=model_policy,
                status=status,
                latest_result_id=(
                    latest_result_id
                    if latest_result_id is not None
                    else previous.latest_result_id
                    if previous is not None
                    else None
                ),
                origin_epoch=origin_epoch,
            )
        )

    def clarification_result(
        self,
        *,
        worker_id: str,
        turn_id: str,
        question: str,
        original_query: str,
        origin_epoch: int | None,
    ) -> GroundedResult:
        """Record a worker's clarifying question as the next turn's pending candidate."""
        result_id = f"result-{uuid4().hex}"
        self._clarification_candidates[result_id] = {
            "worker_id": worker_id,
            "turn_id": turn_id,
            "original_query": original_query,
            "question": question,
        }
        while len(self._clarification_candidates) > self.MAX_CLARIFICATION_CANDIDATES:
            self._clarification_candidates.popitem(last=False)
        return canonical_result(
            worker_id=worker_id,
            turn_id=turn_id,
            text=question,
            result_id=result_id,
            origin_epoch=origin_epoch,
        )

    def pop_clarification_candidate(self, result_id: str) -> dict[str, str] | None:
        return self._clarification_candidates.pop(result_id, None)

    def clear_all(self) -> None:
        """Drop every retained clarification candidate.

        Called from ``SessionHost.shutdown`` alongside
        ``TurnAckLedger.clear_all()`` and ``RecorderFactory.finalize_all()``:
        candidates whose turn never reached a commit have no other exit, and
        a slice extracted in this pass should not be the one component
        without a teardown hook.
        """
        self._clarification_candidates.clear()

    @staticmethod
    def clarification_context(pending: Any, transcript: str) -> ClarificationContext | None:
        if pending is None or not pending.original_query:
            return None
        return ClarificationContext(
            original_query=pending.original_query,
            question=pending.question,
            answer=transcript,
        )
