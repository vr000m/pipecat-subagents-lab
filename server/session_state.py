"""Authoritative process-lifetime session state and delivery state machine."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, NamedTuple
from uuid import uuid4

from .contracts import (
    WORK_STATUS_TERMINAL,
    DeliveryState,
    GroundedResult,
    RoutingState,
    RuntimeSnapshot,
    SpeechProgress,
    TranscriptEntry,
    WorkerState,
    WorkStatus,
    legal_work_status_transition,
)
from .results import ResultLog

# Terminal work_status records remain in capable-client snapshots for a fixed
# five-minute session-clock TTL (Requirements). SessionState has no timer
# source, so pruning is lazy: a record is excluded from the projection once
# its age is >= this TTL, never removed by a background timer.
WORK_STATUS_TTL_SECONDS = 5 * 60.0

_TERMINAL = {
    DeliveryState.DELIVERY_COMPLETED,
    DeliveryState.DELIVERY_UNKNOWN,
    DeliveryState.INTERRUPTED,
    DeliveryState.INTERRUPTED_BY_RECONNECT,
}
_RANK = {
    DeliveryState.DISPLAYED: 0,
    DeliveryState.QUEUED: 1,
    DeliveryState.STARTED: 2,
    DeliveryState.SYNTHESIS_ENDED: 3,
    DeliveryState.PAUSED: 3,
    # A replay may transition from RESUMED back to STARTED.
    DeliveryState.RESUMED: 2,
    DeliveryState.DELIVERY_UNKNOWN: 5,
    DeliveryState.DELIVERY_COMPLETED: 6,
    DeliveryState.INTERRUPTED: 7,
    DeliveryState.INTERRUPTED_BY_RECONNECT: 8,
}


def _now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class StateEvent:
    sequence: int
    kind: str
    payload: dict[str, Any]


class WorkStatusKey(NamedTuple):
    """Identity of one client-visible parent work-status record.

    ``parent_key`` is the delegated-child grouping key -- the parent
    ``work_item_id`` for a mixed multi-intent turn, or the sole
    ``work_item_id`` for a single-delegation turn. Only delegated children
    ever allocate a key; direct/unsupported/clarify/declined outcomes never
    hold a delegated parent status open (Requirements).
    """

    origin_epoch: int | None
    turn_id: str
    parent_key: str


@dataclass
class _WorkStatusRecord:
    status: WorkStatus
    terminal_at: float | None = None


class SessionState:
    """The single source of truth projected to each active connection."""

    def __init__(self, session_id: str | None = None, resume_token: str | None = None) -> None:
        self.session_id = session_id or f"session-{uuid4().hex}"
        RuntimeSnapshot.reset_monotonicity(self.session_id)
        self.resume_token = resume_token or uuid4().hex
        self.sequence = 0
        self.workers: dict[str, WorkerState] = {}
        self.results = ResultLog()
        self.speech: dict[str, SpeechProgress] = {}
        self.routing: RoutingState | None = None
        self.transcript: list[TranscriptEntry] = []
        self._events: list[StateEvent] = []
        self.active_epoch: int | None = None
        self._speech_history: dict[str, list[SpeechProgress]] = {}
        self._listeners: list[Callable[[StateEvent], Any]] = []
        # Phase 3 work-status ledger. Children are keyed per delegated work
        # item; the parent ledger holds the one client-visible aggregate per
        # WorkStatusKey. Both hold the per-key event_sequence independently
        # of the global SessionState sequence (Requirements/Sequence
        # namespaces).
        self._work_status_children: dict[WorkStatusKey, dict[str, WorkStatus]] = {}
        self._work_status_parents: dict[WorkStatusKey, _WorkStatusRecord] = {}
        self._work_status_sequence: dict[WorkStatusKey, int] = {}

    @property
    def events(self) -> tuple[StateEvent, ...]:
        return tuple(self._events)

    def subscribe(self, listener: Callable[[StateEvent], Any]) -> Callable[[], None]:
        """Register a live projection listener.

        Listeners are deliberately observational: they cannot replace the
        authoritative state mutation and are expected to fence by epoch.
        """
        self._listeners.append(listener)

        def unsubscribe() -> None:
            try:
                self._listeners.remove(listener)
            except ValueError:
                pass

        return unsubscribe

    def _emit(self, kind: str, payload: dict[str, Any]) -> StateEvent:
        self.sequence += 1
        event = StateEvent(self.sequence, kind, payload)
        self._events.append(event)
        for listener in tuple(self._listeners):
            listener(event)
        return event

    def set_worker(self, worker: WorkerState) -> StateEvent | None:
        if self.active_epoch is not None and (
            worker.origin_epoch is None or worker.origin_epoch != self.active_epoch
        ):
            return None
        self.workers[worker.worker_id] = worker
        return self._emit("worker", worker.model_dump(mode="json"))

    def set_routing(self, routing: RoutingState) -> StateEvent | None:
        if self.active_epoch is not None and (
            routing.origin_epoch is None or routing.origin_epoch != self.active_epoch
        ):
            return None
        self.routing = routing
        return self._emit("routing", routing.model_dump(mode="json"))

    def append_transcript(self, entry: TranscriptEntry) -> StateEvent | None:
        if self.active_epoch is not None and (
            entry.origin_epoch is None or entry.origin_epoch != self.active_epoch
        ):
            return None
        if any(
            item.role == entry.role and item.turn_id == entry.turn_id and item.text == entry.text
            for item in self.transcript
        ):
            return None
        self.transcript.append(entry)
        kind = "user_transcript" if entry.role == "user" else "bot_transcript"
        return self._emit(kind, entry.model_dump(mode="json"))

    def append_result(
        self,
        result: GroundedResult,
        sequence: int | None = None,
        origin_epoch: int | None = None,
    ) -> StateEvent | None:
        if self.active_epoch is not None and origin_epoch is None:
            return None
        if (
            origin_epoch is not None
            and result.origin_epoch is not None
            and origin_epoch != result.origin_epoch
        ):
            return None
        if origin_epoch is not None and result.origin_epoch is None:
            result = result.model_copy(update={"origin_epoch": origin_epoch})
        if any(item.result_id == result.result_id for item in self.results.results):
            # Duplicate suppression is an internal idempotency decision. It is
            # not a public event because consuming a sequence number for it
            # would create a gap in the authoritative browser projection.
            return None
        effective_epoch = origin_epoch if origin_epoch is not None else result.origin_epoch
        if self.active_epoch is not None and effective_epoch != self.active_epoch:
            # Late provider work is still an immutable result commit, but it cannot
            # update active worker pointers or speech state.
            self.results.append(result)
            return self._emit("result", result.model_dump(mode="json"))
        if sequence is not None and sequence > self.sequence:
            self.sequence = sequence - 1
        self.results.append(result)
        worker = self.workers.get(result.worker_id)
        if worker:
            self.workers[result.worker_id] = worker.model_copy(
                update={"latest_result_id": result.result_id}
            )
        return self._emit("result", result.model_dump(mode="json"))

    commit_result = append_result

    def result_history(self, worker_id: str) -> tuple[GroundedResult, ...]:
        return self.results.for_worker(worker_id)

    def speech_progress(
        self,
        *,
        result_id: str,
        work_item_id: str,
        run_id: str,
        utterance_id: str,
        state: DeliveryState,
        origin_epoch: int | None = None,
        allow_stale_reconnect: bool = False,
    ) -> SpeechProgress:
        if (
            self.active_epoch is not None
            and not allow_stale_reconnect
            and (origin_epoch is None or origin_epoch != self.active_epoch)
        ):
            return self.speech.get(
                utterance_id,
                SpeechProgress(
                    result_id=result_id,
                    work_item_id=work_item_id,
                    run_id=run_id,
                    utterance_id=utterance_id,
                    state=state,
                    origin_epoch=origin_epoch,
                ),
            )
        previous = self.speech.get(utterance_id)
        if previous is not None and previous.state in _TERMINAL:
            return previous
        if previous is not None and _RANK[state] < _RANK[previous.state]:
            return previous
        progress = SpeechProgress(
            result_id=result_id,
            work_item_id=work_item_id,
            run_id=run_id,
            utterance_id=utterance_id,
            state=state,
            origin_epoch=origin_epoch,
        )
        self.speech[utterance_id] = progress
        self._speech_history.setdefault(utterance_id, []).append(progress)
        self._emit("speech_progress", progress.model_dump(mode="json"))
        return progress

    def apply_speech_progress(
        self,
        progress: SpeechProgress,
        origin_epoch: int | None = None,
        allow_stale_reconnect: bool = False,
    ) -> SpeechProgress:
        return self.speech_progress(
            result_id=progress.result_id,
            work_item_id=progress.work_item_id,
            run_id=progress.run_id,
            utterance_id=progress.utterance_id,
            state=progress.state,
            origin_epoch=origin_epoch if origin_epoch is not None else progress.origin_epoch,
            allow_stale_reconnect=allow_stale_reconnect,
        )

    def speech_history(self, utterance_id: str) -> tuple[SpeechProgress, ...]:
        return tuple(self._speech_history.get(utterance_id, ()))

    # -- Phase 3: progressive work status --------------------------------

    def set_child_work_status(
        self,
        *,
        turn_id: str,
        work_item_id: str,
        parent_work_item_id: str | None = None,
        worker_id: str | None = None,
        state: str,
        origin_epoch: int | None,
        terminal_reason: str | None = None,
    ) -> StateEvent | None:
        """Record one delegated child's status and re-emit the parent aggregate.

        Only delegated children participate in the client-visible parent
        ledger. The parent join is exhaustive: routing while any child is
        routing; searching while any child is searching and none routing;
        background once no child is active but at least one remains
        retained; and, once every child is terminal, failed wins over
        cancelled (all-cancelled) which wins over result_ready (Requirements).
        """
        key = WorkStatusKey(origin_epoch, turn_id, parent_work_item_id or work_item_id)
        children = self._work_status_children.setdefault(key, {})
        previous_child = children.get(work_item_id)
        if previous_child is not None and not legal_work_status_transition(
            previous_child.state, state
        ):
            return None
        children[work_item_id] = WorkStatus(
            turn_id=turn_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            state=state,
            event_sequence=0,
            terminal_reason=terminal_reason,
            origin_epoch=origin_epoch,
        )
        return self._reaggregate_parent(key)

    def _reaggregate_parent(self, key: WorkStatusKey) -> StateEvent | None:
        children = list(self._work_status_children.get(key, {}).values())
        if not children:
            return None
        parent_state, parent_reason = self._aggregate(children)
        record = self._work_status_parents.get(key)
        if record is not None and record.status.state == parent_state:
            return None
        # The parent's "state" is a pure recomputation over the current child
        # set on every call, not a step in its own forward state machine, so
        # legal_work_status_transition (built for one entity's sequential
        # progression) does not apply here. The only invariant to preserve is
        # that a terminal parent state never regresses.
        if record is not None and record.status.state in WORK_STATUS_TERMINAL:
            return None
        next_sequence = self._work_status_sequence.get(key, 0) + 1
        self._work_status_sequence[key] = next_sequence
        status = WorkStatus(
            turn_id=key.turn_id,
            work_item_id=key.parent_key,
            worker_id=None,
            state=parent_state,
            event_sequence=next_sequence,
            terminal_reason=parent_reason,
            origin_epoch=key.origin_epoch,
        )
        terminal_at = time.monotonic() if parent_state in WORK_STATUS_TERMINAL else None
        self._work_status_parents[key] = _WorkStatusRecord(status=status, terminal_at=terminal_at)
        return self._emit("work_status", status.model_dump(mode="json"))

    @staticmethod
    def _aggregate(children: list[WorkStatus]) -> tuple[str, str | None]:
        if any(child.state == "routing" for child in children):
            return "routing", None
        if any(child.state == "searching" for child in children):
            return "searching", None
        if any(child.state not in WORK_STATUS_TERMINAL for child in children):
            return "background", None
        failed = [child for child in children if child.state == "failed"]
        if failed:
            reason = next(
                (child.terminal_reason for child in failed if child.terminal_reason), None
            )
            return "failed", reason
        if all(child.state == "cancelled" for child in children):
            return "cancelled", None
        return "result_ready", None

    def work_status_snapshot(self) -> tuple[WorkStatus, ...]:
        """Return live parent work-status records, pruning expired terminals."""
        now = time.monotonic()
        live: list[WorkStatus] = []
        for record in self._work_status_parents.values():
            if (
                record.terminal_at is not None
                and (now - record.terminal_at) >= WORK_STATUS_TTL_SECONDS
            ):
                continue
            live.append(record.status)
        return tuple(live)

    @classmethod
    def from_snapshot(cls, snapshot: RuntimeSnapshot) -> SessionState:
        state = cls(snapshot.session_id)
        state.sequence = snapshot.snapshot_sequence
        for worker in snapshot.workers:
            state.workers[worker.worker_id] = worker
        for result in snapshot.results:
            state.results.append(result)
        for progress in snapshot.speech_progress:
            state.speech[progress.utterance_id] = progress
            state._speech_history[progress.utterance_id] = [progress]
        state.routing = snapshot.routing
        state.transcript.extend(snapshot.transcript)
        return state

    def snapshot(
        self, origin_epoch: int | None = None, *, include_work_status: bool = False
    ) -> RuntimeSnapshot:
        return RuntimeSnapshot(
            contract_version="v1.0",
            session_id=self.session_id,
            snapshot_sequence=self.sequence,
            workers=list(self.workers.values()),
            results=list(self.results.results),
            speech_progress=list(self.speech.values()),
            routing=self.routing,
            transcript=list(self.transcript),
            origin_epoch=origin_epoch,
            work_status=list(self.work_status_snapshot()) if include_work_status else [],
        )
