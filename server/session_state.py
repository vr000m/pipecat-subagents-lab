"""Authoritative process-lifetime session state and delivery state machine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import uuid4

from .contracts import GroundedResult, RuntimeSnapshot, SpeechProgress, DeliveryState, WorkerState
from .results import ResultLog


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
    DeliveryState.RESUMED: 4,
    DeliveryState.DELIVERY_UNKNOWN: 5,
    DeliveryState.DELIVERY_COMPLETED: 6,
    DeliveryState.INTERRUPTED: 7,
    DeliveryState.INTERRUPTED_BY_RECONNECT: 8,
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class StateEvent:
    sequence: int
    kind: str
    payload: dict[str, Any]


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
        self._events: list[StateEvent] = []
        self.active_epoch: int | None = None
        self._speech_history: dict[str, list[SpeechProgress]] = {}
        self._listeners: list[Callable[[StateEvent], Any]] = []

    @property
    def events(self) -> tuple[StateEvent, ...]:
        return tuple(self._events)

    def subscribe(self, listener: Callable[[StateEvent], Any]) -> None:
        """Register a live projection listener.

        Listeners are deliberately observational: they cannot replace the
        authoritative state mutation and are expected to fence by epoch.
        """
        self._listeners.append(listener)

    def _emit(self, kind: str, payload: dict[str, Any]) -> StateEvent:
        self.sequence += 1
        event = StateEvent(self.sequence, kind, payload)
        self._events.append(event)
        for listener in tuple(self._listeners):
            listener(event)
        return event

    def set_worker(self, worker: WorkerState) -> StateEvent:
        self.workers[worker.worker_id] = worker
        return self._emit("worker", worker.model_dump(mode="json"))

    def append_result(
        self,
        result: GroundedResult,
        sequence: int | None = None,
        origin_epoch: int | None = None,
    ) -> StateEvent:
        if any(item.result_id == result.result_id for item in self.results.results):
            return self._emit("result_duplicate", {"result_id": result.result_id})
        if (
            origin_epoch is not None
            and self.active_epoch is not None
            and origin_epoch != self.active_epoch
        ):
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
        elif result.worker_id:
            self.workers[result.worker_id] = WorkerState(
                worker_id=result.worker_id,
                topic=result.worker_id,
                model_policy="unknown",
                status="idle",
                latest_result_id=result.result_id,
                origin_epoch=origin_epoch,
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
            origin_epoch is not None
            and self.active_epoch is not None
            and origin_epoch != self.active_epoch
            and not allow_stale_reconnect
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
        self, progress: SpeechProgress, origin_epoch: int | None = None
    ) -> SpeechProgress:
        if (
            origin_epoch is not None
            and self.active_epoch is not None
            and origin_epoch != self.active_epoch
        ):
            return progress
        return self.speech_progress(
            result_id=progress.result_id,
            work_item_id=progress.work_item_id,
            run_id=progress.run_id,
            utterance_id=progress.utterance_id,
            state=progress.state,
            origin_epoch=origin_epoch if origin_epoch is not None else progress.origin_epoch,
        )

    def speech_history(self, utterance_id: str) -> tuple[SpeechProgress, ...]:
        return tuple(self._speech_history.get(utterance_id, ()))

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
        return state

    def snapshot(self, origin_epoch: int | None = None) -> RuntimeSnapshot:
        return RuntimeSnapshot(
            contract_version="v1.0",
            session_id=self.session_id,
            snapshot_sequence=self.sequence,
            workers=list(self.workers.values()),
            results=list(self.results.results),
            speech_progress=list(self.speech.values()),
            origin_epoch=origin_epoch,
        )
