"""Small typed RTVI message envelope with epoch and sequence fencing."""

from __future__ import annotations

from typing import Any, Callable, Literal

from pydantic import BaseModel, ConfigDict, Field

from .contracts import CONTRACT_VERSION, GroundedResult, RuntimeSnapshot, SpeechProgress


class RTVIMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")
    contract_version: str = CONTRACT_VERSION
    session_id: str
    sequence: int = Field(ge=0)
    kind: Literal["result", "speech", "runtime_snapshot"]
    data: dict[str, Any]
    origin_epoch: int | None = None


class RTVIMessagePublisher:
    def __init__(
        self,
        session_id: str,
        active_epoch: int,
        sequence_provider: Callable[[], int] | None = None,
    ) -> None:
        self.session_id, self.active_epoch = session_id, active_epoch
        RuntimeSnapshot.reset_monotonicity(session_id)
        self._sequence = 0
        self._sequence_provider = sequence_provider
        self._ready = False
        self._snapshot: RuntimeSnapshot | None = None

    def _message(self, kind: str, data: dict[str, Any], origin_epoch: int) -> RTVIMessage | None:
        if origin_epoch != self.active_epoch:
            return None
        self._sequence = (
            self._sequence_provider() if self._sequence_provider is not None else self._sequence + 1
        )
        return RTVIMessage(
            session_id=self.session_id,
            sequence=self._sequence,
            kind=kind,
            data=data,
            origin_epoch=origin_epoch,
        )

    def result(self, result: GroundedResult, *, origin_epoch: int) -> RTVIMessage | None:
        return self._message("result", result.model_dump(mode="json"), origin_epoch)

    def speech(self, progress: SpeechProgress, *, origin_epoch: int) -> RTVIMessage | None:
        return self._message("speech", progress.model_dump(mode="json"), origin_epoch)

    def client_ready(self, *, epoch: int) -> None:
        if epoch != self.active_epoch:
            raise ValueError("client-ready from stale epoch")
        self._ready = True

    def set_snapshot(self, snapshot: RuntimeSnapshot) -> None:
        self._snapshot = snapshot
        # Snapshot sequence is authoritative session state, not a publisher
        # sequence. Repeated snapshots do not invent state events.
        self._sequence = max(self._sequence, snapshot.snapshot_sequence)

    def snapshot(self) -> RTVIMessage | None:
        if not self._ready:
            return None
        data = (
            self._snapshot.model_dump(mode="json")
            if self._snapshot
            else {
                "contract_version": CONTRACT_VERSION,
                "session_id": self.session_id,
                "workers": [],
                "results": [],
                "speech_progress": [],
                "routing": None,
                "transcript": [],
                "origin_epoch": None,
            }
        )
        sequence = (
            self._sequence_provider() if self._sequence_provider is not None else self._sequence
        )
        data["snapshot_sequence"] = sequence
        self._sequence = sequence
        return RTVIMessage(
            session_id=self.session_id,
            sequence=sequence,
            kind="runtime_snapshot",
            data=data,
            origin_epoch=self.active_epoch,
        )
