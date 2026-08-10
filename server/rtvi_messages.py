"""Versioned RTVI message envelopes with epoch and payload validation."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator

from .contracts import (
    CONTRACT_VERSION,
    GroundedResult,
    RoutingState,
    RuntimeSnapshot,
    SpeechProgress,
    TranscriptEntry,
    WorkerState,
    WorkStatus,
)

RTVI_MESSAGE_KINDS = (
    "runtime_snapshot",
    "result",
    "speech_progress",
    "worker",
    "routing",
    "user_transcript",
    "bot_transcript",
    "work_status",
)
RTVIMessageKind = Literal[
    "runtime_snapshot",
    "result",
    "speech_progress",
    "worker",
    "routing",
    "user_transcript",
    "bot_transcript",
    "work_status",
]

_PAYLOAD_MODELS = {
    "runtime_snapshot": RuntimeSnapshot,
    "result": GroundedResult,
    "speech_progress": SpeechProgress,
    "worker": WorkerState,
    "routing": RoutingState,
    "user_transcript": TranscriptEntry,
    "bot_transcript": TranscriptEntry,
    "work_status": WorkStatus,
}


class RTVIMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")
    contract_version: str = CONTRACT_VERSION
    session_id: str = Field(min_length=1)
    sequence: int = Field(ge=0)
    kind: RTVIMessageKind
    data: dict[str, Any]
    origin_epoch: int = Field(ge=0)
    _payload: Any = PrivateAttr(default=None)

    @model_validator(mode="after")
    def validate_versioned_payload(self) -> RTVIMessage:
        if self.contract_version != CONTRACT_VERSION:
            raise ValueError(f"unsupported contract version: {self.contract_version}")
        payload = _PAYLOAD_MODELS[self.kind].model_validate(self.data)
        payload_epoch = payload.origin_epoch
        if payload_epoch != self.origin_epoch:
            raise ValueError("payload origin_epoch must match envelope origin_epoch")
        if self.kind == "user_transcript" and payload.role != "user":
            raise ValueError("user_transcript payload must have the user role")
        if self.kind == "bot_transcript" and payload.role != "assistant":
            raise ValueError("bot_transcript payload must have the assistant role")
        if self.kind == "runtime_snapshot":
            if payload.session_id != self.session_id:
                raise ValueError("snapshot session_id must match envelope session_id")
            if payload.snapshot_sequence != self.sequence:
                raise ValueError("snapshot_sequence must match envelope sequence")
        self.data = payload.model_dump(mode="json")
        self._payload = payload
        return self

    def wire_payload(self, *, include_work_status: bool = True) -> dict[str, Any]:
        """Serialize the envelope for the wire.

        Payload-level field projection is delegated to the validated payload
        model -- see :meth:`RuntimeSnapshot.wire_payload` -- so the decision
        about which fields reach the wire lives on the typed model and never
        as an ad-hoc key removal at the call site.
        """
        frame = self.model_dump(mode="json")
        if isinstance(self._payload, RuntimeSnapshot):
            frame["data"] = self._payload.wire_payload(include_work_status=include_work_status)
        return frame


class RTVIMessagePublisher:
    """Serializes RTVI envelopes for one connection.

    ``_watermark`` is a watermark, not an allocator. :meth:`incremental`
    never allocates a sequence: it serializes the caller-supplied sequence
    verbatim (``RuntimeObserver`` owns the connection-projected counter) and
    only clamps the watermark upward to it. :meth:`set_snapshot` likewise
    only clamps upward from authoritative session state. :meth:`snapshot` is
    the sole allocation point, and it allocates from ``_sequence_provider``
    when one is installed, falling back to the watermark otherwise.
    """

    def __init__(
        self,
        session_id: str,
        active_epoch: int,
        sequence_provider: Callable[[], int] | None = None,
    ) -> None:
        self.session_id, self.active_epoch = session_id, active_epoch
        RuntimeSnapshot.reset_monotonicity(session_id)
        self._watermark = 0
        self._sequence_provider = sequence_provider
        self._ready = False
        self._snapshot: RuntimeSnapshot | None = None

    def incremental(
        self, kind: RTVIMessageKind, data: dict[str, Any], *, sequence: int, origin_epoch: int
    ) -> RTVIMessage | None:
        """Serialize an already-sequenced typed event (Phase 3 observer path).

        Sequence ownership is described once in this class's docstring.
        Returns ``None`` for a stale (non-active-epoch) origin, matching
        every other publisher method's epoch fence.
        """
        if origin_epoch != self.active_epoch:
            return None
        self._watermark = max(self._watermark, sequence)
        return RTVIMessage(
            session_id=self.session_id,
            sequence=sequence,
            kind=kind,
            data=data,
            origin_epoch=origin_epoch,
        )

    def client_ready(self, *, epoch: int) -> None:
        if epoch != self.active_epoch:
            raise ValueError("client-ready from stale epoch")
        self._ready = True

    def set_snapshot(self, snapshot: RuntimeSnapshot) -> None:
        if snapshot.origin_epoch is None:
            snapshot = snapshot.model_copy(update={"origin_epoch": self.active_epoch})
        self._snapshot = snapshot
        # Snapshot sequence is authoritative session state, not a publisher
        # sequence. Repeated snapshots do not invent state events.
        self._watermark = max(self._watermark, snapshot.snapshot_sequence)

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
                "origin_epoch": self.active_epoch,
            }
        )
        sequence = (
            self._sequence_provider() if self._sequence_provider is not None else self._watermark
        )
        data["snapshot_sequence"] = sequence
        self._watermark = sequence
        return RTVIMessage(
            session_id=self.session_id,
            sequence=sequence,
            kind="runtime_snapshot",
            data=data,
            origin_epoch=self.active_epoch,
        )
