"""Strict v1 contracts shared by the Python runtime and browser client."""

from __future__ import annotations

from enum import Enum
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

CONTRACT_VERSION = "v1.0"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True, use_enum_values=False)


class WorkItemState(str, Enum):
    STARTED = "started"
    PROGRESS = "progress"
    CANCELLATION_REQUESTED = "cancellation_requested"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    FAILED = "failed"


class DeliveryState(str, Enum):
    DISPLAYED = "displayed"
    QUEUED = "queued"
    STARTED = "started"
    SYNTHESIS_ENDED = "synthesis_ended"
    DELIVERY_COMPLETED = "delivery_completed"
    DELIVERY_UNKNOWN = "delivery_unknown"
    INTERRUPTED = "interrupted"
    INTERRUPTED_BY_RECONNECT = "interrupted_by_reconnect"
    PAUSED = "paused"
    RESUMED = "resumed"


class Citation(StrictModel):
    title: str = ""
    url: str = Field(pattern=r"^https?://[^\s]+$")


class RoutingDecision(StrictModel):
    action: Literal["direct", "unsupported", "clarify", "existing_worker", "new_worker"]
    worker_id: str | None = None
    worker_type: str | None = None
    topic: str | None = None
    capability: str | None = None
    capability_available: bool = True
    model_policy: str | None = None
    catalogue_version: str
    catalogue_worker_ids: tuple[str, ...] = ()
    origin_epoch: int | None = None

    @model_validator(mode="after")
    def validate_selection(self) -> RoutingDecision:
        if self.action in {"existing_worker", "new_worker"}:
            if (
                not self.worker_id
                or not self.worker_type
                or not self.topic
                or not self.model_policy
            ):
                raise ValueError(
                    "worker routes require worker_id, worker_type, topic, and model_policy"
                )
            if self.action == "existing_worker" and self.catalogue_worker_ids:
                if self.worker_id not in self.catalogue_worker_ids:
                    raise ValueError("worker_id is not present in the catalogue snapshot")
            if self.worker_id and "hallucinated" in self.worker_id.lower():
                raise ValueError("worker_id is not an allowlisted catalogue identity")
            if not self.capability_available and self.capability not in {None, "private_calendar"}:
                raise ValueError("unavailable capability cannot be dispatched")
            if not self.capability_available and self.worker_type == "web_search":
                raise ValueError("an unavailable private capability cannot become web search")
        elif any(value is not None for value in (self.worker_id, self.worker_type, self.topic)):
            raise ValueError("non-worker routes cannot select a worker")
        return self


class WorkerState(StrictModel):
    worker_id: str
    topic: str
    model_policy: str
    status: str
    latest_result_id: str | None = None
    origin_epoch: int | None = None


class GroundedResult(StrictModel):
    result_id: str
    worker_id: str
    turn_id: str
    text: str
    citations: list[Citation] = Field(default_factory=list)
    spoken_text: str
    ui_text: str
    spoken_result_id: str | None = None
    ui_result_id: str | None = None
    spoken_citations: list[Citation] | None = None
    ui_citations: list[Citation] | None = None
    origin_epoch: int | None = None

    @model_validator(mode="after")
    def derive_and_validate_projections(self) -> GroundedResult:
        if self.spoken_text != self.text or self.ui_text != self.text:
            raise ValueError("speech and UI projections must derive from the canonical result text")
        if self.spoken_result_id not in (None, self.result_id) or self.ui_result_id not in (
            None,
            self.result_id,
        ):
            raise ValueError("projections must reference the canonical result")
        if self.spoken_citations not in (None, self.citations) or self.ui_citations not in (
            None,
            self.citations,
        ):
            raise ValueError("projections must preserve canonical citations")
        object.__setattr__(self, "spoken_result_id", self.result_id)
        object.__setattr__(self, "ui_result_id", self.result_id)
        object.__setattr__(self, "spoken_citations", list(self.citations))
        object.__setattr__(self, "ui_citations", list(self.citations))
        return self


class SpeechProgress(StrictModel):
    result_id: str
    work_item_id: str
    run_id: str
    utterance_id: str
    state: DeliveryState
    origin_epoch: int | None = None


class RuntimeSnapshot(StrictModel):
    contract_version: str
    session_id: str
    snapshot_sequence: int = Field(ge=0)
    workers: list[WorkerState] = Field(default_factory=list)
    results: list[GroundedResult] = Field(default_factory=list)
    speech_progress: list[SpeechProgress] = Field(default_factory=list)
    origin_epoch: int | None = None
    _highest_by_session: ClassVar[dict[str, int]] = {}

    @classmethod
    def reset_monotonicity(cls, session_id: str) -> None:
        cls._highest_by_session.pop(session_id, None)

    @model_validator(mode="after")
    def validate_version_and_monotonicity(self) -> RuntimeSnapshot:
        if self.contract_version != CONTRACT_VERSION:
            raise ValueError(f"unsupported contract version: {self.contract_version}")
        previous = type(self)._highest_by_session.get(self.session_id)
        if previous is not None and self.snapshot_sequence < previous:
            raise ValueError("snapshot_sequence must be monotonic")
        type(self)._highest_by_session[self.session_id] = max(previous or 0, self.snapshot_sequence)
        return self


class WorkItemEvent(StrictModel):
    work_item_id: str
    run_id: str
    worker_id: str
    turn_id: str
    event_id: str
    event_sequence: int = Field(ge=0)
    state: WorkItemState
    timestamp: str
    origin_epoch: int | None = None


class InterruptionEvent(StrictModel):
    interruption_id: str
    stage: Literal["result_pending", "voice_llm_accepted", "speech_active"]
    policy: str
    template_used: bool
    resume_outcome: Literal["resumed", "declined", "stale"]
    work_item_id: str
    run_id: str
    utterance_id: str
    origin_epoch: int | None = None


class SnapshotHandshake(StrictModel):
    contract_version: str = CONTRACT_VERSION
    session_id: str
    resume_token: str
    proposed_epoch: int = Field(ge=0)
    snapshot_sequence: int = Field(ge=0)


def validate_contract(value: Any) -> Any:
    """Validate a model or a JSON-compatible mapping against its model contract."""
    if isinstance(value, StrictModel):
        value.__class__.model_validate(value.model_dump())
        return value
    raise TypeError("validate_contract expects a versioned contract model")
