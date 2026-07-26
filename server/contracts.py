"""Strict v1 contracts shared by the Python runtime and browser client."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
import re
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

CONTRACT_VERSION = "v1.0"
RFC3339_TIMESTAMP = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)


def utc_timestamp() -> str:
    """Return a producer-generated RFC 3339 timestamp for wire contracts."""
    return datetime.now(timezone.utc).isoformat()


def validate_rfc3339_timestamp(value: str) -> str:
    """Validate the strict date-time representation shared with the browser."""
    if RFC3339_TIMESTAMP.fullmatch(value) is None:
        raise ValueError("timestamp must be RFC 3339")
    try:
        datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("timestamp must be a valid RFC 3339 date-time") from exc
    return value


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
    origin_epoch: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_selection(self) -> RoutingDecision:
        if self.action in {"existing_worker", "new_worker"}:
            if not self.worker_type or not self.topic or not self.model_policy:
                raise ValueError("worker routes require worker_type, topic, and model_policy")
            if self.action == "existing_worker" and not self.worker_id:
                raise ValueError("existing-worker routes require worker_id")
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


class RoutingState(StrictModel):
    turn_id: str
    action: Literal["direct", "unsupported", "clarify", "existing_worker", "new_worker"]
    worker_id: str | None = None
    worker_type: str | None = None
    topic: str | None = None
    model_policy: str | None = None
    origin_epoch: int | None = Field(default=None, ge=0)


class TranscriptEntry(StrictModel):
    role: Literal["user", "assistant"]
    text: str
    turn_id: str
    timestamp: str = Field(default_factory=utc_timestamp)
    origin_epoch: int | None = Field(default=None, ge=0)

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        return validate_rfc3339_timestamp(value)


class WorkerState(StrictModel):
    worker_id: str
    topic: str
    model_policy: str
    status: str
    latest_result_id: str | None = None
    origin_epoch: int | None = Field(default=None, ge=0)


class GroundedResult(StrictModel):
    result_id: str = Field(min_length=1)
    worker_id: str = Field(min_length=1)
    turn_id: str = Field(min_length=1)
    timestamp: str = Field(default_factory=utc_timestamp, min_length=1)
    text: str = Field(min_length=1)
    citations: list[Citation] = Field(default_factory=list)
    spoken_text: str = Field(min_length=1)
    origin_epoch: int | None = Field(default=None, ge=0)

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        return validate_rfc3339_timestamp(value)

    @model_validator(mode="after")
    def validate_projections(self) -> GroundedResult:
        if not self.text.strip() or not self.spoken_text.strip():
            raise ValueError("result projections must not be blank")
        return self

    @property
    def ui_text(self) -> str:
        return self.text

    @property
    def spoken_result_id(self) -> str:
        return self.result_id

    @property
    def ui_result_id(self) -> str:
        return self.result_id

    @property
    def spoken_citations(self) -> list[Citation]:
        return list(self.citations)

    @property
    def ui_citations(self) -> list[Citation]:
        return list(self.citations)


class SpeechProgress(StrictModel):
    result_id: str
    work_item_id: str
    run_id: str
    utterance_id: str
    state: DeliveryState
    origin_epoch: int | None = Field(default=None, ge=0)


class RuntimeSnapshot(StrictModel):
    contract_version: str
    session_id: str
    snapshot_sequence: int = Field(ge=0)
    workers: list[WorkerState] = Field(default_factory=list)
    results: list[GroundedResult] = Field(default_factory=list)
    speech_progress: list[SpeechProgress] = Field(default_factory=list)
    routing: RoutingState | None = None
    transcript: list[TranscriptEntry] = Field(default_factory=list)
    origin_epoch: int | None = Field(default=None, ge=0)
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
    origin_epoch: int | None = Field(default=None, ge=0)

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        return validate_rfc3339_timestamp(value)


class InterruptionEvent(StrictModel):
    interruption_id: str
    stage: Literal["result_pending", "voice_llm_accepted", "speech_active"]
    policy: str
    template_used: bool
    resume_outcome: Literal["resumed", "declined", "stale"]
    work_item_id: str
    run_id: str
    utterance_id: str
    origin_epoch: int | None = Field(default=None, ge=0)


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
