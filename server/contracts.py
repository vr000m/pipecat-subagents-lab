"""Strict v1 contracts shared by the Python runtime and browser client."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from enum import Enum
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

CONTRACT_VERSION = "v1.0"
RFC3339_TIMESTAMP = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)


def utc_timestamp() -> str:
    """Return a producer-generated RFC 3339 timestamp for wire contracts."""
    return datetime.now(UTC).isoformat()


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
            if (
                self.action == "existing_worker"
                and self.catalogue_worker_ids
                and self.worker_id not in self.catalogue_worker_ids
            ):
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
    # Two distinct gates govern this field, and they are not interchangeable:
    #   * *content* -- ``SessionState.snapshot(include_work_status=...)``
    #     decides whether the ledger is computed at all (a non-capable
    #     projection carries an empty list on the model).
    #   * *wire presence* -- :meth:`wire_payload` is the sole mechanism that
    #     drops the key from the serialized frame, so a non-capable
    #     connection's snapshot still validates against the frozen
    #     pre-Phase-3 runtime-snapshot schema, which does not know the field.
    # The model itself always has the attribute; never assume absence here.
    work_status: list[WorkStatus] = Field(default_factory=list)
    _highest_by_session: ClassVar[dict[str, int]] = {}

    def wire_payload(self, *, include_work_status: bool) -> dict[str, Any]:
        """Serialize for the wire, dropping ``work_status`` when not negotiated.

        The single choke point for snapshot serialization. ``work_status`` is
        excluded by name rather than via ``exclude_none``/``exclude_defaults``:
        ``routing`` and ``origin_epoch`` are nullable but *required* by
        shared/schemas/runtime-snapshot.json, so a blanket exclusion would
        strip them and break schema validation.
        """
        return self.model_dump(
            mode="json", exclude=None if include_work_status else {"work_status"}
        )

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


# The single spelling of the capability name that gates the `work_status`
# wire kind. The browser mirrors it in `web/src/protocol.js`; the two literals
# must agree exactly (asserted in tests/test_contracts.py).
WORK_STATUS_V1 = "work_status_v1"
WORK_STATUS_STATES = (
    "routing",
    "searching",
    "background",
    "result_ready",
    "failed",
    "cancelled",
)
WorkStatusState = Literal[
    "routing", "searching", "background", "result_ready", "failed", "cancelled"
]
TerminalReason = Literal["missing_worker", "retention_rejected"]
_WORK_STATUS_TRANSITIONS: dict[str, frozenset[str]] = {
    "routing": frozenset({"searching", "failed", "cancelled"}),
    "searching": frozenset({"background", "result_ready", "failed", "cancelled"}),
    "background": frozenset({"result_ready", "failed", "cancelled"}),
    "result_ready": frozenset(),
    "failed": frozenset(),
    "cancelled": frozenset(),
}
WORK_STATUS_TERMINAL = frozenset({"result_ready", "failed", "cancelled"})
# States a work item may be recorded at with no prior status of its own.
# ``failed`` is cold-startable because a child can fail before it is ever
# routed (missing worker, missing search capability -- server/pipeline.py
# emits `failed`/`missing_worker` there without a preceding `routing`), and
# the parent join must still terminalize. ``cancelled`` is deliberately NOT
# cold-startable: SessionHost._cancel_child_work_statuses sweeps the whole
# delegated child set on a turn cancel and relies on this rejection to make
# a child that never had a status a no-op rather than inventing one.
WORK_STATUS_COLD_START = frozenset({"routing", "searching", "background", "result_ready", "failed"})


def legal_work_status_transition(previous: str | None, state: str) -> bool:
    """Return whether ``state`` may legally follow ``previous`` (or start cold)."""
    if previous is None:
        return state in WORK_STATUS_COLD_START
    if previous == state:
        return False
    return state in _WORK_STATUS_TRANSITIONS.get(previous, frozenset())


class WorkStatus(StrictModel):
    turn_id: str = Field(min_length=1)
    work_item_id: str | None = None
    worker_id: str | None = None
    state: WorkStatusState
    event_sequence: int = Field(ge=0)
    terminal_reason: TerminalReason | None = None
    origin_epoch: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_terminal_reason(self) -> WorkStatus:
        if self.terminal_reason is not None and self.state != "failed":
            raise ValueError("terminal_reason may only be set for the failed state")
        return self


class SnapshotHandshake(StrictModel):
    contract_version: str = CONTRACT_VERSION
    session_id: str
    resume_token: str
    proposed_epoch: int = Field(ge=0)
    snapshot_sequence: int = Field(ge=0)
    # Capability negotiation (Phase 3): a normalized, deduplicated, lexically
    # sorted set of capability names the browser declares support for.
    # ``capabilities_present`` distinguishes an explicitly-empty/all-unknown
    # array from omission, so PATCH inheritance-vs-mismatch can be decided
    # without conflating the two (see SessionHost.validate_patch_handshake).
    capabilities: tuple[str, ...] = ()
    capabilities_present: bool = False

    @field_validator("capabilities")
    @classmethod
    def validate_capabilities(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not isinstance(item, str) or not item for item in value):
            raise ValueError("capabilities must be non-empty strings")
        return tuple(sorted(set(value)))


def validate_contract(value: Any) -> Any:
    """Validate a model or a JSON-compatible mapping against its model contract."""
    if isinstance(value, StrictModel):
        value.__class__.model_validate(value.model_dump())
        return value
    raise TypeError("validate_contract expects a versioned contract model")
