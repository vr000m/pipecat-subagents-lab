"""Grep-friendly ``PERF_METRIC`` console contract for framework and application timing.

Every record is one physical console line: ``PERF_METRIC event=<name> schema=1
key=value ...``. This module owns the closed event registry, the safe
formatter that rejects anything outside that registry, the injectable
measurement sink protocol, the Pipecat 1.6.0 observer callback factories
that translate framework timing reports into records, and the application
recorders (``AppTurnRecorder``, ``WorkItemRecorder``, ``RetainedRecorder``)
that translate application-turn and retained-work timing into the same
records.
"""

from __future__ import annotations

import json
import math
import re
import time
from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from functools import partial
from types import MappingProxyType
from typing import Any, Literal, Protocol, get_args

from loguru import logger

PREFIX = "PERF_METRIC"
SCHEMA = 1


class PerfMetricError(ValueError):
    """Raised when a record would violate the PERF_METRIC operator contract."""


# --------------------------------------------------------------------------
# Field specifications and the closed event registry
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FieldSpec:
    """One allowed field: its name, value kind, and (for enums) closed values."""

    name: str
    kind: str  # "id" | "int" | "ms" | "bool" | "enum"
    enum_values: frozenset[str] | None = None
    minimum: int = 0
    maxlen: int = 128


def _id(name: str, *, maxlen: int = 128) -> FieldSpec:
    return FieldSpec(name, "id", maxlen=maxlen)


def _int(name: str, *, minimum: int = 0) -> FieldSpec:
    return FieldSpec(name, "int", minimum=minimum)


def _ms(name: str) -> FieldSpec:
    return FieldSpec(name, "ms")


def _bool(name: str) -> FieldSpec:
    return FieldSpec(name, "bool")


def _enum(name: str, values: frozenset[str]) -> FieldSpec:
    return FieldSpec(name, "enum", enum_values=values)


# Each closed vocabulary is declared once as a ``Literal`` alias and its
# runtime frozenset is derived from that alias, so a member can never exist
# statically without also being accepted at runtime (or the reverse).

MetricKind = Literal["ttfb", "text_aggregation", "function_calls", "user_turn_secs"]
METRIC_KINDS: frozenset[str] = frozenset(get_args(MetricKind))

AppTurnOutcome = Literal[
    "direct",
    "unsupported",
    "control",
    "clarify",
    "completed",
    "mixed",
    "retained",
    "declined",
    "failed",
    "cancelled",
]
APP_TURN_OUTCOMES: frozenset[str] = frozenset(get_args(AppTurnOutcome))

ControlAction = Literal["pause", "resume", "cancel", "stop", "consent"]
CONTROL_ACTIONS: frozenset[str] = frozenset(get_args(ControlAction))

ControlOutcome = Literal["applied", "unknown_target", "no_active", "no_pending"]
CONTROL_OUTCOMES: frozenset[str] = frozenset(get_args(ControlOutcome))

WorkItemOutcome = Literal[
    "direct",
    "unsupported",
    "completed",
    "retained",
    "clarify",
    "declined",
    "failed",
    "cancelled",
    "missing_worker",
    "missing_search",
    "capacity_rejected",
    "retention_rejected",
]
WORK_ITEM_OUTCOMES: frozenset[str] = frozenset(get_args(WorkItemOutcome))

WorkOutcome = Literal["completed", "failed", "cancelled", "invalid_result"]
WORK_OUTCOMES: frozenset[str] = frozenset(get_args(WorkOutcome))

CommitOutcome = Literal[
    "committed",
    "not_applicable",
    "suppressed_cancelled",
    "suppressed_stale",
    "suppressed_duplicate",
    "suppressed_shutdown",
    "failed",
]
COMMIT_OUTCOMES: frozenset[str] = frozenset(get_args(CommitOutcome))

SpeechOutcome = Literal[
    "queued",
    "no_tts",
    "disconnected",
    "stale_connection",
    "cancelled",
    "not_applicable",
    "enqueue_failed",
    "start_failed",
]
SPEECH_OUTCOMES: frozenset[str] = frozenset(get_args(SpeechOutcome))

_APP_TURN_COUNTER_FIELDS = (
    "direct_count",
    "unsupported_count",
    "completed_count",
    "retained_count",
    "clarification_count",
    "declined_count",
    "failed_count",
    "cancelled_count",
)


def _validate_service_latency(fields: Mapping[str, Any]) -> None:
    kind = fields.get("metric_kind")
    has_processor = "processor" in fields
    has_function_name = "function_name" in fields
    if kind in ("ttfb", "text_aggregation"):
        if not has_processor:
            raise PerfMetricError(f"service_latency: metric_kind={kind} requires 'processor'")
        if has_function_name:
            raise PerfMetricError(
                f"service_latency: metric_kind={kind} must not carry 'function_name'"
            )
    elif kind == "function_calls":
        if not has_function_name:
            raise PerfMetricError(
                "service_latency: metric_kind=function_calls requires 'function_name'"
            )
        if has_processor:
            raise PerfMetricError(
                "service_latency: metric_kind=function_calls must not carry 'processor'"
            )
    elif kind == "user_turn_secs":
        if has_processor or has_function_name:
            raise PerfMetricError(
                "service_latency: metric_kind=user_turn_secs must not carry 'processor' or 'function_name'"
            )


def _validate_app_turn_foreground(fields: Mapping[str, Any]) -> None:
    child_count = fields["child_count"]
    counted = sum(fields.get(name, 0) for name in _APP_TURN_COUNTER_FIELDS)
    if counted != child_count:
        raise PerfMetricError(
            f"app_turn_foreground: child_count={child_count} does not match counter sum {counted}"
        )
    outcome = fields["outcome"]
    has_control_fields = "control_action" in fields or "control_outcome" in fields
    if outcome == "control":
        if "control_action" not in fields or "control_outcome" not in fields:
            raise PerfMetricError(
                "app_turn_foreground: outcome=control requires control_action and control_outcome"
            )
    elif has_control_fields:
        raise PerfMetricError(
            "app_turn_foreground: control_action/control_outcome only allowed with outcome=control"
        )


@dataclass(frozen=True, slots=True)
class EventSpec:
    required: tuple[FieldSpec, ...]
    optional: tuple[FieldSpec, ...] = ()
    validate: Callable[[Mapping[str, Any]], None] | None = None
    _by_name: Mapping[str, FieldSpec] = field(init=False, repr=False, compare=False)
    _ordered_names: tuple[str, ...] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_by_name", {f.name: f for f in (*self.required, *self.optional)})
        object.__setattr__(
            self, "_ordered_names", tuple(f.name for f in (*self.required, *self.optional))
        )

    @property
    def by_name(self) -> Mapping[str, FieldSpec]:
        return self._by_name

    @property
    def ordered_names(self) -> tuple[str, ...]:
        return self._ordered_names


EVENT_REGISTRY: Mapping[str, EventSpec] = MappingProxyType(
    {
        "pipeline_startup": EventSpec(
            required=(
                _id("session_id"),
                _int("origin_epoch"),
                _id("connection_worker"),
                _ms("total_ms"),
            ),
            optional=(_int("processor_count"),),
        ),
        "transport_ready": EventSpec(
            required=(
                _id("session_id"),
                _int("origin_epoch"),
                _id("connection_worker"),
                _ms("client_connected_ms"),
            ),
            optional=(_ms("bot_connected_ms"),),
        ),
        "pipecat_turn_start": EventSpec(
            required=(
                _id("session_id"),
                _int("origin_epoch"),
                _id("connection_worker"),
                _int("pipecat_turn", minimum=1),
            ),
        ),
        "pipecat_turn_end": EventSpec(
            required=(
                _id("session_id"),
                _int("origin_epoch"),
                _id("connection_worker"),
                _int("pipecat_turn", minimum=1),
                _ms("duration_ms"),
                _bool("interrupted"),
            ),
        ),
        "first_bot_speech_latency": EventSpec(
            required=(
                _id("session_id"),
                _int("origin_epoch"),
                _id("connection_worker"),
                _ms("latency_ms"),
            ),
        ),
        "user_bot_latency": EventSpec(
            required=(
                _id("session_id"),
                _int("origin_epoch"),
                _id("connection_worker"),
                _ms("latency_ms"),
            ),
        ),
        "service_latency": EventSpec(
            required=(
                _id("session_id"),
                _int("origin_epoch"),
                _id("connection_worker"),
                _enum("metric_kind", METRIC_KINDS),
                _ms("value_ms"),
            ),
            optional=(_id("processor"), _id("function_name"), _int("pipecat_turn", minimum=1)),
            validate=_validate_service_latency,
        ),
        "app_turn_foreground": EventSpec(
            required=(
                _id("session_id"),
                _int("origin_epoch"),
                _id("turn_id"),
                _enum("outcome", APP_TURN_OUTCOMES),
                _ms("total_ms"),
                _int("child_count"),
                _int("direct_count"),
                _int("unsupported_count"),
                _int("completed_count"),
                _int("retained_count"),
                _int("clarification_count"),
                _int("declined_count"),
                _int("failed_count"),
                _int("cancelled_count"),
            ),
            optional=(
                _enum("control_action", CONTROL_ACTIONS),
                _enum("control_outcome", CONTROL_OUTCOMES),
                _ms("routing_ms"),
                _ms("commit_ms"),
            ),
            validate=_validate_app_turn_foreground,
        ),
        "work_item_foreground": EventSpec(
            required=(
                _id("session_id"),
                _int("origin_epoch"),
                _id("turn_id"),
                _id("work_item_id"),
                _enum("outcome", WORK_ITEM_OUTCOMES),
                _ms("total_ms"),
            ),
            optional=(_id("app_worker_id"), _id("result_id"), _ms("search_ms"), _ms("commit_ms")),
        ),
        "work_item_background": EventSpec(
            required=(
                _id("session_id"),
                _int("origin_epoch"),
                _id("turn_id"),
                _id("work_item_id"),
                _id("app_worker_id"),
                _ms("background_ms"),
                _enum("work_outcome", WORK_OUTCOMES),
                _enum("commit_outcome", COMMIT_OUTCOMES),
                _enum("speech_outcome", SPEECH_OUTCOMES),
            ),
            optional=(_id("result_id"),),
        ),
    }
)


# --------------------------------------------------------------------------
# Value validation and formatting
# --------------------------------------------------------------------------


_UNSAFE_ID_CHARS = re.compile(r'["\x00-\x1f\x7f]')


def _format_value(spec: FieldSpec, value: Any) -> str:
    if spec.kind == "id":
        if not isinstance(value, str) or not value:
            raise PerfMetricError(f"{spec.name}: expected a non-empty string, got {value!r}")
        if _UNSAFE_ID_CHARS.search(value):
            raise PerfMetricError(
                f"{spec.name}: identifiers must not contain quotes or control characters, got {value!r}"
            )
        if len(value) > spec.maxlen:
            raise PerfMetricError(
                f"{spec.name}: expected at most {spec.maxlen} characters, got {len(value)}"
            )
        return json.dumps(value)
    if spec.kind == "int":
        if isinstance(value, bool) or not isinstance(value, int):
            raise PerfMetricError(f"{spec.name}: expected an int, got {value!r}")
        if value < spec.minimum:
            raise PerfMetricError(f"{spec.name}: expected >= {spec.minimum}, got {value!r}")
        return str(value)
    if spec.kind == "ms":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise PerfMetricError(
                f"{spec.name}: expected a duration in milliseconds, got {value!r}"
            )
        if not math.isfinite(value) or value < 0:
            raise PerfMetricError(
                f"{spec.name}: expected a finite non-negative duration, got {value!r}"
            )
        return f"{value:.1f}"
    if spec.kind == "bool":
        if not isinstance(value, bool):
            raise PerfMetricError(f"{spec.name}: expected a bool, got {value!r}")
        return "true" if value else "false"
    if spec.kind == "enum":
        if not isinstance(value, str) or value not in (spec.enum_values or frozenset()):
            raise PerfMetricError(
                f"{spec.name}: expected one of {sorted(spec.enum_values or ())}, got {value!r}"
            )
        return value
    raise PerfMetricError(f"{spec.name}: unsupported field kind {spec.kind!r}")  # pragma: no cover


def format_record(event: str, fields: Mapping[str, Any]) -> str:
    """Render one validated ``PERF_METRIC`` line, or raise ``PerfMetricError``."""
    spec = EVENT_REGISTRY.get(event)
    if spec is None:
        raise PerfMetricError(f"unknown event {event!r}")

    allowed = spec.by_name
    unknown = [name for name in fields if name not in allowed]
    if unknown:
        raise PerfMetricError(f"{event}: unknown field(s) {sorted(unknown)}")
    missing = [f.name for f in spec.required if f.name not in fields]
    if missing:
        raise PerfMetricError(f"{event}: missing required field(s) {missing}")

    if spec.validate is not None:
        spec.validate(fields)

    parts = [PREFIX, f"event={event}", f"schema={SCHEMA}"]
    for name in spec.ordered_names:
        if name not in fields:
            continue
        parts.append(f"{name}={_format_value(allowed[name], fields[name])}")
    return " ".join(parts)


# --------------------------------------------------------------------------
# Measurement records and sinks
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MeasurementRecord:
    """One validated, immutable ``PERF_METRIC`` measurement."""

    event: str
    fields: Mapping[str, Any]
    line: str


def build_record(event: str, **fields: Any) -> MeasurementRecord:
    line = format_record(event, fields)
    return MeasurementRecord(event=event, fields=MappingProxyType(dict(fields)), line=line)


class MeasurementSink(Protocol):
    """Narrow sink protocol every measurement producer emits through."""

    def emit(self, record: MeasurementRecord) -> None: ...


class ConsoleMeasurementSink:
    """Production sink: one raw Loguru line beginning ``PERF_METRIC event=``."""

    def emit(self, record: MeasurementRecord) -> None:
        logger.opt(raw=True).info(record.line + "\n")


class CollectingMeasurementSink:
    """Test/smoke sink: in-memory records indexed by event, turn_id, work_item_id."""

    def __init__(self) -> None:
        self._records: list[MeasurementRecord] = []
        self._by_key: dict[tuple[str, Any, Any], list[MeasurementRecord]] = defaultdict(list)

    def emit(self, record: MeasurementRecord) -> None:
        self._records.append(record)
        key = (record.event, record.fields.get("turn_id"), record.fields.get("work_item_id"))
        self._by_key[key].append(record)

    @property
    def records(self) -> tuple[MeasurementRecord, ...]:
        return tuple(self._records)

    def get(
        self, event: str, *, turn_id: str | None = None, work_item_id: str | None = None
    ) -> tuple[MeasurementRecord, ...]:
        return tuple(self._by_key.get((event, turn_id, work_item_id), ()))


def emit_metric(sink: MeasurementSink, event: str, **fields: Any) -> None:
    """Build one validated record and deliver it to ``sink``.

    This is the one producer entry point: observer callbacks in this module
    and application recorders in a later phase both emit through it. Both a
    contract violation (``PerfMetricError``) and a raised ``sink.emit``
    propagate to the caller — callers that must never let telemetry affect
    pipeline/application behavior (this module's observer callbacks, and
    later application recorders) are responsible for containing them at
    their own boundary.
    """
    record = build_record(event, **fields)
    sink.emit(record)


def _safe_emit(sink: MeasurementSink, event: str, fields: Mapping[str, Any]) -> None:
    """Emit one record, containing any contract or sink failure.

    Used by observer callbacks: telemetry must never crash Pipecat frame
    processing or change routing/result/speech behavior, so failures are
    logged outside the ``PERF_METRIC`` namespace and dropped.
    """
    try:
        emit_metric(sink, event, **fields)
    except Exception:  # noqa: BLE001  # telemetry failures must never propagate into pipeline callbacks
        logger.exception(f"PERF_METRIC failed to emit event={event}")


# --------------------------------------------------------------------------
# Connection context and Pipecat 1.6.0 observer callback factories
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PerfConnectionContext:
    """Immutable identity captured by observer callback closures.

    Callbacks built from this context may close over only this value, the
    sink, and the logger — never the host, runtime, pipeline worker, RTVI
    publisher, or mutable session state.
    """

    session_id: str
    origin_epoch: int
    connection_worker: str

    def base_fields(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "origin_epoch": self.origin_epoch,
            "connection_worker": self.connection_worker,
        }


def make_startup_timing_handlers(
    context: PerfConnectionContext, sink: MeasurementSink
) -> tuple[Callable[[Any, Any], Any], Callable[[Any, Any], Any]]:
    """Build ``on_startup_timing_report``/``on_transport_timing_report`` handlers."""

    async def on_startup_timing_report(_observer: Any, report: Any) -> None:
        fields = context.base_fields()
        fields["total_ms"] = report.total_duration_secs * 1000
        fields["processor_count"] = len(report.processor_timings)
        _safe_emit(sink, "pipeline_startup", fields)

    async def on_transport_timing_report(_observer: Any, report: Any) -> None:
        fields = context.base_fields()
        fields["client_connected_ms"] = report.client_connected_secs * 1000
        if report.bot_connected_secs is not None:
            fields["bot_connected_ms"] = report.bot_connected_secs * 1000
        _safe_emit(sink, "transport_ready", fields)

    return on_startup_timing_report, on_transport_timing_report


def make_user_bot_latency_handlers(
    context: PerfConnectionContext, sink: MeasurementSink
) -> tuple[Callable[[Any, float], Any], Callable[[Any, float], Any], Callable[[Any, Any], Any]]:
    """Build ``on_first_bot_speech_latency``/``on_latency_measured``/``on_latency_breakdown`` handlers."""

    async def on_first_bot_speech_latency(_observer: Any, latency_seconds: float) -> None:
        fields = context.base_fields()
        fields["latency_ms"] = latency_seconds * 1000
        _safe_emit(sink, "first_bot_speech_latency", fields)

    async def on_latency_measured(_observer: Any, latency_seconds: float) -> None:
        fields = context.base_fields()
        fields["latency_ms"] = latency_seconds * 1000
        _safe_emit(sink, "user_bot_latency", fields)

    async def on_latency_breakdown(_observer: Any, breakdown: Any) -> None:
        for ttfb in breakdown.ttfb:
            fields = context.base_fields()
            fields["metric_kind"] = "ttfb"
            fields["value_ms"] = ttfb.duration_secs * 1000
            fields["processor"] = ttfb.processor
            _safe_emit(sink, "service_latency", fields)

        if breakdown.text_aggregation is not None:
            ta = breakdown.text_aggregation
            fields = context.base_fields()
            fields["metric_kind"] = "text_aggregation"
            fields["value_ms"] = ta.duration_secs * 1000
            fields["processor"] = ta.processor
            _safe_emit(sink, "service_latency", fields)

        for call in breakdown.function_calls:
            fields = context.base_fields()
            fields["metric_kind"] = "function_calls"
            fields["value_ms"] = call.duration_secs * 1000
            fields["function_name"] = call.function_name
            _safe_emit(sink, "service_latency", fields)

        if breakdown.user_turn_secs is not None:
            fields = context.base_fields()
            fields["metric_kind"] = "user_turn_secs"
            fields["value_ms"] = breakdown.user_turn_secs * 1000
            _safe_emit(sink, "service_latency", fields)

    return on_first_bot_speech_latency, on_latency_measured, on_latency_breakdown


def make_turn_tracking_handlers(
    context: PerfConnectionContext, sink: MeasurementSink
) -> tuple[Callable[[Any, int], Any], Callable[[Any, int, float, bool], Any]]:
    """Build ``on_turn_started``/``on_turn_ended`` handlers for the default turn tracker."""

    async def on_turn_started(_observer: Any, turn_count: int) -> None:
        fields = context.base_fields()
        fields["pipecat_turn"] = turn_count
        _safe_emit(sink, "pipecat_turn_start", fields)

    async def on_turn_ended(
        _observer: Any, turn_count: int, duration_secs: float, was_interrupted: bool
    ) -> None:
        fields = context.base_fields()
        fields["pipecat_turn"] = turn_count
        fields["duration_ms"] = duration_secs * 1000
        fields["interrupted"] = bool(was_interrupted)
        _safe_emit(sink, "pipecat_turn_end", fields)

    return on_turn_started, on_turn_ended


def attach_framework_observers(
    *,
    startup_observer: Any,
    latency_observer: Any,
    turn_tracking_observer: Any,
    context: PerfConnectionContext,
    sink: MeasurementSink,
) -> None:
    """Register PERF_METRIC handlers on one connection's framework observers.

    ``turn_tracking_observer`` must be the worker's default
    ``turn_tracking_observer`` (``PipelineWorker.turn_tracking_observer``);
    this never constructs a second ``TurnTrackingObserver``.
    """
    on_startup, on_transport = make_startup_timing_handlers(context, sink)
    startup_observer.event_handler("on_startup_timing_report")(on_startup)
    startup_observer.event_handler("on_transport_timing_report")(on_transport)

    on_first_speech, on_latency, on_breakdown = make_user_bot_latency_handlers(context, sink)
    latency_observer.event_handler("on_first_bot_speech_latency")(on_first_speech)
    latency_observer.event_handler("on_latency_measured")(on_latency)
    latency_observer.event_handler("on_latency_breakdown")(on_breakdown)

    on_turn_started, on_turn_ended = make_turn_tracking_handlers(context, sink)
    turn_tracking_observer.event_handler("on_turn_started")(on_turn_started)
    turn_tracking_observer.event_handler("on_turn_ended")(on_turn_ended)


# --------------------------------------------------------------------------
# Application-turn and retained-work recorders
# --------------------------------------------------------------------------


_CHILD_COUNTER_FOR_OUTCOME: Mapping[WorkItemOutcome, str] = MappingProxyType(
    {
        "direct": "direct_count",
        "unsupported": "unsupported_count",
        "completed": "completed_count",
        "retained": "retained_count",
        "clarify": "clarification_count",
        "declined": "declined_count",
        "failed": "failed_count",
        "cancelled": "cancelled_count",
        "missing_worker": "failed_count",
        "missing_search": "failed_count",
        "capacity_rejected": "failed_count",
        "retention_rejected": "failed_count",
    }
)

_PARENT_OUTCOME_FOR_COUNTER: Mapping[str, AppTurnOutcome] = MappingProxyType(
    {
        "direct_count": "direct",
        "unsupported_count": "unsupported",
        "completed_count": "completed",
        "retained_count": "retained",
        "clarification_count": "clarify",
        "declined_count": "declined",
        "failed_count": "failed",
        "cancelled_count": "cancelled",
    }
)


class AppTurnRecorder:
    """Parent ``app_turn_foreground`` recorder: one per accepted semantic turn.

    Callers create children through :meth:`new_child`, which registers each
    one with this parent; a child attributes itself to the parent's counters
    when it finalizes. Any child still open when :meth:`finalize` runs is
    swept to a terminal record first, so every child created under a turn is
    guaranteed exactly one ``work_item_foreground`` record even if the turn
    is cancelled or raises between construction and finalization.

    ``finalize`` is called exactly once and is idempotent so a stray second
    call from a defensive branch cannot double-emit; the emission itself is
    contained by :func:`_safe_emit`, so a producer bug here cannot change
    routing/result/speech behavior.
    """

    def __init__(
        self,
        sink: MeasurementSink,
        *,
        session_id: str,
        origin_epoch: int,
        turn_id: str,
        clock: Callable[[], float] = time.perf_counter,
    ) -> None:
        self._sink = sink
        self._session_id = session_id
        self._origin_epoch = origin_epoch
        self._turn_id = turn_id
        self._clock = clock
        self._start = clock()
        self._counters: dict[str, int] = dict.fromkeys(_APP_TURN_COUNTER_FIELDS, 0)
        self._dispatched_children = 0
        self._open_children: dict[str, WorkItemRecorder] = {}
        self._finalized = False
        self._routing_ms: float | None = None
        self._commit_ms: float | None = None

    @property
    def turn_id(self) -> str:
        return self._turn_id

    @property
    def finalized(self) -> bool:
        return self._finalized

    def new_child(self, *, work_item_id: str) -> WorkItemRecorder:
        """Create one child recorder owned by this turn.

        The child attributes itself to this parent when it finalizes, and is
        swept to a terminal record by :meth:`finalize` if it never does.

        A duplicate ``work_item_id`` cannot displace the already-tracked child:
        the new recorder is still returned so the caller is not broken, but it
        is not registered for the sweep, since overwriting would silently
        orphan the first child from :meth:`finalize`'s sweep.
        """
        child = WorkItemRecorder(
            self._sink,
            session_id=self._session_id,
            origin_epoch=self._origin_epoch,
            turn_id=self._turn_id,
            work_item_id=work_item_id,
            clock=self._clock,
            on_finalize=partial(self._record_child, work_item_id),
        )
        if work_item_id in self._open_children:
            logger.warning(
                f"app_turn_foreground: duplicate work_item_id={work_item_id!r} for "
                f"turn_id={self._turn_id!r}; the new child recorder will not be "
                f"tracked for the finalize sweep"
            )
            return child
        self._open_children[work_item_id] = child
        return child

    def _record_child(self, work_item_id: str, outcome: WorkItemOutcome) -> None:
        """Attribute one dispatched child's outcome to its exhaustive counter
        and count it toward ``child_count``.

        Counting is unconditional: each ``WorkItemRecorder`` instance invokes
        this at most once, guarded by its own finalize latch, so a duplicate
        ``work_item_id`` producing two independently-finalizing children must
        still count both. ``_open_children`` is popped best-effort, keyed only
        by ``work_item_id``: for a duplicate id it may de-register whichever
        child currently holds that key rather than the specific caller, so
        under a duplicate id at most one of the pair is ever reachable by the
        finalize sweep, and it is not guaranteed to be a particular one.
        """
        self._open_children.pop(work_item_id, None)
        # ``outcome`` is validated (and degraded to "failed" if unrecognized)
        # by ``WorkItemRecorder.finalize`` before it emits and invokes this
        # callback, so ``_CHILD_COUNTER_FOR_OUTCOME`` is exhaustive here by
        # construction; the fallback below is defense-in-depth only.
        counter = _CHILD_COUNTER_FOR_OUTCOME.get(outcome)
        if counter is None:
            logger.warning(
                f"app_turn_foreground: unrecognized child outcome {outcome!r}; "
                f"counting it as failed"
            )
            counter = "failed_count"
        self._counters[counter] += 1
        self._dispatched_children += 1

    def record_routing(self, routing_ms: float) -> None:
        self._routing_ms = routing_ms

    def record_commit(self, commit_ms: float) -> None:
        self._commit_ms = commit_ms

    def finalize(
        self,
        *,
        outcome: AppTurnOutcome | None = None,
        control_action: ControlAction | None = None,
        control_outcome: ControlOutcome | None = None,
    ) -> None:
        if self._finalized:
            return
        self._finalized = True
        # Sweep before deriving the outcome so a fully-swept turn still derives
        # a real outcome from its counters instead of the no-children fallback.
        if self._open_children:
            sweep_outcome: WorkItemOutcome = "cancelled" if outcome == "cancelled" else "failed"
            for child in tuple(self._open_children.values()):
                logger.warning(
                    f"app_turn_foreground: child work_item was never finalized for "
                    f"turn_id={self._turn_id!r}; recording outcome={sweep_outcome}"
                )
                child.finalize(outcome=sweep_outcome)
        total_ms = (self._clock() - self._start) * 1000
        if outcome is None:
            nonzero = [name for name in _APP_TURN_COUNTER_FIELDS if self._counters[name]]
            if len(nonzero) == 1:
                outcome = _PARENT_OUTCOME_FOR_COUNTER[nonzero[0]]
            elif len(nonzero) > 1:
                outcome = "mixed"
            else:
                logger.warning(
                    f"app_turn_foreground: no outcome could be derived for "
                    f"turn_id={self._turn_id!r}; recording outcome=failed"
                )
                outcome = "failed"
        # ``_validate_app_turn_foreground`` enforces three rules: outcome=control
        # requires both control fields, a half-populated pair is rejected, and
        # control fields are forbidden when outcome != control. A rejected record
        # is a dropped record and ``_finalized`` is already latched, so it can
        # never be retried. Check all three here — plus vocabulary membership,
        # which the validator only catches later inside ``_safe_emit`` — so this
        # method stays total for every argument combination rather than depending
        # on every caller getting the pair right.
        # ``outcome`` itself is checked here too: an out-of-vocabulary value
        # (from a caller passing an unchecked string) would otherwise reach
        # the enum validator inside ``_safe_emit`` after ``_finalized`` is
        # already latched, dropping the record with no retry — the same
        # failure mode the control-field checks above exist to prevent.
        outcome_vocabulary_ok = outcome in APP_TURN_OUTCOMES
        control_pair_present = control_action is not None and control_outcome is not None
        control_pair_absent = control_action is None and control_outcome is None
        control_vocabulary_ok = (control_action is None or control_action in CONTROL_ACTIONS) and (
            control_outcome is None or control_outcome in CONTROL_OUTCOMES
        )
        control_consistent = (
            outcome_vocabulary_ok
            and control_vocabulary_ok
            and (control_pair_present or control_pair_absent)
            and (outcome == "control") == control_pair_present
        )
        if not control_consistent:
            logger.warning(
                f"app_turn_foreground: inconsistent control fields for "
                f"turn_id={self._turn_id!r} (outcome={outcome!r}, "
                f"control_action={control_action!r}, control_outcome={control_outcome!r}); "
                f"recording outcome=failed without control fields"
            )
            control_action = None
            control_outcome = None
            outcome = "failed"
        fields: dict[str, Any] = {
            "session_id": self._session_id,
            "origin_epoch": self._origin_epoch,
            "turn_id": self._turn_id,
            "outcome": outcome,
            "total_ms": total_ms,
            "child_count": self._dispatched_children,
            **self._counters,
        }
        if control_action is not None:
            fields["control_action"] = control_action
        if control_outcome is not None:
            fields["control_outcome"] = control_outcome
        if self._routing_ms is not None:
            fields["routing_ms"] = self._routing_ms
        if self._commit_ms is not None:
            fields["commit_ms"] = self._commit_ms
        _safe_emit(self._sink, "app_turn_foreground", fields)


class WorkItemRecorder:
    """Child ``work_item_foreground`` recorder: one per dispatched work item.

    Created through :meth:`AppTurnRecorder.new_child`, which supplies the
    ``on_finalize`` callback that attributes this item's outcome to the parent
    turn. The callback runs exactly once, on the emitting finalize call; its
    failures are contained, never re-raised.
    """

    def __init__(
        self,
        sink: MeasurementSink,
        *,
        session_id: str,
        origin_epoch: int,
        turn_id: str,
        work_item_id: str,
        clock: Callable[[], float] = time.perf_counter,
        on_finalize: Callable[[WorkItemOutcome], None] | None = None,
    ) -> None:
        self._sink = sink
        self._session_id = session_id
        self._origin_epoch = origin_epoch
        self._turn_id = turn_id
        self._work_item_id = work_item_id
        self._clock = clock
        self._on_finalize = on_finalize
        self._start = clock()
        self._finalized = False

    @property
    def finalized(self) -> bool:
        return self._finalized

    def finalize(
        self,
        *,
        outcome: WorkItemOutcome,
        app_worker_id: str | None = None,
        result_id: str | None = None,
        search_ms: float | None = None,
        commit_ms: float | None = None,
    ) -> WorkItemOutcome:
        """Emit once, notify the owning turn, and return ``outcome``.

        A failure inside the owning turn's attribution callback is logged and
        swallowed: parent accounting is telemetry, and must never propagate
        into the caller's control flow.
        """
        if self._finalized:
            return outcome
        self._finalized = True
        if outcome not in WORK_ITEM_OUTCOMES:
            # An unrecognized outcome would otherwise fail the enum validator
            # deep inside ``_safe_emit`` after ``_finalized`` is already
            # latched, dropping the record with no retry. Degrade to
            # "failed" here so emission and the parent's attribution
            # (``AppTurnRecorder._record_child``) always agree on what was
            # counted.
            logger.warning(
                f"work_item_foreground: unrecognized outcome {outcome!r} for "
                f"work_item_id={self._work_item_id!r}; recording outcome=failed"
            )
            outcome = "failed"
        total_ms = (self._clock() - self._start) * 1000
        fields: dict[str, Any] = {
            "session_id": self._session_id,
            "origin_epoch": self._origin_epoch,
            "turn_id": self._turn_id,
            "work_item_id": self._work_item_id,
            "outcome": outcome,
            "total_ms": total_ms,
        }
        if app_worker_id is not None:
            fields["app_worker_id"] = app_worker_id
        if result_id is not None:
            fields["result_id"] = result_id
        if search_ms is not None:
            fields["search_ms"] = search_ms
        if commit_ms is not None:
            fields["commit_ms"] = commit_ms
        _safe_emit(self._sink, "work_item_foreground", fields)
        if self._on_finalize is not None:
            try:
                self._on_finalize(outcome)
            except Exception:  # noqa: BLE001  # parent accounting must never propagate into the caller
                logger.exception(
                    f"app_turn_foreground: failed to attribute child outcome={outcome!r} "
                    f"for work_item_id={self._work_item_id!r}"
                )
        return outcome


class RetainedRecorder:
    """Host-owned, telemetry-only recorder for one retained work item.

    State machine: ``pending`` -> ``claimed`` -> ``commit_recorded`` ->
    ``speech_recorded`` -> ``finalized``. Every transition is idempotent so a
    duplicate or late callback cannot corrupt an already-progressing or
    already-finalized recorder. This registry is telemetry-only: it never
    participates in cancellation/dedup decisions, which remain owned by
    ``SessionHost``'s existing bookkeeping sets.
    """

    def __init__(
        self,
        sink: MeasurementSink,
        *,
        session_id: str,
        origin_epoch: int,
        turn_id: str,
        work_item_id: str,
        app_worker_id: str,
        clock: Callable[[], float] = time.perf_counter,
    ) -> None:
        self._sink = sink
        self._session_id = session_id
        self._origin_epoch = origin_epoch
        self._turn_id = turn_id
        self._work_item_id = work_item_id
        self._app_worker_id = app_worker_id
        self._clock = clock
        self._start = clock()
        self.state = "pending"
        self.work_outcome: WorkOutcome | None = None
        self.commit_outcome: CommitOutcome | None = None
        self.speech_outcome: SpeechOutcome | None = None
        self.result_id: str | None = None

    @property
    def finalized(self) -> bool:
        return self.state == "finalized"

    def claim(self, terminal_kind: WorkOutcome) -> bool:
        """Advance ``pending`` -> ``claimed`` with the classified terminal kind.

        Invoked synchronously from the coordinator's ``on_late_terminal`` hook,
        before completion-callback shutdown suppression can run, so a claimed
        work outcome is captured even when the normal completion path is
        suppressed.
        """
        if self.state != "pending":
            return False
        self.work_outcome = terminal_kind
        self.state = "claimed"
        return True

    def record_commit(self, commit_outcome: CommitOutcome, *, result_id: str | None = None) -> bool:
        if self.state == "finalized":
            return False
        self.commit_outcome = commit_outcome
        if result_id is not None:
            self.result_id = result_id
        if self.state == "claimed":
            self.state = "commit_recorded"
        return True

    def record_speech(self, speech_outcome: SpeechOutcome) -> bool:
        if self.state == "finalized":
            return False
        self.speech_outcome = speech_outcome
        if self.state == "commit_recorded":
            self.state = "speech_recorded"
        return True

    def finalize(
        self,
        *,
        work_outcome: WorkOutcome | None = None,
        commit_outcome: CommitOutcome | None = None,
        speech_outcome: SpeechOutcome | None = None,
        result_id: str | None = None,
    ) -> bool:
        """Emit the terminal ``work_item_background`` record and close the recorder.

        Idempotent: a callback arriving after finalization is a no-op, which is
        exactly the "callback after recorder already finalized" contract.

        ``work_outcome``, ``commit_outcome``, and ``speech_outcome`` are
        required fields in the event registry. Any left unset by the caller
        (and not already set by a prior ``claim``/``record_commit``/
        ``record_speech`` call) default here to the same terminal values a
        shutdown-triggered finalize implies, so this method can never
        construct an incomplete record.
        """
        if self.state == "finalized":
            return False
        if work_outcome is not None:
            self.work_outcome = work_outcome
        if commit_outcome is not None:
            self.commit_outcome = commit_outcome
        if speech_outcome is not None:
            self.speech_outcome = speech_outcome
        if result_id is not None:
            self.result_id = result_id
        self.work_outcome = self.work_outcome or "cancelled"
        self.commit_outcome = self.commit_outcome or "suppressed_shutdown"
        self.speech_outcome = self.speech_outcome or "cancelled"
        self.state = "finalized"
        background_ms = (self._clock() - self._start) * 1000
        fields: dict[str, Any] = {
            "session_id": self._session_id,
            "origin_epoch": self._origin_epoch,
            "turn_id": self._turn_id,
            "work_item_id": self._work_item_id,
            "app_worker_id": self._app_worker_id,
            "background_ms": background_ms,
            "work_outcome": self.work_outcome,
            "commit_outcome": self.commit_outcome,
            "speech_outcome": self.speech_outcome,
        }
        if self.result_id is not None:
            fields["result_id"] = self.result_id
        _safe_emit(self._sink, "work_item_background", fields)
        return True
