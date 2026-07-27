"""Grep-friendly ``PERF_METRIC`` console contract for framework and application timing.

Every record is one physical console line: ``PERF_METRIC event=<name> schema=1
key=value ...``. This module owns the closed event registry, the safe
formatter that rejects anything outside that registry, the injectable
measurement sink protocol, and the Pipecat 1.6.0 observer callback factories
that translate framework timing reports into records. Application-turn and
retained-work producers land in a later phase; this module already knows
their event shapes so the console contract is versioned as one whole.
"""

from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol

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


def _id(name: str) -> FieldSpec:
    return FieldSpec(name, "id")


def _int(name: str, *, minimum: int = 0) -> FieldSpec:
    return FieldSpec(name, "int", minimum=minimum)


def _ms(name: str) -> FieldSpec:
    return FieldSpec(name, "ms")


def _bool(name: str) -> FieldSpec:
    return FieldSpec(name, "bool")


def _enum(name: str, values: frozenset[str]) -> FieldSpec:
    return FieldSpec(name, "enum", enum_values=values)


METRIC_KINDS = frozenset({"ttfb", "text_aggregation", "function_calls", "user_turn_secs"})

APP_TURN_OUTCOMES = frozenset(
    {
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
    }
)
CONTROL_ACTIONS = frozenset({"pause", "resume", "cancel", "stop", "consent"})
CONTROL_OUTCOMES = frozenset({"applied", "unknown_target", "no_active", "no_pending"})
WORK_ITEM_OUTCOMES = frozenset(
    {
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
    }
)
WORK_OUTCOMES = frozenset({"completed", "failed", "cancelled", "invalid_result"})
COMMIT_OUTCOMES = frozenset(
    {
        "committed",
        "not_applicable",
        "suppressed_cancelled",
        "suppressed_stale",
        "suppressed_duplicate",
        "suppressed_shutdown",
        "failed",
    }
)
SPEECH_OUTCOMES = frozenset(
    {
        "queued",
        "no_tts",
        "disconnected",
        "stale_connection",
        "cancelled",
        "not_applicable",
        "enqueue_failed",
        "start_failed",
    }
)

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

    @property
    def by_name(self) -> Mapping[str, FieldSpec]:
        return {f.name: f for f in (*self.required, *self.optional)}

    @property
    def ordered_names(self) -> tuple[str, ...]:
        return tuple(f.name for f in (*self.required, *self.optional))


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

    def _base_fields(self) -> dict[str, Any]:
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
        fields = context._base_fields()
        fields["total_ms"] = report.total_duration_secs * 1000
        fields["processor_count"] = len(report.processor_timings)
        _safe_emit(sink, "pipeline_startup", fields)

    async def on_transport_timing_report(_observer: Any, report: Any) -> None:
        fields = context._base_fields()
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
        fields = context._base_fields()
        fields["latency_ms"] = latency_seconds * 1000
        _safe_emit(sink, "first_bot_speech_latency", fields)

    async def on_latency_measured(_observer: Any, latency_seconds: float) -> None:
        fields = context._base_fields()
        fields["latency_ms"] = latency_seconds * 1000
        _safe_emit(sink, "user_bot_latency", fields)

    async def on_latency_breakdown(_observer: Any, breakdown: Any) -> None:
        for ttfb in breakdown.ttfb:
            fields = context._base_fields()
            fields["metric_kind"] = "ttfb"
            fields["value_ms"] = ttfb.duration_secs * 1000
            fields["processor"] = ttfb.processor
            _safe_emit(sink, "service_latency", fields)

        if breakdown.text_aggregation is not None:
            ta = breakdown.text_aggregation
            fields = context._base_fields()
            fields["metric_kind"] = "text_aggregation"
            fields["value_ms"] = ta.duration_secs * 1000
            fields["processor"] = ta.processor
            _safe_emit(sink, "service_latency", fields)

        for call in breakdown.function_calls:
            fields = context._base_fields()
            fields["metric_kind"] = "function_calls"
            fields["value_ms"] = call.duration_secs * 1000
            fields["function_name"] = call.function_name
            _safe_emit(sink, "service_latency", fields)

        if breakdown.user_turn_secs is not None:
            fields = context._base_fields()
            fields["metric_kind"] = "user_turn_secs"
            fields["value_ms"] = breakdown.user_turn_secs * 1000
            _safe_emit(sink, "service_latency", fields)

    return on_first_bot_speech_latency, on_latency_measured, on_latency_breakdown


def make_turn_tracking_handlers(
    context: PerfConnectionContext, sink: MeasurementSink
) -> tuple[Callable[[Any, int], Any], Callable[[Any, int, float, bool], Any]]:
    """Build ``on_turn_started``/``on_turn_ended`` handlers for the default turn tracker."""

    async def on_turn_started(_observer: Any, turn_count: int) -> None:
        fields = context._base_fields()
        fields["pipecat_turn"] = turn_count
        _safe_emit(sink, "pipecat_turn_start", fields)

    async def on_turn_ended(
        _observer: Any, turn_count: int, duration_secs: float, was_interrupted: bool
    ) -> None:
        fields = context._base_fields()
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
