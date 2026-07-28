"""Contract tests for the PERF_METRIC console telemetry (Phase 1).

Covers docs/dev_plans/20260727-feature-latency-observability.md's Performance
Event Registry, Measurement Sink Contract, and Timing Boundaries sections, plus
the exact locked Pipecat 1.6.0 observer facts the plan requires verifying
before implementation ("Verify the selected observer imports...").

Two kinds of coverage live in this file:

- Pipecat-only "verified starting facts" tests that pin the exact installed
  pipecat-ai==1.6.0 observer import paths, constructor signatures, callback
  event names and payload units, the default turn tracker, ``enable_metrics``,
  and Small WebRTC's SFU-only ``bot_connected_secs`` field. These fail loudly
  if the locked artifact disagrees with the plan, per the plan's own gating
  rule.
- ``server.perf_metrics`` formatter/safety/sink/observer-adapter contract
  tests against the real module (``build_record``, ``format_record``,
  ``PerfMetricError``, ``ConsoleMeasurementSink``, ``CollectingMeasurementSink``,
  ``PerfConnectionContext``, and the ``make_*_handlers``/
  ``attach_framework_observers`` callback factories).
"""

from __future__ import annotations

import asyncio
import dataclasses
import math
import shlex
from typing import Any, get_args

import pytest
from loguru import logger as loguru_logger
from pipecat.observers.startup_timing_observer import (
    ProcessorStartupTiming,
    StartupTimingObserver,
    StartupTimingReport,
    TransportTimingReport,
)
from pipecat.observers.turn_tracking_observer import TurnTrackingObserver
from pipecat.observers.user_bot_latency_observer import (
    FunctionCallMetrics,
    LatencyBreakdown,
    TextAggregationBreakdownMetrics,
    TTFBBreakdownMetrics,
    UserBotLatencyObserver,
)
from pipecat.pipeline.worker import PipelineParams, PipelineWorker

from server.perf_metrics import (
    APP_TURN_OUTCOMES,
    COMMIT_OUTCOMES,
    CONTROL_ACTIONS,
    CONTROL_OUTCOMES,
    EVENT_REGISTRY,
    METRIC_KINDS,
    SPEECH_OUTCOMES,
    WORK_ITEM_OUTCOMES,
    WORK_OUTCOMES,
    AppTurnOutcome,
    AppTurnRecorder,
    CollectingMeasurementSink,
    CommitOutcome,
    ConsoleMeasurementSink,
    ControlAction,
    ControlOutcome,
    MetricKind,
    PerfConnectionContext,
    PerfMetricError,
    RetainedRecorder,
    SpeechOutcome,
    WorkItemOutcome,
    WorkOutcome,
    attach_framework_observers,
    build_record,
    make_startup_timing_handlers,
    make_turn_tracking_handlers,
    make_user_bot_latency_handlers,
)

# The alias is the single source of truth; the frozenset is derived from it.
# Each pair is asserted identical so a member added to only one side fails
# loudly instead of silently splitting static and runtime vocabularies.
_ALIAS_TO_FROZENSET = (
    (MetricKind, METRIC_KINDS),
    (AppTurnOutcome, APP_TURN_OUTCOMES),
    (ControlAction, CONTROL_ACTIONS),
    (ControlOutcome, CONTROL_OUTCOMES),
    (WorkItemOutcome, WORK_ITEM_OUTCOMES),
    (WorkOutcome, WORK_OUTCOMES),
    (CommitOutcome, COMMIT_OUTCOMES),
    (SpeechOutcome, SPEECH_OUTCOMES),
)

# Every ``enum`` field in the closed registry, mapped to the vocabulary it is
# contractually supposed to carry. Catches a copy-paste that wires the wrong
# frozenset onto a field.
_ENUM_FIELD_VOCABULARY = {
    ("service_latency", "metric_kind"): METRIC_KINDS,
    ("app_turn_foreground", "outcome"): APP_TURN_OUTCOMES,
    ("app_turn_foreground", "control_action"): CONTROL_ACTIONS,
    ("app_turn_foreground", "control_outcome"): CONTROL_OUTCOMES,
    ("work_item_foreground", "outcome"): WORK_ITEM_OUTCOMES,
    ("work_item_background", "work_outcome"): WORK_OUTCOMES,
    ("work_item_background", "commit_outcome"): COMMIT_OUTCOMES,
    ("work_item_background", "speech_outcome"): SPEECH_OUTCOMES,
}


@pytest.mark.parametrize(("alias", "vocabulary"), _ALIAS_TO_FROZENSET)
def test_each_frozenset_matches_its_literal_alias(alias: Any, vocabulary: frozenset[str]) -> None:
    assert frozenset(get_args(alias)) == vocabulary


def test_every_registry_enum_field_is_bound_to_its_intended_vocabulary() -> None:
    bound = {
        (event, spec_field.name): spec_field.enum_values
        for event, spec in EVENT_REGISTRY.items()
        for spec_field in (*spec.required, *spec.optional)
        if spec_field.kind == "enum"
    }
    assert bound == _ENUM_FIELD_VOCABULARY


# ---------------------------------------------------------------------------
# Verified Starting Facts: exact locked Pipecat 1.6.0 observer contract.
# These tests gate every subsequent observer-wiring test per the plan's
# Phase 1 sequencing.
# ---------------------------------------------------------------------------


def test_turn_tracking_observer_reports_duration_in_seconds_not_ms() -> None:
    """on_turn_ended payload is (turn_number, duration_secs, was_interrupted)."""
    observer = TurnTrackingObserver()
    assert "on_turn_started" in observer._event_handlers
    assert "on_turn_ended" in observer._event_handlers
    # duration is computed as (timestamp_delta_ns) / 1_000_000_000, i.e. seconds.
    assert observer._turn_end_timeout_secs == pytest.approx(2.5)


def test_user_bot_latency_observer_events_and_units() -> None:
    """UserBotLatencyObserver exposes the three events the plan relies on."""
    observer = UserBotLatencyObserver()
    for event_name in (
        "on_latency_measured",
        "on_latency_breakdown",
        "on_first_bot_speech_latency",
    ):
        assert event_name in observer._event_handlers

    # LatencyBreakdown carries duration_secs (not ms) fields, matching the
    # plan's "every Pipecat 1.6.0 observer payload ... is expressed in
    # seconds" claim.
    breakdown = LatencyBreakdown(
        ttfb=[TTFBBreakdownMetrics(processor="tts-1", start_time=0.0, duration_secs=0.42)],
        text_aggregation=TextAggregationBreakdownMetrics(
            processor="llm-1", start_time=0.0, duration_secs=0.1
        ),
        user_turn_start_time=0.0,
        user_turn_secs=1.5,
        function_calls=[
            FunctionCallMetrics(function_name="lookup", start_time=0.0, duration_secs=0.2)
        ],
    )
    assert breakdown.ttfb[0].duration_secs == pytest.approx(0.42)
    assert breakdown.text_aggregation is not None
    assert breakdown.text_aggregation.duration_secs == pytest.approx(0.1)
    assert breakdown.user_turn_secs == pytest.approx(1.5)
    assert breakdown.function_calls[0].duration_secs == pytest.approx(0.2)

    # metric_kind field-set facts the registry depends on: ttfb/text_aggregation
    # carry `processor`; function_calls carries `function_name` instead;
    # user_turn_secs is a bare scalar with neither.
    assert hasattr(breakdown.ttfb[0], "processor")
    assert hasattr(breakdown.text_aggregation, "processor")
    assert hasattr(breakdown.function_calls[0], "function_name")
    assert not hasattr(breakdown.function_calls[0], "processor")


def test_startup_timing_observer_reports_seconds_and_processor_count() -> None:
    """StartupTimingReport/TransportTimingReport carry *_secs floats."""
    observer = StartupTimingObserver()
    for event_name in ("on_startup_timing_report", "on_transport_timing_report"):
        assert event_name in observer._event_handlers

    report = StartupTimingReport(
        start_time=0.0,
        total_duration_secs=0.284,
        processor_timings=[
            ProcessorStartupTiming(processor_name="stt", start_offset_secs=0.0, duration_secs=0.284)
        ],
    )
    assert report.total_duration_secs == pytest.approx(0.284)
    assert len(report.processor_timings) == 1

    transport_report = TransportTimingReport(start_time=0.0, client_connected_secs=0.05)
    assert transport_report.bot_connected_secs is None
    assert transport_report.client_connected_secs == pytest.approx(0.05)


def test_small_webrtc_transport_never_emits_bot_connected_frame() -> None:
    """Small WebRTC is not an SFU transport: it never pushes BotConnectedFrame.

    StartupTimingObserver only sets bot_connected_secs from BotConnectedFrame,
    which only Daily/LiveKit/Tavus/HeyGen transports push (verified against
    the locked pipecat-ai==1.6.0 source). ``transport_ready`` must therefore
    omit ``bot_connected_ms`` under this app's Small WebRTC transport rather
    than zero-fill it.
    """
    import pipecat.transports.smallwebrtc.transport as smallwebrtc_transport_module

    assert "BotConnectedFrame" not in dir(smallwebrtc_transport_module)


def _minimal_pipeline():
    from pipecat.pipeline.pipeline import Pipeline
    from pipecat.processors.frame_processor import FrameProcessor

    class _NoOp(FrameProcessor):
        pass

    return Pipeline([_NoOp()])


def test_pipeline_worker_enables_default_turn_tracker_by_default() -> None:
    """PipelineWorker(enable_turn_tracking=True) builds exactly one internal tracker."""
    worker = PipelineWorker(pipeline=_minimal_pipeline(), enable_rtvi=False)
    assert isinstance(worker.turn_tracking_observer, TurnTrackingObserver)


def test_pipeline_params_supports_enable_metrics_flag() -> None:
    params = PipelineParams(enable_metrics=True)
    assert params.enable_metrics is True
    assert PipelineParams().enable_metrics is False


# ---------------------------------------------------------------------------
# server.perf_metrics: formatter, safety, and sink contract.
# ---------------------------------------------------------------------------


class TestPerfConnectionContext:
    def test_is_immutable(self) -> None:
        ctx = PerfConnectionContext(
            session_id="session-1", origin_epoch=1, connection_worker="browser-1"
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            ctx.session_id = "session-2"  # type: ignore[misc]

    def test_carries_only_the_three_documented_fields(self) -> None:
        ctx = PerfConnectionContext(
            session_id="session-1", origin_epoch=1, connection_worker="browser-1"
        )
        field_names = {f.name for f in dataclasses.fields(ctx)}
        assert field_names == {"session_id", "origin_epoch", "connection_worker"}


class TestConsoleMeasurementSink:
    def test_writes_exactly_one_raw_line_beginning_with_prefix(self) -> None:
        captured: list[str] = []
        handler_id = loguru_logger.add(captured.append, level="TRACE")
        try:
            record = build_record(
                "pipeline_startup",
                session_id="session-1",
                origin_epoch=1,
                connection_worker="browser-1",
                total_ms=284.1,
            )
            ConsoleMeasurementSink().emit(record)
        finally:
            loguru_logger.remove(handler_id)

        assert len(captured) == 1
        line = captured[0]
        assert line.startswith("PERF_METRIC event=pipeline_startup")
        assert line.count("\n") == 1
        assert line.endswith("\n")


class TestCollectingMeasurementSink:
    def test_indexes_records_by_event_turn_and_work_item(self) -> None:
        sink = CollectingMeasurementSink()
        record = build_record(
            "work_item_foreground",
            session_id="session-1",
            origin_epoch=1,
            turn_id="turn-7",
            work_item_id="work-turn-7",
            outcome="completed",
            total_ms=1084.2,
        )
        sink.emit(record)

        assert sink.records == (record,)
        assert sink.get("work_item_foreground", turn_id="turn-7", work_item_id="work-turn-7") == (
            record,
        )
        assert sink.get("work_item_foreground", turn_id="turn-8", work_item_id="work-turn-7") == ()

    def test_accumulates_rather_than_overwriting_a_latest_value(self) -> None:
        """Two emits accumulate; the sink must not act as a last-write cache."""
        sink = CollectingMeasurementSink()
        for turn_id in ("turn-1", "turn-2"):
            sink.emit(
                build_record(
                    "app_turn_foreground",
                    session_id="session-1",
                    origin_epoch=1,
                    turn_id=turn_id,
                    outcome="direct",
                    total_ms=10.0,
                    child_count=0,
                    direct_count=0,
                    unsupported_count=0,
                    completed_count=0,
                    retained_count=0,
                    clarification_count=0,
                    declined_count=0,
                    failed_count=0,
                    cancelled_count=0,
                )
            )
        assert len(sink.records) == 2
        assert {record.fields["turn_id"] for record in sink.records} == {"turn-1", "turn-2"}


class TestFormatterFieldSafety:
    """Formatter/safety tests for the PERF_METRIC line contract."""

    def test_unknown_event_rejected(self) -> None:
        with pytest.raises(PerfMetricError):
            build_record("not_a_registered_event", session_id="s")

    def test_unknown_field_rejected(self) -> None:
        with pytest.raises(PerfMetricError):
            build_record(
                "pipeline_startup",
                session_id="session-1",
                origin_epoch=1,
                connection_worker="browser-1",
                total_ms=1.0,
                transcript="not allowed",
            )

    def test_missing_required_field_rejected(self) -> None:
        with pytest.raises(PerfMetricError):
            build_record(
                "pipeline_startup",
                session_id="session-1",
                origin_epoch=1,
                # connection_worker and total_ms omitted
            )

    def test_optional_field_may_be_omitted_without_zero_fill(self) -> None:
        record = build_record(
            "transport_ready",
            session_id="session-1",
            origin_epoch=1,
            connection_worker="browser-1",
            client_connected_ms=52.0,
            # bot_connected_ms omitted: Small WebRTC never supplies it.
        )
        assert "bot_connected_ms" not in record.fields
        assert "bot_connected_ms" not in record.line

    @pytest.mark.parametrize(
        "raw_ms,expected_rendering", [(284.12, "284.1"), (1000.0, "1000.0"), (0.0, "0.0")]
    )
    def test_ms_formatting_rounds_to_one_decimal(
        self, raw_ms: float, expected_rendering: str
    ) -> None:
        record = build_record(
            "pipeline_startup",
            session_id="session-1",
            origin_epoch=1,
            connection_worker="browser-1",
            total_ms=raw_ms,
        )
        assert f"total_ms={expected_rendering}" in record.line

    def test_negative_duration_rejected(self) -> None:
        with pytest.raises(PerfMetricError):
            build_record(
                "pipeline_startup",
                session_id="session-1",
                origin_epoch=1,
                connection_worker="browser-1",
                total_ms=-1.0,
            )

    def test_zero_duration_accepted(self) -> None:
        record = build_record(
            "pipeline_startup",
            session_id="session-1",
            origin_epoch=1,
            connection_worker="browser-1",
            total_ms=0.0,
        )
        assert "total_ms=0.0" in record.line

    @pytest.mark.parametrize("bad_value", [math.nan, math.inf, -math.inf])
    def test_non_finite_duration_rejected(self, bad_value: float) -> None:
        with pytest.raises(PerfMetricError):
            build_record(
                "pipeline_startup",
                session_id="session-1",
                origin_epoch=1,
                connection_worker="browser-1",
                total_ms=bad_value,
            )

    def test_bool_rejected_for_integer_field(self) -> None:
        with pytest.raises(PerfMetricError):
            build_record(
                "pipecat_turn_start",
                session_id="session-1",
                origin_epoch=1,
                connection_worker="browser-1",
                pipecat_turn=True,
            )

    def test_bool_rejected_for_ms_field(self) -> None:
        with pytest.raises(PerfMetricError):
            build_record(
                "pipeline_startup",
                session_id="session-1",
                origin_epoch=1,
                connection_worker="browser-1",
                total_ms=True,
            )

    @pytest.mark.parametrize("bad_value", [1.5, "1", -1])
    def test_fractional_string_negative_integer_fields_rejected(self, bad_value: object) -> None:
        with pytest.raises(PerfMetricError):
            build_record(
                "pipecat_turn_start",
                session_id="session-1",
                origin_epoch=1,
                connection_worker="browser-1",
                pipecat_turn=bad_value,
            )

    def test_pipecat_turn_below_one_rejected(self) -> None:
        with pytest.raises(PerfMetricError):
            build_record(
                "pipecat_turn_start",
                session_id="session-1",
                origin_epoch=1,
                connection_worker="browser-1",
                pipecat_turn=0,
            )

    def test_origin_epoch_below_zero_rejected(self) -> None:
        with pytest.raises(PerfMetricError):
            build_record(
                "pipeline_startup",
                session_id="session-1",
                origin_epoch=-1,
                connection_worker="browser-1",
                total_ms=1.0,
            )

    def test_invalid_enum_value_rejected(self) -> None:
        with pytest.raises(PerfMetricError):
            build_record(
                "app_turn_foreground",
                session_id="session-1",
                origin_epoch=1,
                turn_id="turn-1",
                outcome="not_a_real_outcome",
                total_ms=1.0,
                child_count=0,
                direct_count=0,
                unsupported_count=0,
                completed_count=0,
                retained_count=0,
                clarification_count=0,
                declined_count=0,
                failed_count=0,
                cancelled_count=0,
            )

    def test_empty_identifier_rejected(self) -> None:
        with pytest.raises(PerfMetricError):
            build_record(
                "pipeline_startup",
                session_id="",
                origin_epoch=1,
                connection_worker="browser-1",
                total_ms=1.0,
            )

    @pytest.mark.parametrize(
        "bad_id", ["has\rcarriage", "has\nnewline", "has\x00control", "has\ttab"]
    )
    def test_control_characters_rejected_in_identifiers(self, bad_id: str) -> None:
        """CR/LF and other control characters are rejected outright, not merely escaped.

        The plan's Testing Notes require "rejection of ... CR/LF, control
        characters" as part of identifier validation, distinct from the
        allowed JSON-string escaping of spaces/quotes/`=`.
        """
        with pytest.raises(PerfMetricError):
            build_record(
                "pipeline_startup",
                session_id=bad_id,
                origin_epoch=1,
                connection_worker="browser-1",
                total_ms=1.0,
            )

    def test_overlong_identifier_rejected(self) -> None:
        with pytest.raises(PerfMetricError):
            build_record(
                "pipeline_startup",
                session_id="s" * 129,
                origin_epoch=1,
                connection_worker="browser-1",
                total_ms=1.0,
            )

    def test_identifier_at_maxlen_accepted(self) -> None:
        record = build_record(
            "pipeline_startup",
            session_id="s" * 128,
            origin_epoch=1,
            connection_worker="browser-1",
            total_ms=1.0,
        )
        assert "session_id=" in record.line

    def test_overlong_function_name_rejected(self) -> None:
        """function_name can originate from an LLM tool call, so an
        adversarial/hallucinated over-length value must not reach the log line."""
        with pytest.raises(PerfMetricError):
            build_record(
                "service_latency",
                session_id="session-1",
                origin_epoch=1,
                connection_worker="browser-1",
                metric_kind="function_calls",
                value_ms=1.0,
                function_name="f" * 129,
            )

    def test_identifiers_with_spaces_and_equals_do_not_split_fields(self) -> None:
        """Identifiers are JSON-string encoded so `=` and spaces cannot inject fields."""
        record = build_record(
            "pipeline_startup",
            session_id="session with spaces and key=value",
            origin_epoch=1,
            connection_worker="browser-1",
            total_ms=1.0,
        )
        assert record.line.count("\n") == 0
        # Tokenize respecting quotes, exactly like an operator's `rg`/parser
        # would: the injected `key=value` text must stay inside the quoted
        # session_id value, never surface as its own top-level `key` field.
        tokens = shlex.split(record.line)
        assert tokens[0] == "PERF_METRIC"
        parsed_fields = dict(token.split("=", 1) for token in tokens[1:])
        assert set(parsed_fields) == {
            "event",
            "schema",
            "session_id",
            "origin_epoch",
            "connection_worker",
            "total_ms",
        }
        assert parsed_fields["session_id"] == "session with spaces and key=value"

    def test_app_turn_foreground_child_counter_sum_must_match_child_count(self) -> None:
        with pytest.raises(PerfMetricError):
            build_record(
                "app_turn_foreground",
                session_id="session-1",
                origin_epoch=1,
                turn_id="turn-1",
                outcome="completed",
                total_ms=1.0,
                child_count=2,
                direct_count=0,
                unsupported_count=0,
                completed_count=1,  # sums to 1, not 2
                retained_count=0,
                clarification_count=0,
                declined_count=0,
                failed_count=0,
                cancelled_count=0,
            )

    def test_app_turn_foreground_zero_child_count_rejects_nonzero_counter(self) -> None:
        """The child_count/counter sum check is unconditional: a zero-child
        turn must carry every counter at zero."""
        with pytest.raises(PerfMetricError):
            build_record(
                "app_turn_foreground",
                session_id="session-1",
                origin_epoch=1,
                turn_id="turn-1",
                outcome="direct",
                total_ms=1.0,
                child_count=0,
                direct_count=1,
                unsupported_count=0,
                completed_count=0,
                retained_count=0,
                clarification_count=0,
                declined_count=0,
                failed_count=0,
                cancelled_count=0,
            )

    def test_app_turn_foreground_accepts_matching_child_counter_sum(self) -> None:
        record = build_record(
            "app_turn_foreground",
            session_id="session-1",
            origin_epoch=1,
            turn_id="turn-1",
            outcome="mixed",
            total_ms=1.0,
            child_count=2,
            direct_count=1,
            unsupported_count=0,
            completed_count=1,
            retained_count=0,
            clarification_count=0,
            declined_count=0,
            failed_count=0,
            cancelled_count=0,
        )
        assert record.fields["child_count"] == 2

    def test_required_fields_render_before_optional_fields_in_registry_order(self) -> None:
        """app_turn_foreground renders routing_ms before commit_ms when both present."""
        record = build_record(
            "app_turn_foreground",
            session_id="session-1",
            origin_epoch=1,
            turn_id="turn-1",
            outcome="completed",
            total_ms=100.0,
            child_count=1,
            direct_count=0,
            unsupported_count=0,
            completed_count=1,
            retained_count=0,
            clarification_count=0,
            declined_count=0,
            failed_count=0,
            cancelled_count=0,
            commit_ms=5.0,
            routing_ms=10.0,
        )
        assert record.line.index("routing_ms=") < record.line.index("commit_ms=")
        assert record.line.index("total_ms=") < record.line.index("child_count=")

    def test_nested_value_rejected(self) -> None:
        with pytest.raises(PerfMetricError):
            build_record(
                "pipeline_startup",
                session_id="session-1",
                origin_epoch=1,
                connection_worker="browser-1",
                total_ms=1.0,
                processor_count={"nested": 1},
            )

    def test_service_latency_ttfb_requires_processor(self) -> None:
        with pytest.raises(PerfMetricError):
            build_record(
                "service_latency",
                session_id="session-1",
                origin_epoch=1,
                connection_worker="browser-1",
                metric_kind="ttfb",
                value_ms=1.0,
                # processor omitted
            )

    def test_service_latency_function_calls_uses_function_name_not_processor(self) -> None:
        record = build_record(
            "service_latency",
            session_id="session-1",
            origin_epoch=1,
            connection_worker="browser-1",
            metric_kind="function_calls",
            value_ms=1.0,
            function_name="lookup_weather",
        )
        assert "function_name=" in record.line
        assert "processor=" not in record.line

    def test_service_latency_user_turn_secs_is_a_bare_scalar(self) -> None:
        record = build_record(
            "service_latency",
            session_id="session-1",
            origin_epoch=1,
            connection_worker="browser-1",
            metric_kind="user_turn_secs",
            value_ms=1500.0,
        )
        assert "processor=" not in record.line
        assert "function_name=" not in record.line

    def test_content_sentinels_never_appear_in_line_when_rejected(self) -> None:
        """An attempted injection through an unknown field is rejected outright."""
        sentinel = "SENTINEL-TRANSCRIPT-DO-NOT-LEAK"
        with pytest.raises(PerfMetricError):
            build_record(
                "pipeline_startup",
                session_id="session-1",
                origin_epoch=1,
                connection_worker="browser-1",
                total_ms=1.0,
                transcript=sentinel,
            )

    def test_registry_is_closed_to_exactly_the_documented_events(self) -> None:
        assert set(EVENT_REGISTRY) == {
            "pipeline_startup",
            "transport_ready",
            "pipecat_turn_start",
            "pipecat_turn_end",
            "first_bot_speech_latency",
            "user_bot_latency",
            "service_latency",
            "app_turn_foreground",
            "work_item_foreground",
            "work_item_background",
        }


class TestObserverCallbackFactories:
    """Duration_secs * 1000 conversion and connection-context capture rules."""

    def test_turn_ended_converts_duration_secs_to_ms_not_a_raw_pass_through(self) -> None:
        sink = CollectingMeasurementSink()
        ctx = PerfConnectionContext(
            session_id="session-1", origin_epoch=1, connection_worker="browser-1"
        )
        _on_started, on_turn_ended = make_turn_tracking_handlers(ctx, sink)
        duration_secs = 5.6203

        asyncio.run(on_turn_ended(object(), 3, duration_secs, False))

        record = sink.records[0]
        assert record.event == "pipecat_turn_end"
        assert record.fields["duration_ms"] == pytest.approx(duration_secs * 1000, abs=0.05)
        assert record.fields["duration_ms"] != pytest.approx(duration_secs, abs=0.05)

    def test_startup_report_total_ms_converts_from_total_duration_secs(self) -> None:
        sink = CollectingMeasurementSink()
        ctx = PerfConnectionContext(
            session_id="session-1", origin_epoch=1, connection_worker="browser-1"
        )
        on_startup, _on_transport = make_startup_timing_handlers(ctx, sink)
        report = StartupTimingReport(
            start_time=0.0,
            total_duration_secs=0.2841,
            processor_timings=[
                ProcessorStartupTiming(
                    processor_name="stt", start_offset_secs=0.0, duration_secs=0.2841
                )
            ],
        )

        asyncio.run(on_startup(object(), report))

        record = sink.records[0]
        assert record.fields["total_ms"] == pytest.approx(284.1, abs=0.05)
        assert record.fields["processor_count"] == 1

    def test_transport_ready_omits_bot_connected_ms_when_absent(self) -> None:
        sink = CollectingMeasurementSink()
        ctx = PerfConnectionContext(
            session_id="session-1", origin_epoch=1, connection_worker="browser-1"
        )
        _on_startup, on_transport = make_startup_timing_handlers(ctx, sink)
        report = TransportTimingReport(start_time=0.0, client_connected_secs=0.052)

        asyncio.run(on_transport(object(), report))

        record = sink.records[0]
        assert record.event == "transport_ready"
        assert "bot_connected_ms" not in record.fields
        assert record.fields["client_connected_ms"] == pytest.approx(52.0, abs=0.05)

    def test_transport_ready_includes_bot_connected_ms_when_supplied(self) -> None:
        sink = CollectingMeasurementSink()
        ctx = PerfConnectionContext(
            session_id="session-1", origin_epoch=1, connection_worker="browser-1"
        )
        _on_startup, on_transport = make_startup_timing_handlers(ctx, sink)
        report = TransportTimingReport(
            start_time=0.0, client_connected_secs=0.052, bot_connected_secs=0.030
        )

        asyncio.run(on_transport(object(), report))

        record = sink.records[0]
        assert record.fields["bot_connected_ms"] == pytest.approx(30.0, abs=0.05)

    def test_latency_breakdown_emits_one_service_latency_record_per_datum(self) -> None:
        sink = CollectingMeasurementSink()
        ctx = PerfConnectionContext(
            session_id="session-1", origin_epoch=1, connection_worker="browser-1"
        )
        _first, _measured, on_breakdown = make_user_bot_latency_handlers(ctx, sink)
        breakdown = LatencyBreakdown(
            ttfb=[TTFBBreakdownMetrics(processor="tts-1", start_time=0.0, duration_secs=0.4)],
            text_aggregation=TextAggregationBreakdownMetrics(
                processor="llm-1", start_time=0.0, duration_secs=0.1
            ),
            user_turn_start_time=0.0,
            user_turn_secs=1.5,
            function_calls=[
                FunctionCallMetrics(function_name="lookup", start_time=0.0, duration_secs=0.2)
            ],
        )

        asyncio.run(on_breakdown(object(), breakdown))

        events = [r.event for r in sink.records]
        assert events == ["service_latency"] * 4
        kinds = {r.fields["metric_kind"] for r in sink.records}
        assert kinds == {"ttfb", "text_aggregation", "function_calls", "user_turn_secs"}
        ttfb_record = next(r for r in sink.records if r.fields["metric_kind"] == "ttfb")
        assert ttfb_record.fields["value_ms"] == pytest.approx(400.0, abs=0.05)

    def test_partial_breakdown_omits_absent_metrics_without_zero_filling(self) -> None:
        """No ttfb, no text aggregation, no function calls: only the user-turn datum."""
        sink = CollectingMeasurementSink()
        ctx = PerfConnectionContext(
            session_id="session-1", origin_epoch=1, connection_worker="browser-1"
        )
        _first, _measured, on_breakdown = make_user_bot_latency_handlers(ctx, sink)
        breakdown = LatencyBreakdown(
            ttfb=[],
            text_aggregation=None,
            user_turn_start_time=None,
            user_turn_secs=None,
            function_calls=[],
        )

        asyncio.run(on_breakdown(object(), breakdown))

        assert sink.records == ()

    def test_handlers_capture_only_context_sink_and_use_no_extra_closures(self) -> None:
        """Callback closures capture only PerfConnectionContext + sink, per plan."""
        sink = CollectingMeasurementSink()
        ctx = PerfConnectionContext(
            session_id="session-1", origin_epoch=1, connection_worker="browser-1"
        )
        on_started, on_ended = make_turn_tracking_handlers(ctx, sink)
        for handler in (on_started, on_ended):
            cell_values = [cell.cell_contents for cell in (handler.__closure__ or ())]
            captured_types = {type(value) for value in cell_values}
            assert captured_types <= {PerfConnectionContext, type(sink)}


class TestAttachFrameworkObservers:
    """attach_framework_observers wires all three observer event names in one call."""

    def test_registers_all_six_handlers_on_the_given_observers(self) -> None:
        class RecordingObserver:
            def __init__(self) -> None:
                self.registered: list[str] = []

            def event_handler(self, name: str):
                def decorator(fn):
                    self.registered.append(name)
                    return fn

                return decorator

        startup_observer = RecordingObserver()
        latency_observer = RecordingObserver()
        turn_tracking_observer = RecordingObserver()
        ctx = PerfConnectionContext(
            session_id="session-1", origin_epoch=1, connection_worker="browser-1"
        )

        attach_framework_observers(
            startup_observer=startup_observer,
            latency_observer=latency_observer,
            turn_tracking_observer=turn_tracking_observer,
            context=ctx,
            sink=CollectingMeasurementSink(),
        )

        assert startup_observer.registered == [
            "on_startup_timing_report",
            "on_transport_timing_report",
        ]
        assert latency_observer.registered == [
            "on_first_bot_speech_latency",
            "on_latency_measured",
            "on_latency_breakdown",
        ]
        assert turn_tracking_observer.registered == ["on_turn_started", "on_turn_ended"]


class TestRetainedRecorderFinalizeDefaults:
    """RetainedRecorder.finalize() must never construct an incomplete record.

    ``work_outcome``, ``commit_outcome``, and ``speech_outcome`` are required
    per EVENT_REGISTRY's ``work_item_background`` spec but are typed optional
    on the recorder. finalize() must supply its own terminal defaults for any
    still-unset field so shutdown-time (and any other) callers don't have to
    hand-write fallback ``or`` expressions to avoid a silently swallowed
    formatter error.
    """

    @staticmethod
    def _recorder(sink: CollectingMeasurementSink) -> RetainedRecorder:
        return RetainedRecorder(
            sink,
            session_id="session-1",
            origin_epoch=1,
            turn_id="turn-1",
            work_item_id="work-1",
            app_worker_id="worker-1",
        )

    def test_finalize_with_no_arguments_produces_a_complete_record(self) -> None:
        sink = CollectingMeasurementSink()
        recorder = self._recorder(sink)

        assert recorder.finalize() is True

        record = sink.records[0]
        assert record.fields["work_outcome"] == "cancelled"
        assert record.fields["commit_outcome"] == "suppressed_shutdown"
        assert record.fields["speech_outcome"] == "cancelled"
        assert EVENT_REGISTRY["work_item_background"].by_name  # sanity: registry has the event

    def test_finalize_fills_only_the_fields_left_unset(self) -> None:
        sink = CollectingMeasurementSink()
        recorder = self._recorder(sink)
        recorder.claim("completed")
        recorder.record_commit("committed", result_id="result-1")

        assert recorder.finalize() is True

        record = sink.records[0]
        assert record.fields["work_outcome"] == "completed"
        assert record.fields["commit_outcome"] == "committed"
        assert record.fields["speech_outcome"] == "cancelled"
        assert record.fields["result_id"] == "result-1"

    def test_finalize_explicit_arguments_still_take_precedence_over_defaults(self) -> None:
        sink = CollectingMeasurementSink()
        recorder = self._recorder(sink)

        assert recorder.finalize(
            work_outcome="completed",
            commit_outcome="suppressed_stale",
            speech_outcome="not_applicable",
        )

        record = sink.records[0]
        assert record.fields["work_outcome"] == "completed"
        assert record.fields["commit_outcome"] == "suppressed_stale"
        assert record.fields["speech_outcome"] == "not_applicable"


class TestAppTurnRecorderFinalizeTotality:
    """``AppTurnRecorder.finalize`` must emit exactly one schema-valid record
    for every state/argument combination.

    The recorder latches ``_finalized`` before deriving anything, so any path
    that returned without emitting would leave the turn permanently
    unrecorded while still reporting ``finalized is True``.
    """

    @staticmethod
    def _recorder(sink: CollectingMeasurementSink) -> AppTurnRecorder:
        return AppTurnRecorder(
            sink,
            session_id="session-1",
            origin_epoch=1,
            turn_id="turn-1",
        )

    @staticmethod
    def _warnings(records: list[object]) -> list[object]:
        return [record for record in records if record["level"].name == "WARNING"]

    def test_bare_finalize_with_no_children_emits_one_failed_record(self) -> None:
        sink = CollectingMeasurementSink()
        recorder = self._recorder(sink)
        captured: list[object] = []
        handler_id = loguru_logger.add(lambda message: captured.append(message.record))
        try:
            recorder.finalize()
        finally:
            loguru_logger.remove(handler_id)

        assert recorder.finalized is True
        assert len(sink.records) == 1
        fields = sink.records[0].fields
        assert fields["outcome"] == "failed"
        assert fields["child_count"] == 0
        assert all(
            fields[name] == 0
            for name in (
                "direct_count",
                "unsupported_count",
                "completed_count",
                "retained_count",
                "clarification_count",
                "declined_count",
                "failed_count",
                "cancelled_count",
            )
        )
        assert self._warnings(captured)

    def test_control_outcome_without_control_action_degrades_to_failed(self) -> None:
        sink = CollectingMeasurementSink()
        recorder = self._recorder(sink)
        captured: list[object] = []
        handler_id = loguru_logger.add(lambda message: captured.append(message.record))
        try:
            recorder.finalize(
                outcome="control", control_action=None, control_outcome="unknown_target"
            )
        finally:
            loguru_logger.remove(handler_id)

        assert len(sink.records) == 1
        fields = sink.records[0].fields
        assert fields["outcome"] == "failed"
        assert "control_action" not in fields
        assert "control_outcome" not in fields
        assert self._warnings(captured)

    def test_control_action_without_control_outcome_degrades_to_failed(self) -> None:
        sink = CollectingMeasurementSink()
        recorder = self._recorder(sink)

        recorder.finalize(outcome="control", control_action="pause", control_outcome=None)

        assert len(sink.records) == 1
        fields = sink.records[0].fields
        assert fields["outcome"] == "failed"
        assert "control_action" not in fields
        assert "control_outcome" not in fields

    def test_complete_control_pair_is_preserved(self) -> None:
        sink = CollectingMeasurementSink()
        recorder = self._recorder(sink)

        recorder.finalize(outcome="control", control_action="pause", control_outcome="applied")

        fields = sink.records[0].fields
        assert fields["outcome"] == "control"
        assert fields["control_action"] == "pause"
        assert fields["control_outcome"] == "applied"

    def test_second_finalize_never_emits_a_second_record(self) -> None:
        sink = CollectingMeasurementSink()
        recorder = self._recorder(sink)

        recorder.finalize()
        recorder.finalize(outcome="completed")

        assert len(sink.records) == 1
        assert sink.records[0].fields["outcome"] == "failed"


class TestAppTurnRecorderChildOwnership:
    """Every child created under a turn gets exactly one terminal record.

    The parent owns its children, so a turn cancelled (or raising) between
    child construction and the child's own finalize cannot orphan it.
    """

    @staticmethod
    def _recorder(sink: CollectingMeasurementSink) -> AppTurnRecorder:
        return AppTurnRecorder(
            sink,
            session_id="session-1",
            origin_epoch=1,
            turn_id="turn-1",
        )

    @staticmethod
    def _children(sink: CollectingMeasurementSink) -> list[object]:
        return [record for record in sink.records if record.event == "work_item_foreground"]

    @staticmethod
    def _parent(sink: CollectingMeasurementSink) -> object:
        parents = [record for record in sink.records if record.event == "app_turn_foreground"]
        assert len(parents) == 1
        return parents[0]

    def test_cancelled_turn_sweeps_only_the_still_open_child(self) -> None:
        sink = CollectingMeasurementSink()
        recorder = self._recorder(sink)
        done = recorder.new_child(work_item_id="work-1")
        recorder.new_child(work_item_id="work-2")
        done.finalize(outcome="completed")

        recorder.finalize(outcome="cancelled")

        children = self._children(sink)
        assert len(children) == 2
        assert {child.fields["work_item_id"]: child.fields["outcome"] for child in children} == {
            "work-1": "completed",
            "work-2": "cancelled",
        }
        parent = self._parent(sink).fields
        assert parent["outcome"] == "cancelled"
        assert parent["child_count"] == 2
        assert parent["completed_count"] == 1
        assert parent["cancelled_count"] == 1

    def test_sweep_runs_before_outcome_derivation(self) -> None:
        """A bare finalize whose only child is swept must derive its outcome
        from the swept counter, not from the no-children fallback."""
        sink = CollectingMeasurementSink()
        recorder = self._recorder(sink)
        recorder.new_child(work_item_id="work-1")

        recorder.finalize()

        children = self._children(sink)
        assert len(children) == 1
        assert children[0].fields["outcome"] == "failed"
        parent = self._parent(sink).fields
        assert parent["outcome"] == "failed"
        assert parent["child_count"] == 1
        assert parent["failed_count"] == 1

    def test_already_finalized_child_is_not_swept_again(self) -> None:
        sink = CollectingMeasurementSink()
        recorder = self._recorder(sink)
        child = recorder.new_child(work_item_id="work-1")
        child.finalize(outcome="completed")
        child.finalize(outcome="failed")

        recorder.finalize()

        children = self._children(sink)
        assert len(children) == 1
        assert children[0].fields["outcome"] == "completed"
        parent = self._parent(sink).fields
        assert parent["outcome"] == "completed"
        assert parent["child_count"] == 1
        assert parent["completed_count"] == 1

    def test_child_inherits_the_parents_identity(self) -> None:
        sink = CollectingMeasurementSink()
        recorder = self._recorder(sink)

        recorder.new_child(work_item_id="work-1").finalize(outcome="completed")

        fields = self._children(sink)[0].fields
        assert fields["session_id"] == "session-1"
        assert fields["origin_epoch"] == 1
        assert fields["turn_id"] == "turn-1"
        assert fields["work_item_id"] == "work-1"
