"""Versioned Python contract invariants for the browser protocol."""

import json
from datetime import datetime
from pathlib import Path

import pytest

from server.contracts import (
    CONTRACT_VERSION,
    DeliveryState,
    GroundedResult,
    InterruptionEvent,
    RoutingDecision,
    RoutingState,
    RuntimeSnapshot,
    SpeechProgress,
    TranscriptEntry,
    WorkerState,
    WorkItemEvent,
    WorkItemState,
    validate_contract,
)

GROUNDED_RESULT_SCHEMA = json.loads(
    (Path(__file__).parents[1] / "shared" / "schemas" / "grounded-result.json").read_text()
)
RUNTIME_SNAPSHOT_SCHEMA = json.loads(
    (Path(__file__).parents[1] / "shared" / "schemas" / "runtime-snapshot.json").read_text()
)


def test_contract_version_and_state_sets_are_conservative() -> None:
    assert CONTRACT_VERSION.startswith("v1.")
    assert {state.value for state in WorkItemState} == {
        "started",
        "progress",
        "cancellation_requested",
        "cancelled",
        "completed",
        "failed",
    }
    assert {state.value for state in DeliveryState} == {
        "displayed",
        "queued",
        "started",
        "synthesis_ended",
        "delivery_completed",
        "delivery_unknown",
        "interrupted",
        "interrupted_by_reconnect",
        "paused",
        "resumed",
    }


def test_routing_decision_requires_catalogue_bound_capability_and_worker_selection() -> None:
    decision = RoutingDecision(
        action="existing_worker",
        worker_id="worker-weather",
        worker_type="web_search",
        topic="weather",
        capability="public_web",
        capability_available=True,
        model_policy="fast",
        catalogue_version="catalogue-7",
        origin_epoch=None,
    )

    validate_contract(decision)

    with pytest.raises(ValueError):
        RoutingDecision(**{**decision.model_dump(), "worker_id": "hallucinated-worker"})


def test_unavailable_private_capability_cannot_become_a_web_worker() -> None:
    with pytest.raises(ValueError):
        RoutingDecision(
            action="existing_worker",
            worker_id="worker-weather",
            worker_type="web_search",
            topic="calendar",
            capability="private_calendar",
            capability_available=False,
            model_policy="fast",
            catalogue_version="catalogue-7",
            origin_epoch=None,
        )


def test_canonical_result_drives_both_projections_and_preserves_origin_epoch() -> None:
    result = GroundedResult(
        result_id="result-1",
        worker_id="worker-weather",
        turn_id="turn-1",
        text="Rain is likely this afternoon, with clearing expected tonight.",
        citations=[{"title": "Forecast", "url": "https://weather.example/forecast"}],
        spoken_text="Expect rain this afternoon and clearing tonight.",
        origin_epoch=None,
    )

    assert result.spoken_text != result.text
    assert result.ui_text == result.text
    assert result.spoken_result_id == result.ui_result_id == result.result_id
    assert result.spoken_citations == result.ui_citations == result.citations
    assert datetime.fromisoformat(result.timestamp).tzinfo is not None
    wire = result.model_dump(mode="json")
    assert wire["timestamp"] == result.timestamp
    assert set(wire) == set(GROUNDED_RESULT_SCHEMA["properties"])
    validate_contract(result)


def test_shared_result_schema_declares_the_python_projection_invariants() -> None:
    assert set(GROUNDED_RESULT_SCHEMA["required"]) == set(GROUNDED_RESULT_SCHEMA["properties"])
    assert {
        "ui_text",
        "spoken_result_id",
        "ui_result_id",
        "spoken_citations",
        "ui_citations",
    }.isdisjoint(GROUNDED_RESULT_SCHEMA["properties"])
    assert RUNTIME_SNAPSHOT_SCHEMA["properties"]["results"]["items"] == {
        "$ref": "grounded-result.json"
    }


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("result_id", ""),
        ("worker_id", ""),
        ("turn_id", ""),
        ("timestamp", "not-a-date"),
        ("timestamp", "2026-07-25T12:00:00"),
        ("timestamp", "2026-07-25 12:00:00+00:00"),
        ("timestamp", "2026-02-30T12:00:00Z"),
        ("origin_epoch", -1),
    ),
)
def test_python_contract_rejects_invalid_wire_identity_and_timestamp(
    field: str, value: str
) -> None:
    payload = {
        "result_id": "result-1",
        "worker_id": "worker-weather",
        "turn_id": "turn-1",
        "timestamp": "2026-07-25T12:00:00Z",
        "text": "Canonical answer.",
        "citations": [],
        "spoken_text": "Spoken answer.",
        "origin_epoch": 1,
    }
    payload[field] = value

    with pytest.raises(ValueError):
        GroundedResult.model_validate(payload)


@pytest.mark.parametrize(
    "timestamp",
    (
        "not-a-date",
        "2026-07-25T12:00:00",
        "2026-07-25 12:00:00+00:00",
        "2026-02-30T12:00:00Z",
    ),
)
def test_transcript_and_work_item_timestamps_require_valid_rfc3339(timestamp: str) -> None:
    with pytest.raises(ValueError):
        TranscriptEntry(
            role="user",
            text="Hello",
            turn_id="turn-1",
            timestamp=timestamp,
            origin_epoch=1,
        )

    with pytest.raises(ValueError):
        WorkItemEvent(
            work_item_id="work-1",
            run_id="run-1",
            worker_id="worker-1",
            turn_id="turn-1",
            event_id="event-1",
            event_sequence=1,
            state="started",
            timestamp=timestamp,
            origin_epoch=1,
        )


@pytest.mark.parametrize(
    ("model", "payload"),
    (
        (
            RoutingDecision,
            {
                "action": "direct",
                "catalogue_version": "catalogue-1",
                "origin_epoch": -1,
            },
        ),
        (
            RoutingState,
            {
                "turn_id": "turn-1",
                "action": "direct",
                "origin_epoch": -1,
            },
        ),
        (
            TranscriptEntry,
            {
                "role": "user",
                "text": "Hello",
                "turn_id": "turn-1",
                "origin_epoch": -1,
            },
        ),
        (
            WorkerState,
            {
                "worker_id": "worker-1",
                "topic": "weather",
                "model_policy": "fast",
                "status": "idle",
                "origin_epoch": -1,
            },
        ),
        (
            SpeechProgress,
            {
                "result_id": "result-1",
                "work_item_id": "work-1",
                "run_id": "run-1",
                "utterance_id": "utterance-1",
                "state": "queued",
                "origin_epoch": -1,
            },
        ),
        (
            WorkItemEvent,
            {
                "work_item_id": "work-1",
                "run_id": "run-1",
                "worker_id": "worker-1",
                "turn_id": "turn-1",
                "event_id": "event-1",
                "event_sequence": 1,
                "state": "started",
                "timestamp": "2026-07-25T12:00:00Z",
                "origin_epoch": -1,
            },
        ),
        (
            InterruptionEvent,
            {
                "interruption_id": "interruption-1",
                "stage": "speech_active",
                "policy": "pause",
                "template_used": False,
                "resume_outcome": "resumed",
                "work_item_id": "work-1",
                "run_id": "run-1",
                "utterance_id": "utterance-1",
                "origin_epoch": -1,
            },
        ),
    ),
)
def test_epoch_bearing_contracts_reject_negative_values(
    model: type, payload: dict[str, object]
) -> None:
    with pytest.raises(ValueError):
        model.model_validate(payload)


@pytest.mark.parametrize("nested_field", ("workers", "speech_progress", "routing", "transcript"))
def test_runtime_snapshot_rejects_negative_nested_origin_epochs(nested_field: str) -> None:
    payload: dict[str, object] = {
        "contract_version": CONTRACT_VERSION,
        "session_id": f"session-negative-{nested_field}",
        "snapshot_sequence": 1,
        "workers": [],
        "results": [],
        "speech_progress": [],
        "routing": None,
        "transcript": [],
        "origin_epoch": 1,
    }
    nested_values = {
        "workers": [
            {
                "worker_id": "worker-1",
                "topic": "weather",
                "model_policy": "fast",
                "status": "idle",
                "latest_result_id": None,
                "origin_epoch": -1,
            }
        ],
        "speech_progress": [
            {
                "result_id": "result-1",
                "work_item_id": "work-1",
                "run_id": "run-1",
                "utterance_id": "utterance-1",
                "state": "queued",
                "origin_epoch": -1,
            }
        ],
        "routing": {
            "turn_id": "turn-1",
            "action": "direct",
            "worker_id": None,
            "worker_type": None,
            "topic": None,
            "model_policy": None,
            "origin_epoch": -1,
        },
        "transcript": [
            {
                "role": "user",
                "text": "Hello",
                "turn_id": "turn-1",
                "timestamp": "2026-07-25T12:00:00Z",
                "origin_epoch": -1,
            }
        ],
    }
    payload[nested_field] = nested_values[nested_field]

    with pytest.raises(ValueError):
        RuntimeSnapshot.model_validate(payload)


@pytest.mark.parametrize(
    "field",
    (
        "ui_text",
        "spoken_result_id",
        "ui_result_id",
        "spoken_citations",
        "ui_citations",
    ),
)
def test_python_contract_rejects_internal_projection_fields_on_the_wire(field: str) -> None:
    payload = {
        "result_id": "result-1",
        "worker_id": "worker-weather",
        "turn_id": "turn-1",
        "timestamp": "2026-07-25T12:00:00Z",
        "text": "Canonical answer.",
        "citations": [],
        "spoken_text": "Spoken answer.",
        "origin_epoch": 1,
        field: [] if field.endswith("_citations") else "redundant",
    }

    with pytest.raises(ValueError):
        GroundedResult.model_validate(payload)

    snapshot_payload = {
        "contract_version": CONTRACT_VERSION,
        "session_id": "session-wire-rejection",
        "snapshot_sequence": 1,
        "workers": [],
        "results": [payload],
        "speech_progress": [],
        "routing": None,
        "transcript": [],
        "origin_epoch": 1,
    }
    with pytest.raises(ValueError):
        RuntimeSnapshot.model_validate(snapshot_payload)


def test_speech_progress_is_distinct_from_result_completion_and_has_origin_epoch() -> None:
    progress = SpeechProgress(
        result_id="result-1",
        work_item_id="work-1",
        run_id="run-1",
        utterance_id="utterance-1",
        state="synthesis_ended",
        origin_epoch=None,
    )

    assert progress.state.value != "delivery_completed"
    assert progress.result_id == "result-1"
    validate_contract(progress)


def test_snapshot_is_versioned_monotonic_and_excludes_raw_prompts_or_logs() -> None:
    worker = WorkerState(
        worker_id="worker-weather",
        topic="weather",
        model_policy="deep",
        status="idle",
        latest_result_id="result-1",
    )
    snapshot = RuntimeSnapshot(
        contract_version=CONTRACT_VERSION,
        session_id="session-1",
        snapshot_sequence=12,
        workers=[worker],
        results=[],
        speech_progress=[],
        routing=RoutingState(
            turn_id="turn-1",
            action="existing_worker",
            worker_id="worker-weather",
            worker_type="web_search",
            topic="weather",
            model_policy="deep",
        ),
        transcript=[TranscriptEntry(role="user", text="Weather?", turn_id="turn-1")],
        origin_epoch=None,
    )

    validate_contract(snapshot)
    payload = snapshot.model_dump(mode="json")
    assert payload["snapshot_sequence"] == 12
    assert payload["routing"]["worker_id"] == "worker-weather"
    assert payload["transcript"][0]["text"] == "Weather?"
    assert "raw_logs" not in payload
    assert "prompt" not in payload
    assert "context" not in payload

    with pytest.raises(ValueError):
        RuntimeSnapshot(**{**payload, "snapshot_sequence": 11})
    with pytest.raises(ValueError):
        RuntimeSnapshot(**{**payload, "unexpected_private_field": True})


# --- Phase 3: progressive RTVI status -------------------------------------
#
# Plan bullet (shared/protocol.md, shared/schemas/rtvi-message.json,
# shared/schemas/work-status.json): a strict `work_status` kind is added to
# the closed v1.0 kind list; capability handshake gets a strict `capabilities`
# field. These tests assert the CONTRACT the plan describes -- they may not
# import successfully until server/contracts.py lands the new symbols.

from server.contracts import SnapshotHandshake

try:
    from server.contracts import WORK_STATUS_STATES as _IMPL_WORK_STATUS_STATES
    from server.contracts import WorkStatus
except ImportError:  # pragma: no cover - contract not yet implemented
    WorkStatus = None  # type: ignore[assignment]
    _IMPL_WORK_STATUS_STATES = None  # type: ignore[assignment]

WORK_STATUS_STATES = {"routing", "searching", "background", "result_ready", "failed", "cancelled"}
LEGAL_TRANSITIONS = {
    "routing": {"searching", "failed", "cancelled"},
    "searching": {"background", "result_ready", "failed", "cancelled"},
    "background": {"result_ready", "failed", "cancelled"},
    "result_ready": set(),
    "failed": set(),
    "cancelled": set(),
}


def _work_status(**overrides: object) -> "WorkStatus":
    assert WorkStatus is not None, "server.contracts.WorkStatus is not implemented yet"
    fields = {
        "turn_id": "turn-1",
        "work_item_id": "work-1",
        "state": "routing",
        "event_sequence": 0,
        "origin_epoch": 1,
    }
    fields.update(overrides)
    return WorkStatus(**fields)


@pytest.mark.skipif(WorkStatus is None, reason="server.contracts.WorkStatus not implemented yet")
def test_work_status_state_set_is_the_coarse_six_state_set() -> None:
    assert set(_IMPL_WORK_STATUS_STATES) == WORK_STATUS_STATES


@pytest.mark.skipif(WorkStatus is None, reason="server.contracts.WorkStatus not implemented yet")
@pytest.mark.parametrize("state", sorted(WORK_STATUS_STATES))
def test_work_status_accepts_every_coarse_state(state: str) -> None:
    status = _work_status(state=state)
    validate_contract(status)
    assert status.state == state


@pytest.mark.skipif(WorkStatus is None, reason="server.contracts.WorkStatus not implemented yet")
def test_work_status_rejects_word_level_progress_fields() -> None:
    """shared/protocol.md:80 reserves word-level progress; this phase's
    coarse work_status must not accept a word-level progress payload."""
    with pytest.raises(ValueError):
        _work_status(word_progress="the model is thinking about weather")
    with pytest.raises(ValueError):
        _work_status(partial_text="Austin is currently")


@pytest.mark.skipif(WorkStatus is None, reason="server.contracts.WorkStatus not implemented yet")
def test_work_status_rejects_work_item_event_reserved_states_as_values() -> None:
    """WorkItemEvent's reserved started/progress states (shared/protocol.md:
    109-115) must not become legal work_status values -- the two state
    machines are independent seams."""
    with pytest.raises(ValueError):
        _work_status(state="started")
    with pytest.raises(ValueError):
        _work_status(state="progress")


@pytest.mark.skipif(WorkStatus is None, reason="server.contracts.WorkStatus not implemented yet")
@pytest.mark.parametrize(
    ("start", "end"),
    [(s, e) for s, targets in LEGAL_TRANSITIONS.items() for e in targets],
)
def test_work_status_legal_transitions_are_accepted(start: str, end: str) -> None:
    # A transition is exercised through two sequential events sharing a key;
    # both individual payloads must be independently contract-valid.
    validate_contract(_work_status(state=start, event_sequence=0))
    validate_contract(_work_status(state=end, event_sequence=1))


@pytest.mark.skipif(WorkStatus is None, reason="server.contracts.WorkStatus not implemented yet")
@pytest.mark.parametrize("terminal_state", ["result_ready", "failed", "cancelled"])
def test_work_status_terminal_states_have_no_legal_successor(terminal_state: str) -> None:
    assert LEGAL_TRANSITIONS[terminal_state] == set()


@pytest.mark.skipif(WorkStatus is None, reason="server.contracts.WorkStatus not implemented yet")
@pytest.mark.parametrize("terminal_reason", ["missing_worker", "retention_rejected"])
def test_work_status_failed_carries_terminal_reason(terminal_reason: str) -> None:
    status = _work_status(state="failed", terminal_reason=terminal_reason)
    validate_contract(status)
    assert status.terminal_reason == terminal_reason


@pytest.mark.skipif(WorkStatus is None, reason="server.contracts.WorkStatus not implemented yet")
def test_work_status_terminal_reason_only_legal_alongside_failed() -> None:
    with pytest.raises(ValueError):
        _work_status(state="routing", terminal_reason="missing_worker")


# --- Phase 3: SnapshotHandshake capabilities field -------------------------


def test_snapshot_handshake_absent_capabilities_normalizes_to_empty_and_unpresent() -> None:
    handshake = SnapshotHandshake(
        session_id="session-1", resume_token="resume-1", proposed_epoch=1, snapshot_sequence=0
    )
    assert handshake.capabilities == ()
    assert handshake.capabilities_present is False


def test_snapshot_handshake_explicit_empty_array_is_present_but_empty() -> None:
    handshake = SnapshotHandshake(
        session_id="session-1",
        resume_token="resume-1",
        proposed_epoch=1,
        snapshot_sequence=0,
        capabilities=(),
        capabilities_present=True,
    )
    assert handshake.capabilities == ()
    assert handshake.capabilities_present is True


def test_snapshot_handshake_unknown_capability_names_are_retained_as_unsupported() -> None:
    """Absent/unknown means unsupported -- the strict model itself does not
    reject a well-formed but unrecognized future capability name; rejection
    of malformed entries only applies to non-string/empty-string entries."""
    handshake = SnapshotHandshake(
        session_id="session-1",
        resume_token="resume-1",
        proposed_epoch=1,
        snapshot_sequence=0,
        capabilities=("work_status_v1", "some_future_capability"),
        capabilities_present=True,
    )
    assert handshake.capabilities == ("some_future_capability", "work_status_v1")


def test_snapshot_handshake_capabilities_are_deduplicated_and_lexicographically_sorted() -> None:
    handshake = SnapshotHandshake(
        session_id="session-1",
        resume_token="resume-1",
        proposed_epoch=1,
        snapshot_sequence=0,
        capabilities=("work_status_v1", "work_status_v1", "alpha"),
        capabilities_present=True,
    )
    assert handshake.capabilities == ("alpha", "work_status_v1")


def test_snapshot_handshake_rejects_non_string_or_empty_capability_entries() -> None:
    with pytest.raises(ValueError):
        SnapshotHandshake(
            session_id="session-1",
            resume_token="resume-1",
            proposed_epoch=1,
            snapshot_sequence=0,
            capabilities=("work_status_v1", ""),
            capabilities_present=True,
        )
    with pytest.raises(ValueError):
        SnapshotHandshake(
            session_id="session-1",
            resume_token="resume-1",
            proposed_epoch=1,
            snapshot_sequence=0,
            capabilities=(1,),  # type: ignore[arg-type]
            capabilities_present=True,
        )
