"""Versioned Python contract invariants for the browser protocol."""

from datetime import datetime

import pytest

from server.contracts import (
    CONTRACT_VERSION,
    DeliveryState,
    GroundedResult,
    RoutingDecision,
    RoutingState,
    RuntimeSnapshot,
    SpeechProgress,
    TranscriptEntry,
    WorkerState,
    WorkItemState,
    validate_contract,
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
        text="Rain is likely.",
        citations=[{"title": "Forecast", "url": "https://weather.example/forecast"}],
        spoken_text="Rain is likely.",
        ui_text="Rain is likely.",
        origin_epoch=None,
    )

    assert result.spoken_text == result.ui_text == result.text
    assert result.spoken_result_id == result.ui_result_id == result.result_id
    assert result.spoken_citations == result.ui_citations == result.citations
    assert datetime.fromisoformat(result.timestamp).tzinfo is not None
    assert result.model_dump(mode="json")["timestamp"] == result.timestamp
    validate_contract(result)


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
