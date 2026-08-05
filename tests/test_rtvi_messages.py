"""RTVI state messages expose ordered runtime projections, not raw diagnostics."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from server.contracts import (
    DeliveryState,
    GroundedResult,
    RoutingState,
    RuntimeSnapshot,
    SpeechProgress,
    TranscriptEntry,
    WorkerState,
)
from server.rtvi_messages import RTVI_MESSAGE_KINDS, RTVIMessage, RTVIMessagePublisher

try:
    from server.contracts import WorkStatus
except ImportError:  # pragma: no cover - contract not yet implemented
    WorkStatus = None  # type: ignore[assignment]


def test_messages_have_monotonic_sequences_and_reject_stale_epoch_emission() -> None:
    publisher = RTVIMessagePublisher(session_id="session-1", active_epoch=4)
    result = GroundedResult(
        result_id="result-1",
        worker_id="worker-weather",
        turn_id="turn-1",
        text="Rain is likely.",
        spoken_text="Rain is likely.",
        origin_epoch=4,
    )
    first = publisher.incremental(
        "result", result.model_dump(mode="json"), sequence=1, origin_epoch=4
    )
    second = publisher.incremental(
        "speech_progress",
        SpeechProgress(
            result_id="result-1",
            work_item_id="work-1",
            run_id="run-1",
            utterance_id="utt-1",
            state=DeliveryState.STARTED,
            origin_epoch=4,
        ).model_dump(mode="json"),
        sequence=2,
        origin_epoch=4,
    )

    assert second.sequence == first.sequence + 1
    assert (
        publisher.incremental("result", result.model_dump(mode="json"), sequence=3, origin_epoch=3)
        is None
    )
    payload = first.model_dump(mode="json")
    assert "raw_logs" not in payload
    assert "prompt" not in payload
    assert "context" not in payload


def test_snapshot_is_gated_until_client_ready() -> None:
    publisher = RTVIMessagePublisher(session_id="session-1", active_epoch=1)
    assert publisher.snapshot() is None
    publisher.client_ready(epoch=1)
    snapshot = publisher.snapshot()
    assert snapshot is not None
    assert snapshot.kind == "runtime_snapshot"
    assert snapshot.sequence >= 0


def test_first_ready_snapshot_is_schema_complete_before_authoritative_state_is_set() -> None:
    publisher = RTVIMessagePublisher(session_id="session-empty", active_epoch=1)
    publisher.client_ready(epoch=1)

    snapshot = publisher.snapshot()

    assert snapshot is not None
    assert snapshot.data == {
        "contract_version": "v1.0",
        "session_id": "session-empty",
        "snapshot_sequence": 0,
        "workers": [],
        "results": [],
        "speech_progress": [],
        "routing": None,
        "transcript": [],
        "work_status": [],
        "origin_epoch": 1,
    }
    RuntimeSnapshot.model_validate(snapshot.data)


def test_stale_epoch_cannot_advance_sequence_or_emit_after_readiness() -> None:
    publisher = RTVIMessagePublisher(session_id="session-1", active_epoch=2)
    publisher.client_ready(epoch=2)
    first = publisher.snapshot()

    assert first is not None
    assert (
        publisher.incremental(
            "speech_progress",
            SpeechProgress(
                result_id="result-1",
                work_item_id="work-1",
                run_id="run-1",
                utterance_id="utt-1",
                state=DeliveryState.STARTED,
                origin_epoch=1,
            ).model_dump(mode="json"),
            sequence=first.sequence + 1,
            origin_epoch=1,
        )
        is None
    )
    second = publisher.snapshot()

    assert second is not None
    assert second.sequence == first.sequence


def test_snapshot_contains_authoritative_history_and_delivery_state_after_reconnect() -> None:
    publisher = RTVIMessagePublisher(session_id="session-1", active_epoch=3)
    publisher.client_ready(epoch=3)
    publisher.set_snapshot(
        RuntimeSnapshot(
            contract_version="v1.0",
            session_id="session-1",
            snapshot_sequence=8,
            results=[
                GroundedResult(
                    result_id="result-1",
                    worker_id="worker-weather",
                    turn_id="turn-1",
                    text="Answer",
                    spoken_text="Answer",
                )
            ],
            speech_progress=[
                SpeechProgress(
                    result_id="result-1",
                    work_item_id="work-1",
                    run_id="run-1",
                    utterance_id="utt-1",
                    state=DeliveryState.INTERRUPTED_BY_RECONNECT,
                    origin_epoch=1,
                )
            ],
        )
    )

    snapshot = publisher.snapshot()
    assert snapshot is not None
    assert snapshot.data["snapshot_sequence"] == 8
    assert snapshot.data["results"][0]["result_id"] == "result-1"
    assert snapshot.data["speech_progress"][0]["state"] == "interrupted_by_reconnect"


def test_high_snapshot_sequence_is_shared_with_envelope_and_following_events() -> None:
    publisher = RTVIMessagePublisher(session_id="session-high", active_epoch=1)
    publisher.client_ready(epoch=1)
    publisher.set_snapshot(
        RuntimeSnapshot(
            contract_version="v1.0",
            session_id="session-high",
            snapshot_sequence=40,
        )
    )

    snapshot = publisher.snapshot()
    repeated_snapshot = publisher.snapshot()
    event = publisher.incremental(
        "speech_progress",
        SpeechProgress(
            result_id="result-1",
            work_item_id="work-1",
            run_id="run-1",
            utterance_id="utt-1",
            state=DeliveryState.STARTED,
            origin_epoch=1,
        ).model_dump(mode="json"),
        sequence=41,
        origin_epoch=1,
    )

    assert snapshot is not None and snapshot.sequence == 40
    assert snapshot.data["snapshot_sequence"] == snapshot.sequence
    assert repeated_snapshot is not None and repeated_snapshot.sequence == 40
    assert repeated_snapshot.data["snapshot_sequence"] == repeated_snapshot.sequence
    assert event is not None and event.sequence == 41


def test_repeated_zero_snapshot_requests_do_not_advance_authoritative_watermark() -> None:
    publisher = RTVIMessagePublisher(session_id="session-zero", active_epoch=1)
    publisher.client_ready(epoch=1)
    publisher.set_snapshot(
        RuntimeSnapshot(contract_version="v1.0", session_id="session-zero", snapshot_sequence=0)
    )

    first = publisher.snapshot()
    second = publisher.snapshot()

    assert first is not None and first.sequence == 0
    assert first.data["snapshot_sequence"] == 0
    assert second is not None and second.sequence == 0
    assert second.data["snapshot_sequence"] == 0


def test_python_message_kinds_match_the_shared_wire_schema() -> None:
    schema = json.loads(
        (Path(__file__).parents[1] / "shared" / "schemas" / "rtvi-message.json").read_text()
    )

    assert tuple(schema["properties"]["kind"]["enum"]) == RTVI_MESSAGE_KINDS
    assert "runtime_result" not in RTVI_MESSAGE_KINDS
    assert "speech" not in RTVI_MESSAGE_KINDS
    for branch in schema["allOf"]:
        payload = branch["then"]["properties"]["data"]
        overlays = payload.get("allOf", [])
        assert any(
            "origin_epoch" in overlay.get("required", [])
            and overlay.get("properties", {}).get("origin_epoch")
            == {"type": "integer", "minimum": 0}
            for overlay in overlays
        )


def test_every_versioned_message_kind_validates_its_direct_payload() -> None:
    epoch = 3
    session_id = "session-parity"
    RuntimeSnapshot.reset_monotonicity(session_id)
    result = GroundedResult(
        result_id="result-1",
        worker_id="worker-1",
        turn_id="turn-1",
        text="Answer",
        spoken_text="Answer",
        origin_epoch=epoch,
    )
    payloads = {
        "runtime_snapshot": RuntimeSnapshot(
            contract_version="v1.0",
            session_id=session_id,
            snapshot_sequence=7,
            origin_epoch=epoch,
        ),
        "result": result,
        "speech_progress": SpeechProgress(
            result_id="result-1",
            work_item_id="work-1",
            run_id="run-1",
            utterance_id="utterance-1",
            state=DeliveryState.STARTED,
            origin_epoch=epoch,
        ),
        "worker": WorkerState(
            worker_id="worker-1",
            topic="weather",
            model_policy="deep",
            status="idle",
            origin_epoch=epoch,
        ),
        "routing": RoutingState(
            turn_id="turn-1",
            action="new_worker",
            worker_type="web_search",
            topic="weather",
            model_policy="deep",
            origin_epoch=epoch,
        ),
        "user_transcript": TranscriptEntry(
            role="user", text="Question", turn_id="turn-1", origin_epoch=epoch
        ),
        "bot_transcript": TranscriptEntry(
            role="assistant", text="Answer", turn_id="turn-1", origin_epoch=epoch
        ),
    }
    if WorkStatus is not None:
        payloads["work_status"] = WorkStatus(
            turn_id="turn-1",
            work_item_id="work-1",
            state="routing",
            event_sequence=0,
            origin_epoch=epoch,
        )

    for kind in RTVI_MESSAGE_KINDS:
        if kind not in payloads:
            continue  # contract not yet implemented for this kind
        message = RTVIMessage(
            session_id=session_id,
            sequence=7 if kind == "runtime_snapshot" else 8,
            kind=kind,
            data=payloads[kind].model_dump(mode="json"),
            origin_epoch=epoch,
        )
        assert message.kind == kind


@pytest.mark.parametrize("kind", ["runtime_result", "speech"])
def test_undocumented_message_aliases_are_rejected(kind: str) -> None:
    with pytest.raises(ValidationError):
        RTVIMessage(
            session_id="session-1",
            sequence=1,
            kind=kind,
            data={},
            origin_epoch=1,
        )


def test_envelope_rejects_payload_kind_epoch_and_snapshot_fence_mismatches() -> None:
    worker = WorkerState(
        worker_id="worker-1",
        topic="weather",
        model_policy="deep",
        status="idle",
        origin_epoch=2,
    )
    with pytest.raises(ValidationError, match="result_id"):
        RTVIMessage(
            session_id="session-1",
            sequence=1,
            kind="result",
            data=worker.model_dump(mode="json"),
            origin_epoch=2,
        )
    with pytest.raises(ValidationError, match="origin_epoch"):
        RTVIMessage(
            session_id="session-1",
            sequence=1,
            kind="worker",
            data=worker.model_dump(mode="json"),
            origin_epoch=3,
        )
    with pytest.raises(ValidationError, match="snapshot_sequence"):
        RTVIMessage(
            session_id="session-1",
            sequence=2,
            kind="runtime_snapshot",
            data=RuntimeSnapshot(
                contract_version="v1.0",
                session_id="session-1",
                snapshot_sequence=1,
                origin_epoch=2,
            ).model_dump(mode="json"),
            origin_epoch=2,
        )


# --- Phase 3: work_status kind and the generic incremental(...) contract --


def test_rtvi_message_kinds_gains_work_status_in_the_closed_v1_0_list() -> None:
    assert "work_status" in RTVI_MESSAGE_KINDS


@pytest.mark.skipif(WorkStatus is None, reason="server.contracts.WorkStatus not implemented yet")
def test_incremental_validates_and_serializes_the_supplied_sequence_without_a_second_counter() -> (
    None
):
    """`incremental(...)` validates/serializes the supplied sequence rather
    than allocating a second one -- the observer/adapter assigns the single
    projected sequence, and the publisher is only its serializer."""
    publisher = RTVIMessagePublisher(session_id="session-1", active_epoch=1)
    status = WorkStatus(
        turn_id="turn-1", work_item_id="work-1", state="routing", event_sequence=0, origin_epoch=1
    )

    first = publisher.incremental(
        "work_status", status.model_dump(mode="json"), sequence=7, origin_epoch=1
    )
    second = publisher.incremental(
        "work_status", status.model_dump(mode="json"), sequence=8, origin_epoch=1
    )

    assert first.sequence == 7
    assert second.sequence == 8


@pytest.mark.skipif(WorkStatus is None, reason="server.contracts.WorkStatus not implemented yet")
def test_incremental_rejects_stale_epoch_like_the_other_publisher_methods() -> None:
    publisher = RTVIMessagePublisher(session_id="session-1", active_epoch=4)
    status = WorkStatus(
        turn_id="turn-1", work_item_id="work-1", state="routing", event_sequence=0, origin_epoch=3
    )

    assert (
        publisher.incremental(
            "work_status", status.model_dump(mode="json"), sequence=1, origin_epoch=3
        )
        is None
    )


def test_no_publisher_method_allocates_its_own_incremental_sequence_after_phase3() -> None:
    """Plan bullet 234: after Phase 3, no RTVIMessagePublisher method may
    allocate its own incremental sequence. `result`/`speech_progress` are
    caller-less; this migration either removes them or routes them through
    the supplied-sequence `incremental(...)` contract -- either disposition
    is acceptable, but a self-incrementing `self._watermark + 1` path with no
    sequence_provider must not remain reachable from either method."""
    publisher = RTVIMessagePublisher(session_id="session-1", active_epoch=1)
    result_method = getattr(publisher, "result", None)
    speech_progress_method = getattr(publisher, "speech_progress", None)
    if result_method is None and speech_progress_method is None:
        return  # removed entirely -- satisfies the invariant trivially
    grounded = GroundedResult(
        result_id="result-1",
        worker_id="worker-weather",
        turn_id="turn-1",
        text="Rain is likely.",
        spoken_text="Rain is likely.",
        origin_epoch=1,
    )
    before = publisher._watermark
    if result_method is not None:
        result_method(grounded, origin_epoch=1)
    after = publisher._watermark
    # A caller-less method that still self-increments without a
    # sequence_provider is exactly the forbidden disposition.
    assert after == before or publisher._sequence_provider is not None


# --- M10: _watermark is a watermark, not an allocator ---------------------


@pytest.mark.skipif(WorkStatus is None, reason="server.contracts.WorkStatus not implemented yet")
def test_incremental_never_advances_the_watermark_past_the_supplied_sequence() -> None:
    """M10: `incremental()` never allocates. It only clamps the watermark
    upward to the caller-supplied sequence; a lower sequence leaves the
    watermark untouched (and is still serialized as supplied)."""
    publisher = RTVIMessagePublisher(session_id="session-m10", active_epoch=1)
    status = WorkStatus(
        turn_id="turn-1", work_item_id="work-1", state="routing", event_sequence=0, origin_epoch=1
    )

    assert publisher._watermark == 0
    publisher.incremental("work_status", status.model_dump(mode="json"), sequence=7, origin_epoch=1)
    assert publisher._watermark == 7, "clamps upward to the supplied sequence"

    lower = publisher.incremental(
        "work_status", status.model_dump(mode="json"), sequence=3, origin_epoch=1
    )
    assert publisher._watermark == 7, "never advances past, and never rolls back below"
    assert lower is not None and lower.sequence == 3, "serializes the supplied sequence verbatim"


def test_snapshot_is_the_sole_allocator_and_reads_the_sequence_provider() -> None:
    """M10: `snapshot()` is the only allocation point; it takes its sequence
    from `_sequence_provider` when one is installed, overriding the
    watermark, and re-seeds the watermark from what it stamped."""
    provided = [42]
    publisher = RTVIMessagePublisher(
        session_id="session-m10", active_epoch=1, sequence_provider=lambda: provided[0]
    )
    publisher.client_ready(epoch=1)

    message = publisher.snapshot()

    assert message is not None
    assert message.sequence == 42
    assert message.data["snapshot_sequence"] == 42
    assert publisher._watermark == 42
