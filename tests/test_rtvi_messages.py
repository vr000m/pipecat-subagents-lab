"""RTVI state messages expose ordered runtime projections, not raw diagnostics."""

from server.contracts import DeliveryState, GroundedResult, RuntimeSnapshot, SpeechProgress
from server.rtvi_messages import RTVIMessagePublisher


def test_messages_have_monotonic_sequences_and_reject_stale_epoch_emission() -> None:
    publisher = RTVIMessagePublisher(session_id="session-1", active_epoch=4)
    result = GroundedResult(
        result_id="result-1",
        worker_id="worker-weather",
        turn_id="turn-1",
        text="Rain is likely.",
        spoken_text="Rain is likely.",
        ui_text="Rain is likely.",
    )
    first = publisher.result(result, origin_epoch=4)
    second = publisher.speech(
        SpeechProgress(
            result_id="result-1",
            work_item_id="work-1",
            run_id="run-1",
            utterance_id="utt-1",
            state=DeliveryState.STARTED,
            origin_epoch=4,
        ),
        origin_epoch=4,
    )

    assert second.sequence == first.sequence + 1
    assert publisher.result(result, origin_epoch=3) is None
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
        "origin_epoch": None,
    }
    RuntimeSnapshot.model_validate(snapshot.data)


def test_stale_epoch_cannot_advance_sequence_or_emit_after_readiness() -> None:
    publisher = RTVIMessagePublisher(session_id="session-1", active_epoch=2)
    publisher.client_ready(epoch=2)
    first = publisher.snapshot()

    assert first is not None
    assert (
        publisher.speech(
            SpeechProgress(
                result_id="result-1",
                work_item_id="work-1",
                run_id="run-1",
                utterance_id="utt-1",
                state=DeliveryState.STARTED,
                origin_epoch=1,
            ),
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
                    ui_text="Answer",
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
    event = publisher.speech(
        SpeechProgress(
            result_id="result-1",
            work_item_id="work-1",
            run_id="run-1",
            utterance_id="utt-1",
            state=DeliveryState.STARTED,
            origin_epoch=1,
        ),
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
