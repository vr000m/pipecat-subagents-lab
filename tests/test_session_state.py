"""Session state is authoritative and retains immutable result history."""

from server.contracts import CONTRACT_VERSION, DeliveryState, GroundedResult, WorkerState
from server.session_state import SessionState


def result(result_id: str, worker_id: str = "worker-weather") -> GroundedResult:
    return GroundedResult(
        result_id=result_id,
        worker_id=worker_id,
        turn_id=f"turn-{result_id}",
        text=f"Answer {result_id}",
        spoken_text=f"Answer {result_id}",
        ui_text=f"Answer {result_id}",
    )


def test_result_history_is_append_only_and_snapshot_retains_all_results() -> None:
    state = SessionState(session_id="session-1")
    state.set_worker(
        WorkerState(worker_id="worker-weather", topic="weather", model_policy="deep", status="idle")
    )
    state.append_result(result("result-1"))
    state.append_result(result("result-2"))

    snapshot = state.snapshot()
    assert [item.result_id for item in snapshot.results] == ["result-1", "result-2"]
    assert snapshot.workers[0].latest_result_id == "result-2"
    assert snapshot.contract_version == CONTRACT_VERSION


def test_speech_progress_is_monotonic_and_synthesis_end_is_not_delivery_completion() -> None:
    state = SessionState(session_id="session-1")
    kwargs = dict(
        result_id="result-1",
        work_item_id="work-1",
        run_id="run-1",
        utterance_id="utt-1",
        origin_epoch=2,
    )
    for state_value in (
        DeliveryState.DISPLAYED,
        DeliveryState.QUEUED,
        DeliveryState.STARTED,
        DeliveryState.SYNTHESIS_ENDED,
        DeliveryState.DELIVERY_COMPLETED,
        DeliveryState.STARTED,
    ):
        state.speech_progress(state=state_value, **kwargs)

    assert state.speech["utt-1"].state == DeliveryState.DELIVERY_COMPLETED
    assert [item.state for item in state.speech_history("utt-1")] == [
        DeliveryState.DISPLAYED,
        DeliveryState.QUEUED,
        DeliveryState.STARTED,
        DeliveryState.SYNTHESIS_ENDED,
        DeliveryState.DELIVERY_COMPLETED,
    ]


def test_terminal_interruption_wins_over_late_completion_and_duplicate_progress() -> None:
    state = SessionState(session_id="session-1")
    kwargs = dict(
        result_id="result-1",
        work_item_id="work-1",
        run_id="run-1",
        utterance_id="utt-1",
    )

    state.speech_progress(state=DeliveryState.STARTED, **kwargs)
    state.speech_progress(state=DeliveryState.INTERRUPTED, **kwargs)
    state.speech_progress(state=DeliveryState.DELIVERY_COMPLETED, **kwargs)
    state.speech_progress(state=DeliveryState.INTERRUPTED, **kwargs)

    assert state.speech["utt-1"].state == DeliveryState.INTERRUPTED
    assert [item.state for item in state.speech_history("utt-1")] == [
        DeliveryState.STARTED,
        DeliveryState.INTERRUPTED,
    ]


def test_old_epoch_result_can_commit_but_cannot_schedule_speech() -> None:
    state = SessionState(session_id="session-1")
    state.append_result(result("late"))
    state.speech_progress(
        result_id="late",
        work_item_id="work-1",
        run_id="run-1",
        utterance_id="utt-1",
        state=DeliveryState.QUEUED,
        origin_epoch=2,
    )

    assert state.results.results[0].result_id == "late"
    assert state.speech["utt-1"].state == DeliveryState.QUEUED


def test_duplicate_late_old_epoch_result_commit_is_idempotent() -> None:
    state = SessionState(session_id="session-1")
    late = result("late")

    state.append_result(late, origin_epoch=1)
    duplicate = state.append_result(late, origin_epoch=1)

    assert duplicate.kind == "result_duplicate"
    assert [item.result_id for item in state.results.results] == ["late"]


def test_stale_epoch_speech_callback_cannot_mutate_active_authoritative_state() -> None:
    state = SessionState(session_id="session-1")
    state.active_epoch = 3
    progress = state.speech_progress(
        result_id="result-1",
        work_item_id="work-1",
        run_id="run-1",
        utterance_id="utt-1",
        state=DeliveryState.STARTED,
        origin_epoch=3,
    )

    stale = state.apply_speech_progress(
        progress.model_copy(update={"state": DeliveryState.DELIVERY_COMPLETED}),
        origin_epoch=2,
    )

    assert stale.state == DeliveryState.STARTED
    assert state.speech["utt-1"].state == DeliveryState.STARTED


def test_promoted_state_rejects_epochless_callbacks_but_keeps_pre_activation_setup() -> None:
    state = SessionState(session_id="session-1")
    state.set_worker(
        WorkerState(worker_id="worker-weather", topic="weather", model_policy="deep", status="idle")
    )
    state.append_result(result("setup"))
    state.active_epoch = 3

    state.set_worker(
        WorkerState(
            worker_id="worker-weather", topic="weather", model_policy="deep", status="working"
        )
    )
    state.append_result(result("epochless"))
    state.speech_progress(
        result_id="epochless",
        work_item_id="work-1",
        run_id="run-1",
        utterance_id="utt-1",
        state=DeliveryState.STARTED,
    )

    assert state.workers["worker-weather"].status == "idle"
    assert [item.result_id for item in state.results.results] == ["setup"]
    assert "utt-1" not in state.speech


def test_result_callback_epoch_must_match_result_origin_and_old_result_stays_history_only() -> None:
    state = SessionState(session_id="session-1")
    state.active_epoch = 3
    state.set_worker(
        WorkerState(
            worker_id="worker-weather",
            topic="weather",
            model_policy="deep",
            status="idle",
            origin_epoch=3,
        )
    )
    mismatched = result("mismatched").model_copy(update={"origin_epoch": 2})
    state.append_result(mismatched, origin_epoch=3)
    old = result("old").model_copy(update={"origin_epoch": 2})
    state.append_result(old, origin_epoch=2)

    assert [item.result_id for item in state.results.results] == ["old"]
    assert state.workers["worker-weather"].latest_result_id is None


def test_reconnected_session_keeps_old_epoch_result_history_only() -> None:
    state = SessionState(session_id="session-1")
    state.set_worker(
        WorkerState(
            worker_id="worker-weather",
            topic="weather",
            model_policy="deep",
            status="idle",
            origin_epoch=1,
        )
    )
    state.active_epoch = 1
    current = result("current").model_copy(update={"origin_epoch": 1})
    state.append_result(current, origin_epoch=1)
    state.speech_progress(
        result_id="current",
        work_item_id="work-current",
        run_id="run-current",
        utterance_id="utt-current",
        state=DeliveryState.STARTED,
        origin_epoch=1,
    )

    state.active_epoch = 2
    old = result("old").model_copy(update={"origin_epoch": 1})
    state.append_result(old, origin_epoch=1)
    state.apply_speech_progress(
        state.speech["utt-current"].model_copy(
            update={"state": DeliveryState.DELIVERY_COMPLETED, "origin_epoch": 1}
        ),
        origin_epoch=1,
    )

    assert [item.result_id for item in state.results.results] == ["current", "old"]
    assert state.workers["worker-weather"].latest_result_id == "current"
    assert state.speech["utt-current"].state == DeliveryState.STARTED


def test_snapshot_round_trip_rebuilds_worker_result_history_and_delivery_projection() -> None:
    state = SessionState(session_id="session-1")
    state.set_worker(
        WorkerState(worker_id="worker-weather", topic="weather", model_policy="deep", status="idle")
    )
    state.append_result(result("result-1"))
    state.append_result(result("result-2"))
    state.speech_progress(
        result_id="result-2",
        work_item_id="work-2",
        run_id="run-2",
        utterance_id="utt-2",
        state=DeliveryState.INTERRUPTED_BY_RECONNECT,
        origin_epoch=1,
    )

    restored = SessionState.from_snapshot(state.snapshot(origin_epoch=2))

    assert [item.result_id for item in restored.result_history("worker-weather")] == [
        "result-1",
        "result-2",
    ]
    assert restored.workers["worker-weather"].latest_result_id == "result-2"
    assert restored.speech["utt-2"].state == DeliveryState.INTERRUPTED_BY_RECONNECT
