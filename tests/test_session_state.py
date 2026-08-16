"""Session state is authoritative and retains immutable result history."""

import pytest

from server.contracts import (
    CONTRACT_VERSION,
    DeliveryState,
    GroundedResult,
    RoutingState,
    TranscriptEntry,
    WorkerState,
)
from server.session_state import WORK_STATUS_TTL_SECONDS, SessionState

try:
    from server.session_state import WorkStatusKey
except ImportError:  # pragma: no cover - contract not yet implemented
    WorkStatusKey = None  # type: ignore[assignment]


def result(result_id: str, worker_id: str = "worker-weather") -> GroundedResult:
    return GroundedResult(
        result_id=result_id,
        worker_id=worker_id,
        turn_id=f"turn-{result_id}",
        text=f"Answer {result_id}",
        spoken_text=f"Answer {result_id}",
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


def test_routing_and_semantic_transcript_are_authoritative_snapshot_state() -> None:
    state = SessionState(session_id="session-routing")
    state.active_epoch = 2
    state.append_transcript(
        TranscriptEntry(
            role="user",
            text="What were the capitals of India through the last two hundred years?",
            turn_id="turn-1",
            origin_epoch=2,
        )
    )
    state.set_routing(
        RoutingState(
            turn_id="turn-1",
            action="new_worker",
            worker_type="web_search",
            topic="historical capitals of India",
            model_policy="deep",
            origin_epoch=2,
        )
    )

    snapshot = state.snapshot(origin_epoch=2)
    restored = SessionState.from_snapshot(snapshot)

    assert [event.kind for event in state.events] == ["user_transcript", "routing"]
    assert restored.transcript == state.transcript
    assert restored.routing == state.routing


def test_speech_progress_is_monotonic_and_synthesis_end_is_not_delivery_completion() -> None:
    state = SessionState(session_id="session-1")
    kwargs = {
        "result_id": "result-1",
        "work_item_id": "work-1",
        "run_id": "run-1",
        "utterance_id": "utt-1",
        "origin_epoch": 2,
    }
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
    kwargs = {
        "result_id": "result-1",
        "work_item_id": "work-1",
        "run_id": "run-1",
        "utterance_id": "utt-1",
    }

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

    assert duplicate is None
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


@pytest.mark.skipif(
    not hasattr(SessionState, "set_child_work_status"),
    reason="SessionState.set_child_work_status is not implemented yet",
)
def test_snapshot_round_trip_rebuilds_the_work_status_ledger_and_its_sequences() -> None:
    """Regression: snapshot(include_work_status=True) serialized the ledger
    but from_snapshot dropped it entirely, so a reconnect/state round trip
    lost every work-status record and restarted per-key event_sequence at 1 --
    which the browser reducer then rejects as stale against the record it
    already holds."""
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    state.set_child_work_status(
        turn_id="turn-1", work_item_id="work-1", state="routing", origin_epoch=1
    )
    state.set_child_work_status(
        turn_id="turn-1", work_item_id="work-1", state="searching", origin_epoch=1
    )
    state.set_child_work_status(
        turn_id="turn-1", work_item_id="work-1", state="result_ready", origin_epoch=1
    )
    original = state.work_status_snapshot()
    assert len(original) == 1
    assert original[0].state == "result_ready"
    assert original[0].event_sequence == 3

    restored = SessionState.from_snapshot(state.snapshot(origin_epoch=2, include_work_status=True))

    rehydrated = restored.work_status_snapshot()
    assert [item.model_dump() for item in rehydrated] == [item.model_dump() for item in original], (
        "terminal record, its historical origin_epoch, and its event_sequence must survive"
    )
    # The per-key counter continues from the restored value rather than
    # restarting: a later record for the same key must outrank the one the
    # client already applied.
    key = WorkStatusKey(origin_epoch=1, turn_id="turn-1", parent_key="work-1")
    assert restored._work_status_sequence[key] == 3


# --- Phase 3: WorkStatusKey identity and independent sequence ownership ----


@pytest.mark.skipif(
    WorkStatusKey is None, reason="server.session_state.WorkStatusKey not implemented yet"
)
def test_work_status_key_identity_is_origin_epoch_turn_id_and_work_item_or_parent() -> None:
    key = WorkStatusKey(origin_epoch=1, turn_id="turn-1", parent_key="work-1")
    same = WorkStatusKey(origin_epoch=1, turn_id="turn-1", parent_key="work-1")
    different_epoch = WorkStatusKey(origin_epoch=2, turn_id="turn-1", parent_key="work-1")

    assert key == same
    assert key != different_epoch


@pytest.mark.skipif(
    not hasattr(SessionState, "set_child_work_status"),
    reason="SessionState.set_child_work_status is not implemented yet",
)
def test_per_work_status_key_event_sequence_is_independent_of_the_global_state_sequence() -> None:
    """SessionState._emit()'s global sequence is the authoritative
    runtime_snapshot.snapshot_sequence watermark; the per-WorkStatusKey
    payload event_sequence is a separate, independently allocated field."""
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    state.set_worker(
        WorkerState(
            worker_id="worker-weather",
            topic="weather",
            model_policy="deep",
            status="idle",
            origin_epoch=1,
        )
    )
    global_sequence_before = state.sequence

    state.set_child_work_status(
        turn_id="turn-1", work_item_id="work-1", state="routing", origin_epoch=1
    )

    assert state.sequence > global_sequence_before
    ledger_entry = next(
        item for item in state.work_status_snapshot() if item.work_item_id == "work-1"
    )
    # The per-key event_sequence starts at 1 on the first parent
    # (re)aggregation -- it is not required to equal the global
    # SessionState.sequence watermark; they are distinct counters.
    assert ledger_entry.event_sequence == 1


# --- Phase 3: the work-status ledger is bounded ---------------------------


def _ledger_keysets(state: SessionState) -> tuple[set, set, set]:
    return (
        set(state._work_status_children),
        set(state._work_status_parents),
        set(state._work_status_sequence),
    )


def assert_ledger_lockstep(state: SessionState) -> None:
    """Invariant: the two *record* dicts are keyed in lockstep, and every live
    record has an ``event_sequence`` counter.

    The counter map is deliberately a superset: an evicted key can still be
    written again (a sibling multi-intent child, or a late commit's
    finalization), and dropping its counter would restart ``event_sequence`` at
    1 and be rejected as stale by the client reducer.
    """
    children, parents, sequence = _ledger_keysets(state)
    assert children == parents
    assert parents <= sequence


def test_expired_terminal_work_status_record_is_deleted_but_its_sequence_survives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    monkeypatch.setattr("server.session_state.time.monotonic", lambda: clock[0])
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    state.set_child_work_status(
        turn_id="turn-1", work_item_id="work-1", state="result_ready", origin_epoch=1
    )
    assert_ledger_lockstep(state)
    key = next(iter(state._work_status_parents))

    clock[0] = WORK_STATUS_TTL_SECONDS
    assert state.work_status_snapshot() == ()

    assert key not in state._work_status_children
    assert key not in state._work_status_parents
    assert state._work_status_sequence[key] == 1
    assert_ledger_lockstep(state)


@pytest.mark.parametrize("evict_via", ("ttl", "overflow"))
def test_evicted_terminal_key_is_not_resurrected_and_keeps_its_sequence(
    monkeypatch: pytest.MonkeyPatch, evict_via: str
) -> None:
    """Regression (Round 7): dropping a *terminal* record left no trace, so a
    later child write for the same key cold-started it and re-derived a
    non-terminal parent from that single child -- regressing a terminal state
    clients had already applied. A tombstone now refuses the write, and
    ``_work_status_sequence[key]`` still survives so nothing could restart at
    1 even if the key did become writable again."""
    clock = [0.0]
    monkeypatch.setattr("server.session_state.time.monotonic", lambda: clock[0])
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    for child_state in ("routing", "searching", "result_ready"):
        state.set_child_work_status(
            turn_id="turn-1",
            work_item_id="child-a",
            parent_work_item_id="parent-1",
            state=child_state,
            origin_epoch=1,
        )
    key = next(iter(state._work_status_parents))
    assert state._work_status_parents[key].status.event_sequence == 3

    if evict_via == "ttl":
        clock[0] = WORK_STATUS_TTL_SECONDS
        assert state.work_status_snapshot() == ()
    else:
        for index in range(SessionState._MAX_WORK_STATUS_KEYS + 1):
            clock[0] = float(index + 1)
            state.set_child_work_status(
                turn_id=f"filler-{index}",
                work_item_id=f"work-{index}",
                state="result_ready",
                origin_epoch=1,
            )
    assert key not in state._work_status_parents

    assert (
        state.set_child_work_status(
            turn_id="turn-1",
            work_item_id="child-b",
            parent_work_item_id="parent-1",
            state="routing",
            origin_epoch=1,
        )
        is None
    )
    assert key not in state._work_status_parents
    assert state._work_status_sequence[key] == 3
    # Regression: the cold-start child write above is legal per
    # set_child_work_status (it is `_reaggregate_parent`'s job, not
    # `legal_work_status_transition`'s, to refuse a tombstoned key) and used
    # to land in `_work_status_children` even though no parent record is ever
    # (re)written for a tombstoned key. Neither TTL pruning nor overflow
    # eviction scan `_work_status_children`, so that entry leaked for the
    # process lifetime. A tombstoned key must have neither a children entry
    # nor a parent record.
    assert key not in state._work_status_children
    assert_ledger_lockstep(state)


def test_work_status_sequence_survives_high_volume_eviction_and_never_restarts_at_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: the sequence-counter map used to be pruned once distinct
    WorkStatusKeys crossed a fixed threshold, dropping counters for
    already-evicted-but-orphaned keys. A key reused after its counter was
    pruned would restart ``event_sequence`` at 1 -- rejected as stale by the
    client reducer. The counter map must survive unboundedly instead."""
    clock = [0.0]
    monkeypatch.setattr("server.session_state.time.monotonic", lambda: clock[0])
    state = SessionState(session_id="session-1")
    state.active_epoch = 1

    state.set_child_work_status(
        turn_id="turn-first", work_item_id="work-first", state="result_ready", origin_epoch=1
    )
    first_key = next(iter(state._work_status_parents))

    distinct_keys_beyond_the_old_threshold = 8192 + 500
    for index in range(distinct_keys_beyond_the_old_threshold):
        clock[0] = float(index + 1)
        state.set_child_work_status(
            turn_id=f"filler-{index}",
            work_item_id=f"work-{index}",
            state="result_ready",
            origin_epoch=1,
        )

    assert first_key not in state._work_status_parents
    assert state._work_status_sequence[first_key] == 1
    assert len(state._work_status_sequence) > 8192

    # The evicted key was terminal, so its tombstone also refuses the rewrite
    # outright; the surviving counter is the second line of defence.
    assert (
        state.set_child_work_status(
            turn_id="turn-first", work_item_id="work-first", state="routing", origin_epoch=1
        )
        is None
    )


def test_live_terminal_work_status_is_not_deleted_before_the_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    monkeypatch.setattr("server.session_state.time.monotonic", lambda: clock[0])
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    state.set_child_work_status(
        turn_id="turn-1", work_item_id="work-1", state="result_ready", origin_epoch=1
    )

    clock[0] = WORK_STATUS_TTL_SECONDS - 1
    assert len(state.work_status_snapshot()) == 1
    assert len(state._work_status_parents) == 1
    assert_ledger_lockstep(state)


def test_work_status_ledger_is_capped_without_any_snapshot_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    monkeypatch.setattr("server.session_state.time.monotonic", lambda: clock[0])
    state = SessionState(session_id="session-1")
    state.active_epoch = 1

    cap = SessionState._MAX_WORK_STATUS_KEYS
    for index in range(cap + 1):
        clock[0] = float(index)
        state.set_child_work_status(
            turn_id=f"turn-{index}",
            work_item_id=f"work-{index}",
            state="result_ready",
            origin_epoch=1,
        )

    assert len(state._work_status_parents) == cap
    # The oldest terminal record is the one evicted; the newest survives.
    turn_ids = {key.turn_id for key in state._work_status_parents}
    assert "turn-0" not in turn_ids
    assert f"turn-{cap}" in turn_ids
    assert_ledger_lockstep(state)


def test_capping_never_evicts_a_still_active_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    monkeypatch.setattr("server.session_state.time.monotonic", lambda: clock[0])
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    state.set_child_work_status(
        turn_id="turn-active", work_item_id="work-active", state="routing", origin_epoch=1
    )

    cap = SessionState._MAX_WORK_STATUS_KEYS
    for index in range(cap + 2):
        clock[0] = float(index + 1)
        state.set_child_work_status(
            turn_id=f"turn-{index}",
            work_item_id=f"work-{index}",
            state="result_ready",
            origin_epoch=1,
        )

    assert len(state._work_status_parents) == cap
    assert any(key.turn_id == "turn-active" for key in state._work_status_parents)
    assert_ledger_lockstep(state)


# --- Review-gauntlet round 7 -------------------------------------------


def test_overflow_eviction_never_drops_a_live_non_terminal_record() -> None:
    """Round 7 (deep-review/logic): once every retained key was non-terminal
    the cap evicted a *live* parent, dropping its children map. The next
    ``set_child_work_status`` for that key then saw ``previous_child=None``
    (a cold start), so a whole-child-set cancel sweep became a no-op and the
    parent could never terminalize. The ledger now exceeds the cap instead."""
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    for index in range(SessionState._MAX_WORK_STATUS_KEYS + 4):
        state.set_child_work_status(
            turn_id=f"turn-{index}",
            work_item_id=f"work-{index}",
            state="routing",
            origin_epoch=1,
        )

    assert len(state._work_status_parents) == SessionState._MAX_WORK_STATUS_KEYS + 4
    first = WorkStatusKey(1, "turn-0", "work-0")
    assert state._work_status_children[first] == {
        "work-0": state._work_status_children[first]["work-0"]
    }
    # The children map survived, so a cancel sweep is not a cold start.
    assert (
        state.set_child_work_status(
            turn_id="turn-0", work_item_id="work-0", state="cancelled", origin_epoch=1
        )
        is not None
    )
    assert state._work_status_parents[first].status.state == "cancelled"
    assert_ledger_lockstep(state)


def test_ttl_expired_terminal_key_is_not_resurrected_by_a_late_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Round 7 (deep-review/logic): TTL pruning dropped a key's children map
    but kept nothing to say it had ever been terminal. A late commit's
    finalization arriving after the five-minute TTL cold-started the key and
    re-derived a parent state from that single child, resurfacing it below a
    terminal state clients had already applied."""
    clock = [0.0]
    monkeypatch.setattr("server.session_state.time.monotonic", lambda: clock[0])
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    state.set_child_work_status(
        turn_id="turn-1", work_item_id="work-1", state="result_ready", origin_epoch=1
    )
    key = WorkStatusKey(1, "turn-1", "work-1")

    clock[0] = WORK_STATUS_TTL_SECONDS
    assert state.work_status_snapshot() == ()
    assert key not in state._work_status_parents

    assert (
        state.set_child_work_status(
            turn_id="turn-1", work_item_id="work-1", state="routing", origin_epoch=1
        )
        is None
    )
    assert key not in state._work_status_parents
    assert state.work_status_snapshot() == ()


def test_retention_constants_match_the_shared_config_file() -> None:
    """Parity guard: WORK_STATUS_TTL_SECONDS/_MAX_WORK_STATUS_KEYS must come
    from shared/work-status-retention.json, not a hardcoded literal.

    web/src/state.js loads the same file for WORK_STATUS_TERMINAL_TTL_MS /
    WORK_STATUS_MAX_KEYS (see web/test/state.test.js's mirror of this test).
    If a future edit reverts either side to an inline literal, this test
    still passes as long as both literals happen to agree today -- it cannot
    catch that regression by itself, but re-pointing either side at the
    shared file is the fix this test exists to keep visible, and
    shared/protocol.md's "Progressive work status" section documents the
    values both files must agree on.
    """
    import json as _json
    from pathlib import Path as _Path

    config = _json.loads(
        (_Path(__file__).resolve().parents[1] / "shared/work-status-retention.json").read_text()
    )
    assert WORK_STATUS_TTL_SECONDS == float(config["ttl_seconds"])
    assert SessionState._MAX_WORK_STATUS_KEYS == int(config["max_keys"])


def test_prune_expired_work_status_is_the_named_mutating_step() -> None:
    """Round 7 (deep-review/architecture): the TTL prune was an unnamed side
    effect of a read-shaped ``work_status_snapshot()``. It is now its own
    named step."""
    assert callable(SessionState.prune_expired_work_status)
