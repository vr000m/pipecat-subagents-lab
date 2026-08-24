"""Session state is authoritative and retains immutable result history."""

from pathlib import Path

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


def test_events_log_is_bounded_and_evicts_oldest_first() -> None:
    """Regression: `_events` used to be a plain, never-pruned `list`. The
    only reader (`RuntimeObserver.messages()`) is a diagnostic replay API,
    but nothing bounded the log's growth for a long-running session -- it
    accumulated one entry per state-mutating call for the process lifetime.
    A `deque(maxlen=SessionState._MAX_EVENTS)` caps memory, evicting the
    oldest event once the cap is exceeded."""
    state = SessionState(session_id="session-1")
    state.set_worker(
        WorkerState(worker_id="worker-weather", topic="weather", model_policy="deep", status="idle")
    )
    total = SessionState._MAX_EVENTS + 50
    for index in range(total):
        state.append_result(result(f"result-{index}"))

    events = state.events
    assert len(events) == SessionState._MAX_EVENTS
    # The oldest 50 events (the worker-set plus the first 49 results) were
    # evicted; the log now starts wherever the cap first bit.
    kinds_and_payloads = [(event.kind, event.payload.get("result_id")) for event in events]
    assert ("worker", None) not in kinds_and_payloads
    assert kinds_and_payloads[0] == ("result", f"result-{total - SessionState._MAX_EVENTS}")
    assert kinds_and_payloads[-1] == ("result", f"result-{total - 1}")


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


@pytest.mark.skipif(
    WorkStatusKey is None, reason="server.session_state.WorkStatusKey not implemented yet"
)
def test_from_snapshot_warns_when_two_none_work_item_id_records_collapse_onto_one_key() -> None:
    """Round 1 gauntlet logic finding: ``from_snapshot`` maps every
    ``work_item_id is None`` record for a given ``(origin_epoch, turn_id)``
    onto the same ``WorkStatusKey`` (via ``work_item_id or ""``). No current
    producer emits ``work_item_id=None`` (the wire schema still declares it
    Optional), but a manually-constructed snapshot with two such records must
    not silently lose one -- the collision must at least be logged rather
    than overwriting silently."""
    import io

    from loguru import logger

    from server.contracts import RuntimeSnapshot, WorkStatus

    RuntimeSnapshot.reset_monotonicity("session-collision")
    snapshot = RuntimeSnapshot(
        contract_version=CONTRACT_VERSION,
        session_id="session-collision",
        snapshot_sequence=1,
        work_status=[
            WorkStatus(turn_id="turn-1", work_item_id=None, state="routing", event_sequence=1),
            WorkStatus(turn_id="turn-1", work_item_id=None, state="searching", event_sequence=2),
        ],
    )

    sink = io.StringIO()
    handler_id = logger.add(sink, level="WARNING", format="{message}")
    try:
        restored = SessionState.from_snapshot(snapshot)
    finally:
        logger.remove(handler_id)

    log_output = sink.getvalue()
    assert "overwriting existing work-status record" in log_output

    key = WorkStatusKey(origin_epoch=None, turn_id="turn-1", parent_key="")
    # Documents the known-collision outcome (last-write-wins), not a fix for
    # the collision itself -- only one of the two records survives.
    assert restored._work_status_parents[key].status.state == "searching"


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
    # The non-authoritative stamp is per-key state on a *live* record, so
    # unlike the counter map it must never outlive the record it clocks.
    assert set(state._work_status_nonauthoritative_at) <= parents


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


def test_restored_parent_never_terminalizes_from_a_partial_child_set() -> None:
    """Regression (#8): ``from_snapshot`` restores a multi-intent parent's
    aggregate but not its children (child records are server-internal and
    never cross the wire), so the restored record's children map starts empty.
    A single child reporting ``result_ready`` after the restore was then
    treated as the *complete* aggregate and emitted ``result_ready`` while a
    sibling was still searching."""
    state = SessionState(session_id="session-restore")
    state.active_epoch = 2
    state.set_child_work_status(
        turn_id="turn-mi",
        work_item_id="work-mi-0",
        parent_work_item_id="work-mi",
        state="searching",
        origin_epoch=2,
    )
    state.set_child_work_status(
        turn_id="turn-mi",
        work_item_id="work-mi-1",
        parent_work_item_id="work-mi",
        state="searching",
        origin_epoch=2,
    )
    snapshot = state.snapshot(origin_epoch=2, include_work_status=True)
    assert [status.state for status in snapshot.work_status] == ["searching"]

    restored = SessionState.from_snapshot(snapshot)
    restored.active_epoch = 2
    key = WorkStatusKey(2, "turn-mi", "work-mi")
    assert restored._work_status_children[key] == {}
    assert key in restored._work_status_nonauthoritative_at

    # Only ONE of the two children reports; the other is still searching.
    event = restored.set_child_work_status(
        turn_id="turn-mi",
        work_item_id="work-mi-0",
        parent_work_item_id="work-mi",
        state="result_ready",
        origin_epoch=2,
    )
    assert event is None
    assert restored._work_status_parents[key].status.state == "searching"
    assert [status.state for status in restored.work_status_snapshot()] == ["searching"]


def test_restored_parent_stays_non_authoritative_across_a_non_terminal_update() -> None:
    """The incompleteness is a property of the key, not of one record: a
    non-terminal re-aggregation must not reset the flag and re-open the
    premature-terminalization hole."""
    state = SessionState(session_id="session-restore-2")
    state.active_epoch = 3
    state.set_child_work_status(
        turn_id="turn-mi",
        work_item_id="work-mi-0",
        parent_work_item_id="work-mi",
        state="routing",
        origin_epoch=3,
    )
    restored = SessionState.from_snapshot(state.snapshot(origin_epoch=3, include_work_status=True))
    restored.active_epoch = 3
    key = WorkStatusKey(3, "turn-mi", "work-mi")

    # A legal non-terminal refinement still flows through.
    assert (
        restored.set_child_work_status(
            turn_id="turn-mi",
            work_item_id="work-mi-0",
            parent_work_item_id="work-mi",
            state="searching",
            origin_epoch=3,
        )
        is not None
    )
    assert restored._work_status_parents[key].status.state == "searching"
    assert key in restored._work_status_nonauthoritative_at

    assert (
        restored.set_child_work_status(
            turn_id="turn-mi",
            work_item_id="work-mi-0",
            parent_work_item_id="work-mi",
            state="result_ready",
            origin_epoch=3,
        )
        is None
    )
    assert restored._work_status_parents[key].status.state == "searching"


def test_restored_non_terminal_parent_is_pruned_by_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression (Round 9, #5/#9): round 8's non-terminalization guard made a
    restored NON-terminal parent immortal.

    Such a record can never reach a terminal state, so ``terminal_at`` stayed
    ``None`` forever -- and both removal paths (``prune_expired_work_status``
    and ``_evict_work_status_overflow``) filtered on ``terminal_at is not
    None``. The record was therefore never pruned, never evicted, and
    re-shipped to the client on every snapshot for the process lifetime, which
    is exactly the "expires by TTL" escape hatch round 8's own comment claimed
    but did not provide. Round 8's tests only checked the accuracy half (no
    premature terminalization), not the retention half.
    """
    clock = [0.0]
    monkeypatch.setattr("server.session_state.time.monotonic", lambda: clock[0])
    state = SessionState(session_id="session-restore-ttl")
    state.active_epoch = 4
    state.set_child_work_status(
        turn_id="turn-mi",
        work_item_id="work-mi-0",
        parent_work_item_id="work-mi",
        state="searching",
        origin_epoch=4,
    )
    snapshot = state.snapshot(origin_epoch=4, include_work_status=True)

    clock[0] = 100.0
    restored = SessionState.from_snapshot(snapshot)
    key = WorkStatusKey(4, "turn-mi", "work-mi")
    assert restored._work_status_parents[key].terminal_at is None
    assert [status.state for status in restored.work_status_snapshot()] == ["searching"]

    # One tick short of the TTL measured from the restore instant: retained.
    clock[0] = 100.0 + WORK_STATUS_TTL_SECONDS - 1.0
    assert [status.state for status in restored.work_status_snapshot()] == ["searching"]

    clock[0] = 100.0 + WORK_STATUS_TTL_SECONDS
    assert restored.work_status_snapshot() == ()
    assert key not in restored._work_status_parents
    assert key not in restored._work_status_children
    assert key not in restored._work_status_nonauthoritative_at
    # The counter still survives, as for any other forgotten key.
    assert restored._work_status_sequence[key] >= 1
    assert_ledger_lockstep(restored)


def test_pruned_restored_key_still_refuses_to_terminalize_from_a_partial_child_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression (Round 10, #3/#19): round 9 moved the non-authoritative marker
    off the record and onto the key precisely so it would outlive record pops --
    then had ``_forget_work_status`` pop it anyway.

    A restored record that survives a TTL prune therefore lost the only evidence
    that its child set was known-incomplete. The next child report cold-started
    the key with no live record and no terminal tombstone (the restored record
    was non-terminal), so ``_reaggregate_parent``'s guard did not fire and a
    single child's ``result_ready`` terminalized the whole parent -- announcing
    a result for a turn whose siblings may still be searching, which is the
    exact failure the guard exists to prevent.
    """
    clock = [0.0]
    monkeypatch.setattr("server.session_state.time.monotonic", lambda: clock[0])
    state = SessionState(session_id="session-prune-then-report")
    state.active_epoch = 7
    state.set_child_work_status(
        turn_id="turn-mi",
        work_item_id="work-mi-0",
        parent_work_item_id="work-mi",
        state="searching",
        origin_epoch=7,
    )
    snapshot = state.snapshot(origin_epoch=7, include_work_status=True)

    restored = SessionState.from_snapshot(snapshot)
    restored.active_epoch = 7
    key = WorkStatusKey(7, "turn-mi", "work-mi")

    # TTL-prune the restored record. The key-scoped tombstone must survive it.
    clock[0] = WORK_STATUS_TTL_SECONDS
    assert restored.work_status_snapshot() == ()
    assert key not in restored._work_status_parents
    assert key in restored._work_status_nonauthoritative_keys

    # A late sibling reports terminal. The parent must NOT be resurrected at a
    # terminal state off this one child.
    event = restored.set_child_work_status(
        turn_id="turn-mi",
        work_item_id="work-mi-1",
        parent_work_item_id="work-mi",
        state="result_ready",
        origin_epoch=7,
    )
    assert event is None
    assert key not in restored._work_status_parents
    assert restored.work_status_snapshot() == ()
    assert_ledger_lockstep(restored)

    # A non-terminal report still flows through, and gets a *fresh* TTL rather
    # than inheriting the already-expired restore instant.
    event = restored.set_child_work_status(
        turn_id="turn-mi",
        work_item_id="work-mi-2",
        parent_work_item_id="work-mi",
        state="searching",
        origin_epoch=7,
    )
    assert event is not None
    assert restored._work_status_parents[key].status.state == "searching"
    assert restored._work_status_nonauthoritative_at[key] == WORK_STATUS_TTL_SECONDS
    assert [status.state for status in restored.work_status_snapshot()] == ["searching"]
    assert_ledger_lockstep(restored)


def test_restored_non_terminal_parent_is_evictable_on_overflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Companion to the TTL case (#5/#9): a restored non-terminal parent must
    also be an overflow-eviction candidate.

    The usual reason to protect a non-terminal record -- its children map is
    the sole memory of which children exist -- does not apply to a restored
    one, whose children map is empty by construction. Without this it would
    pin a ledger slot for the process lifetime even under cap pressure.
    """
    clock = [0.0]
    monkeypatch.setattr("server.session_state.time.monotonic", lambda: clock[0])
    seed = SessionState(session_id="session-restore-evict")
    seed.active_epoch = 5
    seed.set_child_work_status(
        turn_id="turn-restored",
        work_item_id="work-restored-0",
        parent_work_item_id="work-restored",
        state="searching",
        origin_epoch=5,
    )
    restored = SessionState.from_snapshot(seed.snapshot(origin_epoch=5, include_work_status=True))
    restored.active_epoch = 5
    restored_key = WorkStatusKey(5, "turn-restored", "work-restored")
    assert restored_key in restored._work_status_parents

    for index in range(SessionState._MAX_WORK_STATUS_KEYS + 1):
        clock[0] = float(index + 10)
        restored.set_child_work_status(
            turn_id=f"filler-{index}",
            work_item_id=f"work-{index}",
            state="result_ready",
            origin_epoch=5,
        )

    assert restored_key not in restored._work_status_parents
    assert len(restored._work_status_parents) <= SessionState._MAX_WORK_STATUS_KEYS
    assert_ledger_lockstep(restored)


def test_retention_fallback_matches_shared_config() -> None:
    """Round-2 confirm pass: the packaged-install import fix added a *second*
    copy of the retention numbers as an inline literal fallback.

    shared/work-status-retention.json declares itself the single source of
    truth for both, so nothing detected drift: editing ``ttl_seconds`` there
    would leave packaged installs silently running the stale fallback while
    dev/CI ran the new value, with server and browser disagreeing about
    terminal-record retention. This test makes that drift fail CI.
    """
    import json as _json

    from server.session_state import _RETENTION_CONFIG_PATH, _RETENTION_FALLBACK

    shared = _json.loads(_RETENTION_CONFIG_PATH.read_text())
    # ``$comment`` is documentation, not a retention bound; every other key in
    # the file is one and must be mirrored in the fallback.
    numeric = {key: value for key, value in shared.items() if not key.startswith("$")}

    assert _RETENTION_FALLBACK == numeric, (
        "the packaged-install fallback has drifted from shared/work-status-retention.json"
    )


def test_retention_config_read_refuses_fifo_and_symlink(tmp_path: Path) -> None:
    """Round-3 gauntlet, Security finding: the boot-path retention read used
    ``Path.read_text()``, which follows a symlink silently and blocks forever
    on a FIFO planted at this predictable, repo-relative path -- and does so at
    ``import server.session_state`` time, before every other guard in the
    process. The ``except (OSError, ValueError)`` fallback catches neither.

    Pins the primitive the module now reads through: it must refuse a FIFO
    (rather than block) and refuse to read *through* a symlink, returning
    ``None`` so the caller lands on ``_RETENTION_FALLBACK``.
    """
    import os

    # Each name from its owning module: the read primitive belongs to
    # ``server.config`` (``session_state`` only imports it), and reaching it
    # through the importer made the same function addressable under three
    # names (round-4 confirm pass, Architecture finding).
    from server.config import read_regular_file_no_follow
    from server.session_state import _RETENTION_MAX_BYTES

    real = tmp_path / "real.json"
    real.write_text('{"ttl_seconds": 1, "max_keys": 2}')
    assert read_regular_file_no_follow(real, max_bytes=_RETENTION_MAX_BYTES) is not None

    fifo = tmp_path / "fifo.json"
    os.mkfifo(fifo)
    # Must return promptly with None rather than hanging on the open/read.
    assert read_regular_file_no_follow(fifo, max_bytes=_RETENTION_MAX_BYTES) is None

    link = tmp_path / "link.json"
    link.symlink_to(real)
    assert read_regular_file_no_follow(link, max_bytes=_RETENTION_MAX_BYTES) is None

    oversized = tmp_path / "big.json"
    oversized.write_bytes(b"x" * (_RETENTION_MAX_BYTES + 1))
    assert read_regular_file_no_follow(oversized, max_bytes=_RETENTION_MAX_BYTES) is None
