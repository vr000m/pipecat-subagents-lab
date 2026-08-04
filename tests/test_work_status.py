"""Parent/child work-status aggregation, legal transitions, and TTL rules.

Plan: docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md,
"### Phase 3: Progressive RTVI status (P1/P2)". Exercises
`SessionState.set_child_work_status`, `SessionState._aggregate` (the
exhaustive parent join), `legal_work_status_transition`, and the
5-minute `work_status_snapshot()` TTL projection.
"""

from __future__ import annotations

import pytest

from server.contracts import WorkStatus, legal_work_status_transition
from server.session_state import WORK_STATUS_TTL_SECONDS, SessionState


def child(
    state: str, terminal_reason: str | None = None, work_item_id: str = "child-1"
) -> WorkStatus:
    return WorkStatus(
        turn_id="turn-1",
        work_item_id=work_item_id,
        state=state,
        event_sequence=0,
        terminal_reason=terminal_reason,
        origin_epoch=1,
    )


# --- Exhaustive parent aggregation (plan bullet 232) -----------------------


def test_any_child_routing_makes_parent_routing() -> None:
    assert SessionState._aggregate([child("routing"), child("searching")]) == ("routing", None)


def test_any_child_searching_with_none_routing_makes_parent_searching() -> None:
    assert SessionState._aggregate([child("searching"), child("background")]) == ("searching", None)


def test_no_active_child_with_one_retained_makes_parent_background() -> None:
    assert SessionState._aggregate([child("background"), child("background")]) == (
        "background",
        None,
    )


def test_all_terminal_any_failed_wins_failed_with_reason() -> None:
    assert SessionState._aggregate([child("result_ready"), child("failed", "missing_worker")]) == (
        "failed",
        "missing_worker",
    )
    assert SessionState._aggregate([child("cancelled"), child("failed", "retention_rejected")]) == (
        "failed",
        "retention_rejected",
    )


def test_all_terminal_all_cancelled_makes_parent_cancelled() -> None:
    assert SessionState._aggregate([child("cancelled"), child("cancelled")]) == ("cancelled", None)


def test_completed_plus_cancelled_is_result_ready() -> None:
    assert SessionState._aggregate([child("result_ready"), child("cancelled")]) == (
        "result_ready",
        None,
    )


def test_all_result_ready_is_result_ready() -> None:
    assert SessionState._aggregate([child("result_ready")]) == ("result_ready", None)


def test_out_of_order_child_completion_does_not_change_the_aggregation_result() -> None:
    """Aggregation is a pure function of the terminal child-state set -- the
    order children complete in must not change the parent's result."""
    forward = SessionState._aggregate(
        [child("result_ready"), child("cancelled"), child("failed", "missing_worker")]
    )
    backward = SessionState._aggregate(
        [child("failed", "missing_worker"), child("cancelled"), child("result_ready")]
    )
    assert forward == backward == ("failed", "missing_worker")


# --- Legal transitions (server.contracts.legal_work_status_transition) ----


LEGAL = {
    "routing": {"searching", "failed", "cancelled"},
    "searching": {"background", "result_ready", "failed", "cancelled"},
    "background": {"result_ready", "failed", "cancelled"},
}
TERMINAL = {"result_ready", "failed", "cancelled"}


@pytest.mark.parametrize(
    ("start", "end"), [(s, e) for s, targets in LEGAL.items() for e in targets]
)
def test_legal_transition_pairs_are_accepted(start: str, end: str) -> None:
    assert legal_work_status_transition(start, end) is True


def test_terminal_states_have_no_legal_successor() -> None:
    for terminal_state in TERMINAL:
        for candidate in {"routing", "searching", "background", *TERMINAL}:
            assert legal_work_status_transition(terminal_state, candidate) is False


def test_routing_time_cancellation_and_failure_do_not_imply_a_retained_search_task() -> None:
    assert legal_work_status_transition("routing", "failed") is True
    assert legal_work_status_transition("routing", "cancelled") is True
    assert legal_work_status_transition("routing", "background") is False


def test_fast_success_may_skip_background_entirely() -> None:
    assert legal_work_status_transition("searching", "result_ready") is True


def test_same_state_is_not_a_legal_transition() -> None:
    for state in ("routing", "searching", "background"):
        assert legal_work_status_transition(state, state) is False


# --- SessionState.set_child_work_status / parent join / ledger ------------


def test_all_clarify_or_declined_turn_creates_no_client_visible_parent_status() -> None:
    """Clarify/declined/direct/unsupported outcomes never call
    set_child_work_status -- an empty delegated child set for a turn simply
    never allocates a WorkStatusKey, so nothing appears in the snapshot."""
    state = SessionState(session_id="session-1")
    state.active_epoch = 1

    assert state.work_status_snapshot() == ()


def test_single_delegated_child_creates_a_visible_parent_status() -> None:
    state = SessionState(session_id="session-1")
    state.active_epoch = 1

    state.set_child_work_status(
        turn_id="turn-1", work_item_id="work-1", state="routing", origin_epoch=1
    )

    statuses = state.work_status_snapshot()
    assert len(statuses) == 1
    assert statuses[0].state == "routing"
    assert statuses[0].work_item_id == "work-1"


def test_mixed_multi_intent_parent_aggregates_only_its_delegated_children() -> None:
    """Plan bullet 232: the parent join is exhaustive over the CURRENT
    delegated child-state set -- 'routing while any child is routing' -- and
    is not a transition from the parent's own previous coarse state. If this
    fails, check `SessionState._reaggregate_parent`'s
    `legal_work_status_transition(record.status.state, parent_state)` guard:
    gating the recomputed parent aggregate through a transition table
    intended for a single entity's own progression can wrongly reject a
    second child regressing the aggregate view back to "routing" after the
    parent has already advanced to "searching" from an earlier child."""
    state = SessionState(session_id="session-1")
    state.active_epoch = 1

    state.set_child_work_status(
        turn_id="turn-1",
        work_item_id="child-a",
        parent_work_item_id="parent-1",
        state="searching",
        origin_epoch=1,
    )
    state.set_child_work_status(
        turn_id="turn-1",
        work_item_id="child-b",
        parent_work_item_id="parent-1",
        state="routing",
        origin_epoch=1,
    )

    statuses = state.work_status_snapshot()
    assert len(statuses) == 1
    assert statuses[0].work_item_id == "parent-1"
    assert statuses[0].state == "routing"  # any child routing wins


def test_out_of_order_child_completion_through_the_real_ledger() -> None:
    state = SessionState(session_id="session-1")
    state.active_epoch = 1

    state.set_child_work_status(
        turn_id="turn-1",
        work_item_id="child-a",
        parent_work_item_id="parent-1",
        state="routing",
        origin_epoch=1,
    )
    state.set_child_work_status(
        turn_id="turn-1",
        work_item_id="child-a",
        parent_work_item_id="parent-1",
        state="searching",
        origin_epoch=1,
    )
    state.set_child_work_status(
        turn_id="turn-1",
        work_item_id="child-b",
        parent_work_item_id="parent-1",
        state="routing",
        origin_epoch=1,
    )
    state.set_child_work_status(
        turn_id="turn-1",
        work_item_id="child-b",
        parent_work_item_id="parent-1",
        state="failed",
        terminal_reason="missing_worker",
        origin_epoch=1,
    )
    state.set_child_work_status(
        turn_id="turn-1",
        work_item_id="child-a",
        parent_work_item_id="parent-1",
        state="result_ready",
        origin_epoch=1,
    )

    statuses = state.work_status_snapshot()
    assert len(statuses) == 1
    assert statuses[0].state == "failed"
    assert statuses[0].terminal_reason == "missing_worker"


def test_child_terminal_records_never_overwrite_the_parent_directly() -> None:
    """Every visible record in the snapshot is the aggregated parent view --
    a child's own terminal WorkStatus is never itself surfaced."""
    state = SessionState(session_id="session-1")
    state.active_epoch = 1

    state.set_child_work_status(
        turn_id="turn-1",
        work_item_id="child-a",
        parent_work_item_id="parent-1",
        state="result_ready",
        origin_epoch=1,
    )

    statuses = state.work_status_snapshot()
    assert all(status.work_item_id == "parent-1" for status in statuses)


def test_illegal_child_transition_is_ignored_and_does_not_regress_the_parent() -> None:
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    state.set_child_work_status(
        turn_id="turn-1", work_item_id="work-1", state="result_ready", origin_epoch=1
    )

    # result_ready -> routing is illegal; the ledger must ignore it.
    state.set_child_work_status(
        turn_id="turn-1", work_item_id="work-1", state="routing", origin_epoch=1
    )

    statuses = state.work_status_snapshot()
    assert statuses[0].state == "result_ready"


# --- TTL boundary: just-below / exactly-at / just-above --------------------


def _terminal_state(monkeypatch: pytest.MonkeyPatch) -> tuple[SessionState, list[float]]:
    clock = [0.0]
    monkeypatch.setattr("server.session_state.time.monotonic", lambda: clock[0])
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    state.set_child_work_status(
        turn_id="turn-1", work_item_id="work-1", state="result_ready", origin_epoch=1
    )
    return state, clock


def test_just_below_ttl_is_included(monkeypatch: pytest.MonkeyPatch) -> None:
    state, clock = _terminal_state(monkeypatch)
    clock[0] = WORK_STATUS_TTL_SECONDS - 1

    assert len(state.work_status_snapshot()) == 1


def test_exactly_at_ttl_is_excluded_inclusive_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    state, clock = _terminal_state(monkeypatch)
    clock[0] = WORK_STATUS_TTL_SECONDS

    assert state.work_status_snapshot() == ()


def test_just_above_ttl_is_excluded(monkeypatch: pytest.MonkeyPatch) -> None:
    state, clock = _terminal_state(monkeypatch)
    clock[0] = WORK_STATUS_TTL_SECONDS + 1

    assert state.work_status_snapshot() == ()


# --- Cross-epoch terminal-status preservation in snapshots -----------------


def test_terminal_status_preserves_its_original_origin_epoch_across_reconnect() -> None:
    """A terminal work_status record from an earlier epoch must keep that
    historical origin_epoch inside a later snapshot -- it is embedded
    payload state, distinct from the snapshot envelope's current epoch."""
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    state.set_child_work_status(
        turn_id="turn-1", work_item_id="work-1", state="result_ready", origin_epoch=1
    )
    state.active_epoch = 2  # reconnect promotes a new epoch

    preserved = [item for item in state.work_status_snapshot() if item.work_item_id == "work-1"]
    assert preserved and preserved[0].origin_epoch == 1


def test_snapshot_projection_embeds_work_status_only_when_requested() -> None:
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    state.set_child_work_status(
        turn_id="turn-1", work_item_id="work-1", state="background", origin_epoch=1
    )

    assert state.snapshot(origin_epoch=1).work_status == []
    included = state.snapshot(origin_epoch=1, include_work_status=True).work_status
    assert len(included) == 1
    assert included[0].work_item_id == "work-1"
