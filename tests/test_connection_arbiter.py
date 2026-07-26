"""Connection epochs fence stale transports and callbacks."""

import pytest

from server.connection_arbiter import ConnectionArbiter, ConnectionEpochArbiter


def handshake(epoch: int) -> dict[str, object]:
    return {
        "session_id": "session-1",
        "resume_token": "resume-1",
        "proposed_epoch": epoch,
        "snapshot_sequence": 0,
    }


def test_replacement_promotes_new_epoch_and_fences_old_transport() -> None:
    arbiter = ConnectionArbiter(session_id="session-1", resume_token="resume-1")
    old = arbiter.promote(handshake(1))
    new = arbiter.promote(handshake(2))

    assert new.epoch > old.epoch
    assert arbiter.accepts(new.epoch)
    assert not arbiter.accepts(old.epoch)
    assert arbiter.fence(old.epoch) is False


def test_initial_connection_accepts_schema_valid_epoch_zero() -> None:
    arbiter = ConnectionArbiter(session_id="session-1", resume_token="resume-1")

    connection = arbiter.promote(handshake(0))

    assert connection.epoch == 0
    assert arbiter.accepts(0)
    with pytest.raises(ValueError, match="connection epoch is stale"):
        arbiter.promote(handshake(0))

    compatibility_arbiter = ConnectionEpochArbiter(session_id="session-1", resume_token="resume-1")
    active = compatibility_arbiter.activate("client-a", 0)
    assert compatibility_arbiter.accepts_input(active.client_id, active.epoch)


def test_handshake_requires_durable_identity_and_monotonic_epoch() -> None:
    arbiter = ConnectionArbiter(session_id="session-1", resume_token="resume-1")
    with pytest.raises(ValueError):
        arbiter.promote({**handshake(1), "session_id": "other"})
    with pytest.raises(ValueError):
        arbiter.promote({**handshake(1), "resume_token": "wrong"})
    arbiter.promote(handshake(2))
    with pytest.raises(ValueError):
        arbiter.promote(handshake(2))


def test_handshake_rejects_unsupported_contract_version_at_boundary() -> None:
    arbiter = ConnectionArbiter(session_id="session-1", resume_token="resume-1")

    with pytest.raises(ValueError, match="unsupported contract version"):
        arbiter.promote({**handshake(1), "contract_version": "v2.0"})


def test_stale_callbacks_are_rejected_at_the_state_boundary() -> None:
    arbiter = ConnectionArbiter(session_id="session-1", resume_token="resume-1")
    arbiter.promote(handshake(1))
    arbiter.promote(handshake(2))

    assert arbiter.fence(1) is False
    assert arbiter.fence(2) is True


def test_client_arbiter_fences_input_snapshot_and_callbacks_after_replacement() -> None:
    arbiter = ConnectionEpochArbiter(session_id="session-1", resume_token="resume-1")
    old = arbiter.activate("client-a", 1)
    new = arbiter.activate("client-b", 2)

    assert arbiter.accepts_input(new.client_id, new.epoch)
    assert arbiter.snapshot_allowed(new.client_id, new.epoch)
    assert arbiter.accepts_callback(new.epoch)
    assert not arbiter.accepts_input(old.client_id, old.epoch)
    assert not arbiter.snapshot_allowed(old.client_id, old.epoch)
    assert not arbiter.accepts_callback(old.epoch)
    assert arbiter.replacement_interruptions() == ((1, "interrupted_by_reconnect"),)
