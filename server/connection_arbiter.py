"""Connection epoch fencing for one active browser transport."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .contracts import CONTRACT_VERSION, SnapshotHandshake


@dataclass(frozen=True)
class Connection:
    epoch: int
    session_id: str
    resume_token: str


class ConnectionArbiter:
    def __init__(self, session_id: str, resume_token: str) -> None:
        self.session_id = session_id
        self.resume_token = resume_token
        self._epoch = 0
        self._active: Connection | None = None

    @property
    def active(self) -> Connection | None:
        return self._active

    @property
    def epoch(self) -> int:
        return self._epoch

    def promote(self, handshake: SnapshotHandshake | dict[str, Any]) -> Connection:
        value = SnapshotHandshake.model_validate(handshake)
        if value.contract_version != CONTRACT_VERSION:
            raise ValueError(f"unsupported contract version: {value.contract_version}")
        if value.session_id != self.session_id or value.resume_token != self.resume_token:
            raise ValueError("session identity or resume token is invalid")
        if self._active is not None and value.proposed_epoch <= self._epoch:
            raise ValueError("connection epoch is stale")
        self._epoch = value.proposed_epoch
        self._active = Connection(self._epoch, self.session_id, self.resume_token)
        return self._active

    def accepts(self, epoch: int | None) -> bool:
        return self._active is not None and epoch == self._active.epoch

    def require_active(self, epoch: int | None) -> None:
        if not self.accepts(epoch):
            raise ValueError("stale or missing connection epoch")

    def fence(self, epoch: int | None) -> bool:
        """Return whether an inbound callback belongs to the promoted connection."""
        return self.accepts(epoch)

    def route_callback(self, epoch: int | None, callback: Any, *args: Any, **kwargs: Any) -> Any:
        """Run a callback only while its originating connection is authoritative."""
        self.require_active(epoch)
        return callback(*args, **kwargs)


@dataclass(frozen=True)
class ActiveConnection:
    client_id: str
    epoch: int


class ConnectionEpochArbiter(ConnectionArbiter):
    """Client-tracking facade that delegates all fencing decisions to ConnectionArbiter.

    This exists only to add a client-ID/replacement-history projection on top of
    the parent's promote()/accepts() state machine; it must never reimplement the
    epoch stale-check or active/epoch mutation itself, or the two can silently drift.
    """

    def __init__(self, session_id: str, resume_token: str) -> None:
        super().__init__(session_id, resume_token)
        self.active_client_id: str | None = None
        self._previous: list[ActiveConnection] = []

    def validate_handshake(
        self, *, session_id: str, resume_token: str, proposed_epoch: int
    ) -> bool:
        if session_id != self.session_id or resume_token != self.resume_token:
            return False
        return self._active is None or proposed_epoch > self.epoch

    def activate(self, client_id: str, proposed_epoch: int) -> ActiveConnection:
        previous_client, previous_epoch = self.active_client_id, self._active
        connection = self.promote(
            {
                "contract_version": CONTRACT_VERSION,
                "session_id": self.session_id,
                "resume_token": self.resume_token,
                "proposed_epoch": proposed_epoch,
                "snapshot_sequence": 0,
            }
        )
        if previous_epoch is not None:
            self._previous.append(ActiveConnection(previous_client or "", previous_epoch.epoch))
        self.active_client_id = client_id
        return ActiveConnection(client_id, connection.epoch)

    def accepts_input(self, client_id: str, epoch: int) -> bool:
        return self.active_client_id == client_id and self.accepts(epoch)

    def snapshot_allowed(self, client_id: str, epoch: int) -> bool:
        return self.accepts_input(client_id, epoch)

    def accepts_callback(self, epoch: int) -> bool:
        return self.accepts(epoch)

    def replacement_interruptions(self) -> tuple[tuple[int, str], ...]:
        return tuple(
            (connection.epoch, "interrupted_by_reconnect") for connection in self._previous
        )
