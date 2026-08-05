"""Connection-scoped projection of authoritative session events."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from typing import Any

from .contracts import RuntimeSnapshot
from .session_state import SessionState, StateEvent

_ALWAYS_VISIBLE_KINDS = frozenset(
    {
        "runtime_snapshot",
        "result",
        "speech_progress",
        "worker",
        "routing",
        "user_transcript",
        "bot_transcript",
    }
)


class RuntimeObserver:
    """Owns entitlement filtering and the connection-projected sequence.

    ``SessionState._emit()``'s global sequence remains the authoritative
    snapshot watermark; this observer assigns a separate, contiguous,
    connection-local sequence to every event actually delivered to this
    connection's client, seeded from the snapshot watermark at subscribe
    time. Invisible events (wrong epoch, or a capability-gated kind the
    connection never advertised) never advance that projected sequence.
    """

    def __init__(
        self, state: SessionState, epoch: int, capabilities: frozenset[str] = frozenset()
    ) -> None:
        self.state, self.epoch = state, epoch
        self.capabilities = frozenset(capabilities)
        self._unsubscribe: Callable[[], None] | None = None
        self._projected_sequence = 0

    @property
    def supports_work_status(self) -> bool:
        return "work_status_v1" in self.capabilities

    @property
    def projected_sequence(self) -> int:
        """The last connection-projected envelope sequence handed to the client.

        Read-only view of the projected counter so callers and tests can
        assert the snapshot/incremental namespace invariant -- after any
        snapshot install this must equal the ``snapshot_sequence`` stamped on
        the wire, and the next :meth:`project` returns that value plus one --
        without reaching into private state.
        """
        return self._projected_sequence

    def _visible(self, event: StateEvent) -> bool:
        if event.payload.get("origin_epoch") != self.epoch:
            return False
        if event.kind == "work_status" and not self.supports_work_status:
            return False
        return event.kind in _ALWAYS_VISIBLE_KINDS or event.kind == "work_status"

    def seed(self, sequence: int) -> None:
        """Install the connection-local projected sequence at a snapshot watermark.

        Called at attach time (before :meth:`subscribe`) **and again at every
        snapshot install**, seeded from the ``snapshot_sequence`` actually
        stamped on the outgoing snapshot frame. The re-seed is mandatory:
        the snapshot watermark comes from the global ``SessionState``
        sequence, which advances for events this connection never sees (a
        capability-gated ``work_status`` on a connection that did not
        advertise ``work_status_v1``, or a foreign-epoch event). Without it
        the projected counter drifts below the watermark the client installs
        as ``lastAppliedSequence``, and every later incremental is silently
        discarded by the browser reducer with no gap detected.

        The caller must not ``await`` between reading the stamped snapshot
        sequence and this call, so the install is atomic with respect to
        further state events.
        """
        self._projected_sequence = sequence

    def project(self, event: StateEvent) -> dict[str, Any] | None:
        """Return the typed projected event for a visible state event, or None."""
        if not self._visible(event):
            return None
        self._projected_sequence += 1
        return {
            "kind": event.kind,
            "data": event.payload,
            "origin_epoch": event.payload.get("origin_epoch", self.epoch),
            "sequence": self._projected_sequence,
        }

    def subscribe(self, emit: Callable[[Any], Any]) -> Callable[[], None]:
        """Forward future authoritative events as typed projected events.

        ``emit`` receives the typed dict from :meth:`project`, not a
        framework frame; ``server/app.py`` is responsible for handing that
        dict to ``RTVIMessagePublisher.incremental(...)``.
        """

        def on_event(event: StateEvent) -> None:
            projected = self.project(event)
            if projected is None:
                return
            result = emit(projected)
            # A connection emitter is normally a coroutine scheduled by the
            # transport callback. Do not make state mutation await a network send.
            if inspect.isawaitable(result):
                asyncio.create_task(result)

        self.unsubscribe()
        self._unsubscribe = self.state.subscribe(on_event)
        return self._unsubscribe

    def unsubscribe(self) -> None:
        if self._unsubscribe is not None:
            self._unsubscribe()
            self._unsubscribe = None

    def snapshot(self) -> RuntimeSnapshot:
        return self.state.snapshot(
            origin_epoch=self.epoch, include_work_status=self.supports_work_status
        )

    def _payload(self, event: StateEvent) -> dict[str, Any]:
        return {
            "contract_version": "v1.0",
            "session_id": self.state.session_id,
            "sequence": event.sequence,
            "kind": event.kind,
            "data": event.payload,
            "origin_epoch": (
                event.payload["origin_epoch"]
                if event.payload.get("origin_epoch") is not None
                else self.epoch
            ),
        }

    def messages(self, after_sequence: int = 0) -> tuple[dict[str, Any], ...]:
        """Diagnostic projected-event API; never a network serializer."""
        return tuple(
            self._payload(event)
            for event in self.state.events
            if event.sequence > after_sequence and self._visible(event)
        )
