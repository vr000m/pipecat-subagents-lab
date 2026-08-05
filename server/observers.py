"""Connection-scoped projection of authoritative session events."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .contracts import WORK_STATUS_V1, RuntimeSnapshot
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
# Kinds admitted only when the connection negotiated the matching capability.
# Membership here is the *admission* half; the per-kind capability check in
# ``RuntimeObserver._visible`` is the *gate* half. A kind must appear in
# exactly one of these two sets.
_CAPABILITY_GATED_KINDS = frozenset({"work_status"})
_VISIBLE_KINDS = _ALWAYS_VISIBLE_KINDS | _CAPABILITY_GATED_KINDS


@dataclass(frozen=True)
class ProjectedEvent:
    """One authoritative state event as projected onto a single connection.

    ``sequence`` is the connection-local projected sequence assigned by
    :meth:`RuntimeObserver.project`, not the global ``SessionState``
    sequence; ``server/app.py`` hands it straight to
    ``RTVIMessagePublisher.incremental(...)``, which serializes it verbatim.
    """

    kind: str
    data: dict[str, Any]
    sequence: int
    origin_epoch: int


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
        self._emit: Callable[[Any], Any] | None = None
        # SnapshotBarrier pause/replay state. Buffers raw StateEvents (not yet
        # projected) while a barrier owns the handoff between deciding to
        # (re)install a snapshot and actually installing its watermark.
        self._paused = False
        self._buffer: list[StateEvent] = []

    @property
    def supports_work_status(self) -> bool:
        return WORK_STATUS_V1 in self.capabilities

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
        return event.kind in _VISIBLE_KINDS

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

    def project(self, event: StateEvent) -> ProjectedEvent | None:
        """Return the typed projected event for a visible state event, or None."""
        if not self._visible(event):
            return None
        self._projected_sequence += 1
        return ProjectedEvent(
            kind=event.kind,
            data=event.payload,
            sequence=self._projected_sequence,
            origin_epoch=event.payload.get("origin_epoch", self.epoch),
        )

    def _deliver(self, event: StateEvent) -> None:
        projected = self.project(event)
        if projected is None or self._emit is None:
            return
        result = self._emit(projected)
        # A connection emitter is normally a coroutine scheduled by the
        # transport callback. Do not make state mutation await a network send.
        if inspect.isawaitable(result):
            asyncio.create_task(result)

    def _on_event(self, event: StateEvent) -> None:
        if self._paused:
            self._buffer.append(event)
            return
        self._deliver(event)

    def subscribe(self, emit: Callable[[Any], Any]) -> Callable[[], None]:
        """Forward future authoritative events as typed projected events.

        ``emit`` receives the :class:`ProjectedEvent` from :meth:`project`,
        not a framework frame; ``server/app.py`` is responsible for handing
        that event to ``RTVIMessagePublisher.incremental(...)``.

        Idempotent with respect to the underlying ``SessionState``
        subscription: a :class:`SnapshotBarrier` may already have attached
        the raw listener via :meth:`pause`, in which case this only swaps
        the emit target, so a paused observer stays paused across a
        ``subscribe()`` call made while a barrier owns the handoff.
        """
        self._emit = emit
        if self._unsubscribe is None:
            self._unsubscribe = self.state.subscribe(self._on_event)
        return self.unsubscribe

    def unsubscribe(self) -> None:
        if self._unsubscribe is not None:
            self._unsubscribe()
            self._unsubscribe = None
        self._emit = None

    def pause(self) -> None:
        """Buffer subsequent state events instead of projecting/emitting them.

        The sole caller is :class:`SnapshotBarrier`. Buffering (rather than
        continuing to let ``_on_event`` dispatch through ``asyncio.create_task``)
        is what actually closes the race: a task already scheduled before
        the pause is free to land on either side of the barrier frame, but
        no event captured *after* this call can reach the network ahead of
        the barrier, because it never gets a chance to be scheduled at all
        until :meth:`resume` replays it.
        """
        self._paused = True
        if self._unsubscribe is None:
            self._unsubscribe = self.state.subscribe(self._on_event)

    def resume(self, watermark: int | None = None) -> None:
        """Unpause, optionally reseeding at ``watermark`` and replaying the buffer.

        Buffered events at or below ``watermark`` are dropped (already
        represented by the snapshot); events above it are delivered exactly
        once, in order, before any newly-arriving event is dispatched.
        ``watermark=None`` (an aborted snapshot install) replays every
        buffered event without dropping or reseeding anything.
        """
        self._paused = False
        if watermark is not None:
            self.seed(watermark)
        buffered, self._buffer = self._buffer, []
        for event in buffered:
            if watermark is not None and event.sequence <= watermark:
                continue
            self._deliver(event)

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


class SnapshotBarrier:
    """Owns the ordering handoff when a snapshot is (re)installed for a connection.

    Pairs with ``SnapshotBarrierFlushFrame`` (`server/speech_lifecycle.py`):
    :meth:`subscribe_paused` pauses ``observer`` synchronously so no event
    captured after that point can be dispatched ahead of the barrier, then
    :meth:`install_baseline` pushes the flush frame through the same
    serialized writer incrementals use and only reseeds/replays the buffer
    once that frame's ``acknowledge`` callback proves it actually reached
    the network.
    """

    def __init__(self, observer: RuntimeObserver, state: SessionState) -> None:
        self.observer = observer
        self.state = state
        self._generation = 0

    def subscribe_paused(self) -> None:
        self.observer.pause()

    async def install_baseline(self, *, watermark: int, flush_writer: Callable[[Any], Any]) -> None:
        """Write the barrier frame, await its delivery, then reseed and replay."""
        from .speech_lifecycle import SnapshotBarrierFlushFrame

        self._generation += 1
        acked: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        def acknowledge() -> None:
            if not acked.done():
                acked.set_result(None)

        frame = SnapshotBarrierFlushFrame(
            token=f"{self.state.session_id}-{watermark}-{self._generation}",
            acknowledge=acknowledge,
        )
        await flush_writer(frame)
        await acked
        self.observer.resume(watermark)

    def cancel(self) -> None:
        """Abort a paused install without a watermark change (e.g. snapshot not ready)."""
        self.observer.resume(None)
