"""Connection-scoped projection of authoritative session events."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .contracts import RuntimeSnapshot, resolve_work_status_wire_presence
from .frames import SnapshotBarrierFlushFrame
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
# Each predicate is the *gate* half, consulted by ``RuntimeObserver._visible``;
# membership in this mapping is the *admission* half. A gated kind is looked
# up here rather than hardcoded per-kind in ``_visible``, so a future gated
# kind added only here cannot be silently admitted ungated.
_CAPABILITY_GATED_KINDS: dict[str, Callable[[RuntimeObserver], bool]] = {
    "work_status": lambda observer: observer.supports_work_status,
}
_VISIBLE_KINDS = _ALWAYS_VISIBLE_KINDS | frozenset(_CAPABILITY_GATED_KINDS)

# Upper bound on how long `SnapshotBarrier.install_baseline` waits for its
# flush frame to be acknowledged. The plan requires the observer to own
# "timeout/error completion" and to fail closed when the writer cannot
# acknowledge: the frame only resolves its handle once it traverses the
# connection pipeline, so a worker cancelled or replaced between the write
# and the drain would otherwise never resolve it, leaving the observer
# paused forever with an unbounded buffer and the caller's snapshot lock
# held.
SNAPSHOT_BARRIER_ACK_TIMEOUT_SECONDS = 5.0


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
        # Keep deferred emit tasks referenced so they aren't garbage-collected
        # mid-flight (e.g. suspended inside worker.queue_frame); see _deliver.
        self._emit_tasks: set[asyncio.Task[Any]] = set()
        # SnapshotBarrier pause/replay state. Buffers raw StateEvents (not yet
        # projected) while a barrier owns the handoff between deciding to
        # (re)install a snapshot and actually installing its watermark.
        self._paused = False
        self._buffer: list[StateEvent] = []

    @property
    def supports_work_status(self) -> bool:
        return resolve_work_status_wire_presence(self.capabilities)

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
        gate = _CAPABILITY_GATED_KINDS.get(event.kind)
        if gate is not None and not gate(self):
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
        # The emitter check must precede project(): project() advances the
        # connection-projected sequence as a side effect, so projecting an
        # event that can never be emitted burns a sequence number and opens a
        # permanent gap between this counter and the client's
        # lastAppliedSequence.
        if self._emit is None:
            return
        projected = self.project(event)
        if projected is None:
            return
        result = self._emit(projected)
        # A connection emitter is normally a coroutine scheduled by the
        # transport callback. Do not make state mutation await a network send.
        if inspect.isawaitable(result):
            task = asyncio.create_task(result)
            self._emit_tasks.add(task)
            task.add_done_callback(self._emit_tasks.discard)

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
        # Teardown while a SnapshotBarrier is open would otherwise leave the
        # observer paused holding a buffer that can no longer be delivered
        # (the emitter is gone). A later subscribe() re-attaches the raw
        # listener but nothing resumes it, so the observer would stay mute
        # for the rest of its life. Detaching clears the pause/buffer state.
        self._paused = False
        self._buffer.clear()

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

    def _payload(self, event: StateEvent, sequence: int) -> dict[str, Any]:
        return {
            "contract_version": "v1.0",
            "session_id": self.state.session_id,
            "sequence": sequence,
            "kind": event.kind,
            "data": event.payload,
            "origin_epoch": (
                event.payload["origin_epoch"]
                if event.payload.get("origin_epoch") is not None
                else self.epoch
            ),
        }

    def messages(self, after_sequence: int = 0) -> tuple[dict[str, Any], ...]:
        """Diagnostic projected-event API; never a network serializer.

        Both the reported ``sequence`` and ``after_sequence`` are
        connection-projected, matching :meth:`project`: an invisible event does
        not advance the numbering, so a returned sequence can be fed straight
        back in without skipping the events that followed it.
        """
        projected: list[dict[str, Any]] = []
        sequence = 0
        for event in self.state.events:
            if not self._visible(event):
                continue
            sequence += 1
            if sequence > after_sequence:
                projected.append(self._payload(event, sequence))
        return tuple(projected)


class SnapshotBarrier:
    """Owns the ordering handoff when a snapshot is (re)installed for a connection.

    Pairs with ``SnapshotBarrierFlushFrame`` (`server/frames.py`):
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
        # True between subscribe_paused() and whichever settle path runs
        # first. Makes cancel() idempotent so a caller that also cancels
        # defensively in its own except/finally cannot replay a buffer twice
        # or unpause a barrier it no longer owns.
        self._open = False

    def subscribe_paused(self) -> None:
        self._open = True
        self.observer.pause()

    async def install_baseline(
        self,
        *,
        watermark: int,
        flush_writer: Callable[[Any], Any],
        snapshot_writer: Callable[[], Any],
        timeout: float | None = None,
    ) -> None:
        """Write the barrier frame, await its delivery, write the snapshot, replay.

        Ordering contract, in this exact order:

        1. push the barrier frame through the same serialized writer the
           incrementals use;
        2. wait (bounded by ``timeout``) for its acknowledgement, which proves
           every incremental queued before the barrier has already drained
           past the acknowledging processor;
        3. call ``snapshot_writer`` -- the frame that actually establishes the
           client's ``lastAppliedSequence`` watermark must be queued *before*
           any buffered event is replayed, or a replayed event reaches the
           client describing state it has no baseline to apply it against;
        4. only then reseed the observer and replay the buffer.

        Fails closed: any writer error, acknowledgement timeout, or
        cancellation aborts the install via :meth:`cancel` (no watermark
        change, buffer replayed) and re-raises, rather than leaving the
        observer paused with a growing buffer forever.
        """

        self._generation += 1
        acked: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        def acknowledge() -> None:
            if not acked.done():
                acked.set_result(None)

        frame = SnapshotBarrierFlushFrame(
            token=f"{self.state.session_id}-{watermark}-{self._generation}",
            acknowledge=acknowledge,
        )
        wait_seconds = SNAPSHOT_BARRIER_ACK_TIMEOUT_SECONDS if timeout is None else timeout
        try:
            await flush_writer(frame)
            await asyncio.wait_for(acked, wait_seconds)
            await snapshot_writer()
        except BaseException:
            acked.cancel()
            self.cancel()
            raise
        self._open = False
        self.observer.resume(watermark)

    def cancel(self) -> None:
        """Abort a paused install without a watermark change (e.g. snapshot not ready).

        Idempotent: only the first call after :meth:`subscribe_paused`
        resumes the observer.
        """
        if not self._open:
            return
        self._open = False
        self.observer.resume(None)
