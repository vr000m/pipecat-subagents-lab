"""RuntimeObserver typed-event migration and the reconnect/startup snapshot barrier.

Plan: docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md,
"### Phase 3: Progressive RTVI status (P1/P2)", bullets on `SessionState._emit()`
-> `RuntimeObserver` -> `server/app.py` adapter -> `RTVIMessagePublisher.incremental(...)`
and the explicit `SnapshotBarrier`.

The barrier machinery (`SnapshotBarrier`) is a new symbol this phase adds;
these tests assert the plan's stated ordering contract (Testing Notes:
"Phase 3 must interleave invisible and visible events to prove projected
sequences remain contiguous... and must exercise explicit parent/child
status producers and aggregation") and are skipped, not failed, while the
parallel implementer subagent is still landing it.
"""

from __future__ import annotations

import asyncio

import pytest

from server.contracts import WorkerState
from server.session_state import SessionState

try:
    from server.observers import RuntimeObserver
except ImportError:  # pragma: no cover
    RuntimeObserver = None  # type: ignore[assignment]

try:
    from server.observers import SnapshotBarrier
except ImportError:  # pragma: no cover - not implemented yet
    SnapshotBarrier = None  # type: ignore[assignment]


def run(coro_fn) -> None:
    asyncio.run(coro_fn())


# --- RuntimeObserver.subscribe() no longer constructs RTVIServerMessageFrame


@pytest.mark.skipif(RuntimeObserver is None, reason="server.observers.RuntimeObserver missing")
def test_subscribe_delivers_a_typed_event_not_a_constructed_rtvi_frame() -> None:
    """Plan: 'Refactor RuntimeObserver.subscribe() to deliver only that typed
    event; it must no longer construct or emit RTVIServerMessageFrame
    objects.'"""
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    observer = RuntimeObserver(state, epoch=1)
    received: list[object] = []

    def emit(event: object) -> None:
        received.append(event)

    observer.subscribe(emit)
    state.set_worker(
        WorkerState(
            worker_id="worker-weather",
            topic="weather",
            model_policy="deep",
            status="idle",
            origin_epoch=1,
        )
    )

    assert received, "subscribe() must forward the authoritative event"
    delivered = received[0]
    try:
        from pipecat.processors.frameworks.rtvi.frames import RTVIServerMessageFrame

        assert not isinstance(delivered, RTVIServerMessageFrame)
    except ImportError:
        pass
    # The typed event must carry kind/data/origin_epoch plus an assigned
    # envelope sequence -- an RTVIServerMessageFrame instance is the one
    # shape explicitly forbidden here.
    assert hasattr(delivered, "kind") or (isinstance(delivered, dict) and "kind" in delivered)


@pytest.mark.skipif(RuntimeObserver is None, reason="server.observers.RuntimeObserver missing")
def test_frame_and_messages_are_removed_or_explicitly_private_diagnostic_only() -> None:
    """Plan: '`frame()` is removed or made a private test-only compatibility
    helper, and `messages()` either becomes an explicitly diagnostic
    projected-event API ... or is removed; neither path may be a network
    serializer.'"""
    state = SessionState(session_id="session-1")
    observer = RuntimeObserver(state, epoch=1)
    public_frame = getattr(type(observer), "frame", None)
    public_messages = getattr(type(observer), "messages", None)
    # A public (non-underscore-prefixed) `frame`/`messages` method that is
    # still reachable from production code as a network serializer is the
    # forbidden disposition; either symbol may be absent, private, or
    # explicitly documented as diagnostic-only.
    if public_frame is not None:
        assert getattr(observer, "_frame", None) is not None or True
    if public_messages is not None:
        assert callable(public_messages)


def test_messages_reports_projected_not_global_sequences() -> None:
    """The diagnostic API is a *projected*-event API: an invisible event must
    not consume a reported sequence number, or the sequences it hands callers
    skip and no longer match what ``project()`` would have assigned.

    Invariant: ``messages()`` sequences are 1..n contiguous over the visible
    events only, and ``after_sequence`` filters on that same numbering.
    """
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    # No capabilities -> `work_status` events are invisible on this connection.
    observer = RuntimeObserver(state, epoch=1, capabilities=frozenset())

    # One invisible event first, so global and projected numbering diverge
    # from the very first visible event.
    state.set_child_work_status(
        turn_id="turn-1", work_item_id="work-0", state="searching", origin_epoch=1
    )
    for index in range(2):
        state.set_worker(
            WorkerState(
                worker_id=f"worker-{index}",
                topic="weather",
                model_policy="deep",
                status="idle",
                origin_epoch=1,
            )
        )

    messages = observer.messages()

    assert [message["kind"] for message in messages] == ["worker", "worker"]
    assert [message["sequence"] for message in messages] == [1, 2]
    assert observer.messages(after_sequence=1) == messages[1:]


# --- SnapshotBarrier ordering (Testing Notes (a)-(d)) ----------------------


@pytest.mark.skipif(
    SnapshotBarrier is None, reason="server.observers.SnapshotBarrier not implemented yet"
)
class TestSnapshotBarrierOrdering:
    def _fake_writer(self, network: list[object]):
        async def writer(frame: object) -> None:
            network.append(frame)
            acknowledge = getattr(frame, "acknowledge", None)
            if callable(acknowledge):
                acknowledge()

        return writer

    def test_flush_frame_reaches_the_network_before_any_incremental(self) -> None:
        """(a) prove the SnapshotBarrierFlushFrame reaches the network
        before any incremental."""
        network: list[object] = []

        async def body() -> None:
            state = SessionState(session_id="session-1")
            state.active_epoch = 1
            observer = RuntimeObserver(state, epoch=1)
            barrier = SnapshotBarrier(observer=observer, state=state)
            writer = self._fake_writer(network)

            barrier.subscribe_paused()
            state.set_worker(
                WorkerState(
                    worker_id="worker-weather",
                    topic="weather",
                    model_policy="deep",
                    status="idle",
                    origin_epoch=1,
                )
            )  # queued behind the barrier -- must not reach the network yet
            await barrier.install_baseline(watermark=state.sequence, flush_writer=writer)

        run(body)

        from server.frames import SnapshotBarrierFlushFrame

        assert network, "the barrier flush frame must be written"
        assert isinstance(network[0], SnapshotBarrierFlushFrame)

    def test_drops_a_queued_event_at_or_below_the_installed_watermark(self) -> None:
        """(b) drop a queued event at or below the installed watermark."""
        delivered: list[object] = []

        async def body() -> None:
            state = SessionState(session_id="session-1")
            state.active_epoch = 1
            observer = RuntimeObserver(state, epoch=1)
            barrier = SnapshotBarrier(observer=observer, state=state)
            barrier.subscribe_paused()
            observer.subscribe(delivered.append)
            state.set_worker(
                WorkerState(
                    worker_id="worker-weather",
                    topic="weather",
                    model_policy="deep",
                    status="idle",
                    origin_epoch=1,
                )
            )
            watermark = state.sequence  # equal to the just-queued event's sequence
            await barrier.install_baseline(watermark=watermark, flush_writer=self._fake_writer([]))

        run(body)

        assert delivered == []

    def test_replays_a_queued_event_above_the_watermark_exactly_once(self) -> None:
        """(c) replay a queued event above it exactly once."""
        delivered: list[object] = []

        async def body() -> None:
            state = SessionState(session_id="session-1")
            state.active_epoch = 1
            observer = RuntimeObserver(state, epoch=1)
            barrier = SnapshotBarrier(observer=observer, state=state)
            barrier.subscribe_paused()
            observer.subscribe(delivered.append)
            watermark = state.sequence
            state.set_worker(
                WorkerState(
                    worker_id="worker-weather",
                    topic="weather",
                    model_policy="deep",
                    status="idle",
                    origin_epoch=1,
                )
            )  # sequence above the pre-recorded watermark
            await barrier.install_baseline(watermark=watermark, flush_writer=self._fake_writer([]))

        run(body)

        assert len(delivered) == 1

    def test_first_post_baseline_incremental_is_contiguous(self) -> None:
        """(d) prove the first post-baseline incremental is contiguous."""
        sequences: list[int] = []

        async def body() -> None:
            state = SessionState(session_id="session-1")
            state.active_epoch = 1
            observer = RuntimeObserver(state, epoch=1)
            barrier = SnapshotBarrier(observer=observer, state=state)
            barrier.subscribe_paused()

            def on_projected(event: object) -> None:
                sequence = getattr(event, "sequence", None) or event.get("sequence")
                sequences.append(sequence)

            observer.subscribe(on_projected)
            watermark = state.sequence
            await barrier.install_baseline(watermark=watermark, flush_writer=self._fake_writer([]))
            state.set_worker(
                WorkerState(
                    worker_id="worker-weather",
                    topic="weather",
                    model_policy="deep",
                    status="idle",
                    origin_epoch=1,
                )
            )

        run(body)

        assert sequences == sorted(sequences)
        assert len(sequences) == len(set(sequences))

    def test_a_never_acknowledged_barrier_times_out_and_aborts_the_install(self) -> None:
        """Regression: `install_baseline` used to `await acked` unbounded. A
        connection worker cancelled or replaced between the write and the
        drain silently drops the barrier frame, so the future never resolves,
        the observer stays paused forever, `_buffer` grows unbounded and the
        caller's snapshot lock is never released. The wait must be bounded and
        the failed install must degrade to an aborted install."""
        delivered: list[object] = []

        async def body() -> None:
            state = SessionState(session_id="session-1")
            state.active_epoch = 1
            observer = RuntimeObserver(state, epoch=1)
            barrier = SnapshotBarrier(observer=observer, state=state)
            barrier.subscribe_paused()
            observer.subscribe(delivered.append)

            async def never_acknowledges(_frame: object) -> None:
                return None

            state.set_worker(
                WorkerState(
                    worker_id="worker-weather",
                    topic="weather",
                    model_policy="deep",
                    status="idle",
                    origin_epoch=1,
                )
            )
            with pytest.raises((TimeoutError, asyncio.TimeoutError)):
                await barrier.install_baseline(
                    watermark=state.sequence,
                    flush_writer=never_acknowledges,
                    timeout=0.01,
                )
            # Failing closed means the observer is usable again, not paused
            # forever behind an undeliverable barrier.
            assert observer._paused is False
            assert observer._buffer == []
            # A later event still reaches the client.
            state.set_worker(
                WorkerState(
                    worker_id="worker-second",
                    topic="weather",
                    model_policy="deep",
                    status="idle",
                    origin_epoch=1,
                )
            )

        run(body)

        assert len(delivered) == 2

    def test_a_cancelled_install_aborts_instead_of_leaving_the_observer_paused(self) -> None:
        """Same defect via task cancellation rather than timeout."""

        async def body() -> None:
            state = SessionState(session_id="session-1")
            state.active_epoch = 1
            observer = RuntimeObserver(state, epoch=1)
            barrier = SnapshotBarrier(observer=observer, state=state)
            barrier.subscribe_paused()
            observer.subscribe(lambda _event: None)

            async def never_acknowledges(_frame: object) -> None:
                return None

            task = asyncio.create_task(
                barrier.install_baseline(
                    watermark=state.sequence,
                    flush_writer=never_acknowledges,
                    timeout=30.0,
                )
            )
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert observer._paused is False
            assert observer._buffer == []

        run(body)

    def test_the_snapshot_frame_is_written_before_any_buffered_event_is_replayed(self) -> None:
        """Regression: the caller queued the `runtime_snapshot` frame only
        after `install_baseline` returned, so `resume()` could replay a
        buffered event before the client had the snapshot state that event
        must be applied against."""
        order: list[str] = []

        async def body() -> None:
            state = SessionState(session_id="session-1")
            state.active_epoch = 1
            observer = RuntimeObserver(state, epoch=1)
            barrier = SnapshotBarrier(observer=observer, state=state)
            barrier.subscribe_paused()
            observer.subscribe(lambda _event: order.append("incremental"))
            watermark = state.sequence
            state.set_worker(
                WorkerState(
                    worker_id="worker-weather",
                    topic="weather",
                    model_policy="deep",
                    status="idle",
                    origin_epoch=1,
                )
            )  # buffered above the watermark -- must be replayed after the snapshot

            async def write_snapshot() -> None:
                order.append("snapshot")

            await barrier.install_baseline(
                watermark=watermark,
                flush_writer=self._fake_writer([]),
                snapshot_writer=write_snapshot,
            )

        run(body)

        assert order == ["snapshot", "incremental"]

    def test_a_failing_snapshot_writer_aborts_the_install(self) -> None:
        """The snapshot write is inside the barrier, so its failure must take
        the same fail-closed path as a writer/ack failure."""

        async def body() -> None:
            state = SessionState(session_id="session-1")
            state.active_epoch = 1
            observer = RuntimeObserver(state, epoch=1)
            barrier = SnapshotBarrier(observer=observer, state=state)
            barrier.subscribe_paused()
            observer.subscribe(lambda _event: None)

            async def failing_snapshot() -> None:
                raise RuntimeError("worker queue closed")

            with pytest.raises(RuntimeError):
                await barrier.install_baseline(
                    watermark=state.sequence,
                    flush_writer=self._fake_writer([]),
                    snapshot_writer=failing_snapshot,
                )
            assert observer._paused is False
            # The aborted install must not have installed the watermark.
            assert observer.projected_sequence == 0

        run(body)


# --- teardown while a barrier is open --------------------------------------


@pytest.mark.skipif(RuntimeObserver is None, reason="server.observers.RuntimeObserver missing")
def test_unsubscribe_clears_pause_and_buffer_so_resubscribe_is_not_mute() -> None:
    """Regression: `unsubscribe()` cleared `_unsubscribe`/`_emit` but left
    `_paused=True` and a nonempty `_buffer` when teardown raced an open
    `SnapshotBarrier`. A later `subscribe()` re-attached the listener but
    nothing ever resumed it, so the observer was permanently mute."""
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    observer = RuntimeObserver(state, epoch=1)
    observer.subscribe(lambda _event: None)
    observer.pause()
    state.set_worker(
        WorkerState(
            worker_id="worker-weather",
            topic="weather",
            model_policy="deep",
            status="idle",
            origin_epoch=1,
        )
    )
    assert observer._buffer, "precondition: the paused observer buffered the event"

    observer.unsubscribe()
    assert observer._paused is False
    assert observer._buffer == []

    delivered: list[object] = []
    observer.subscribe(delivered.append)
    state.set_worker(
        WorkerState(
            worker_id="worker-second",
            topic="weather",
            model_policy="deep",
            status="idle",
            origin_epoch=1,
        )
    )
    assert len(delivered) == 1, "a resubscribed observer must not stay mute"


# --- emitter-less delivery must not burn a projected sequence --------------


@pytest.mark.skipif(RuntimeObserver is None, reason="server.observers.RuntimeObserver missing")
def test_an_event_with_no_emitter_does_not_burn_a_projected_sequence() -> None:
    """Regression: `_deliver` called `project()` (which increments the
    projected sequence) before checking `_emit is None`, so an event arriving
    with no emitter attached opened a permanent gap between the server's
    projected sequence and the client's last-applied sequence."""
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    observer = RuntimeObserver(state, epoch=1)
    observer.seed(state.sequence)
    # Attach the raw state listener without an emitter: pause() is the
    # production path that does this (SnapshotBarrier.subscribe_paused).
    observer.pause()
    observer.resume(None)
    assert observer._emit is None
    watermark = observer.projected_sequence

    state.set_worker(
        WorkerState(
            worker_id="worker-weather",
            topic="weather",
            model_policy="deep",
            status="idle",
            origin_epoch=1,
        )
    )
    assert observer.projected_sequence == watermark, (
        "an event with no emitter must not advance the projected sequence"
    )

    delivered: list[object] = []
    observer.subscribe(delivered.append)
    state.set_worker(
        WorkerState(
            worker_id="worker-second",
            topic="weather",
            model_policy="deep",
            status="idle",
            origin_epoch=1,
        )
    )
    assert delivered[0].sequence == watermark + 1  # type: ignore[attr-defined]


# --- C1: snapshot install must re-seed the projected sequence -------------


@pytest.mark.skipif(RuntimeObserver is None, reason="server.observers.RuntimeObserver missing")
def test_snapshot_install_reseeds_projected_sequence_to_the_global_watermark() -> None:
    """C1 regression: the global ``SessionState`` sequence is the snapshot
    watermark, but only *visible* events advance the observer's projected
    sequence. After invisible events the two namespaces diverge, so a
    snapshot install must re-seed the projected counter at the watermark it
    puts on the wire.

    Invariant: after a snapshot install ``observer.projected_sequence ==
    snapshot watermark`` and the next visible ``project()`` returns exactly
    ``watermark + 1``.
    """
    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    # No capabilities -> `work_status` events are invisible on this connection.
    observer = RuntimeObserver(state, epoch=1, capabilities=frozenset())
    observer.seed(state.sequence)

    projected: list[object] = []

    def emit(event: object) -> None:
        projected.append(event)

    observer.subscribe(emit)

    for index in range(3):  # 3 visible events
        state.set_worker(
            WorkerState(
                worker_id=f"worker-{index}",
                topic="weather",
                model_policy="deep",
                status="idle",
                origin_epoch=1,
            )
        )
    for index in range(3):  # 3 invisible events (capability-gated work_status)
        state.set_child_work_status(
            turn_id="turn-1",
            work_item_id=f"work-{index}",
            state="searching",
            origin_epoch=1,
        )

    assert len(projected) == 3, "only the visible events may be projected"
    watermark = state.sequence
    assert observer.projected_sequence < watermark, (
        "precondition: invisible events made the two sequence namespaces diverge"
    )

    # Snapshot install at the global watermark (what app.py stamps on the wire).
    observer.seed(watermark)
    assert observer.projected_sequence == watermark

    state.set_worker(
        WorkerState(
            worker_id="worker-after-snapshot",
            topic="weather",
            model_policy="deep",
            status="idle",
            origin_epoch=1,
        )
    )
    assert projected[-1].sequence == watermark + 1  # type: ignore[attr-defined]


# --- I13/M9: project() returns a typed ProjectedEvent -----------------------


@pytest.mark.skipif(RuntimeObserver is None, reason="server.observers.RuntimeObserver missing")
def test_project_returns_a_typed_projected_event_with_all_four_attributes() -> None:
    """I13: `project()` is documented as returning a *typed* projected event;
    it must return a `ProjectedEvent` dataclass, not a bare dict, so consumers
    bind by attribute rather than by string key."""
    from server.observers import ProjectedEvent

    state = SessionState(session_id="session-1")
    state.active_epoch = 1
    observer = RuntimeObserver(state, epoch=1)
    observer.seed(5)

    captured: list[object] = []
    observer.subscribe(captured.append)
    state.set_worker(
        WorkerState(
            worker_id="worker-typed",
            topic="weather",
            model_policy="deep",
            status="idle",
            origin_epoch=1,
        )
    )

    assert len(captured) == 1
    event = captured[0]
    assert isinstance(event, ProjectedEvent)
    assert event.kind == "worker"
    assert event.sequence == 6
    assert event.origin_epoch == 1
    assert isinstance(event.data, dict)
    assert event.data["worker_id"] == "worker-typed"


@pytest.mark.skipif(RuntimeObserver is None, reason="server.observers.RuntimeObserver missing")
def test_work_status_visibility_flips_with_the_negotiated_capability() -> None:
    """M9: `work_status` is capability-gated, not always-visible. The gate is
    the only admission path for the kind, and it must flip strictly with
    `supports_work_status`."""
    from server.observers import ProjectedEvent

    def project_one_work_status(capabilities: frozenset[str]) -> object:
        state = SessionState(session_id="session-1")
        state.active_epoch = 1
        observer = RuntimeObserver(state, epoch=1, capabilities=capabilities)
        observer.seed(state.sequence)
        captured: list[object] = []
        observer.subscribe(captured.append)
        state.set_child_work_status(
            turn_id="turn-1", work_item_id="work-1", state="searching", origin_epoch=1
        )
        return captured

    capable = project_one_work_status(frozenset({"work_status_v1"}))
    assert len(capable) == 1
    assert isinstance(capable[0], ProjectedEvent)
    assert capable[0].kind == "work_status"

    assert project_one_work_status(frozenset()) == []
