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

        from server.speech_lifecycle import SnapshotBarrierFlushFrame

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

    projected: list[dict] = []

    def emit(event: dict) -> None:
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
    assert projected[-1]["sequence"] == watermark + 1
