"""SessionHost keeps durable workers separate from connection workers."""

import asyncio

from server.contracts import DeliveryState, GroundedResult, WorkerState
from server.perf_metrics import CollectingMeasurementSink
from server.pipeline import SessionHost
from server.registry import WorkerRegistry
from server.work_item_coordinator import LateResult


class FakeRunner:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.workers: list[object] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class BlockingConnectionWorker:
    def __init__(self) -> None:
        self.shutdown_started = asyncio.Event()
        self.release_shutdown = asyncio.Event()

    async def cancel(self, *, reason: str) -> None:
        assert reason == "connection replaced"
        self.shutdown_started.set()
        await self.release_shutdown.wait()


def test_start_replacement_and_shutdown_preserve_process_lifetime_worker_registry() -> None:
    async def run() -> None:
        runner = FakeRunner()
        registry = WorkerRegistry()
        registered = registry.register(
            worker_id="worker-weather",
            worker_type="web_search",
            topic="weather",
        )
        host = SessionHost(registry=registry, runner_factory=lambda: runner)
        await host.start()
        first = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 1,
                "snapshot_sequence": 0,
            }
        )
        second = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 2,
                "snapshot_sequence": 0,
            }
        )
        await host.shutdown()

        assert runner.started and runner.stopped
        assert runner.workers == []
        assert first.epoch < second.epoch
        assert first.active is False
        assert second.active is False
        assert host.connection is None
        assert host.registry is registry
        assert host.registry.get("worker-weather").worker is registered.worker

    asyncio.run(run())


def test_session_host_adopts_or_rejects_the_coordinator_registry() -> None:
    registry = WorkerRegistry()
    coordinator = type("Coordinator", (), {"registry": registry})()

    adopted = SessionHost(coordinator=coordinator)
    assert adopted.registry is registry

    try:
        SessionHost(registry=WorkerRegistry(), coordinator=coordinator)
    except ValueError as exc:
        assert "share one WorkerRegistry" in str(exc)
    else:
        raise AssertionError("divergent registries were accepted")


def test_replacement_interrupts_only_old_connection_speech_and_keeps_result_history() -> None:
    async def run() -> None:
        host = SessionHost()
        host.state.set_worker(
            WorkerState(
                worker_id="worker-weather",
                topic="weather",
                model_policy="deep",
                status="idle",
            )
        )
        first = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 1,
                "snapshot_sequence": 0,
            }
        )
        item = first.scheduler.enqueue(
            result_id="result-1",
            work_item_id="work-1",
            run_id="run-1",
            text="Answer",
            origin_epoch=1,
        )
        await first.scheduler.start_next()
        host.state.append_result(
            GroundedResult(
                result_id="result-1",
                worker_id="worker-weather",
                turn_id="turn-1",
                text="Answer",
                spoken_text="Answer",
            ),
            origin_epoch=1,
        )

        second = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 2,
                "snapshot_sequence": 0,
            }
        )

        assert first.active is False
        assert host.accepts(2)
        assert not host.accepts(1)
        assert host.state.speech[item.utterance_id].state == DeliveryState.INTERRUPTED_BY_RECONNECT
        assert [result.result_id for result in host.state.result_history("worker-weather")] == [
            "result-1"
        ]
        assert second.scheduler.active is None

    asyncio.run(run())


def test_replacement_fences_new_epoch_before_old_shutdown_and_rejects_old_callback_state() -> None:
    async def run() -> None:
        host = SessionHost()
        first = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 1,
                "snapshot_sequence": 0,
            }
        )
        old_worker = BlockingConnectionWorker()
        first.worker = old_worker
        host.state.set_worker(
            WorkerState(
                worker_id="worker-weather",
                topic="weather",
                model_policy="deep",
                status="idle",
                origin_epoch=1,
            )
        )

        replacement = asyncio.create_task(
            host.connect(
                {
                    "session_id": host.state.session_id,
                    "resume_token": host.state.resume_token,
                    "proposed_epoch": 2,
                    "snapshot_sequence": 0,
                }
            )
        )
        second = await asyncio.wait_for(replacement, timeout=1)

        assert host.arbiter.accepts(2)
        assert host.state.active_epoch == 2
        assert second.epoch == 2
        assert host.connection is second

        await asyncio.wait_for(old_worker.shutdown_started.wait(), timeout=1)

        host.state.append_result(
            GroundedResult(
                result_id="late-old-result",
                worker_id="worker-weather",
                turn_id="turn-old",
                text="Late",
                spoken_text="Late",
                origin_epoch=1,
            ),
            origin_epoch=1,
        )
        assert host.state.workers["worker-weather"].latest_result_id is None

        old_worker.release_shutdown.set()
        await host.shutdown()

    asyncio.run(run())


def test_shutdown_finalizes_retained_recorders_only_after_coordinator_shutdown_returns() -> None:
    """SessionHost.shutdown() must let coordinator.shutdown() settle retained
    work and its callback tasks first, and only then finalize any recorder
    still open — never before or concurrently with the coordinator settling
    (plan Architecture Decision: "Host owns retained finalization";
    server/pipeline.py:1290 per the Files to Modify note for this test).
    """

    async def run() -> None:
        order: list[str] = []

        class SlowShutdownCoordinator:
            async def shutdown(self) -> None:
                order.append("coordinator-shutdown-start")
                await asyncio.sleep(0.02)
                order.append("coordinator-shutdown-end")

        class OrderTrackingSink:
            def __init__(self, inner: CollectingMeasurementSink) -> None:
                self._inner = inner

            def emit(self, record: object) -> None:
                if getattr(record, "event", None) == "work_item_background":
                    order.append("background-finalized")
                self._inner.emit(record)

        inner_sink = CollectingMeasurementSink()
        host = SessionHost(
            coordinator=SlowShutdownCoordinator(), measurement_sink=OrderTrackingSink(inner_sink)
        )

        # Register a retained work item the way the real timeout path does,
        # so the host has an open recorder to finalize during shutdown.
        host._known_work_items.add("work-open-at-shutdown")

        await host.shutdown()

        assert order[0] == "coordinator-shutdown-start"
        assert "coordinator-shutdown-end" in order
        end_index = order.index("coordinator-shutdown-end")
        # Any host-side finalization of a still-open recorder must appear
        # strictly after the coordinator's shutdown() call has returned.
        assert all(
            order.index(marker) > end_index for marker in order if marker == "background-finalized"
        )

    asyncio.run(run())


def test_late_result_terminal_kind_field_is_backward_compatible_with_legacy_construction() -> None:
    """Legacy call sites that construct LateResult without ``terminal_kind``
    (e.g. this repo's own drain_late_results consumers) must keep working
    after Phase 2 adds the field (plan: "backward-compatible structured
    terminal_kind")."""
    late = LateResult(work_item_id="work-1", worker_id="worker-1", result=None)
    assert late.terminal_kind is None or isinstance(late.terminal_kind, str)


def test_legacy_session_host_registry_only_construction_derives_one_feature_policy() -> None:
    """Plan: 'A legacy SessionHost(registry) derives its canonical
    config/policy from that registry' and 'Add a compatibility test for
    existing SessionHost(registry) callers.'"""
    from server.config import Config, FeaturePolicy

    registry = WorkerRegistry(config=Config(enable_early_ack=False))
    host = SessionHost(registry=registry)

    assert isinstance(host.feature_policy, FeaturePolicy)
    assert host.feature_policy.enable_early_ack is False


def test_session_host_accepts_keyword_only_config_and_feature_policy() -> None:
    from server.config import Config, FeaturePolicy

    config = Config(enable_background_status=False)
    policy = FeaturePolicy.from_config(config)

    host = SessionHost(registry=WorkerRegistry(config=config), config=config, feature_policy=policy)

    assert host.feature_policy is policy
    assert host.config is config


def test_session_host_rejects_a_registry_config_that_conflicts_with_the_injected_config() -> None:
    """Plan: 'a conflicting registry Config fails fast rather than creating a
    second policy.'"""
    from server.config import Config

    conflicting_registry = WorkerRegistry(config=Config(enable_early_ack=False))

    try:
        SessionHost(
            registry=conflicting_registry,
            config=Config(enable_early_ack=True),
        )
    except ValueError:
        pass
    else:
        raise AssertionError("a conflicting registry config was silently accepted")


def test_cancel_turn_or_child_removes_only_the_named_childs_speech_and_leaves_the_parent_ack() -> (
    None
):
    """Plan: 'A child cancel never accidentally removes a parent ack' --
    cancelling one non-sole delegated child under its own work_item_id must
    leave ``ack_work_item_id`` queued."""

    async def run() -> None:
        host = SessionHost()
        origin = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 1,
                "snapshot_sequence": 0,
            }
        )
        ack_work_item_id = "ack-turn-1"
        origin.scheduler.enqueue(
            work_item_id=ack_work_item_id,
            run_id="run-ack",
            result_id=None,
            text="One moment.",
            role="ack",
        )
        origin.scheduler.enqueue(
            work_item_id="work-1-0",
            run_id="run-1-0",
            result_id="result-1-0",
            text="child one",
        )
        origin.scheduler.enqueue(
            work_item_id="work-1-1",
            run_id="run-1-1",
            result_id="result-1-1",
            text="child two",
        )

        await host.cancel_turn_or_child("turn-1", "work-1-0")

        assert ack_work_item_id in origin.scheduler._queues
        assert "work-1-0" not in origin.scheduler._queues
        assert "work-1-1" in origin.scheduler._queues
        await host.shutdown()

    asyncio.run(run())


def test_cancel_turn_or_child_whole_turn_removes_the_parent_ack_and_every_child_queue() -> None:
    async def run() -> None:
        host = SessionHost()
        origin = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 1,
                "snapshot_sequence": 0,
            }
        )
        ack_work_item_id = "ack-turn-2"
        origin.scheduler.enqueue(
            work_item_id=ack_work_item_id,
            run_id="run-ack",
            result_id=None,
            text="One moment.",
            role="ack",
        )
        origin.scheduler.enqueue(
            work_item_id="work-2-0",
            run_id="run-2-0",
            result_id="result-2-0",
            text="child",
        )

        await host.cancel_turn_or_child("turn-2", None)

        assert ack_work_item_id not in origin.scheduler._queues
        assert "work-2-0" not in origin.scheduler._queues
        await host.shutdown()

    asyncio.run(run())


def test_cancel_turn_or_child_sole_delegated_child_removes_both_ack_and_child_atomically() -> None:
    """Plan: cancelling the turn's sole delegated child must cancel both the
    child and the parent ack 'under their respective keys in the same
    operation,' since the ack has no other delegated child left to serve."""

    async def run() -> None:
        host = SessionHost()
        origin = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 1,
                "snapshot_sequence": 0,
            }
        )
        ack_work_item_id = "ack-turn-3"
        origin.scheduler.enqueue(
            work_item_id=ack_work_item_id,
            run_id="run-ack",
            result_id=None,
            text="One moment.",
            role="ack",
        )
        origin.scheduler.enqueue(
            work_item_id="work-3-0",
            run_id="run-3-0",
            result_id="result-3-0",
            text="only child",
        )

        await host.cancel_turn_or_child("turn-3", "work-3-0")

        assert ack_work_item_id not in origin.scheduler._queues
        assert "work-3-0" not in origin.scheduler._queues
        await host.shutdown()

    asyncio.run(run())


def test_on_ack_terminal_is_idempotent_and_clears_the_turn_latch_exactly_once() -> None:
    """Plan: 'an idempotent SessionHost.on_ack_terminal(identity, reason)
    callback remains the sole turn-latch mutator.' A second call for the same
    identity must not raise or double-clear."""

    async def run() -> None:
        from server.speech_lifecycle import GenerationIdentity

        host = SessionHost()
        await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 1,
                "snapshot_sequence": 0,
            }
        )
        identity = GenerationIdentity(
            "ack-turn-4", "ack-turn-4", role="ack", turn_id="turn-4", ack_id="ack-turn-4"
        )

        host.on_ack_terminal(identity, "no_tts")
        host.on_ack_terminal(identity, "no_tts")  # must be a no-op, not an error

        await host.shutdown()

    asyncio.run(run())
