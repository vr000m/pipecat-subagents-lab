"""SessionHost keeps durable workers separate from connection workers."""

import asyncio

import pytest

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
        # A speakable connection: with no TTS lane at all, speech now
        # terminalizes before admission (nothing could ever hand it to a
        # provider), so there would be no active utterance for the
        # replacement to interrupt.
        host = SessionHost(tts=object())
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
        first.worker = _FakeLateResultWorker()
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
        host._work_ledger.known_ids.add("work-open-at-shutdown")

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


def test_session_host_rejects_a_coordinator_config_that_conflicts_with_the_host_config() -> None:
    """The coordinator is a second config holder; a divergent one is a
    split-brain (its ``Config`` used to drive foreground/shutdown timeouts
    while every other switch came from ``self.config``). Construction must
    fail fast instead."""
    from server.config import Config

    config = Config(enable_early_ack=True)
    coordinator = type("Coordinator", (), {"config": Config(enable_early_ack=False)})()

    with pytest.raises(ValueError, match="coordinator"):
        SessionHost(
            registry=WorkerRegistry(config=config),
            config=config,
            coordinator=coordinator,
        )


def test_session_host_accepts_a_coordinator_whose_config_matches() -> None:
    from server.config import Config

    config = Config(enable_early_ack=False)
    coordinator = type("Coordinator", (), {"config": Config(enable_early_ack=False)})()

    host = SessionHost(
        registry=WorkerRegistry(config=config),
        config=config,
        coordinator=coordinator,
    )

    assert host.config == config
    assert host.coordinator is coordinator


def test_session_host_accepts_a_coordinator_whose_config_only_diverges_via_its_own_overrides() -> (
    None
):
    """WorkItemCoordinator's constructor is allowed to layer
    max_work_items_per_turn/multi_intent_wait_timeout_ms overrides onto
    whatever Config it was handed (see test_work_item_coordinator.py's
    test_constructor_overrides_preserve_pending_dialogue_timeout). Passing
    that same original config to SessionHost must not be treated as a
    conflict just because those two coordinator-owned fields differ."""
    from server.config import Config
    from server.work_item_coordinator import WorkItemCoordinator

    config = Config(pending_dialogue_timeout_seconds=45)
    coordinator = WorkItemCoordinator(config=config, max_work_items_per_turn=3)

    host = SessionHost(
        registry=WorkerRegistry(config=config),
        config=config,
        coordinator=coordinator,
    )

    assert host.config == config
    assert host.coordinator.config.max_work_items_per_turn == 3


def test_session_host_accepts_a_coordinator_without_any_config_attribute() -> None:
    from server.config import Config

    coordinator = type("Coordinator", (), {})()
    config = Config(enable_early_ack=False)

    host = SessionHost(
        registry=WorkerRegistry(config=config),
        config=config,
        coordinator=coordinator,
    )

    assert host.config is config


def test_shutdown_grace_period_comes_from_the_host_config_not_the_coordinator() -> None:
    """``shutdown_grace_seconds`` used to be read off ``coordinator.config``.
    A coordinator with no config of its own must still honour the host's
    configured grace period rather than silently falling back to the
    hard-coded 2.0s default."""

    async def run() -> None:
        from server.config import Config

        config = Config(shutdown_grace_seconds=0.01)
        coordinator = type("Coordinator", (), {})()
        host = SessionHost(
            registry=WorkerRegistry(config=config),
            config=config,
            coordinator=coordinator,
        )

        async def never_finishes() -> None:
            await asyncio.Event().wait()

        hanging = asyncio.create_task(never_finishes())
        host._background_shutdowns.add(hanging)

        started = asyncio.get_running_loop().time()
        await host.shutdown()
        elapsed = asyncio.get_running_loop().time() - started

        # The 2.0s fallback would blow straight through this bound.
        assert elapsed < 1.0
        # Let the cancellation requested by shutdown() actually land.
        for _ in range(5):
            if hanging.done():
                break
            await asyncio.sleep(0)
        assert hanging.cancelled()

    asyncio.run(run())


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
        # Ack ownership and the sole-child decision are answered from the
        # turn -> delegated-child registry, so this hand-built turn registers
        # its children exactly as a real turn handler would.
        host._register_turn_work_item("turn-1", "work-1-0")
        host._register_turn_work_item("turn-1", "work-1-1")

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


# -- Phase 2: SessionHost.commit_late_result_once() ------------------------
#
# Plan bullets 195-205: the sole host-owned atomic API for late-result
# callbacks. It commits every valid result exactly once and separately
# computes the autoplay/display-only delivery disposition; cancellation,
# staleness, and duplication continue to suppress commit entirely.
# ``commit_late_result_once`` returns nothing (it is a coordinator-callback
# entry point, not a query), so these tests observe the two things the plan
# actually promises: (1) `host.state.result_history(...)` for exactly-once
# commit, and (2) `origin.scheduler.active` for whether the committed result
# was additionally admitted for speech (autoplay) or left uncommitted-to-
# speech (display-only). Without a manifest, `enable_autoplay_policy=True`
# always fails closed to display-only (`_late_result_disposition`: "manifest
# is None or not manifest.promotion_eligible: return display_only") -- so a
# `PromotionManifest(promotion_eligible=True)` is passed explicitly wherever
# a test needs to reach the autoplay branch.


class _FakeLateResultWorker:
    def __init__(self) -> None:
        self.frames: list[object] = []

    async def queue_frame(self, frame: object) -> None:
        self.frames.append(frame)


def _late_delivery_context(host: SessionHost, **overrides: object):
    try:
        from server.pipeline import LateDeliveryContext
    except ImportError:
        pytest.skip("LateDeliveryContext not yet implemented (Phase 2 concurrent implementer)")
    fields = {
        "turn_id": "turn-late-1",
        "work_item_id": "work-late-1",
        "origin_epoch": 1,
        "ack_timestamp": None,
        "accepted_turn_sequence": host._turn_sequence,
    }
    fields.update(overrides)
    return LateDeliveryContext(**fields)


def _grounded_result(**overrides: object) -> GroundedResult:
    fields = {
        "result_id": "result-late-1",
        "worker_id": "worker-weather",
        "turn_id": "turn-late-1",
        "text": "Late answer",
        "spoken_text": "Late answer",
        "origin_epoch": 1,
    }
    fields.update(overrides)
    return GroundedResult(**fields)


async def _connected_host(
    *,
    enable_autoplay_policy: bool = True,
    promotion_manifest: object | None = None,
    speakable: bool = False,
    measurement_sink: object | None = None,
):
    from server.config import Config

    config = Config(enable_autoplay_policy=enable_autoplay_policy)
    host = SessionHost(
        registry=WorkerRegistry(config=config),
        config=config,
        tts=object() if speakable else None,
        promotion_manifest=promotion_manifest,
        measurement_sink=measurement_sink,
    )
    host.state.set_worker(
        WorkerState(
            worker_id="worker-weather",
            topic="weather",
            model_policy="deep",
            status="idle",
        )
    )
    origin = await host.connect(
        {
            "session_id": host.state.session_id,
            "resume_token": host.state.resume_token,
            "proposed_epoch": 1,
            "snapshot_sequence": 0,
        }
    )
    if speakable:
        origin.worker = _FakeLateResultWorker()
    return host, origin


def _has_commit_late_result_once() -> bool:
    return hasattr(SessionHost, "commit_late_result_once")


def test_commit_late_result_once_commits_every_valid_result_exactly_once() -> None:
    async def run() -> None:
        if not _has_commit_late_result_once():
            pytest.skip(
                "commit_late_result_once not yet implemented (Phase 2 concurrent implementer)"
            )
        host, _origin = await _connected_host()
        context = _late_delivery_context(host)
        result = _grounded_result()

        await host.commit_late_result_once(
            context,
            LateResult(work_item_id="work-late-1", worker_id="worker-weather", result=result),
        )
        await host.commit_late_result_once(
            context,
            LateResult(work_item_id="work-late-1", worker_id="worker-weather", result=result),
        )

        committed = [r.result_id for r in host.state.result_history("worker-weather")]
        assert committed.count("result-late-1") == 1
        await host.shutdown()

    asyncio.run(run())


def test_commit_late_result_once_cancelled_before_callback_still_commits_display_only() -> None:
    """Plan cancellation matrix: 'before callback = commit display-only/no
    speech.'"""

    async def run() -> None:
        if not _has_commit_late_result_once():
            pytest.skip(
                "commit_late_result_once not yet implemented (Phase 2 concurrent implementer)"
            )
        from server.config import PromotionManifest

        host, origin = await _connected_host(
            promotion_manifest=PromotionManifest(promotion_eligible=True), speakable=True
        )
        # Simulate the turn's child work item having already been cancelled
        # before this late callback arrives -- the same _work_ledger.cancelled_ids
        # membership host.cancel_turn_or_child() itself populates for a real
        # in-flight task (see cancel_turn_or_child -> _cancel_work), driven
        # directly here since this scenario has no real dispatched task to
        # cancel through the coordinator.
        host._work_ledger.cancelled_ids.add("work-late-1")
        context = _late_delivery_context(host)
        result = _grounded_result()

        await host.commit_late_result_once(
            context,
            LateResult(work_item_id="work-late-1", worker_id="worker-weather", result=result),
        )

        assert any(
            r.result_id == "result-late-1" for r in host.state.result_history("worker-weather")
        )
        assert origin.scheduler.active is None
        await host.shutdown()

    asyncio.run(run())


def test_commit_late_result_once_same_epoch_newer_turn_forces_display_only() -> None:
    """Plan bullet 211: register callback at sequence n, accept sequence
    n+1, deliver the callback, assert exactly-once display-only commit with
    no autoplay."""

    async def run() -> None:
        if not _has_commit_late_result_once():
            pytest.skip(
                "commit_late_result_once not yet implemented (Phase 2 concurrent implementer)"
            )
        from server.config import PromotionManifest

        host, origin = await _connected_host(
            promotion_manifest=PromotionManifest(promotion_eligible=True), speakable=True
        )
        context = _late_delivery_context(host, accepted_turn_sequence=host._turn_sequence)
        host._next_turn_id()  # advances _turn_sequence past the captured snapshot
        result = _grounded_result()

        await host.commit_late_result_once(
            context,
            LateResult(work_item_id="work-late-1", worker_id="worker-weather", result=result),
        )

        assert any(
            r.result_id == "result-late-1" for r in host.state.result_history("worker-weather")
        )
        assert origin.scheduler.active is None
        await host.shutdown()

    asyncio.run(run())


def test_commit_late_result_once_stale_origin_epoch_commits_display_only_and_does_not_mutate_worker() -> (
    None
):
    async def run() -> None:
        if not _has_commit_late_result_once():
            pytest.skip(
                "commit_late_result_once not yet implemented (Phase 2 concurrent implementer)"
            )
        from server.config import PromotionManifest

        host, _origin = await _connected_host(
            promotion_manifest=PromotionManifest(promotion_eligible=True), speakable=True
        )
        context = _late_delivery_context(host, origin_epoch=1)
        result = _grounded_result(origin_epoch=1)
        # Advance the active epoch so the result's origin becomes historical.
        second = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 2,
                "snapshot_sequence": 0,
            }
        )
        second.worker = _FakeLateResultWorker()

        await host.commit_late_result_once(
            context,
            LateResult(work_item_id="work-late-1", worker_id="worker-weather", result=result),
        )

        assert any(
            r.result_id == "result-late-1" for r in host.state.result_history("worker-weather")
        )
        assert second.scheduler.active is None
        await host.shutdown()

    asyncio.run(run())


def test_commit_late_result_once_malformed_result_produces_no_commit() -> None:
    async def run() -> None:
        if not _has_commit_late_result_once():
            pytest.skip(
                "commit_late_result_once not yet implemented (Phase 2 concurrent implementer)"
            )
        host, _origin = await _connected_host()
        context = _late_delivery_context(host)

        await host.commit_late_result_once(
            context,
            LateResult(
                work_item_id="work-late-1",
                worker_id="worker-weather",
                result="not-a-grounded-result",
            ),
        )

        assert host.state.result_history("worker-weather") == ()
        await host.shutdown()

    asyncio.run(run())


def test_commit_late_result_once_duplicate_result_id_retains_the_first_commit_unchanged() -> None:
    async def run() -> None:
        if not _has_commit_late_result_once():
            pytest.skip(
                "commit_late_result_once not yet implemented (Phase 2 concurrent implementer)"
            )
        host, _origin = await _connected_host()
        context = _late_delivery_context(host)
        first = _grounded_result(text="First")
        duplicate = _grounded_result(text="Different text, same result_id")

        await host.commit_late_result_once(
            context,
            LateResult(work_item_id="work-late-1", worker_id="worker-weather", result=first),
        )
        await host.commit_late_result_once(
            context,
            LateResult(work_item_id="work-late-1", worker_id="worker-weather", result=duplicate),
        )

        history = [
            r for r in host.state.result_history("worker-weather") if r.result_id == "result-late-1"
        ]
        assert len(history) == 1
        assert history[0].text == "First"
        await host.shutdown()

    asyncio.run(run())


# -- Late-commit fence precedence over cancellation (finding I5) -----------
#
# The cancellation branch must never bypass the structural fences: a
# cancelled work item whose late result carries a foreign origin_epoch, or
# whose result_id has already been committed, must still be fenced out
# instead of being written to authoritative state as a "committed" cancel.


def _register_late_recorder(
    host: SessionHost,
    work_item_id: str = "work-late-1",
    *,
    turn_id: str = "turn-late-1",
    origin_epoch: int = 1,
    worker_id: str = "worker-weather",
) -> object:
    recorder = host._recorder_factory.new_retained_recorder(
        origin_epoch=origin_epoch,
        turn_id=turn_id,
        work_item_id=work_item_id,
        app_worker_id=worker_id,
    )
    host._recorder_factory._retained_recorders[work_item_id] = recorder
    return recorder


def _background_fields(sink: CollectingMeasurementSink) -> list[dict]:
    return [record.fields for record in sink.records if record.event == "work_item_background"]


def test_commit_late_result_once_cancelled_foreign_epoch_result_is_never_committed() -> None:
    """A cancelled work item whose late result was produced under a different
    epoch must be fenced by the origin_epoch check, not written through the
    cancellation branch."""

    async def run() -> None:
        if not _has_commit_late_result_once():
            pytest.skip("commit_late_result_once not yet implemented")
        sink = CollectingMeasurementSink()
        host, _origin = await _connected_host(measurement_sink=sink)
        host._work_ledger.cancelled_ids.add("work-late-1")
        _register_late_recorder(host)
        context = _late_delivery_context(host, origin_epoch=1)
        # Foreign-epoch result: the worker produced it under epoch 7 while the
        # context (and therefore the commit fence) is bound to epoch 1.
        result = _grounded_result(origin_epoch=7)

        await host.commit_late_result_once(
            context,
            LateResult(work_item_id="work-late-1", worker_id="worker-weather", result=result),
        )

        assert host.state.result_history("worker-weather") == ()
        fields = _background_fields(sink)
        assert len(fields) == 1
        assert fields[0]["commit_outcome"] == "suppressed_stale"
        assert fields[0]["work_outcome"] == "cancelled"
        await host.shutdown()

    asyncio.run(run())


def test_commit_late_result_once_cancelled_duplicate_is_classified_suppressed_duplicate() -> None:
    """Redelivering the same result_id for a cancelled work item commits
    exactly once; the second delivery is classified as a suppressed
    duplicate rather than a second 'committed' cancel."""

    async def run() -> None:
        if not _has_commit_late_result_once():
            pytest.skip("commit_late_result_once not yet implemented")
        sink = CollectingMeasurementSink()
        host, _origin = await _connected_host(measurement_sink=sink)
        result = _grounded_result()

        for _ in range(2):
            host._work_ledger.cancelled_ids.add("work-late-1")
            _register_late_recorder(host)
            await host.commit_late_result_once(
                _late_delivery_context(host),
                LateResult(work_item_id="work-late-1", worker_id="worker-weather", result=result),
            )

        history = [
            r for r in host.state.result_history("worker-weather") if r.result_id == "result-late-1"
        ]
        assert len(history) == 1
        fields = _background_fields(sink)
        assert len(fields) == 2
        assert fields[0]["commit_outcome"] == "committed"
        assert fields[1]["commit_outcome"] == "suppressed_duplicate"
        assert fields[1]["work_outcome"] == "cancelled"
        await host.shutdown()

    asyncio.run(run())


def test_commit_late_result_once_flag_off_preserves_pre_v013_active_origin_enqueue_start() -> None:
    """Plan bullet 201: with enable_autoplay_policy disabled, preserve the
    pre-v0.1.3 active-origin enqueue/start behavior -- commit once and
    enqueue/start speech, skipping the new policy predicates (no promotion
    manifest needed for this path)."""

    async def run() -> None:
        if not _has_commit_late_result_once():
            pytest.skip(
                "commit_late_result_once not yet implemented (Phase 2 concurrent implementer)"
            )
        host, origin = await _connected_host(enable_autoplay_policy=False, speakable=True)
        context = _late_delivery_context(host)
        result = _grounded_result()

        await host.commit_late_result_once(
            context,
            LateResult(work_item_id="work-late-1", worker_id="worker-weather", result=result),
        )

        assert any(
            r.result_id == "result-late-1" for r in host.state.result_history("worker-weather")
        )
        assert origin.scheduler.active is not None
        await host.shutdown()

    asyncio.run(run())


def test_commit_late_result_once_promotion_eligible_manifest_enables_autoplay() -> None:
    """The positive counterpart: with a promotion_eligible manifest, the
    originating epoch still active, and no newer turn/cancellation/pause,
    the committed result is additionally admitted for speech."""

    async def run() -> None:
        if not _has_commit_late_result_once():
            pytest.skip(
                "commit_late_result_once not yet implemented (Phase 2 concurrent implementer)"
            )
        from server.config import PromotionManifest

        host, origin = await _connected_host(
            promotion_manifest=PromotionManifest(promotion_eligible=True), speakable=True
        )
        context = _late_delivery_context(host)
        result = _grounded_result()

        await host.commit_late_result_once(
            context,
            LateResult(work_item_id="work-late-1", worker_id="worker-weather", result=result),
        )

        assert any(
            r.result_id == "result-late-1" for r in host.state.result_history("worker-weather")
        )
        assert origin.scheduler.active is not None
        await host.shutdown()

    asyncio.run(run())


def test_commit_late_result_once_active_generation_is_never_interrupted() -> None:
    """Plan bullet 206: hold an unrelated lifecycle-owned generation in the
    connection-scoped slot and assert a late result never interrupts or
    queue-jumps it. The oracle is coordinator occupancy, not scheduler or
    connection convenience flags."""

    async def run() -> None:
        if not _has_commit_late_result_once():
            pytest.skip(
                "commit_late_result_once not yet implemented (Phase 2 concurrent implementer)"
            )
        from server.config import PromotionManifest
        from server.speech_lifecycle import GenerationIdentity

        host, origin = await _connected_host(
            promotion_manifest=PromotionManifest(promotion_eligible=True), speakable=True
        )
        lifecycle = origin.lifecycle
        assert lifecycle is not None
        held = lifecycle.try_admit(GenerationIdentity("held-utt", "held-work", origin_epoch=1))
        assert held is not None
        assert lifecycle.occupied is True

        context = _late_delivery_context(host)
        result = _grounded_result()
        await host.commit_late_result_once(
            context,
            LateResult(work_item_id="work-late-1", worker_id="worker-weather", result=result),
        )

        # The unrelated held generation must still be the sole slot occupant;
        # the late result's own speech admission attempt was queued behind
        # it, not admitted in its place.
        assert lifecycle.occupied is True
        assert lifecycle.slot_token == held.token
        assert any(
            r.result_id == "result-late-1" for r in host.state.result_history("worker-weather")
        )

        # Review Focus bullet 7 requires the coordinator token to stay the
        # oracle *through* synthesis end, cleanup, transport stop and
        # teardown -- not only at admission. Drive each stage and re-assert.
        lifecycle.mark_handed_to_tts(held.token)
        # The context id is the scheduler utterance id, by contract.
        assert lifecycle.bind_context(held.token, "held-utt") is True
        assert lifecycle.on_tts_started("held-utt") is True
        assert (
            lifecycle.on_tts_audio(
                "held-utt", audio=b"\x00\x00" * 800, sample_rate=16_000, num_channels=1
            )
            is True
        )
        assert lifecycle.on_tts_stopped("held-utt") is True
        # Synthesis end is explicitly non-terminal: the slot is still held.
        assert lifecycle.occupied is True
        assert lifecycle.slot_token == held.token

        lifecycle.on_transport_bot_started()
        assert lifecycle.slot_token == held.token

        stop = lifecycle.on_transport_bot_stopped()
        if stop is not None:
            await stop
        await asyncio.sleep(0)
        # Only now, after transport stop drove the one terminal transition, is
        # the held generation released. The slot may be immediately reclaimed
        # by the late result that was queued behind it -- that is the point of
        # ``on_terminal``'s queue re-probe -- so the assertion is that the
        # *held* generation no longer owns it, not that the slot is idle.
        assert lifecycle.slot_token != held.token
        assert lifecycle.generation_for_token(held.token) is None

        await host.shutdown()

    asyncio.run(run())


def test_lifecycle_slot_is_re_evaluated_after_release_for_each_supersession_trigger() -> None:
    """Review Focus bullet 7's second half: "post-release re-evaluation when a
    newer turn, pause, cancellation, or reconnect occurs while queued". The
    existing coverage stopped at admission and drove none of the four
    triggers. The coordinator's occupied-generation token stays the oracle at
    every step; scheduler mirrors and connection flags are not authoritative.
    """

    async def run() -> None:
        from server.speech_lifecycle import GenerationIdentity

        host, origin = await _connected_host(speakable=True)
        lifecycle = origin.lifecycle
        assert lifecycle is not None

        # (1) A newer turn cannot displace the held slot while it is occupied.
        held = lifecycle.try_admit(GenerationIdentity("held-utt", "held-work", origin_epoch=1))
        assert held is not None
        newer = lifecycle.try_admit(GenerationIdentity("newer-utt", "newer-work", origin_epoch=1))
        assert newer is None
        assert lifecycle.slot_token == held.token

        # (2) Pause records a disposition without freeing the slot: a paused
        # generation still owns the transport.
        lifecycle.record_interruption(held.token, pause=True)
        assert lifecycle.slot_token == held.token

        # (3) Cancellation (release) frees it, and only then...
        lifecycle.release_generation(held.token)
        await asyncio.sleep(0)
        assert lifecycle.occupied is False
        assert lifecycle.slot_token is None

        # ...does the previously-refused newer turn become admissible.
        readmitted = lifecycle.try_admit(
            GenerationIdentity("newer-utt", "newer-work", origin_epoch=1)
        )
        assert readmitted is not None
        assert lifecycle.slot_token == readmitted.token

        # (4) Reconnect/teardown: a closed connection refuses admission
        # outright rather than reporting a merely-busy slot.
        lifecycle.connection_closed()
        assert lifecycle.occupied is False
        assert (
            lifecycle.try_admit(GenerationIdentity("post-close", "post-work", origin_epoch=2))
            is None
        )

        await host.shutdown()

    asyncio.run(run())


def test_sole_child_cancel_still_removes_the_ack_after_an_earlier_item_was_drained() -> None:
    """A work item that has already been admitted and drained must not leave a
    stale queue key behind: the sole-remaining-child ack cancellation is gated
    on "no other pending work", and a stale empty key would make that gate
    permanently false for any connection that ever admitted an item."""

    async def run() -> None:
        # Speakable: an item can only be admitted and drained on a connection
        # that has a TTS lane; with none, admission is terminal before it
        # starts.
        host = SessionHost(tts=object())
        origin = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 1,
                "snapshot_sequence": 0,
            }
        )
        origin.worker = _FakeLateResultWorker()
        drained = origin.scheduler.enqueue(
            work_item_id="work-earlier",
            run_id="run-earlier",
            result_id="result-earlier",
            text="an earlier answer",
            origin_epoch=1,
        )
        admitted = await origin.scheduler.start_next("work-earlier")
        assert admitted is not None and admitted.utterance_id == drained.utterance_id
        origin.scheduler.delivery_completed(admitted.utterance_id)
        assert origin.scheduler.active is None
        assert origin.scheduler.pending_work_item_ids() == frozenset()

        turn_id = "turn-5"
        ack_work_item_id = f"ack-{turn_id}"
        host._turn_ack_ledger._ack_emitted_turns.add(turn_id)
        origin.scheduler.enqueue(
            work_item_id=ack_work_item_id,
            run_id="run-ack",
            result_id=None,
            text="One moment.",
            role="ack",
            ack_id=ack_work_item_id,
            turn_id=turn_id,
        )
        origin.scheduler.enqueue(
            work_item_id="work-5-0",
            run_id="run-5-0",
            result_id="result-5-0",
            text="only child",
        )

        await host.cancel_turn_or_child(turn_id, "work-5-0")

        assert ack_work_item_id not in origin.scheduler._queues
        assert "work-5-0" not in origin.scheduler._queues
        assert turn_id not in host._turn_ack_ledger._ack_emitted_turns
        await host.shutdown()

    asyncio.run(run())


def test_late_result_disposition_reads_only_the_cached_promotion_eligible_boolean() -> None:
    """`_late_result_disposition` must consult the boolean cached at
    construction time (`self._promotion_eligible`), never re-derive it from
    `self._promotion_manifest` at call time. Mutating `_promotion_manifest`
    to None after construction -- without touching the cached boolean --
    must not flip the disposition to display-only."""

    async def run() -> None:
        from server.config import PromotionManifest

        host, origin = await _connected_host(
            promotion_manifest=PromotionManifest(promotion_eligible=True), speakable=True
        )
        context = _late_delivery_context(host)

        host._promotion_manifest = None

        disposition = host._late_result_disposition(context, origin=origin)
        assert disposition == "autoplay"
        await host.shutdown()

    asyncio.run(run())


def test_clarification_candidates_are_bounded_and_cleared_at_shutdown() -> None:
    """Regression (#21/#27): ``WorkerProjection._clarification_candidates``
    grew without bound and had no lifecycle hook. Entries are inserted on
    every worker-raised ``WorkerClarify`` and removed only by
    ``pop_clarification_candidate`` on the commit path, so a clarification
    whose turn is cancelled, whose connection drops, or whose result never
    reaches ``_commit_result_state`` left its entry for the process lifetime
    -- with no cap, TTL, or shutdown sweep, unlike the handshake-token cap and
    the work-status ledger's cap plus TTL the branch already applies
    elsewhere."""

    async def run() -> None:
        host = SessionHost()
        projection = host._worker_projection
        cap = projection.MAX_CLARIFICATION_CANDIDATES

        first_result_id = ""
        for index in range(cap + 10):
            result = projection.clarification_result(
                worker_id=f"worker-{index}",
                turn_id=f"turn-{index}",
                question="which one?",
                original_query="ambiguous",
                origin_epoch=1,
            )
            if index == 0:
                first_result_id = result.result_id

        assert len(projection._clarification_candidates) <= cap
        # Oldest-first eviction, matching the sibling bounded maps.
        assert projection.pop_clarification_candidate(first_result_id) is None

        await host.shutdown()
        assert projection._clarification_candidates == {}

    asyncio.run(run())


def test_track_background_shutdown_is_retained_and_drained_by_shutdown() -> None:
    """Regression (#23): ``server/app.py``'s Small-WebRTC ``worker_finished``
    handler created its shutdown task with a bare ``asyncio.create_task`` and
    discarded the handle, bypassing the ``_background_shutdowns`` set every
    other background shutdown in the codebase uses. An untracked task can be
    collected mid-flight, and ``SessionHost.shutdown`` -- which awaits exactly
    that set -- had no way to drain it. This pins the seam the app now routes
    through."""

    async def run() -> None:
        host = SessionHost()
        finished: list[str] = []
        release = asyncio.Event()

        async def slow_shutdown() -> None:
            await release.wait()
            finished.append("done")

        task = host.track_background_shutdown(asyncio.create_task(slow_shutdown()))
        await asyncio.sleep(0)
        assert task in host._background_shutdowns

        release.set()
        await host.shutdown()
        # shutdown() drained it rather than leaving it in flight...
        assert finished == ["done"]
        # ...and the done-callback keeps the set from growing unboundedly.
        assert host._background_shutdowns == set()

    asyncio.run(run())


def test_active_speech_oracle_holds_through_cleanup_and_teardown() -> None:
    """Regression (Round 9, #20): Review Focus bullet 7 names four stages the
    coordinator's occupied-generation token must remain the oracle through --
    synthesis end, cleanup, transport stop, and *teardown*. Round 8's extended
    test drove synthesis-end and transport-stop signals only, so
    ``SpeechLifecycleCoordinator._begin_cleanup`` and ``teardown_complete``
    were never reached by it at all.

    This drives the other two: an interruption that goes through cleanup
    dispatch, then teardown completion, re-asserting ``slot_token`` /
    ``generation_for_token`` at each stage. The point of the assertions is
    that cleanup does *not* release the slot on its own -- only the terminal
    transition teardown drives does.
    """

    async def run() -> None:
        if not _has_commit_late_result_once():
            pytest.skip("commit_late_result_once not yet implemented")
        from server.config import PromotionManifest
        from server.speech_lifecycle import GenerationIdentity

        host, origin = await _connected_host(
            promotion_manifest=PromotionManifest(promotion_eligible=True), speakable=True
        )
        lifecycle = origin.lifecycle
        assert lifecycle is not None
        held = lifecycle.try_admit(GenerationIdentity("held-utt", "held-work", origin_epoch=1))
        assert held is not None
        lifecycle.mark_handed_to_tts(held.token)
        assert lifecycle.bind_context(held.token, "held-utt") is True
        assert lifecycle.on_tts_started("held-utt") is True
        assert (
            lifecycle.on_tts_audio(
                "held-utt", audio=b"\x00\x00" * 800, sample_rate=16_000, num_channels=1
            )
            is True
        )
        assert lifecycle.slot_token == held.token

        # Stage: interruption. Tombstones and arms the cleanup deadline; the
        # slot is deliberately *not* released here.
        lifecycle.record_interruption(held.token)
        await asyncio.sleep(0)
        assert lifecycle.slot_token == held.token
        generation = lifecycle.generation_for_token(held.token)
        assert generation is not None
        assert generation.tombstoned is True
        assert generation.terminalized is False

        # Stage: cleanup. Audio already crossed into output, so cleanup
        # escalates to teardown -- and still holds the slot until the
        # connection lane confirms.
        await lifecycle._begin_cleanup(generation)
        assert generation.cleanup_pending is True
        assert lifecycle.slot_token == held.token
        assert lifecycle.generation_for_token(held.token) is not None

        # Stage: teardown. This is the one terminal transition, and only now
        # does the held generation stop owning the slot.
        await lifecycle.teardown_complete(held.token)
        await asyncio.sleep(0)
        assert lifecycle.slot_token != held.token
        assert lifecycle.generation_for_token(held.token) is None

        await host.shutdown()

    asyncio.run(run())
