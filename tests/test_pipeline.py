"""Connection pipelines expose authoritative state through a fakeable observer."""

import asyncio
import threading
import time

import pytest
from pipecat.audio.turn.smart_turn.local_smart_turn_v3 import LocalSmartTurnAnalyzerV3
from pipecat.bus.bridge_processor import BusBridgeProcessor as FrameworkBusBridgeProcessor
from pipecat.frames.frames import (
    InterruptionFrame,
    SystemFrame,
    TranscriptionFrame,
    TTSSpeakFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.processors.frameworks.rtvi.frames import RTVIServerMessageFrame
from pipecat.turns.user_stop import TurnAnalyzerUserTurnStopStrategy
from pipecat.turns.user_turn_processor import UserTurnProcessor

import server.app as app_module
import server.pipeline as pipeline_module
import server.speech_lifecycle
from server.config import Config, PromotionManifest
from server.contracts import GroundedResult, RoutingDecision, WorkerState
from server.frames import CONNECTION_LOCAL_FRAMES, SnapshotBarrierFlushFrame
from server.perf_metrics import CollectingMeasurementSink
from server.pipeline import (
    CanonicalResultAdapter,
    LateDeliveryContext,
    SessionHost,
    build_pipeline,
    framework_bridge,
)
from server.registry import UnsupportedWorkerType, WorkerRegistry
from server.services.tts import CorrelatedTTSSpeakFrame
from server.speech_lifecycle import (
    SpeechGenerationFlushAckFrame,
    SpeechGenerationMarkerFrame,
)
from server.speech_scheduler import ROLE_RESULT, ROLE_TIMEOUT_NOTICE
from server.turns import FinalTurnTranscriptProcessor, smart_turn_processor
from server.work_item_coordinator import LateResult, WorkItemCoordinator
from server.workers.web_search import ClarificationContext, WorkerClarify, WorkerDeclined


class RoutedCoordinator:
    def __init__(self, worker: object) -> None:
        self.worker = worker
        self.clarifications: list[dict[str, str]] = []

    def arbitrate(self, _session_id: str, transcript: str) -> object:
        return type(
            "Outcome",
            (),
            {"kind": "routed", "decision": object(), "transcript": transcript},
        )()

    def dispatch(self, _decision: object) -> object:
        return self.worker

    def add_worker_clarification(
        self,
        *,
        session_id: str,
        worker_id: str,
        turn_id: str,
        result_id: str,
        original_query: str,
        question: str,
    ) -> None:
        self.clarifications.append(
            {
                "session_id": session_id,
                "worker_id": worker_id,
                "turn_id": turn_id,
                "result_id": result_id,
                "original_query": original_query,
                "question": question,
            }
        )


class ResultWorker:
    async def search(self, query: str, *, turn_id: str, origin_epoch: int | None) -> GroundedResult:
        return GroundedResult(
            result_id=f"result-{turn_id}",
            worker_id="worker-search",
            turn_id=turn_id,
            text=f"Answer for {query}",
            spoken_text=f"Spoken answer for {query}",
            origin_epoch=origin_epoch,
        )


class ProjectedResultWorker(ResultWorker):
    metadata = type(
        "Metadata",
        (),
        {
            "worker_id": "worker-search",
            "topic": "historical capitals of India",
            "model_policy": "deep",
        },
    )()

    def __init__(self) -> None:
        self.queries: list[str] = []

    async def search(
        self,
        query: str,
        *,
        turn_id: str,
        origin_epoch: int | None,
        clarification_context: ClarificationContext | None = None,
    ) -> GroundedResult:
        provider_query = (
            clarification_context.provider_query() if clarification_context is not None else query
        )
        self.queries.append(provider_query)
        return await super().search(provider_query, turn_id=turn_id, origin_epoch=origin_epoch)


class ProjectedCoordinator(RoutedCoordinator):
    def __init__(self, worker: object) -> None:
        super().__init__(worker)
        self.transcripts: list[str] = []

    def arbitrate(self, _session_id: str, transcript: str) -> object:
        self.transcripts.append(transcript)
        return type(
            "Outcome",
            (),
            {
                "kind": "routed",
                "decision": RoutingDecision(
                    action="new_worker",
                    worker_type="web_search",
                    topic="historical capitals of India",
                    capability="public_web",
                    capability_available=True,
                    model_policy="deep",
                    catalogue_version="catalogue-0",
                ),
                "transcript": transcript,
            },
        )()


class BlockingResultWorker(ResultWorker):
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.origin_epochs: list[int | None] = []

    async def search(self, query: str, *, turn_id: str, origin_epoch: int | None) -> GroundedResult:
        self.origin_epochs.append(origin_epoch)
        self.started.set()
        await self.release.wait()
        return await super().search(query, turn_id=turn_id, origin_epoch=origin_epoch)


class DecliningResultWorker(ResultWorker):
    async def search(self, query: str, *, turn_id: str, origin_epoch: int | None) -> GroundedResult:
        raise WorkerDeclined(f"cannot satisfy {query}")


class ClarifyingResultWorker(ResultWorker):
    async def search(self, query: str, *, turn_id: str, origin_epoch: int | None) -> GroundedResult:
        raise WorkerClarify("Which city's weather do you mean?")


class ContinuationResultWorker(ResultWorker):
    metadata = type(
        "Metadata",
        (),
        {
            "worker_id": "worker-search",
            "topic": "weather",
            "model_policy": "deep",
        },
    )()

    def __init__(self) -> None:
        self.queries: list[str] = []

    async def search(
        self,
        query: str,
        *,
        turn_id: str,
        origin_epoch: int | None,
        clarification_context: ClarificationContext | None = None,
    ) -> GroundedResult:
        provider_query = (
            clarification_context.provider_query() if clarification_context is not None else query
        )
        self.queries.append(provider_query)
        return await super().search(provider_query, turn_id=turn_id, origin_epoch=origin_epoch)


class PendingRegistry:
    def __init__(self, worker: object, config: Config | None = None) -> None:
        self.worker = worker
        self.config = config or Config()

    def get(self, worker_id: str) -> object:
        assert worker_id == "worker-search"
        return type("Registered", (), {"worker": self.worker})()


class FakeTTS:
    def __init__(self) -> None:
        self.on_event = None

    @staticmethod
    def correlated_speak_frame(
        text: str, *, correlation_id: str, append_to_context: bool
    ) -> CorrelatedTTSSpeakFrame:
        return CorrelatedTTSSpeakFrame(
            text=text,
            correlation_id=correlation_id,
            append_to_context=append_to_context,
        )


class QueueingPipelineWorker:
    def __init__(self) -> None:
        self.frames: list[object] = []

    async def queue_frame(self, frame: object) -> None:
        self.frames.append(frame)

    async def cancel(self, *, reason: str) -> None:
        assert reason in {
            "connection replaced",
            "session shutdown",
            "speech output teardown",
            "speech output teardown failed",
        }


class LifecycleRunner:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


def connection_handshake(host: SessionHost, epoch: int) -> dict[str, object]:
    return {
        "session_id": host.state.session_id,
        "resume_token": host.state.resume_token,
        "proposed_epoch": epoch,
        "snapshot_sequence": 0,
    }


async def release_lifecycle_slot(connection: object, token: str, context_id: str) -> None:
    """Stand in for what ``TransportSpeechLifecycleProcessor`` does to a real
    frame pipeline: bind the marker token to a generated context, observe
    synthesis start/stop, then the fieldless upstream bot stop. Phase 1 makes
    the coordinator (not synthesis end alone) the sole authority for
    releasing the transport slot and admitting the next generation, so tests
    built around ``QueueingPipelineWorker`` (no real frame pipeline) must
    drive the coordinator explicitly rather than relying on
    ``tts.on_event("synthesis_ended", ...)`` to do it.
    """
    lifecycle = connection.lifecycle
    if lifecycle is None:
        return
    lifecycle.bind_context(token, context_id)
    lifecycle.on_tts_started(context_id)
    lifecycle.on_tts_stopped(context_id)
    lifecycle.on_transport_bot_stopped()
    await asyncio.sleep(0)  # let on_terminal's fire-and-forget task run


async def acknowledge_lifecycle_flush(connection: object, token: str) -> None:
    """Model the private token-bearing frame reaching the post-TTS bridge."""
    await connection.scheduler.wait_for_stops()
    lifecycle = connection.lifecycle
    assert lifecycle is not None
    await lifecycle.acknowledge_tts_lane_flush(token)


def test_successful_result_starts_speech_on_same_pipecat_worker() -> None:
    async def run() -> None:
        tts = FakeTTS()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=tts,
            coordinator=RoutedCoordinator(ResultWorker()),
        )
        connection = await host.connect(connection_handshake(host, 1))
        worker = QueueingPipelineWorker()
        connection.worker = worker
        start_next = connection.scheduler.start_next
        start_calls = 0

        async def tracked_start_next(work_item_id: str | None = None) -> object:
            nonlocal start_calls
            start_calls += 1
            return await start_next(work_item_id)

        connection.scheduler.start_next = tracked_start_next  # type: ignore[method-assign]

        result = await host._handle_transcript("Riga weather")

        assert start_calls == 1
        speak_frames = [frame for frame in worker.frames if isinstance(frame, TTSSpeakFrame)]
        assert len(speak_frames) == 1
        speak_frame = speak_frames[0]
        assert speak_frame.text == result.spoken_text
        assert speak_frame.text != result.text
        assert speak_frame.append_to_context is False
        assert connection.scheduler.active is not None
        utterance_id = connection.scheduler.active.item.utterance_id
        connection.scheduler.enqueue(
            result_id="result-next",
            work_item_id="work-next",
            run_id="run-next",
            text="Next answer",
            origin_epoch=1,
        )

        assert speak_frame.correlation_id == utterance_id
        active_token = connection.scheduler.active.token
        await tts.on_event("synthesis_started", utterance_id)
        await tts.on_event("synthesis_ended", utterance_id)
        assert host.state.speech[utterance_id].state.value == "synthesis_ended"
        assert connection.scheduler.active is not None, (
            "synthesis end alone must not release the transport slot or admit the next item"
        )

        await release_lifecycle_slot(connection, active_token, utterance_id)

        assert host.state.speech[utterance_id].state.value == "delivery_unknown"
        speak_frames = [frame for frame in worker.frames if isinstance(frame, TTSSpeakFrame)]
        assert len(speak_frames) == 2
        assert connection.scheduler.active is not None
        assert connection.scheduler.active.item.result_id == "result-next"
        await host.shutdown()

    asyncio.run(run())


def test_synthesis_started_on_a_superseded_connection_does_not_mutate_scheduler_state() -> None:
    """Only the *active* connection's provider callbacks may mutate scheduler
    state. ``synthesis_started`` was the one branch of ``on_tts_event`` missing
    the ``current`` guard its three siblings carry."""

    async def run() -> None:
        tts = FakeTTS()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=tts,
            coordinator=RoutedCoordinator(ResultWorker()),
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()

        await host._handle_transcript("Riga weather")
        assert connection.scheduler.active is not None
        utterance_id = connection.scheduler.active.item.utterance_id

        # Supersede without deactivate()'s interrupt, which would release the
        # lease and make provider_started's own guard fire for the wrong reason.
        connection.active = False

        await tts.on_event("synthesis_started", utterance_id)

        assert connection.scheduler._provider_contexts == {}
        await host.shutdown()

    asyncio.run(run())


def test_delivery_completed_event_does_not_bypass_the_lifecycle_coordinator() -> None:
    """With a coordinator installed, an on_event-based "delivery_completed"
    callback must not release the scheduler's active lease directly -- only
    the coordinator's own token-bearing transport/tombstone barriers may."""

    async def run() -> None:
        tts = FakeTTS()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=tts,
            coordinator=RoutedCoordinator(ResultWorker()),
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        assert connection.lifecycle is not None

        await host._handle_transcript("Riga weather")

        assert connection.scheduler.active is not None
        utterance_id = connection.scheduler.active.item.utterance_id
        connection.scheduler.enqueue(
            result_id="result-next",
            work_item_id="work-next",
            run_id="run-next",
            text="Next answer",
            origin_epoch=1,
        )

        await tts.on_event("synthesis_started", utterance_id)
        await tts.on_event("delivery_completed", utterance_id)

        assert connection.scheduler.active is not None, (
            "a lifecycle-bypassing delivery_completed callback must not release the slot"
        )
        assert connection.scheduler.active.item.utterance_id == utterance_id
        await host.shutdown()

    asyncio.run(run())


def test_output_teardown_must_finish_before_lifecycle_slot_is_released() -> None:
    async def run() -> None:
        host = SessionHost(runner_factory=LifecycleRunner, tts=FakeTTS())
        connection = await host.connect(connection_handshake(host, 1))
        worker = QueueingPipelineWorker()
        connection.worker = worker
        teardown_started = asyncio.Event()
        allow_teardown = asyncio.Event()

        async def teardown_output() -> None:
            teardown_started.set()
            await allow_teardown.wait()

        connection.output_teardown = teardown_output
        active = connection.scheduler.enqueue(
            result_id="result-active",
            work_item_id="work-active",
            run_id="run-active",
            text="Active answer",
            origin_epoch=1,
        )
        await connection.scheduler.start_next()
        connection.scheduler.enqueue(
            result_id="result-queued",
            work_item_id="work-queued",
            run_id="run-queued",
            text="Queued answer",
            origin_epoch=1,
        )
        assert connection.scheduler.active is not None
        token = connection.scheduler.active.token
        lifecycle = connection.lifecycle
        assert lifecycle is not None
        lifecycle.bind_context(token, active.utterance_id)
        lifecycle.on_tts_started(active.utterance_id)
        assert lifecycle.on_tts_audio(
            active.utterance_id,
            audio=b"\0\0",
            sample_rate=16000,
            num_channels=1,
        )

        cleanup = asyncio.create_task(lifecycle.provider_error(token))
        await teardown_started.wait()

        held = lifecycle.generation_for_token(token)
        assert held is not None
        assert lifecycle.slot_token == token, (
            "the admitted generation's token must still be the sole slot occupant "
            "while its teardown is in flight"
        )
        assert held.tombstoned is True
        assert held.teardown_pending is True
        # Corroborating, non-authoritative mirrors.
        assert connection.scheduler.active is not None
        assert connection.active is False
        assert await connection.scheduler.start_next() is None

        allow_teardown.set()
        await cleanup

        assert lifecycle.generation_for_token(token) is None, (
            "the terminalized generation must be reaped once teardown completes"
        )
        assert lifecycle.slot_token is None, (
            "the token oracle must show the slot released, not merely a mirror flag"
        )
        assert connection.scheduler.active is None
        assert [frame.text for frame in worker.frames if isinstance(frame, TTSSpeakFrame)] == [
            "Active answer"
        ]
        await host.shutdown()

    asyncio.run(run())


def test_shutdown_still_forces_scheduler_cleanup_after_teardown_exception() -> None:
    async def run() -> None:
        host = SessionHost(runner_factory=LifecycleRunner, tts=FakeTTS())
        connection = await host.connect(connection_handshake(host, 1))
        worker = QueueingPipelineWorker()
        connection.worker = worker

        async def failing_teardown() -> None:
            raise RuntimeError("output teardown boom")

        connection.output_teardown = failing_teardown
        active = connection.scheduler.enqueue(
            result_id="result-active",
            work_item_id="work-active",
            run_id="run-active",
            text="Active answer",
            origin_epoch=1,
        )
        await connection.scheduler.start_next()
        assert connection.scheduler.active is not None
        token = connection.scheduler.active.token
        lifecycle = connection.lifecycle
        assert lifecycle is not None
        lifecycle.bind_context(token, active.utterance_id)
        lifecycle.on_tts_started(active.utterance_id)
        assert lifecycle.on_tts_audio(
            active.utterance_id,
            audio=b"\0\0",
            sample_rate=16000,
            num_channels=1,
        )

        # Drive the terminal path directly: the failing teardown sets
        # connection.active = False before raising, without ever releasing
        # the scheduler's lease.
        await lifecycle.provider_error(token)

        assert connection.active is False
        assert connection.scheduler.active is not None, (
            "sanity: the failing teardown should not itself release the lease"
        )

        await connection.shutdown(reason="session shutdown")

        assert connection.scheduler.active is None

    asyncio.run(run())


@pytest.mark.parametrize("action", ["pause", "cancel"])
def test_interruption_terminal_barrier_starts_queued_speech_only_after_release(
    action: str,
) -> None:
    async def run() -> None:
        host = SessionHost(runner_factory=LifecycleRunner, tts=FakeTTS())
        connection = await host.connect(connection_handshake(host, 1))
        worker = QueueingPipelineWorker()
        connection.worker = worker
        active = connection.scheduler.enqueue(
            result_id="result-active",
            work_item_id="work-active",
            run_id="run-active",
            text="Active answer",
            origin_epoch=1,
        )
        await connection.scheduler.start_next()
        connection.scheduler.enqueue(
            result_id="result-queued",
            work_item_id="work-queued",
            run_id="run-queued",
            text="Queued answer",
            origin_epoch=1,
        )
        assert connection.scheduler.active is not None
        token = connection.scheduler.active.token
        lifecycle = connection.lifecycle
        assert lifecycle is not None
        lifecycle.bind_context(token, active.utterance_id)
        lifecycle.on_tts_started(active.utterance_id)
        assert lifecycle.on_tts_audio(
            active.utterance_id,
            audio=b"\0\0",
            sample_rate=16000,
            num_channels=1,
        )

        if action == "pause":
            connection.scheduler.pause("work-active")
        else:
            connection.scheduler.cancel("work-active")
        await connection.scheduler.wait_for_stops()
        assert connection.scheduler.active is None
        held = lifecycle.generation_for_token(token)
        assert held is not None
        assert lifecycle.slot_token == token, (
            "the interrupted generation's token must still be the sole slot "
            "occupant before the terminal barrier runs"
        )
        assert held.tombstoned is True

        terminal = lifecycle.on_transport_bot_stopped()
        assert terminal is not None
        await terminal

        assert lifecycle.generation_for_token(token) is None, (
            "the interrupted generation must be reaped once terminalized"
        )
        assert connection.scheduler.active is not None
        assert connection.scheduler.active.item.work_item_id == "work-queued"
        queued_token = connection.scheduler.active.token
        assert queued_token != token
        assert lifecycle.slot_token == queued_token, (
            "the token oracle must show the queued generation as the new sole "
            "slot occupant, not merely the scheduler mirror"
        )
        assert [frame.text for frame in worker.frames if isinstance(frame, TTSSpeakFrame)] == [
            "Active answer",
            "Queued answer",
        ]
        await host.shutdown()

    asyncio.run(run())


def test_speak_request_emits_a_generation_marker_immediately_before_the_tts_speak_frame() -> None:
    """Phase 1 requires a private, token-bearing SpeechGenerationMarkerFrame
    inserted immediately before each TTSSpeakFrame, so the post-TTS lifecycle
    bridge can bind the next generated TTS context_id without ever inferring
    identity from a fieldless bot frame. This asserts pipeline-level wiring;
    if server/pipeline.py names the marker frame differently, reconcile the
    import/name here rather than dropping the ordering assertion.
    """

    async def run() -> None:
        tts = FakeTTS()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=tts,
            coordinator=RoutedCoordinator(ResultWorker()),
        )
        connection = await host.connect(connection_handshake(host, 1))
        worker = QueueingPipelineWorker()
        connection.worker = worker

        await host._handle_transcript("Riga weather")

        speak_index = next(
            index for index, frame in enumerate(worker.frames) if isinstance(frame, TTSSpeakFrame)
        )
        assert speak_index > 0, "a marker frame must precede the TTSSpeakFrame"
        marker = worker.frames[speak_index - 1]
        assert isinstance(marker, SpeechGenerationMarkerFrame)
        assert connection.scheduler.active is not None
        assert marker.utterance_id == connection.scheduler.active.item.utterance_id

        await host.shutdown()

    asyncio.run(run())


def test_completed_turn_projects_routing_transcript_and_real_worker_state() -> None:
    async def run() -> None:
        search = ProjectedResultWorker()
        coordinator = ProjectedCoordinator(search)
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=coordinator,
        )
        await host.connect(connection_handshake(host, 1))
        processor = FinalTurnTranscriptProcessor(
            host._handle_transcript,
            complete_grace_seconds=0.01,
        )

        async def push(_frame: object, _direction: object) -> None:
            return None

        processor.push_frame = push  # type: ignore[method-assign]
        await processor.process_frame(
            TranscriptionFrame("What were the capitals of", "", ""),
            FrameDirection.DOWNSTREAM,
        )
        await processor.process_frame(UserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
        await processor.process_frame(UserStartedSpeakingFrame(), FrameDirection.DOWNSTREAM)
        for fragment in ("India through", "the last two hundred years?"):
            await processor.process_frame(
                TranscriptionFrame(fragment, "", ""),
                FrameDirection.DOWNSTREAM,
            )
        await processor.process_frame(UserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)

        async def wait_for_result() -> GroundedResult:
            while not host.state.results.results:
                await asyncio.sleep(0.01)
            return host.state.results.results[-1]

        result = await asyncio.wait_for(wait_for_result(), timeout=1)

        expected = "What were the capitals of India through the last two hundred years?"
        assert coordinator.transcripts == [expected]
        assert search.queries == [expected]
        assert [entry.role for entry in host.state.transcript] == ["user", "assistant"]
        assert host.state.transcript[0].turn_id == result.turn_id
        assert host.state.routing is not None
        assert host.state.routing.action == "new_worker"
        worker = host.state.workers["worker-search"]
        assert worker.topic == "historical capitals of India"
        assert worker.status == "idle"
        assert worker.latest_result_id == result.result_id
        assert [event.kind for event in host.state.events] == [
            "user_transcript",
            "routing",
            "worker",
            "work_status",
            "work_status",
            "bot_transcript",
            "result",
            # Terminal `result_ready` is emitted only after the canonical
            # commit above succeeds -- never before it (round-1 finding I7).
            "work_status",
            "worker",
        ]
        assert [
            event.payload["state"] for event in host.state.events if event.kind == "work_status"
        ] == ["routing", "searching", "result_ready"]
        await host.shutdown()

    asyncio.run(run())


def test_reconnect_while_search_is_blocked_keeps_late_result_history_only() -> None:
    async def run() -> None:
        search = BlockingResultWorker()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=FakeTTS(),
            coordinator=RoutedCoordinator(search),
        )
        first = await host.connect(connection_handshake(host, 1))
        first_worker = QueueingPipelineWorker()
        first.worker = first_worker
        pending = asyncio.create_task(host._handle_transcript("old request"))
        await asyncio.wait_for(search.started.wait(), timeout=1)

        second = await host.connect(connection_handshake(host, 2))
        second_worker = QueueingPipelineWorker()
        second.worker = second_worker
        search.release.set()
        result = await asyncio.wait_for(pending, timeout=1)

        assert search.origin_epochs == [1]
        assert result.origin_epoch == 1
        assert host.state.result_history("worker-search") == (result,)
        assert host.state.workers.get("worker-search") is None
        assert first_worker.frames == []
        assert second_worker.frames == []
        assert first.scheduler._queues == {}
        assert second.scheduler._queues == {}
        assert host.state.speech == {}
        await host.shutdown()

    asyncio.run(run())


def test_search_decline_becomes_a_safe_canonical_result() -> None:
    async def run() -> None:
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=RoutedCoordinator(DecliningResultWorker()),
        )
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("private calendar")

        assert result.text == "I could not find a reliable result for that request."
        assert host.state.result_history("main") == (result,)
        await host.shutdown()

    asyncio.run(run())


def test_worker_clarify_records_a_pending_dialogue_and_speaks_the_question() -> None:
    async def run() -> None:
        coordinator = RoutedCoordinator(ClarifyingResultWorker())
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=coordinator,
        )
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("What's the weather like?")

        assert result.text == "Which city's weather do you mean?"
        assert host.state.result_history("main") == (result,)
        assert len(coordinator.clarifications) == 1
        clarification = coordinator.clarifications[0]
        assert clarification["worker_id"] == "main"
        assert clarification["result_id"] == result.result_id
        assert clarification["session_id"] == host.state.session_id
        assert clarification["original_query"] == "What's the weather like?"
        assert clarification["question"] == "Which city's weather do you mean?"
        await host.shutdown()

    asyncio.run(run())


def test_stale_worker_clarification_cannot_take_pending_dialogue_after_reconnect() -> None:
    async def run() -> None:
        class BlockingClarifier(ClarifyingResultWorker):
            def __init__(self) -> None:
                self.started = asyncio.Event()
                self.release = asyncio.Event()

            async def search(
                self, query: str, *, turn_id: str, origin_epoch: int | None
            ) -> GroundedResult:
                self.started.set()
                await self.release.wait()
                return await super().search(
                    query,
                    turn_id=turn_id,
                    origin_epoch=origin_epoch,
                )

        worker = BlockingClarifier()
        coordinator = RoutedCoordinator(worker)
        host = SessionHost(runner_factory=LifecycleRunner, coordinator=coordinator)
        await host.connect(connection_handshake(host, 1))
        pending = asyncio.create_task(host._handle_transcript("weather"))
        await worker.started.wait()

        await host.connect(connection_handshake(host, 2))
        worker.release.set()
        await pending

        assert coordinator.clarifications == []
        assert host._clarification_candidates == {}
        await host.shutdown()

    asyncio.run(run())


def test_natural_clarification_answer_resumes_original_query_on_same_worker() -> None:
    async def run() -> None:
        worker = ContinuationResultWorker()
        registry = PendingRegistry(worker)
        coordinator = WorkItemCoordinator(registry=registry)
        host = SessionHost(
            registry=registry,
            runner_factory=LifecycleRunner,
            coordinator=coordinator,
        )
        coordinator.add_worker_clarification(
            session_id=host.state.session_id,
            worker_id="worker-search",
            turn_id="turn-original",
            result_id="result-question",
            original_query="What's the weather like?",
            question="Which location should I use?",
        )
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("Riga")

        assert result.text.startswith("Answer for Original request: What's the weather like?")
        assert worker.queries == [
            (
                "Original request: What's the weather like?\n"
                "Clarification asked: Which location should I use?\n"
                "User answer: Riga"
            )
        ]
        assert coordinator.pending(host.state.session_id) is None
        await host.shutdown()

    asyncio.run(run())


def test_timed_out_pending_search_queues_late_speech_once_after_active_audio() -> None:
    async def run() -> None:
        class SlowWorker(ContinuationResultWorker):
            def __init__(self) -> None:
                super().__init__()
                self.started = asyncio.Event()
                self.release = asyncio.Event()
                self.cancelled = False

            async def search(
                self,
                query: str,
                *,
                turn_id: str,
                origin_epoch: int | None,
                clarification_context: ClarificationContext | None = None,
            ) -> GroundedResult:
                provider_query = (
                    clarification_context.provider_query()
                    if clarification_context is not None
                    else query
                )
                self.queries.append(provider_query)
                self.started.set()
                try:
                    await self.release.wait()
                except asyncio.CancelledError:
                    self.cancelled = True
                    raise
                return await ResultWorker.search(
                    self,
                    provider_query,
                    turn_id=turn_id,
                    origin_epoch=origin_epoch,
                )

        worker = SlowWorker()
        config = Config(multi_intent_wait_timeout_ms=1)
        registry = PendingRegistry(worker, config)
        coordinator = WorkItemCoordinator(registry=registry, config=config)
        tts = FakeTTS()
        host = SessionHost(
            registry=registry,
            runner_factory=LifecycleRunner,
            coordinator=coordinator,
            tts=tts,
            promotion_manifest=PromotionManifest(promotion_eligible=True),
        )
        coordinator.add_worker_clarification(
            session_id=host.state.session_id,
            worker_id="worker-search",
            turn_id="turn-original",
            result_id="result-question",
            original_query="What's the weather like?",
            question="Which location should I use?",
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()

        timeout_result = await host._handle_transcript("Riga")
        await worker.started.wait()
        assert "continue in the background" in timeout_result.text
        assert worker.cancelled is False

        worker.release.set()
        for _ in range(5):
            await asyncio.sleep(0)

        history = host.state.result_history("worker-search")
        assert len(history) == 2
        assert history[-1].text.startswith("Answer for Original request:")
        late_progress = [
            progress
            for progress in host.state.speech.values()
            if progress.result_id == history[-1].result_id
        ]
        assert len(late_progress) == 1
        assert late_progress[0].state.value == "queued"
        assert sum(isinstance(frame, TTSSpeakFrame) for frame in connection.worker.frames) == 1

        assert connection.scheduler.active is not None
        foreground_utterance_id = connection.scheduler.active.item.utterance_id
        foreground_token = connection.scheduler.active.token
        assert tts.on_event is not None
        await tts.on_event("synthesis_started", foreground_utterance_id)
        await tts.on_event("synthesis_ended", foreground_utterance_id)
        await release_lifecycle_slot(connection, foreground_token, foreground_utterance_id)

        spoken_frames = [
            frame for frame in connection.worker.frames if isinstance(frame, TTSSpeakFrame)
        ]
        assert len(spoken_frames) == 2
        assert spoken_frames[-1].text == history[-1].spoken_text
        await host.shutdown()

    asyncio.run(run())


def test_late_result_supersedes_queued_timeout_speech_before_it_starts() -> None:
    async def run() -> None:
        tts = FakeTTS()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=tts,
            promotion_manifest=PromotionManifest(promotion_eligible=True),
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()

        active = connection.scheduler.enqueue(
            result_id="result-active",
            work_item_id="work-active",
            run_id="run-active",
            text="Helsinki weather",
            origin_epoch=1,
        )
        await connection.scheduler.start_next()

        timeout = GroundedResult(
            result_id="result-timeout",
            worker_id="worker-search",
            turn_id="turn-sf",
            text="That is taking longer than expected; I will continue in the background.",
            spoken_text="That is taking longer than expected; I will continue in the background.",
            origin_epoch=1,
        )
        await host._commit_and_speak(timeout, connection, role=ROLE_TIMEOUT_NOTICE)
        stale = connection.scheduler._queues["work-turn-sf"][0]
        assert stale.role == ROLE_TIMEOUT_NOTICE

        final = GroundedResult(
            result_id="result-final",
            worker_id="worker-search",
            turn_id="turn-sf",
            text="The current temperature in San Francisco is 69 degrees Fahrenheit.",
            spoken_text="The current temperature in San Francisco is 69 degrees Fahrenheit.",
            origin_epoch=1,
        )
        await host.commit_late_result_once(
            LateDeliveryContext(
                turn_id="turn-sf",
                work_item_id="work-turn-sf",
                origin_epoch=1,
                ack_timestamp=None,
                accepted_turn_sequence=host._turn_sequence,
            ),
            LateResult(
                work_item_id="work-turn-sf",
                worker_id="worker-search",
                result=final,
            ),
        )

        assert connection.scheduler.active is not None
        assert connection.scheduler.active.item == active
        assert host.state.speech[stale.utterance_id].state.value == "interrupted"
        queued = connection.scheduler._queues["work-turn-sf"]
        assert len(queued) == 1
        assert queued[0].result_id == final.result_id

        assert tts.on_event is not None
        assert connection.scheduler.active is not None
        active_token = connection.scheduler.active.token
        await tts.on_event("synthesis_started", active.utterance_id)
        await tts.on_event("synthesis_ended", active.utterance_id)
        await release_lifecycle_slot(connection, active_token, active.utterance_id)

        spoken_frames = [
            frame for frame in connection.worker.frames if isinstance(frame, TTSSpeakFrame)
        ]
        assert [frame.text for frame in spoken_frames] == [
            "Helsinki weather",
            final.spoken_text,
        ]
        await host.shutdown()

    asyncio.run(run())


def test_reported_race_final_result_removes_only_bs_own_queued_notice_without_interrupting_a() -> (
    None
):
    """The reported two-query race: A already occupies the proven transport
    slot (admitted through the real lifecycle coordinator, matching the
    admission path exercised in Phase 1). B has a mixed same-work queue with
    non-supersedable speech on both sides of its timeout notice, and B's
    final result arrives before A stops. Only B's notice may terminate, B's
    other same-work items must keep their order, another work item's queue
    must be untouched, the notice-discard and final-enqueue must happen with
    no ``start_next()`` landing between them, and A must not be interrupted.
    """

    async def run() -> None:
        tts = FakeTTS()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=tts,
            promotion_manifest=PromotionManifest(promotion_eligible=True),
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        scheduler = connection.scheduler

        a_item = scheduler.enqueue(
            result_id="result-a",
            work_item_id="work-a",
            run_id="run-a",
            text="Helsinki weather",
            origin_epoch=1,
        )
        await scheduler.start_next()
        assert scheduler.active is not None and scheduler.active.item == a_item
        assert scheduler.lifecycle is not None and scheduler.lifecycle.occupied is True

        before_notice = scheduler.enqueue(
            result_id="result-b-before",
            work_item_id="work-b",
            run_id="run-b-before",
            text="Still checking on that.",
            origin_epoch=1,
            role=ROLE_RESULT,
        )
        notice = scheduler.enqueue(
            result_id="result-b-notice",
            work_item_id="work-b",
            run_id="run-b-notice",
            text="That is taking longer than expected; I will continue in the background.",
            origin_epoch=1,
            role=ROLE_TIMEOUT_NOTICE,
        )
        after_notice = scheduler.enqueue(
            result_id="result-b-after",
            work_item_id="work-b",
            run_id="run-b-after",
            text="One more update.",
            origin_epoch=1,
            role=ROLE_RESULT,
        )
        other_item = scheduler.enqueue(
            result_id="result-other",
            work_item_id="work-other",
            run_id="run-other",
            text="Unrelated speech.",
            origin_epoch=1,
        )
        other_queue_before = list(scheduler._queues["work-other"])

        call_order: list[str] = []
        real_discard_queued_notice = scheduler.discard_queued_notice
        real_enqueue = scheduler.enqueue
        real_start_next = scheduler.start_next

        def tracked_discard_queued_notice(work_item_id: str) -> tuple[object, ...]:
            call_order.append("discard_queued_notice")
            return real_discard_queued_notice(work_item_id)

        def tracked_enqueue(**kwargs: object) -> object:
            call_order.append("enqueue")
            return real_enqueue(**kwargs)

        async def tracked_start_next(work_item_id: str | None = None) -> object:
            call_order.append("start_next")
            return await real_start_next(work_item_id)

        scheduler.discard_queued_notice = tracked_discard_queued_notice  # type: ignore[method-assign]
        scheduler.enqueue = tracked_enqueue  # type: ignore[method-assign]
        scheduler.start_next = tracked_start_next  # type: ignore[method-assign]

        final = GroundedResult(
            result_id="result-b-final",
            worker_id="worker-search",
            turn_id="turn-b",
            text="The current temperature in San Francisco is 69 degrees Fahrenheit.",
            spoken_text="The current temperature in San Francisco is 69 degrees Fahrenheit.",
            origin_epoch=1,
        )
        await host.commit_late_result_once(
            LateDeliveryContext(
                turn_id="turn-b",
                work_item_id="work-b",
                origin_epoch=1,
                ack_timestamp=None,
                accepted_turn_sequence=host._turn_sequence,
            ),
            LateResult(work_item_id="work-b", worker_id="worker-search", result=final),
        )

        # No start_next() lands between the notice discard and the final
        # result's enqueue: the discard and enqueue are adjacent in the call
        # log, and the only start_next() is the one issued after both.
        discard_at = call_order.index("discard_queued_notice")
        enqueue_at = call_order.index("enqueue")
        assert enqueue_at == discard_at + 1
        assert "start_next" not in call_order[discard_at:enqueue_at]

        # Only B's notice terminates, and it terminates exactly once.
        assert host.state.speech[notice.utterance_id].state.value == "interrupted"
        notice_history = host.state.speech_history(notice.utterance_id)
        assert sum(progress.state.value == "interrupted" for progress in notice_history) == 1

        # B's remaining same-work items retain order, with the final result
        # appended after them.
        remaining = scheduler._queues["work-b"]
        assert [item.result_id for item in remaining] == [
            before_notice.result_id,
            after_notice.result_id,
            final.result_id,
        ]

        # Another work item's queue is untouched.
        assert scheduler._queues["work-other"] == other_queue_before
        assert other_item.result_id == "result-other"

        # A is not interrupted and still owns the transport slot.
        assert scheduler.active is not None
        assert scheduler.active.item == a_item
        assert host.state.speech[a_item.utterance_id].state.value != "interrupted"

        await host.shutdown()

    asyncio.run(run())


def test_late_result_from_replaced_epoch_remains_display_only() -> None:
    async def run() -> None:
        tts = FakeTTS()
        host = SessionHost(runner_factory=LifecycleRunner, tts=tts)
        first = await host.connect(connection_handshake(host, 1))
        first.worker = QueueingPipelineWorker()
        replacement = await host.connect(connection_handshake(host, 2))
        replacement.worker = QueueingPipelineWorker()

        result = GroundedResult(
            result_id="result-old-epoch",
            worker_id="worker-search",
            turn_id="turn-old-epoch",
            text="Old complete answer",
            spoken_text="Old spoken answer",
            origin_epoch=1,
        )
        await host.commit_late_result_once(
            LateDeliveryContext(
                turn_id="turn-old-epoch",
                work_item_id="work-old-epoch",
                origin_epoch=1,
                ack_timestamp=None,
                accepted_turn_sequence=host._turn_sequence,
            ),
            LateResult(
                work_item_id="work-old-epoch",
                worker_id="worker-search",
                result=result,
            ),
        )

        assert host.state.result_history("worker-search")[-1] == result
        assert not any(
            progress.result_id == result.result_id for progress in host.state.speech.values()
        )
        assert not any(isinstance(frame, TTSSpeakFrame) for frame in replacement.worker.frames)
        await host.shutdown()

    asyncio.run(run())


def test_duplicate_late_result_callback_commits_and_enqueues_speech_once() -> None:
    async def run() -> None:
        tts = FakeTTS()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=tts,
            promotion_manifest=PromotionManifest(promotion_eligible=True),
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        result = GroundedResult(
            result_id="result-late-once",
            worker_id="worker-search",
            turn_id="turn-late-once",
            text="Complete late answer",
            spoken_text="Spoken late answer",
            origin_epoch=1,
        )
        late = LateResult(
            work_item_id="work-late-once",
            worker_id="worker-search",
            result=result,
        )
        context = LateDeliveryContext(
            turn_id="turn-late-once",
            work_item_id="work-late-once",
            origin_epoch=1,
            ack_timestamp=None,
            accepted_turn_sequence=host._turn_sequence,
        )

        await host.commit_late_result_once(context, late)
        await host.commit_late_result_once(context, late)

        assert host.state.result_history("worker-search") == (result,)
        assert (
            sum(
                item.role == "assistant" and item.turn_id == result.turn_id
                for item in host.state.transcript
            )
            == 1
        )
        assert (
            sum(progress.result_id == result.result_id for progress in host.state.speech.values())
            == 1
        )
        spoken_frames = [
            frame for frame in connection.worker.frames if isinstance(frame, TTSSpeakFrame)
        ]
        assert len(spoken_frames) == 1
        assert spoken_frames[0].text == result.spoken_text
        await host.shutdown()

    asyncio.run(run())


def test_unimplemented_worker_type_becomes_a_safe_canonical_result() -> None:
    async def run() -> None:
        class UnsupportedCoordinator:
            def arbitrate(self, _session_id: str, transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {"kind": "routed", "decision": object(), "transcript": transcript},
                )()

            def dispatch(self, _decision: object) -> object:
                raise UnsupportedWorkerType("unsupported worker type: calendar")

        host = SessionHost(runner_factory=LifecycleRunner, coordinator=UnsupportedCoordinator())
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("private calendar")

        assert result.text == "I cannot access that capability here."
        assert host.state.result_history("main") == (result,)
        await host.shutdown()

    asyncio.run(run())


def test_router_provider_failure_becomes_a_safe_canonical_result() -> None:
    async def run() -> None:
        class FailingCoordinator:
            def arbitrate(self, _session_id: str, _transcript: str) -> object:
                raise RuntimeError("provider detail must stay in server logs")

        host = SessionHost(runner_factory=LifecycleRunner, coordinator=FailingCoordinator())
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("search for India's historical capitals")

        assert result.text == "Routing is temporarily unavailable. Please try that request again."
        assert "provider detail" not in result.text
        assert host.state.result_history("main") == (result,)
        await host.shutdown()

    asyncio.run(run())


def test_cancel_control_interrupts_active_speech() -> None:

    async def run() -> None:
        class ControlCoordinator:
            def arbitrate(self, _session_id: str, _transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "control",
                        "decision": None,
                        "work_items": (),
                        "control_action": "cancel",
                    },
                )()

        tts = FakeTTS()
        host = SessionHost(
            runner_factory=LifecycleRunner, tts=tts, coordinator=ControlCoordinator()
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        item = connection.scheduler.enqueue(
            result_id="result-active",
            work_item_id="work-active",
            run_id="run-active",
            text="Active answer",
            origin_epoch=1,
        )
        await connection.scheduler.start_next()
        assert connection.scheduler.active is not None
        interrupted_token = connection.scheduler.active.token

        await host._handle_transcript("cancel")
        await acknowledge_lifecycle_flush(connection, interrupted_token)

        assert host.state.speech[item.utterance_id].state.value == "interrupted"
        assert connection.scheduler.active is not None
        assert connection.scheduler.active.item.result_id != item.result_id
        assert isinstance(connection.worker.frames[-1], TTSSpeakFrame)
        assert isinstance(connection.worker.frames[-2], SpeechGenerationMarkerFrame)
        interruption_index = next(
            index
            for index, frame in enumerate(connection.worker.frames)
            if isinstance(frame, InterruptionFrame)
        )
        speak_index = len(connection.worker.frames) - 1
        assert interruption_index < speak_index
        await host.shutdown()

    asyncio.run(run())


def test_pause_control_stops_active_speech_before_confirmation() -> None:

    async def run() -> None:
        class ControlCoordinator:
            def arbitrate(self, _session_id: str, _transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "control",
                        "decision": None,
                        "work_items": (),
                        "control_action": "pause",
                    },
                )()

        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=FakeTTS(),
            coordinator=ControlCoordinator(),
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        connection.scheduler.enqueue(
            result_id="result-active",
            work_item_id="work-active",
            run_id="run-active",
            text="Active answer",
            origin_epoch=1,
        )
        await connection.scheduler.start_next()
        assert connection.scheduler.active is not None
        paused_token = connection.scheduler.active.token

        await host._handle_transcript("pause")
        await acknowledge_lifecycle_flush(connection, paused_token)

        assert isinstance(connection.worker.frames[-1], TTSSpeakFrame)
        assert connection.worker.frames[-1].text == "Pausing the active response."
        assert isinstance(connection.worker.frames[-2], SpeechGenerationMarkerFrame)
        interruption_index = next(
            index
            for index, frame in enumerate(connection.worker.frames)
            if isinstance(frame, InterruptionFrame)
        )
        speak_index = len(connection.worker.frames) - 1
        assert interruption_index < speak_index
        await host.shutdown()

    asyncio.run(run())


def test_pause_targeting_a_queued_item_does_not_tombstone_the_active_speech() -> None:
    async def run() -> None:
        class ControlCoordinator:
            def arbitrate(self, _session_id: str, _transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "control",
                        "decision": None,
                        "work_items": ("work-queued",),
                        "control_action": "pause",
                    },
                )()

        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=FakeTTS(),
            coordinator=ControlCoordinator(),
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        active = connection.scheduler.enqueue(
            result_id="result-active",
            work_item_id="work-active",
            run_id="run-active",
            text="Active answer",
            origin_epoch=1,
        )
        await connection.scheduler.start_next()
        active_token = connection.scheduler.active.token
        connection.scheduler.enqueue(
            result_id="result-queued",
            work_item_id="work-queued",
            run_id="run-queued",
            text="Queued answer",
            origin_epoch=1,
        )

        await host._handle_transcript("pause work-queued")

        assert connection.scheduler.active is not None
        assert connection.scheduler.active.item.work_item_id == "work-active"
        assert connection.scheduler.active.item.utterance_id == active.utterance_id
        assert connection.lifecycle is not None
        generation = connection.lifecycle.generation_for_token(active_token)
        assert generation is not None
        assert generation.tombstoned is False
        await host.shutdown()

    asyncio.run(run())


def test_late_tts_callback_cannot_complete_replacement_utterance() -> None:
    async def run() -> None:
        tts = FakeTTS()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=tts,
            coordinator=RoutedCoordinator(ResultWorker()),
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        old = connection.scheduler.enqueue(
            result_id="result-old",
            work_item_id="work-old",
            run_id="run-old",
            text="Old answer",
            origin_epoch=1,
        )
        await connection.scheduler.start_next()
        await tts.on_event("synthesis_started", "context-old")
        assert connection.scheduler.active is not None
        old_token = connection.scheduler.active.token
        connection.scheduler.pause("work-old")

        replacement = connection.scheduler.enqueue(
            result_id="result-new",
            work_item_id="work-new",
            run_id="run-new",
            text="New answer",
            origin_epoch=1,
        )
        await connection.scheduler.start_next()
        await acknowledge_lifecycle_flush(connection, old_token)
        await tts.on_event("synthesis_started", "context-new")
        events_before_stale_callback = host.state.events

        await tts.on_event("synthesis_ended", "context-old")

        assert connection.scheduler.active is not None
        assert connection.scheduler.active.item == replacement
        assert host.state.speech[replacement.utterance_id].state.value == "started"
        assert host.state.speech[old.utterance_id].state.value == "paused"
        assert host.state.events == events_before_stale_callback
        await host.shutdown()

    asyncio.run(run())


def test_late_tts_start_before_pause_does_not_bind_replacement_utterance() -> None:
    async def run() -> None:
        tts = FakeTTS()
        host = SessionHost(runner_factory=LifecycleRunner, tts=tts)
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        old = connection.scheduler.enqueue(
            result_id="result-old",
            work_item_id="work-old",
            run_id="run-old",
            text="Old answer",
            origin_epoch=1,
        )
        await connection.scheduler.start_next()
        assert connection.scheduler.active is not None
        old_token = connection.scheduler.active.token
        connection.scheduler.pause("work-old")
        replacement = connection.scheduler.enqueue(
            result_id="result-new",
            work_item_id="work-new",
            run_id="run-new",
            text="New answer",
            origin_epoch=1,
        )
        await connection.scheduler.start_next()
        await acknowledge_lifecycle_flush(connection, old_token)

        await tts.on_event("synthesis_started", "context-old")
        await tts.on_event("synthesis_ended", "context-old")

        assert connection.scheduler.active is not None
        assert connection.scheduler.active.item == replacement
        assert host.state.speech[old.utterance_id].state.value == "paused"
        assert host.state.speech[replacement.utterance_id].state.value == "started"
        await host.shutdown()

    asyncio.run(run())


def test_resume_control_requeues_and_starts_targeted_paused_item() -> None:
    async def run() -> None:
        class ControlCoordinator:
            def arbitrate(self, _session_id: str, _transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "control",
                        "decision": None,
                        "work_items": ("work-active",),
                        "control_action": "resume",
                    },
                )()

        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=FakeTTS(),
            coordinator=ControlCoordinator(),
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        original = connection.scheduler.enqueue(
            result_id="result-active",
            work_item_id="work-active",
            run_id="run-active",
            text="Active answer",
            origin_epoch=1,
        )
        await connection.scheduler.start_next()
        assert connection.scheduler.active is not None
        paused_token = connection.scheduler.active.token
        connection.scheduler.pause("work-active")

        await host._handle_transcript("resume work-active")
        await acknowledge_lifecycle_flush(connection, paused_token)

        assert connection.scheduler.paused("work-active") is None
        assert connection.scheduler.active is not None
        assert connection.scheduler.active.item.work_item_id == "work-active"
        assert connection.scheduler.active.item.utterance_id != original.utterance_id
        assert connection.worker.frames[-1].text == "Active answer"
        await host.shutdown()

    asyncio.run(run())


def test_cancel_fences_cancellation_resistant_worker_result_and_speech() -> None:
    async def run() -> None:
        class ResistantWorker(ResultWorker):
            def __init__(self) -> None:
                self.started = asyncio.Event()
                self.cancelled = asyncio.Event()

            async def search(
                self, query: str, *, turn_id: str, origin_epoch: int | None
            ) -> GroundedResult:
                self.started.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    self.cancelled.set()
                    return await super().search(
                        query,
                        turn_id=turn_id,
                        origin_epoch=origin_epoch,
                    )

        class CancellableCoordinator(RoutedCoordinator):
            def arbitrate(self, _session_id: str, transcript: str) -> object:
                if transcript.startswith("cancel"):
                    return type(
                        "Outcome",
                        (),
                        {
                            "kind": "control",
                            "decision": None,
                            "work_items": ("work-turn-1",),
                            "control_action": "cancel",
                        },
                    )()
                return super().arbitrate(_session_id, transcript)

        worker = ResistantWorker()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=FakeTTS(),
            coordinator=CancellableCoordinator(worker),
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        pending = asyncio.create_task(host._handle_transcript("slow search"))
        await worker.started.wait()

        await host._handle_transcript("cancel work-turn-1")
        with pytest.raises(asyncio.CancelledError):
            await pending

        assert worker.cancelled.is_set()
        assert host.state.result_history("worker-search") == ()
        assert all(
            not isinstance(frame, TTSSpeakFrame) or frame.text != "Spoken answer for slow search"
            for frame in connection.worker.frames
        )
        await host.shutdown()

    asyncio.run(run())


def test_cancel_fences_a_turn_while_router_arbitration_is_still_running() -> None:
    async def run() -> None:
        class BlockingRouterCoordinator(RoutedCoordinator):
            def __init__(self) -> None:
                super().__init__(ResultWorker())
                self.routing_started = threading.Event()
                self.release_routing = threading.Event()

            def arbitrate(self, _session_id: str, transcript: str) -> object:
                if transcript == "cancel":
                    return type(
                        "Outcome",
                        (),
                        {
                            "kind": "control",
                            "decision": None,
                            "work_items": (),
                            "control_action": "cancel",
                        },
                    )()
                self.routing_started.set()
                self.release_routing.wait(timeout=2)
                return super().arbitrate(_session_id, transcript)

        coordinator = BlockingRouterCoordinator()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=FakeTTS(),
            coordinator=coordinator,
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        pending = asyncio.create_task(host._handle_transcript("slow route"))
        await asyncio.to_thread(coordinator.routing_started.wait, 1)

        cancellation = await host._handle_transcript("cancel")
        coordinator.release_routing.set()
        with pytest.raises(asyncio.CancelledError):
            await pending

        assert cancellation.text == "Cancelling the active response."
        assert host.state.result_history("worker-search") == ()
        await host.shutdown()

    asyncio.run(run())


def test_unknown_cancel_target_does_not_poison_future_work_or_accumulate_state() -> None:
    async def run() -> None:
        host = SessionHost(runner_factory=LifecycleRunner)
        origin = await host.connect(connection_handshake(host, 1))

        for index in range(100):
            host._cancel_work(f"work-turn-{index}")

        assert host._cancelled_work_items == set()
        future = GroundedResult(
            result_id="future-result",
            worker_id="worker-search",
            turn_id="turn-50",
            text="Future answer",
            spoken_text="Future answer",
            origin_epoch=1,
        )
        await host._commit_and_speak(future, origin)
        assert host.state.result_history("worker-search") == (future,)
        await host.shutdown()

    asyncio.run(run())


def test_concurrent_registration_of_one_worker_uses_one_runner_operation() -> None:
    async def run() -> None:
        class BlockingRunner:
            def __init__(self) -> None:
                self.calls = 0
                self.started = asyncio.Event()
                self.release = asyncio.Event()

            async def add_workers(self, _worker: object) -> None:
                self.calls += 1
                self.started.set()
                await self.release.wait()

        worker = type(
            "Worker",
            (),
            {
                "metadata": type(
                    "Metadata",
                    (),
                    {"worker_id": "worker-1"},
                )()
            },
        )()
        runner = BlockingRunner()
        host = SessionHost(runner_factory=LifecycleRunner)
        host.runner = runner

        first = asyncio.create_task(host._register_runner_worker(worker))
        await runner.started.wait()
        second = asyncio.create_task(host._register_runner_worker(worker))
        await asyncio.sleep(0)
        runner.release.set()
        await asyncio.gather(first, second)

        assert runner.calls == 1
        assert host._runner_handles == {"worker-1": worker}
        assert host._runner_registered == {"worker-1"}

    asyncio.run(run())


def test_search_cancellation_cancels_child_without_retaining_it() -> None:
    async def run() -> None:
        class RetainingCoordinator:
            def __init__(self) -> None:
                self.retained: list[asyncio.Task[object]] = []

            def retain_late_task(self, task: asyncio.Task[object], **_: object) -> None:
                self.retained.append(task)

        coordinator = RetainingCoordinator()
        host = SessionHost(coordinator=coordinator)
        started = asyncio.Event()
        child_cancelled = asyncio.Event()

        async def search(_query: str, *, turn_id: str, origin_epoch: int | None) -> GroundedResult:
            started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                child_cancelled.set()
                raise

        parent = asyncio.create_task(
            host._search_with_timeout(
                search,
                "query",
                turn_id="turn-cancelled",
                origin_epoch=1,
                timeout=30,
                worker_id="worker-search",
            )
        )
        await started.wait()
        parent.cancel()
        with pytest.raises(asyncio.CancelledError):
            await parent

        assert child_cancelled.is_set()
        assert coordinator.retained == []

    asyncio.run(run())


def test_multi_intent_preserves_envelope_fallbacks_and_uses_submit() -> None:
    async def run() -> None:
        worker = ResultWorker()

        class Registry:
            config = Config(max_work_items_per_turn=4)

            @staticmethod
            def catalogue() -> object:
                return object()

        class Router:
            @staticmethod
            def route_envelope(text: str, _catalogue: object) -> object:
                action = {
                    "answer directly": "direct",
                    "ask me": "clarify",
                    "unsupported thing": "unsupported",
                    "search it": "existing_worker",
                }[text]
                decision = type("Decision", (), {"action": action})()
                prose = {
                    "direct": "Direct answer.",
                    "clarify": "Which one?",
                    "unsupported": None,
                    "existing_worker": None,
                }[action]
                return type("Envelope", (), {"decision": decision, "prose": prose})()

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=Registry(), router=Router())
                self.submit_calls = 0

            def dispatch(self, decision: object, **_: object) -> object:
                assert decision.action == "existing_worker"
                return worker

            async def submit(self, *args: object, **kwargs: object) -> object:
                self.submit_calls += 1
                return await super().submit(*args, **kwargs)

        coordinator = Coordinator()
        host = SessionHost(runner_factory=LifecycleRunner, coordinator=coordinator)
        origin = await host.connect(connection_handshake(host, 1))
        outcome = type(
            "Outcome",
            (),
            {
                "work_items": (
                    "answer directly",
                    "ask me",
                    "unsupported thing",
                    "search it",
                ),
                "pending_dialogue": None,
            },
        )()

        results = await host._handle_multi_intent(
            outcome,
            "",
            origin,
            "turn-compound",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-compound"),
        )

        assert [result.text for result in results] == [
            "Direct answer.",
            "Which one?",
            "I cannot access that capability here.",
            "Answer for search it",
        ]
        assert coordinator.submit_calls == 1
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_reclarification_preserves_the_original_pending_query() -> None:
    async def run() -> None:
        class ReclarifyingWorker(ContinuationResultWorker):
            async def search(
                self,
                query: str,
                *,
                turn_id: str,
                origin_epoch: int | None,
                clarification_context: ClarificationContext | None = None,
            ) -> GroundedResult:
                assert clarification_context is not None
                raise WorkerClarify("Which date should I use?")

        worker = ReclarifyingWorker()
        registry = PendingRegistry(worker)
        coordinator = WorkItemCoordinator(registry=registry)
        host = SessionHost(
            registry=registry,
            runner_factory=LifecycleRunner,
            coordinator=coordinator,
        )
        coordinator.add_worker_clarification(
            session_id=host.state.session_id,
            worker_id="worker-search",
            turn_id="turn-original",
            result_id="result-question",
            original_query="What's the weather like?",
            question="Which location should I use?",
        )
        origin = await host.connect(connection_handshake(host, 1))
        pending = coordinator.pending(host.state.session_id)
        outcome = type(
            "Outcome",
            (),
            {"work_items": ("Riga",), "pending_dialogue": pending},
        )()

        await host._handle_multi_intent(
            outcome,
            "",
            origin,
            "turn-compound",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-compound"),
        )

        next_pending = coordinator.pending(host.state.session_id)
        assert next_pending is not None
        assert next_pending.original_query == "What's the weather like?"
        assert next_pending.question == "Which date should I use?"
        await host.shutdown()

    asyncio.run(run())


def test_new_dynamic_worker_is_registered_before_search_dispatch() -> None:
    async def run() -> None:
        runner = AsyncAddRunner()

        class RegisteredBeforeSearchWorker(ProjectedResultWorker):
            async def search(self, *args: object, **kwargs: object) -> GroundedResult:
                assert self in runner.added
                return await super().search(*args, **kwargs)

        worker = RegisteredBeforeSearchWorker()
        host = SessionHost(
            runner_factory=lambda: runner,
            coordinator=ProjectedCoordinator(worker),
        )
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("historical capitals")

        assert result.text == "Answer for historical capitals"
        assert runner.added == [worker]
        assert host._runner_handles["worker-search"] is worker
        await host.shutdown()

    asyncio.run(run())


def test_direct_search_timeout_transfers_to_background_and_commits_late_result() -> None:
    async def run() -> None:
        worker = BlockingResultWorker()
        worker.metadata = type(
            "Metadata",
            (),
            {
                "worker_id": "worker-search",
                "topic": "slow search",
                "model_policy": "deep",
            },
        )()

        class RetainingRoutedCoordinator(RoutedCoordinator):
            def __init__(self) -> None:
                super().__init__(worker)
                self.owner = WorkItemCoordinator(
                    config=Config(foreground_search_timeout_seconds=0.001)
                )
                self.config = self.owner.config

            def start_task(self, operation: object) -> asyncio.Task[object] | None:
                return self.owner.start_task(operation)

            def retain_late_task(self, task: asyncio.Task[object], **kwargs: object) -> bool:
                return self.owner.retain_late_task(task, **kwargs)

            async def shutdown(self) -> None:
                await self.owner.shutdown()

        coordinator = RetainingRoutedCoordinator()
        host = SessionHost(
            registry=WorkerRegistry(config=coordinator.config),
            config=coordinator.config,
            runner_factory=LifecycleRunner,
            coordinator=coordinator,
        )
        await host.connect(connection_handshake(host, 1))

        foreground = await host._handle_transcript("slow query")

        assert "continue in the background" in foreground.text
        worker.release.set()
        for _ in range(10):
            await asyncio.sleep(0)
            if len(host.state.result_history("worker-search")) == 2:
                break

        history = host.state.result_history("worker-search")
        assert len(history) == 2
        assert history[-1].text == "Answer for slow query"
        assert host.state.workers["worker-search"].latest_result_id == history[-1].result_id
        await host.shutdown()

    asyncio.run(run())


def test_rejected_direct_search_does_not_claim_background_continuation() -> None:
    async def run() -> None:
        worker = BlockingResultWorker()

        class RejectingCoordinator(RoutedCoordinator):
            config = Config(foreground_search_timeout_seconds=0.001)

            def start_task(self, operation: object) -> asyncio.Task[object]:
                return asyncio.create_task(operation)

            def retain_late_task(self, task: asyncio.Task[object], **_: object) -> bool:
                task.cancel()
                return False

        host = SessionHost(
            registry=WorkerRegistry(config=RejectingCoordinator.config),
            config=RejectingCoordinator.config,
            runner_factory=LifecycleRunner,
            coordinator=RejectingCoordinator(worker),
        )
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("slow query")

        assert result.text == "The search service is busy; please try again shortly."
        assert "background" not in result.text
        await asyncio.sleep(0)
        await host.shutdown()

    asyncio.run(run())


def test_late_worker_error_log_omits_untrusted_exception_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> None:
        messages: list[str] = []
        monkeypatch.setattr(pipeline_module.logger, "warning", messages.append)
        host = SessionHost()

        await host.commit_late_result_once(
            LateDeliveryContext(
                turn_id="turn-1",
                work_item_id="work-1",
                origin_epoch=1,
                ack_timestamp=None,
                accepted_turn_sequence=host._turn_sequence,
            ),
            LateResult(
                work_item_id="work-1",
                worker_id="worker-search",
                error="ProviderError: secret-token\nforged log line",
            ),
        )

        assert messages == ["Late worker result failed for work_item=work-1 worker=worker-search"]

    asyncio.run(run())


def test_session_shutdown_closes_coordinator() -> None:
    async def run() -> None:
        class Coordinator:
            def __init__(self) -> None:
                self.closed = False

            async def shutdown(self) -> None:
                self.closed = True

        coordinator = Coordinator()
        host = SessionHost(coordinator=coordinator)

        await host.shutdown()

        assert coordinator.closed is True

    asyncio.run(run())


def test_session_shutdown_fences_connection_before_coordinator() -> None:
    async def run() -> None:
        events: list[str] = []

        class Connection:
            def deactivate(self, *, reconnect: bool) -> None:
                assert reconnect is False

            async def shutdown(self, *, reason: str) -> None:
                assert reason == "session shutdown"
                events.append("connection")

        class Coordinator:
            async def shutdown(self) -> None:
                events.append("coordinator")

        host = SessionHost(coordinator=Coordinator())
        host.connection = Connection()  # type: ignore[assignment]

        await host.shutdown()

        assert events == ["connection", "coordinator"]

    asyncio.run(run())


def test_successful_result_without_tts_is_history_only() -> None:
    async def run() -> None:
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=RoutedCoordinator(ResultWorker()),
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()

        result = await host._handle_transcript("text only")

        assert host.state.result_history("worker-search") == (result,)
        assert connection.worker.frames == []
        assert connection.scheduler._queues == {}
        assert host.state.speech == {}
        await host.shutdown()

    asyncio.run(run())


class AsyncCancelRunner:
    def __init__(self) -> None:
        self.cancelled = False

    async def cancel(self, reason: str) -> None:
        await asyncio.sleep(0)
        self.cancelled = reason


def test_shutdown_awaits_async_cancel_fallback() -> None:
    async def run() -> None:
        runner = AsyncCancelRunner()
        host = SessionHost(runner_factory=lambda: runner)
        await host.start()
        await host.shutdown()
        assert runner.cancelled == "session shutdown"

    asyncio.run(run())


class HangingRunRunner:
    def __init__(self) -> None:
        self.cancelled = False
        self.finished = asyncio.Event()

    async def run(self, *, auto_end: bool) -> None:
        try:
            await asyncio.Future()
        finally:
            self.finished.set()

    async def cancel(self, reason: str) -> None:
        self.cancelled = reason


def test_shutdown_cancels_owned_runner_task() -> None:
    async def run() -> None:
        runner = HangingRunRunner()
        host = SessionHost(runner_factory=lambda: runner)
        await host.start()
        await asyncio.sleep(0)

        await asyncio.wait_for(host.shutdown(), timeout=1)

        assert runner.cancelled == "session shutdown"
        assert runner.finished.is_set()

    asyncio.run(run())


def handshake(host: SessionHost, epoch: int) -> dict[str, object]:
    return {
        "session_id": host.state.session_id,
        "resume_token": host.state.resume_token,
        "proposed_epoch": epoch,
        "snapshot_sequence": 0,
    }


def test_session_handshake_tokens_are_pruned_and_bounded() -> None:
    host = SessionHost(runner_factory=LifecycleRunner)
    host._handshake_tokens["expired"] = (1, 0.0, False)

    handshakes = [host.session_handshake() for _ in range(host._MAX_HANDSHAKE_TOKENS + 10)]

    assert "expired" not in host._handshake_tokens
    assert len(host._handshake_tokens) == host._MAX_HANDSHAKE_TOKENS
    assert handshakes[-1]["resume_token"] in host._handshake_tokens


def test_connection_observer_projects_canonical_runtime_events_without_live_services() -> None:
    async def run() -> None:
        host = SessionHost()
        host.state.set_worker(
            WorkerState(
                worker_id="worker-weather", topic="weather", model_policy="deep", status="idle"
            )
        )
        await host.connect(handshake(host, 1))
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

        messages = host.connection.observer.messages()
        assert [message["kind"] for message in messages] == ["result"]
        assert {message["session_id"] for message in messages} == {host.state.session_id}
        assert messages[-1]["data"]["result_id"] == "result-1"
        assert messages[-1]["origin_epoch"] == 1

    asyncio.run(run())


def test_observer_does_not_relabel_old_epoch_events_as_current() -> None:
    async def run() -> None:
        host = SessionHost()
        await host.connect(handshake(host, 2))
        host.state.append_result(
            GroundedResult(
                result_id="old-result",
                worker_id="worker-weather",
                turn_id="turn-old",
                text="Old answer",
                spoken_text="Old answer",
                origin_epoch=1,
            ),
            origin_epoch=1,
        )

        assert host.connection.observer.messages() == ()

    asyncio.run(run())


def test_connection_pipeline_uses_framework_bridge_and_canonical_result_gate() -> None:
    pipeline = build_pipeline(transport=object(), stt=object(), tts=object())

    assert any(isinstance(item, FrameworkBusBridgeProcessor) for item in pipeline.processors)
    assert any(isinstance(item, CanonicalResultAdapter) for item in pipeline.processors)
    assert asyncio.run(pipeline.emit_worker_frame({"kind": "raw_llm_text"})) is False


def test_canonical_adapter_rejects_raw_frames_and_only_admits_downstream_results() -> None:
    adapter = CanonicalResultAdapter()
    assert adapter.accepts({"kind": "raw_llm_text", "text": "leak"}) is False
    frame = {
        "kind": "canonical_result",
        "result_id": "r",
        "worker_id": "w",
        "turn_id": "t",
        "text": "A complete answer.",
        "spoken_text": "A short answer.",
        "citations": [],
    }
    assert adapter.accepts(frame) is True
    assert adapter._normalized_result(frame)["spoken_text"] == "A short answer."


def test_canonical_adapter_forwards_versioned_rtvi_runtime_envelopes() -> None:
    async def run() -> None:
        adapter = CanonicalResultAdapter()
        forwarded: list[object] = []

        async def push(frame: object, _direction: object) -> None:
            forwarded.append(frame)

        adapter.push_frame = push  # type: ignore[method-assign]
        frame = RTVIServerMessageFrame(
            data={
                "contract_version": "v1.0",
                "session_id": "adapter-session",
                "sequence": 4,
                "kind": "runtime_snapshot",
                "data": {
                    "contract_version": "v1.0",
                    "session_id": "adapter-session",
                    "snapshot_sequence": 4,
                    "workers": [],
                    "results": [],
                    "speech_progress": [],
                    "routing": None,
                    "transcript": [],
                    "origin_epoch": 2,
                },
                "origin_epoch": 2,
            }
        )

        assert adapter.accepts(frame)
        await adapter.process_frame(frame, FrameDirection.DOWNSTREAM)
        assert forwarded == [frame]

        raw = RTVIServerMessageFrame(data={"kind": "raw_llm_text", "text": "untrusted"})
        assert not adapter.accepts(raw)
        await adapter.process_frame(raw, FrameDirection.DOWNSTREAM)
        assert forwarded == [frame]

        tts_frame = TTSSpeakFrame(text="hello")
        await adapter.process_frame(tts_frame, FrameDirection.DOWNSTREAM)
        assert forwarded[-1] is tts_frame

        malformed = RTVIServerMessageFrame(
            data={
                "contract_version": "v1.0",
                "session_id": "session-1",
                "sequence": 5,
                "kind": "result",
                "data": {},
                "origin_epoch": 2,
            }
        )
        assert not adapter.accepts(malformed)

    asyncio.run(run())


def test_framework_bridge_keeps_speech_on_connection_pipeline() -> None:
    factories = {
        TTSSpeakFrame: lambda: TTSSpeakFrame(text="hello", append_to_context=False),
        SpeechGenerationMarkerFrame: lambda: SpeechGenerationMarkerFrame(token="t"),
        SpeechGenerationFlushAckFrame: lambda: SpeechGenerationFlushAckFrame(token="t"),
        SnapshotBarrierFlushFrame: lambda: SnapshotBarrierFlushFrame(token="t"),
    }
    assert set(CONNECTION_LOCAL_FRAMES) == set(factories)

    class RecordingBus:
        def __init__(self) -> None:
            self.messages: list[object] = []

        async def send(self, message: object) -> None:
            self.messages.append(message)

    async def run() -> None:
        bus = RecordingBus()
        bridge = framework_bridge(bus=bus, worker_name="browser-1")
        forwarded: list[object] = []

        async def push(frame: object, _direction: object) -> None:
            forwarded.append(frame)

        bridge.push_frame = push  # type: ignore[method-assign]

        constructed = [factories[frame_type]() for frame_type in CONNECTION_LOCAL_FRAMES]
        for frame in constructed:
            await bridge.process_frame(frame, FrameDirection.DOWNSTREAM)

        assert forwarded == constructed
        assert bus.messages == []

    asyncio.run(run())


def test_connection_local_frames_covers_every_private_speech_frame() -> None:
    private_speech_frames = {
        obj
        for obj in vars(server.speech_lifecycle).values()
        if isinstance(obj, type)
        and issubclass(obj, SystemFrame)
        and obj.__module__ == "server.speech_lifecycle"
    }
    assert private_speech_frames <= set(CONNECTION_LOCAL_FRAMES)


def test_final_turn_transcript_waits_for_smart_turn_stop() -> None:
    async def run() -> None:
        routed: list[str] = []
        routed_event = asyncio.Event()
        forwarded: list[object] = []

        async def on_final(text: str) -> None:
            routed.append(text)
            routed_event.set()

        processor = FinalTurnTranscriptProcessor(on_final, complete_grace_seconds=0.01)

        async def push(frame: object, _direction: object) -> None:
            forwarded.append(frame)

        processor.push_frame = push  # type: ignore[method-assign]
        fragments = [
            TranscriptionFrame("Can you look for", "", ""),
            TranscriptionFrame("the capital of", "", ""),
            TranscriptionFrame("India?", "", ""),
        ]

        for fragment in fragments:
            await processor.process_frame(fragment, FrameDirection.DOWNSTREAM)
            await processor.process_frame(VADUserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)

        await asyncio.sleep(0)
        assert routed == []

        await processor.process_frame(UserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
        await processor.process_frame(UserStartedSpeakingFrame(), FrameDirection.DOWNSTREAM)
        await processor.process_frame(
            TranscriptionFrame("over the last two hundred years.", "", ""),
            FrameDirection.DOWNSTREAM,
        )
        await asyncio.sleep(0.02)
        assert routed == []

        await processor.process_frame(UserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
        await asyncio.wait_for(routed_event.wait(), timeout=1)
        assert routed == ["Can you look for the capital of India? over the last two hundred years."]

        await processor.process_frame(UserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
        await asyncio.sleep(0)
        assert routed == ["Can you look for the capital of India? over the last two hundred years."]
        assert not any(isinstance(frame, TranscriptionFrame) for frame in forwarded)
        assert sum(isinstance(frame, UserStoppedSpeakingFrame) for frame in forwarded) == 1

    asyncio.run(run())


def test_final_turn_dispatch_is_backpressured_and_cleanup_cancels_all_tasks() -> None:
    async def run() -> None:
        release = asyncio.Event()
        started = 0

        async def on_final(_text: str) -> None:
            nonlocal started
            started += 1
            await release.wait()

        processor = FinalTurnTranscriptProcessor(
            on_final,
            complete_grace_seconds=0,
            max_pending_dispatches=3,
        )

        async def push(_frame: object, _direction: object) -> None:
            return None

        processor.push_frame = push  # type: ignore[method-assign]

        async def produce_many_turns() -> None:
            for index in range(100):
                await processor.process_frame(
                    TranscriptionFrame(f"turn {index}", "", ""),
                    FrameDirection.DOWNSTREAM,
                )
                await processor.process_frame(
                    UserStoppedSpeakingFrame(),
                    FrameDirection.DOWNSTREAM,
                )
                while (
                    processor._completion_task is not None and not processor._completion_committed
                ):
                    await asyncio.sleep(0)

        producer = asyncio.create_task(produce_many_turns())
        for _ in range(20):
            if started == 3 and processor._completion_committed:
                break
            await asyncio.sleep(0)

        assert started == 3
        assert len(processor._dispatch_tasks) == 3
        assert processor._completion_committed
        assert not producer.done()

        dispatch_tasks = list(processor._dispatch_tasks)
        completion_task = processor._completion_task
        producer.cancel()
        await asyncio.gather(producer, return_exceptions=True)
        await processor.cleanup()

        assert processor._completion_task is None
        assert processor._completion_tasks == set()
        assert processor._dispatch_tasks == set()
        assert completion_task is not None and completion_task.done()
        assert all(task.done() for task in dispatch_tasks)

    asyncio.run(run())


def test_final_turn_transcript_input_is_capped() -> None:
    async def run() -> None:
        routed: list[str] = []
        routed_event = asyncio.Event()

        async def on_final(text: str) -> None:
            routed.append(text)
            routed_event.set()

        processor = FinalTurnTranscriptProcessor(
            on_final,
            complete_grace_seconds=0,
            max_transcript_chars=32,
        )
        processor.push_frame = lambda *_args: asyncio.sleep(0)  # type: ignore[method-assign]

        await processor.process_frame(
            TranscriptionFrame("a" * 20, "", ""),
            FrameDirection.DOWNSTREAM,
        )
        await processor.process_frame(
            TranscriptionFrame("b" * 20, "", ""),
            FrameDirection.DOWNSTREAM,
        )
        await processor.process_frame(UserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
        await asyncio.wait_for(routed_event.wait(), timeout=1)

        assert routed == [f"{'a' * 20} {'b' * 11}"]
        await processor.cleanup()

    asyncio.run(run())


def test_smart_turn_processor_uses_pipecat_semantic_stop_strategy() -> None:
    processor = smart_turn_processor(timeout_seconds=7.5)
    strategies = processor._user_turn_controller.user_turn_strategies

    assert isinstance(processor, UserTurnProcessor)
    assert len(strategies.stop) == 1
    assert isinstance(strategies.stop[0], TurnAnalyzerUserTurnStopStrategy)
    assert isinstance(strategies.stop[0]._turn_analyzer, LocalSmartTurnAnalyzerV3)
    assert processor._user_turn_controller._user_turn_stop_timeout == 7.5


def test_connection_observer_unsubscribe_stops_future_listener_delivery() -> None:
    from server.observers import RuntimeObserver
    from server.session_state import SessionState

    state = SessionState()
    observer = RuntimeObserver(state, epoch=1)
    received: list[object] = []
    observer.subscribe(received.append)
    observer.unsubscribe()
    state.set_worker(WorkerState(worker_id="w", topic="t", model_policy="deep", status="idle"))
    assert received == []


class AsyncAddRunner:
    def __init__(self) -> None:
        self.added: list[object] = []

    def start(self) -> None:
        pass

    async def add_workers(self, *workers: object) -> None:
        self.added.extend(workers)

    def stop(self) -> None:
        pass


class FakeTransport:
    def __init__(self) -> None:
        self.frames: list[object] = []

    def input(self) -> str:
        return "input"

    def output(self) -> str:
        return "output"


class FakeRTVI:
    def __init__(self) -> None:
        self.handlers: dict[str, object] = {}
        self.bot_ready = False
        self.bot_ready_calls = 0

    def event_handler(self, name: str):
        def register(function: object) -> object:
            self.handlers[name] = function
            return function

        return register

    async def set_bot_ready(self) -> None:
        self.bot_ready = True
        self.bot_ready_calls += 1


class FakeTurnTrackingObserver:
    """Stands in for Pipecat's default ``worker.turn_tracking_observer``."""

    def __init__(self) -> None:
        self.handlers: dict[str, object] = {}

    def event_handler(self, name: str):
        def register(function: object) -> object:
            self.handlers[name] = function
            return function

        return register


class FakePipelineWorker:
    def __init__(self, pipeline: object, **_: object) -> None:
        self.pipeline = pipeline
        self.rtvi = FakeRTVI()
        self.turn_tracking_observer = FakeTurnTrackingObserver()

    async def queue_frame(self, _frame: object) -> None:
        pass


@pytest.mark.usefixtures("monkeypatch")
def test_connection_attach_registers_worker_with_async_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> None:
        runner = AsyncAddRunner()
        stt = object()
        host = SessionHost(
            runner_factory=lambda: runner,
            stt=stt,
            coordinator=RoutedCoordinator(ResultWorker()),
        )
        host.registry.config = Config(smart_turn_timeout_seconds=7.5)
        await host.start()
        transport = FakeTransport()
        pipeline_args: list[object] = []
        vad_analyzer = object()
        vad_processor = object()
        turn_processor = object()
        turn_timeouts: list[float] = []
        completion_graces: list[float] = []
        transcript_processor = object()
        transcript_callbacks: list[object] = []

        monkeypatch.setattr(app_module, "SmallWebRTCTransport", lambda *_args: transport)
        monkeypatch.setattr(
            app_module,
            "SileroVADAnalyzer",
            lambda *, sample_rate: vad_analyzer if sample_rate == 16000 else None,
        )
        monkeypatch.setattr(
            app_module,
            "VADProcessor",
            lambda *, vad_analyzer: vad_processor if vad_analyzer is not None else None,
        )
        monkeypatch.setattr(
            app_module,
            "smart_turn_processor",
            lambda *, timeout_seconds: turn_timeouts.append(timeout_seconds) or turn_processor,
        )
        monkeypatch.setattr(
            app_module,
            "FinalTurnTranscriptProcessor",
            lambda callback, *, complete_grace_seconds: (
                transcript_callbacks.append(callback)
                or completion_graces.append(complete_grace_seconds)
                or transcript_processor
            ),
        )
        monkeypatch.setattr(app_module, "TransportParams", lambda **kwargs: kwargs)
        monkeypatch.setattr(app_module, "PipelineParams", lambda **kwargs: kwargs)
        monkeypatch.setattr(
            app_module,
            "Pipeline",
            lambda processors: pipeline_args.append(processors) or processors,
        )
        monkeypatch.setattr(app_module, "PipelineWorker", FakePipelineWorker)

        await app_module._attach_connection(
            host,
            object(),
            app_module.SnapshotHandshake(
                session_id=host.state.session_id,
                resume_token=host.state.resume_token,
                proposed_epoch=1,
                snapshot_sequence=0,
            ),
        )

        assert len(runner.added) == 1
        assert runner.added[0] is host.connection.worker
        assert pipeline_args
        assert pipeline_args[0][:5] == [
            "input",
            vad_processor,
            stt,
            turn_processor,
            transcript_processor,
        ]
        assert turn_timeouts == [7.5]
        assert completion_graces == [1.5]
        assert len(transcript_callbacks) == 1
        assert callable(transcript_callbacks[0])
        result = await transcript_callbacks[0]("Riga weather")
        assert result.text == "Answer for Riga weather"
        assert any(isinstance(item, FrameworkBusBridgeProcessor) for item in pipeline_args[0])
        assert any(isinstance(item, CanonicalResultAdapter) for item in pipeline_args[0])

        rtvi = runner.added[0].rtvi
        await rtvi.handlers["on_client_ready"](rtvi)
        await rtvi.handlers["on_client_ready"](rtvi)
        # PipelineWorker owns the framework bot-ready handler. The app handler
        # only publishes the application-level client-ready state.
        assert rtvi.bot_ready is False
        assert rtvi.bot_ready_calls == 0


# ---------------------------------------------------------------------------
# Phase 1: framework observer identity, console-only staleness, and
# real-PipelineWorker parity between enable_metrics=False and True.
# ---------------------------------------------------------------------------


def _monkeypatch_attach_connection_scaffolding(
    monkeypatch: pytest.MonkeyPatch, *, pipeline_args: list[object]
) -> None:
    """Same passthrough monkeypatching as the async-runner attach test above,
    but leaves StartupTimingObserver/UserBotLatencyObserver as the real
    Pipecat classes so real PERF_METRIC handler closures get registered.
    """
    monkeypatch.setattr(app_module, "SmallWebRTCTransport", lambda *_args: FakeTransport())
    monkeypatch.setattr(app_module, "SileroVADAnalyzer", lambda *, sample_rate: object())
    monkeypatch.setattr(app_module, "VADProcessor", lambda *, vad_analyzer: object())
    monkeypatch.setattr(app_module, "smart_turn_processor", lambda *, timeout_seconds: object())
    monkeypatch.setattr(
        app_module,
        "FinalTurnTranscriptProcessor",
        lambda callback, *, complete_grace_seconds: object(),
    )
    monkeypatch.setattr(app_module, "TransportParams", lambda **kwargs: kwargs)
    monkeypatch.setattr(app_module, "PipelineParams", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        app_module, "Pipeline", lambda processors: pipeline_args.append(processors) or processors
    )
    monkeypatch.setattr(app_module, "PipelineWorker", FakePipelineWorker)


class _NonRetainingAddRunner:
    """Like AsyncAddRunner, but does not keep a permanent list of added
    workers — a real WorkerRunner does not hold connection-replaced workers
    alive either, so a non-retaining test double is required to observe
    garbage collection of the replaced connection's worker.
    """

    async def start(self) -> None:
        pass

    async def add_workers(self, *_workers: object) -> None:
        pass

    async def stop(self) -> None:
        pass


def test_stale_epoch_observer_callbacks_stay_console_only_and_become_collectible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Old-epoch framework observers survive only as console-only, collectible
    callbacks after connection replacement (Acceptance Criteria: "Observer
    callbacks retain no replaced host/runtime/worker/publisher state; old
    connection objects are collectible and stale callbacks only reach the
    console sink.")
    """
    import gc
    import weakref

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=_NonRetainingAddRunner,
            coordinator=RoutedCoordinator(ResultWorker()),
            measurement_sink=sink,
        )
        await host.start()

        pipeline_args: list[object] = []
        _monkeypatch_attach_connection_scaffolding(monkeypatch, pipeline_args=pipeline_args)

        await app_module._attach_connection(
            host,
            object(),
            app_module.SnapshotHandshake(
                session_id=host.state.session_id,
                resume_token=host.state.resume_token,
                proposed_epoch=1,
                snapshot_sequence=0,
            ),
        )
        first_worker = host.connection.worker
        first_tracker = first_worker.turn_tracking_observer
        stale_turn_ended = first_tracker.handlers["on_turn_ended"]
        worker_ref = weakref.ref(first_worker)
        tracker_ref = weakref.ref(first_tracker)

        await app_module._attach_connection(
            host,
            object(),
            app_module.SnapshotHandshake(
                session_id=host.state.session_id,
                resume_token=host.state.resume_token,
                proposed_epoch=2,
                snapshot_sequence=0,
            ),
        )

        # Connection replacement fences the old connection and schedules its
        # shutdown as a background task; await it so the coroutine frame
        # (which references the old worker) is released before collecting.
        pending_shutdowns = set(host._background_shutdowns)
        if pending_shutdowns:
            await asyncio.gather(*pending_shutdowns)

        del first_worker, first_tracker
        for _ in range(3):
            gc.collect()

        assert worker_ref() is None
        assert tracker_ref() is None

        # The stale closure itself captured no worker/host/runtime reference,
        # so it is still callable and still reaches only the console sink.
        sequence_before = host.state.sequence
        await stale_turn_ended(object(), 7, 1.5, False)

        assert host.state.sequence == sequence_before
        matching = [r for r in sink.records if r.event == "pipecat_turn_end"]
        assert len(matching) == 1
        assert matching[0].fields["origin_epoch"] == 1  # the stale, replaced epoch

        await host.shutdown()

    asyncio.run(run())


def test_enable_metrics_toggle_does_not_change_real_pipeline_worker_frame_flow() -> None:
    """Paired synthetic traces through the real locked PipelineWorker: enabling
    processor metrics and attaching the Phase 1 framework observers must not
    change processor order, frame flow, or downstream frame types.
    """
    from pipecat.frames.frames import (
        BotStartedSpeakingFrame,
        BotStoppedSpeakingFrame,
        Frame,
        MetricsFrame,
    )
    from pipecat.observers.startup_timing_observer import StartupTimingObserver
    from pipecat.observers.user_bot_latency_observer import UserBotLatencyObserver
    from pipecat.pipeline.worker import PipelineParams
    from pipecat.processors.frame_processor import FrameProcessor
    from pipecat.tests.utils import run_test

    class _PassthroughProcessor(FrameProcessor):
        async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
            await super().process_frame(frame, direction)
            await self.push_frame(frame, direction)

    async def trace(*, enable_metrics: bool) -> list[type]:
        observers = [StartupTimingObserver(), UserBotLatencyObserver()]
        down, _up = await run_test(
            _PassthroughProcessor(),
            frames_to_send=[
                UserStartedSpeakingFrame(),
                VADUserStoppedSpeakingFrame(stop_secs=0.2),
                UserStoppedSpeakingFrame(),
                BotStartedSpeakingFrame(),
                BotStoppedSpeakingFrame(),
            ],
            observers=observers,
            pipeline_params=PipelineParams(enable_metrics=enable_metrics),
        )
        # Excluding expected metrics-observation frames per the plan's Testing
        # Notes: enable_metrics=True legitimately adds an initial empty
        # MetricsFrame (PipelineParams.send_initial_empty_metrics), which is
        # not a change to transcript/TTS/media frame flow.
        return [type(frame) for frame in down if not isinstance(frame, MetricsFrame)]

    async def run() -> None:
        disabled_trace = await trace(enable_metrics=False)
        enabled_trace = await trace(enable_metrics=True)

        assert disabled_trace == enabled_trace
        assert disabled_trace == [
            UserStartedSpeakingFrame,
            VADUserStoppedSpeakingFrame,
            UserStoppedSpeakingFrame,
            BotStartedSpeakingFrame,
            BotStoppedSpeakingFrame,
        ]

    asyncio.run(run())


def test_rtvi_observer_params_suppress_metrics_messages_to_the_client() -> None:
    """The console-only release must not leak MetricsFrames over the RTVI wire."""
    from pipecat.frames.frames import MetricsFrame
    from pipecat.metrics.metrics import TTFBMetricsData
    from pipecat.observers.base_observer import FramePushed
    from pipecat.processors.frame_processor import FrameProcessor
    from pipecat.processors.frameworks.rtvi import (
        RTVIObserver,
        RTVIObserverParams,
        RTVIProcessor,
    )

    async def sent_messages(params: RTVIObserverParams) -> list[object]:
        rtvi = RTVIProcessor()
        sent: list[object] = []

        async def record(model: object, exclude_none: bool = True) -> None:
            sent.append(model)

        rtvi.push_transport_message = record  # type: ignore[method-assign]
        observer = RTVIObserver(rtvi, params=params)
        source = FrameProcessor(name="metrics-source")
        await observer.on_push_frame(
            FramePushed(
                source=source,
                destination=source,
                frame=MetricsFrame(
                    data=[TTFBMetricsData(processor="tts", value=0.25)],
                ),
                direction=FrameDirection.DOWNSTREAM,
                timestamp=0,
            )
        )
        return sent

    async def run() -> None:
        suppressed = await sent_messages(RTVIObserverParams(metrics_enabled=False))
        assert suppressed == []

        # Positive control: the same push does produce a wire message by default,
        # so the assertion above is about the flag and not a frame that never
        # reached the dispatcher.
        default = await sent_messages(RTVIObserverParams())
        assert len(default) == 1
        assert default[0].data["ttfb"][0]["value"] == 0.25

    asyncio.run(run())


# --------------------------------------------------------------------------
# Phase 2: application-turn foreground and retained-work background timing.
#
# These tests are black-box: they only construct SessionHost with a
# CollectingMeasurementSink and inspect the emitted PERF_METRIC records, so
# they do not depend on the shape of whatever parent/child/retained recorder
# classes the Phase 2 implementation adds. They cover representative rows of
# the plan's Foreground Branch and Fault Matrix and Retained Branch and Race
# Matrix, not literally every row (see the test-writer's final report for the
# rows intentionally left uncovered).
# --------------------------------------------------------------------------


def _events(sink: CollectingMeasurementSink, name: str) -> tuple[object, ...]:
    return tuple(record for record in sink.records if record.event == name)


def test_direct_response_emits_zero_child_app_turn_foreground() -> None:
    async def run() -> None:
        class DirectCoordinator:
            def arbitrate(self, _session_id: str, transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "routed",
                        "decision": type("Decision", (), {"action": "direct"})(),
                        "prose": "Here is a direct answer.",
                        "transcript": transcript,
                    },
                )()

        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=DirectCoordinator(),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("what is 2+2")

        assert result.text == "Here is a direct answer."
        parents = _events(sink, "app_turn_foreground")
        assert len(parents) == 1
        fields = parents[0].fields
        assert fields["outcome"] == "direct"
        assert fields["child_count"] == 0
        assert all(
            fields[name] == 0
            for name in (
                "direct_count",
                "unsupported_count",
                "completed_count",
                "retained_count",
                "clarification_count",
                "declined_count",
                "failed_count",
                "cancelled_count",
            )
        )
        assert _events(sink, "work_item_foreground") == ()
        await host.shutdown()

    asyncio.run(run())


def test_unsupported_capability_emits_zero_child_app_turn_foreground() -> None:
    async def run() -> None:
        class UnsupportedActionCoordinator:
            def arbitrate(self, _session_id: str, transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "routed",
                        "decision": type("Decision", (), {"action": "unsupported"})(),
                        "prose": None,
                        "transcript": transcript,
                    },
                )()

        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=UnsupportedActionCoordinator(),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("open my calendar")

        assert result.text == "I cannot access that capability here."
        fields = _events(sink, "app_turn_foreground")[0].fields
        assert fields["outcome"] == "unsupported"
        assert fields["child_count"] == 0
        assert fields["unsupported_count"] == 0
        assert _events(sink, "work_item_foreground") == ()
        await host.shutdown()

    asyncio.run(run())


def test_router_clarification_emits_clarify_outcome_with_zero_child() -> None:
    async def run() -> None:
        class ClarifyActionCoordinator:
            def arbitrate(self, _session_id: str, transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "routed",
                        "decision": type("Decision", (), {"action": "clarify"})(),
                        "prose": None,
                        "transcript": transcript,
                    },
                )()

        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=ClarifyActionCoordinator(),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        await host._handle_transcript("search for something vague")

        fields = _events(sink, "app_turn_foreground")[0].fields
        assert fields["outcome"] == "clarify"
        assert fields["child_count"] == 0
        assert fields["clarification_count"] == 0
        assert _events(sink, "work_item_foreground") == ()
        await host.shutdown()

    asyncio.run(run())


def test_cancel_control_emits_control_action_and_applied_outcome() -> None:
    async def run() -> None:
        class ControlCoordinator:
            def arbitrate(self, _session_id: str, _transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "control",
                        "decision": None,
                        "work_items": (),
                        "control_action": "cancel",
                    },
                )()

        sink = CollectingMeasurementSink()
        tts = FakeTTS()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=tts,
            coordinator=ControlCoordinator(),
            measurement_sink=sink,
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        connection.scheduler.enqueue(
            result_id="result-active",
            work_item_id="work-active",
            run_id="run-active",
            text="Active answer",
            origin_epoch=1,
        )
        await connection.scheduler.start_next()

        await host._handle_transcript("cancel")

        fields = _events(sink, "app_turn_foreground")[0].fields
        assert fields["outcome"] == "control"
        assert fields["control_action"] == "cancel"
        assert fields["control_outcome"] == "applied"
        assert fields["child_count"] == 0
        assert _events(sink, "work_item_foreground") == ()
        await host.shutdown()

    asyncio.run(run())


def test_unknown_cancel_target_emits_control_with_unknown_target_outcome() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=WorkItemCoordinator(),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        await host._handle_transcript("cancel work-item-does-not-exist")

        fields = _events(sink, "app_turn_foreground")[0].fields
        assert fields["outcome"] == "control"
        assert fields["control_action"] == "cancel"
        assert fields["control_outcome"] == "unknown_target"
        assert fields["child_count"] == 0
        await host.shutdown()

    asyncio.run(run())


def test_unknown_cancel_target_speaks_the_miss_acknowledgement_text() -> None:
    """A cancel naming a target that matches neither work nor speech must
    speak the dedicated miss acknowledgement from ``_CONTROL_MISS_ACK_TEXT``
    (keyed by ``ControlOutcome``, looked up via ``ack_override``), not the
    generic ``_CONTROL_ACK_TEXT`` table keyed by ``action`` — which would
    otherwise fall through to "Control request noted.".
    """

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=WorkItemCoordinator(),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("cancel work-item-does-not-exist")

        assert result.text == "I could not find that active work item."
        fields = _events(sink, "app_turn_foreground")[0].fields
        assert fields["control_action"] == "cancel"
        assert fields["control_outcome"] == "unknown_target"
        await host.shutdown()

    asyncio.run(run())


def test_consent_without_pending_dialogue_emits_control_no_pending_outcome() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=WorkItemCoordinator(),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        await host._handle_transcript("consent")

        fields = _events(sink, "app_turn_foreground")[0].fields
        assert fields["outcome"] == "control"
        assert fields["control_action"] == "consent"
        assert fields["control_outcome"] == "no_pending"
        assert fields["child_count"] == 0
        await host.shutdown()

    asyncio.run(run())


def test_worker_clarification_emits_clarify_parent_and_one_child() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=RoutedCoordinator(ClarifyingResultWorker()),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("weather")

        assert result.text == "Which city's weather do you mean?"
        parent = _events(sink, "app_turn_foreground")[0].fields
        assert parent["outcome"] == "clarify"
        assert parent["child_count"] == 1
        assert parent["clarification_count"] == 1
        children = _events(sink, "work_item_foreground")
        assert len(children) == 1
        assert children[0].fields["outcome"] == "clarify"
        assert children[0].fields["turn_id"] == parent["turn_id"]
        await host.shutdown()

    asyncio.run(run())


def test_worker_decline_emits_declined_parent_and_one_child() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=RoutedCoordinator(DecliningResultWorker()),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("private calendar")

        assert result.text == "I could not find a reliable result for that request."
        parent = _events(sink, "app_turn_foreground")[0].fields
        assert parent["outcome"] == "declined"
        assert parent["child_count"] == 1
        assert parent["declined_count"] == 1
        children = _events(sink, "work_item_foreground")
        assert len(children) == 1
        assert children[0].fields["outcome"] == "declined"
        await host.shutdown()

    asyncio.run(run())


def test_missing_worker_emits_failed_parent_and_missing_worker_child() -> None:
    async def run() -> None:
        class NoWorkerCoordinator(RoutedCoordinator):
            def dispatch(self, _decision: object) -> object:
                return None

        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=NoWorkerCoordinator(None),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("dispatch to missing worker")

        # Existing behavior (unchanged): the raw routed outcome is returned
        # when dispatch cannot resolve a worker.
        assert getattr(result, "kind", None) == "routed"
        parent = _events(sink, "app_turn_foreground")[0].fields
        assert parent["outcome"] == "failed"
        assert parent["child_count"] == 1
        assert parent["failed_count"] == 1
        children = _events(sink, "work_item_foreground")
        assert len(children) == 1
        assert children[0].fields["outcome"] == "missing_worker"
        await host.shutdown()

    asyncio.run(run())


def test_missing_search_capability_emits_failed_parent_and_missing_search_child() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=RoutedCoordinator(object()),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("no search capability")

        assert getattr(result, "kind", None) == "routed"
        parent = _events(sink, "app_turn_foreground")[0].fields
        assert parent["outcome"] == "failed"
        assert parent["child_count"] == 1
        assert parent["failed_count"] == 1
        children = _events(sink, "work_item_foreground")
        assert len(children) == 1
        assert children[0].fields["outcome"] == "missing_search"
        await host.shutdown()

    asyncio.run(run())


def test_delegated_foreground_success_emits_completed_parent_and_one_child() -> None:
    async def run() -> None:
        search = ProjectedResultWorker()
        coordinator = ProjectedCoordinator(search)
        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner, coordinator=coordinator, measurement_sink=sink
        )
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("historical capitals")

        assert result.text == "Answer for historical capitals"
        parent = _events(sink, "app_turn_foreground")[0].fields
        assert parent["outcome"] == "completed"
        assert parent["child_count"] == 1
        assert parent["completed_count"] == 1
        assert all(
            parent[name] == 0
            for name in (
                "direct_count",
                "unsupported_count",
                "retained_count",
                "clarification_count",
                "declined_count",
                "failed_count",
                "cancelled_count",
            )
        )
        children = _events(sink, "work_item_foreground")
        assert len(children) == 1
        child = children[0].fields
        assert child["outcome"] == "completed"
        assert child["turn_id"] == parent["turn_id"]
        assert "search_ms" in child
        await host.shutdown()

    asyncio.run(run())


def test_delegated_foreground_timeout_emits_retained_parent_and_child_then_background_completion() -> (
    None
):
    async def run() -> None:
        worker = BlockingResultWorker()
        worker.metadata = type(
            "Metadata",
            (),
            {"worker_id": "worker-search", "topic": "slow search", "model_policy": "deep"},
        )()

        class RetainingRoutedCoordinator(RoutedCoordinator):
            def __init__(self) -> None:
                super().__init__(worker)
                self.owner = WorkItemCoordinator(
                    config=Config(foreground_search_timeout_seconds=0.001)
                )
                self.config = self.owner.config

            def start_task(self, operation: object) -> asyncio.Task[object] | None:
                return self.owner.start_task(operation)

            def retain_late_task(self, task: asyncio.Task[object], **kwargs: object) -> bool:
                return self.owner.retain_late_task(task, **kwargs)

            async def shutdown(self) -> None:
                await self.owner.shutdown()

        sink = CollectingMeasurementSink()
        coordinator = RetainingRoutedCoordinator()
        host = SessionHost(
            registry=WorkerRegistry(config=coordinator.config),
            config=coordinator.config,
            runner_factory=LifecycleRunner,
            coordinator=coordinator,
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        foreground = await host._handle_transcript("slow query")
        assert "continue in the background" in foreground.text

        parent = _events(sink, "app_turn_foreground")[0].fields
        assert parent["outcome"] == "retained"
        assert parent["retained_count"] == 1
        children = _events(sink, "work_item_foreground")
        assert len(children) == 1
        assert children[0].fields["outcome"] == "retained"
        assert _events(sink, "work_item_background") == ()

        worker.release.set()
        for _ in range(20):
            await asyncio.sleep(0)
            if len(host.state.result_history("worker-search")) == 2:
                break

        background = _events(sink, "work_item_background")
        assert len(background) == 1
        assert background[0].fields["work_outcome"] == "completed"
        assert background[0].fields["turn_id"] == parent["turn_id"]
        await host.shutdown()

    asyncio.run(run())


def test_router_exception_emits_failed_parent_and_no_child() -> None:
    async def run() -> None:
        class FailingCoordinator:
            def arbitrate(self, _session_id: str, _transcript: str) -> object:
                raise RuntimeError("provider detail must stay in server logs")

        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner, coordinator=FailingCoordinator(), measurement_sink=sink
        )
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("search for India's historical capitals")

        assert result.text == "Routing is temporarily unavailable. Please try that request again."
        parent = _events(sink, "app_turn_foreground")[0].fields
        assert parent["outcome"] == "failed"
        assert parent["child_count"] == 0
        assert _events(sink, "work_item_foreground") == ()
        await host.shutdown()

    asyncio.run(run())


def test_dispatch_exception_emits_failed_parent_with_no_completed_child() -> None:
    """Dispatch raising UnsupportedWorkerType/RoutingValidationError falls back
    to a safe result (existing behavior) but must be labelled ``failed``: the
    matrix's "Router, dispatch, ... raises Exception" row, distinct from the
    router's own ``unsupported`` routing action.
    """

    async def run() -> None:
        class UnsupportedCoordinator:
            def arbitrate(self, _session_id: str, transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {"kind": "routed", "decision": object(), "transcript": transcript},
                )()

            def dispatch(self, _decision: object) -> object:
                raise UnsupportedWorkerType("unsupported worker type: calendar")

        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=UnsupportedCoordinator(),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("private calendar")

        assert result.text == "I cannot access that capability here."
        parent = _events(sink, "app_turn_foreground")[0].fields
        assert parent["outcome"] == "failed"
        await host.shutdown()

    asyncio.run(run())


def test_router_cancellation_emits_cancelled_parent_and_no_child() -> None:
    async def run() -> None:
        class BlockingRouterCoordinator(RoutedCoordinator):
            def __init__(self) -> None:
                super().__init__(ResultWorker())
                self.routing_started = threading.Event()
                self.release_routing = threading.Event()

            def arbitrate(self, _session_id: str, transcript: str) -> object:
                if transcript == "cancel":
                    return type(
                        "Outcome",
                        (),
                        {
                            "kind": "control",
                            "decision": None,
                            "work_items": (),
                            "control_action": "cancel",
                        },
                    )()
                self.routing_started.set()
                self.release_routing.wait(timeout=2)
                return super().arbitrate(_session_id, transcript)

        sink = CollectingMeasurementSink()
        coordinator = BlockingRouterCoordinator()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=FakeTTS(),
            coordinator=coordinator,
            measurement_sink=sink,
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        pending = asyncio.create_task(host._handle_transcript("slow route"))
        await asyncio.to_thread(coordinator.routing_started.wait, 1)

        cancellation = await host._handle_transcript("cancel")
        coordinator.release_routing.set()
        with pytest.raises(asyncio.CancelledError):
            await pending

        assert cancellation.text == "Cancelling the active response."
        cancelled_parents = [
            record
            for record in _events(sink, "app_turn_foreground")
            if record.fields["outcome"] == "cancelled"
        ]
        assert len(cancelled_parents) == 1
        assert cancelled_parents[0].fields["child_count"] == 0
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_mixed_outcomes_emit_one_child_per_item_and_mixed_parent() -> None:
    async def run() -> None:
        worker = ResultWorker()

        class Registry:
            config = Config(max_work_items_per_turn=4)

            @staticmethod
            def catalogue() -> object:
                return object()

        class Router:
            @staticmethod
            def route_envelope(text: str, _catalogue: object) -> object:
                action = {
                    "answer directly": "direct",
                    "ask me": "clarify",
                    "unsupported thing": "unsupported",
                    "search it": "existing_worker",
                }[text]
                decision = type("Decision", (), {"action": action})()
                prose = {
                    "direct": "Direct answer.",
                    "clarify": "Which one?",
                    "unsupported": None,
                    "existing_worker": None,
                }[action]
                return type("Envelope", (), {"decision": decision, "prose": prose})()

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=Registry(), router=Router())

            def dispatch(self, decision: object, **_: object) -> object:
                assert decision.action == "existing_worker"
                return worker

        sink = CollectingMeasurementSink()
        coordinator = Coordinator()
        host = SessionHost(
            runner_factory=LifecycleRunner, coordinator=coordinator, measurement_sink=sink
        )
        origin = await host.connect(connection_handshake(host, 1))
        outcome = type(
            "Outcome",
            (),
            {
                "work_items": (
                    "answer directly",
                    "ask me",
                    "unsupported thing",
                    "search it",
                ),
                "pending_dialogue": None,
            },
        )()

        results = await host._handle_multi_intent(
            outcome,
            "",
            origin,
            "turn-compound",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-compound"),
        )

        assert [result.text for result in results] == [
            "Direct answer.",
            "Which one?",
            "I cannot access that capability here.",
            "Answer for search it",
        ]
        parent = _events(sink, "app_turn_foreground")
        assert len(parent) == 1
        parent_fields = parent[0].fields
        assert parent_fields["outcome"] == "mixed"
        assert parent_fields["turn_id"] == "turn-compound"
        assert parent_fields["child_count"] == 4
        assert parent_fields["direct_count"] == 1
        assert parent_fields["clarification_count"] == 1
        assert parent_fields["unsupported_count"] == 1
        assert parent_fields["completed_count"] == 1
        children = _events(sink, "work_item_foreground")
        assert len(children) == 4
        assert {child.fields["outcome"] for child in children} == {
            "direct",
            "clarify",
            "unsupported",
            "completed",
        }
        assert all(child.fields["turn_id"] == "turn-compound" for child in children)
        assert len({child.fields["work_item_id"] for child in children}) == 4
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_all_completed_emits_completed_parent_and_all_completed_children() -> None:
    async def run() -> None:
        worker = ResultWorker()

        class Registry:
            config = Config(max_work_items_per_turn=2)

            @staticmethod
            def catalogue() -> object:
                return object()

        class Router:
            @staticmethod
            def route_envelope(_text: str, _catalogue: object) -> object:
                decision = type("Decision", (), {"action": "existing_worker"})()
                return type("Envelope", (), {"decision": decision, "prose": None})()

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=Registry(), router=Router())

            def dispatch(self, _decision: object, **_: object) -> object:
                return worker

        sink = CollectingMeasurementSink()
        coordinator = Coordinator()
        host = SessionHost(
            runner_factory=LifecycleRunner, coordinator=coordinator, measurement_sink=sink
        )
        origin = await host.connect(connection_handshake(host, 1))
        outcome = type(
            "Outcome",
            (),
            {"work_items": ("first item", "second item"), "pending_dialogue": None},
        )()

        await host._handle_multi_intent(
            outcome,
            "",
            origin,
            "turn-all-complete",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-all-complete"),
        )

        parent = _events(sink, "app_turn_foreground")[0].fields
        assert parent["outcome"] == "completed"
        assert parent["child_count"] == 2
        assert parent["completed_count"] == 2
        children = _events(sink, "work_item_foreground")
        assert len(children) == 2
        assert all(child.fields["outcome"] == "completed" for child in children)
        await host.shutdown()

    asyncio.run(run())


def test_pending_search_submit_exception_still_emits_failed_parent() -> None:
    """``_handle_pending`` must finalize ``app_turn_foreground`` as ``failed``
    even when ``coordinator.submit`` raises before returning, matching the
    matrix's "Router, dispatch, search, commit, or enqueue helper raises
    Exception" row that ``_handle_transcript_impl`` already honors.
    """

    async def run() -> None:
        worker = ResultWorker()

        class Registry:
            config = Config(max_work_items_per_turn=2)

            @staticmethod
            def get(_worker_id: str) -> object:
                return type("Registered", (), {"worker": worker})()

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=Registry(), router=object())

            async def submit(self, *_args: object, **_kwargs: object) -> object:
                raise RuntimeError("submit exploded")

        sink = CollectingMeasurementSink()
        coordinator = Coordinator()
        host = SessionHost(
            runner_factory=LifecycleRunner, coordinator=coordinator, measurement_sink=sink
        )
        origin = await host.connect(connection_handshake(host, 1))
        pending = type(
            "Pending",
            (),
            {
                "owner_id": "worker-search",
                "original_query": "continue please",
                "question": "Which one?",
            },
        )()
        outcome = type("Outcome", (), {"pending_dialogue": pending, "work_items": ()})()

        with pytest.raises(RuntimeError, match="submit exploded"):
            await host._handle_pending(
                outcome,
                "continue please",
                origin,
                "turn-pending-fail",
                host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-pending-fail"),
            )

        parent = _events(sink, "app_turn_foreground")[0].fields
        assert parent["outcome"] == "failed"
        # The child recorder is created before ``submit`` is called, so the
        # work item is already dispatched when submit raises. It was previously
        # asserted as child_count == 0 with no child record at all, which
        # encoded the orphaned-child defect: the parent now sweeps it to a
        # terminal ``failed`` record instead of dropping it.
        assert parent["child_count"] == 1
        assert parent["failed_count"] == 1
        children = _events(sink, "work_item_foreground")
        assert len(children) == 1
        assert children[0].fields["outcome"] == "failed"
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_commit_failure_does_not_drop_sibling_results() -> None:
    """A commit/speak failure on one item must not abort processing of the
    remaining fanned-out items in the same multi-intent turn: each item's
    state is already durably committed by ``_commit_and_speak`` before it can
    raise, so aborting the loop early would silently discard an
    already-completed sibling result.
    """

    async def run() -> None:
        worker = ResultWorker()

        class Registry:
            config = Config(max_work_items_per_turn=2)

            @staticmethod
            def catalogue() -> object:
                return object()

        class Router:
            @staticmethod
            def route_envelope(_text: str, _catalogue: object) -> object:
                decision = type("Decision", (), {"action": "existing_worker"})()
                return type("Envelope", (), {"decision": decision, "prose": None})()

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=Registry(), router=Router())

            def dispatch(self, _decision: object, **_: object) -> object:
                return worker

        sink = CollectingMeasurementSink()
        coordinator = Coordinator()
        host = SessionHost(
            runner_factory=LifecycleRunner, coordinator=coordinator, measurement_sink=sink
        )
        origin = await host.connect(connection_handshake(host, 1))
        outcome = type(
            "Outcome", (), {"work_items": ("first item", "second item"), "pending_dialogue": None}
        )()

        attempted: list[object] = []

        async def flaky_commit_and_speak(
            result: object, *_args: object, **_kwargs: object
        ) -> object:
            attempted.append(result)
            if len(attempted) == 1:
                raise RuntimeError("commit exploded")
            return result

        host._commit_and_speak = flaky_commit_and_speak  # type: ignore[method-assign]

        with pytest.raises(RuntimeError, match="commit exploded"):
            await host._handle_multi_intent(
                outcome,
                "",
                origin,
                "turn-multi-commit-partial-fail",
                host._new_app_turn_recorder(
                    origin_epoch=origin.epoch, turn_id="turn-multi-commit-partial-fail"
                ),
            )

        assert len(attempted) == 2
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_commit_exception_still_emits_failed_parent() -> None:
    """``_handle_multi_intent`` must finalize ``app_turn_foreground`` as
    ``failed`` even when ``_commit_and_speak`` raises after work items have
    already completed, matching the same matrix row as the sibling test
    above and ``_handle_transcript_impl``'s existing exception safety.
    """

    async def run() -> None:
        worker = ResultWorker()

        class Registry:
            config = Config(max_work_items_per_turn=2)

            @staticmethod
            def catalogue() -> object:
                return object()

        class Router:
            @staticmethod
            def route_envelope(_text: str, _catalogue: object) -> object:
                decision = type("Decision", (), {"action": "existing_worker"})()
                return type("Envelope", (), {"decision": decision, "prose": None})()

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=Registry(), router=Router())

            def dispatch(self, _decision: object, **_: object) -> object:
                return worker

        sink = CollectingMeasurementSink()
        coordinator = Coordinator()
        host = SessionHost(
            runner_factory=LifecycleRunner, coordinator=coordinator, measurement_sink=sink
        )
        origin = await host.connect(connection_handshake(host, 1))
        outcome = type(
            "Outcome", (), {"work_items": ("first item", "second item"), "pending_dialogue": None}
        )()

        async def raising_commit_and_speak(*_args: object, **_kwargs: object) -> object:
            raise RuntimeError("commit exploded")

        host._commit_and_speak = raising_commit_and_speak  # type: ignore[method-assign]

        with pytest.raises(RuntimeError, match="commit exploded"):
            await host._handle_multi_intent(
                outcome,
                "",
                origin,
                "turn-multi-commit-fail",
                host._new_app_turn_recorder(
                    origin_epoch=origin.epoch, turn_id="turn-multi-commit-fail"
                ),
            )

        parent = _events(sink, "app_turn_foreground")[0].fields
        assert parent["outcome"] == "failed"
        await host.shutdown()

    asyncio.run(run())


def test_foreground_timing_fields_respect_start_stop_boundary_ordering() -> None:
    """Deterministic-clock stand-in: exact per-field durations against the
    Timing Boundaries table require knowing the Phase 2 recorder's internal
    ``time.perf_counter()`` call sites, which are implementation-internal and
    not yet built (see ``needs_impl_clarification`` in the final report).
    This asserts the observable relationships the boundaries imply instead:
    ``routing_ms``/``search_ms`` are each individually bounded below by a
    real, injected delay, and ``total_ms`` is at least their sum.
    """

    async def run() -> None:
        class SlowRoutingCoordinator(RoutedCoordinator):
            def arbitrate(self, session_id: str, transcript: str) -> object:
                # `coordinator.arbitrate` runs inside `asyncio.to_thread`, so a
                # thread-blocking sleep produces a real, measurable delay.
                time.sleep(0.05)
                return super().arbitrate(session_id, transcript)

        class SlowSearchWorker(ResultWorker):
            async def search(
                self, query: str, *, turn_id: str, origin_epoch: int | None
            ) -> GroundedResult:
                await asyncio.sleep(0.05)
                return await super().search(query, turn_id=turn_id, origin_epoch=origin_epoch)

        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=SlowRoutingCoordinator(SlowSearchWorker()),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        await host._handle_transcript("Riga weather")

        parent = _events(sink, "app_turn_foreground")[0].fields
        child = _events(sink, "work_item_foreground")[0].fields

        assert parent.get("routing_ms", 0) >= 30
        assert child.get("search_ms", 0) >= 30
        assert parent["total_ms"] >= parent.get("routing_ms", 0) + child.get("search_ms", 0) - 5
        await host.shutdown()

    asyncio.run(run())


def _background_records(sink: CollectingMeasurementSink) -> tuple[object, ...]:
    return _events(sink, "work_item_background")


def _register_dispatch_recorder(
    host: SessionHost,
    work_item_id: str,
    *,
    origin_epoch: int = 1,
    turn_id: str = "turn-retained",
    worker_id: str = "worker-search",
) -> object:
    """Stand in for the dispatch-time provisional recorder registration that
    every retained work item goes through before its late callback can run."""
    recorder = host._new_retained_recorder(
        origin_epoch=origin_epoch,
        turn_id=turn_id,
        work_item_id=work_item_id,
        app_worker_id=worker_id,
    )
    host._retained_recorders[work_item_id] = recorder
    return recorder


def _late_delivery_context(host: SessionHost, **overrides: object) -> LateDeliveryContext:
    fields: dict[str, object] = {
        "turn_id": "turn-retained",
        "work_item_id": "work-retained",
        "origin_epoch": 1,
        "ack_timestamp": None,
        "accepted_turn_sequence": host._turn_sequence,
    }
    fields.update(overrides)
    return LateDeliveryContext(**fields)


def test_retained_completion_with_speech_emits_completed_committed_queued() -> None:
    async def run() -> None:
        tts = FakeTTS()
        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=tts,
            measurement_sink=sink,
            promotion_manifest=PromotionManifest(promotion_eligible=True),
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        result = GroundedResult(
            result_id="result-retained-ok",
            worker_id="worker-search",
            turn_id="turn-retained-ok",
            text="Complete late answer",
            spoken_text="Spoken late answer",
            origin_epoch=1,
        )
        _register_dispatch_recorder(host, "work-retained-ok", turn_id="turn-retained-ok")
        late = LateResult(
            work_item_id="work-retained-ok",
            worker_id="worker-search",
            result=result,
            terminal_kind="completed",
        )

        await host.commit_late_result_once(
            _late_delivery_context(
                host, turn_id="turn-retained-ok", work_item_id="work-retained-ok"
            ),
            late,
        )

        records = _background_records(sink)
        assert len(records) == 1
        fields = records[0].fields
        assert fields["work_outcome"] == "completed"
        assert fields["commit_outcome"] == "committed"
        assert fields["speech_outcome"] == "queued"
        assert fields["work_item_id"] == "work-retained-ok"
        await host.shutdown()

    asyncio.run(run())


def test_retained_late_result_cancelled_during_speech_start_still_emits_background_record() -> None:
    """A CancelledError delivered during the post-pop
    ``await origin.scheduler.start_next()`` window inside ``_commit_late_result``
    must not skip finalization: the recorder was already popped from the
    registry above that await, so a skipped finalize would both drop the
    ``work_item_background`` record and remove the shutdown-sweep backstop."""

    async def run() -> None:
        tts = FakeTTS()
        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=tts,
            measurement_sink=sink,
            promotion_manifest=PromotionManifest(promotion_eligible=True),
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()

        async def raising_start_next(work_item_id: str | None = None) -> object:
            raise asyncio.CancelledError()

        connection.scheduler.start_next = raising_start_next  # type: ignore[method-assign]

        result = GroundedResult(
            result_id="result-cancel-during-start",
            worker_id="worker-search",
            turn_id="turn-cancel-during-start",
            text="Complete late answer",
            spoken_text="Spoken late answer",
            origin_epoch=1,
        )
        _register_dispatch_recorder(
            host, "work-cancel-during-start", turn_id="turn-cancel-during-start"
        )
        late = LateResult(
            work_item_id="work-cancel-during-start",
            worker_id="worker-search",
            result=result,
            terminal_kind="completed",
        )

        with pytest.raises(asyncio.CancelledError):
            await host.commit_late_result_once(
                _late_delivery_context(
                    host,
                    turn_id="turn-cancel-during-start",
                    work_item_id="work-cancel-during-start",
                ),
                late,
            )

        records = _background_records(sink)
        assert len(records) == 1
        fields = records[0].fields
        assert fields["work_item_id"] == "work-cancel-during-start"
        assert fields["work_outcome"] == "completed"
        assert fields["commit_outcome"] == "committed"
        # start_next raised before it could set "queued"; finalize's own
        # terminal default fills the still-unset speech axis.
        assert fields["speech_outcome"] == "cancelled"
        assert "work-cancel-during-start" not in host._retained_recorders
        await host.shutdown()

    asyncio.run(run())


def test_retained_worker_exception_emits_failed_not_applicable_axes() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(measurement_sink=sink)
        _register_dispatch_recorder(host, "work-failed")

        await host.commit_late_result_once(
            _late_delivery_context(host, work_item_id="work-failed"),
            LateResult(
                work_item_id="work-failed",
                worker_id="worker-search",
                error="RuntimeError: provider exploded",
                terminal_kind="failed",
            ),
        )

        fields = _background_records(sink)[0].fields
        assert fields["work_outcome"] == "failed"
        assert fields["commit_outcome"] == "not_applicable"
        assert fields["speech_outcome"] == "not_applicable"
        assert host.state.result_history("worker-search") == ()

    asyncio.run(run())


def test_retained_worker_cancellation_emits_cancelled_axes() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(measurement_sink=sink)
        _register_dispatch_recorder(host, "work-cancelled")

        await host.commit_late_result_once(
            _late_delivery_context(host, work_item_id="work-cancelled"),
            LateResult(
                work_item_id="work-cancelled",
                worker_id="worker-search",
                error="CancelledError: worker task was cancelled",
                terminal_kind="cancelled",
            ),
        )

        fields = _background_records(sink)[0].fields
        assert fields["work_outcome"] == "cancelled"
        assert fields["commit_outcome"] == "suppressed_cancelled"
        assert fields["speech_outcome"] == "cancelled"

    asyncio.run(run())


def test_retained_user_cancelled_work_item_suppresses_commit_and_speech() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(measurement_sink=sink)
        host._cancelled_work_items.add("work-user-cancelled")
        _register_dispatch_recorder(host, "work-user-cancelled", turn_id="turn-user-cancelled")
        result = GroundedResult(
            result_id="result-user-cancelled",
            worker_id="worker-search",
            turn_id="turn-user-cancelled",
            text="Should not commit",
            spoken_text="Should not commit",
            origin_epoch=1,
        )

        await host.commit_late_result_once(
            _late_delivery_context(
                host, turn_id="turn-user-cancelled", work_item_id="work-user-cancelled"
            ),
            LateResult(
                work_item_id="work-user-cancelled", worker_id="worker-search", result=result
            ),
        )

        # Dev plan Phase 2 cancellation matrix (dev plan lines 200/205/353):
        # cancellation before/while-queued/while-admitted suppresses or
        # reclassifies *speech delivery* only -- it no longer suppresses a
        # valid canonical commit. A cancelled work item's otherwise-valid
        # result is still committed exactly once, display-only, speech
        # suppressed.
        assert host.state.result_history("worker-search") == (result,)
        fields = _background_records(sink)[0].fields
        assert fields["work_outcome"] == "cancelled"
        assert fields["commit_outcome"] == "committed"
        assert fields["speech_outcome"] == "cancelled"
        assert fields["delivery_disposition"] == "display_only"

    asyncio.run(run())


def test_retained_invalid_result_type_emits_invalid_result_axes() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(measurement_sink=sink)
        _register_dispatch_recorder(host, "work-invalid")

        await host.commit_late_result_once(
            _late_delivery_context(host, work_item_id="work-invalid"),
            LateResult(
                work_item_id="work-invalid",
                worker_id="worker-search",
                result="not a GroundedResult",
            ),
        )

        fields = _background_records(sink)[0].fields
        assert fields["work_outcome"] == "invalid_result"
        assert fields["commit_outcome"] == "not_applicable"
        assert fields["speech_outcome"] == "not_applicable"

    asyncio.run(run())


def test_retained_duplicate_result_id_suppresses_commit() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(measurement_sink=sink)
        existing = GroundedResult(
            result_id="result-dup",
            worker_id="worker-search",
            turn_id="turn-dup",
            text="First",
            spoken_text="First",
            origin_epoch=1,
        )
        host.state.append_result(existing, origin_epoch=1)
        duplicate = GroundedResult(
            result_id="result-dup",
            worker_id="worker-search",
            turn_id="turn-dup",
            text="First",
            spoken_text="First",
            origin_epoch=1,
        )

        _register_dispatch_recorder(host, "work-dup", turn_id="turn-dup")

        await host.commit_late_result_once(
            _late_delivery_context(host, turn_id="turn-dup", work_item_id="work-dup"),
            LateResult(work_item_id="work-dup", worker_id="worker-search", result=duplicate),
        )

        fields = _background_records(sink)[0].fields
        assert fields["work_outcome"] == "completed"
        assert fields["commit_outcome"] == "suppressed_duplicate"
        assert fields["speech_outcome"] == "not_applicable"

    asyncio.run(run())


def test_retained_result_after_connection_replaced_commits_but_marks_speech_stale() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        tts = FakeTTS()
        host = SessionHost(runner_factory=LifecycleRunner, tts=tts, measurement_sink=sink)
        first = await host.connect(connection_handshake(host, 1))
        first.worker = QueueingPipelineWorker()
        second = await host.connect(connection_handshake(host, 2))
        second.worker = QueueingPipelineWorker()

        result = GroundedResult(
            result_id="result-replaced-epoch",
            worker_id="worker-search",
            turn_id="turn-replaced-epoch",
            text="Complete",
            spoken_text="Complete",
            origin_epoch=1,
        )

        _register_dispatch_recorder(host, "work-replaced-epoch", turn_id="turn-replaced-epoch")

        await host.commit_late_result_once(
            _late_delivery_context(
                host, turn_id="turn-replaced-epoch", work_item_id="work-replaced-epoch"
            ),
            LateResult(
                work_item_id="work-replaced-epoch", worker_id="worker-search", result=result
            ),
        )

        assert host.state.result_history("worker-search")[-1] == result
        fields = _background_records(sink)[0].fields
        assert fields["work_outcome"] == "completed"
        assert fields["commit_outcome"] == "committed"
        assert fields["speech_outcome"] == "stale_connection"
        assert not any(isinstance(frame, TTSSpeakFrame) for frame in second.worker.frames)
        await host.shutdown()

    asyncio.run(run())


def test_retained_result_from_replaced_origin_epoch_suppresses_commit_as_stale() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(measurement_sink=sink)
        result = GroundedResult(
            result_id="result-stale-origin",
            worker_id="worker-search",
            turn_id="turn-stale-origin",
            text="Old",
            spoken_text="Old",
            origin_epoch=1,
        )

        _register_dispatch_recorder(
            host, "work-stale-origin", origin_epoch=2, turn_id="turn-stale-origin"
        )

        await host.commit_late_result_once(
            _late_delivery_context(
                host,
                turn_id="turn-stale-origin",
                work_item_id="work-stale-origin",
                origin_epoch=2,
            ),
            LateResult(work_item_id="work-stale-origin", worker_id="worker-search", result=result),
        )

        assert host.state.result_history("worker-search") == ()
        fields = _background_records(sink)[0].fields
        assert fields["work_outcome"] == "completed"
        assert fields["commit_outcome"] == "suppressed_stale"
        assert fields["speech_outcome"] == "not_applicable"

    asyncio.run(run())


def test_suppressed_stale_late_commit_terminalizes_the_work_status() -> None:
    """``suppressed_stale`` fires when the late result's own ``origin_epoch``
    differs from the one its ledger key was dispatched under, so *no* copy
    committed under this key. Emitting no status (the ``suppressed_duplicate``
    rule) stranded the child at its dispatch-time ``searching`` and shipped the
    non-terminal parent in every subsequent snapshot."""

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(measurement_sink=sink)
        result = GroundedResult(
            result_id="result-stale-status",
            worker_id="worker-search",
            turn_id="turn-stale-status",
            text="Old",
            spoken_text="Old",
            origin_epoch=1,
        )
        _register_dispatch_recorder(
            host, "work-stale-status", origin_epoch=2, turn_id="turn-stale-status"
        )
        host._emit_work_status(
            turn_id="turn-stale-status",
            work_item_id="work-stale-status",
            worker_id="worker-search",
            state="searching",
            origin_epoch=2,
        )
        assert host.state.work_status_snapshot()[0].state == "searching"

        await host.commit_late_result_once(
            _late_delivery_context(
                host,
                turn_id="turn-stale-status",
                work_item_id="work-stale-status",
                origin_epoch=2,
            ),
            LateResult(work_item_id="work-stale-status", worker_id="worker-search", result=result),
        )

        parent = host.state.work_status_snapshot()[0]
        assert parent.work_item_id == "work-stale-status"
        assert parent.state == "failed"

    asyncio.run(run())


def test_retained_result_without_active_connection_marks_speech_disconnected() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(measurement_sink=sink)
        result = GroundedResult(
            result_id="result-disconnected",
            worker_id="worker-search",
            turn_id="turn-disconnected",
            text="Complete",
            spoken_text="Complete",
            origin_epoch=1,
        )

        _register_dispatch_recorder(host, "work-disconnected", turn_id="turn-disconnected")

        await host.commit_late_result_once(
            _late_delivery_context(
                host, turn_id="turn-disconnected", work_item_id="work-disconnected"
            ),
            LateResult(work_item_id="work-disconnected", worker_id="worker-search", result=result),
        )

        assert host.state.result_history("worker-search")[-1] == result
        fields = _background_records(sink)[0].fields
        assert fields["commit_outcome"] == "committed"
        assert fields["speech_outcome"] == "disconnected"

    asyncio.run(run())


def test_retained_result_without_tts_service_marks_speech_no_tts() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(runner_factory=LifecycleRunner, measurement_sink=sink)
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        result = GroundedResult(
            result_id="result-no-tts",
            worker_id="worker-search",
            turn_id="turn-no-tts",
            text="Complete",
            spoken_text="Complete",
            origin_epoch=1,
        )

        _register_dispatch_recorder(host, "work-no-tts", turn_id="turn-no-tts")

        await host.commit_late_result_once(
            _late_delivery_context(host, turn_id="turn-no-tts", work_item_id="work-no-tts"),
            LateResult(work_item_id="work-no-tts", worker_id="worker-search", result=result),
        )

        fields = _background_records(sink)[0].fields
        assert fields["commit_outcome"] == "committed"
        assert fields["speech_outcome"] == "no_tts"
        await host.shutdown()

    asyncio.run(run())


def test_shutdown_finalizes_open_retained_recorder_after_coordinator_settles() -> None:
    async def run() -> None:
        class BlockingForeverWorker(ResultWorker):
            async def search(
                self, query: str, *, turn_id: str, origin_epoch: int | None
            ) -> GroundedResult:
                await asyncio.Future()
                raise AssertionError("unreachable")  # pragma: no cover

        worker = BlockingForeverWorker()
        worker.metadata = type(
            "Metadata",
            (),
            {"worker_id": "worker-search", "topic": "slow", "model_policy": "deep"},
        )()
        sink = CollectingMeasurementSink()

        class RetainingRoutedCoordinator(RoutedCoordinator):
            def __init__(self) -> None:
                super().__init__(worker)
                self.owner = WorkItemCoordinator(
                    config=Config(foreground_search_timeout_seconds=0.001)
                )
                self.config = self.owner.config

            def start_task(self, operation: object) -> asyncio.Task[object] | None:
                return self.owner.start_task(operation)

            def retain_late_task(self, task: asyncio.Task[object], **kwargs: object) -> bool:
                return self.owner.retain_late_task(task, **kwargs)

            async def shutdown(self) -> None:
                await self.owner.shutdown()

        coordinator = RetainingRoutedCoordinator()
        host = SessionHost(
            registry=WorkerRegistry(config=coordinator.config),
            config=coordinator.config,
            runner_factory=LifecycleRunner,
            coordinator=coordinator,
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        foreground = await host._handle_transcript("slow forever")
        assert "continue in the background" in foreground.text
        assert _background_records(sink) == ()

        await host.shutdown()

        background = _background_records(sink)
        assert len(background) == 1
        assert background[0].fields["work_outcome"] == "cancelled"

    asyncio.run(run())


def test_retained_background_ms_spans_dispatch_to_completion_for_late_callbacks() -> None:
    """``background_ms`` must measure dispatch-to-completion, and a duplicate
    late callback must not open a replacement recorder whose start instant is
    completion time (which would emit a second, near-zero record)."""

    async def run() -> None:
        class SlowWorker(ResultWorker):
            async def search(
                self, query: str, *, turn_id: str, origin_epoch: int | None
            ) -> GroundedResult:
                await asyncio.sleep(0.08)
                return await super().search(query, turn_id=turn_id, origin_epoch=origin_epoch)

        worker = SlowWorker()
        worker.metadata = type(
            "Metadata",
            (),
            {"worker_id": "worker-search", "topic": "slow", "model_policy": "deep"},
        )()
        sink = CollectingMeasurementSink()

        class RetainingRoutedCoordinator(RoutedCoordinator):
            def __init__(self) -> None:
                super().__init__(worker)
                self.owner = WorkItemCoordinator(
                    config=Config(foreground_search_timeout_seconds=0.001)
                )
                self.config = self.owner.config

            def start_task(self, operation: object) -> asyncio.Task[object] | None:
                return self.owner.start_task(operation)

            def retain_late_task(self, task: asyncio.Task[object], **kwargs: object) -> bool:
                return self.owner.retain_late_task(task, **kwargs)

            async def shutdown(self) -> None:
                await self.owner.shutdown()

        coordinator = RetainingRoutedCoordinator()
        host = SessionHost(
            registry=WorkerRegistry(config=coordinator.config),
            config=coordinator.config,
            runner_factory=LifecycleRunner,
            coordinator=coordinator,
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        foreground = await host._handle_transcript("slow but finishing")
        assert "continue in the background" in foreground.text

        deadline = time.perf_counter() + 2.0
        while not _background_records(sink) and time.perf_counter() < deadline:
            await asyncio.sleep(0.01)

        records = _background_records(sink)
        assert len(records) == 1
        fields = records[0].fields
        assert fields["work_outcome"] == "completed"
        # The provisional recorder starts at dispatch, so the 80 ms worker delay
        # is inside the span; a recorder built when the late result arrived
        # would report near zero instead.
        assert fields["background_ms"] >= 70

        committed = host.state.result_history("worker-search")[-1]
        await host.commit_late_result_once(
            _late_delivery_context(host, work_item_id=fields["work_item_id"]),
            LateResult(
                work_item_id=fields["work_item_id"],
                worker_id="worker-search",
                result=committed,
                terminal_kind="completed",
            ),
        )
        after_duplicate = _background_records(sink)
        assert len(after_duplicate) == 1
        assert all(record.fields["background_ms"] >= 70 for record in after_duplicate)

        await host.shutdown()

    asyncio.run(run())


def test_search_with_timeout_registers_recorder_before_retention_captures_eager_completion() -> (
    None
):
    """Finding 3 regression: the provisional recorder is now registered
    before the coordinator call that could trigger the completion callback,
    not after. A fake coordinator here fires ``on_complete`` eagerly --
    synchronously inside ``retain_late_task``, standing in for whatever a
    future refactor might do -- to prove the reordered call sequence still
    finds the recorder in the registry. Under the old order (register after
    retain_late_task), this would have found no recorder to pop, silently
    dropped the work_item_background record, and then registered a recorder
    for already-completed work that only shutdown would later finalize as an
    incorrect ``cancelled``/``suppressed_shutdown``."""

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(measurement_sink=sink)

        class EagerCoordinator:
            def __init__(self) -> None:
                self.search_task: asyncio.Task[object] | None = None

            def start_task(self, operation: object) -> asyncio.Task[object] | None:
                self.search_task = asyncio.ensure_future(operation)
                return self.search_task

            def retain_late_task(
                self,
                task: asyncio.Task[object],
                *,
                work_item_id: str,
                worker_id: str,
                on_complete: object = None,
                on_late_terminal: object = None,
            ) -> bool:
                task.cancel()
                if on_late_terminal is not None:
                    on_late_terminal(work_item_id, "completed")
                if on_complete is not None:
                    result = GroundedResult(
                        result_id="result-eager",
                        worker_id=worker_id,
                        turn_id="turn-eager",
                        text="Eager result",
                        spoken_text="Eager result",
                        origin_epoch=1,
                    )
                    late = LateResult(
                        work_item_id=work_item_id,
                        worker_id=worker_id,
                        result=result,
                        terminal_kind="completed",
                    )
                    self._callback = asyncio.ensure_future(on_complete(late))
                return True

        host.coordinator = EagerCoordinator()

        async def never_completes(*_args: object, **_kwargs: object) -> GroundedResult:
            await asyncio.Future()
            raise AssertionError("unreachable")

        execution = await host._search_with_timeout(
            never_completes,
            "query",
            turn_id="turn-eager",
            origin_epoch=1,
            timeout=0.001,
            worker_id="worker-search",
            work_item_id="work-eager",
        )
        assert execution.status == "retained"

        await host.coordinator._callback
        await asyncio.gather(host.coordinator.search_task, return_exceptions=True)

        assert "work-eager" not in host._retained_recorders
        fields = _background_records(sink)[0].fields
        assert fields["work_item_id"] == "work-eager"
        assert fields["work_outcome"] == "completed"
        assert fields["commit_outcome"] == "committed"

    asyncio.run(run())


def test_raising_sink_does_not_change_turn_result_or_speech_behavior() -> None:
    async def run() -> None:
        class RaisingSink:
            def __init__(self) -> None:
                self.attempts = 0

            def emit(self, record: object) -> None:
                self.attempts += 1
                raise RuntimeError("sink exploded")

        search = ProjectedResultWorker()
        coordinator = ProjectedCoordinator(search)
        tts = FakeTTS()
        sink = RaisingSink()
        host = SessionHost(
            runner_factory=LifecycleRunner, tts=tts, coordinator=coordinator, measurement_sink=sink
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()

        result = await host._handle_transcript("historical capitals")

        assert result.text == "Answer for historical capitals"
        assert host.state.result_history("worker-search") == (result,)
        assert any(isinstance(frame, TTSSpeakFrame) for frame in connection.worker.frames)
        assert sink.attempts >= 1
        await host.shutdown()

    asyncio.run(run())


def test_stale_observer_callback_after_shutdown_is_console_only_and_state_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=_NonRetainingAddRunner,
            coordinator=RoutedCoordinator(ResultWorker()),
            measurement_sink=sink,
        )
        await host.start()

        pipeline_args: list[object] = []
        _monkeypatch_attach_connection_scaffolding(monkeypatch, pipeline_args=pipeline_args)

        await app_module._attach_connection(
            host,
            object(),
            app_module.SnapshotHandshake(
                session_id=host.state.session_id,
                resume_token=host.state.resume_token,
                proposed_epoch=1,
                snapshot_sequence=0,
            ),
        )
        tracker = host.connection.worker.turn_tracking_observer
        stale_turn_ended = tracker.handlers["on_turn_ended"]

        await host.shutdown()

        sequence_before = host.state.sequence
        await stale_turn_ended(object(), 9, 2.0, False)

        assert host.state.sequence == sequence_before
        matching = [record for record in sink.records if record.event == "pipecat_turn_end"]
        assert len(matching) == 1
        assert matching[0].fields["pipecat_turn"] == 9

    asyncio.run(run())


def test_stale_latest_value_turn_metrics_cache_is_fully_removed() -> None:
    """Repo-wide search: the legacy latest-value cache and all consumers must
    be fully removed after Phase 2 (Acceptance Criteria). The needle is built
    by concatenation so this assertion does not match its own source line.
    """
    import pathlib

    root = pathlib.Path(__file__).resolve().parent.parent
    needle = "last_turn" + "_metrics"
    offenders = [
        f"{path}:{lineno}: {line.strip()}"
        for directory in ("server", "scripts", "tests")
        for path in (root / directory).rglob("*.py")
        for lineno, line in enumerate(path.read_text().splitlines(), start=1)
        if needle in line
    ]
    assert offenders == [], f"legacy {needle} reference(s) found: {offenders}"


class _DelegatingRegistry:
    config = Config(max_work_items_per_turn=2)

    @staticmethod
    def catalogue() -> object:
        return object()


class _DelegatingRouter:
    @staticmethod
    def route_envelope(_text: str, _catalogue: object) -> object:
        decision = type("Decision", (), {"action": "existing_worker"})()
        return type("Envelope", (), {"decision": decision, "prose": None})()


def test_cancelled_delegated_turn_emits_exactly_one_parent_event() -> None:
    """A multi_intent turn cancelled inside the delegate must emit exactly one
    ``app_turn_foreground`` record: ``_handle_transcript_impl`` and the
    delegate share a single recorder, so the outer ``finalized`` guard sees
    the same object the delegate already closed.
    """

    async def run() -> None:
        worker = ResultWorker()
        submit_started = asyncio.Event()

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=_DelegatingRegistry(), router=_DelegatingRouter())

            def arbitrate(self, _session_id: str, _transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "multi_intent",
                        "decision": None,
                        "work_items": ("first item", "second item"),
                        "pending_dialogue": None,
                    },
                )()

            def dispatch(self, _decision: object, **_: object) -> object:
                return worker

            async def submit(self, *_args: object, **_kwargs: object) -> object:
                submit_started.set()
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner, coordinator=Coordinator(), measurement_sink=sink
        )
        await host.connect(connection_handshake(host, 1))

        turn = asyncio.create_task(host._handle_transcript("do two things"))
        await asyncio.wait_for(submit_started.wait(), timeout=2)
        turn.cancel()
        with pytest.raises(asyncio.CancelledError):
            await turn

        parents = _events(sink, "app_turn_foreground")
        assert len(parents) == 1
        assert parents[0].fields["outcome"] == "cancelled"
        await host.shutdown()

    asyncio.run(run())


def test_delegated_turn_parent_carries_routing_ms_and_total_ms_including_routing() -> None:
    """A delegated turn's parent event must carry the ``routing_ms`` measured
    by ``_handle_transcript_impl``, and its ``total_ms`` must start at parent
    turn_id allocation (before routing), per the plan's Timing Boundaries.
    """

    async def run() -> None:
        worker = ResultWorker()
        routing_delay_seconds = 0.05

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=_DelegatingRegistry(), router=_DelegatingRouter())

            def arbitrate(self, _session_id: str, _transcript: str) -> object:
                time.sleep(routing_delay_seconds)
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "multi_intent",
                        "decision": None,
                        "work_items": ("first item",),
                        "pending_dialogue": None,
                    },
                )()

            def dispatch(self, _decision: object, **_: object) -> object:
                return worker

        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner, coordinator=Coordinator(), measurement_sink=sink
        )
        await host.connect(connection_handshake(host, 1))

        await host._handle_transcript("do one thing")

        parents = _events(sink, "app_turn_foreground")
        assert len(parents) == 1
        fields = parents[0].fields
        routing_floor_ms = routing_delay_seconds * 1000 * 0.8
        assert fields["routing_ms"] >= routing_floor_ms
        assert fields["total_ms"] >= fields["routing_ms"]

        await host.shutdown()

    asyncio.run(run())


def test_uncaught_exception_in_handle_transcript_emits_one_failed_parent() -> None:
    """A non-cancellation exception escaping ``_handle_transcript_impl`` must
    still emit exactly one ``app_turn_foreground`` record with
    ``outcome=failed``, matching the two delegate handlers.
    """

    async def run() -> None:
        class DirectCoordinator:
            def arbitrate(self, _session_id: str, transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "routed",
                        "decision": type("Decision", (), {"action": "direct"})(),
                        "prose": "Here is a direct answer.",
                        "transcript": transcript,
                    },
                )()

        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=DirectCoordinator(),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        async def raising_commit_and_speak(*_args: object, **_kwargs: object) -> object:
            raise RuntimeError("commit exploded")

        host._commit_and_speak = raising_commit_and_speak  # type: ignore[method-assign]

        with pytest.raises(RuntimeError, match="commit exploded"):
            await host._handle_transcript("what is 2+2")

        parents = _events(sink, "app_turn_foreground")
        assert len(parents) == 1
        assert parents[0].fields["outcome"] == "failed"
        await host.shutdown()

    asyncio.run(run())


def test_router_resolved_direct_turn_reports_all_counters_zero() -> None:
    """A router-resolved direct turn dispatches no child, so ``child_count``
    and every category counter (including ``direct_count``) are zero while
    ``outcome`` still names the category.
    """

    async def run() -> None:
        class DirectCoordinator:
            def arbitrate(self, _session_id: str, transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "routed",
                        "decision": type("Decision", (), {"action": "direct"})(),
                        "prose": "Here is a direct answer.",
                        "transcript": transcript,
                    },
                )()

        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=DirectCoordinator(),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        await host._handle_transcript("what is 2+2")

        fields = _events(sink, "app_turn_foreground")[0].fields
        assert fields["outcome"] == "direct"
        assert fields["child_count"] == 0
        assert all(
            fields[name] == 0
            for name in (
                "direct_count",
                "unsupported_count",
                "completed_count",
                "retained_count",
                "clarification_count",
                "declined_count",
                "failed_count",
                "cancelled_count",
            )
        )
        await host.shutdown()

    asyncio.run(run())


def _work_item_failure(error_type: str, failure_kind: str) -> object:
    from server.work_item_coordinator import WorkItemFailure

    return WorkItemFailure(
        work_item_id="work-turn-0",
        worker_id="worker-a",
        error_type=error_type,
        error_message="worker execution failed",
        failure_kind=failure_kind,
    )


@pytest.mark.parametrize(
    ("error_type", "failure_kind", "expected_outcome"),
    [
        # Every row deliberately disagrees between the free-text ``error_type``
        # and the structured ``failure_kind``: the classification must follow
        # ``failure_kind`` alone, so pattern-matching ``error_type`` against a
        # sentinel table would fail these.
        ("CapacityError", "failed", "failed"),
        ("RetentionCapacityError", "failed", "failed"),
        ("CancelledError", "failed", "failed"),
        ("ValueError", "capacity_rejected", "capacity_rejected"),
        ("SomeRenamedClass", "cancelled", "cancelled"),
        ("ValueError", "retention_rejected", "retention_rejected"),
    ],
)
def test_failure_child_outcome_classifies_via_structured_failure_kind_not_error_type_name(
    error_type: str, failure_kind: str, expected_outcome: str
) -> None:
    failure = _work_item_failure(error_type, failure_kind)
    assert SessionHost._failure_child_outcome(failure) == expected_outcome


def test_failure_child_outcome_falls_back_to_failed_and_warns_for_off_domain_kind() -> None:
    records: list[object] = []
    sink_id = pipeline_module.logger.add(lambda message: records.append(message.record))
    try:
        failure = _work_item_failure("CapacityError", "nonsense")
        assert SessionHost._failure_child_outcome(failure) == "failed"
    finally:
        pipeline_module.logger.remove(sink_id)

    warnings = [record for record in records if "unclassified failure_kind" in record["message"]]
    assert len(warnings) == 1
    assert warnings[0]["level"].name == "WARNING"
    assert "'nonsense'" in warnings[0]["message"]


def test_failure_kinds_are_a_subset_of_work_item_outcomes() -> None:
    """Drift guard: every classification this helper can return must be a
    value the ``work_item_foreground`` outcome enum accepts."""
    from server.perf_metrics import WORK_ITEM_OUTCOMES
    from server.work_item_coordinator import FAILURE_KINDS

    assert FAILURE_KINDS <= WORK_ITEM_OUTCOMES


def test_routing_failure_logs_exception_type_without_traceback_or_message() -> None:
    """Routing failures must log only the exception's class name. A traceback
    (or the exception's own message) can carry provider payloads and secrets
    into the console, so the handler uses ``logger.warning`` rather than
    ``logger.exception``.
    """

    async def run() -> None:
        secret = "sk-live-do-not-log-this"

        class ExplodingCoordinator:
            def arbitrate(self, _session_id: str, _transcript: str) -> object:
                raise RuntimeError(f"provider rejected token {secret}")

        records: list[object] = []
        sink_id = pipeline_module.logger.add(lambda message: records.append(message.record))
        try:
            host = SessionHost(
                runner_factory=LifecycleRunner,
                coordinator=ExplodingCoordinator(),
                measurement_sink=CollectingMeasurementSink(),
            )
            await host.connect(connection_handshake(host, 1))

            result = await host._handle_transcript("what is 2+2")

            assert result.text.startswith("Routing is temporarily unavailable")
            routing_logs = [record for record in records if "Routing failed" in record["message"]]
            assert len(routing_logs) == 1
            entry = routing_logs[0]
            assert entry["level"].name == "WARNING"
            assert "RuntimeError" in entry["message"]
            assert secret not in entry["message"]
            assert entry["exception"] is None
            await host.shutdown()
        finally:
            pipeline_module.logger.remove(sink_id)

    asyncio.run(run())


def test_multi_intent_with_no_work_items_still_emits_exactly_one_parent() -> None:
    """A multi-intent turn that dispatches nothing must still produce one
    ``app_turn_foreground``: ``finalize`` derives ``failed`` rather than
    latching finalized and emitting nothing at all."""

    async def run() -> None:
        class EmptyMultiIntentCoordinator:
            @staticmethod
            def arbitrate(_session_id: str, _transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "multi_intent",
                        "decision": None,
                        "work_items": (),
                        "pending_dialogue": None,
                    },
                )()

            @staticmethod
            async def submit(*_args: object, **_kwargs: object) -> object:
                return type(
                    "Submitted",
                    (),
                    {
                        "results": (),
                        "pending_work_item_ids": (),
                        "failures": (),
                    },
                )()

        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=EmptyMultiIntentCoordinator(),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        assert await host._handle_transcript("do two things") == ()

        parents = _events(sink, "app_turn_foreground")
        assert len(parents) == 1
        assert parents[0].fields["outcome"] == "failed"
        assert parents[0].fields["child_count"] == 0
        assert _events(sink, "work_item_foreground") == ()
        await host.shutdown()

    asyncio.run(run())


def test_control_turn_without_control_action_emits_failed_parent_and_same_speech() -> None:
    """A control outcome carrying no ``control_action`` cannot form a valid
    ``outcome=control`` record (the pair is half-populated and the validator
    rejects it, dropping the record). It is recorded as ``failed`` instead,
    with the spoken fallback text unchanged."""

    async def run() -> None:
        class ActionlessControlCoordinator:
            @staticmethod
            def arbitrate(_session_id: str, _transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {"kind": "control", "decision": None, "work_items": ()},
                )()

        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=ActionlessControlCoordinator(),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("do the control thing")

        assert result.text == "Control request noted."
        parents = _events(sink, "app_turn_foreground")
        assert len(parents) == 1
        fields = parents[0].fields
        assert fields["outcome"] == "failed"
        assert "control_action" not in fields
        assert "control_outcome" not in fields
        await host.shutdown()

    asyncio.run(run())


def test_cancelling_a_turn_mid_search_emits_a_cancelled_child_and_counts_it() -> None:
    """A turn cancelled while its search is in flight must still emit the
    child's terminal record. The child recorder is created before the search
    begins, so cancellation used to orphan it: the parent reported
    ``child_count=0`` and no ``work_item_foreground`` was ever emitted."""

    async def run() -> None:
        worker = BlockingResultWorker()

        class CancellableCoordinator(RoutedCoordinator):
            def arbitrate(self, session_id: str, transcript: str) -> object:
                if transcript == "cancel":
                    return type(
                        "Outcome",
                        (),
                        {
                            "kind": "control",
                            "decision": None,
                            "work_items": (),
                            "control_action": "cancel",
                        },
                    )()
                return super().arbitrate(session_id, transcript)

        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=CancellableCoordinator(worker),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        pending = asyncio.create_task(host._handle_transcript("slow query"))
        await asyncio.wait_for(worker.started.wait(), 1)

        await host._handle_transcript("cancel")
        with pytest.raises(asyncio.CancelledError):
            await pending

        children = _events(sink, "work_item_foreground")
        assert len(children) == 1
        assert children[0].fields["outcome"] == "cancelled"
        cancelled_parents = [
            record
            for record in _events(sink, "app_turn_foreground")
            if record.fields["outcome"] == "cancelled"
        ]
        assert len(cancelled_parents) == 1
        assert cancelled_parents[0].fields["child_count"] == 1
        assert cancelled_parents[0].fields["cancelled_count"] == 1
        assert cancelled_parents[0].fields["turn_id"] == children[0].fields["turn_id"]
        worker.release.set()
        await host.shutdown()

    asyncio.run(run())


def test_commit_exception_after_a_completed_search_sweeps_the_child_as_failed() -> None:
    """``_commit_and_speak`` raises between the search completing and the
    child recorder being finalized, so the child is swept to ``failed``
    rather than left with no terminal record at all."""

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=RoutedCoordinator(ResultWorker()),
            measurement_sink=sink,
        )
        await host.connect(connection_handshake(host, 1))

        async def raising_commit_and_speak(*_args: object, **_kwargs: object) -> object:
            raise RuntimeError("commit exploded")

        host._commit_and_speak = raising_commit_and_speak  # type: ignore[method-assign]

        with pytest.raises(RuntimeError, match="commit exploded"):
            await host._handle_transcript("Riga weather")

        children = _events(sink, "work_item_foreground")
        assert len(children) == 1
        assert children[0].fields["outcome"] == "failed"
        parents = _events(sink, "app_turn_foreground")
        assert len(parents) == 1
        assert parents[0].fields["outcome"] == "failed"
        assert parents[0].fields["child_count"] == 1
        assert parents[0].fields["failed_count"] == 1
        await host.shutdown()

    asyncio.run(run())


def test_start_next_exception_after_commit_sweeps_the_turn_as_failed() -> None:
    """A genuine ``start_next`` failure (distinct from the documented
    no-TTS/DELIVERY_UNKNOWN short-circuit, which ``start_next`` itself
    resolves without raising) must propagate out of ``_commit_and_speak``
    so the turn is finalized as ``failed`` rather than silently standing
    as a successful commit."""

    async def run() -> None:
        tts = FakeTTS()
        sink = CollectingMeasurementSink()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=RoutedCoordinator(ResultWorker()),
            tts=tts,
            measurement_sink=sink,
        )
        connection = await host.connect(connection_handshake(host, 1))

        async def raising_start_next(work_item_id: str | None = None) -> object:
            raise RuntimeError("submission exploded")

        connection.scheduler.start_next = raising_start_next  # type: ignore[method-assign]

        with pytest.raises(RuntimeError, match="submission exploded"):
            await host._handle_transcript("Riga weather")

        children = _events(sink, "work_item_foreground")
        assert len(children) == 1
        assert children[0].fields["outcome"] == "failed"
        parents = _events(sink, "app_turn_foreground")
        assert len(parents) == 1
        assert parents[0].fields["outcome"] == "failed"
        await host.shutdown()

    asyncio.run(run())


def test_commit_and_speak_no_tts_short_circuit_completes_without_raising() -> None:
    """The legitimate no-TTS path (``require_tts and origin.tts is None``)
    must still complete ``_commit_and_speak`` without raising -- this is the
    documented short-circuit that removing the old broad except must not
    break."""

    async def run() -> None:
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=RoutedCoordinator(ResultWorker()),
        )
        connection = await host.connect(connection_handshake(host, 1))
        assert connection.tts is None

        result = GroundedResult(
            result_id="result-no-tts",
            worker_id="worker-search",
            turn_id="turn-no-tts",
            text="Answer",
            spoken_text="Spoken answer",
            origin_epoch=1,
        )
        committed = await host._commit_and_speak(result, connection)
        assert committed is result
        await host.shutdown()

    asyncio.run(run())


def test_handle_pending_snapshots_turn_sequence_before_first_await() -> None:
    """``_handle_pending`` must snapshot ``_turn_sequence`` before its first
    await (``_register_runner_worker``), matching ``_search_with_timeout``/
    ``_handle_multi_intent``. If a newer turn is accepted during that yield
    and the snapshot were instead taken after it (the bug), the late
    delivery context would wrongly believe its own dispatch was the latest
    turn and later resolve to ``autoplay`` instead of ``display_only``."""

    async def run() -> None:
        worker = ResultWorker()

        class Registry:
            config = Config(max_work_items_per_turn=2)

            @staticmethod
            def get(_worker_id: str) -> object:
                return type("Registered", (), {"worker": worker})()

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=Registry(), router=object())

            async def submit(
                self,
                work_item_id: str,
                _items: object,
                _execute: object,
                **_kwargs: object,
            ) -> object:
                # Report the work item as retained/still-pending so
                # _handle_pending takes the late-delivery-context path.
                return type(
                    "Submitted",
                    (),
                    {"results": (), "pending_work_item_ids": (work_item_id,), "failures": ()},
                )()

        sink = CollectingMeasurementSink()
        coordinator = Coordinator()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=coordinator,
            measurement_sink=sink,
            promotion_manifest=PromotionManifest(promotion_eligible=True),
        )
        origin = await host.connect(connection_handshake(host, 1))

        original_register_runner_worker = host._register_runner_worker

        async def register_and_accept_newer_turn(worker_arg: object) -> None:
            # Simulate a newer semantic turn being accepted concurrently,
            # during the exact yield _handle_pending suspends on.
            host._turn_sequence += 1
            await original_register_runner_worker(worker_arg)

        host._register_runner_worker = register_and_accept_newer_turn  # type: ignore[method-assign]

        captured_contexts: list[LateDeliveryContext] = []
        original_new_late_delivery_context = host._new_late_delivery_context

        def capturing_new_late_delivery_context(**kwargs: object) -> LateDeliveryContext:
            context = original_new_late_delivery_context(**kwargs)
            captured_contexts.append(context)
            return context

        host._new_late_delivery_context = capturing_new_late_delivery_context  # type: ignore[method-assign]

        pending_dialogue = type(
            "Pending",
            (),
            {
                "owner_id": "worker-search",
                "original_query": "continue please",
                "question": "Which one?",
            },
        )()
        outcome = type("Outcome", (), {"pending_dialogue": pending_dialogue, "work_items": ()})()

        await host._handle_pending(
            outcome,
            "continue please",
            origin,
            "turn-pending-snapshot",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-pending-snapshot"),
        )

        assert len(captured_contexts) == 1
        late_context = captured_contexts[0]
        # The snapshot must reflect the sequence at dispatch time (0), not
        # the post-yield value (1) that the newer turn bumped it to.
        assert late_context.accepted_turn_sequence == 0
        assert host._turn_sequence == 1
        assert host._late_result_disposition(late_context, origin=origin) == "display_only"
        await host.shutdown()

    asyncio.run(run())


def test_pending_turn_cancelled_mid_submit_sweeps_the_child_as_cancelled() -> None:
    """The ``_handle_pending`` path owns its child the same way: a turn
    cancelled while ``coordinator.submit`` is in flight still emits one
    terminal ``work_item_foreground``."""

    async def run() -> None:
        worker = ResultWorker()

        class Registry:
            config = Config(max_work_items_per_turn=2)

            @staticmethod
            def get(_worker_id: str) -> object:
                return type("Registered", (), {"worker": worker})()

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=Registry(), router=object())
                self.submitting = asyncio.Event()

            async def submit(self, *_args: object, **_kwargs: object) -> object:
                self.submitting.set()
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

        sink = CollectingMeasurementSink()
        coordinator = Coordinator()
        host = SessionHost(
            runner_factory=LifecycleRunner, coordinator=coordinator, measurement_sink=sink
        )
        origin = await host.connect(connection_handshake(host, 1))
        pending_dialogue = type(
            "Pending",
            (),
            {
                "owner_id": "worker-search",
                "original_query": "continue please",
                "question": "Which one?",
            },
        )()
        outcome = type("Outcome", (), {"pending_dialogue": pending_dialogue, "work_items": ()})()

        turn = asyncio.create_task(
            host._handle_pending(
                outcome,
                "continue please",
                origin,
                "turn-pending-cancel",
                host._new_app_turn_recorder(
                    origin_epoch=origin.epoch, turn_id="turn-pending-cancel"
                ),
            )
        )
        await asyncio.wait_for(coordinator.submitting.wait(), 1)
        turn.cancel()
        with pytest.raises(asyncio.CancelledError):
            await turn

        children = _events(sink, "work_item_foreground")
        assert len(children) == 1
        assert children[0].fields["outcome"] == "cancelled"
        parents = _events(sink, "app_turn_foreground")
        assert len(parents) == 1
        assert parents[0].fields["outcome"] == "cancelled"
        assert parents[0].fields["child_count"] == 1
        assert parents[0].fields["cancelled_count"] == 1
        await host.shutdown()

    asyncio.run(run())


class _FanInRegistry:
    config = Config(max_work_items_per_turn=4)

    @staticmethod
    def catalogue() -> object:
        return object()


class _FanInRouter:
    @staticmethod
    def route_envelope(_text: str, _catalogue: object) -> object:
        decision = type("Decision", (), {"action": "existing_worker"})()
        return type("Envelope", (), {"decision": decision, "prose": None})()


def _fan_in_host(
    submitted: object,
    sink: CollectingMeasurementSink,
    *,
    promotion_manifest: PromotionManifest | None = None,
) -> tuple[SessionHost, object]:
    """A host whose coordinator returns a hand-built ``submit`` outcome, so the
    multi-intent fan-in loops can be driven with mismatched echoes."""
    worker = ResultWorker()

    class Coordinator(WorkItemCoordinator):
        def __init__(self) -> None:
            super().__init__(registry=_FanInRegistry(), router=_FanInRouter())

        def dispatch(self, _decision: object, **_: object) -> object:
            return worker

        async def submit(self, *_args: object, **_kwargs: object) -> object:
            return submitted

    return (
        SessionHost(
            runner_factory=LifecycleRunner,
            coordinator=Coordinator(),
            measurement_sink=sink,
            promotion_manifest=promotion_manifest,
        ),
        worker,
    )


def _submitted(
    *,
    results: tuple[object, ...] = (),
    pending_work_item_ids: tuple[str, ...] = (),
    failures: tuple[object, ...] = (),
) -> object:
    return type(
        "Submitted",
        (),
        {
            "results": results,
            "pending_work_item_ids": pending_work_item_ids,
            "failures": failures,
        },
    )()


def _multi_intent_outcome(*items: str) -> object:
    return type("Outcome", (), {"work_items": items, "pending_dialogue": None})()


def _fan_in_result(turn_id: str, text: str) -> GroundedResult:
    return GroundedResult(
        result_id=f"result-{turn_id}-{text}",
        worker_id="worker-search",
        turn_id=turn_id,
        text=text,
        spoken_text=text,
        origin_epoch=1,
    )


def test_multi_intent_unmatched_result_turn_id_is_dropped_without_losing_the_turn() -> None:
    """A worker-echoed turn_id that matches no dispatched work item must not
    take down the whole turn: siblings still commit and emit, and the
    unattributable item is swept to a terminal ``failed`` record."""

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host, _worker = _fan_in_host(
            _submitted(
                results=(
                    _fan_in_result("turn-fan-0", "first answer"),
                    _fan_in_result("turn-fan-not-a-dispatched-item", "orphan answer"),
                )
            ),
            sink,
        )
        origin = await host.connect(connection_handshake(host, 1))

        committed = await host._handle_multi_intent(
            _multi_intent_outcome("first item", "second item"),
            "",
            origin,
            "turn-fan",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-fan"),
        )

        assert [result.text for result in committed] == ["first answer"]
        children = _events(sink, "work_item_foreground")
        assert {child.fields["work_item_id"]: child.fields["outcome"] for child in children} == {
            "work-turn-fan-0": "completed",
            "work-turn-fan-1": "failed",
        }
        parent = _events(sink, "app_turn_foreground")[0].fields
        assert parent["child_count"] == len(children)
        assert parent["completed_count"] == 1
        assert parent["failed_count"] == 1
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_duplicate_result_turn_ids_keep_last_content_and_one_child_record() -> None:
    """Two results colliding on one turn_id preserve the existing last-wins
    commit semantics while emitting exactly one child record for that item,
    and that child record names the same last-wins result that committed."""

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host, _worker = _fan_in_host(
            _submitted(
                results=(
                    _fan_in_result("turn-dup-0", "first answer"),
                    _fan_in_result("turn-dup-0", "second answer"),
                )
            ),
            sink,
        )
        origin = await host.connect(connection_handshake(host, 1))

        committed = await host._handle_multi_intent(
            _multi_intent_outcome("first item", "second item"),
            "",
            origin,
            "turn-dup",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-dup"),
        )

        assert [result.text for result in committed] == ["second answer"]
        children = _events(sink, "work_item_foreground")
        item_zero = [
            child for child in children if child.fields["work_item_id"] == "work-turn-dup-0"
        ]
        assert len(item_zero) == 1
        assert item_zero[0].fields["outcome"] == "completed"
        assert item_zero[0].fields["result_id"] == "result-turn-dup-0-second answer"
        parent = _events(sink, "app_turn_foreground")[0].fields
        assert parent["child_count"] == len(children)
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_malformed_pending_and_failure_ids_warn_instead_of_raising() -> None:
    """A non-numeric work_item_id suffix and an out-of-range index must both
    warn and continue: neither can raise out of the handler and silently drop
    the whole turn."""

    async def run() -> None:
        from server.work_item_coordinator import WorkItemFailure

        sink = CollectingMeasurementSink()
        host, _worker = _fan_in_host(
            _submitted(
                results=(_fan_in_result("turn-bad-0", "first answer"),),
                pending_work_item_ids=("work-turn-bad-not-an-index",),
                failures=(
                    WorkItemFailure(
                        work_item_id="work-turn-bad-99",
                        worker_id="worker-search",
                        error_type="RuntimeError",
                        error_message="boom",
                        failure_kind="failed",
                    ),
                ),
            ),
            sink,
        )
        origin = await host.connect(connection_handshake(host, 1))

        committed = await host._handle_multi_intent(
            _multi_intent_outcome("first item", "second item"),
            "",
            origin,
            "turn-bad",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-bad"),
        )

        assert [result.text for result in committed] == ["first answer"]
        children = _events(sink, "work_item_foreground")
        assert {child.fields["work_item_id"]: child.fields["outcome"] for child in children} == {
            "work-turn-bad-0": "completed",
            "work-turn-bad-1": "failed",
        }
        parent = _events(sink, "app_turn_foreground")[0].fields
        assert parent["child_count"] == len(children)
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_retained_notice_is_removed_before_late_result() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host, _worker = _fan_in_host(
            _submitted(
                results=(_fan_in_result("turn-role-0", "first answer"),),
                pending_work_item_ids=("work-turn-role-1",),
            ),
            sink,
            promotion_manifest=PromotionManifest(promotion_eligible=True),
        )
        host.tts = FakeTTS()
        origin = await host.connect(connection_handshake(host, 1))
        origin.worker = QueueingPipelineWorker()

        committed = await host._handle_multi_intent(
            _multi_intent_outcome("first item", "second item"),
            "",
            origin,
            "turn-role",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-role"),
        )

        assert [result.text for result in committed] == [
            "first answer",
            "That item is taking longer than expected; I will continue in the background.",
        ]
        queued_notice = origin.scheduler._queues["work-turn-role-1"][0]
        assert queued_notice.role == ROLE_TIMEOUT_NOTICE

        await host.commit_late_result_once(
            _late_delivery_context(
                host,
                turn_id="turn-role",
                work_item_id="work-turn-role-1",
                origin_epoch=origin.epoch,
            ),
            LateResult(
                work_item_id="work-turn-role-1",
                worker_id="worker-search",
                result=_fan_in_result("turn-role-1", "late answer"),
            ),
        )

        queued = origin.scheduler._queues["work-turn-role-1"]
        assert [item.text for item in queued] == ["late answer"]
        assert queued[0].role == ROLE_RESULT
        await host.shutdown()

    asyncio.run(run())


def _ack_items(scheduler: object) -> list[object]:
    """Every item across queues plus the active lease with ``role == "ack"``.

    Phase 1's ack items are wire-invisible (never call
    ``SessionState.speech_progress()``), so they cannot be observed through
    ``host.state.speech``; this scans the scheduler's own bookkeeping
    instead, matching the plan's "SpeechScheduler owns the ack item, ack_id
    index, queued discard" ownership bullet.
    """
    from server.speech_scheduler import ROLE_ACK

    items = [
        item for queue in scheduler._queues.values() for item in queue if item.role == ROLE_ACK
    ]
    active = scheduler.active
    if active is not None and active.item.role == ROLE_ACK:
        items.append(active.item)
    return items


def test_early_ack_is_enqueued_immediately_after_delegated_search_dispatch() -> None:
    """Phase 1 Goal: 'Replace timeout-gated silence with a deterministic ack
    emitted the instant routing confirms delegation.' The ack must be visible
    while the delegated search is still in flight, before any result exists
    (plan: 'without claiming progress that hasn't happened')."""

    async def run() -> None:
        search = BlockingResultWorker()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=FakeTTS(),
            coordinator=RoutedCoordinator(search),
        )
        origin = await host.connect(connection_handshake(host, 1))
        origin.worker = QueueingPipelineWorker()

        pending = asyncio.create_task(host._handle_transcript("today's weather"))
        await asyncio.wait_for(search.started.wait(), timeout=1)

        acks = _ack_items(origin.scheduler)
        assert len(acks) == 1
        assert acks[0].result_id is None or acks[0].ack_id is not None

        # Let the deferred ack admission run, then complete/release its TTS
        # lease -- mirroring the real pipeline, where the ack always finishes
        # speaking well before a genuinely slow delegated search returns --
        # so the real result can be admitted once the search resolves.
        while origin.scheduler.active is None:
            await asyncio.sleep(0)
        lease = origin.scheduler.active
        await release_lifecycle_slot(origin, lease.token, lease.item.utterance_id)

        search.release.set()
        result = await asyncio.wait_for(pending, timeout=1)

        assert result.text.startswith("Answer for")
        assert _ack_items(origin.scheduler) == []
        await host.shutdown()

    asyncio.run(run())


def test_no_early_ack_for_the_non_delegated_direct_route() -> None:
    """The plan disambiguates: ``RoutingDecision.action=="direct"`` is the
    non-delegated main-responder path and remains ack-free -- only a direct
    *web-search delegation* (existing_worker/new_worker) is eligible."""

    async def run() -> None:
        class DirectCoordinator:
            def arbitrate(self, _session_id: str, transcript: str) -> object:
                decision = type("Decision", (), {"action": "direct"})()
                return type(
                    "Outcome",
                    (),
                    {"kind": "routed", "decision": decision, "prose": "Direct answer text."},
                )()

        host = SessionHost(runner_factory=LifecycleRunner, coordinator=DirectCoordinator())
        origin = await host.connect(connection_handshake(host, 1))

        result = await host._handle_transcript("what is 2 + 2")

        assert result.text == "Direct answer text."
        assert _ack_items(origin.scheduler) == []
        assert len(origin.scheduler.state.speech) == 1
        await host.shutdown()

    asyncio.run(run())


@pytest.mark.parametrize("action", ["unsupported", "clarify"])
def test_no_early_ack_for_unsupported_and_clarify_routes(action: str) -> None:
    async def run() -> None:
        class UnsupportedOrClarifyCoordinator:
            def arbitrate(self, _session_id: str, transcript: str) -> object:
                decision = type("Decision", (), {"action": action})()
                return type("Outcome", (), {"kind": "routed", "decision": decision})()

        host = SessionHost(
            runner_factory=LifecycleRunner, coordinator=UnsupportedOrClarifyCoordinator()
        )
        origin = await host.connect(connection_handshake(host, 1))

        await host._handle_transcript("do something out of scope")

        assert _ack_items(origin.scheduler) == []
        assert len(origin.scheduler.state.speech) == 1
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_turn_emits_exactly_one_parent_ack_for_its_sole_delegated_child() -> None:
    """Plan: 'emit one parent ack when at least one child is delegated, never
    one ack per child.' Uses the same mixed-route Router/Coordinator
    scaffolding as
    ``test_multi_intent_mixed_outcomes_emit_one_child_per_item_and_mixed_parent``,
    but with a blocking delegated worker so the ack is observable mid-flight
    and only one delegated child exists."""

    async def run() -> None:
        search = BlockingResultWorker()

        class Registry:
            config = Config(max_work_items_per_turn=4)

            @staticmethod
            def catalogue() -> object:
                return object()

        class Router:
            @staticmethod
            def route_envelope(text: str, _catalogue: object) -> object:
                action = {
                    "answer directly": "direct",
                    "ask me": "clarify",
                    "unsupported thing": "unsupported",
                    "search it": "existing_worker",
                }[text]
                decision = type("Decision", (), {"action": action})()
                prose = {
                    "direct": "Direct answer.",
                    "clarify": "Which one?",
                    "unsupported": None,
                    "existing_worker": None,
                }[action]
                return type("Envelope", (), {"decision": decision, "prose": prose})()

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=Registry(), router=Router())

            def dispatch(self, decision: object, **_: object) -> object:
                assert decision.action == "existing_worker"
                return search

        host = SessionHost(runner_factory=LifecycleRunner, tts=FakeTTS(), coordinator=Coordinator())
        origin = await host.connect(connection_handshake(host, 1))
        origin.worker = QueueingPipelineWorker()
        outcome = type(
            "Outcome",
            (),
            {
                "work_items": (
                    "answer directly",
                    "ask me",
                    "unsupported thing",
                    "search it",
                ),
                "pending_dialogue": None,
            },
        )()

        pending = asyncio.create_task(
            host._handle_multi_intent(
                outcome,
                "",
                origin,
                "turn-mixed-ack",
                host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-mixed-ack"),
            )
        )
        await asyncio.wait_for(search.started.wait(), timeout=1)

        acks = _ack_items(origin.scheduler)
        assert len(acks) == 1

        # Let the deferred ack admission run, then complete/release its TTS
        # lease (see test_early_ack_is_enqueued_immediately_after_delegated_
        # search_dispatch) so the real result can be admitted once the
        # search resolves.
        while origin.scheduler.active is None:
            await asyncio.sleep(0)
        lease = origin.scheduler.active
        await release_lifecycle_slot(origin, lease.token, lease.item.utterance_id)

        search.release.set()
        await asyncio.wait_for(pending, timeout=1)

        assert _ack_items(origin.scheduler) == []
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_turn_with_only_non_delegated_children_gets_no_ack() -> None:
    async def run() -> None:
        class Registry:
            config = Config(max_work_items_per_turn=4)

            @staticmethod
            def catalogue() -> object:
                return object()

        class Router:
            @staticmethod
            def route_envelope(text: str, _catalogue: object) -> object:
                action = {
                    "answer directly": "direct",
                    "ask me": "clarify",
                    "unsupported thing": "unsupported",
                }[text]
                decision = type("Decision", (), {"action": action})()
                prose = {
                    "direct": "Direct answer.",
                    "clarify": "Which one?",
                    "unsupported": None,
                }[action]
                return type("Envelope", (), {"decision": decision, "prose": prose})()

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=Registry(), router=Router())

        host = SessionHost(runner_factory=LifecycleRunner, coordinator=Coordinator())
        origin = await host.connect(connection_handshake(host, 1))
        outcome = type(
            "Outcome",
            (),
            {
                "work_items": ("answer directly", "ask me", "unsupported thing"),
                "pending_dialogue": None,
            },
        )()

        await host._handle_multi_intent(
            outcome,
            "",
            origin,
            "turn-no-ack",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-no-ack"),
        )

        assert _ack_items(origin.scheduler) == []
        await host.shutdown()

    asyncio.run(run())


def test_early_ack_is_discarded_without_being_admitted_when_the_result_is_already_ready() -> None:
    """Plan: 'an admitted ack may finish but never blocks a result from being
    committed' and the ack 'must be dropped atomically via an explicit
    discard_queued_ack(ack_id) operation if the real result is ready before
    admission.' Occupies the one global transport slot with an unrelated
    generation first, so the ack can only ever be queued, never admitted,
    while its result resolves underneath it."""

    async def run() -> None:
        from server.speech_lifecycle import GenerationIdentity, SpeechLifecycleCoordinator

        search = BlockingResultWorker()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=FakeTTS(),
            coordinator=RoutedCoordinator(search),
        )
        origin = await host.connect(connection_handshake(host, 1))
        # Occupy the single connection-scoped slot with unrelated speech so
        # the ack this turn produces can only ever be queued.
        assert isinstance(origin.lifecycle, SpeechLifecycleCoordinator)
        occupying = origin.lifecycle.try_admit(GenerationIdentity("occupier", "work-occupier"))
        assert occupying is not None

        pending = asyncio.create_task(host._handle_transcript("today's weather"))
        await asyncio.wait_for(search.started.wait(), timeout=1)
        assert len(_ack_items(origin.scheduler)) == 1

        search.release.set()
        result = await asyncio.wait_for(pending, timeout=1)

        assert result.text.startswith("Answer for")
        assert _ack_items(origin.scheduler) == []
        await host.shutdown()

    asyncio.run(run())


def test_a_child_that_bails_before_enqueueing_does_not_consume_the_turns_ack_slot() -> None:
    """The turn latch must be claimed only by the child that actually enqueues
    the ack. A first eligible child of a multi-intent turn that bails (no TTS
    on the origin, or a search that resolves within one tick) must leave the
    turn's single ack slot available for a later, genuinely slow child of the
    same ``turn_id``."""

    async def run() -> None:
        host = SessionHost(runner_factory=LifecycleRunner, tts=FakeTTS())
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        turn_id = "turn-multi"

        # First child: origin has no TTS, so it bails before enqueueing.
        tts = connection.tts
        connection.tts = None
        await host._emit_early_ack(
            connection, turn_id=turn_id, origin_epoch=connection.epoch, dispatched=False
        )
        connection.tts = tts
        assert f"ack-{turn_id}" not in connection.scheduler._queues

        # Second child: a search that resolves within one tick, so it also
        # bails before enqueueing.
        fast: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(0))
        await host._emit_early_ack(
            connection,
            turn_id=turn_id,
            origin_epoch=connection.epoch,
            dispatched=True,
            search_task=fast,
        )
        await fast
        assert f"ack-{turn_id}" not in connection.scheduler._queues

        # Third child of the same turn is genuinely slow: it must still get
        # the turn's one ack.
        slow: asyncio.Task[None] = asyncio.create_task(asyncio.Event().wait())
        await host._emit_early_ack(
            connection,
            turn_id=turn_id,
            origin_epoch=connection.epoch,
            dispatched=True,
            search_task=slow,
        )
        assert f"ack-{turn_id}" in connection.scheduler._queues or any(
            lease is not None and lease.item.work_item_id == f"ack-{turn_id}"
            for lease in (connection.scheduler.active,)
        )
        assert turn_id in host._ack_emitted_turns

        slow.cancel()
        await host.shutdown()

    asyncio.run(run())


def test_explicit_cancel_of_one_child_of_a_multi_child_turn_leaves_the_ack_for_the_others() -> None:
    """An explicit ``cancel``/``stop`` control message targeting a single child
    must go through the host's cancellation boundary: the parent ack survives
    while another delegated child remains, and is cancelled once the targeted
    child was the sole remaining one."""

    from server.speech_scheduler import ROLE_ACK

    async def run() -> None:
        class ControlCoordinator:
            def __init__(self) -> None:
                self.target: str | None = None

            def arbitrate(self, _session_id: str, _transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "control",
                        "decision": None,
                        "work_items": (self.target,) if self.target else (),
                        "control_action": "cancel",
                    },
                )()

        coordinator = ControlCoordinator()
        host = SessionHost(runner_factory=LifecycleRunner, tts=FakeTTS(), coordinator=coordinator)
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()
        turn_id = "turn-cancel"
        ack_work_item_id = f"ack-{turn_id}"
        host._ack_emitted_turns.add(turn_id)
        connection.scheduler.enqueue(
            result_id=None,
            work_item_id=ack_work_item_id,
            run_id=f"run-{ack_work_item_id}",
            text="One moment.",
            origin_epoch=1,
            role=ROLE_ACK,
            ack_id=ack_work_item_id,
            turn_id=turn_id,
        )
        for index in (0, 1):
            connection.scheduler.enqueue(
                result_id=f"result-{index}",
                work_item_id=f"work-{turn_id}-{index}",
                run_id=f"run-{index}",
                text=f"child {index}",
                origin_epoch=1,
            )

        # The turn -> delegated-child registry is the authority for ack
        # ownership and the sole-child decision, so this hand-built turn has to
        # register its children exactly as a real turn handler would.
        for index in (0, 1):
            host._register_turn_work_item(turn_id, f"work-{turn_id}-{index}")

        coordinator.target = f"work-{turn_id}-0"
        await host._handle_transcript(f"cancel work-{turn_id}-0")
        assert ack_work_item_id in connection.scheduler._queues
        assert turn_id in host._ack_emitted_turns

        # Let the first control confirmation finish speaking, so the second
        # cancel really does target the turn's sole remaining delegated child.
        active = connection.scheduler.active
        assert active is not None
        connection.scheduler.delivery_completed(active.item.utterance_id)
        await acknowledge_lifecycle_flush(connection, active.token)

        coordinator.target = f"work-{turn_id}-1"
        await host._handle_transcript(f"cancel work-{turn_id}-1")
        assert ack_work_item_id not in connection.scheduler._queues
        assert turn_id not in host._ack_emitted_turns

        # A whole-turn cancel removes an earlier turn's live ack and its latch
        # too: nothing may speak an ack for work the user just cancelled.
        other_turn = "turn-swept"
        other_ack = f"ack-{other_turn}"
        host._ack_emitted_turns.add(other_turn)
        connection.scheduler.enqueue(
            result_id=None,
            work_item_id=other_ack,
            run_id=f"run-{other_ack}",
            text="One moment.",
            origin_epoch=1,
            role=ROLE_ACK,
            ack_id=other_ack,
            turn_id=other_turn,
        )
        coordinator.target = None
        frames_before_sweep = len(connection.worker.frames)
        await host._handle_transcript("cancel")
        assert other_ack not in connection.scheduler._queues
        assert other_turn not in host._ack_emitted_turns
        assert all(
            not isinstance(frame, TTSSpeakFrame) or frame.text != "One moment."
            for frame in connection.worker.frames[frames_before_sweep:]
        )

        await host.shutdown()

    asyncio.run(run())


# --- M12: one shared outcome -> work-status derivation ---------------------
#
# Both status-deriving call sites (the delegated-child finalization path and
# ``commit_late_result_once``'s finally block) must route through
# ``_work_status_for_outcome``. These references reimplement the two former
# inline derivations; the parametrised tables below lock them to the shared
# helper so the two sites can never drift apart again.


def _reference_delegated_child_site(
    child_outcome_label: str, was_cancelled: bool
) -> tuple[str, str | None]:
    if was_cancelled:
        return "cancelled", None
    if child_outcome_label in {"completed", "clarify", "declined"}:
        return "result_ready", None
    return (
        "failed",
        "retention_rejected" if child_outcome_label == "retention_rejected" else None,
    )


def _reference_late_commit_site(
    work_outcome: str | None, commit_outcome: str | None, terminal_kind: str | None
) -> tuple[str, str | None] | None:
    if commit_outcome == "suppressed_duplicate":
        # Another copy of this exact result_id already committed and drove the
        # work item terminal, so no status is emitted at all. suppressed_stale
        # is not in that position -- nothing committed under this ledger key --
        # so it must still terminalize below.
        return None
    if work_outcome == "completed" and commit_outcome == "committed":
        return "result_ready", None
    if work_outcome == "cancelled":
        return "cancelled", None
    if work_outcome is not None:
        return (
            "failed",
            "retention_rejected" if terminal_kind == "retention_rejected" else None,
        )
    return None


_CHILD_OUTCOME_LABELS = (
    "completed",
    "retained",
    "capacity_rejected",
    "retention_rejected",
    "failed",
    "clarify",
    "declined",
)


@pytest.mark.parametrize("label", _CHILD_OUTCOME_LABELS)
@pytest.mark.parametrize("was_cancelled", (False, True))
def test_work_status_helper_matches_the_delegated_child_derivation(
    label: str, was_cancelled: bool
) -> None:
    assert pipeline_module._work_status_for_outcome(
        label, cancelled=was_cancelled, terminal_kind=label
    ) == _reference_delegated_child_site(label, was_cancelled)


@pytest.mark.parametrize(
    "work_outcome",
    (None, "completed", "cancelled", "invalid_result", "failed", "retention_rejected"),
)
@pytest.mark.parametrize(
    "commit_outcome",
    (
        None,
        "committed",
        "not_applicable",
        "suppressed_cancelled",
        "suppressed_stale",
        "suppressed_duplicate",
        "failed",
    ),
)
@pytest.mark.parametrize("terminal_kind", (None, "completed", "cancelled", "retention_rejected"))
def test_work_status_helper_matches_the_late_commit_derivation(
    work_outcome: str | None, commit_outcome: str | None, terminal_kind: str | None
) -> None:
    assert pipeline_module._late_commit_work_status(
        work_outcome,
        commit_outcome=commit_outcome,
        terminal_kind=terminal_kind,
    ) == _reference_late_commit_site(work_outcome, commit_outcome, terminal_kind)


# --- I6/I7/I9: work-status emission gaps -----------------------------------
#
# I6: ``_handle_pending`` and ``_handle_multi_intent`` emitted no work_status
# at all -- a continuation or compound turn was invisible to a capable client.
# I7: a retained child on a non-capable connection fell through to the generic
# terminal block and was reported as ``failed`` -- a benign "still working"
# state published as a failure, which the coordinator could never later flip
# to ``result_ready`` (a terminal parent never regresses).
# I9: a directly-cancelled turn emitted no terminal status, stranding a
# capable client on a non-terminal ``routing``/``searching`` record.


def _work_status_states(host: SessionHost) -> list[str]:
    return [event.payload["state"] for event in host.state.events if event.kind == "work_status"]


def _pending_outcome() -> object:
    pending = type(
        "Pending",
        (),
        {
            "owner_id": "worker-search",
            "original_query": "continue please",
            "question": "Which one?",
        },
    )()
    return type("Outcome", (), {"pending_dialogue": pending, "work_items": ()})()


def _pending_host(submitted: object, sink: CollectingMeasurementSink) -> SessionHost:
    """A host whose coordinator returns a hand-built ``submit`` outcome for the
    ``continue_pending`` path."""
    worker = ResultWorker()

    class Registry:
        config = Config(max_work_items_per_turn=2)

        @staticmethod
        def get(_worker_id: str) -> object:
            return type("Registered", (), {"worker": worker})()

    class Coordinator(WorkItemCoordinator):
        def __init__(self) -> None:
            super().__init__(registry=Registry(), router=object())

        async def submit(self, *_args: object, **_kwargs: object) -> object:
            return submitted

    return SessionHost(
        runner_factory=LifecycleRunner, coordinator=Coordinator(), measurement_sink=sink
    )


def test_pending_continuation_turn_emits_routing_through_terminal_work_status() -> None:
    """I6: a ``continue_pending`` turn delegates real work, so its parent
    record must progress routing -> searching -> terminal like any other
    delegated turn."""

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = _pending_host(_submitted(results=(_fan_in_result("turn-pending", "answer"),)), sink)
        origin = await host.connect(connection_handshake(host, 1))

        await host._handle_pending(
            _pending_outcome(),
            "continue please",
            origin,
            "turn-pending",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-pending"),
        )

        assert _work_status_states(host) == ["routing", "searching", "result_ready"]
        statuses = host.state.work_status_snapshot()
        assert [status.work_item_id for status in statuses] == ["work-turn-pending"]
        await host.shutdown()

    asyncio.run(run())


def test_pending_continuation_retained_child_reports_background_not_failed() -> None:
    """I6 + I7: a retained continuation is still working -- never ``failed``."""

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = _pending_host(_submitted(pending_work_item_ids=("work-turn-pending",)), sink)
        origin = await host.connect(connection_handshake(host, 1))

        await host._handle_pending(
            _pending_outcome(),
            "continue please",
            origin,
            "turn-pending",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-pending"),
        )

        states = _work_status_states(host)
        assert states == ["routing", "searching", "background"]
        assert "failed" not in states
        await host.shutdown()

    asyncio.run(run())


@pytest.mark.parametrize("path", ["direct_delegated", "pending_continuation", "mixed_multi_intent"])
def test_disabled_early_ack_leaves_no_scheduling_latch_index_or_media_residue(path: str) -> None:
    """Acceptance criterion: with ``enable_early_ack=False``, no ack
    scheduling/metrics/latch/index/media residue occurs on any of the three
    ack-eligible turn shapes -- direct-delegated, pending-continuation, and
    mixed multi-intent. ``_emit_early_ack`` returns at its very first line
    when the flag is off, so this asserts the turn-scoped latch
    (``host._ack_emitted_turns``), the scheduler's ack queue/active lease
    (``_ack_items``, matching the enabled-ack tests above), and the
    scheduler's ack_id index (``origin.scheduler._ack_index``) all stay
    empty even though every path here is otherwise ack-eligible (a real TTS
    connection, a genuinely in-flight delegated child)."""

    async def run() -> None:
        if path == "direct_delegated":
            search = BlockingResultWorker()
            config = Config(enable_early_ack=False)
            host = SessionHost(
                registry=WorkerRegistry(config=config),
                config=config,
                runner_factory=LifecycleRunner,
                tts=FakeTTS(),
                coordinator=RoutedCoordinator(search),
            )
            origin = await host.connect(connection_handshake(host, 1))
            origin.worker = QueueingPipelineWorker()
            assert host.feature_policy.enable_early_ack is False

            pending = asyncio.create_task(host._handle_transcript("today's weather"))
            await asyncio.wait_for(search.started.wait(), timeout=1)

            assert _ack_items(origin.scheduler) == []
            assert host._ack_emitted_turns == set()
            assert origin.scheduler._ack_index == {}

            search.release.set()
            result = await asyncio.wait_for(pending, timeout=1)
            assert result.text.startswith("Answer for")

        elif path == "pending_continuation":
            worker = ResultWorker()

            class Registry:
                config = Config(max_work_items_per_turn=2, enable_early_ack=False)

                @staticmethod
                def get(_worker_id: str) -> object:
                    return type("Registered", (), {"worker": worker})()

            class Coordinator(WorkItemCoordinator):
                def __init__(self) -> None:
                    super().__init__(registry=Registry(), router=object())

                async def submit(self, *_args: object, **_kwargs: object) -> object:
                    return _submitted(results=(_fan_in_result("turn-pending-noack", "answer"),))

            host = SessionHost(
                runner_factory=LifecycleRunner, tts=FakeTTS(), coordinator=Coordinator()
            )
            origin = await host.connect(connection_handshake(host, 1))
            origin.worker = QueueingPipelineWorker()
            assert host.feature_policy.enable_early_ack is False

            await host._handle_pending(
                _pending_outcome(),
                "continue please",
                origin,
                "turn-pending-noack",
                host._new_app_turn_recorder(
                    origin_epoch=origin.epoch, turn_id="turn-pending-noack"
                ),
            )

            assert _ack_items(origin.scheduler) == []
            assert host._ack_emitted_turns == set()
            assert origin.scheduler._ack_index == {}

        else:
            assert path == "mixed_multi_intent"
            search = BlockingResultWorker()

            class Registry:
                config = Config(max_work_items_per_turn=4, enable_early_ack=False)

                @staticmethod
                def catalogue() -> object:
                    return object()

            class Router:
                @staticmethod
                def route_envelope(text: str, _catalogue: object) -> object:
                    action = {
                        "answer directly": "direct",
                        "ask me": "clarify",
                        "unsupported thing": "unsupported",
                        "search it": "existing_worker",
                    }[text]
                    decision = type("Decision", (), {"action": action})()
                    prose = {
                        "direct": "Direct answer.",
                        "clarify": "Which one?",
                        "unsupported": None,
                        "existing_worker": None,
                    }[action]
                    return type("Envelope", (), {"decision": decision, "prose": prose})()

            class Coordinator(WorkItemCoordinator):
                def __init__(self) -> None:
                    super().__init__(registry=Registry(), router=Router())

                def dispatch(self, decision: object, **_: object) -> object:
                    assert decision.action == "existing_worker"
                    return search

            host = SessionHost(
                runner_factory=LifecycleRunner, tts=FakeTTS(), coordinator=Coordinator()
            )
            origin = await host.connect(connection_handshake(host, 1))
            origin.worker = QueueingPipelineWorker()
            assert host.feature_policy.enable_early_ack is False
            outcome = type(
                "Outcome",
                (),
                {
                    "work_items": (
                        "answer directly",
                        "ask me",
                        "unsupported thing",
                        "search it",
                    ),
                    "pending_dialogue": None,
                },
            )()

            pending = asyncio.create_task(
                host._handle_multi_intent(
                    outcome,
                    "",
                    origin,
                    "turn-mixed-noack",
                    host._new_app_turn_recorder(
                        origin_epoch=origin.epoch, turn_id="turn-mixed-noack"
                    ),
                )
            )
            await asyncio.wait_for(search.started.wait(), timeout=1)

            assert _ack_items(origin.scheduler) == []
            assert host._ack_emitted_turns == set()
            assert origin.scheduler._ack_index == {}

            search.release.set()
            await asyncio.wait_for(pending, timeout=1)

        # Common post-turn assertion for every path: the turn's completion
        # (or, for the direct-delegated path, its release) must not have
        # left any deferred ack admission task or index entry behind either.
        assert _ack_items(origin.scheduler) == []
        assert host._ack_emitted_turns == set()
        assert origin.scheduler._ack_index == {}
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_parent_aggregate_progresses_through_its_children() -> None:
    """I6: every delegated multi-intent child must feed the turn's single
    parent aggregate, keyed off ``work-{turn_id}``."""

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host, _worker = _fan_in_host(
            _submitted(
                results=(
                    _fan_in_result("turn-multi-0", "first answer"),
                    _fan_in_result("turn-multi-1", "second answer"),
                )
            ),
            sink,
        )
        origin = await host.connect(connection_handshake(host, 1))

        await host._handle_multi_intent(
            _multi_intent_outcome("first item", "second item"),
            "",
            origin,
            "turn-multi",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-multi"),
        )

        assert _work_status_states(host) == ["routing", "searching", "result_ready"]
        statuses = host.state.work_status_snapshot()
        assert [status.work_item_id for status in statuses] == ["work-turn-multi"]
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_mixed_terminal_children_aggregate_to_failed_parent() -> None:
    """A retained sibling holds the aggregate at ``background`` only while it
    is the sole non-terminal child; a failed sibling still wins the terminal
    join."""

    async def run() -> None:
        from server.work_item_coordinator import WorkItemFailure

        sink = CollectingMeasurementSink()
        host, _worker = _fan_in_host(
            _submitted(
                results=(_fan_in_result("turn-mix-0", "first answer"),),
                failures=(
                    WorkItemFailure(
                        work_item_id="work-turn-mix-1",
                        worker_id="worker-search",
                        error_type="RuntimeError",
                        error_message="boom",
                        failure_kind="retention_rejected",
                    ),
                ),
            ),
            sink,
        )
        origin = await host.connect(connection_handshake(host, 1))

        await host._handle_multi_intent(
            _multi_intent_outcome("first item", "second item"),
            "",
            origin,
            "turn-mix",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-mix"),
        )

        assert _work_status_states(host) == ["routing", "searching", "failed"]
        statuses = host.state.work_status_snapshot()
        assert statuses[0].terminal_reason == "retention_rejected"
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_declined_child_still_terminates_the_parent_aggregate() -> None:
    """A worker-raised WorkerDeclined still commits and speaks its own
    canonical result (see ``WorkerDeclined`` handling in
    ``_handle_multi_intent``), so the child's work item is done -- the parent
    aggregate must reach ``result_ready`` rather than staying stuck at
    ``searching`` forever."""

    async def run() -> None:
        worker = DecliningResultWorker()

        class Registry:
            config = Config(max_work_items_per_turn=2)

            @staticmethod
            def catalogue() -> object:
                return object()

        class Router:
            @staticmethod
            def route_envelope(_text: str, _catalogue: object) -> object:
                decision = type("Decision", (), {"action": "existing_worker"})()
                return type("Envelope", (), {"decision": decision, "prose": None})()

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=Registry(), router=Router())

            def dispatch(self, _decision: object, **_: object) -> object:
                return worker

        sink = CollectingMeasurementSink()
        coordinator = Coordinator()
        host = SessionHost(
            runner_factory=LifecycleRunner, coordinator=coordinator, measurement_sink=sink
        )
        origin = await host.connect(connection_handshake(host, 1))
        outcome = type(
            "Outcome",
            (),
            {"work_items": ("search it",), "pending_dialogue": None},
        )()

        await host._handle_multi_intent(
            outcome,
            "",
            origin,
            "turn-declined",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-declined"),
        )

        assert _work_status_states(host) == ["routing", "searching", "result_ready"]
        statuses = host.state.work_status_snapshot()
        assert statuses[0].state == "result_ready"
        await host.shutdown()

    asyncio.run(run())


def test_retained_foreground_child_records_background_even_without_capability() -> None:
    """I7: the ledger must record ``background`` for a retained child on a
    non-capable connection too. The wire filter lives in the observer, so
    emitting ``failed`` here only ever mislabels the work -- and permanently,
    since a terminal parent never regresses to ``result_ready``."""

    async def run() -> None:
        worker = BlockingResultWorker()
        worker.metadata = type(
            "Metadata",
            (),
            {"worker_id": "worker-search", "topic": "slow search", "model_policy": "deep"},
        )()

        class RetainingRoutedCoordinator(RoutedCoordinator):
            def __init__(self) -> None:
                super().__init__(worker)
                self.owner = WorkItemCoordinator(
                    config=Config(foreground_search_timeout_seconds=0.001)
                )
                self.config = self.owner.config

            def start_task(self, operation: object) -> asyncio.Task[object] | None:
                return self.owner.start_task(operation)

            def retain_late_task(self, task: asyncio.Task[object], **kwargs: object) -> bool:
                return self.owner.retain_late_task(task, **kwargs)

            async def shutdown(self) -> None:
                await self.owner.shutdown()

        coordinator = RetainingRoutedCoordinator()
        host = SessionHost(
            registry=WorkerRegistry(config=coordinator.config),
            config=coordinator.config,
            runner_factory=LifecycleRunner,
            coordinator=coordinator,
        )
        origin = await host.connect(connection_handshake(host, 1))
        assert not origin.supports_work_status

        foreground = await host._handle_transcript("slow query")
        assert "continue in the background" in foreground.text

        states = _work_status_states(host)
        assert states == ["routing", "searching", "background"]
        assert "failed" not in states
        assert host.state.work_status_snapshot()[0].state == "background"

        worker.release.set()
        await host.shutdown()

    asyncio.run(run())


def test_retained_foreground_child_on_capable_connection_stays_status_only() -> None:
    """Capable clients (``work_status_v1`` + ``enable_background_status``)
    receive the ``background`` status alone: no legacy "taking longer than
    expected" canonical result is ever committed or spoken for the turn."""

    async def run() -> None:
        worker = BlockingResultWorker()
        worker.metadata = type(
            "Metadata",
            (),
            {"worker_id": "worker-search", "topic": "slow search", "model_policy": "deep"},
        )()

        class RetainingRoutedCoordinator(RoutedCoordinator):
            def __init__(self) -> None:
                super().__init__(worker)
                self.owner = WorkItemCoordinator(
                    config=Config(foreground_search_timeout_seconds=0.001)
                )
                self.config = self.owner.config

            def start_task(self, operation: object) -> asyncio.Task[object] | None:
                return self.owner.start_task(operation)

            def retain_late_task(self, task: asyncio.Task[object], **kwargs: object) -> bool:
                return self.owner.retain_late_task(task, **kwargs)

            async def shutdown(self) -> None:
                await self.owner.shutdown()

        coordinator = RetainingRoutedCoordinator()
        host = SessionHost(
            registry=WorkerRegistry(config=coordinator.config),
            config=coordinator.config,
            runner_factory=LifecycleRunner,
            coordinator=coordinator,
        )
        handshake = connection_handshake(host, 1)
        handshake["capabilities"] = ("work_status_v1",)
        handshake["capabilities_present"] = True
        origin = await host.connect(handshake)
        assert origin.supports_work_status
        assert host.feature_policy.enable_background_status

        foreground = await host._handle_transcript("slow query")

        # Nothing was committed for this turn, so the handler returns None --
        # not the internal SearchExecution, and not a GroundedResult (round-1
        # finding I5: sibling branches all return the committed result).
        assert foreground is None

        states = _work_status_states(host)
        assert states == ["routing", "searching", "background"]
        assert "failed" not in states
        assert host.state.work_status_snapshot()[0].state == "background"

        assert len(host.state.result_history("worker-search")) == 0
        assert all(
            "taking longer than expected" not in entry.text for entry in host.state.transcript
        )

        worker.release.set()
        await host.shutdown()

    asyncio.run(run())


def test_cancelled_pending_turn_terminalizes_its_non_terminal_child_status() -> None:
    """I9: a turn cancelled mid-flight must publish a terminal ``cancelled``
    status instead of stranding a capable client on ``searching``."""

    async def run() -> None:
        worker = ResultWorker()

        class Registry:
            config = Config(max_work_items_per_turn=2)

            @staticmethod
            def get(_worker_id: str) -> object:
                return type("Registered", (), {"worker": worker})()

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=Registry(), router=object())
                self.submitting = asyncio.Event()

            async def submit(self, *_args: object, **_kwargs: object) -> object:
                self.submitting.set()
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

        coordinator = Coordinator()
        host = SessionHost(runner_factory=LifecycleRunner, coordinator=coordinator)
        origin = await host.connect(connection_handshake(host, 1))

        turn = asyncio.create_task(
            host._handle_pending(
                _pending_outcome(),
                "continue please",
                origin,
                "turn-cancel",
                host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-cancel"),
            )
        )
        await asyncio.wait_for(coordinator.submitting.wait(), 1)
        assert _work_status_states(host) == ["routing", "searching"]

        turn.cancel()
        with pytest.raises(asyncio.CancelledError):
            await turn

        assert _work_status_states(host) == ["routing", "searching", "cancelled"]
        assert host.state.work_status_snapshot()[0].state == "cancelled"
        await host.shutdown()

    asyncio.run(run())


def test_cancelled_multi_intent_turn_terminalizes_every_non_terminal_child() -> None:
    """I9, fan-out shape: both delegated children of a cancelled compound turn
    settle, so the parent aggregate reaches ``cancelled`` rather than holding
    at ``searching`` forever."""

    async def run() -> None:
        worker = ResultWorker()

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=_FanInRegistry(), router=_FanInRouter())
                self.submitting = asyncio.Event()

            def dispatch(self, _decision: object, **_: object) -> object:
                return worker

            async def submit(self, *_args: object, **_kwargs: object) -> object:
                self.submitting.set()
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

        coordinator = Coordinator()
        host = SessionHost(runner_factory=LifecycleRunner, coordinator=coordinator)
        origin = await host.connect(connection_handshake(host, 1))

        turn = asyncio.create_task(
            host._handle_multi_intent(
                _multi_intent_outcome("first item", "second item"),
                "",
                origin,
                "turn-multi-cancel",
                host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-multi-cancel"),
            )
        )
        await asyncio.wait_for(coordinator.submitting.wait(), 1)
        turn.cancel()
        with pytest.raises(asyncio.CancelledError):
            await turn

        assert _work_status_states(host)[-1] == "cancelled"
        statuses = host.state.work_status_snapshot()
        assert [status.work_item_id for status in statuses] == ["work-turn-multi-cancel"]
        await host.shutdown()

    asyncio.run(run())


def test_cancel_status_sweep_is_idempotent_for_already_terminal_children() -> None:
    """The I9 sweep re-emits ``cancelled`` for children that may already have
    settled. Terminal children reject the transition and an already-terminal
    parent never regresses, so a repeated sweep publishes nothing new."""

    async def run() -> None:
        host = SessionHost(runner_factory=LifecycleRunner)
        await host.connect(connection_handshake(host, 1))
        host._emit_work_status(
            turn_id="turn-idem",
            work_item_id="work-turn-idem",
            state="searching",
            origin_epoch=1,
        )
        host._emit_work_status(
            turn_id="turn-idem",
            work_item_id="work-turn-idem",
            state="result_ready",
            origin_epoch=1,
        )
        settled = _work_status_states(host)

        for _ in range(3):
            host._cancel_child_work_statuses(
                turn_id="turn-idem",
                origin_epoch=1,
                children={"work-turn-idem": None},
            )

        assert _work_status_states(host) == settled
        assert host.state.work_status_snapshot()[0].state == "result_ready"
        await host.shutdown()

    asyncio.run(run())


def test_cancel_of_retained_child_after_turn_task_returns_emits_terminal_status() -> None:
    """Review-gauntlet round-1 follow-up: a work item retained as background
    work (its owning multi-intent turn task has already returned) must still
    reach a terminal work-status when cancelled through the host's
    ``cancel_turn_or_child`` API -- the real entrypoint a "cancel work-item-X"
    voice command uses. This path is distinct from ``_handle_multi_intent``'s
    own ``except asyncio.CancelledError`` handler, which only covers
    cancellation of the turn's *own* still-running task."""

    async def run() -> None:
        class SlowWorker(ResultWorker):
            def __init__(self) -> None:
                self.started = asyncio.Event()

            async def search(
                self, query: str, *, turn_id: str, origin_epoch: int | None
            ) -> GroundedResult:
                self.started.set()
                await asyncio.Event().wait()  # only ends via cancellation
                return await super().search(query, turn_id=turn_id, origin_epoch=origin_epoch)

        worker = SlowWorker()

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(
                    registry=_FanInRegistry(),
                    router=_FanInRouter(),
                    config=Config(multi_intent_wait_timeout_ms=1, max_work_items_per_turn=4),
                )

            def dispatch(self, _decision: object, **_: object) -> object:
                return worker

        coordinator = Coordinator()
        host = SessionHost(runner_factory=LifecycleRunner, coordinator=coordinator)
        origin = await host.connect(connection_handshake(host, 1))

        turn_id = "turn-retained-cancel"
        parent_work_item_id = f"work-{turn_id}"
        child_work_item_id = f"work-{turn_id}-0"
        turn = asyncio.create_task(
            host._handle_multi_intent(
                _multi_intent_outcome("slow item"),
                "",
                origin,
                turn_id,
                host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id=turn_id),
            )
        )
        await asyncio.wait_for(worker.started.wait(), 1)
        await turn  # the owning multi-intent turn task fully returns; the sole
        # child stays retained as background work

        assert child_work_item_id in host._retained_recorders
        statuses = {s.work_item_id: s.state for s in host.state.work_status_snapshot()}
        assert statuses[parent_work_item_id] == "background"

        cancelled_work, _ = await host.cancel_turn_or_child(turn_id, child_work_item_id)
        assert child_work_item_id in cancelled_work

        for _ in range(50):
            statuses = {s.work_item_id: s.state for s in host.state.work_status_snapshot()}
            if statuses.get(parent_work_item_id) == "cancelled":
                break
            await asyncio.sleep(0)
        else:
            pytest.fail(
                "no terminal work-status emitted for a child cancelled after its owning "
                f"turn task returned; last observed state={statuses.get(parent_work_item_id)!r}"
            )

        await host.shutdown()

    asyncio.run(run())


def test_foreground_search_timeout_comes_from_the_host_config_not_the_coordinator() -> None:
    """``foreground_search_timeout_seconds`` used to be read off
    ``coordinator.config`` first, so a coordinator without a config of its own
    silently fell back to the hard-coded 15.0s default instead of the host's
    canonical Config."""

    async def run() -> None:
        recorded: list[float] = []

        class RecordingHost(SessionHost):
            async def _search_with_timeout(self, *args: object, **kwargs: object) -> object:
                recorded.append(kwargs["timeout"])  # type: ignore[arg-type]
                return await super()._search_with_timeout(*args, **kwargs)  # type: ignore[arg-type]

        config = Config(foreground_search_timeout_seconds=0.75)
        host = RecordingHost(
            registry=WorkerRegistry(config=config),
            config=config,
            runner_factory=LifecycleRunner,
            coordinator=RoutedCoordinator(ResultWorker()),
        )
        await host.connect(connection_handshake(host, 1))

        await host._handle_transcript("what is the weather?")

        assert recorded == [0.75]
        await host.shutdown()

    asyncio.run(run())


# --- Review-gauntlet round 1: early-ack / work-status correctness ----------
#
# One regression test per applied fix, each of which fails against the
# pre-fix implementation.


class _CountingSearchWorker:
    """A worker whose ``search`` counts *coroutine creations*, not awaits.

    ``search`` is deliberately a plain (non-``async``) callable returning a
    fresh coroutine so a dispatch that is refused and closed still shows up in
    the count -- the double-dispatch this guards against never awaits either
    coroutine.
    """

    metadata = type(
        "Metadata",
        (),
        {"worker_id": "worker-search", "topic": "counting", "model_policy": "deep"},
    )()

    def __init__(self) -> None:
        self.creations = 0

    def search(self, query: str, **_kwargs: object) -> object:
        self.creations += 1

        async def _run() -> GroundedResult:
            return GroundedResult(
                result_id="result-counting",
                worker_id="worker-search",
                turn_id="turn-1",
                text=f"Answer for {query}",
                spoken_text=f"Answer for {query}",
                origin_epoch=1,
            )

        return _run()


def test_capacity_rejected_dispatch_emits_no_ack_and_never_dispatches_twice() -> None:
    """I1/I2/I3: when the coordinator refuses a search for lack of capacity,
    ``_dispatch_search_task`` returns ``None``. The turn must then speak only
    the "search service is busy" reply -- an early ack would claim delegated
    progress that was never delegated -- and must not build a second search
    coroutine only for the coordinator to close it again."""

    async def run() -> None:
        worker = _CountingSearchWorker()

        class RefusingCoordinator(RoutedCoordinator):
            @staticmethod
            def start_task(operation: object) -> None:
                close = getattr(operation, "close", None)
                if close is not None:
                    close()

        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=FakeTTS(),
            coordinator=RefusingCoordinator(worker),
        )
        origin = await host.connect(connection_handshake(host, 1))
        origin.worker = QueueingPipelineWorker()

        result = await asyncio.wait_for(host._handle_transcript("today's weather"), timeout=1)

        assert "search service is busy" in result.text
        assert _ack_items(origin.scheduler) == []
        assert host._ack_emitted_turns == set()
        assert origin.scheduler._ack_index == {}
        # Exactly one dispatch attempt: the refused one.
        assert worker.creations == 1
        await host.shutdown()

    asyncio.run(run())


def test_emit_early_ack_rejects_a_search_task_on_an_undispatched_path() -> None:
    """I2: ``dispatched`` is a required, per-call-site declaration. The
    pending/multi-intent paths dispatch inside ``coordinator.submit`` and have
    no handle, so supplying one is a programming error rather than a silently
    different ack semantic."""

    async def run() -> None:
        host = SessionHost(runner_factory=LifecycleRunner, tts=FakeTTS())
        origin = await host.connect(connection_handshake(host, 1))
        task: asyncio.Task[None] = asyncio.create_task(asyncio.Event().wait())
        with pytest.raises(ValueError):
            await host._emit_early_ack(
                origin,
                turn_id="turn-x",
                origin_epoch=origin.epoch,
                dispatched=False,
                search_task=task,
            )
        task.cancel()
        await host.shutdown()

    asyncio.run(run())


def test_suppressed_duplicate_late_result_emits_no_work_status() -> None:
    """I4: a suppressed duplicate/stale late result means the work already
    succeeded once. Reporting the suppression as ``failed`` would tell a
    capable client that successful work failed -- and a terminal parent never
    regresses, so nothing could correct it afterwards."""

    from server.work_item_coordinator import LateResult

    async def run() -> None:
        host = SessionHost(runner_factory=LifecycleRunner)
        await host.connect(connection_handshake(host, 1))
        result = _fan_in_result("turn-dup", "answer")
        host._commit_result_state(result)
        before = _work_status_states(host)

        context = host._new_late_delivery_context(
            turn_id="turn-dup", work_item_id="work-turn-dup", origin_epoch=1
        )
        await host.commit_late_result_once(
            context,
            LateResult(
                work_item_id="work-turn-dup",
                worker_id="worker-search",
                result=result,
                terminal_kind="completed",
            ),
        )

        assert _work_status_states(host) == before
        assert "failed" not in _work_status_states(host)
        await host.shutdown()

    asyncio.run(run())


def test_suppressed_stale_late_result_emits_terminal_work_status() -> None:
    """I4, stale-epoch half of the suppression rule.

    Unlike a suppressed duplicate (some other copy already committed and
    drove the work item terminal), a suppressed-stale result means no copy
    committed under this ledger key at all -- emitting nothing would strand
    the child, and the parent aggregate with it, at its non-terminal
    dispatch-time status forever.
    """

    from server.work_item_coordinator import LateResult

    async def run() -> None:
        host = SessionHost(runner_factory=LifecycleRunner)
        await host.connect(connection_handshake(host, 1))
        stale = GroundedResult(
            result_id="result-stale",
            worker_id="worker-search",
            turn_id="turn-stale",
            text="stale answer",
            spoken_text="stale answer",
            origin_epoch=99,
        )
        context = host._new_late_delivery_context(
            turn_id="turn-stale", work_item_id="work-turn-stale", origin_epoch=1
        )
        await host.commit_late_result_once(
            context,
            LateResult(
                work_item_id="work-turn-stale",
                worker_id="worker-search",
                result=stale,
                terminal_kind="completed",
            ),
        )

        assert _work_status_states(host) == ["failed"]
        await host.shutdown()

    asyncio.run(run())


def _capable_handshake(host: SessionHost, epoch: int) -> dict[str, object]:
    handshake = connection_handshake(host, epoch)
    handshake["capabilities"] = ("work_status_v1",)
    handshake["capabilities_present"] = True
    return handshake


def test_pending_retained_child_on_capable_connection_stays_status_only() -> None:
    """I6: a capable client's retained continuation gets the ``background``
    status *instead of* the legacy timeout result -- not both. Emitting both
    duplicated the canonical result, transcript, and history the status-only
    contract is meant to replace."""

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = _pending_host(_submitted(pending_work_item_ids=("work-turn-pending",)), sink)
        origin = await host.connect(_capable_handshake(host, 1))
        assert origin.supports_work_status

        returned = await host._handle_pending(
            _pending_outcome(),
            "continue please",
            origin,
            "turn-pending",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-pending"),
        )

        assert returned is None
        assert _work_status_states(host) == ["routing", "searching", "background"]
        assert all(
            "taking longer than expected" not in entry.text for entry in host.state.transcript
        )
        assert host.state.results.results == ()
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_retained_child_on_capable_connection_stays_status_only() -> None:
    """I6, multi-intent half: the retained item contributes only its
    ``background`` status; its siblings still commit and speak normally."""

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host, _worker = _fan_in_host(
            _submitted(
                results=(_fan_in_result("turn-fan-0", "first answer"),),
                pending_work_item_ids=("work-turn-fan-1",),
            ),
            sink,
        )
        origin = await host.connect(_capable_handshake(host, 1))
        assert origin.supports_work_status

        committed = await host._handle_multi_intent(
            _multi_intent_outcome("first", "second"),
            "first and second",
            origin,
            "turn-fan",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-fan"),
        )

        assert [result.text for result in committed] == ["first answer"]
        assert "background" in _work_status_states(host)
        assert all(
            "taking longer than expected" not in entry.text for entry in host.state.transcript
        )
        await host.shutdown()

    asyncio.run(run())


def test_result_ready_is_not_emitted_when_the_canonical_commit_raises() -> None:
    """I7: terminal ``result_ready`` claims the result is committed and
    display-ready. Emitting it before ``_commit_and_speak`` let a client see
    that claim for a turn whose commit then raised and committed nothing."""

    async def run() -> None:
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=FakeTTS(),
            coordinator=RoutedCoordinator(ProjectedResultWorker()),
        )
        origin = await host.connect(connection_handshake(host, 1))
        origin.worker = QueueingPipelineWorker()

        async def failing_commit(*_args: object, **_kwargs: object) -> object:
            raise RuntimeError("commit failed")

        host._commit_and_speak = failing_commit  # type: ignore[method-assign]

        with pytest.raises(RuntimeError):
            await asyncio.wait_for(host._handle_transcript("today's weather"), timeout=1)

        states = _work_status_states(host)
        assert "result_ready" not in states
        assert states[-1] == "failed"
        await host.shutdown()

    asyncio.run(run())


def test_submit_failure_terminalizes_every_registered_child_as_failed() -> None:
    """I8: an exception raised after ``routing``/``searching`` was published
    left every delegated child -- and therefore the parent aggregate --
    permanently non-terminal for a capable client."""

    async def run() -> None:
        worker = ResultWorker()

        class Registry:
            config = Config(max_work_items_per_turn=2)

            @staticmethod
            def get(_worker_id: str) -> object:
                return type("Registered", (), {"worker": worker})()

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=Registry(), router=object())

            async def submit(self, *_args: object, **_kwargs: object) -> object:
                raise RuntimeError("submit exploded")

        host = SessionHost(runner_factory=LifecycleRunner, coordinator=Coordinator())
        origin = await host.connect(_capable_handshake(host, 1))

        with pytest.raises(RuntimeError):
            await host._handle_pending(
                _pending_outcome(),
                "continue please",
                origin,
                "turn-pending",
                host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-pending"),
            )

        assert _work_status_states(host) == ["routing", "searching", "failed"]
        assert host.state.work_status_snapshot()[0].state == "failed"
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_child_never_returned_by_fan_in_is_reconciled_to_failed() -> None:
    """I9: a dispatched child that appears in no fan-in bucket previously only
    logged, leaving its status on ``searching`` and the parent non-terminal
    forever."""

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host, _worker = _fan_in_host(
            _submitted(results=(_fan_in_result("turn-fan-0", "first answer"),)), sink
        )
        origin = await host.connect(_capable_handshake(host, 1))

        await host._handle_multi_intent(
            _multi_intent_outcome("first", "second"),
            "first and second",
            origin,
            "turn-fan",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-fan"),
        )

        # Only the parent aggregate is client-visible. With one child completed
        # and the unattributed child reconciled to a terminal ``failed``, the
        # exhaustive join settles the parent as ``failed``; before the fix that
        # child stayed ``searching`` and pinned the parent there forever.
        parent = host.state.work_status_snapshot()[0]
        assert parent.work_item_id == "work-turn-fan"
        assert parent.state == "failed"
        assert _work_status_states(host)[-1] == "failed"
        await host.shutdown()

    asyncio.run(run())


def test_cancel_during_the_early_ack_yield_window_reaches_the_dispatched_search() -> None:
    """``_search_with_timeout`` registers the search task, but it is not entered
    until after ``_emit_early_ack`` awaits a scheduling tick. A cancel arriving
    inside that yield window unwinds the (tracked) turn task; unless the search
    task is tracked at dispatch it stays untracked and keeps running with its
    result discarded."""

    async def run() -> None:
        class SlowWorker(ResultWorker):
            async def search(
                self, query: str, *, turn_id: str, origin_epoch: int | None
            ) -> GroundedResult:
                await asyncio.sleep(5)
                return await super().search(query, turn_id=turn_id, origin_epoch=origin_epoch)

        worker = SlowWorker()
        worker.metadata = type(
            "Metadata",
            (),
            {"worker_id": "worker-search", "topic": "slow", "model_policy": "deep"},
        )()
        host = SessionHost(
            runner_factory=LifecycleRunner,
            tts=FakeTTS(),
            coordinator=RoutedCoordinator(worker),
        )
        connection = await host.connect(connection_handshake(host, 1))
        connection.worker = QueueingPipelineWorker()

        dispatched: list[asyncio.Task[object]] = []
        real_dispatch = host._dispatch_search_task

        def recording_dispatch(*args: object, **kwargs: object) -> object:
            task = real_dispatch(*args, **kwargs)  # type: ignore[arg-type]
            if task is not None:
                dispatched.append(task)
            return task

        host._dispatch_search_task = recording_dispatch  # type: ignore[method-assign]

        tracked_at_ack: list[bool] = []
        real_emit_early_ack = host._emit_early_ack

        async def cancelling_emit_early_ack(*args: object, **kwargs: object) -> None:
            tracked_at_ack.append(
                any(dispatched[0] in tasks for tasks in host._inflight_work_tasks.values())
            )
            # A cancel landing exactly inside the ack's scheduling-tick yield.
            host._cancel_work(None)
            await real_emit_early_ack(*args, **kwargs)  # type: ignore[arg-type]

        host._emit_early_ack = cancelling_emit_early_ack  # type: ignore[method-assign]

        turn = asyncio.create_task(host._handle_transcript("slow search"))
        with pytest.raises(asyncio.CancelledError):
            await turn

        assert dispatched, "the search was never dispatched"
        assert tracked_at_ack == [True], "the search task was untracked across the yield window"
        await asyncio.gather(*dispatched, return_exceptions=True)
        assert dispatched[0].cancelled()
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_cancel_racing_the_commit_downgrades_the_pre_derived_status() -> None:
    """``deferred_status`` is derived for every matched item *before* the commit
    loop. A cancel landing between that derivation and an item's
    ``_commit_and_speak`` await makes the commit silently no-op (it
    short-circuits and returns the result unchanged rather than raising), so the
    pre-derived ``result_ready`` must be downgraded to the outcome that actually
    happened instead of being published as-is."""

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host, _worker = _fan_in_host(
            _submitted(results=(_fan_in_result("turn-race-0", "first answer"),)), sink
        )
        origin = await host.connect(_capable_handshake(host, 1))

        real_commit_and_speak = host._commit_and_speak

        async def cancel_racing_commit_and_speak(
            result: GroundedResult, *args: object, **kwargs: object
        ) -> object:
            # Stand in for a concurrent cancel arriving after derivation but
            # before this item's commit reads the cancel set.
            host._cancelled_work_items.add(f"work-{result.turn_id}")
            return await real_commit_and_speak(result, *args, **kwargs)  # type: ignore[arg-type]

        host._commit_and_speak = cancel_racing_commit_and_speak  # type: ignore[method-assign]

        await host._handle_multi_intent(
            _multi_intent_outcome("only item"),
            "only item",
            origin,
            "turn-race",
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-race"),
        )

        # The turn's sole child was suppressed, so the parent aggregate is that
        # child's state. Before the fix this published ``result_ready`` for a
        # commit that never happened.
        parent = host.state.work_status_snapshot()[0]
        assert parent.work_item_id == "work-turn-race"
        assert parent.state == "cancelled"
        assert "result_ready" not in _work_status_states(host)
        await host.shutdown()

    asyncio.run(run())


def test_sole_child_cancel_ignores_another_turns_speech_when_settling_the_ack() -> None:
    """I10: the sole-delegated-child decision must consider only the target
    turn's own work. Another turn's queued speech previously kept this turn's
    ack latched and queued, so it could still be admitted and spoken after the
    user cancelled the work it acknowledged."""

    from server.speech_scheduler import ROLE_ACK

    async def run() -> None:
        host = SessionHost(runner_factory=LifecycleRunner, tts=FakeTTS())
        origin = await host.connect(connection_handshake(host, 1))
        origin.worker = QueueingPipelineWorker()

        turn_id = "turn-a"
        ack_work_item_id = f"ack-{turn_id}"
        host._ack_emitted_turns.add(turn_id)
        host._register_turn_work_item(turn_id, "work-turn-a")
        origin.scheduler.enqueue(
            result_id=None,
            work_item_id=ack_work_item_id,
            run_id=f"run-{ack_work_item_id}",
            text="One moment.",
            origin_epoch=1,
            role=ROLE_ACK,
            ack_id=ack_work_item_id,
            turn_id=turn_id,
        )
        origin.scheduler.enqueue(
            result_id="result-turn-a",
            work_item_id="work-turn-a",
            run_id="run-turn-a",
            text="turn a answer",
            origin_epoch=1,
        )
        # An entirely unrelated turn on the same connection with live speech.
        origin.scheduler.enqueue(
            result_id="result-turn-b",
            work_item_id="work-turn-b",
            run_id="run-turn-b",
            text="turn b answer",
            origin_epoch=1,
        )

        await host.cancel_turn_or_child(turn_id, "work-turn-a", origin=origin)

        assert ack_work_item_id not in origin.scheduler._queues
        assert turn_id not in host._ack_emitted_turns
        # The unrelated turn's own speech is untouched.
        assert "work-turn-b" in origin.scheduler._queues
        await host.shutdown()

    asyncio.run(run())


def test_ack_admission_retry_is_discarded_when_the_turn_is_no_longer_live() -> None:
    """I11: re-queueing a failed ack admission without re-checking turn/epoch
    liveness could leave a stale ack queued after a cancellation or reconnect,
    to be spoken later."""

    async def run() -> None:
        host = SessionHost(runner_factory=LifecycleRunner, tts=FakeTTS())
        origin = await host.connect(connection_handshake(host, 1))
        requeued: list[int] = []

        async def failing_start_next(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("worker not attached")

        origin.scheduler.start_next = failing_start_next  # type: ignore[method-assign]

        # The turn's latch was already cleared (cancellation/reconnect), so the
        # ack is no longer live and must not be re-queued.
        host._schedule_ack_admission(
            origin,
            "ack-turn-dead",
            lambda: requeued.append(1),
            turn_id="turn-dead",
            origin_epoch=origin.epoch,
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert requeued == []

        # A still-latched, still-current turn keeps the existing retry.
        host._ack_emitted_turns.add("turn-live")
        host._schedule_ack_admission(
            origin,
            "ack-turn-live",
            lambda: requeued.append(1),
            turn_id="turn-live",
            origin_epoch=origin.epoch,
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert requeued == [1]
        await host.shutdown()

    asyncio.run(run())


def test_coordinator_declares_the_config_fields_it_owns() -> None:
    """I12: ``SessionHost`` reconciles its Config against the coordinator's by
    excluding exactly the coordinator-owned fields. That set must be declared
    once, by the coordinator, and must match what its constructor actually
    overrides -- otherwise adding a field silently turns every construction
    into a boot-time ValueError raised from the wrong component."""
    from dataclasses import fields as dataclass_fields

    base = Config()
    coordinator = WorkItemCoordinator(config=base, max_work_items=4, wait_timeout_ms=1234)
    overridden = {
        field.name
        for field in dataclass_fields(base)
        if getattr(coordinator.config, field.name) != getattr(base, field.name)
    }
    assert overridden <= WorkItemCoordinator.OWNED_CONFIG_FIELDS
    assert WorkItemCoordinator.OWNED_CONFIG_FIELDS == {
        "max_work_items_per_turn",
        "multi_intent_wait_timeout_ms",
    }
    # A coordinator differing only in its owned fields is not a split brain.
    SessionHost(
        registry=WorkerRegistry(config=base),
        config=base,
        coordinator=coordinator,
        runner_factory=LifecycleRunner,
    )


def test_ack_owner_lookup_is_unambiguous_for_hyphenated_turn_ids() -> None:
    """I13: ack ownership is answered from the turn -> delegated-child
    registry, so a turn id that itself contains a hyphen can never make one
    turn's work item resolve to another turn."""

    async def run() -> None:
        host = SessionHost(runner_factory=LifecycleRunner)
        await host.connect(connection_handshake(host, 1))
        host._ack_emitted_turns.update({"turn-a", "turn-a-b"})
        host._register_turn_work_item("turn-a", "work-turn-a")
        host._register_turn_work_item("turn-a-b", "work-turn-a-b")

        assert host._ack_turn_for_work_item("work-turn-a-b") == "turn-a-b"
        assert host._ack_turn_for_work_item("work-turn-a") == "turn-a"
        assert host._ack_turn_for_work_item("work-unknown") is None
        await host.shutdown()

    asyncio.run(run())


def test_validate_patch_handshake_raises_on_a_wrong_handshake_argument() -> None:
    """I14: the parameters are concrete types read directly, so a wrong
    argument or a renamed field raises instead of silently degrading to "no
    capabilities presented" and accepting the request."""
    host = SessionHost(runner_factory=LifecycleRunner)
    with pytest.raises(AttributeError):
        host.validate_patch_handshake(None, object())  # type: ignore[arg-type]


# --- Batch 1: ack lifecycle ownership across retained work -----------------
#
# An ack belongs to the semantic turn that delegated the work. Four coupled
# gaps let that ownership be dropped or misattributed while the work was still
# running, so an ack could be spoken for work the user had cancelled or that
# had already been rejected.


def test_ack_work_item_id_accessor_matches_every_call_site() -> None:
    """The ack's scheduler key is built in exactly one place. A call site that
    rebuilt the ``ack-{turn_id}`` literal itself would keep working until the
    format changed, then silently fail to find the ack it meant to settle."""
    import inspect

    from server.speech_scheduler import ROLE_ACK

    assert inspect.getsource(SessionHost).count('f"ack-{turn_id}"') == 1

    async def run() -> None:
        from unittest.mock import patch

        host = SessionHost(runner_factory=LifecycleRunner, tts=FakeTTS())
        origin = await host.connect(connection_handshake(host, 1))
        origin.worker = QueueingPipelineWorker()
        turn_id = "turn-accessor"
        calls: list[str] = []

        def spy(spied_turn_id: str) -> str:
            calls.append(spied_turn_id)
            return f"spy-ack-{spied_turn_id}"

        with patch.object(SessionHost, "_ack_work_item_id", staticmethod(spy)):
            # Emission: the enqueued key is the accessor's, not a literal.
            await host._emit_early_ack(origin, turn_id=turn_id, origin_epoch=1, dispatched=False)
            assert f"spy-ack-{turn_id}" in origin.scheduler._queues
            assert origin.scheduler._queues[f"spy-ack-{turn_id}"][0].role == ROLE_ACK

            # Cancellation: settling the sole delegated child finds that key.
            host._register_turn_work_item(turn_id, "work-turn-accessor")
            await host.cancel_turn_or_child(turn_id, "work-turn-accessor", origin=origin)
            assert f"spy-ack-{turn_id}" not in origin.scheduler._queues
            assert turn_id not in host._ack_emitted_turns

            # A turn handler's post-commit discard reaches the same key.
            await host._emit_early_ack(origin, turn_id=turn_id, origin_epoch=1, dispatched=False)
            assert origin.scheduler.discard_queued_ack(host._ack_work_item_id(turn_id)) is not None

        assert calls.count(turn_id) >= 3
        await host.shutdown()

    asyncio.run(run())


def test_cancel_after_retained_early_return_still_discards_queued_ack() -> None:
    """A capable client's retained continuation returns from its handler while
    the child keeps running. Releasing the turn's ack ownership on that early
    return left a later cancel unable to resolve the child back to its turn,
    so the queued ack survived and could still be spoken for cancelled work."""

    from server.speech_scheduler import ROLE_ACK

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = _pending_host(_submitted(pending_work_item_ids=("work-turn-pending",)), sink)
        origin = await host.connect(_capable_handshake(host, 1))
        turn_id = "turn-pending"
        ack_work_item_id = host._ack_work_item_id(turn_id)
        host._ack_emitted_turns.add(turn_id)
        origin.scheduler.enqueue(
            result_id=None,
            work_item_id=ack_work_item_id,
            run_id=f"run-{ack_work_item_id}",
            text="One moment.",
            origin_epoch=1,
            role=ROLE_ACK,
            ack_id=ack_work_item_id,
            turn_id=turn_id,
        )

        assert (
            await host._handle_pending(
                _pending_outcome(),
                "continue please",
                origin,
                turn_id,
                host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id=turn_id),
            )
            is None
        )

        # The child is still running, so its ack ownership outlives the handler.
        assert host._turn_work_items.get(turn_id) == {"work-turn-pending"}
        assert host._ack_turn_for_work_item("work-turn-pending") == turn_id

        await host.cancel_turn_or_child(
            host._ack_turn_for_work_item("work-turn-pending"),
            "work-turn-pending",
            origin=origin,
        )

        assert ack_work_item_id not in origin.scheduler._queues
        assert ack_work_item_id not in origin.scheduler._ack_index
        assert turn_id not in host._ack_emitted_turns
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_rejection_retracts_an_already_admitted_ack() -> None:
    """A turn whose every dispatched item is rejected speaks nothing, so an ack
    already admitted to the transport would be the turn's only utterance -- a
    promise of a result that is never coming. ``discard_queued_ack`` cannot
    reach an admitted item, so the retraction has to use ``cancel``."""

    from server.speech_scheduler import ROLE_ACK

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host, _worker = _fan_in_host(_submitted(), sink)
        host.tts = FakeTTS()
        origin = await host.connect(connection_handshake(host, 1))
        origin.worker = QueueingPipelineWorker()
        turn_id = "turn-fan"
        ack_work_item_id = host._ack_work_item_id(turn_id)
        host._ack_emitted_turns.add(turn_id)
        origin.scheduler.enqueue(
            result_id=None,
            work_item_id=ack_work_item_id,
            run_id=f"run-{ack_work_item_id}",
            text="One moment.",
            origin_epoch=1,
            role=ROLE_ACK,
            ack_id=ack_work_item_id,
            turn_id=turn_id,
        )
        await origin.scheduler.start_next(ack_work_item_id)
        active = origin.scheduler.active
        assert active is not None and active.item.role == ROLE_ACK

        committed = await host._handle_multi_intent(
            _multi_intent_outcome("first", "second"),
            "first and second",
            origin,
            turn_id,
            host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id=turn_id),
        )

        assert committed == ()
        assert origin.scheduler.active is None
        assert ack_work_item_id not in origin.scheduler._queues
        assert ack_work_item_id not in origin.scheduler._ack_index
        await host.shutdown()

    asyncio.run(run())


def test_control_cancel_target_with_no_known_ack_owner_does_not_misroute() -> None:
    """A cancel whose target has no latched ack owner must still cancel that
    target's work and speech, and must not settle some other turn's ack. The
    control turn's own id is not a safe fallback owner."""

    from server.speech_scheduler import ROLE_ACK

    async def run() -> None:
        class ControlCoordinator:
            def __init__(self) -> None:
                self.target: str | None = None

            def arbitrate(self, _session_id: str, _transcript: str) -> object:
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "control",
                        "decision": None,
                        "work_items": (self.target,) if self.target else (),
                        "control_action": "cancel",
                    },
                )()

        coordinator = ControlCoordinator()
        host = SessionHost(runner_factory=LifecycleRunner, tts=FakeTTS(), coordinator=coordinator)
        origin = await host.connect(connection_handshake(host, 1))
        origin.worker = QueueingPipelineWorker()

        # An unrelated turn holding a live, latched ack. Nothing about
        # cancelling an unowned target may touch it.
        other_turn = "turn-other"
        other_ack = host._ack_work_item_id(other_turn)
        host._ack_emitted_turns.add(other_turn)
        host._register_turn_work_item(other_turn, "work-turn-other")
        origin.scheduler.enqueue(
            result_id=None,
            work_item_id=other_ack,
            run_id=f"run-{other_ack}",
            text="One moment.",
            origin_epoch=1,
            role=ROLE_ACK,
            ack_id=other_ack,
            turn_id=other_turn,
        )
        # The cancel target: real queued speech, but no latched ack owner.
        origin.scheduler.enqueue(
            result_id="result-orphan",
            work_item_id="work-orphan",
            run_id="run-orphan",
            text="orphan answer",
            origin_epoch=1,
        )
        assert host._ack_turn_for_work_item("work-orphan") is None

        coordinator.target = "work-orphan"
        await host._handle_transcript("cancel work-orphan")

        # The target's own cancellation still ran...
        assert "work-orphan" not in origin.scheduler._queues
        # ...and the unrelated turn's ack and latch are untouched.
        assert other_ack in origin.scheduler._queues
        assert other_turn in host._ack_emitted_turns
        assert host._turn_work_items.get(other_turn) == {"work-turn-other"}
        await host.shutdown()

    asyncio.run(run())


def test_multi_intent_sibling_commit_exception_does_not_fail_a_retained_child() -> None:
    """A still-running retained child must not be swept to ``failed`` by the
    generic exception handler when an unrelated sibling's commit raises in
    the same turn.

    Before the fix, the outer ``except Exception:`` in ``_handle_multi_intent``
    passed the *whole* ``delegated_children`` mapping -- including a child
    still legitimately running in the background -- to
    ``_fail_child_work_statuses``. ``background -> failed`` is itself a legal
    transition, so that child was permanently terminalized and its later,
    genuinely successful late result could never flip it to ``result_ready``.
    """

    async def run() -> None:
        sink = CollectingMeasurementSink()
        host, _worker = _fan_in_host(
            _submitted(
                results=(_fan_in_result("turn-fan-0", "first answer"),),
                pending_work_item_ids=("work-turn-fan-1",),
            ),
            sink,
        )
        origin = await host.connect(_capable_handshake(host, 1))
        assert origin.supports_work_status
        assert host.feature_policy.enable_background_status

        async def boom(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("speak failed")

        host._commit_and_speak = boom  # item 0's commit/speak raises

        with pytest.raises(RuntimeError):
            await host._handle_multi_intent(
                _multi_intent_outcome("first item", "second item"),
                "",
                origin,
                "turn-fan",
                host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-fan"),
            )

        def child_states() -> dict[str, str]:
            return {
                work_item_id: status.state
                for kids in host.state._work_status_children.values()
                for work_item_id, status in kids.items()
            }

        states = child_states()
        assert states["work-turn-fan-0"] == "failed"
        # The retained sibling must stay non-terminal, not be swept to
        # "failed" by the outer exception handler.
        assert states["work-turn-fan-1"] == "background"
        assert "work-turn-fan-1" in host._retained_recorders

        # The late result for the retained child now lands and succeeds; it
        # must still be able to terminalize the child to "result_ready".
        late = type(
            "Late",
            (),
            {
                "work_item_id": "work-turn-fan-1",
                "worker_id": "worker-search",
                "error": None,
                "terminal_kind": None,
                "result": _fan_in_result("turn-fan-1", "late answer"),
            },
        )()
        late_context = host._new_late_delivery_context(
            turn_id="turn-fan",
            work_item_id="work-turn-fan-1",
            origin_epoch=1,
            parent_work_item_id="work-turn-fan",
        )
        await host.commit_late_result_once(late_context, late)

        assert child_states()["work-turn-fan-1"] == "result_ready"
        await host.shutdown()

    asyncio.run(run())


def test_late_result_display_only_still_discards_the_stale_queued_notice() -> None:
    """A still-queued 'taking longer' notice for a retained work item must be
    discarded once its real result commits, even when the late-result
    disposition verdict is ``display_only`` rather than ``autoplay``.

    Before the fix, ``discard_queued_notice`` was only called on the
    autoplay branch of ``commit_late_result_once``; a display-only verdict
    left the stale notice queued, to be spoken later once some other item
    admitted and drained it.
    """

    async def run() -> None:
        # tts=FakeTTS() keeps this connection speakable; no promotion_manifest
        # means `_promotion_eligible` defaults false, so
        # `_late_result_disposition` returns "display_only" below.
        host = SessionHost(runner_factory=LifecycleRunner, tts=FakeTTS())
        origin = await host.connect(connection_handshake(host, 1))
        scheduler = origin.scheduler

        scheduler.enqueue(
            result_id="result-notice",
            work_item_id="work-b",
            run_id="run-notice",
            text="That is taking longer than expected; I will continue in the background.",
            origin_epoch=1,
            role=ROLE_TIMEOUT_NOTICE,
        )

        late_context = LateDeliveryContext(
            turn_id="turn-b",
            work_item_id="work-b",
            origin_epoch=1,
            ack_timestamp=None,
            accepted_turn_sequence=host._turn_sequence,
        )
        assert host._late_result_disposition(late_context, origin=origin) == "display_only"

        final = GroundedResult(
            result_id="result-final",
            worker_id="worker-search",
            turn_id="turn-b",
            text="final answer",
            spoken_text="final answer",
            origin_epoch=1,
        )
        await host.commit_late_result_once(
            late_context,
            LateResult(work_item_id="work-b", worker_id="worker-search", result=final),
        )

        remaining_roles = [item.role for item in scheduler._queues.get("work-b", ())]
        assert ROLE_TIMEOUT_NOTICE not in remaining_roles
        await host.shutdown()

    asyncio.run(run())


def test_commit_late_result_once_discards_a_still_queued_ack_on_every_terminal_path() -> None:
    """A capable retained turn's early ack must not survive its own work
    item's terminalization.

    Before the fix, ``commit_late_result_once``'s ``finally`` only released
    turn ownership; it never discarded a still-queued ack. If some unrelated
    generation later freed the transport lane, the stale ack could then be
    spoken after the real result had already committed.
    """

    async def run() -> None:
        from server.speech_scheduler import ROLE_ACK

        host = SessionHost(runner_factory=LifecycleRunner)
        origin = await host.connect(connection_handshake(host, 1))
        scheduler = origin.scheduler

        ack_work_item_id = host._ack_work_item_id("turn-b")
        scheduler.enqueue(
            result_id=None,
            work_item_id=ack_work_item_id,
            run_id=f"run-{ack_work_item_id}",
            text="One moment.",
            origin_epoch=1,
            role=ROLE_ACK,
            ack_id=ack_work_item_id,
            turn_id="turn-b",
        )
        host._ack_emitted_turns.add("turn-b")
        host._register_turn_work_item("turn-b", "work-b")

        final = GroundedResult(
            result_id="result-final",
            worker_id="worker-search",
            turn_id="turn-b",
            text="final answer",
            spoken_text="final answer",
            origin_epoch=1,
        )
        await host.commit_late_result_once(
            LateDeliveryContext(
                turn_id="turn-b",
                work_item_id="work-b",
                origin_epoch=1,
                ack_timestamp=None,
                accepted_turn_sequence=host._turn_sequence,
            ),
            LateResult(work_item_id="work-b", worker_id="worker-search", result=final),
        )

        assert ack_work_item_id not in scheduler._ack_index
        assert ack_work_item_id not in scheduler._queues
        await host.shutdown()

    asyncio.run(run())


def test_pending_submit_failure_discards_the_still_queued_early_ack() -> None:
    """Fix 6 regression: if ``coordinator.submit()`` raises after
    ``_emit_early_ack`` already enqueued this turn's ack, the exception path
    must discard that ack rather than leaving it queued. Before the fix,
    this no-result exception path cleared turn ownership but never touched
    the scheduler's queued ack item."""

    async def run() -> None:
        worker = ResultWorker()

        class Registry:
            config = Config(max_work_items_per_turn=2)

            @staticmethod
            def get(_worker_id: str) -> object:
                return type("Registered", (), {"worker": worker})()

        ack_seen_queued = False

        class Coordinator(WorkItemCoordinator):
            def __init__(self) -> None:
                super().__init__(registry=Registry(), router=object())

            async def submit(self, *_args: object, **_kwargs: object) -> object:
                nonlocal ack_seen_queued
                ack_seen_queued = ack_work_item_id in origin.scheduler._ack_index
                raise RuntimeError("submit exploded")

        tts = FakeTTS()
        host = SessionHost(runner_factory=LifecycleRunner, coordinator=Coordinator(), tts=tts)
        origin = await host.connect(_capable_handshake(host, 1))
        ack_work_item_id = host._ack_work_item_id("turn-pending")

        with pytest.raises(RuntimeError):
            await host._handle_pending(
                _pending_outcome(),
                "continue please",
                origin,
                "turn-pending",
                host._new_app_turn_recorder(origin_epoch=origin.epoch, turn_id="turn-pending"),
            )

        # The ack must have actually been queued before the exception, or
        # this test would not exercise the gap at all.
        assert ack_seen_queued is True
        assert ack_work_item_id not in origin.scheduler._ack_index
        await host.shutdown()

    asyncio.run(run())
