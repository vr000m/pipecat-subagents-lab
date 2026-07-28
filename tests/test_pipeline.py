"""Connection pipelines expose authoritative state through a fakeable observer."""

import asyncio
import threading
import time

import pytest
from pipecat.audio.turn.smart_turn.local_smart_turn_v3 import LocalSmartTurnAnalyzerV3
from pipecat.bus.bridge_processor import BusBridgeProcessor as FrameworkBusBridgeProcessor
from pipecat.frames.frames import (
    InterruptionFrame,
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
from server.config import Config
from server.contracts import GroundedResult, RoutingDecision, WorkerState
from server.perf_metrics import CollectingMeasurementSink
from server.pipeline import CanonicalResultAdapter, SessionHost, build_pipeline, framework_bridge
from server.registry import UnsupportedWorkerType
from server.services.tts import CorrelatedTTSSpeakFrame
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
        assert reason in {"connection replaced", "session shutdown"}


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
        assert len(worker.frames) == 1
        assert isinstance(worker.frames[0], TTSSpeakFrame)
        assert worker.frames[0].text == result.spoken_text
        assert worker.frames[0].text != result.text
        assert worker.frames[0].append_to_context is False
        assert connection.scheduler.active is not None
        utterance_id = connection.scheduler.active.item.utterance_id
        connection.scheduler.enqueue(
            result_id="result-next",
            work_item_id="work-next",
            run_id="run-next",
            text="Next answer",
            origin_epoch=1,
        )

        assert worker.frames[0].correlation_id == utterance_id
        await tts.on_event("synthesis_started", utterance_id)
        await tts.on_event("synthesis_ended", utterance_id)
        assert host.state.speech[utterance_id].state.value == "delivery_unknown"
        assert len(worker.frames) == 2
        assert connection.scheduler.active is not None
        assert connection.scheduler.active.item.result_id == "result-next"
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
            "bot_transcript",
            "result",
            "worker",
        ]
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
        assert tts.on_event is not None
        await tts.on_event("synthesis_started", foreground_utterance_id)
        await tts.on_event("synthesis_ended", foreground_utterance_id)

        spoken_frames = [
            frame for frame in connection.worker.frames if isinstance(frame, TTSSpeakFrame)
        ]
        assert len(spoken_frames) == 2
        assert spoken_frames[-1].text == history[-1].spoken_text
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
        await host._commit_late_result(
            LateResult(
                work_item_id="work-old-epoch",
                worker_id="worker-search",
                result=result,
            ),
            1,
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
        host = SessionHost(runner_factory=LifecycleRunner, tts=tts)
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

        await host._commit_late_result(late, 1)
        await host._commit_late_result(late, 1)

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

        await host._handle_transcript("cancel")

        assert host.state.speech[item.utterance_id].state.value == "interrupted"
        assert connection.scheduler.active is not None
        assert connection.scheduler.active.item.result_id != item.result_id
        assert isinstance(connection.worker.frames[-2], InterruptionFrame)
        assert isinstance(connection.worker.frames[-1], TTSSpeakFrame)
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

        await host._handle_transcript("pause")

        assert isinstance(connection.worker.frames[-2], InterruptionFrame)
        assert isinstance(connection.worker.frames[-1], TTSSpeakFrame)
        assert connection.worker.frames[-1].text == "Pausing the active response."
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
        connection.scheduler.pause("work-old")

        replacement = connection.scheduler.enqueue(
            result_id="result-new",
            work_item_id="work-new",
            run_id="run-new",
            text="New answer",
            origin_epoch=1,
        )
        await connection.scheduler.start_next()
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
        connection.scheduler.pause("work-old")
        replacement = connection.scheduler.enqueue(
            result_id="result-new",
            work_item_id="work-new",
            run_id="run-new",
            text="New answer",
            origin_epoch=1,
        )
        await connection.scheduler.start_next()

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
        connection.scheduler.pause("work-active")

        await host._handle_transcript("resume work-active")

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

        results = await host._handle_multi_intent(outcome, "", origin, "turn-compound")

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

        await host._handle_multi_intent(outcome, "", origin, "turn-compound")

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

        await host._commit_late_result(
            LateResult(
                work_item_id="work-1",
                worker_id="worker-search",
                error="ProviderError: secret-token\nforged log line",
            ),
            1,
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
        tts_frame = TTSSpeakFrame(text="hello", append_to_context=False)

        await bridge.process_frame(tts_frame, FrameDirection.DOWNSTREAM)

        assert forwarded == [tts_frame]
        assert bus.messages == []

    asyncio.run(run())


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
        assert fields["direct_count"] == 1
        assert all(
            fields[name] == 0
            for name in (
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
        assert fields["unsupported_count"] == 1
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
        assert fields["clarification_count"] == 1
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
        host = SessionHost(runner_factory=LifecycleRunner, measurement_sink=sink)
        await host.connect(connection_handshake(host, 1))

        await host._handle_transcript("cancel work-item-does-not-exist")

        fields = _events(sink, "app_turn_foreground")[0].fields
        assert fields["outcome"] == "control"
        assert fields["control_action"] == "cancel"
        assert fields["control_outcome"] == "unknown_target"
        assert fields["child_count"] == 0
        await host.shutdown()

    asyncio.run(run())


def test_consent_without_pending_dialogue_emits_control_no_pending_outcome() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(runner_factory=LifecycleRunner, measurement_sink=sink)
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
            runner_factory=LifecycleRunner, coordinator=coordinator, measurement_sink=sink
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

        results = await host._handle_multi_intent(outcome, "", origin, "turn-compound")

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

        await host._handle_multi_intent(outcome, "", origin, "turn-all-complete")

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
            await host._handle_pending(outcome, "continue please", origin, "turn-pending-fail")

        parent = _events(sink, "app_turn_foreground")[0].fields
        assert parent["outcome"] == "failed"
        assert parent["child_count"] == 0
        assert _events(sink, "work_item_foreground") == ()
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
            await host._handle_multi_intent(outcome, "", origin, "turn-multi-commit-fail")

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


def test_retained_completion_with_speech_emits_completed_committed_queued() -> None:
    async def run() -> None:
        tts = FakeTTS()
        sink = CollectingMeasurementSink()
        host = SessionHost(runner_factory=LifecycleRunner, tts=tts, measurement_sink=sink)
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

        await host._commit_late_result(late, 1)

        records = _background_records(sink)
        assert len(records) == 1
        fields = records[0].fields
        assert fields["work_outcome"] == "completed"
        assert fields["commit_outcome"] == "committed"
        assert fields["speech_outcome"] == "queued"
        assert fields["work_item_id"] == "work-retained-ok"
        await host.shutdown()

    asyncio.run(run())


def test_retained_worker_exception_emits_failed_not_applicable_axes() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(measurement_sink=sink)
        _register_dispatch_recorder(host, "work-failed")

        await host._commit_late_result(
            LateResult(
                work_item_id="work-failed",
                worker_id="worker-search",
                error="RuntimeError: provider exploded",
                terminal_kind="failed",
            ),
            1,
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

        await host._commit_late_result(
            LateResult(
                work_item_id="work-cancelled",
                worker_id="worker-search",
                error="CancelledError: worker task was cancelled",
                terminal_kind="cancelled",
            ),
            1,
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

        await host._commit_late_result(
            LateResult(
                work_item_id="work-user-cancelled", worker_id="worker-search", result=result
            ),
            1,
        )

        assert host.state.result_history("worker-search") == ()
        fields = _background_records(sink)[0].fields
        assert fields["work_outcome"] == "cancelled"
        assert fields["commit_outcome"] == "suppressed_cancelled"

    asyncio.run(run())


def test_retained_invalid_result_type_emits_invalid_result_axes() -> None:
    async def run() -> None:
        sink = CollectingMeasurementSink()
        host = SessionHost(measurement_sink=sink)
        _register_dispatch_recorder(host, "work-invalid")

        await host._commit_late_result(
            LateResult(
                work_item_id="work-invalid",
                worker_id="worker-search",
                result="not a GroundedResult",
            ),
            1,
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

        await host._commit_late_result(
            LateResult(work_item_id="work-dup", worker_id="worker-search", result=duplicate),
            1,
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

        await host._commit_late_result(
            LateResult(
                work_item_id="work-replaced-epoch", worker_id="worker-search", result=result
            ),
            1,
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

        await host._commit_late_result(
            LateResult(work_item_id="work-stale-origin", worker_id="worker-search", result=result),
            2,
        )

        assert host.state.result_history("worker-search") == ()
        fields = _background_records(sink)[0].fields
        assert fields["work_outcome"] == "completed"
        assert fields["commit_outcome"] == "suppressed_stale"
        assert fields["speech_outcome"] == "not_applicable"

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

        await host._commit_late_result(
            LateResult(work_item_id="work-disconnected", worker_id="worker-search", result=result),
            1,
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

        await host._commit_late_result(
            LateResult(work_item_id="work-no-tts", worker_id="worker-search", result=result),
            1,
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
            runner_factory=LifecycleRunner, coordinator=coordinator, measurement_sink=sink
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
            runner_factory=LifecycleRunner, coordinator=coordinator, measurement_sink=sink
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
        await host._commit_late_result(
            LateResult(
                work_item_id=fields["work_item_id"],
                worker_id="worker-search",
                result=committed,
                terminal_kind="completed",
            ),
            1,
        )
        after_duplicate = _background_records(sink)
        assert len(after_duplicate) == 1
        assert all(record.fields["background_ms"] >= 70 for record in after_duplicate)

        await host.shutdown()

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
