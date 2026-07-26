"""Connection pipelines expose authoritative state through a fakeable observer."""

import asyncio
import threading

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
            "Original request: What's the weather like?\n"
            "Clarification asked: Which location should I use?\n"
            "User answer: Riga"
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
                assert getattr(decision, "action") == "existing_worker"
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


class FakePipelineWorker:
    def __init__(self, pipeline: object, **_: object) -> None:
        self.pipeline = pipeline
        self.rtvi = FakeRTVI()

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

    asyncio.run(run())
