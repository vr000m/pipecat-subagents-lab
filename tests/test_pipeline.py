"""Connection pipelines expose authoritative state through a fakeable observer."""

import asyncio

import pytest
from pipecat.audio.turn.smart_turn.local_smart_turn_v3 import LocalSmartTurnAnalyzerV3
from pipecat.bus.bridge_processor import BusBridgeProcessor as FrameworkBusBridgeProcessor
from pipecat.frames.frames import (
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
from server.config import Config
from server.contracts import GroundedResult, RoutingDecision, WorkerState
from server.pipeline import CanonicalResultAdapter, SessionHost, build_pipeline, framework_bridge
from server.registry import UnsupportedWorkerType
from server.turns import FinalTurnTranscriptProcessor, smart_turn_processor
from server.workers.web_search import WorkerDeclined


class RoutedCoordinator:
    def __init__(self, worker: object) -> None:
        self.worker = worker

    def arbitrate(self, _session_id: str, transcript: str) -> object:
        return type(
            "Outcome",
            (),
            {"kind": "routed", "decision": object(), "transcript": transcript},
        )()

    def dispatch(self, _decision: object) -> object:
        return self.worker


class ResultWorker:
    async def search(self, query: str, *, turn_id: str, origin_epoch: int | None) -> GroundedResult:
        return GroundedResult(
            result_id=f"result-{turn_id}",
            worker_id="worker-search",
            turn_id=turn_id,
            text=f"Answer for {query}",
            spoken_text=f"Spoken answer for {query}",
            ui_text=f"Answer for {query}",
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

    async def search(self, query: str, *, turn_id: str, origin_epoch: int | None) -> GroundedResult:
        self.queries.append(query)
        return await super().search(query, turn_id=turn_id, origin_epoch=origin_epoch)


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


class FakeTTS:
    def __init__(self) -> None:
        self.on_event = None


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

        await tts.on_event("synthesis_ended", "pipecat-generated-context")
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
        await host.shutdown()

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
                ui_text="Answer",
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
                ui_text="Old answer",
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
        "ui_text": "A complete answer.",
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
                "session_id": "session-1",
                "sequence": 4,
                "kind": "runtime_snapshot",
                "data": {"workers": [], "results": [], "speech_progress": []},
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
