"""Connection pipelines expose authoritative state through a fakeable observer."""

import asyncio

import pytest
from pipecat.bus.bridge_processor import BusBridgeProcessor as FrameworkBusBridgeProcessor
from pipecat.frames.frames import TTSSpeakFrame
from pipecat.processors.frameworks.rtvi.frames import RTVIServerMessageFrame
from pipecat.processors.frame_processor import FrameDirection

import server.app as app_module
from server.contracts import GroundedResult, WorkerState
from server.pipeline import CanonicalResultAdapter, SessionHost, build_pipeline


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
            spoken_text=f"Answer for {query}",
            ui_text=f"Answer for {query}",
            origin_epoch=origin_epoch,
        )


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

        async def tracked_start_next() -> object:
            nonlocal start_calls
            start_calls += 1
            return await start_next()

        connection.scheduler.start_next = tracked_start_next  # type: ignore[method-assign]

        result = await host._handle_transcript("Riga weather")

        assert start_calls == 1
        assert len(worker.frames) == 1
        assert isinstance(worker.frames[0], TTSSpeakFrame)
        assert worker.frames[0].text == result.spoken_text
        assert worker.frames[0].append_to_context is False
        assert connection.scheduler.active is not None
        utterance_id = connection.scheduler.active.item.utterance_id

        await tts.on_event("synthesis_ended", "pipecat-generated-context")
        assert connection.scheduler.active is None
        assert host.state.speech[utterance_id].state.value == "delivery_unknown"
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
        assert [message["kind"] for message in messages] == ["worker", "result"]
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
    assert (
        adapter.accepts(
            {
                "kind": "canonical_result",
                "result_id": "r",
                "worker_id": "w",
                "turn_id": "t",
                "text": "ok",
            }
        )
        is True
    )


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

    asyncio.run(run())


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
    def event_handler(self, _name: str):
        return lambda function: function


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
        host = SessionHost(runner_factory=lambda: runner)
        await host.start()
        transport = FakeTransport()
        pipeline_args: list[object] = []

        monkeypatch.setattr(app_module, "SmallWebRTCTransport", lambda *_args: transport)
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
        assert any(isinstance(item, FrameworkBusBridgeProcessor) for item in pipeline_args[0])
        assert any(isinstance(item, CanonicalResultAdapter) for item in pipeline_args[0])

    asyncio.run(run())
