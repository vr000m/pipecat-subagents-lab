"""Connection pipelines expose authoritative state through a fakeable observer."""

import asyncio

import pytest
from pipecat.bus.bridge_processor import BusBridgeProcessor as FrameworkBusBridgeProcessor

import server.app as app_module
from server.contracts import GroundedResult, WorkerState
from server.pipeline import CanonicalResultAdapter, SessionHost, build_pipeline


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
