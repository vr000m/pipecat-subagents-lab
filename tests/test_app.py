"""HTTP entry-point tests for the local Small WebRTC server."""

import asyncio
import dataclasses
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import pytest
from fastapi.testclient import TestClient
from pipecat.processors.frameworks.rtvi import RTVIObserverParams

import server.app as app_module
from server.app import create_app
from server.config import Config
from server.contracts import GroundedResult
from server.pipeline import SessionHost
from server.registry import WorkerRegistry
from server.router import LazyRouterProvider, Router
from server.services.stt import LocalSTT
from server.services.tts import LocalTTS
from server.work_item_coordinator import WorkItemCoordinator
from server.workers.base import WorkerMetadata


class FakeRunner:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


class FailingTTS:
    def __init__(self) -> None:
        self.cleaned = False

    async def connect(self) -> None:
        raise RuntimeError("TTS unavailable")

    async def cleanup(self) -> None:
        self.cleaned = True


class FakeRouterModel:
    tools: tuple[()] = ()

    def structured_output(self, *, transcript: str, catalogue: tuple[object, ...]) -> dict:
        assert transcript == "Search today's news"
        assert catalogue == ()
        return {
            "action": "new_worker",
            "worker_id": None,
            "worker_type": "web_search",
            "topic": "news",
            "capability": "public_web",
            "capability_available": True,
            "model_policy": "deep",
            "catalogue_version": "catalogue-0",
            "catalogue_worker_ids": (),
        }


class FakeSearchWorker:
    capabilities: ClassVar[dict[str, bool]] = {"public_web": True}

    def __init__(self, worker_id: str) -> None:
        self.worker_id = worker_id
        self.metadata = WorkerMetadata(
            worker_id=worker_id,
            worker_type="web_search",
            topic="news",
            topic_summary="",
            model_policy="deep",
        )
        self.calls: list[tuple[str, str, int | None]] = []

    async def search(self, query: str, *, turn_id: str, origin_epoch: int | None) -> GroundedResult:
        self.calls.append((query, turn_id, origin_epoch))
        return GroundedResult(
            result_id="result-news",
            worker_id=self.worker_id,
            turn_id=turn_id,
            text="News result",
            spoken_text="News result",
            origin_epoch=origin_epoch,
        )


def test_default_host_has_lazy_router_and_real_coordinator(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda: Config())

    host = app_module._default_session_host()

    assert isinstance(host.coordinator, WorkItemCoordinator)
    assert host.coordinator.registry is host.registry
    assert isinstance(host.coordinator.router, Router)
    assert isinstance(host.coordinator.router._call, LazyRouterProvider)
    assert repr(host.coordinator.router._call) == "LazyRouterProvider(initialized=False)"


def test_default_host_dispatches_through_an_injected_router(monkeypatch) -> None:
    async def run() -> None:
        monkeypatch.setattr(app_module, "load_config", lambda: Config())
        host = app_module._default_session_host(router=Router(model=FakeRouterModel()))
        worker = FakeSearchWorker("worker-1")
        host.registry.worker_factory = lambda _worker_id: worker
        host.runner_factory = FakeRunner
        await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 1,
                "snapshot_sequence": 0,
            }
        )

        result = await host._handle_transcript("Search today's news")

        assert result.result_id == "result-news"
        assert worker.calls == [("Search today's news", "turn-1", 1)]
        assert host.state.result_history("worker-1") == (result,)
        assert host.state.speech == {}
        await host.shutdown()

    asyncio.run(run())


def test_default_app_host_materializes_configured_local_speech_adapters(
    monkeypatch,
) -> None:
    monkeypatch.setenv("WEBSEARCH_STT_ENDPOINT", "uds:///tmp/local-stt.sock")
    monkeypatch.setenv("WEBSEARCH_TTS_ENDPOINT", "ws://127.0.0.1:9000")

    host = create_app().state.session_host

    assert isinstance(host.stt, LocalSTT)
    assert host.stt.client_factory is not None
    assert host.stt.endpoint.transport == "uds"
    assert host.stt.endpoint.address == "/tmp/local-stt.sock"
    assert isinstance(host.tts, LocalTTS)
    assert host.tts.client_factory is not None
    assert host.tts.endpoint.transport == "ws"
    assert host.tts.endpoint.address == "127.0.0.1:9000"
    assert host.tts.voice_id == "azelma"


def test_tts_completion_uses_only_one_provider_signal() -> None:
    local = SimpleNamespace(on_event=None)
    hosted = SimpleNamespace()
    host = SessionHost(runner_factory=FakeRunner)

    assert app_module._tts_processors(host, SimpleNamespace(tts=local)) == (local,)
    hosted_processors = app_module._tts_processors(host, SimpleNamespace(tts=hosted))
    assert hosted_processors[0] is hosted
    assert type(hosted_processors[1]).__name__ == "_SpeechCompletionProcessor"


def test_connection_setup_failure_cleans_and_fences_promoted_runtime() -> None:
    async def run() -> None:
        tts = FailingTTS()
        host = SessionHost(runner_factory=FakeRunner, tts=tts)

        with pytest.raises(RuntimeError, match="TTS unavailable"):
            await app_module._attach_connection(
                host,
                object(),
                {
                    "session_id": host.state.session_id,
                    "resume_token": host.state.resume_token,
                    "proposed_epoch": 1,
                    "snapshot_sequence": 0,
                },
            )

        assert host.connection is None
        assert host.state.active_epoch is None
        assert tts.cleaned is True
        await host.shutdown()

    asyncio.run(run())


def test_main_uses_validated_bind_configuration(monkeypatch) -> None:
    import uvicorn

    calls: list[dict[str, object]] = []
    configured: list[bool] = []
    configure_logging = app_module._configure_logging

    def configure_logging_for_main() -> None:
        configure_logging()
        configured.append(True)

    monkeypatch.setattr(
        app_module, "load_config", lambda: Config(bind_host="127.0.0.2", bind_port=9000)
    )
    monkeypatch.setattr(
        app_module,
        "_configure_logging",
        configure_logging_for_main,
    )
    monkeypatch.setattr(
        uvicorn, "run", lambda target, **kwargs: calls.append({"target": target, **kwargs})
    )

    app_module.main()

    assert configured == [True]
    assert calls == [{"target": "server.app:app", "host": "127.0.0.2", "port": 9000}]


def test_injected_session_host_is_preserved() -> None:
    host = SessionHost(runner_factory=FakeRunner)

    assert create_app(host).state.session_host is host


def test_default_session_host_derives_one_feature_policy_from_the_loaded_config(
    monkeypatch,
) -> None:
    """Plan: 'derive one frozen FeaturePolicy' from the immutable Config
    constructed by ``_default_session_host()``, injected into SessionHost."""
    from server.config import FeaturePolicy

    monkeypatch.setattr(app_module, "load_config", lambda: Config(enable_early_ack=False))

    host = app_module._default_session_host()

    assert isinstance(host.feature_policy, FeaturePolicy)
    assert host.feature_policy.enable_early_ack is False
    assert host.feature_policy is FeaturePolicy.from_config(host.config)


def test_create_app_does_not_resolve_a_second_policy_from_worker_registry_config(
    monkeypatch,
) -> None:
    """Plan: 'create_app() must not resolve a second policy from
    WorkerRegistry.config.' A custom injected host is authoritative; its
    feature_policy must be unchanged by create_app()."""
    from server.config import FeaturePolicy

    host = SessionHost(
        runner_factory=FakeRunner,
        registry=WorkerRegistry(config=Config(enable_autoplay_policy=False)),
        config=Config(enable_autoplay_policy=False),
        feature_policy=FeaturePolicy.from_config(Config(enable_autoplay_policy=False)),
    )
    original_policy = host.feature_policy

    app = create_app(host)

    assert app.state.session_host.feature_policy is original_policy


def test_app_exposes_health_and_next_session_handshake() -> None:
    host = SessionHost(runner_factory=FakeRunner)

    with TestClient(create_app(host)) as client:
        assert client.get("/api/healthz").json() == {
            "status": "ok",
            "transport": "smallwebrtc",
        }
        response = client.get("/api/session", headers={"origin": "http://127.0.0.1:7860"})
        handshake = response.json()

    assert handshake["contract_version"] == "v1.0"
    assert handshake["session_id"] == host.state.session_id
    assert handshake["resume_token"] != host.state.resume_token
    assert handshake["proposed_epoch"] == 1
    assert handshake["snapshot_sequence"] == 0
    assert response.headers["cache-control"] == "no-store"


def test_readiness_reports_dependency_failures_without_changing_liveness() -> None:
    class UnreadyProbe:
        @staticmethod
        def discover(service: str) -> tuple[str, str]:
            return ("uds", f"/tmp/{service}.sock")

        @staticmethod
        def healthcheck(_service: str, _transport: str, _address: str) -> bool:
            return False

    host = SessionHost(runner_factory=FakeRunner)
    with TestClient(create_app(host, preflight_probe=UnreadyProbe())) as client:
        assert client.get("/api/healthz").status_code == 200
        response = client.get("/api/readyz")

    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"
    assert set(response.json()["failures"]) == {"stt", "tts"}


def test_offer_rejects_missing_or_wrong_session_identity_before_sdp_handling() -> None:
    host = SessionHost(runner_factory=FakeRunner)

    with TestClient(create_app(host)) as client:
        response = client.post(
            "/api/rtc?session_id=wrong&resume_token=wrong&proposed_epoch=1",
            json={"sdp": "v=0", "type": "offer"},
            headers={"origin": "http://127.0.0.1:7860"},
        )

    assert response.status_code == 401
    assert response.json()["detail"] == "invalid Small WebRTC session identity"


def test_session_discovery_rejects_cross_origin_requests() -> None:
    host = SessionHost(runner_factory=FakeRunner)
    with TestClient(create_app(host)) as client:
        response = client.get("/api/session", headers={"origin": "https://evil.example"})
    assert response.status_code == 403


def test_session_discovery_rejects_requests_without_browser_same_origin_headers() -> None:
    host = SessionHost(runner_factory=FakeRunner)
    with TestClient(create_app(host)) as client:
        response = client.get("/api/session")
    assert response.status_code == 403


def test_session_discovery_accepts_browser_same_origin_get_without_origin() -> None:
    host = SessionHost(runner_factory=FakeRunner)
    with TestClient(create_app(host)) as client:
        response = client.get(
            "/api/session",
            headers={
                "host": "127.0.0.1:7860",
                "sec-fetch-site": "same-origin",
            },
        )
    assert response.status_code == 200


def test_session_discovery_accepts_localhost_alias_for_loopback_config() -> None:
    host = SessionHost(runner_factory=FakeRunner)
    with TestClient(create_app(host)) as client:
        response = client.get(
            "/api/session",
            headers={"origin": "http://localhost:7860"},
        )
    assert response.status_code == 200


def test_session_discovery_accepts_localhost_same_origin_without_origin() -> None:
    host = SessionHost(runner_factory=FakeRunner)
    with TestClient(create_app(host)) as client:
        response = client.get(
            "/api/session",
            headers={
                "host": "localhost:7860",
                "sec-fetch-site": "same-origin",
            },
        )
    assert response.status_code == 200


def test_session_discovery_uses_configured_client_origin() -> None:
    registry = WorkerRegistry(config=Config(known_client_url="https://client.example.test/app"))
    host = SessionHost(registry=registry, runner_factory=FakeRunner)
    with TestClient(create_app(host)) as client:
        response = client.get("/api/session", headers={"origin": "https://client.example.test"})
    assert response.status_code == 200


# ---------------------------------------------------------------------------
# Phase 1: Pipecat observer construction/wiring in _attach_connection.
#
# docs/dev_plans/20260727-feature-latency-observability.md requires: one
# StartupTimingObserver and one UserBotLatencyObserver per connection,
# enable_metrics=True, and the worker's *default* turn_tracking_observer used
# once rather than a duplicate TurnTrackingObserver.
# ---------------------------------------------------------------------------


class RecordingFrameworkObserver:
    """Stands in for StartupTimingObserver/UserBotLatencyObserver."""

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        self.handlers: dict[str, object] = {}

    def event_handler(self, name: str):
        def register(function: object) -> object:
            self.handlers[name] = function
            return function

        return register


class RecordingTurnTrackingObserver:
    def __init__(self) -> None:
        self.handlers: dict[str, object] = {}

    def event_handler(self, name: str):
        def register(function: object) -> object:
            self.handlers[name] = function
            return function

        return register


class CapturingPipelineWorker:
    """Fake PipelineWorker that records the kwargs app.py constructs it with."""

    constructed: ClassVar[list["CapturingPipelineWorker"]] = []
    turn_tracker_factory: ClassVar[Callable[[], object | None]] = RecordingTurnTrackingObserver

    def __init__(self, pipeline: object, **kwargs: object) -> None:
        self.pipeline = pipeline
        self.kwargs = kwargs
        self.observers = kwargs.get("observers")
        self.params = kwargs.get("params")
        self.turn_tracking_observer = type(self).turn_tracker_factory()
        self.rtvi = FakeRTVIForApp()
        type(self).constructed.append(self)

    async def queue_frame(self, _frame: object) -> None:
        pass


class FakeRTVIForApp:
    def __init__(self) -> None:
        self.handlers: dict[str, object] = {}

    def event_handler(self, name: str):
        def register(function: object) -> object:
            self.handlers[name] = function
            return function

        return register

    async def set_bot_ready(self) -> None:
        pass


class AsyncAddRunnerForApp:
    def __init__(self) -> None:
        self.added: list[object] = []

    async def start(self) -> None:
        pass

    async def add_workers(self, *workers: object) -> None:
        self.added.extend(workers)

    async def stop(self) -> None:
        pass


class FakeTransportForApp:
    def input(self) -> str:
        return "input"

    def output(self) -> str:
        return "output"


class FakeSmallWebRTCConnection:
    async def disconnect(self) -> None:
        pass


async def _attach_fake_connection(
    monkeypatch: pytest.MonkeyPatch,
    *,
    host: SessionHost,
    startup_observers: list[RecordingFrameworkObserver],
    latency_observers: list[RecordingFrameworkObserver],
    worker_class: type = CapturingPipelineWorker,
) -> None:
    monkeypatch.setattr(app_module, "SmallWebRTCTransport", lambda *_args: FakeTransportForApp())
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
    monkeypatch.setattr(app_module, "Pipeline", lambda processors: processors)
    monkeypatch.setattr(app_module, "PipelineWorker", worker_class)

    def make_startup_observer(*args: object, **kwargs: object) -> RecordingFrameworkObserver:
        observer = RecordingFrameworkObserver(*args, **kwargs)
        startup_observers.append(observer)
        return observer

    def make_latency_observer(*args: object, **kwargs: object) -> RecordingFrameworkObserver:
        observer = RecordingFrameworkObserver(*args, **kwargs)
        latency_observers.append(observer)
        return observer

    monkeypatch.setattr(app_module, "StartupTimingObserver", make_startup_observer)
    monkeypatch.setattr(app_module, "UserBotLatencyObserver", make_latency_observer)

    await host.start()
    await app_module._attach_connection(
        host,
        FakeSmallWebRTCConnection(),
        app_module.SnapshotHandshake(
            session_id=host.state.session_id,
            resume_token=host.state.resume_token,
            proposed_epoch=1,
            snapshot_sequence=0,
        ),
    )


def test_attach_connection_constructs_one_startup_and_one_latency_observer_per_connection() -> None:
    async def run() -> None:
        CapturingPipelineWorker.constructed = []
        host = SessionHost(runner_factory=AsyncAddRunnerForApp)
        startup_observers: list[RecordingFrameworkObserver] = []
        latency_observers: list[RecordingFrameworkObserver] = []
        monkeypatch = pytest.MonkeyPatch()
        try:
            await _attach_fake_connection(
                monkeypatch,
                host=host,
                startup_observers=startup_observers,
                latency_observers=latency_observers,
            )
        finally:
            monkeypatch.undo()

        assert len(startup_observers) == 1
        assert len(latency_observers) == 1
        assert len(CapturingPipelineWorker.constructed) == 1
        worker = CapturingPipelineWorker.constructed[0]
        assert worker.observers == [startup_observers[0], latency_observers[0]]
        assert host.connection is not None
        assert host.connection.output_teardown is not None

    asyncio.run(run())


def test_attach_connection_enables_pipecat_processor_metrics() -> None:
    async def run() -> None:
        CapturingPipelineWorker.constructed = []
        host = SessionHost(runner_factory=AsyncAddRunnerForApp)
        monkeypatch = pytest.MonkeyPatch()
        try:
            await _attach_fake_connection(
                monkeypatch, host=host, startup_observers=[], latency_observers=[]
            )
        finally:
            monkeypatch.undo()

        worker = CapturingPipelineWorker.constructed[0]
        assert worker.params["enable_metrics"] is True

    asyncio.run(run())


def test_attach_connection_suppresses_rtvi_metrics_messages_only() -> None:
    """Console-only release: metrics stay on for the server, off over the wire."""

    async def run() -> None:
        CapturingPipelineWorker.constructed = []
        host = SessionHost(runner_factory=AsyncAddRunnerForApp)
        monkeypatch = pytest.MonkeyPatch()
        try:
            await _attach_fake_connection(
                monkeypatch, host=host, startup_observers=[], latency_observers=[]
            )
        finally:
            monkeypatch.undo()

        worker = CapturingPipelineWorker.constructed[0]
        assert worker.params["enable_metrics"] is True

        captured = worker.kwargs["rtvi_observer_params"]
        assert isinstance(captured, RTVIObserverParams)
        assert captured.metrics_enabled is False

        stock = RTVIObserverParams()
        differing = {
            field.name
            for field in dataclasses.fields(RTVIObserverParams)
            if getattr(captured, field.name) != getattr(stock, field.name)
        }
        assert differing == {"metrics_enabled"}

    asyncio.run(run())


def test_attach_connection_registers_handlers_on_the_workers_default_turn_tracker() -> None:
    """No duplicate TurnTrackingObserver: handlers land on worker.turn_tracking_observer."""

    async def run() -> None:
        CapturingPipelineWorker.constructed = []
        host = SessionHost(runner_factory=AsyncAddRunnerForApp)
        monkeypatch = pytest.MonkeyPatch()
        try:
            await _attach_fake_connection(
                monkeypatch, host=host, startup_observers=[], latency_observers=[]
            )
        finally:
            monkeypatch.undo()

        worker = CapturingPipelineWorker.constructed[0]
        tracker = worker.turn_tracking_observer
        assert isinstance(tracker, RecordingTurnTrackingObserver)
        assert set(tracker.handlers) == {"on_turn_started", "on_turn_ended"}

        # app.py must never import/construct a second TurnTrackingObserver of
        # its own; it only ever reads worker.turn_tracking_observer.
        assert "TurnTrackingObserver" not in dir(app_module) or not callable(
            getattr(app_module, "TurnTrackingObserver", None)
        )

    asyncio.run(run())


def test_attach_connection_registers_handlers_on_the_real_startup_and_latency_observers() -> None:
    async def run() -> None:
        CapturingPipelineWorker.constructed = []
        host = SessionHost(runner_factory=AsyncAddRunnerForApp)
        startup_observers: list[RecordingFrameworkObserver] = []
        latency_observers: list[RecordingFrameworkObserver] = []
        monkeypatch = pytest.MonkeyPatch()
        try:
            await _attach_fake_connection(
                monkeypatch,
                host=host,
                startup_observers=startup_observers,
                latency_observers=latency_observers,
            )
        finally:
            monkeypatch.undo()

        assert set(startup_observers[0].handlers) == {
            "on_startup_timing_report",
            "on_transport_timing_report",
        }
        assert set(latency_observers[0].handlers) == {
            "on_first_bot_speech_latency",
            "on_latency_measured",
            "on_latency_breakdown",
        }

    asyncio.run(run())


@pytest.mark.parametrize(
    "tts",
    [
        pytest.param(SimpleNamespace(on_event=None), id="local-event-capable-path"),
        pytest.param(SimpleNamespace(), id="generic-tts-path"),
    ],
)
def test_attach_connection_installs_one_shared_lifecycle_processor_before_transport_output(
    tts: object,
) -> None:
    """Phase 1 requires one shared TransportSpeechLifecycleProcessor installed
    immediately before transport.output() for both the local event-capable
    and generic TTS integration paths -- it must not be duplicated per path
    and must sit after whichever TTS processor(s) ``_tts_processors`` selects.
    """
    from server.speech_lifecycle import TransportSpeechLifecycleProcessor

    async def run() -> None:
        CapturingPipelineWorker.constructed = []
        host = SessionHost(runner_factory=AsyncAddRunnerForApp, tts=tts)
        monkeypatch = pytest.MonkeyPatch()
        try:
            await _attach_fake_connection(
                monkeypatch, host=host, startup_observers=[], latency_observers=[]
            )
        finally:
            monkeypatch.undo()

        worker = CapturingPipelineWorker.constructed[0]
        processors = list(worker.pipeline)
        assert processors[-1] == "output", "the transport output sentinel must be last"
        lifecycle_processors = [
            processor
            for processor in processors
            if isinstance(processor, TransportSpeechLifecycleProcessor)
        ]
        assert len(lifecycle_processors) == 1
        assert processors[-2] is lifecycle_processors[0]

    asyncio.run(run())


def test_attach_connection_fails_clearly_when_default_turn_tracker_is_missing() -> None:
    """Fail loudly if Pipecat's enabled-by-default turn tracker contract changes."""

    class NoTurnTrackerWorker(CapturingPipelineWorker):
        turn_tracker_factory = staticmethod(lambda: None)

    async def run() -> None:
        CapturingPipelineWorker.constructed = []
        host = SessionHost(runner_factory=AsyncAddRunnerForApp)
        monkeypatch = pytest.MonkeyPatch()
        try:
            with pytest.raises(RuntimeError, match="turn tracking observer"):
                await _attach_fake_connection(
                    monkeypatch,
                    host=host,
                    startup_observers=[],
                    latency_observers=[],
                    worker_class=NoTurnTrackerWorker,
                )
        finally:
            monkeypatch.undo()

    asyncio.run(run())


def test_default_session_host_forwards_measurement_sink_keyword() -> None:
    sink = object()
    host = app_module._default_session_host(
        router=Router(call=lambda *, transcript, catalogue: {}, config=Config()),
        measurement_sink=sink,
    )
    assert host.measurement_sink is sink


def test_session_host_defaults_measurement_sink_to_console_sink() -> None:
    from server.perf_metrics import ConsoleMeasurementSink

    host = SessionHost(runner_factory=AsyncAddRunnerForApp)
    assert isinstance(host.measurement_sink, ConsoleMeasurementSink)


def test_create_app_preserves_the_supplied_hosts_measurement_sink() -> None:
    sink = object()
    host = SessionHost(runner_factory=FakeRunner, measurement_sink=sink)
    create_app(host)
    assert host.measurement_sink is sink


class TestLoguruStartupConfiguration:
    """The executable entrypoint must disable unsafe Loguru traceback rendering."""

    def test_import_preserves_preexisting_handler(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                """
from loguru import logger

handler_id = logger.add(lambda _message: None)
import server.app

assert handler_id in logger._core.handlers
""",
            ],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0, result.stderr

    def test_configured_sink_has_diagnose_and_backtrace_disabled(self) -> None:
        from loguru import logger as loguru_logger

        app_module._configure_logging()
        handlers = list(loguru_logger._core.handlers.values())
        assert handlers, "the executable entrypoint must configure a Loguru sink"
        for handler in handlers:
            assert handler._exception_formatter._diagnose is False
            assert handler._exception_formatter._backtrace is False

    def test_configured_logger_does_not_leak_local_variable_values(self, capfd) -> None:
        """Loguru's diagnose mode only annotates variables that appear in a
        displayed traceback source line (e.g. a call argument), not every
        local in the frame — so the repro must pass the secret as an
        argument, matching how a transcript/result reaches a raising call.

        The default sink is added by the entrypoint, before capsys can patch
        sys.stderr, so it writes through the original file descriptor;
        capfd (OS-level fd capture) is required to observe it."""
        secret = "SUPER-SECRET-API-KEY-DO-NOT-LEAK"

        def _boom(_value: str) -> None:
            raise ValueError("boom")

        try:
            _boom(secret)
        except ValueError:
            app_module._loguru_logger.exception("failed")

        captured = capfd.readouterr()
        assert secret not in captured.err
        assert secret not in captured.out
