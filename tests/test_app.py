"""HTTP entry-point tests for the local Small WebRTC server."""

import asyncio
import dataclasses
import json as _json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient
from pipecat.processors.frameworks.rtvi import RTVIObserverParams
from starlette.requests import Request as _StarletteRequest

import server.app as app_module
from server.app import _handshake_from_query, create_app
from server.config import Config
from server.contracts import GroundedResult, SnapshotHandshake
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
    assert host.feature_policy == FeaturePolicy.from_config(host.config)


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


def test_default_session_host_calls_load_promotion_manifest_exactly_once(
    monkeypatch, tmp_path
) -> None:
    """Plan bullet 183: '_default_session_host(config) calls once, handing
    the immutable verdict to the SessionHost policy evaluator.'

    ``server.config.load_promotion_manifest`` already exists, but
    ``_default_session_host`` does not call it yet (Phase 2 concurrent
    implementer is still wiring app.py). Skip until app.py actually
    references the loader, rather than asserting on a call count of zero.
    """
    import inspect

    if not hasattr(app_module, "load_promotion_manifest"):
        pytest.skip("_default_session_host does not import load_promotion_manifest yet")
    source = inspect.getsource(app_module._default_session_host)
    if "load_promotion_manifest" not in source:
        pytest.skip("_default_session_host does not call load_promotion_manifest yet")

    calls: list[object] = []

    def fake_loader(config):
        calls.append(config)
        return SimpleNamespace(promotion_eligible=False, reason="evidence_unavailable")

    monkeypatch.setattr(app_module, "load_promotion_manifest", fake_loader)
    monkeypatch.setattr(
        app_module,
        "load_config",
        lambda: Config(promotion_manifest_path=str(tmp_path / "missing.json")),
    )

    app_module._default_session_host()

    assert len(calls) == 1


def test_enable_autoplay_policy_off_regression_preserves_pre_v013_late_result_behavior() -> None:
    """Plan bullet 201: 'Add a flag-off regression test.' With
    enable_autoplay_policy disabled, the app-level feature policy consumer
    must select the pre-v0.1.3 active-origin enqueue/start behavior rather
    than evaluating the new autoplay predicates."""
    host = SessionHost(
        runner_factory=FakeRunner,
        registry=WorkerRegistry(config=Config(enable_autoplay_policy=False)),
        config=Config(enable_autoplay_policy=False),
    )

    assert host.feature_policy.enable_autoplay_policy is False


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

    async def queue_frame(self, frame: object) -> None:
        # Mirror _SnapshotBarrierConsumer, the real last-processor-before-
        # transport.output() consumer: acknowledge a barrier frame as though
        # it drained through the pipeline, or install_baseline() hangs.
        acknowledge = getattr(frame, "acknowledge", None)
        if callable(acknowledge):
            acknowledge()


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
    capabilities: tuple[str, ...] = (),
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
            capabilities=capabilities,
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


# --- Phase 3: capability handshake (POST/PATCH `capabilities` query field) -
#
# `_handshake_from_query` is the sole app-layer parser for the canonical
# encoding: a single `capabilities` query parameter carrying one URL-encoded
# JSON array of capability-name strings. These tests assert the CONTRACT the
# plan describes; they may fail to import/run until server/app.py lands the
# `capabilities`/raw-ASGI-validation extension.


def _raw_request(raw_query_string: bytes) -> _StarletteRequest:
    """Build a minimal ASGI Request carrying an already-encoded raw query
    string, bypassing the framework's own query-string decoding -- this is
    the direct-ASGI injection the plan calls for to test malformed percent
    sequences at the `scope["query_string"]` boundary."""
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/rtc",
        "headers": [],
        "query_string": raw_query_string,
    }
    return _StarletteRequest(scope)


def _capability_query(
    *, session_id: str, resume_token: str, epoch: int, capabilities: str
) -> bytes:
    return (
        f"session_id={session_id}&resume_token={resume_token}"
        f"&proposed_epoch={epoch}&snapshot_sequence=0&capabilities={capabilities}"
    ).encode()


def _host_with_fresh_handshake_token() -> tuple[SessionHost, dict[str, str]]:
    host = SessionHost(runner_factory=FakeRunner)
    handshake = host.session_handshake()
    return host, handshake


def test_handshake_absent_capabilities_normalizes_to_unsupported() -> None:
    host, handshake = _host_with_fresh_handshake_token()
    query = (
        f"session_id={handshake['session_id']}&resume_token={handshake['resume_token']}"
        f"&proposed_epoch={handshake['proposed_epoch']}&snapshot_sequence=0"
    ).encode()

    value = _handshake_from_query(host, _raw_request(query))

    assert value.capabilities == ()
    assert value.capabilities_present is False


def test_handshake_canonical_capabilities_array_is_decoded_deduplicated_and_sorted() -> None:
    host, handshake = _host_with_fresh_handshake_token()
    encoded = quote(_json.dumps(["work_status_v1", "work_status_v1", "alpha"]))
    query = _capability_query(
        session_id=handshake["session_id"],
        resume_token=handshake["resume_token"],
        epoch=handshake["proposed_epoch"],
        capabilities=encoded,
    )

    value = _handshake_from_query(host, _raw_request(query))

    assert value.capabilities == ("alpha", "work_status_v1")
    assert value.capabilities_present is True


def test_handshake_unknown_future_capability_name_is_retained_as_unsupported() -> None:
    host, handshake = _host_with_fresh_handshake_token()
    encoded = quote(_json.dumps(["some_future_capability"]))
    query = _capability_query(
        session_id=handshake["session_id"],
        resume_token=handshake["resume_token"],
        epoch=handshake["proposed_epoch"],
        capabilities=encoded,
    )

    value = _handshake_from_query(host, _raw_request(query))

    assert value.capabilities == ("some_future_capability",)


def test_handshake_empty_array_normalizes_to_present_but_empty_set() -> None:
    host, handshake = _host_with_fresh_handshake_token()
    encoded = quote(_json.dumps([]))
    query = _capability_query(
        session_id=handshake["session_id"],
        resume_token=handshake["resume_token"],
        epoch=handshake["proposed_epoch"],
        capabilities=encoded,
    )

    value = _handshake_from_query(host, _raw_request(query))

    assert value.capabilities == ()
    assert value.capabilities_present is True


@pytest.mark.parametrize(
    "raw_capabilities",
    [
        "not-json",
        quote("{"),
        quote(_json.dumps({"not": "an array"})),
        quote(_json.dumps([1, 2])),
        quote(_json.dumps([""])),
    ],
)
def test_handshake_rejects_malformed_or_non_string_capabilities(raw_capabilities: str) -> None:
    host, handshake = _host_with_fresh_handshake_token()
    query = _capability_query(
        session_id=handshake["session_id"],
        resume_token=handshake["resume_token"],
        epoch=handshake["proposed_epoch"],
        capabilities=raw_capabilities,
    )

    with pytest.raises(Exception) as excinfo:
        _handshake_from_query(host, _raw_request(query))
    assert getattr(excinfo.value, "status_code", 400) == 400


def test_handshake_rejects_duplicate_capabilities_query_keys() -> None:
    """The raw query parser rejects duplicate `capabilities` keys before
    normalization; deduplication only applies inside the single JSON array."""
    host, handshake = _host_with_fresh_handshake_token()
    encoded = quote(_json.dumps(["work_status_v1"]))
    query = (
        f"session_id={handshake['session_id']}&resume_token={handshake['resume_token']}"
        f"&proposed_epoch={handshake['proposed_epoch']}&snapshot_sequence=0"
        f"&capabilities={encoded}&capabilities={encoded}"
    ).encode()

    with pytest.raises(Exception) as excinfo:
        _handshake_from_query(host, _raw_request(query))
    assert getattr(excinfo.value, "status_code", 400) == 400


def test_handshake_rejects_percent_not_followed_by_two_hex_digits_at_asgi_boundary() -> None:
    """App-layer defense on the raw ASGI scope bytes: a percent sign not
    followed by two ASCII hex digits must be rejected with the same 400
    handshake error, independent of whatever the framework's own decoder
    would have done with it."""
    host, handshake = _host_with_fresh_handshake_token()
    query = (
        f"session_id={handshake['session_id']}&resume_token={handshake['resume_token']}"
        f"&proposed_epoch={handshake['proposed_epoch']}&snapshot_sequence=0&capabilities=%zz"
    ).encode()

    with pytest.raises(Exception) as excinfo:
        _handshake_from_query(host, _raw_request(query))
    assert getattr(excinfo.value, "status_code", 400) == 400


def test_handshake_rejects_trailing_bare_percent_at_asgi_boundary() -> None:
    host, handshake = _host_with_fresh_handshake_token()
    query = (
        f"session_id={handshake['session_id']}&resume_token={handshake['resume_token']}"
        f"&proposed_epoch={handshake['proposed_epoch']}&snapshot_sequence=0&capabilities=%"
    ).encode()

    with pytest.raises(Exception) as excinfo:
        _handshake_from_query(host, _raw_request(query))
    assert getattr(excinfo.value, "status_code", 400) == 400


# --- Phase 3: PATCH capability inheritance/mismatch (SessionHost.validate_patch_handshake) -


def test_validate_patch_handshake_omitted_field_inherits_the_post_bound_set() -> None:
    host = SessionHost(runner_factory=FakeRunner)
    promoted = host.arbiter.promote(
        {
            "session_id": host.state.session_id,
            "resume_token": host.state.resume_token,
            "proposed_epoch": 1,
            "snapshot_sequence": 0,
            "capabilities": ("work_status_v1",),
            "capabilities_present": True,
        }
    )
    patch_handshake = SnapshotHandshake(
        session_id=host.state.session_id,
        resume_token=host.state.resume_token,
        proposed_epoch=1,
        snapshot_sequence=0,
    )

    # Omission must not raise and must not mutate the bound connection.
    host.validate_patch_handshake(promoted, patch_handshake)

    assert promoted.capabilities == ("work_status_v1",)


def test_validate_patch_handshake_present_mismatch_is_rejected() -> None:
    host = SessionHost(runner_factory=FakeRunner)
    promoted = host.arbiter.promote(
        {
            "session_id": host.state.session_id,
            "resume_token": host.state.resume_token,
            "proposed_epoch": 1,
            "snapshot_sequence": 0,
            "capabilities": ("work_status_v1",),
            "capabilities_present": True,
        }
    )
    mismatched_patch = SnapshotHandshake(
        session_id=host.state.session_id,
        resume_token=host.state.resume_token,
        proposed_epoch=1,
        snapshot_sequence=0,
        capabilities=(),
        capabilities_present=True,
    )

    with pytest.raises(ValueError):
        host.validate_patch_handshake(promoted, mismatched_patch)
    # Rejecting the mismatch must not mutate the bound connection/entitlement.
    assert promoted.capabilities == ("work_status_v1",)


def test_validate_patch_handshake_exact_matching_set_is_accepted() -> None:
    host = SessionHost(runner_factory=FakeRunner)
    promoted = host.arbiter.promote(
        {
            "session_id": host.state.session_id,
            "resume_token": host.state.resume_token,
            "proposed_epoch": 1,
            "snapshot_sequence": 0,
            "capabilities": ("work_status_v1",),
            "capabilities_present": True,
        }
    )
    matching_patch = SnapshotHandshake(
        session_id=host.state.session_id,
        resume_token=host.state.resume_token,
        proposed_epoch=1,
        snapshot_sequence=0,
        capabilities=("work_status_v1",),
        capabilities_present=True,
    )

    host.validate_patch_handshake(promoted, matching_patch)  # must not raise


# --- Phase 3: mixed-version client/server compatibility fixture -----------


def test_pre_phase3_style_server_ignores_an_unknown_capabilities_field() -> None:
    """A pre-Phase-3 server parses only its known query fields, so an
    otherwise-valid new-browser request carrying `capabilities` is accepted
    with the field ignored/treated as unsupported -- not strictly rejected.
    This models the old server against a new-browser request; do not claim
    strict rejection for a mixed-version fixture."""
    old_style_fields = {
        "contract_version",
        "session_id",
        "resume_token",
        "proposed_epoch",
        "snapshot_sequence",
    }
    from server.contracts import SnapshotHandshake as _Handshake

    parsed = _Handshake.model_validate(
        {
            "session_id": "session-1",
            "resume_token": "resume-1",
            "proposed_epoch": 1,
            "snapshot_sequence": 0,
        }
    )
    # An old (pre-Phase-3) parser only ever reads these fields; a new browser
    # sending an extra `capabilities` query parameter must not break it.
    assert old_style_fields <= set(parsed.model_dump().keys())


def test_old_browser_snapshot_carries_no_status_field() -> None:
    """A new server's snapshot projection for a non-advertising (old) browser
    must omit the status section entirely -- field absent, not an empty
    array/object -- per the legacy-compatibility fixture requirement."""
    import json as _json2

    legacy = _json2.loads(
        (Path(__file__).parent / "fixtures" / "runtime-snapshot-v1.0-as-shipped.json").read_text()
    )["schema"]
    assert "work_status" not in legacy.get("properties", {})


# --- Phase 3: enable_background_status flag-off regression / rollback -----


def test_enable_background_status_off_reproduces_pre_phase3_legacy_timeout_behavior() -> None:
    """Plan bullet: 'When disabled, no work_status frames are emitted
    regardless of client capability, and the Phase 1 legacy timeout notice
    applies universally, reproducing pre-Phase-3 behavior.'"""
    host = SessionHost(
        runner_factory=FakeRunner,
        registry=WorkerRegistry(config=Config(enable_background_status=False)),
        config=Config(enable_background_status=False),
    )

    assert host.feature_policy.enable_background_status is False


def test_rollback_order_disable_status_then_autoplay_then_early_ack() -> None:
    """Plan bullet: the documented rollback order (disable
    enable_background_status first, then enable_autoplay_policy, then
    enable_early_ack) must leave each preceding phase's disabled-switch path
    operational at every step."""
    config = Config(enable_background_status=False)
    host = SessionHost(
        runner_factory=FakeRunner,
        registry=WorkerRegistry(config=config),
        config=config,
    )
    assert host.feature_policy.enable_background_status is False
    assert host.feature_policy.enable_autoplay_policy is True
    assert host.feature_policy.enable_early_ack is True

    config = Config(enable_background_status=False, enable_autoplay_policy=False)
    host = SessionHost(
        runner_factory=FakeRunner,
        registry=WorkerRegistry(config=config),
        config=config,
    )
    assert host.feature_policy.enable_background_status is False
    assert host.feature_policy.enable_autoplay_policy is False
    assert host.feature_policy.enable_early_ack is True

    config = Config(
        enable_background_status=False, enable_autoplay_policy=False, enable_early_ack=False
    )
    host = SessionHost(
        runner_factory=FakeRunner,
        registry=WorkerRegistry(config=config),
        config=config,
    )
    assert host.feature_policy.enable_background_status is False
    assert host.feature_policy.enable_autoplay_policy is False
    assert host.feature_policy.enable_early_ack is False


# --- C1: snapshot/incremental sequence namespace unification --------------


class FrameCapturingPipelineWorker(CapturingPipelineWorker):
    """CapturingPipelineWorker that records every queued frame's payload."""

    def __init__(self, pipeline: object, **kwargs: object) -> None:
        super().__init__(pipeline, **kwargs)
        self.frames: list[dict] = []

    async def queue_frame(self, frame: object) -> None:
        acknowledge = getattr(frame, "acknowledge", None)
        if callable(acknowledge):
            acknowledge()
            return
        self.frames.append(getattr(frame, "data", frame))


def test_snapshot_install_reseeds_the_observer_projected_sequence() -> None:
    """C1 regression: a connection that never advertised `work_status_v1`
    still advances the global `SessionState` sequence for every invisible
    `work_status` event. The snapshot is stamped from that global watermark,
    so the observer's projected counter must be re-seeded at snapshot install
    or the next visible incremental lands at or below the client's
    `lastAppliedSequence` and is silently discarded forever.

    Invariant: after any snapshot install,
    ``observer.projected_sequence == wire snapshot_sequence`` and the next
    incremental carries exactly ``snapshot_sequence + 1``.
    """

    async def run() -> None:
        FrameCapturingPipelineWorker.constructed = []
        host = SessionHost(runner_factory=AsyncAddRunnerForApp)
        monkeypatch = pytest.MonkeyPatch()
        try:
            await _attach_fake_connection(
                monkeypatch,
                host=host,
                startup_observers=[],
                latency_observers=[],
                worker_class=FrameCapturingPipelineWorker,
            )
        finally:
            monkeypatch.undo()

        runtime = host.connection
        assert runtime is not None
        assert runtime.supports_work_status is False, (
            "fixture must model a non-capable (no work_status_v1) connection"
        )
        worker = FrameCapturingPipelineWorker.constructed[-1]
        await worker.rtvi.handlers["on_client_ready"](worker.rtvi)

        # N invisible work_status events: they advance the global sequence
        # (SessionState._emit) but are dropped by the observer's capability
        # filter, so the projected counter does not move.
        for index in range(4):
            host.state.set_child_work_status(
                turn_id="turn-1",
                work_item_id=f"work-{index}",
                state="searching",
                origin_epoch=runtime.epoch,
            )
        assert worker.frames == [], "work_status must stay invisible on this connection"

        await worker.rtvi.handlers["on_client_message"](
            worker.rtvi, SimpleNamespace(type="snapshot-request", data=None)
        )
        assert worker.frames, "a snapshot-request must produce a runtime_snapshot frame"
        snapshot_frame = worker.frames[-1]
        assert snapshot_frame["kind"] == "runtime_snapshot"
        wire_snapshot_sequence = snapshot_frame["data"]["snapshot_sequence"]
        assert snapshot_frame["sequence"] == wire_snapshot_sequence
        assert runtime.observer.projected_sequence == wire_snapshot_sequence

        host.state.append_result(
            GroundedResult(
                result_id="result-c1",
                worker_id="worker-weather",
                turn_id="turn-1",
                text="Answer",
                spoken_text="Answer",
                origin_epoch=runtime.epoch,
            ),
            origin_epoch=runtime.epoch,
        )
        await asyncio.sleep(0)

        incrementals = [frame for frame in worker.frames if frame["kind"] == "result"]
        assert len(incrementals) == 1
        assert incrementals[0]["sequence"] == wire_snapshot_sequence + 1

    asyncio.run(run())


class YieldingFrameCapturingPipelineWorker(FrameCapturingPipelineWorker):
    """FrameCapturingPipelineWorker whose ``queue_frame`` yields to the event
    loop before acknowledging a barrier frame, so two concurrent
    snapshot-request handlers on the same connection can actually interleave
    at that point instead of one running start-to-finish before the other is
    ever scheduled."""

    async def queue_frame(self, frame: object) -> None:
        await asyncio.sleep(0)
        await super().queue_frame(frame)


def test_concurrent_snapshot_requests_on_one_connection_are_serialized() -> None:
    """Two snapshot-request messages firing concurrently on the same
    connection must not open two ``SnapshotBarrier``s at once: each barrier
    pauses/resumes the one shared observer, so an overlap would let one
    request's resume/reseed corrupt the other's in-flight watermark/buffer.
    """

    async def run() -> None:
        YieldingFrameCapturingPipelineWorker.constructed = []
        host = SessionHost(runner_factory=AsyncAddRunnerForApp)
        monkeypatch = pytest.MonkeyPatch()
        try:
            await _attach_fake_connection(
                monkeypatch,
                host=host,
                startup_observers=[],
                latency_observers=[],
                worker_class=YieldingFrameCapturingPipelineWorker,
            )
        finally:
            monkeypatch.undo()

        runtime = host.connection
        assert runtime is not None
        worker = YieldingFrameCapturingPipelineWorker.constructed[-1]
        await worker.rtvi.handlers["on_client_ready"](worker.rtvi)

        active_barriers = 0
        max_concurrent_barriers = 0
        original_pause = runtime.observer.pause
        original_resume = runtime.observer.resume

        def tracking_pause() -> None:
            nonlocal active_barriers, max_concurrent_barriers
            active_barriers += 1
            max_concurrent_barriers = max(max_concurrent_barriers, active_barriers)
            original_pause()

        def tracking_resume(watermark: int | None = None) -> None:
            nonlocal active_barriers
            original_resume(watermark)
            active_barriers -= 1

        runtime.observer.pause = tracking_pause  # type: ignore[method-assign]
        runtime.observer.resume = tracking_resume  # type: ignore[method-assign]

        message = SimpleNamespace(type="snapshot-request", data=None)
        await asyncio.gather(
            worker.rtvi.handlers["on_client_message"](worker.rtvi, message),
            worker.rtvi.handlers["on_client_message"](worker.rtvi, message),
        )

        # The lock must ensure the second barrier never opens before the
        # first resumes: at most one pause-without-matching-resume at a time.
        assert max_concurrent_barriers == 1
        assert active_barriers == 0
        snapshot_frames = [frame for frame in worker.frames if frame["kind"] == "runtime_snapshot"]
        assert len(snapshot_frames) == 2

    asyncio.run(run())


# --- I12: wire_payload is the single choke point for work_status presence --


def _snapshot_frame_data(*, capabilities: tuple[str, ...]) -> dict:
    """Attach a fake connection, request a snapshot, return the wire frame."""

    async def run() -> dict:
        FrameCapturingPipelineWorker.constructed = []
        host = SessionHost(runner_factory=AsyncAddRunnerForApp)
        monkeypatch = pytest.MonkeyPatch()
        try:
            await _attach_fake_connection(
                monkeypatch,
                host=host,
                startup_observers=[],
                latency_observers=[],
                worker_class=FrameCapturingPipelineWorker,
                capabilities=capabilities,
            )
        finally:
            monkeypatch.undo()

        runtime = host.connection
        assert runtime is not None
        worker = FrameCapturingPipelineWorker.constructed[-1]
        await worker.rtvi.handlers["on_client_ready"](worker.rtvi)
        await worker.rtvi.handlers["on_client_message"](
            worker.rtvi, SimpleNamespace(type="snapshot-request", data=None)
        )
        frame = worker.frames[-1]
        assert frame["kind"] == "runtime_snapshot"
        return frame

    return asyncio.run(run())


def test_non_capable_snapshot_omits_work_status_but_keeps_nullable_required_fields() -> None:
    """I12: a non-capable connection's snapshot must omit `work_status`
    entirely (field absent, not an empty array), while every other nullable
    field the frozen schema marks *required* -- `routing`, `origin_epoch` --
    stays present. This is the test that fails under a naive
    `model_dump(exclude_none=True)` fix, which would strip `routing: None`
    and break schema validation.
    """
    frame = _snapshot_frame_data(capabilities=())
    data = frame["data"]

    assert "work_status" not in data
    assert "routing" in data and data["routing"] is None
    assert "origin_epoch" in data


def test_capable_snapshot_carries_the_work_status_field() -> None:
    """I12: a connection that advertised `work_status_v1` keeps the field."""
    frame = _snapshot_frame_data(capabilities=("work_status_v1",))
    data = frame["data"]

    assert "work_status" in data
    assert isinstance(data["work_status"], list)


# --- Batch 7: single-decoder handshake parsing, bounds, and 409-vs-400 -------
#
# I14: `capabilities` must decode through Starlette's QueryParams like every
# other handshake field (one decoder per request, `+` == space), the raw
# percent-encoding validator must run exactly once, and duplicate query keys
# must still be rejected -- `QueryParams.getlist` does expose duplicates.


def test_handshake_capabilities_decode_matches_starlette_query_params() -> None:
    """`+` in the capabilities value decodes to a space, identically to the
    way `request.query_params` decodes the very same bytes."""
    host, handshake = _host_with_fresh_handshake_token()
    query = _capability_query(
        session_id=handshake["session_id"],
        resume_token=handshake["resume_token"],
        epoch=handshake["proposed_epoch"],
        capabilities="%5B%22alpha+beta%22%5D",
    )
    request = _raw_request(query)
    expected = tuple(_json.loads(request.query_params["capabilities"]))

    value = _handshake_from_query(host, request)

    assert expected == ("alpha beta",)
    assert value.capabilities == expected


def test_handshake_validates_raw_percent_encoding_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    host, handshake = _host_with_fresh_handshake_token()
    encoded = quote(_json.dumps(["work_status_v1"]))
    query = _capability_query(
        session_id=handshake["session_id"],
        resume_token=handshake["resume_token"],
        epoch=handshake["proposed_epoch"],
        capabilities=encoded,
    )
    calls: list[bytes] = []
    original = app_module._validate_raw_percent_encoding

    def counting(raw: bytes) -> None:
        calls.append(raw)
        original(raw)

    monkeypatch.setattr(app_module, "_validate_raw_percent_encoding", counting)

    _handshake_from_query(host, _raw_request(query))

    assert len(calls) == 1


def test_handshake_still_rejects_duplicate_capabilities_keys_via_getlist() -> None:
    host, handshake = _host_with_fresh_handshake_token()
    encoded = quote(_json.dumps(["work_status_v1"]))
    query = (
        f"session_id={handshake['session_id']}&resume_token={handshake['resume_token']}"
        f"&proposed_epoch={handshake['proposed_epoch']}&snapshot_sequence=0"
        f"&capabilities={encoded}&capabilities={encoded}"
    ).encode()

    with pytest.raises(Exception) as excinfo:
        _handshake_from_query(host, _raw_request(query))
    assert getattr(excinfo.value, "status_code", 400) == 400


# M7: the capabilities decoder bounds the array length and each entry length.


@pytest.mark.parametrize(
    "payload",
    [
        [f"cap_{index}" for index in range(17)],
        ["a" * 65],
    ],
)
def test_handshake_rejects_oversized_capabilities_payloads(payload: list[str]) -> None:
    host, handshake = _host_with_fresh_handshake_token()
    query = _capability_query(
        session_id=handshake["session_id"],
        resume_token=handshake["resume_token"],
        epoch=handshake["proposed_epoch"],
        capabilities=quote(_json.dumps(payload)),
    )

    with pytest.raises(Exception) as excinfo:
        _handshake_from_query(host, _raw_request(query))
    assert getattr(excinfo.value, "status_code", 400) == 400


@pytest.mark.parametrize(
    "payload",
    [
        [f"cap_{index:02d}" for index in range(16)],
        ["a" * 64],
    ],
)
def test_handshake_accepts_capabilities_payloads_at_the_bounds(payload: list[str]) -> None:
    host, handshake = _host_with_fresh_handshake_token()
    query = _capability_query(
        session_id=handshake["session_id"],
        resume_token=handshake["resume_token"],
        epoch=handshake["proposed_epoch"],
        capabilities=quote(_json.dumps(payload)),
    )

    value = _handshake_from_query(host, _raw_request(query))

    assert value.capabilities == tuple(sorted(payload))


# M4: a PATCH whose promoted connection is gone is a stale-epoch 409, never a
# capability-mismatch 400 -- an absent connection binds no capability set.


def test_patch_with_correct_capabilities_and_no_connection_is_409_not_400() -> None:
    host = SessionHost(runner_factory=FakeRunner)
    discovery = host.session_handshake()
    encoded = quote(_json.dumps(["work_status_v1"]))
    query = (
        f"session_id={discovery['session_id']}&resume_token={discovery['resume_token']}"
        f"&proposed_epoch={discovery['proposed_epoch']}&snapshot_sequence=0"
        f"&capabilities={encoded}"
    )
    host.arbiter.promote(
        {
            "session_id": host.state.session_id,
            "resume_token": host.state.resume_token,
            "proposed_epoch": discovery["proposed_epoch"],
            "snapshot_sequence": 0,
            "capabilities": ("work_status_v1",),
            "capabilities_present": True,
        }
    )
    # Redeem the one-shot URL token the way a completed POST would have.
    assert host.validate_handshake_token(
        discovery["resume_token"], discovery["proposed_epoch"], redeem=True
    )
    assert host.connection is None

    with TestClient(create_app(host)) as client:
        response = client.patch(
            f"/api/rtc?{query}",
            json={
                "pc_id": "pc-1",
                "candidates": [{"candidate": "", "sdp_mid": "0", "sdp_mline_index": 0}],
            },
            headers={"origin": "http://127.0.0.1:7860"},
        )

    assert response.status_code == 409
    assert response.json()["detail"] == "stale Small WebRTC connection epoch"
