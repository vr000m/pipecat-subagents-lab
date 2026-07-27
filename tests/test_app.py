"""HTTP entry-point tests for the local Small WebRTC server."""

import asyncio
from types import SimpleNamespace
from typing import ClassVar

import pytest
from fastapi.testclient import TestClient

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
    monkeypatch.setattr(
        app_module, "load_config", lambda: Config(bind_host="127.0.0.2", bind_port=9000)
    )
    monkeypatch.setattr(
        uvicorn, "run", lambda target, **kwargs: calls.append({"target": target, **kwargs})
    )

    app_module.main()

    assert calls == [{"target": "server.app:app", "host": "127.0.0.2", "port": 9000}]


def test_injected_session_host_is_preserved() -> None:
    host = SessionHost(runner_factory=FakeRunner)

    assert create_app(host).state.session_host is host


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
