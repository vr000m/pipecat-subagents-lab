"""HTTP entry-point tests for the local Small WebRTC server."""

from fastapi.testclient import TestClient

from server.app import create_app
from server.config import Config
from server.pipeline import SessionHost
from server.registry import WorkerRegistry
from server.services.stt import LocalSTT
from server.services.tts import LocalTTS


class FakeRunner:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


def test_default_app_host_materializes_configured_local_speech_adapters(
    monkeypatch,
) -> None:
    monkeypatch.setenv("WEBSEARCH_STT_ENDPOINT", "uds:///tmp/local-stt.sock")
    monkeypatch.setenv("WEBSEARCH_TTS_ENDPOINT", "ws://127.0.0.1:9000")

    host = create_app().state.session_host

    assert isinstance(host.stt, LocalSTT)
    assert host.stt.endpoint.transport == "uds"
    assert host.stt.endpoint.address == "/tmp/local-stt.sock"
    assert isinstance(host.tts, LocalTTS)
    assert host.tts.endpoint.transport == "ws"
    assert host.tts.endpoint.address == "127.0.0.1:9000"


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
        handshake = client.get("/api/session").json()

    assert handshake["contract_version"] == "v1.0"
    assert handshake["session_id"] == host.state.session_id
    assert handshake["resume_token"] == host.state.resume_token
    assert handshake["proposed_epoch"] == 1
    assert handshake["snapshot_sequence"] == 0


def test_offer_rejects_missing_or_wrong_session_identity_before_sdp_handling() -> None:
    host = SessionHost(runner_factory=FakeRunner)

    with TestClient(create_app(host)) as client:
        response = client.post(
            "/api/rtc?session_id=wrong&resume_token=wrong&proposed_epoch=1",
            json={"sdp": "v=0", "type": "offer"},
        )

    assert response.status_code == 401
    assert response.json()["detail"] == "invalid Small WebRTC session identity"


def test_session_discovery_rejects_cross_origin_requests() -> None:
    host = SessionHost(runner_factory=FakeRunner)
    with TestClient(create_app(host)) as client:
        response = client.get("/api/session", headers={"origin": "https://evil.example"})
    assert response.status_code == 403


def test_session_discovery_uses_configured_client_origin() -> None:
    registry = WorkerRegistry(config=Config(known_client_url="https://client.example.test/app"))
    host = SessionHost(registry=registry, runner_factory=FakeRunner)
    with TestClient(create_app(host)) as client:
        response = client.get(
            "/api/session", headers={"origin": "https://client.example.test/app/"}
        )
    assert response.status_code == 200
