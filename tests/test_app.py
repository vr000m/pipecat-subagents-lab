"""HTTP entry-point tests for the local Small WebRTC server."""

from fastapi.testclient import TestClient

from server.app import create_app
from server.config import Config
from server.pipeline import SessionHost
from server.registry import WorkerRegistry


class FakeRunner:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


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
