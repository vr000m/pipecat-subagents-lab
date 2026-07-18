"""HTTP entry-point tests for the local Small WebRTC server."""

from fastapi.testclient import TestClient

from server.app import create_app
from server.pipeline import SessionHost


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
