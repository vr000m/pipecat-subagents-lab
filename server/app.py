"""FastAPI entry point for the local Small WebRTC browser server."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.frameworks.rtvi.frames import RTVIServerMessageFrame
from pipecat.transports.base_transport import TransportParams
from pipecat.transports.smallwebrtc.connection import SmallWebRTCConnection
from pipecat.transports.smallwebrtc.request_handler import (
    SmallWebRTCPatchRequest,
    SmallWebRTCRequest,
    SmallWebRTCRequestHandler,
)
from pipecat.transports.smallwebrtc.transport import SmallWebRTCTransport

from .contracts import CONTRACT_VERSION, SnapshotHandshake
from .pipeline import CanonicalResultAdapter, SessionHost, framework_bridge
from .rtvi_messages import RTVIMessagePublisher


_WEB_ROOT = Path(__file__).resolve().parent.parent / "web"


def _handshake_from_query(host: SessionHost, request: Request) -> SnapshotHandshake:
    """Parse and validate the browser session identity carried by the URL."""
    try:
        value = SnapshotHandshake(
            contract_version=request.query_params.get("contract_version", CONTRACT_VERSION),
            session_id=request.query_params["session_id"],
            resume_token=request.query_params["resume_token"],
            proposed_epoch=int(request.query_params["proposed_epoch"]),
            snapshot_sequence=int(request.query_params.get("snapshot_sequence", "0")),
        )
    except (KeyError, TypeError, ValueError):
        raise HTTPException(status_code=400, detail="invalid Small WebRTC session handshake")
    if value.session_id != host.state.session_id or value.resume_token != host.state.resume_token:
        raise HTTPException(status_code=401, detail="invalid Small WebRTC session identity")
    return value


async def _attach_connection(
    host: SessionHost,
    connection: SmallWebRTCConnection,
    handshake: SnapshotHandshake,
) -> None:
    """Attach a real Pipecat Small WebRTC pipeline to a promoted epoch."""
    runtime = await host.connect(handshake)
    params = TransportParams(
        audio_in_enabled=True,
        audio_out_enabled=True,
        audio_in_sample_rate=16000,
        audio_out_sample_rate=24000,
    )
    transport = SmallWebRTCTransport(connection, params)
    bus = getattr(host.runner, "bus", None)
    if bus is None:
        from .pipeline import _ProbeBus

        bus = _ProbeBus() if _ProbeBus is not None else None
    bridge = framework_bridge(bus=bus, worker_name=f"browser-{runtime.epoch}") if bus else None
    processors = [transport.input()]
    if bridge is not None:
        processors.extend((bridge, CanonicalResultAdapter()))
    processors.append(transport.output())
    worker = PipelineWorker(
        Pipeline(processors),
        name=f"browser-{runtime.epoch}",
        params=PipelineParams(audio_in_sample_rate=16000, audio_out_sample_rate=24000),
        enable_rtvi=True,
        idle_timeout_secs=None,
    )
    publisher = RTVIMessagePublisher(host.state.session_id, runtime.epoch)
    runtime.transport = transport
    runtime.worker = worker

    async def emit_frame(frame: Any) -> None:
        if host.accepts(runtime.epoch):
            await worker.queue_frame(frame)

    runtime.observer.subscribe(emit_frame)

    @worker.rtvi.event_handler("on_client_ready")
    async def on_client_ready(_rtvi: Any) -> None:
        if host.accepts(runtime.epoch):
            publisher.client_ready(epoch=runtime.epoch)

    @worker.rtvi.event_handler("on_client_message")
    async def on_client_message(_rtvi: Any, message: Any) -> None:
        if message.type != "snapshot-request" or not host.accepts(runtime.epoch):
            return
        publisher.set_snapshot(runtime.observer.snapshot())
        snapshot = publisher.snapshot()
        if snapshot is not None:
            await worker.queue_frame(RTVIServerMessageFrame(data=snapshot.model_dump(mode="json")))

    add_workers = getattr(host.runner, "add_workers", None)
    if add_workers is None:
        raise RuntimeError("the configured runner cannot attach a Small WebRTC worker")
    attached = add_workers(worker)
    # The pinned wheel currently returns an awaitable; older/fake runners in
    # this lab expose the documented synchronous registration shape.
    if hasattr(attached, "__await__"):
        await attached


def create_app(host: SessionHost | None = None) -> FastAPI:
    """Create the local FastAPI app and its Small WebRTC signaling routes."""
    session_host = host or SessionHost()
    webrtc_handler = SmallWebRTCRequestHandler()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await session_host.start()
        try:
            yield
        finally:
            await webrtc_handler.close()
            await session_host.shutdown()

    app = FastAPI(title="Pipecat Subagents Lab", lifespan=lifespan)
    app.state.session_host = session_host
    app.state.webrtc_handler = webrtc_handler

    @app.get("/api/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok", "transport": "smallwebrtc"}

    @app.get("/api/session")
    async def session() -> dict[str, Any]:
        return session_host.session_handshake()

    @app.post("/api/rtc")
    async def offer(request: SmallWebRTCRequest, http_request: Request) -> dict[str, str]:
        handshake = _handshake_from_query(session_host, http_request)

        async def connection_callback(connection: SmallWebRTCConnection) -> None:
            await _attach_connection(session_host, connection, handshake)

        answer = await webrtc_handler.handle_web_request(
            request=request,
            webrtc_connection_callback=connection_callback,
        )
        if answer is None:
            raise HTTPException(status_code=502, detail="Small WebRTC produced no answer")
        return answer

    @app.patch("/api/rtc")
    async def ice_candidate(request: SmallWebRTCPatchRequest) -> dict[str, str]:
        await webrtc_handler.handle_patch_request(request)
        return {"status": "success"}

    if (_WEB_ROOT / "dist").is_dir():
        app.mount("/dist", StaticFiles(directory=_WEB_ROOT / "dist"), name="dist")

        @app.get("/", include_in_schema=False)
        async def index() -> FileResponse:
            return FileResponse(_WEB_ROOT / "index.html")

    return app


app = create_app()


async def serve(host: SessionHost | None = None) -> SessionHost:
    """Start a host for embedding; production serving is provided by Uvicorn."""
    runtime = host or SessionHost()
    await runtime.start()
    return runtime
