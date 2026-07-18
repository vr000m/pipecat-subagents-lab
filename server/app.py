"""FastAPI entry point for the local Small WebRTC browser server."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Callable

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
from pipecat.workers.base_worker import WorkerParams

from .config import Config, load_config
from .contracts import CONTRACT_VERSION, SnapshotHandshake
from .pipeline import CanonicalResultAdapter, SessionHost, framework_bridge
from .rtvi_messages import RTVIMessagePublisher
from .registry import WorkerRegistry
from .router import LazyRouterProvider, Router
from .services.stt import LocalSTT, STTEndpoint
from .services.tts import LocalTTS, TTSEndpoint
from .work_item_coordinator import WorkItemCoordinator


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


def _require_local_origin(request: Request, config: Config) -> None:
    """Keep the credential-bearing local discovery surface same-origin."""
    origin = request.headers.get("origin")
    if origin and origin.rstrip("/") != config.known_client_url.rstrip("/"):
        raise HTTPException(status_code=403, detail="origin is not allowed for the local server")


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
    if host.stt is not None:
        processors.append(host.stt)
    if bridge is not None:
        processors.extend((bridge, CanonicalResultAdapter()))
    if host.tts is not None:
        processors.append(host.tts)
    processors.append(transport.output())
    worker = PipelineWorker(
        Pipeline(processors),
        name=f"browser-{runtime.epoch}",
        params=PipelineParams(audio_in_sample_rate=16000, audio_out_sample_rate=24000),
        enable_rtvi=True,
        idle_timeout_secs=None,
    )
    publisher = RTVIMessagePublisher(
        host.state.session_id, runtime.epoch, sequence_provider=lambda: host.state.sequence
    )
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

    # WorkerRunner has no remove-workers API in the pinned wheel. Run each
    # connection worker through its real PipelineWorker lifecycle task so
    # replacement can cancel and await it without leaking runner registry
    # entries. Lightweight test runners retain their documented add_workers
    # registration seam.
    if type(host.runner).__module__.startswith("pipecat."):
        runtime.worker_task = asyncio.create_task(
            worker.run(WorkerParams(loop=asyncio.get_running_loop()))
        )
    else:
        add_workers = getattr(host.runner, "add_workers", None)
        if add_workers is None:
            raise RuntimeError("the configured runner cannot attach a Small WebRTC worker")
        attached = add_workers(worker)
        if hasattr(attached, "__await__"):
            await attached


def _default_session_host(
    *,
    router: Router | None = None,
    router_responses_factory: Callable[[], Any] | None = None,
) -> SessionHost:
    """Build the default host while keeping credentialed providers lazy."""
    config = load_config()
    registry = WorkerRegistry(config=config)
    configured_router = router or Router(
        call=LazyRouterProvider(config, router_responses_factory),
        config=config,
    )
    coordinator = WorkItemCoordinator(
        registry=registry,
        router=configured_router,
        config=config,
    )
    stt = LocalSTT(STTEndpoint(*config.stt_endpoint)) if config.stt_endpoint else None
    tts = LocalTTS(TTSEndpoint(*config.tts_endpoint)) if config.tts_endpoint else None
    return SessionHost(registry=registry, stt=stt, tts=tts, coordinator=coordinator)


def create_app(host: SessionHost | None = None) -> FastAPI:
    """Create the local FastAPI app and its Small WebRTC signaling routes."""
    session_host = host if host is not None else _default_session_host()
    config = getattr(session_host.registry, "config", None) or Config()
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
    async def session(request: Request) -> dict[str, Any]:
        _require_local_origin(request, config)
        return session_host.session_handshake()

    @app.post("/api/rtc")
    async def offer(request: SmallWebRTCRequest, http_request: Request) -> dict[str, str]:
        _require_local_origin(http_request, config)
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
    async def ice_candidate(
        request: SmallWebRTCPatchRequest, http_request: Request
    ) -> dict[str, str]:
        _require_local_origin(http_request, config)
        handshake = _handshake_from_query(session_host, http_request)
        if not session_host.accepts(handshake.proposed_epoch):
            raise HTTPException(status_code=409, detail="stale Small WebRTC connection epoch")
        await webrtc_handler.handle_patch_request(request)
        return {"status": "success"}

    if (_WEB_ROOT / "dist").is_dir():
        app.mount("/dist", StaticFiles(directory=_WEB_ROOT / "dist"), name="dist")

        @app.get("/styles.css", include_in_schema=False)
        async def styles() -> FileResponse:
            return FileResponse(_WEB_ROOT / "src" / "styles.css", media_type="text/css")

        @app.get("/", include_in_schema=False)
        async def index() -> FileResponse:
            return FileResponse(_WEB_ROOT / "index.html")

    return app


app = create_app()


async def serve(host: SessionHost | None = None) -> SessionHost:
    """Start a host for embedding; production serving is provided by Uvicorn."""
    runtime = host if host is not None else _default_session_host()
    await runtime.start()
    return runtime
