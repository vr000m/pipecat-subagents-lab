"""FastAPI entry point for the local Small WebRTC browser server."""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger as _loguru_logger
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.observers.startup_timing_observer import StartupTimingObserver
from pipecat.observers.user_bot_latency_observer import UserBotLatencyObserver
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.audio.vad_processor import VADProcessor
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.processors.frameworks.rtvi import RTVIObserverParams
from pipecat.processors.frameworks.rtvi.frames import RTVIServerMessageFrame
from pipecat.transports.base_transport import TransportParams
from pipecat.transports.smallwebrtc.connection import SmallWebRTCConnection
from pipecat.transports.smallwebrtc.request_handler import (
    SmallWebRTCPatchRequest,
    SmallWebRTCRequest,
    SmallWebRTCRequestHandler,
)
from pipecat.transports.smallwebrtc.transport import SmallWebRTCTransport
from pipecat.utils.asyncio.task_manager import TaskManager
from pipecat.workers.base_worker import WorkerParams

from .config import Config, load_config
from .contracts import CONTRACT_VERSION, SnapshotHandshake
from .perf_metrics import MeasurementSink, PerfConnectionContext, attach_framework_observers
from .pipeline import CanonicalResultAdapter, SessionHost, framework_bridge
from .preflight import ConfiguredServiceProbe, Probe, run_preflight
from .registry import WorkerRegistry
from .router import LazyRouterProvider, Router
from .rtvi_messages import RTVIMessagePublisher
from .services.factory import create_stt, create_tts
from .turns import FinalTurnTranscriptProcessor, smart_turn_processor
from .work_item_coordinator import WorkItemCoordinator

_WEB_ROOT = Path(__file__).resolve().parent.parent / "web"
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


def _configure_logging() -> None:
    """Disable Loguru's diagnose/backtrace rendering so tracebacks never dump
    local variable values (transcripts, provider payloads, API keys) to logs."""
    _loguru_logger.remove()
    _loguru_logger.add(sys.stderr, backtrace=False, diagnose=False)


_configure_logging()


class _SpeechCompletionProcessor(FrameProcessor):
    """Release the active speech lease when any TTS provider finishes."""

    def __init__(self, host: SessionHost, runtime: Any) -> None:
        super().__init__()
        self._host = host
        self._runtime = runtime

    async def process_frame(self, frame: Any, direction: FrameDirection) -> None:
        from pipecat.frames.frames import TTSStartedFrame, TTSStoppedFrame

        await super().process_frame(frame, direction)
        if (
            direction == FrameDirection.DOWNSTREAM
            and isinstance(frame, TTSStartedFrame)
            and self._host.connection is self._runtime
            and self._runtime.active
        ):
            self._runtime.scheduler.provider_started(frame.context_id)
        if (
            direction == FrameDirection.DOWNSTREAM
            and isinstance(frame, TTSStoppedFrame)
            and self._host.connection is self._runtime
            and self._runtime.active
        ):
            matched = self._runtime.scheduler.provider_synthesis_ended(frame.context_id)
            if matched:
                self._runtime.scheduler.provider_delivery_unknown(frame.context_id)
                await self._runtime.scheduler.start_next()
        await self.push_frame(frame, direction)


def _tts_processors(host: SessionHost, runtime: Any) -> tuple[Any, ...]:
    """Choose exactly one completion signal for the configured TTS service."""
    if hasattr(runtime.tts, "on_event"):
        return (runtime.tts,)
    return (runtime.tts, _SpeechCompletionProcessor(host, runtime))


def _handshake_from_query(host: SessionHost, request: Request) -> SnapshotHandshake:
    """Parse a short-lived browser handshake token carried by the URL."""
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
    if value.session_id != host.state.session_id or not host.validate_handshake_token(
        value.resume_token,
        value.proposed_epoch,
        redeem=request.method == "POST",
    ):
        raise HTTPException(status_code=401, detail="invalid Small WebRTC session identity")
    # The URL token is deliberately not the process-lifetime resume bearer. The
    # arbiter still receives its stable identity after the HTTP token is checked.
    return value.model_copy(update={"resume_token": host.state.resume_token})


def _require_local_origin(request: Request, config: Config) -> None:
    """Keep local discovery same-origin without rejecting browser fetch defaults."""
    configured = urlparse(config.known_client_url)
    allowed_hosts = {configured.hostname}
    if configured.hostname in _LOOPBACK_HOSTS:
        allowed_hosts.update(_LOOPBACK_HOSTS)

    def allowed_origin(value: str) -> bool:
        try:
            candidate = urlparse(value)
            candidate_port = candidate.port or (443 if candidate.scheme == "https" else 80)
            configured_port = configured.port or (443 if configured.scheme == "https" else 80)
        except ValueError:
            return False
        return (
            candidate.scheme == configured.scheme
            and candidate.hostname in allowed_hosts
            and candidate_port == configured_port
        )

    origin = request.headers.get("origin")
    if origin:
        if not allowed_origin(origin):
            raise HTTPException(
                status_code=403, detail="origin is not allowed for the local server"
            )
        return

    # Chromium omits Origin on a same-origin GET such as fetch('/api/session').
    # Sec-Fetch-Site is browser-controlled, and Host prevents accepting a
    # same-origin marker sent to an unexpected local listener.
    if request.headers.get("sec-fetch-site") != "same-origin" or not allowed_origin(
        f"{configured.scheme}://{request.headers.get('host', '')}"
    ):
        raise HTTPException(status_code=403, detail="origin is not allowed for the local server")


async def _attach_connection(
    host: SessionHost,
    connection: SmallWebRTCConnection,
    handshake: SnapshotHandshake,
) -> None:
    """Attach a real Pipecat Small WebRTC pipeline to a promoted epoch."""
    runtime = await host.connect(handshake)
    try:
        if runtime.tts is not None and hasattr(runtime.tts, "connect"):
            await runtime.tts.connect()
        if not host.accepts(runtime.epoch):
            await runtime.shutdown(reason="connection replaced during setup")
            return
        output_sample_rate = getattr(runtime.tts, "sample_rate", 24000)
        params = TransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_in_sample_rate=16000,
            audio_out_sample_rate=output_sample_rate,
        )
        transport = SmallWebRTCTransport(connection, params)
        bus = getattr(host.runner, "bus", None)
        if bus is None:
            from .pipeline import _ProbeBus

            bus = _ProbeBus() if _ProbeBus is not None else None
        bridge = framework_bridge(bus=bus, worker_name=f"browser-{runtime.epoch}") if bus else None
        config = getattr(host.registry, "config", None) or Config()
        processors = [transport.input()]
        if runtime.stt is not None:
            processors.extend(
                (
                    VADProcessor(vad_analyzer=SileroVADAnalyzer(sample_rate=16000)),
                    runtime.stt,
                    smart_turn_processor(timeout_seconds=config.smart_turn_timeout_seconds),
                    FinalTurnTranscriptProcessor(
                        runtime.on_transcript,
                        complete_grace_seconds=config.smart_turn_complete_grace_seconds,
                    ),
                )
            )
        if bridge is not None:
            processors.extend((bridge, CanonicalResultAdapter()))
        if runtime.tts is not None:
            processors.extend(_tts_processors(host, runtime))
        processors.append(transport.output())
        task_manager = TaskManager(loop=asyncio.get_running_loop())
        connection_worker_name = f"browser-{runtime.epoch}"
        startup_observer = StartupTimingObserver()
        latency_observer = UserBotLatencyObserver()
        worker = PipelineWorker(
            Pipeline(processors),
            name=connection_worker_name,
            params=PipelineParams(
                audio_in_sample_rate=16000,
                audio_out_sample_rate=output_sample_rate,
                enable_metrics=True,
            ),
            enable_rtvi=True,
            # enable_metrics=True feeds MetricsFrames to the RTVI observer, which
            # defaults to forwarding them to the client. This release is console-only.
            rtvi_observer_params=RTVIObserverParams(metrics_enabled=False),
            idle_timeout_secs=None,
            task_manager=task_manager,
            observers=[startup_observer, latency_observer],
        )
        turn_tracking_observer = worker.turn_tracking_observer
        if turn_tracking_observer is None:
            raise RuntimeError(
                "PipelineWorker did not construct its default turn tracking observer"
            )
        attach_framework_observers(
            startup_observer=startup_observer,
            latency_observer=latency_observer,
            turn_tracking_observer=turn_tracking_observer,
            context=PerfConnectionContext(
                session_id=host.state.session_id,
                origin_epoch=runtime.epoch,
                connection_worker=connection_worker_name,
            ),
            sink=host.measurement_sink,
        )
    except BaseException:
        host.abort_connection(runtime)
        await runtime.shutdown(reason="connection setup failed")
        raise
    try:
        publisher = RTVIMessagePublisher(
            host.state.session_id, runtime.epoch, sequence_provider=lambda: host.state.sequence
        )
        runtime.transport = transport
        runtime.worker = worker
        if not host.accepts(runtime.epoch):
            await runtime.shutdown(reason="connection replaced during setup")
            return

        async def emit_frame(frame: Any) -> None:
            if host.accepts(runtime.epoch):
                await worker.queue_frame(frame)

        runtime.observer.subscribe(emit_frame)

        client_ready_sent = False

        @worker.rtvi.event_handler("on_client_ready")
        async def on_client_ready(_rtvi: Any) -> None:
            nonlocal client_ready_sent
            if client_ready_sent:
                return
            if host.accepts(runtime.epoch):
                client_ready_sent = True
                publisher.client_ready(epoch=runtime.epoch)

        @worker.rtvi.event_handler("on_client_message")
        async def on_client_message(_rtvi: Any, message: Any) -> None:
            data = getattr(message, "data", None)
            snapshot_requested = message.type == "snapshot-request" or (
                message.type == "client-message"
                and isinstance(data, dict)
                and data.get("t") == "snapshot-request"
            )
            if not snapshot_requested or not host.accepts(runtime.epoch):
                return
            publisher.set_snapshot(runtime.observer.snapshot())
            snapshot = publisher.snapshot()
            if snapshot is not None:
                await worker.queue_frame(
                    RTVIServerMessageFrame(data=snapshot.model_dump(mode="json"))
                )

        # WorkerRunner has no remove-workers API in the pinned wheel. Run each
        # connection worker through its real PipelineWorker lifecycle task so
        # replacement can cancel and await it without leaking runner registry
        # entries. Lightweight test runners retain their documented add_workers
        # registration seam.
        if type(host.runner).__module__.startswith("pipecat."):
            runtime.worker_task = asyncio.create_task(
                worker.run(WorkerParams(task_manager=task_manager))
            )

            def worker_finished(task: asyncio.Task[Any]) -> None:
                if task.cancelled():
                    return
                try:
                    error = task.exception()
                except asyncio.CancelledError:
                    return
                if error is not None:
                    asyncio.create_task(runtime.shutdown(reason="Small WebRTC worker failed"))

            runtime.worker_task.add_done_callback(worker_finished)
        else:
            add_workers = getattr(host.runner, "add_workers", None)
            if add_workers is None:
                raise RuntimeError("the configured runner cannot attach a Small WebRTC worker")
            attached = add_workers(worker)
            if hasattr(attached, "__await__"):
                await attached
    except BaseException:
        host.abort_connection(runtime)
        await runtime.shutdown(reason="connection setup failed")
        raise


def _default_session_host(
    *,
    router: Router | None = None,
    router_responses_factory: Callable[[], Any] | None = None,
    measurement_sink: MeasurementSink | None = None,
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
    stt = create_stt(config)
    tts = create_tts(config)
    return SessionHost(
        registry=registry,
        stt=stt,
        tts=tts,
        coordinator=coordinator,
        measurement_sink=measurement_sink,
    )


def create_app(
    host: SessionHost | None = None,
    *,
    preflight_probe: Probe | None = None,
) -> FastAPI:
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

    @app.get("/api/readyz")
    async def readyz() -> JSONResponse:
        probe = preflight_probe or ConfiguredServiceProbe(config)
        report = await asyncio.to_thread(
            run_preflight,
            config,
            probe=probe,
            authenticated_capability_check=lambda value: (
                "available" if value.openai_api_key else "unavailable"
            ),
        )
        return JSONResponse(
            status_code=200 if report.ok else 503,
            content={
                "status": "ready" if report.ok else "not_ready",
                "failures": report.failures,
                "authenticated_capability": report.authenticated_capability,
            },
        )

    @app.get("/api/session")
    async def session(request: Request, response: Response) -> dict[str, Any]:
        _require_local_origin(request, config)
        response.headers["Cache-Control"] = "no-store"
        return session_host.session_handshake()

    @app.post("/api/rtc")
    async def offer(
        request: SmallWebRTCRequest, http_request: Request, response: Response
    ) -> dict[str, str]:
        _require_local_origin(http_request, config)
        response.headers["Cache-Control"] = "no-store"
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
        request: SmallWebRTCPatchRequest, http_request: Request, response: Response
    ) -> dict[str, str]:
        _require_local_origin(http_request, config)
        response.headers["Cache-Control"] = "no-store"
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


def main() -> None:
    """Serve the browser app using the validated bind configuration."""
    import uvicorn

    config = load_config()
    uvicorn.run("server.app:app", host=config.bind_host, port=config.bind_port)


if __name__ == "__main__":
    main()
