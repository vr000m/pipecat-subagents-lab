"""FastAPI entry point for the local Small WebRTC browser server."""

from __future__ import annotations

import asyncio
import json
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

from .config import Config, load_config, load_promotion_manifest
from .contracts import CONTRACT_VERSION, SnapshotHandshake
from .frames import SnapshotBarrierFlushFrame
from .observers import ProjectedEvent, SnapshotBarrier
from .perf_metrics import MeasurementSink, PerfConnectionContext, attach_framework_observers
from .pipeline import CanonicalResultAdapter, SessionHost, framework_bridge
from .preflight import ConfiguredServiceProbe, Probe, run_preflight
from .registry import WorkerRegistry
from .router import LazyRouterProvider, Router
from .rtvi_messages import RTVIMessagePublisher
from .services.factory import create_stt, create_tts
from .speech_lifecycle import (
    GenericProviderErrorObserver,
    TransportSpeechLifecycleProcessor,
)
from .turns import FinalTurnTranscriptProcessor, smart_turn_processor
from .work_item_coordinator import WorkItemCoordinator

_WEB_ROOT = Path(__file__).resolve().parent.parent / "web"
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
# Bounds on the URL-carried `capabilities` array: the handshake is a short
# fixed vocabulary of capability names, so an oversized array or an
# oversized name is malformed input, not a large-but-valid handshake.
_MAX_CAPABILITY_ENTRIES = 16
_MAX_CAPABILITY_NAME_LENGTH = 64
# The widest well-formed field is a JSON array of the maximum number of
# maximum-length names: brackets, one pair of quotes per name, and the commas
# between them. Bounding the raw string keeps pathological input (deep nesting,
# megabyte payloads) away from the parser rather than relying on the parser's
# own limits.
_MAX_CAPABILITY_FIELD_LENGTH = 2 + _MAX_CAPABILITY_ENTRIES * (_MAX_CAPABILITY_NAME_LENGTH + 3)


def _configure_logging() -> None:
    """Disable Loguru's diagnose/backtrace rendering so tracebacks never dump
    local variable values (transcripts, provider payloads, API keys) to logs."""
    _loguru_logger.remove()
    _loguru_logger.add(sys.stderr, backtrace=False, diagnose=False)


class _SpeechCompletionProcessor(FrameProcessor):
    """Record provider synthesis state for the scheduler's public progress.

    Non-terminal: it never releases the active speech lease itself. Release
    is owned by `SpeechLifecycleCoordinator`, which learns synthesis end from
    the same `TTSStoppedFrame` through `TransportSpeechLifecycleProcessor`
    further downstream and only clears the slot on a correlated transport
    stop or completed cleanup.
    """

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
            if matched and self._runtime.lifecycle is None:
                # No coordinator installed (e.g. a TTS-less/test runtime):
                # fall back to the old conservative immediate release.
                self._runtime.scheduler.provider_delivery_unknown(frame.context_id)
                await self._runtime.scheduler.start_next()
        await self.push_frame(frame, direction)


class _SnapshotBarrierConsumer(FrameProcessor):
    """Resolves a `SnapshotBarrierFlushFrame`'s acknowledge handle.

    Not literally the last processor before `transport.output()` --
    `GenericProviderErrorObserver` and `TransportSpeechLifecycleProcessor`
    are appended after it when a TTS/lifecycle pair is configured (see the
    invariant comment at this class's call site in `_attach_connection`).
    That position is still correct because both this frame and every RTVI
    incremental (`RTVIServerMessageFrame`) are pipecat `SystemFrame`
    instances, which pipecat routes through a dedicated per-processor queue
    that bypasses the ordinary `DataFrame` queue entirely -- so relative
    order between an incremental queued before this frame and one queued
    after stays intact end-to-end regardless of how slow or stuck any
    downstream `DataFrame`-only stage (e.g. TTS audio synthesis) is. The
    frame is private and never reaches transport output.
    """

    async def process_frame(self, frame: Any, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)
        if isinstance(frame, SnapshotBarrierFlushFrame):
            if callable(frame.acknowledge):
                frame.acknowledge()
            return
        await self.push_frame(frame, direction)


def _tts_processors(host: SessionHost, runtime: Any) -> tuple[Any, ...]:
    """Choose exactly one completion signal for the configured TTS service."""
    if hasattr(runtime.tts, "on_event"):
        return (runtime.tts,)
    return (runtime.tts, _SpeechCompletionProcessor(host, runtime))


def _validate_raw_percent_encoding(raw_query_string: bytes) -> None:
    """App-layer defense on the raw ASGI scope bytes (before any framework
    decoding): reject a percent sign not followed by two ASCII hex digits.

    Whether a malformed percent sequence even survives uvicorn's own HTTP
    parser to reach ``scope["query_string"]`` is unverified parser behavior
    (see the dev plan's Phase 3 uvicorn-probe bullet); this check is this
    app's own defense for whatever bytes do arrive there.
    """
    text = raw_query_string.decode("latin-1")
    index = 0
    length = len(text)
    hex_digits = "0123456789abcdefABCDEF"
    while index < length:
        if text[index] == "%":
            pair = text[index + 1 : index + 3]
            if len(pair) != 2 or pair[0] not in hex_digits or pair[1] not in hex_digits:
                raise HTTPException(
                    status_code=400, detail="invalid Small WebRTC session handshake"
                )
            index += 3
        else:
            index += 1


def _decode_capabilities(request: Request) -> tuple[tuple[str, ...], bool]:
    """Decode the canonical single URL-encoded JSON-array `capabilities` field.

    Absent means omission (inherit-on-PATCH / unsupported-on-POST); present
    but malformed is a 400, matching every other handshake field. The field
    decodes through Starlette's ``QueryParams`` exactly like every other
    handshake field -- one decoder per request -- and ``getlist`` (unlike
    ``get``/``[]``) preserves duplicate keys, so a duplicate `capabilities`
    key is rejected rather than silently collapsed. Duplicate entries inside
    the single JSON array remain deduplicated by
    ``SnapshotHandshake.validate_capabilities``.
    """
    matches = request.query_params.getlist("capabilities")
    if not matches:
        return (), False
    if len(matches) > 1:
        raise HTTPException(status_code=400, detail="invalid Small WebRTC session handshake")
    if len(matches[0]) > _MAX_CAPABILITY_FIELD_LENGTH:
        raise HTTPException(status_code=400, detail="invalid Small WebRTC session handshake")
    try:
        decoded = json.loads(matches[0])
    except (TypeError, ValueError, RecursionError):
        # RecursionError derives from RuntimeError, not ValueError: without it
        # deeply nested input escapes as an uncaught 500 instead of a 400.
        raise HTTPException(status_code=400, detail="invalid Small WebRTC session handshake")
    if not isinstance(decoded, list) or len(decoded) > _MAX_CAPABILITY_ENTRIES:
        raise HTTPException(status_code=400, detail="invalid Small WebRTC session handshake")
    if any(
        not isinstance(item, str) or not item or len(item) > _MAX_CAPABILITY_NAME_LENGTH
        for item in decoded
    ):
        raise HTTPException(status_code=400, detail="invalid Small WebRTC session handshake")
    return tuple(decoded), True


def _handshake_from_query(host: SessionHost, request: Request) -> SnapshotHandshake:
    """Parse a short-lived browser handshake token carried by the URL."""
    _validate_raw_percent_encoding(request.scope.get("query_string", b""))
    capabilities, capabilities_present = _decode_capabilities(request)
    try:
        value = SnapshotHandshake(
            contract_version=request.query_params.get("contract_version", CONTRACT_VERSION),
            session_id=request.query_params["session_id"],
            resume_token=request.query_params["resume_token"],
            proposed_epoch=int(request.query_params["proposed_epoch"]),
            snapshot_sequence=int(request.query_params.get("snapshot_sequence", "0")),
            capabilities=capabilities,
            capabilities_present=capabilities_present,
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


async def _close_setup_connection(connection: SmallWebRTCConnection) -> None:
    """Best-effort close of the raw peer connection when setup aborts.

    Neither ``ConnectionPipeline.shutdown`` nor the pipecat SmallWebRTC
    request handler closes this on our behalf here: ``handle_web_request``
    only logs an exception propagated from the connection callback (it never
    calls ``connection.disconnect()`` itself), and ``shutdown()``'s worker
    cancellation only reaches transport teardown once ``worker.run(...)``
    has actually started consuming pipeline frames -- which none of the
    three setup-failure paths in ``_attach_connection`` can guarantee. Only
    a direct ``disconnect()`` call closes the peer connection in every case.
    Calling it here is safe even when the transport later performs its own
    teardown: ``SmallWebRTCClient.disconnect`` guards on
    ``is_connected``/``is_closing`` and no-ops if already closed.
    """
    try:
        await connection.disconnect()
    except Exception:  # noqa: BLE001  # best-effort cleanup; never mask the original setup failure
        _loguru_logger.exception(
            "failed to close the Small WebRTC connection during setup teardown"
        )


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
            await runtime.shutdown(reason="connection replaced during setup", reconnect=True)
            await _close_setup_connection(connection)
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
        # Placed here, not after TransportSpeechLifecycleProcessor: that
        # processor must remain the sole processor immediately before
        # transport.output() (Phase 1 invariant). RTVIServerMessageFrame-type
        # frames -- both this barrier frame and every status incremental --
        # are queued directly onto the worker and pass straight through the
        # TTS/lifecycle processors below untouched, so this position still
        # proves every frame queued ahead of the barrier has drained past it.
        processors.append(_SnapshotBarrierConsumer())
        if runtime.tts is not None:
            if runtime.lifecycle is not None:
                processors.append(GenericProviderErrorObserver(runtime.lifecycle, runtime.tts))
            processors.extend(_tts_processors(host, runtime))
        if runtime.lifecycle is not None:
            processors.append(TransportSpeechLifecycleProcessor(runtime.lifecycle))
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
        host.abort_connection(runtime, reconnect=False)
        await runtime.shutdown(reason="connection setup failed", reconnect=False)
        await _close_setup_connection(connection)
        raise
    try:
        publisher = RTVIMessagePublisher(
            host.state.session_id, runtime.epoch, sequence_provider=lambda: host.state.sequence
        )
        runtime.transport = transport
        runtime.worker = worker
        runtime.output_teardown = getattr(connection, "disconnect", None)
        if not host.accepts(runtime.epoch):
            await runtime.shutdown(reason="connection replaced during setup", reconnect=True)
            await _close_setup_connection(connection)
            return

        # Seed the connection-projected sequence at the current snapshot
        # watermark before subscribing, so the first delivered incremental
        # is contiguous with whatever snapshot the client requests next
        # (Phase 3 barrier ordering; see RuntimeObserver.seed, and
        # RTVIMessagePublisher's docstring for who owns which sequence).
        runtime.observer.seed(host.state.sequence)

        async def emit_frame(projected: ProjectedEvent) -> None:
            if not host.accepts(runtime.epoch):
                return
            message = publisher.incremental(
                projected.kind,
                projected.data,
                sequence=projected.sequence,
                origin_epoch=projected.origin_epoch,
            )
            if message is not None:
                await worker.queue_frame(
                    RTVIServerMessageFrame(data=message.model_dump(mode="json"))
                )

        runtime.observer.subscribe(emit_frame)

        client_ready_sent = False
        # Guards SnapshotBarrier construction-through-flush below: two
        # concurrent snapshot-request messages on the same connection would
        # otherwise open two barriers against the same observer/state pair
        # and corrupt each other's watermark/buffer.
        snapshot_lock = asyncio.Lock()
        # Set by a snapshot-request that got coalesced away (found the lock
        # already held) instead of simply being dropped. If the in-flight
        # attempt fails without ever delivering a snapshot -- install_baseline
        # raises, or publisher.snapshot() returns None -- a dropped coalesced
        # request would otherwise strand the client: it already set
        # snapshotRequestPending and discards every incremental until a
        # snapshot arrives, and never retries on its own while that flag is
        # set (web/src/state.js). The lock holder consults this flag after
        # its own attempt and retries once if it is set, rather than queuing
        # unboundedly for every coalesced request.
        snapshot_recheck_requested = False

        @worker.rtvi.event_handler("on_client_ready")
        async def on_client_ready(_rtvi: Any) -> None:
            nonlocal client_ready_sent
            if client_ready_sent:
                return
            if host.accepts(runtime.epoch):
                client_ready_sent = True
                publisher.client_ready(epoch=runtime.epoch)

        async def attempt_snapshot_delivery() -> bool:
            """One snapshot-delivery attempt under ``snapshot_lock``.

            Returns ``True`` iff a snapshot was actually delivered to the
            client, ``False`` on a failure that left nothing delivered (a
            retry may be warranted). ``asyncio.CancelledError`` propagates
            uncaught -- cancellation means this task itself is being torn
            down, not a candidate for an in-place retry.
            """
            # Pause the observer before reading any state so no incremental
            # captured from here on can be dispatched (via the emitter's
            # asyncio.create_task scheduling) ahead of the snapshot -- the
            # barrier owns synchronous SessionState._emit() callbacks between
            # this point and install_baseline() below.
            barrier = SnapshotBarrier(observer=runtime.observer, state=host.state)
            barrier.subscribe_paused()
            try:
                publisher.set_snapshot(runtime.observer.snapshot())
                snapshot = publisher.snapshot()
            except asyncio.CancelledError:
                barrier.cancel()
                raise
            except Exception:  # noqa: BLE001  # intentional catch-all: mirrors install_baseline's own must-not-leave-paused guarantee below
                barrier.cancel()
                _loguru_logger.warning(
                    "snapshot construction failed; incremental delivery resumed "
                    "without a new watermark"
                )
                return False
            if snapshot is None:
                barrier.cancel()
                return False
            # install_baseline() reseeds the observer's projected sequence at
            # snapshot.sequence only after the barrier frame is acknowledged.
            # The observer's projected sequence only advances for events
            # visible to *this* connection, while the snapshot is stamped
            # from the global SessionState watermark, which also advances
            # for invisible events (a capability-gated work_status on a
            # connection that never advertised work_status_v1). Reseeding
            # from the value actually stamped on the wire -- publisher.snapshot()
            # re-reads the sequence provider -- keeps the client's
            # lastAppliedSequence and the observer's counter identical by
            # construction, so the next incremental is snapshot_sequence + 1.
            #
            # Non-capable projections omit the status section entirely
            # (field absent, not an empty array) so the frozen
            # pre-Phase-3 runtime-snapshot schema still validates this
            # connection's snapshots (Requirements). The exclusion is
            # owned by RuntimeSnapshot.wire_payload, not by this caller.
            # Read the entitlement off the observer, which is also what
            # decides snapshot *content* (RuntimeObserver.snapshot() calls
            # SessionState.snapshot(include_work_status=self.supports_work_status)).
            # The content gate and this wire-presence gate are therefore
            # provably one source, not two booleans kept in agreement by
            # convention via ConnectionPipeline's proxy property.
            # The snapshot frame is written *by* install_baseline, between
            # the barrier acknowledgement and the buffered replay, so a
            # buffered incremental can never reach the client ahead of the
            # snapshot that establishes the watermark it applies against.
            # ``wire_payload()`` is deliberately inside this try too -- it can
            # raise (e.g. a monotonicity assertion in the payload/envelope
            # validators), and that must not leave the observer paused any
            # more than a failure inside ``install_baseline`` itself would.
            try:
                frame_data = snapshot.wire_payload(
                    include_work_status=runtime.observer.supports_work_status
                )

                async def write_snapshot() -> None:
                    await worker.queue_frame(RTVIServerMessageFrame(data=frame_data))

                await barrier.install_baseline(
                    watermark=snapshot.sequence,
                    flush_writer=worker.queue_frame,
                    snapshot_writer=write_snapshot,
                )
            except asyncio.CancelledError:
                # Cancellation between the write and the drain (worker
                # replaced/torn down) must not leave the observer paused
                # with an unbounded buffer and this lock's invariant
                # silently broken.
                barrier.cancel()
                raise
            except Exception:  # noqa: BLE001  # intentional catch-all: a failed snapshot install must never leave the observer paused
                barrier.cancel()
                _loguru_logger.warning(
                    "snapshot barrier install failed; incremental delivery resumed "
                    "without a new watermark"
                )
                return False
            return True

        @worker.rtvi.event_handler("on_client_message")
        async def on_client_message(_rtvi: Any, message: Any) -> None:
            nonlocal snapshot_recheck_requested
            data = getattr(message, "data", None)
            snapshot_requested = message.type == "snapshot-request" or (
                message.type == "client-message"
                and isinstance(data, dict)
                and data.get("t") == "snapshot-request"
            )
            if not snapshot_requested or not host.accepts(runtime.epoch):
                return
            # Coalesce, don't queue, concurrent snapshot-request messages: at
            # most one SnapshotBarrier may be open per connection at a time,
            # or two barriers racing against the same observer/state pair
            # could corrupt each other's watermark/buffer. A snapshot rebuild
            # is idempotent, so an in-flight one already satisfies a
            # concurrent request -- blocking on the lock instead would let a
            # client spam this message and build an unbounded lock-waiter
            # queue, each holding a task open for up to
            # SNAPSHOT_BARRIER_ACK_TIMEOUT_SECONDS.
            if snapshot_lock.locked():
                snapshot_recheck_requested = True
                return
            async with snapshot_lock:
                # Retry at most once: the first pass is this request's own
                # attempt, the second only runs if a coalesced request was
                # flagged during that attempt and it did not deliver a
                # snapshot. A request coalesced during the retry itself sets
                # the flag again but is not chased further -- one extra
                # attempt is enough to stop silently stranding a client
                # without letting a spamming client build unbounded retries.
                #
                # Re-checking acceptance on every iteration (not just before
                # the lock was acquired) matters because a single attempt can
                # block for up to SNAPSHOT_BARRIER_ACK_TIMEOUT_SECONDS awaiting
                # its barrier ack: this connection can be superseded by a
                # reconnect (or torn down) entirely within that window. A
                # retry that ran anyway would call barrier.subscribe_paused()
                # on this connection's (by then retired) observer, which
                # re-attaches it to the still-live, shared SessionState event
                # bus with nothing left to ever unsubscribe it again -- and
                # would write the resulting frame through this closure's
                # captured (by then cancelled) ``worker``.
                for _attempt in range(2):
                    if not host.accepts(runtime.epoch):
                        break
                    snapshot_recheck_requested = False
                    if await attempt_snapshot_delivery():
                        break
                    if not snapshot_recheck_requested:
                        break

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
                    # Tracked, not fire-and-forget: an untracked task can be
                    # collected mid-shutdown, and SessionHost.shutdown drains
                    # exactly this set.
                    host.track_background_shutdown(
                        asyncio.create_task(
                            runtime.shutdown(reason="Small WebRTC worker failed", reconnect=False)
                        )
                    )

            runtime.worker_task.add_done_callback(worker_finished)
        else:
            add_workers = getattr(host.runner, "add_workers", None)
            if add_workers is None:
                raise RuntimeError("the configured runner cannot attach a Small WebRTC worker")
            attached = add_workers(worker)
            if hasattr(attached, "__await__"):
                await attached
    except BaseException:
        host.abort_connection(runtime, reconnect=False)
        await runtime.shutdown(reason="connection setup failed", reconnect=False)
        await _close_setup_connection(connection)
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
    # Resolved once per host, exactly like FeaturePolicy.from_config: a
    # missing/unreadable/ineligible manifest degrades to display-only rather
    # than raising, so this never blocks server boot.
    promotion_manifest = load_promotion_manifest(config)
    return SessionHost(
        registry=registry,
        stt=stt,
        tts=tts,
        coordinator=coordinator,
        measurement_sink=measurement_sink,
        promotion_manifest=promotion_manifest,
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
        try:
            session_host.validate_patch_handshake(session_host.connection, handshake)
        except ValueError:
            raise HTTPException(
                status_code=400, detail="capabilities cannot change after connection promotion"
            )
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

    _configure_logging()
    config = load_config()
    uvicorn.run("server.app:app", host=config.bind_host, port=config.bind_port)


if __name__ == "__main__":
    main()
