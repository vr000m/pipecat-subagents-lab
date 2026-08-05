"""Pipecat lifecycle adapter around durable workers and a connection pipeline."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any, Literal
from uuid import uuid4

from loguru import logger
from pipecat.frames.frames import InterruptionFrame, TTSSpeakFrame
from pipecat.processors.frameworks.rtvi.frames import RTVIServerMessageFrame
from pydantic import ValidationError

from .config import Config, FeaturePolicy, PromotionManifest
from .connection_arbiter import ConnectionArbiter
from .contracts import (
    GroundedResult,
    RoutingDecision,
    RoutingState,
    TerminalReason,
    TranscriptEntry,
    WorkerState,
    WorkStatusState,
)
from .observers import RuntimeObserver
from .perf_metrics import (
    AppTurnRecorder,
    CommitOutcome,
    ConsoleMeasurementSink,
    ControlAction,
    ControlOutcome,
    MeasurementSink,
    RetainedRecorder,
    SpeechOutcome,
    WorkItemOutcome,
    WorkItemRecorder,
    WorkOutcome,
)
from .perf_metrics import DeliveryDisposition as LateDeliveryDisposition
from .registry import UnsupportedWorkerType, WorkerRegistry
from .results import canonical_result
from .router import RoutingValidationError
from .rtvi_messages import RTVIMessage
from .session_state import SessionState
from .speech_lifecycle import (
    CONNECTION_LOCAL_FRAMES,
    DeliveryDisposition,
    EventLoopTimerScheduler,
    GenerationIdentity,
    MonotonicClock,
    PreAdmissionTerminalReason,
    SpeechGenerationFlushAckFrame,
    SpeechGenerationMarkerFrame,
    SpeechLifecycleCoordinator,
)
from .speech_scheduler import (
    ROLE_ACK,
    ROLE_RESULT,
    ROLE_TIMEOUT_NOTICE,
    SpeechItem,
    SpeechRole,
    SpeechScheduler,
    UtteranceLease,
)
from .work_item_coordinator import FAILURE_KINDS, LateResult, WorkItemFailure
from .workers.web_search import ClarificationContext, WorkerClarify, WorkerDeclined


class _FallbackBridge:
    """Dependency-free marker used only when the pinned bridge API is absent."""

    framework_fallback = True


try:
    from pipecat.bus.bridge_processor import (
        BusBridgeProcessor as BusBridgeProcessor,  # noqa: PLC0414  # explicit re-export: the bridge type is part of this module's public surface
    )
    from pipecat.bus.bus import WorkerBus
except ImportError:  # pragma: no cover - only for dependency-free contract tests
    BusBridgeProcessor = _FallbackBridge  # type: ignore[assignment,misc]
    WorkerBus = None  # type: ignore[assignment,misc]


_ProbeBus: Any = None

if WorkerBus is not None:

    class _RealProbeBus(WorkerBus):
        """No-op bus used only to construct dependency-free contract pipelines."""

        async def publish(self, _message: Any) -> None:
            return None

    _ProbeBus = _RealProbeBus


_CONTROL_ACK_TEXT: dict[ControlAction, str] = {
    "pause": "Pausing the active response.",
    "resume": "Resuming the paused response.",
    "cancel": "Cancelling the active response.",
    "stop": "Stopping the active response.",
}
"""Acknowledgement spoken for a control action that was understood."""

_CONTROL_MISS_ACK_TEXT: dict[ControlOutcome, str] = {
    "unknown_target": "I could not find that active work item.",
    "no_active": "There is no active response to cancel.",
}
"""Acknowledgement spoken when a cancel/stop named nothing it could act on.

Keyed by ``ControlOutcome``, a vocabulary disjoint from ``ControlAction``;
they are separate tables so neither key type has to widen to ``str | None``.
"""


def framework_bridge(*, bus: Any, worker_name: str, **kwargs: Any) -> Any:
    """Construct the pinned framework bridge with connection-local output frames."""
    if getattr(BusBridgeProcessor, "framework_fallback", False):
        return _FallbackBridge()
    excluded = kwargs.pop("exclude_frames", ())
    # The authoritative set of connection-local frame types lives in
    # CONNECTION_LOCAL_FRAMES (server/speech_lifecycle.py); extend it there,
    # not here.
    kwargs["exclude_frames"] = tuple(dict.fromkeys((*CONNECTION_LOCAL_FRAMES, *excluded)))
    return BusBridgeProcessor(bus=bus, worker_name=worker_name, **kwargs)


def _contract_bridge() -> Any:
    return framework_bridge(
        bus=_ProbeBus() if _ProbeBus is not None else None,
        worker_name="contract-pipeline",
    )


try:
    from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
except ImportError:  # pragma: no cover - dependency-free contract fallback
    FrameProcessor = object  # type: ignore[assignment,misc]
    FrameDirection = Any  # type: ignore[misc,assignment]


class CanonicalResultAdapter(FrameProcessor):
    """Gate result envelopes without interrupting Pipecat frame lifecycles."""

    def __init__(self) -> None:
        if FrameProcessor is not object:
            super().__init__()

    @staticmethod
    def _normalized_result(frame: Any) -> dict[str, Any] | None:
        data = getattr(frame, "data", frame)
        if isinstance(data, dict) and data.get("kind") == "canonical_result":
            data = data.get("data", {key: value for key, value in data.items() if key != "kind"})
        if not isinstance(data, dict) or not all(
            isinstance(data.get(field), str)
            for field in ("result_id", "text", "worker_id", "turn_id")
        ):
            return None
        allowed = set(GroundedResult.model_fields)
        if set(data) - allowed:
            return None
        try:
            parsed = GroundedResult.model_validate(data)
        except ValidationError:
            return None
        return canonical_result(
            result_id=parsed.result_id,
            worker_id=parsed.worker_id,
            turn_id=parsed.turn_id,
            text=parsed.text,
            spoken_text=parsed.spoken_text,
            citations=[citation.model_dump() for citation in parsed.citations],
            origin_epoch=parsed.origin_epoch,
        ).model_dump(mode="json")

    @staticmethod
    def accepts(frame: Any) -> bool:
        data = getattr(frame, "data", frame)
        if isinstance(data, dict) and (
            data.get("kind") == "canonical_result"
            or all(field in data for field in ("result_id", "text", "worker_id", "turn_id"))
        ):
            return CanonicalResultAdapter._normalized_result(frame) is not None
        if not isinstance(frame, RTVIServerMessageFrame) or not isinstance(data, dict):
            return False
        try:
            RTVIMessage.model_validate(data)
        except ValidationError:
            return False
        return True

    async def process_frame(self, frame: Any, direction: Any) -> None:
        """Gate result envelopes while preserving Pipecat pipeline frames."""
        if FrameProcessor is not object:
            await super().process_frame(frame, direction)
        if direction != getattr(FrameDirection, "DOWNSTREAM", direction):
            return

        # Lifecycle, control, audio, and TTS frames must continue through the
        # adapter. Only RTVI result envelopes (and the legacy plain-dict form)
        # are subject to the canonical-result gate.
        data = getattr(frame, "data", frame)
        is_result_envelope = isinstance(frame, RTVIServerMessageFrame) or isinstance(data, dict)
        if not is_result_envelope:
            if FrameProcessor is not object:
                await self.push_frame(frame, direction)
            return
        if not self.accepts(frame):
            return
        if isinstance(frame, RTVIServerMessageFrame):
            frame.data = RTVIMessage.model_validate(data).model_dump(mode="json")
        if isinstance(getattr(frame, "data", frame), dict) and (
            getattr(frame, "data", frame).get("kind") == "canonical_result"
            or all(
                field in getattr(frame, "data", frame)
                for field in ("result_id", "text", "worker_id", "turn_id")
            )
        ):
            normalized = self._normalized_result(frame)
            if normalized is None:
                return
            frame = normalized
        if FrameProcessor is not object:
            await self.push_frame(frame, direction)


@dataclass
class LabPipeline:
    transport: Any
    stt: Any
    tts: Any
    processors: tuple[Any, ...] = field(
        default_factory=lambda: (_contract_bridge(), CanonicalResultAdapter())
    )

    def has_processor(self, name: str) -> bool:
        return any(type(processor).__name__ == name for processor in self.processors)

    async def emit_worker_frame(self, frame: Any) -> bool:
        normalized = CanonicalResultAdapter._normalized_result(frame)
        if normalized is None:
            return False
        frames = getattr(self.transport, "frames", None)
        if frames is None:
            raise TypeError("transport must expose a frames collection for canonical output")
        frames.append(normalized)
        return True


def _speech_role_for_child_outcome(outcome_label: str) -> SpeechRole:
    """A retained/still-pending outcome must be spoken as a timeout notice so
    it stays supersedable; every other outcome speaks as a normal result."""
    return ROLE_TIMEOUT_NOTICE if outcome_label == "retained" else ROLE_RESULT


def _work_status_for_outcome(
    outcome_label: str | None,
    *,
    cancelled: bool = False,
    committed: bool = True,
    terminal_kind: str | None = None,
) -> tuple[WorkStatusState, TerminalReason | None] | None:
    """The single outcome -> coarse work-status derivation for both sites.

    Used by the foreground delegated-child finalization path and by
    ``SessionHost.commit_late_result_once``'s finalization block, so the two
    can never drift apart. Precedence mirrors the commit contract:
    cancellation classifies the work item regardless of whether its result
    additionally cleared the commit fences; otherwise an outcome that both
    committed and delivered a real committed result -- ``completed``, or a
    worker-raised ``clarify``/``declined`` (both commit and speak a canonical
    result of their own, so the work item is just as done as a completed
    search) -- is ``result_ready``, and everything else terminal is
    ``failed``. ``None`` means "no status to emit" (no outcome was ever
    assigned). ``terminal_reason`` is carried only for a retention-rejected
    terminal, the sole reason either site can name.
    """
    if cancelled:
        return "cancelled", None
    if outcome_label is None:
        return None
    if outcome_label in {"completed", "clarify", "declined"} and committed:
        return "result_ready", None
    return "failed", ("retention_rejected" if terminal_kind == "retention_rejected" else None)


def build_pipeline(*, transport: Any, stt: Any, tts: Any) -> LabPipeline:
    """Compose the connection-local bridge, canonical adapter, and speech seams."""
    bus = _ProbeBus() if _ProbeBus is not None else None
    bridge = (
        framework_bridge(bus=bus, worker_name="contract-pipeline")
        if bus is not None
        else _FallbackBridge()
    )
    return LabPipeline(
        transport=transport,
        stt=stt,
        tts=tts,
        processors=(stt, bridge, CanonicalResultAdapter(), tts),
    )


@dataclass
class ConnectionPipeline:
    epoch: int
    observer: RuntimeObserver
    scheduler: SpeechScheduler
    lifecycle: SpeechLifecycleCoordinator | None = None
    stt: Any | None = None
    tts: Any | None = None
    transport: Any | None = None
    worker: Any | None = None
    worker_task: asyncio.Task[Any] | None = None
    output_teardown: Callable[[], Any] | None = None
    on_transcript: Callable[[str], Any] | None = None
    active: bool = True

    @property
    def capabilities(self) -> frozenset[str]:
        """Normalized capability set bound immutably to this connection's
        promoted epoch (Phase 3).

        Read straight off the ``RuntimeObserver`` constructed from the
        promoted ``Connection``'s set, so entitlement lives in exactly one
        place: a mirrored copy here could drift from the set the observer
        actually filters with.
        """
        return self.observer.capabilities

    @property
    def supports_work_status(self) -> bool:
        return self.observer.supports_work_status

    def deactivate(self, *, reconnect: bool = True) -> None:
        self.active = False
        self.scheduler.interrupt(epoch=self.epoch, reconnect=reconnect)

    async def shutdown(self, *, reason: str = "connection replaced") -> None:
        """Fence this connection and stop its Pipecat worker, if attached.

        Always forces scheduler cleanup, even if something upstream (e.g. a
        failed output teardown) already set ``active = False`` directly
        without releasing the scheduler's active lease.
        """
        self.deactivate(reconnect=reason == "connection replaced")
        if self.worker is not None:
            cancel = getattr(self.worker, "cancel", None)
            if cancel is not None:
                try:
                    result = cancel(reason=reason)
                    if hasattr(result, "__await__"):
                        await result
                except Exception:  # noqa: BLE001  # intentional catch-all: worker cancellation failures must not block connection teardown
                    logger.debug(f"worker cancel raised during shutdown for {reason}")
            self.worker = None
        if self.worker_task is not None:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except BaseException:  # noqa: BLE001  # intentional catch-all: awaiting a cancelled task can raise CancelledError/other BaseException; teardown must proceed regardless
                logger.debug("worker task raised while awaiting cancellation during shutdown")
            finally:
                self.worker_task = None
        self.observer.unsubscribe()
        for service in (self.stt, self.tts):
            cleanup = getattr(service, "cleanup", None)
            if cleanup is None:
                continue
            try:
                result = cleanup()
                if inspect.isawaitable(result):
                    await result
            except Exception:  # noqa: BLE001  # intentional catch-all: a single service's cleanup failure must not block teardown of the other services
                logger.debug(f"{service} cleanup raised during shutdown")
        if self.lifecycle is not None:
            self.lifecycle.connection_closed()


SearchExecutionStatus = Literal["completed", "retained", "capacity_rejected", "retention_rejected"]


@dataclass(frozen=True)
class SearchExecution:
    """Result of a foreground search and any ownership transfer."""

    status: SearchExecutionStatus
    result: GroundedResult | None = None


@dataclass(frozen=True)
class LateDeliveryContext:
    """Immutable identity captured at late-task dispatch, consumed at commit.

    Retained across the coordinator's ``on_complete`` closure boundary
    (``retain_late_task``'s callback is already a caller-supplied closure --
    see ``WorkItemCoordinator.retain_late_task``/``:_search_with_timeout`` --
    so this rides that existing closure rather than adding a parameter to
    the coordinator's callback contract). ``accepted_turn_sequence`` is
    ``SessionHost._turn_sequence`` as observed at dispatch time; comparing it
    against the live counter at commit time is how ``commit_late_result_once``
    detects that a newer semantic turn has since been accepted.
    """

    turn_id: str
    work_item_id: str
    origin_epoch: int
    ack_timestamp: float | None
    accepted_turn_sequence: int
    # The turn-level parent work item for a multi-intent child, so a late
    # child's work_status keys off the same WorkStatusKey as its siblings
    # (SessionState.set_child_work_status falls back to work_item_id when
    # this is None, which is correct for a single-intent turn where the
    # dispatched work item *is* the parent).
    parent_work_item_id: str | None = None


class SessionHost:
    """Process-lifetime host; persistent workers outlive connection pipelines."""

    _MAX_HANDSHAKE_TOKENS = 32

    def __init__(
        self,
        registry: WorkerRegistry | None = None,
        runner_factory: Callable[[], Any] | None = None,
        stt: Any | None = None,
        tts: Any | None = None,
        coordinator: Any | None = None,
        *,
        measurement_sink: MeasurementSink | None = None,
        config: Config | None = None,
        feature_policy: FeaturePolicy | None = None,
        promotion_manifest: PromotionManifest | None = None,
    ) -> None:
        self.state = SessionState()
        self.arbiter = ConnectionArbiter(self.state.session_id, self.state.resume_token)
        coordinator_registry = getattr(coordinator, "registry", None)
        if registry is None and coordinator_registry is not None:
            registry = coordinator_registry
        self.registry = registry or WorkerRegistry()
        if coordinator_registry is not None and coordinator_registry is not self.registry:
            raise ValueError("SessionHost and coordinator must share one WorkerRegistry")
        registry_config = getattr(self.registry, "config", None)
        if config is not None and registry_config is not None and registry_config != config:
            raise ValueError("SessionHost config conflicts with the registry's Config")
        self.config = config or registry_config or Config()
        # The coordinator is the other config holder in this object graph; a
        # divergent one is a split-brain, since the host reads every switch
        # (timeouts included) off ``self.config``. Fail fast at construction
        # rather than letting two Configs drive one session. WorkItemCoordinator's
        # own constructor is allowed to override max_work_items_per_turn and
        # multi_intent_wait_timeout_ms onto whatever Config it was given (see
        # its docstring/tests), so those two fields are coordinator-owned and
        # excluded from this comparison; every other field must still match.
        coordinator_config = getattr(coordinator, "config", None)
        if coordinator_config is not None:
            comparable_coordinator_config = replace(
                coordinator_config,
                max_work_items_per_turn=self.config.max_work_items_per_turn,
                multi_intent_wait_timeout_ms=self.config.multi_intent_wait_timeout_ms,
            )
            if comparable_coordinator_config != self.config:
                raise ValueError("SessionHost config conflicts with the coordinator's Config")
        self.feature_policy = feature_policy or FeaturePolicy.from_config(self.config)
        # The immutable evidence-gate verdict handed in by _default_session_host
        # (via server.config.load_promotion_manifest); missing/None is treated
        # exactly like a manifest that failed to load -- fail-closed to
        # display-only, never a boot-time error.
        self._promotion_manifest = promotion_manifest
        self.runner_factory = runner_factory
        self.stt, self.tts = stt, tts
        self.coordinator = coordinator
        self._tts_on_event = getattr(tts, "on_event", None)
        self.runner: Any = None
        self._runner_handles: dict[str, Any] = {}
        self._runner_registered: set[str] = set()
        self._runner_registrations: dict[str, asyncio.Task[None]] = {}
        self._runner_task: asyncio.Task[Any] | None = None
        self.connection: ConnectionPipeline | None = None
        self._background_shutdowns: set[asyncio.Task[None]] = set()
        self._handshake_tokens: dict[str, tuple[int, float, bool]] = {}
        self._turn_sequence = 0
        self._inflight_turn_tasks: dict[str, asyncio.Task[Any]] = {}
        self._inflight_work_tasks: dict[str, set[asyncio.Task[Any]]] = {}
        self._known_work_items: set[str] = set()
        self._cancelled_work_items: set[str] = set()
        self._clarification_candidates: dict[str, dict[str, str]] = {}
        self._retained_recorders: dict[str, RetainedRecorder] = {}
        self._measurement_sink: MeasurementSink = measurement_sink or ConsoleMeasurementSink()
        self._closing = False
        self.started = False
        # Turn-scoped acknowledgement latch (Requirements: one ephemeral ack
        # per semantic turn, including mixed multi-intent turns). Cleared in
        # each turn's normal completion/cancellation/failure cleanup -- never
        # retained as an unbounded process-lifetime set.
        self._ack_emitted_turns: set[str] = set()
        # Keep deferred ack-admission tasks referenced so they aren't
        # garbage-collected mid-flight; see _schedule_ack_admission.
        self._ack_admission_tasks: set[asyncio.Task[Any]] = set()

    @property
    def measurement_sink(self) -> MeasurementSink:
        """The sink resolved exactly once for this host's lifetime.

        Read-only so no caller can swap it mid-session, per the Measurement
        Sink Contract in the latency observability dev plan.
        """
        return self._measurement_sink

    async def start(self) -> None:
        if self.started:
            return
        self._closing = False
        if self.runner_factory is not None:
            self.runner = self.runner_factory()
        else:
            from pipecat.pipeline.runner import WorkerRunner

            self.runner = WorkerRunner(name="websearch-session", handle_sigint=False)
        await self._register_persistent_workers()
        start = getattr(self.runner, "start", None)
        if start is not None:
            result = start()
            if hasattr(result, "__await__"):
                await result
        else:
            run = getattr(self.runner, "run", None)
            if run is not None:
                self._runner_task = asyncio.create_task(run(auto_end=False))
        self.state.active_epoch = None
        self.started = True

    async def _register_persistent_workers(self) -> None:
        """Register durable contexts with the runner when the API can accept them.

        Pipecat 1.6.0 does not expose the planned ``LLMContextWorker`` module;
        the lab's ContextWorker uses the pinned BaseWorker bus lifecycle instead.
        Test registries and runners without ``add_workers`` are left untouched.
        """
        add_workers = getattr(self.runner, "add_workers", None)
        if add_workers is None:
            return
        try:
            from pipecat.workers.base_worker import BaseWorker
        except ImportError:
            return

        for registered in self.registry.workers:
            if isinstance(registered.worker, BaseWorker):
                await self._register_runner_worker(registered.worker)

    async def _register_runner_worker(self, worker: Any) -> None:
        metadata = getattr(worker, "metadata", None)
        worker_id = getattr(metadata, "worker_id", None) or getattr(worker, "name", None)
        if not isinstance(worker_id, str) or not worker_id:
            return
        if worker_id in self._runner_registered:
            return
        add_workers = getattr(self.runner, "add_workers", None)
        if add_workers is None:
            return
        registration = self._runner_registrations.get(worker_id)
        if registration is None:

            async def register() -> None:
                result = add_workers(worker)
                if inspect.isawaitable(result):
                    await result
                self._runner_handles[worker_id] = worker
                self._runner_registered.add(worker_id)

            registration = asyncio.create_task(register())
            self._runner_registrations[worker_id] = registration

            def completed(completed_task: asyncio.Task[Any]) -> None:
                if self._runner_registrations.get(worker_id) is completed_task:
                    self._runner_registrations.pop(worker_id, None)

            registration.add_done_callback(completed)
        await asyncio.shield(registration)

    async def connect(self, handshake: Any) -> ConnectionPipeline:
        if self._closing:
            raise RuntimeError("session host is shutting down")
        if not self.started:
            await self.start()
        connection = self.arbiter.promote(handshake)
        old_connection = self.connection
        # Publish the new authority before awaiting any old transport cleanup.
        self.state.active_epoch = connection.epoch
        pipeline: ConnectionPipeline
        connection_stt = self._connection_service(self.stt)
        connection_tts = self._connection_service(self.tts)

        async def queue_speech(item: Any) -> None:
            if (
                connection_tts is None
                or self.connection is not pipeline
                or not pipeline.active
                or item.origin_epoch != pipeline.epoch
                or not self.accepts(pipeline.epoch)
            ):
                raise RuntimeError("speech target is not the active TTS connection")
            if pipeline.worker is None:
                raise RuntimeError("active connection has no Pipecat worker for TTS")
            lease = pipeline.scheduler.active
            if pipeline.lifecycle is not None and lease is not None:
                await pipeline.worker.queue_frame(
                    SpeechGenerationMarkerFrame(
                        token=lease.token,
                        utterance_id=item.utterance_id,
                        work_item_id=item.work_item_id,
                        origin_epoch=item.origin_epoch,
                    )
                )
            frame_factory = getattr(connection_tts, "correlated_speak_frame", None)
            frame = (
                frame_factory(
                    item.text,
                    correlation_id=item.utterance_id,
                    append_to_context=False,
                )
                if frame_factory is not None
                else TTSSpeakFrame(text=item.text, append_to_context=False)
            )
            await pipeline.worker.queue_frame(frame)

        async def stop_speech(item: Any) -> None:
            if connection_tts is None or self.connection is not pipeline or pipeline.worker is None:
                return
            token = pipeline.lifecycle.slot_token if pipeline.lifecycle is not None else None
            generation = (
                pipeline.lifecycle.generation_for_token(token)
                if pipeline.lifecycle is not None and token is not None
                else None
            )
            if item is not None and (
                generation is None or generation.identity.utterance_id != item.utterance_id
            ):
                token = None
            await pipeline.worker.queue_frame(InterruptionFrame())
            if token is not None:
                await pipeline.worker.queue_frame(SpeechGenerationFlushAckFrame(token=token))

        def schedule_pipeline_shutdown(reason: str) -> None:
            task = asyncio.create_task(pipeline.shutdown(reason=reason))
            self._background_shutdowns.add(task)
            task.add_done_callback(self._background_shutdowns.discard)

        async def on_lifecycle_terminal(
            token: str, identity: GenerationIdentity, disposition: DeliveryDisposition
        ) -> None:
            del token
            if self.connection is not pipeline:
                return
            if disposition == DeliveryDisposition.DELIVERY_UNKNOWN:
                pipeline.scheduler.delivery_unknown(identity.utterance_id)
            if pipeline.active and pipeline.scheduler.active is None:
                await pipeline.scheduler.start_next()

        async def dispatch_lifecycle_cleanup(token: str, identity: GenerationIdentity) -> None:
            del token, identity
            await stop_speech(None)

        async def dispatch_lifecycle_teardown(token: str, identity: GenerationIdentity) -> None:
            del identity
            # Fence this connection before any await or fallback. If the
            # physical output barrier is unavailable/fails, shutdown still
            # prevents another generation from entering this lane.
            pipeline.active = False
            teardown = pipeline.output_teardown
            if teardown is None:
                logger.error("speech output teardown unavailable; shutting down the speech lane")
                schedule_pipeline_shutdown("speech output teardown failed")
                return

            # SmallWebRTCConnection.disconnect() does not return until its
            # tracks and peer connection have been closed, so no fieldless
            # stop from this lane can arrive after teardown_complete().
            try:
                result = teardown()
                if inspect.isawaitable(result):
                    await result
            except Exception:  # noqa: BLE001  # fail closed: never acknowledge an unconfirmed output teardown
                logger.exception("speech output teardown failed; retaining the lifecycle barrier")
                schedule_pipeline_shutdown("speech output teardown failed")
                return

            if pipeline.lifecycle is not None:
                await pipeline.lifecycle.teardown_complete(token)
            schedule_pipeline_shutdown("speech output teardown")

        def transport_acceptable() -> bool:
            return self.connection is pipeline and pipeline.active

        connection_config = self.config
        # Every connection constructs one SpeechLifecycleCoordinator,
        # including when connection_tts is None: the no-TTS/unavailable-
        # transport cases are decided by pre_admission_disposition() rather
        # than by never constructing a coordinator at all.
        lifecycle = SpeechLifecycleCoordinator(
            clock=MonotonicClock(),
            timers=EventLoopTimerScheduler(),
            speech_start_timeout_seconds=connection_config.speech_start_timeout_seconds,
            speech_transport_grace_seconds=connection_config.speech_transport_grace_seconds,
            on_terminal=on_lifecycle_terminal,
            dispatch_cleanup=dispatch_lifecycle_cleanup,
            dispatch_teardown=dispatch_lifecycle_teardown,
            tts_available=connection_tts is not None,
            transport_acceptance=transport_acceptable,
        )

        pipeline = ConnectionPipeline(
            connection.epoch,
            RuntimeObserver(self.state, connection.epoch, frozenset(connection.capabilities)),
            SpeechScheduler(
                self.state,
                speak=queue_speech if connection_tts is not None else None,
                stop=stop_speech if connection_tts is not None else None,
                lifecycle=lifecycle,
                on_ack_terminal=self.on_ack_terminal,
            ),
            lifecycle=lifecycle,
            stt=connection_stt,
            tts=connection_tts,
        )
        if connection_stt is not None and self.coordinator is not None:

            async def on_final(text: str) -> Any:
                if self.connection is not pipeline or not pipeline.active:
                    return None
                return await self._handle_transcript(text, origin=pipeline)

            pipeline.on_transcript = on_final
        if connection_tts is not None and hasattr(connection_tts, "on_event"):

            async def on_tts_event(event: str, context_id: str) -> Any:
                callback_result = None
                if self._tts_on_event is not None:
                    callback_result = self._tts_on_event(event, context_id)
                    if inspect.isawaitable(callback_result):
                        callback_result = await callback_result
                current = self.connection is pipeline and pipeline.active
                has_lifecycle = pipeline.lifecycle is not None
                if event == "synthesis_started":
                    pipeline.scheduler.provider_started(context_id)
                elif event == "synthesis_ended" and current:
                    pipeline.scheduler.provider_synthesis_ended(context_id)
                    # Non-terminal: the real TTSStoppedFrame observed by
                    # TransportSpeechLifecycleProcessor arms the coordinator's
                    # drain deadline. Without a coordinator, fall back to the
                    # old conservative immediate release so later utterances
                    # cannot be starved.
                    if not has_lifecycle:
                        pipeline.scheduler.provider_delivery_unknown(context_id)
                elif event == "delivery_completed" and current:
                    # Same has_lifecycle gate as synthesis_ended above: with a
                    # coordinator installed, only its own token-bearing
                    # transport/tombstone barriers may release the slot.
                    if not has_lifecycle:
                        pipeline.scheduler.provider_delivery_completed(context_id)
                elif event == "delivery_unknown" and current:
                    lifecycle = pipeline.lifecycle
                    token = (
                        lifecycle.token_for_context(context_id) if lifecycle is not None else None
                    )
                    if lifecycle is not None and token is not None:
                        await lifecycle.provider_error(token)
                    else:
                        pipeline.scheduler.provider_delivery_unknown(context_id)
                if (
                    not has_lifecycle
                    and event in {"synthesis_ended", "delivery_completed", "delivery_unknown"}
                    and self.connection is pipeline
                    and pipeline.active
                    and pipeline.scheduler.active is None
                ):
                    await pipeline.scheduler.start_next()
                return callback_result

            connection_tts.on_event = on_tts_event
        self.connection = pipeline
        if old_connection is not None:
            old_connection.deactivate()
            task = asyncio.create_task(old_connection.shutdown(reason="connection replaced"))
            self._background_shutdowns.add(task)
            task.add_done_callback(self._background_shutdowns.discard)
        await self._register_persistent_workers()
        return pipeline

    @staticmethod
    def _connection_service(service: Any | None) -> Any | None:
        if service is None:
            return None
        factory = getattr(service, "for_connection", None)
        return factory() if factory is not None else service

    def _next_turn_id(self) -> str:
        self._turn_sequence += 1
        return f"turn-{self._turn_sequence}"

    def _new_app_turn_recorder(self, *, origin_epoch: int, turn_id: str) -> AppTurnRecorder:
        return AppTurnRecorder(
            self.measurement_sink,
            session_id=self.state.session_id,
            origin_epoch=origin_epoch,
            turn_id=turn_id,
        )

    def _new_retained_recorder(
        self, *, origin_epoch: int, turn_id: str, work_item_id: str, app_worker_id: str
    ) -> RetainedRecorder:
        return RetainedRecorder(
            self.measurement_sink,
            session_id=self.state.session_id,
            origin_epoch=origin_epoch,
            turn_id=turn_id,
            work_item_id=work_item_id,
            app_worker_id=app_worker_id,
        )

    def _register_retained_recorder_if_open(
        self, work_item_id: str, recorder: RetainedRecorder
    ) -> None:
        """Register a provisional retained recorder only if it has not already
        raced to completion between dispatch and accepted retention."""
        if not recorder.finalized:
            self._retained_recorders[work_item_id] = recorder

    @staticmethod
    def _make_late_terminal_handler(
        recorders: Mapping[str, RetainedRecorder],
    ) -> Callable[[str, WorkOutcome], None]:
        """Build a coordinator ``on_late_terminal`` callback that claims the
        matching retained recorder, if any, for a late-completing work item.

        Typed against the wider ``WorkOutcome`` rather than the coordinator's
        narrower ``TerminalKind``: by contravariance this still satisfies the
        coordinator's hook type, while remaining assignable wherever a full
        work outcome (including ``invalid_result``) is claimed.
        """

        def on_late_terminal(item_id: str, terminal_kind: WorkOutcome) -> None:
            recorder = recorders.get(item_id)
            if recorder is not None:
                recorder.claim(terminal_kind)

        return on_late_terminal

    @staticmethod
    def _failure_child_outcome(failure: WorkItemFailure) -> WorkItemOutcome:
        """Classify a work-item failure from its structured ``failure_kind``.

        ``error_type`` is free-text diagnostic and must never drive
        classification: renaming a worker exception class would otherwise
        silently reclassify its telemetry outcome.

        The ``failure_kind in FAILURE_KINDS`` check below is redundant against
        the static type but not against runtime: ``SessionHost.coordinator`` is
        an injected ``Any``, so ``submitted.failures`` is unchecked at both call
        sites and a duck-typed coordinator can supply an off-domain kind. Left
        unguarded it would reach the enum-validated ``outcome`` field and the
        whole record would be silently dropped instead of downgraded.
        """
        kind = failure.failure_kind
        if kind in FAILURE_KINDS:
            return kind
        logger.warning(
            f"work item {failure.work_item_id} carries unclassified "
            f"failure_kind={kind!r}; recording outcome=failed"
        )
        return "failed"

    def validate_handshake_token(self, token: str, proposed_epoch: int, *, redeem: bool) -> bool:
        self._prune_handshake_tokens()
        entry = self._handshake_tokens.get(token)
        if entry is None:
            return False
        epoch, expires_at, redeemed = entry
        if expires_at <= time.monotonic() or epoch != proposed_epoch:
            self._handshake_tokens.pop(token, None)
            return False
        if redeem:
            if redeemed:
                return False
            self._handshake_tokens[token] = (epoch, expires_at, True)
            return True
        return redeemed

    def validate_patch_handshake(self, connection: Any, handshake: Any) -> None:
        """Enforce immutable capability binding for a PATCH ICE-candidate request.

        An omitted ``capabilities`` field inherits the POST-bound set; a
        present field must match it exactly (post-normalization) or the
        request is rejected. Neither branch mutates ``connection`` or its
        already-constructed ``RuntimeObserver``: capability entitlement is
        immutable for the life of a promoted epoch (Requirements).
        """
        if not getattr(handshake, "capabilities_present", False):
            return
        bound = frozenset(getattr(connection, "capabilities", ()))
        presented = frozenset(getattr(handshake, "capabilities", ()))
        if presented != bound:
            raise ValueError("capabilities cannot change after connection promotion")

    def abort_connection(self, pipeline: ConnectionPipeline) -> None:
        """Fence a promoted connection whose transport setup did not complete."""
        if self.connection is pipeline:
            pipeline.deactivate()
            self.connection = None
            self.state.active_epoch = None

    def _clear_ack_latch(self, turn_id: str) -> None:
        self._ack_emitted_turns.discard(turn_id)

    def _ack_turn_for_work_item(self, work_item_id: str) -> str | None:
        """The latched semantic turn that owns ``work_item_id``, if any.

        A control message ("stop that") arrives as its own semantic turn, so
        the turn being cancelled is never the control turn: the ack that must
        be settled belongs to the *target child's* turn. Delegated children
        are named ``work-{turn_id}`` (single intent) or ``work-{turn_id}-{i}``
        (multi-intent) at their two creation sites, so matching against the
        live latch set resolves the owner without a second registry and
        without the ambiguity of parsing a turn id back out of the string.
        """
        for candidate in self._ack_emitted_turns:
            if work_item_id == f"work-{candidate}" or work_item_id.startswith(f"work-{candidate}-"):
                return candidate
        return None

    def on_ack_terminal(
        self, identity: GenerationIdentity, reason: PreAdmissionTerminalReason
    ) -> None:
        """Idempotent sole mutator for the pre-admission-terminal ack path.

        Injected into ``SpeechScheduler`` at connection setup; invoked only
        when an ack is terminalized before admission (``no_tts`` /
        ``unavailable_transport``), never for a normal admitted completion.
        """
        del reason
        if identity.turn_id is not None:
            self._clear_ack_latch(identity.turn_id)

    async def _emit_early_ack(
        self,
        origin: ConnectionPipeline,
        *,
        turn_id: str,
        origin_epoch: int,
        search_task: asyncio.Task[Any] | None = None,
    ) -> None:
        """Enqueue this turn's one delegation-confirmed ack.

        Latches at the moment the ack is enqueued, so later eligible
        multi-intent children in the same turn are no-ops (Requirements: one
        ephemeral ack per semantic turn, never one per child, never claiming
        progress that hasn't happened). A child that bails before enqueueing
        does not consume the turn's one ack slot -- otherwise a turn whose
        first child had no TTS (or resolved instantly) would be permanently
        ack-less. When ``search_task`` is supplied, this yields exactly one
        scheduling tick first: a search that already resolved within that
        tick has no real delegation latency to acknowledge, so no ack is
        queued at all ('without claiming progress that hasn't happened').
        """
        if not self.feature_policy.enable_early_ack or turn_id in self._ack_emitted_turns:
            return
        if (
            origin.tts is None
            or self.connection is not origin
            or not origin.active
            or not self.accepts(origin_epoch)
        ):
            return
        if search_task is not None:
            await asyncio.sleep(0)
            if search_task.done():
                return
        ack_work_item_id = f"ack-{turn_id}"

        def enqueue_ack() -> None:
            origin.scheduler.enqueue(
                result_id=None,
                work_item_id=ack_work_item_id,
                run_id=f"run-{ack_work_item_id}",
                text=self.config.early_ack_text,
                origin_epoch=origin_epoch,
                role=ROLE_ACK,
                ack_id=ack_work_item_id,
                turn_id=turn_id,
            )

        # Latch only on the path that actually enqueues: a child that bailed
        # above (no TTS, superseded origin, stale epoch, or a search that
        # resolved within one tick) never spent this turn's single ack slot,
        # so a later genuinely-slow child of the same turn can still claim it.
        self._ack_emitted_turns.add(turn_id)
        enqueue_ack()
        self._schedule_ack_admission(origin, ack_work_item_id, enqueue_ack)

    def _schedule_ack_admission(
        self,
        origin: ConnectionPipeline,
        ack_work_item_id: str,
        enqueue_ack: Callable[[], None],
    ) -> None:
        """Admit a just-enqueued ack on a later scheduling tick, not inline.

        The caller's enqueue is synchronous and immediate -- the ack is
        visible in the scheduler's own bookkeeping the instant delegation is
        confirmed. Admission is deferred so a same-turn reconnect, or a
        result that is already ready, can still discard the ack while it is
        merely queued (``discard_queued_ack`` / ``SpeechScheduler.interrupt``)
        before it is ever handed to the transport.
        """

        async def admit() -> None:
            try:
                await origin.scheduler.start_next(ack_work_item_id)
            except Exception:  # noqa: BLE001 - ack submission failure must never crash the turn
                # The ack is ephemeral and best-effort: a submission failure
                # (e.g. the connection's Pipecat worker has not attached
                # yet) must never crash the turn's real delegated work.
                # start_next's own except-path already released the
                # coordinator slot and discarded the item from scheduler
                # bookkeeping entirely, so re-queue a fresh ack item rather
                # than leaving none at all.
                logger.opt(exception=True).debug(
                    "early ack failed to start; leaving it queued for a later retry"
                )
                enqueue_ack()

        task = asyncio.create_task(admit())
        self._ack_admission_tasks.add(task)
        task.add_done_callback(self._ack_admission_tasks.discard)

    async def cancel_turn_or_child(
        self,
        turn_id: str,
        child_work_item_id: str | None = None,
        *,
        origin: ConnectionPipeline | None = None,
        exclude_work_item_id: str | None = None,
    ) -> tuple[tuple[str, ...], tuple[SpeechItem, ...]]:
        """Host-owned atomic cancellation of a turn's ack and/or one child.

        A child cancel never accidentally removes the parent ack unless it
        was the turn's sole remaining delegated child; a whole-turn cancel
        (``child_work_item_id is None``) removes the ack and every other
        queued/active work item this connection is currently carrying.
        Synchronous and non-awaiting apart from the scheduler's own stop
        signalling, so nothing else can interleave between child removal and
        the sole-child ack check.

        ``exclude_work_item_id`` protects the caller's own in-flight turn task
        from the sweep. Returns the ``(cancelled_work, cancelled_speech)``
        pair for the *requested* target only: the parent ack is ephemeral and
        never counts towards a caller's "did this cancel hit anything?"
        decision.
        """
        origin = origin or self.connection
        ack_work_item_id = f"ack-{turn_id}"
        if origin is None:
            self._clear_ack_latch(turn_id)
            return (), ()
        scheduler = origin.scheduler
        if child_work_item_id is None:
            cancelled_work = self._cancel_work(None, exclude_work_item_id=exclude_work_item_id)
            cancelled_speech = scheduler.cancel(None)
            self._clear_ack_latch(turn_id)
            for item in cancelled_speech:
                # A whole-turn sweep removes every live ack, including acks
                # belonging to earlier turns; their latches go with them.
                if item.role == ROLE_ACK and item.turn_id is not None:
                    self._clear_ack_latch(item.turn_id)
            return cancelled_work, cancelled_speech
        cancelled_work = self._cancel_work(
            child_work_item_id, exclude_work_item_id=exclude_work_item_id
        )
        cancelled_speech = scheduler.cancel(child_work_item_id)
        remaining = set(scheduler.pending_work_item_ids(exclude=ack_work_item_id))
        if scheduler.active is not None and scheduler.active.item.work_item_id != ack_work_item_id:
            remaining.add(scheduler.active.item.work_item_id)
        if not remaining:
            scheduler.cancel(ack_work_item_id)
            self._clear_ack_latch(turn_id)
        return cancelled_work, cancelled_speech

    def _require_coordinator(self) -> Any:
        coordinator = self.coordinator
        if coordinator is None:
            raise RuntimeError("coordinator is required to execute work")
        return coordinator

    async def _handle_transcript(
        self, transcript: str, *, origin: ConnectionPipeline | None = None
    ) -> Any:
        task = asyncio.create_task(self._handle_transcript_impl(transcript, origin=origin))
        return await task

    async def _handle_transcript_impl(
        self, transcript: str, *, origin: ConnectionPipeline | None = None
    ) -> Any:
        """Route a final local-STT turn through the application coordinator."""
        origin = origin or self.connection
        if (
            self.coordinator is None
            or origin is None
            or self.connection is not origin
            or not origin.active
            or not self.accepts(origin.epoch)
        ):
            return transcript
        origin_epoch = origin.epoch
        turn_id = self._next_turn_id()
        work_item_id = f"work-{turn_id}"
        turn_recorder = self._new_app_turn_recorder(origin_epoch=origin_epoch, turn_id=turn_id)
        # Delegated children that have had a work_status emitted, so a
        # cancellation of this turn can terminalize whichever are still open.
        delegated_children: dict[str, str | None] = {}
        try:
            turn_task = asyncio.current_task()
            if turn_task is not None:
                self._track_turn_task(work_item_id, turn_task)
            self.state.append_transcript(
                TranscriptEntry(
                    role="user",
                    text=transcript,
                    turn_id=turn_id,
                    origin_epoch=origin_epoch,
                )
            )
            try:
                routing_started = time.perf_counter()
                outcome = await asyncio.to_thread(
                    self.coordinator.arbitrate, self.state.session_id, transcript
                )
                routing_ms = (time.perf_counter() - routing_started) * 1000
                turn_recorder.record_routing(routing_ms)
            except Exception as exc:  # noqa: BLE001  # intentional catch-all: routing can raise arbitrary provider/model errors that must fall back to a safe result
                logger.warning(
                    f"Routing failed for {turn_id}; returning a safe result "
                    f"without provider details: {type(exc).__name__}"
                )
                result = await self._commit_and_speak(
                    canonical_result(
                        worker_id="main",
                        turn_id=turn_id,
                        text="Routing is temporarily unavailable. Please try that request again.",
                        origin_epoch=origin_epoch,
                    ),
                    origin,
                )
                turn_recorder.finalize(outcome="failed")
                return result
            if outcome.kind != "routed" or outcome.decision is None:
                control_action: ControlAction | None = None
                control_outcome: ControlOutcome | None = None
                ack_override: str | None = None
                if outcome.kind == "control":
                    # Explicit ``Any`` (not inferred): getattr's 3-arg overload with a
                    # literal ``None`` default types as ``Any | None``, and that ``| None``
                    # is not assignable where a Literal-keyed dict is looked up below.
                    action: Any = getattr(outcome, "control_action", None)
                    control_action = action
                    control_outcome = (
                        "no_active" if action in {"pause", "resume"} else "unknown_target"
                    )
                    if action == "pause" and origin.scheduler.active is not None:
                        target = (
                            outcome.work_items[0]
                            if outcome.work_items
                            else origin.scheduler.active.item.work_item_id
                        )
                        active_lease: UtteranceLease | None = origin.scheduler.active
                        if (
                            origin.lifecycle is not None
                            and active_lease is not None
                            and active_lease.item.work_item_id == target
                        ):
                            origin.lifecycle.record_interruption(active_lease.token, pause=True)
                        origin.scheduler.pause(target)
                        await origin.scheduler.wait_for_stops()
                        control_outcome = "applied"
                    elif action == "resume":
                        target = outcome.work_items[0] if outcome.work_items else None
                        replay = origin.scheduler.resume(target)
                        if replay is not None:
                            await origin.scheduler.start_next(replay.work_item_id)
                            control_outcome = "applied"
                    elif action in {"cancel", "stop"}:
                        target = outcome.work_items[0] if outcome.work_items else None
                        active_lease = origin.scheduler.active
                        if (
                            origin.lifecycle is not None
                            and active_lease is not None
                            and (target is None or active_lease.item.work_item_id == target)
                        ):
                            origin.lifecycle.record_interruption(active_lease.token, pause=False)
                        # Route every explicit cancel/stop through the host's
                        # cancellation boundary so a targeted child cancel also
                        # settles this turn's parent ack (and its latch) when
                        # that child was the sole remaining delegated child --
                        # an ack must never be spoken after the user cancelled
                        # the work it was acknowledging.
                        cancel_turn_id = (
                            self._ack_turn_for_work_item(target) or turn_id
                            if target is not None
                            else turn_id
                        )
                        cancelled_work, cancelled_speech = await self.cancel_turn_or_child(
                            cancel_turn_id,
                            target,
                            origin=origin,
                            exclude_work_item_id=work_item_id,
                        )
                        await origin.scheduler.wait_for_stops()
                        if not cancelled_work and not cancelled_speech:
                            control_outcome = (
                                "unknown_target" if target is not None else "no_active"
                            )
                            ack_override = _CONTROL_MISS_ACK_TEXT[control_outcome]
                        else:
                            control_outcome = "applied"
                    elif action == "consent":
                        control_outcome = "no_pending"
                    text = ack_override or _CONTROL_ACK_TEXT.get(action, "Control request noted.")
                elif outcome.kind == "multi_intent":
                    return await self._handle_multi_intent(
                        outcome, transcript, origin, turn_id, turn_recorder
                    )
                elif outcome.kind == "continue_pending":
                    return await self._handle_pending(
                        outcome, transcript, origin, turn_id, turn_recorder
                    )
                else:
                    text = None
                if text is None:
                    turn_recorder.finalize(outcome="failed")
                    return outcome
                result = await self._commit_and_speak(
                    canonical_result(
                        worker_id="main",
                        turn_id=turn_id,
                        text=text,
                        origin_epoch=origin_epoch,
                    ),
                    origin,
                )
                if control_action is None:
                    # ``outcome=control`` requires both control fields; a
                    # control turn that never named an action cannot produce a
                    # schema-valid control record, so it is recorded as failed
                    # rather than dropped. Spoken text is unaffected.
                    logger.warning(
                        f"Control turn {turn_id} carried no control_action; "
                        f"recording app_turn_foreground outcome=failed"
                    )
                    turn_recorder.finalize(outcome="failed")
                else:
                    turn_recorder.finalize(
                        outcome="control",
                        control_action=control_action,
                        control_outcome=control_outcome,
                    )
                return result
            if isinstance(outcome.decision, RoutingDecision):
                self.state.set_routing(
                    RoutingState(
                        turn_id=turn_id,
                        action=outcome.decision.action,
                        worker_id=outcome.decision.worker_id,
                        worker_type=outcome.decision.worker_type,
                        topic=outcome.decision.topic,
                        model_policy=outcome.decision.model_policy,
                        origin_epoch=origin_epoch,
                    )
                )
                logger.info(
                    f"Routing {turn_id}: action={outcome.decision.action}, "
                    f"worker={outcome.decision.worker_id or outcome.decision.worker_type or 'main'}"
                )
            action = getattr(outcome.decision, "action", None)
            if action in {"direct", "unsupported", "clarify"}:
                text = getattr(outcome, "prose", None)
                if not text:
                    text = {
                        "direct": "I could not produce a direct answer yet.",
                        "unsupported": "I cannot access that capability here.",
                        "clarify": "Could you clarify what you want me to search for?",
                    }[action]
                result = await self._commit_and_speak(
                    canonical_result(
                        worker_id="main",
                        turn_id=turn_id,
                        text=text,
                        origin_epoch=origin_epoch,
                    ),
                    origin,
                    require_tts=False,
                )
                turn_recorder.finalize(outcome=action)
                return result
            try:
                worker = self._dispatch(outcome.decision, getattr(outcome, "catalogue", None))
                await self._register_runner_worker(worker)
            except (RoutingValidationError, UnsupportedWorkerType):
                result = await self._commit_and_speak(
                    canonical_result(
                        worker_id="main",
                        turn_id=turn_id,
                        text="I cannot access that capability here.",
                        origin_epoch=origin_epoch,
                    ),
                    origin,
                )
                turn_recorder.finalize(outcome="failed")
                return result
            if worker is None:
                child = turn_recorder.new_child(work_item_id=work_item_id)
                child.finalize(outcome="missing_worker")
                turn_recorder.finalize(outcome="failed")
                self._emit_work_status(
                    turn_id=turn_id,
                    work_item_id=work_item_id,
                    state="failed",
                    origin_epoch=origin_epoch,
                    terminal_reason="missing_worker",
                )
                return outcome
            self._project_worker(worker, origin_epoch=origin_epoch, status="running")
            search = getattr(worker, "search", None)
            worker_id = getattr(getattr(worker, "metadata", None), "worker_id", "main")
            if search is None:
                child = turn_recorder.new_child(work_item_id=work_item_id)
                child.finalize(outcome="missing_search", app_worker_id=worker_id)
                turn_recorder.finalize(outcome="failed")
                self._emit_work_status(
                    turn_id=turn_id,
                    work_item_id=work_item_id,
                    worker_id=worker_id,
                    state="failed",
                    origin_epoch=origin_epoch,
                )
                return outcome
            child = turn_recorder.new_child(work_item_id=work_item_id)
            delegated_children[work_item_id] = worker_id
            self._emit_work_status(
                turn_id=turn_id,
                work_item_id=work_item_id,
                worker_id=worker_id,
                state="routing",
                origin_epoch=origin_epoch,
            )
            try:
                search_started = time.perf_counter()
                search_task = self._dispatch_search_task(
                    search, transcript, turn_id=turn_id, origin_epoch=origin_epoch
                )
                self._emit_work_status(
                    turn_id=turn_id,
                    work_item_id=work_item_id,
                    worker_id=worker_id,
                    state="searching",
                    origin_epoch=origin_epoch,
                )
                # Dispatch before the ack: a search that resolves within the
                # same tick (no real delegation latency) never gets an ack at
                # all, and one still in flight only gets an ack admitted via
                # a deferred task -- scheduled strictly after this search
                # task's own first step -- so a reconnect racing in on the
                # very same turn can still fence the still-queued ack before
                # it ever reaches the old connection's transport.
                await self._emit_early_ack(
                    origin,
                    turn_id=turn_id,
                    origin_epoch=origin_epoch,
                    search_task=search_task,
                )
                execution = await self._search_with_timeout(
                    search,
                    transcript,
                    turn_id=turn_id,
                    origin_epoch=origin_epoch,
                    timeout=self.config.foreground_search_timeout_seconds,
                    worker_id=worker_id,
                    work_item_id=work_item_id,
                    task=search_task,
                )
                search_ms = (time.perf_counter() - search_started) * 1000
                child_outcome_label: WorkItemOutcome
                if execution.status == "completed" and execution.result is not None:
                    result = execution.result
                    child_outcome_label = "completed"
                elif execution.status == "retained":
                    # A retained work item is still working, so `background` is
                    # its truthful status on every connection -- the observer,
                    # not this call site, decides which connections may see it.
                    # Deriving a status from the "retained" outcome label
                    # instead (below) would publish `failed`, and a terminal
                    # parent never regresses, so the late result could never
                    # flip it to `result_ready`.
                    self._emit_work_status(
                        turn_id=turn_id,
                        work_item_id=work_item_id,
                        worker_id=worker_id,
                        state="background",
                        origin_epoch=origin_epoch,
                    )
                    if origin.supports_work_status and self.feature_policy.enable_background_status:
                        # Phase 3 capability-gated path: retain the work item
                        # and let the `background` status stand alone instead
                        # of speaking a second canonical timeout result or
                        # writing a transcript/history entry (Requirements).
                        child.finalize(outcome="retained", app_worker_id=worker_id)
                        turn_recorder.finalize()
                        return execution
                    result = canonical_result(
                        worker_id=worker_id,
                        turn_id=turn_id,
                        text="That is taking longer than expected; I will continue in the background.",
                        origin_epoch=origin_epoch,
                    )
                    child_outcome_label = "retained"
                else:
                    result = canonical_result(
                        worker_id=worker_id,
                        turn_id=turn_id,
                        text="The search service is busy; please try again shortly.",
                        origin_epoch=origin_epoch,
                    )
                    child_outcome_label = (
                        execution.status
                        if execution.status in {"capacity_rejected", "retention_rejected"}
                        else "failed"
                    )
            except WorkerClarify as exc:
                search_ms = (time.perf_counter() - search_started) * 1000
                result = self._worker_clarification_result(
                    worker_id=worker_id,
                    turn_id=turn_id,
                    question=exc.question,
                    original_query=transcript,
                    origin_epoch=origin_epoch,
                )
                child_outcome_label = "clarify"
            except WorkerDeclined:
                search_ms = (time.perf_counter() - search_started) * 1000
                result = canonical_result(
                    worker_id=worker_id,
                    turn_id=turn_id,
                    text="I could not find a reliable result for that request.",
                    origin_epoch=origin_epoch,
                )
                child_outcome_label = "declined"
            except Exception as exc:  # noqa: BLE001  # intentional catch-all: search worker failures are arbitrary provider errors that must fall back to a safe result
                logger.warning(
                    f"Web search failed for {turn_id}; returning a safe result: "
                    f"{type(exc).__name__}"
                )
                search_ms = (time.perf_counter() - search_started) * 1000
                result = canonical_result(
                    worker_id=worker_id,
                    turn_id=turn_id,
                    text="The web search is temporarily unavailable.",
                    origin_epoch=origin_epoch,
                )
                child_outcome_label = "failed"
            was_cancelled = f"work-{result.turn_id}" in self._cancelled_work_items
            commit_started = time.perf_counter()
            speech_role = _speech_role_for_child_outcome(child_outcome_label)
            origin.scheduler.discard_queued_ack(f"ack-{turn_id}")
            # A retained child is not terminal: its truthful `background`
            # status was already emitted above and the coordinator terminalizes
            # it when the late result lands. Only an actual cancellation
            # settles it here.
            retained_still_open = child_outcome_label == "retained" and not was_cancelled
            if not retained_still_open:
                derived = _work_status_for_outcome(
                    child_outcome_label,
                    cancelled=was_cancelled,
                    terminal_kind=child_outcome_label,
                )
                if derived is not None:
                    status_state, status_reason = derived
                    self._emit_work_status(
                        turn_id=turn_id,
                        work_item_id=work_item_id,
                        worker_id=worker_id,
                        state=status_state,
                        origin_epoch=origin_epoch,
                        terminal_reason=status_reason,
                    )
            committed = await self._commit_and_speak(result, origin, role=speech_role)
            commit_ms = (time.perf_counter() - commit_started) * 1000
            child.finalize(
                outcome=child_outcome_label,
                app_worker_id=worker_id,
                result_id=result.result_id,
                search_ms=search_ms,
                commit_ms=commit_ms,
            )
            turn_recorder.record_commit(commit_ms)
            turn_recorder.finalize()
            self._project_worker(
                worker,
                origin_epoch=origin_epoch,
                status="idle",
                latest_result_id=None if was_cancelled else result.result_id,
            )
            return committed
        except asyncio.CancelledError:
            self._cancel_child_work_statuses(
                turn_id=turn_id, origin_epoch=origin_epoch, children=delegated_children
            )
            if not turn_recorder.finalized:
                turn_recorder.finalize(outcome="cancelled")
            raise
        except Exception:
            if not turn_recorder.finalized:
                turn_recorder.finalize(outcome="failed")
            raise
        finally:
            self._clear_ack_latch(turn_id)

    async def _handle_pending(
        self,
        outcome: Any,
        transcript: str,
        origin: Any,
        turn_id: str,
        turn_recorder: AppTurnRecorder,
    ) -> Any:
        coordinator = self._require_coordinator()
        origin_epoch = origin.epoch
        delegated_children: dict[str, str | None] = {}
        try:
            pending = getattr(outcome, "pending_dialogue", None)
            owner_id = pending.owner_id if pending is not None else None
            if owner_id is None:
                owner_id = outcome.work_items[0] if outcome.work_items else None
            registered = coordinator.registry.get(owner_id) if owner_id else None
            worker = registered.worker if registered is not None else None
            search = getattr(worker, "search", None)
            work_item_id = f"work-{turn_id}"
            if search is None:
                child = turn_recorder.new_child(work_item_id=work_item_id)
                child.finalize(outcome="missing_search", app_worker_id=owner_id)
                turn_recorder.finalize(outcome="failed")
                return outcome
            await self._register_runner_worker(worker)
            worker_id = owner_id or "main"
            self._known_work_items.add(work_item_id)
            await self._emit_early_ack(origin, turn_id=turn_id, origin_epoch=origin.epoch)
            clarification_context = self._clarification_context(pending, transcript)
            child = turn_recorder.new_child(work_item_id=work_item_id)
            # A continuation turn delegates genuine worker work, so it owns a
            # client-visible parent status exactly like a single-intent turn.
            delegated_children[work_item_id] = worker_id
            self._emit_work_status(
                turn_id=turn_id,
                work_item_id=work_item_id,
                worker_id=worker_id,
                state="routing",
                origin_epoch=origin_epoch,
            )
            retained_recorder = self._new_retained_recorder(
                origin_epoch=origin.epoch,
                turn_id=turn_id,
                work_item_id=work_item_id,
                app_worker_id=worker_id,
            )
            outcome_label: WorkItemOutcome = "completed"
            on_late_terminal = self._make_late_terminal_handler({work_item_id: retained_recorder})

            async def execute(_worker_id: str, query: str) -> GroundedResult:
                nonlocal outcome_label
                try:
                    kwargs: dict[str, Any] = {
                        "turn_id": turn_id,
                        "origin_epoch": origin.epoch,
                    }
                    if clarification_context is not None:
                        kwargs["clarification_context"] = clarification_context
                    result = await search(query, **kwargs)
                    outcome_label = "completed"
                    return result
                except WorkerClarify as exc:
                    outcome_label = "clarify"
                    return self._worker_clarification_result(
                        worker_id=worker_id,
                        turn_id=turn_id,
                        question=exc.question,
                        original_query=(pending.original_query if pending is not None else query),
                        origin_epoch=origin.epoch,
                    )
                except WorkerDeclined:
                    outcome_label = "declined"
                    return canonical_result(
                        worker_id=worker_id,
                        turn_id=turn_id,
                        text="I could not find a reliable result for that request.",
                        origin_epoch=origin.epoch,
                    )

            late_context = self._new_late_delivery_context(
                turn_id=turn_id, work_item_id=work_item_id, origin_epoch=origin.epoch
            )
            self._emit_work_status(
                turn_id=turn_id,
                work_item_id=work_item_id,
                worker_id=worker_id,
                state="searching",
                origin_epoch=origin_epoch,
            )
            submitted = await coordinator.submit(
                work_item_id,
                [(worker_id, transcript)],
                execute,
                on_late_complete=lambda late: self.commit_late_result_once(late_context, late),
                work_item_ids=[work_item_id],
                on_late_terminal=on_late_terminal,
            )
            if submitted.results:
                result = submitted.results[0]
                child_outcome_label = outcome_label
                child.finalize(
                    outcome=outcome_label, app_worker_id=worker_id, result_id=result.result_id
                )
            elif submitted.pending_work_item_ids:
                result = canonical_result(
                    worker_id=worker_id,
                    turn_id=turn_id,
                    text="That is taking longer than expected; I will continue in the background.",
                    origin_epoch=origin.epoch,
                )
                child_outcome_label = "retained"
                child.finalize(outcome="retained", app_worker_id=worker_id)
                self._register_retained_recorder_if_open(work_item_id, retained_recorder)
            else:
                failure_outcome: WorkItemOutcome = (
                    self._failure_child_outcome(submitted.failures[0])
                    if submitted.failures
                    else "failed"
                )
                result = canonical_result(
                    worker_id=worker_id,
                    turn_id=turn_id,
                    text="The pending web request could not be completed.",
                    origin_epoch=origin.epoch,
                )
                child_outcome_label = failure_outcome
                child.finalize(outcome=failure_outcome, app_worker_id=worker_id)
            speech_role = _speech_role_for_child_outcome(child_outcome_label)
            origin.scheduler.discard_queued_ack(f"ack-{turn_id}")
            was_cancelled = work_item_id in self._cancelled_work_items
            if child_outcome_label == "retained" and not was_cancelled:
                # Still working, not terminal (see the same rule in
                # ``_handle_transcript_impl``): the late result terminalizes it.
                self._emit_work_status(
                    turn_id=turn_id,
                    work_item_id=work_item_id,
                    worker_id=worker_id,
                    state="background",
                    origin_epoch=origin_epoch,
                )
            else:
                derived = _work_status_for_outcome(
                    child_outcome_label,
                    cancelled=was_cancelled,
                    terminal_kind=child_outcome_label,
                )
                if derived is not None:
                    status_state, status_reason = derived
                    self._emit_work_status(
                        turn_id=turn_id,
                        work_item_id=work_item_id,
                        worker_id=worker_id,
                        state=status_state,
                        origin_epoch=origin_epoch,
                        terminal_reason=status_reason,
                    )
            committed = await self._commit_and_speak(result, origin, role=speech_role)
            turn_recorder.finalize()
            return committed
        except asyncio.CancelledError:
            self._cancel_child_work_statuses(
                turn_id=turn_id, origin_epoch=origin_epoch, children=delegated_children
            )
            if not turn_recorder.finalized:
                turn_recorder.finalize(outcome="cancelled")
            raise
        except Exception:
            if not turn_recorder.finalized:
                turn_recorder.finalize(outcome="failed")
            raise
        finally:
            self._clear_ack_latch(turn_id)

    async def _handle_multi_intent(
        self,
        outcome: Any,
        transcript: str,
        origin: Any,
        turn_id: str,
        turn_recorder: AppTurnRecorder,
    ) -> tuple[Any, ...]:
        """Execute bounded compound work in the user's stated order."""
        del transcript
        coordinator = self._require_coordinator()
        origin_epoch = origin.epoch
        # Every delegated child of this turn aggregates under the one parent
        # key the turn submits under, so a compound turn shows the client a
        # single progressive record instead of one per item.
        parent_work_item_id = f"work-{turn_id}"
        delegated_children: dict[str, str | None] = {}
        try:
            results: dict[int, Any] = {}
            runnable: list[tuple[str, str]] = []
            runnable_indexes: list[int] = []
            runnable_workers: dict[int, Any] = {}
            contexts: dict[int, ClarificationContext | None] = {}
            child_recorders: dict[int, WorkItemRecorder] = {}
            pending = getattr(outcome, "pending_dialogue", None)
            for index, item_text in enumerate(outcome.work_items):
                item_work_item_id = f"work-{turn_id}-{index}"
                child = turn_recorder.new_child(work_item_id=item_work_item_id)
                child_recorders[index] = child
                worker = None
                if index == 0 and pending is not None:
                    registered = coordinator.registry.get(pending.owner_id)
                    worker = registered.worker if registered is not None else None
                else:
                    catalogue = coordinator.registry.catalogue()
                    try:
                        envelope = await asyncio.to_thread(
                            coordinator.router.route_envelope,
                            item_text,
                            catalogue,
                        )
                    except Exception as exc:  # noqa: BLE001  # intentional catch-all: routing can raise arbitrary provider/model errors that must fall back to a safe result
                        logger.warning(
                            f"Routing failed for {turn_id}-{index}; returning a safe result: "
                            f"{type(exc).__name__}"
                        )
                        results[index] = canonical_result(
                            worker_id="main",
                            turn_id=f"{turn_id}-{index}",
                            text="Routing is temporarily unavailable. Please try that request again.",
                            origin_epoch=origin.epoch,
                        )
                        child.finalize(outcome="failed")
                        continue
                    decision = envelope.decision
                    action = getattr(decision, "action", None)
                    if action in {"direct", "unsupported", "clarify"}:
                        text = (
                            envelope.prose
                            or {
                                "direct": "I could not produce a direct answer yet.",
                                "unsupported": "I cannot access that capability here.",
                                "clarify": "Could you clarify what you want me to search for?",
                            }[action]
                        )
                        results[index] = canonical_result(
                            worker_id="main",
                            turn_id=f"{turn_id}-{index}",
                            text=text,
                            origin_epoch=origin.epoch,
                        )
                        child.finalize(outcome=action)
                        continue
                    try:
                        worker = await asyncio.to_thread(self._dispatch, decision, catalogue)
                        await self._register_runner_worker(worker)
                    except (RoutingValidationError, UnsupportedWorkerType):
                        results[index] = canonical_result(
                            worker_id="main",
                            turn_id=f"{turn_id}-{index}",
                            text="I cannot access that capability here.",
                            origin_epoch=origin.epoch,
                        )
                        child.finalize(outcome="failed")
                        continue
                search = getattr(worker, "search", None)
                if search is None:
                    results[index] = canonical_result(
                        worker_id="main",
                        turn_id=f"{turn_id}-{index}",
                        text="I cannot access that capability here.",
                        origin_epoch=origin.epoch,
                    )
                    worker_id_for_child = (
                        getattr(getattr(worker, "metadata", None), "worker_id", None)
                        if worker is not None
                        else None
                    )
                    child.finalize(
                        outcome="missing_worker" if worker is None else "missing_search",
                        app_worker_id=worker_id_for_child,
                    )
                    continue
                worker_id = getattr(getattr(worker, "metadata", None), "worker_id", "main")
                # Only genuinely delegated items reach here: direct,
                # unsupported, clarify, and dispatch-failure items returned
                # above and never allocate a client-visible status.
                delegated_children[item_work_item_id] = worker_id
                self._emit_work_status(
                    turn_id=turn_id,
                    work_item_id=item_work_item_id,
                    parent_work_item_id=parent_work_item_id,
                    worker_id=worker_id,
                    state="routing",
                    origin_epoch=origin_epoch,
                )
                runnable.append((worker_id, item_text))
                runnable_indexes.append(index)
                runnable_workers[index] = worker
                contexts[index] = (
                    self._clarification_context(pending, item_text)
                    if index == 0 and pending is not None
                    else None
                )
                await self._emit_early_ack(origin, turn_id=turn_id, origin_epoch=origin.epoch)

            index_to_worker_id = dict(zip(runnable_indexes, runnable, strict=True))

            execution_indexes: dict[tuple[str, str], list[int]] = {}
            for item_index, item in zip(runnable_indexes, runnable, strict=True):
                execution_indexes.setdefault(item, []).append(item_index)

            outcome_labels: dict[int, WorkItemOutcome] = {}

            async def execute(worker_id: str, query: str) -> GroundedResult:
                item_index = execution_indexes[(worker_id, query)].pop(0)
                item_turn_id = f"{turn_id}-{item_index}"
                search = runnable_workers[item_index].search
                try:
                    kwargs: dict[str, Any] = {
                        "turn_id": item_turn_id,
                        "origin_epoch": origin.epoch,
                    }
                    if contexts[item_index] is not None:
                        kwargs["clarification_context"] = contexts[item_index]
                    result = await search(query, **kwargs)
                    outcome_labels[item_index] = "completed"
                    return result
                except WorkerClarify as exc:
                    outcome_labels[item_index] = "clarify"
                    context = contexts[item_index]
                    original_query = context.original_query if context is not None else query
                    return self._worker_clarification_result(
                        worker_id=worker_id,
                        turn_id=item_turn_id,
                        question=exc.question,
                        original_query=original_query,
                        origin_epoch=origin.epoch,
                    )
                except WorkerDeclined:
                    outcome_labels[item_index] = "declined"
                    return canonical_result(
                        worker_id=worker_id,
                        turn_id=item_turn_id,
                        text="I could not find a reliable result for that request.",
                        origin_epoch=origin.epoch,
                    )

            work_item_ids = [f"work-{turn_id}-{index}" for index in runnable_indexes]
            self._known_work_items.update(work_item_ids)
            retained_recorders: dict[str, RetainedRecorder] = {
                f"work-{turn_id}-{index}": self._new_retained_recorder(
                    origin_epoch=origin.epoch,
                    turn_id=turn_id,
                    work_item_id=f"work-{turn_id}-{index}",
                    app_worker_id=index_to_worker_id[index][0],
                )
                for index in runnable_indexes
            }
            on_late_terminal = self._make_late_terminal_handler(retained_recorders)

            dispatch_turn_sequence = self._turn_sequence

            def _multi_intent_late_context(late: LateResult) -> LateDeliveryContext:
                return self._new_late_delivery_context(
                    turn_id=turn_id,
                    work_item_id=late.work_item_id,
                    origin_epoch=origin.epoch,
                    accepted_turn_sequence=dispatch_turn_sequence,
                    # Same parent id the turn submits under, so a late
                    # child's status aggregates with its siblings instead
                    # of opening a second parent record.
                    parent_work_item_id=f"work-{turn_id}",
                )

            for item_work_item_id, item_worker_id in delegated_children.items():
                self._emit_work_status(
                    turn_id=turn_id,
                    work_item_id=item_work_item_id,
                    parent_work_item_id=parent_work_item_id,
                    worker_id=item_worker_id,
                    state="searching",
                    origin_epoch=origin_epoch,
                )
            submitted = await coordinator.submit(
                f"work-{turn_id}",
                runnable,
                execute,
                on_late_complete=lambda late: self.commit_late_result_once(
                    _multi_intent_late_context(late), late
                ),
                work_item_ids=work_item_ids,
                on_late_terminal=on_late_terminal,
            )
            # Both maps are built from this turn's own dispatched indexes, never
            # from worker- or coordinator-echoed strings, so a mismatched echo
            # can only miss the lookup. Every fan-in loop below warns and skips
            # on a miss rather than raising: the whole turn's remaining results
            # still commit and speak, and the unattributable item's child is
            # swept to a terminal ``failed`` record when the turn finalizes.
            index_for_item_turn_id = {f"{turn_id}-{index}": index for index in runnable_indexes}
            index_for_work_item_id = {
                f"work-{turn_id}-{index}": index for index in runnable_indexes
            }
            speech_roles: dict[int, SpeechRole] = {}
            # Two passes: settle last-wins content first, then attribute each
            # matched item exactly once from the result that actually commits.
            # Finalizing on first sight would name a discarded result_id in the
            # child record while a later duplicate won the committed content.
            final_result_for_index: dict[int, Any] = {}
            for result in submitted.results:
                matched = index_for_item_turn_id.get(result.turn_id)
                if matched is None:
                    logger.warning(
                        f"multi-intent fan-in for {turn_id}: dropping a result whose "
                        f"turn_id does not match any dispatched work item"
                    )
                    continue
                if matched in final_result_for_index:
                    logger.warning(
                        f"multi-intent fan-in for {turn_id}: duplicate result for the same "
                        f"work item; keeping the last result for both the committed "
                        f"content and the child record"
                    )
                results[matched] = result
                final_result_for_index[matched] = result
            for index, final_result in final_result_for_index.items():
                worker_id = index_to_worker_id[index][0]
                item_outcome = outcome_labels.get(index, "completed")
                child_recorders[index].finalize(
                    outcome=item_outcome,
                    app_worker_id=worker_id,
                    result_id=final_result.result_id,
                )
                item_work_item_id = f"work-{turn_id}-{index}"
                derived = _work_status_for_outcome(
                    item_outcome,
                    cancelled=item_work_item_id in self._cancelled_work_items,
                    terminal_kind=item_outcome,
                )
                if derived is not None:
                    status_state, status_reason = derived
                    self._emit_work_status(
                        turn_id=turn_id,
                        work_item_id=item_work_item_id,
                        parent_work_item_id=parent_work_item_id,
                        worker_id=worker_id,
                        state=status_state,
                        origin_epoch=origin_epoch,
                        terminal_reason=status_reason,
                    )
            for work_item_id in submitted.pending_work_item_ids:
                matched = index_for_work_item_id.get(work_item_id)
                if matched is None:
                    logger.warning(
                        f"multi-intent fan-in for {turn_id}: dropping a pending work_item_id "
                        f"that does not match any dispatched work item"
                    )
                    continue
                item_index = matched
                worker_id = index_to_worker_id[item_index][0]
                results[item_index] = canonical_result(
                    worker_id=worker_id,
                    turn_id=f"{turn_id}-{item_index}",
                    text="That item is taking longer than expected; I will continue in the background.",
                    origin_epoch=origin.epoch,
                )
                speech_roles[item_index] = ROLE_TIMEOUT_NOTICE
                child_recorders[item_index].finalize(outcome="retained", app_worker_id=worker_id)
                # Retained is not terminal: `background` is the truthful state
                # until the late result lands (same rule as the single-intent
                # path); a `failed` here could never be flipped back.
                self._emit_work_status(
                    turn_id=turn_id,
                    work_item_id=work_item_id,
                    parent_work_item_id=parent_work_item_id,
                    worker_id=worker_id,
                    state=(
                        "cancelled" if work_item_id in self._cancelled_work_items else "background"
                    ),
                    origin_epoch=origin_epoch,
                )
                recorder = retained_recorders.get(work_item_id)
                if recorder is not None:
                    self._register_retained_recorder_if_open(work_item_id, recorder)
            for failure in submitted.failures:
                matched = index_for_work_item_id.get(failure.work_item_id)
                if matched is None:
                    logger.warning(
                        f"multi-intent fan-in for {turn_id}: dropping a failure whose "
                        f"work_item_id does not match any dispatched work item"
                    )
                    continue
                item_index = matched
                results[item_index] = canonical_result(
                    worker_id=failure.worker_id,
                    turn_id=f"{turn_id}-{item_index}",
                    text="The web search is temporarily unavailable.",
                    origin_epoch=origin.epoch,
                )
                failure_outcome = self._failure_child_outcome(failure)
                child_recorders[item_index].finalize(
                    outcome=failure_outcome,
                    app_worker_id=failure.worker_id,
                )
                derived = _work_status_for_outcome(
                    failure_outcome,
                    cancelled=failure.work_item_id in self._cancelled_work_items,
                    terminal_kind=failure_outcome,
                )
                if derived is not None:
                    status_state, status_reason = derived
                    self._emit_work_status(
                        turn_id=turn_id,
                        work_item_id=failure.work_item_id,
                        parent_work_item_id=parent_work_item_id,
                        worker_id=failure.worker_id,
                        state=status_state,
                        origin_epoch=origin_epoch,
                        terminal_reason=status_reason,
                    )
            origin.scheduler.discard_queued_ack(f"ack-{turn_id}")
            committed = []
            for index in sorted(results):
                committed.append(
                    await self._commit_and_speak(
                        results[index], origin, role=speech_roles.get(index, ROLE_RESULT)
                    )
                )
            turn_recorder.finalize()
            return tuple(committed)
        except asyncio.CancelledError:
            self._cancel_child_work_statuses(
                turn_id=turn_id,
                origin_epoch=origin_epoch,
                children=delegated_children,
                parent_work_item_id=parent_work_item_id,
            )
            if not turn_recorder.finalized:
                turn_recorder.finalize(outcome="cancelled")
            raise
        except Exception:
            if not turn_recorder.finalized:
                turn_recorder.finalize(outcome="failed")
            raise
        finally:
            self._clear_ack_latch(turn_id)

    def _worker_clarification_result(
        self,
        *,
        worker_id: str,
        turn_id: str,
        question: str,
        original_query: str,
        origin_epoch: int | None,
    ) -> GroundedResult:
        """Record a worker's clarifying question as the next turn's pending candidate."""
        result_id = f"result-{uuid4().hex}"
        self._clarification_candidates[result_id] = {
            "worker_id": worker_id,
            "turn_id": turn_id,
            "original_query": original_query,
            "question": question,
        }
        return canonical_result(
            worker_id=worker_id,
            turn_id=turn_id,
            text=question,
            result_id=result_id,
            origin_epoch=origin_epoch,
        )

    @staticmethod
    def _clarification_context(pending: Any, transcript: str) -> ClarificationContext | None:
        if pending is None or not pending.original_query:
            return None
        return ClarificationContext(
            original_query=pending.original_query,
            question=pending.question,
            answer=transcript,
        )

    def _dispatch_search_task(
        self,
        search: Callable[..., Any],
        query: str,
        *,
        turn_id: str,
        origin_epoch: int,
        clarification_context: ClarificationContext | None = None,
    ) -> asyncio.Task[Any] | None:
        """Start the delegated search coroutine and return its task.

        Split out of ``_search_with_timeout`` so a caller can dispatch the
        search, give it exactly one scheduling tick, and only then decide
        whether an early ack is still warranted (see ``_emit_early_ack``'s
        ``search_task`` parameter).
        """
        kwargs: dict[str, Any] = {
            "turn_id": turn_id,
            "origin_epoch": origin_epoch,
        }
        if clarification_context is not None:
            kwargs["clarification_context"] = clarification_context
        starter = getattr(self.coordinator, "start_task", None)
        return (
            starter(search(query, **kwargs))
            if starter is not None
            else asyncio.create_task(search(query, **kwargs))
        )

    async def _search_with_timeout(
        self,
        search: Callable[..., Any],
        query: str,
        *,
        turn_id: str,
        origin_epoch: int,
        timeout: float,
        worker_id: str,
        work_item_id: str | None = None,
        clarification_context: ClarificationContext | None = None,
        task: asyncio.Task[Any] | None = None,
    ) -> SearchExecution:
        coordinator = self._require_coordinator()
        if task is None:
            task = self._dispatch_search_task(
                search,
                query,
                turn_id=turn_id,
                origin_epoch=origin_epoch,
                clarification_context=clarification_context,
            )
        if task is None:
            return SearchExecution("capacity_rejected")
        work_item_id = work_item_id or f"work-{turn_id}"
        self._track_work_task(work_item_id, task)
        # Captured before the foreground wait below, which may span other
        # turns being accepted concurrently: this is the turn-sequence
        # snapshot LateDeliveryContext needs to detect a newer-turn arrival.
        dispatch_turn_sequence = self._turn_sequence
        # The provisional retained recorder is created here, at dispatch time,
        # before the foreground wait -- not only if it later times out -- so
        # background_ms always starts at work dispatch (Timing Boundaries).
        # It is discarded unregistered if the foreground wait completes first.
        retained_recorder = self._new_retained_recorder(
            origin_epoch=origin_epoch,
            turn_id=turn_id,
            work_item_id=work_item_id,
            app_worker_id=worker_id,
        )
        try:
            done, _ = await asyncio.wait({task}, timeout=timeout)
        except asyncio.CancelledError:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise
        if task in done:
            return SearchExecution("completed", await task)
        # Register the provisional recorder before handing the completion
        # callback to the coordinator, so it is always in the registry by the
        # time that callback could possibly run -- regardless of whether a
        # future refactor inserts an await between the two calls.
        self._register_retained_recorder_if_open(work_item_id, retained_recorder)
        late_context = self._new_late_delivery_context(
            turn_id=turn_id,
            work_item_id=work_item_id,
            origin_epoch=origin_epoch,
            accepted_turn_sequence=dispatch_turn_sequence,
        )
        accepted = coordinator.retain_late_task(
            task,
            work_item_id=work_item_id,
            worker_id=worker_id,
            on_complete=lambda late: self.commit_late_result_once(late_context, late),
            on_late_terminal=self._make_late_terminal_handler({work_item_id: retained_recorder}),
        )
        if not accepted:
            self._retained_recorders.pop(work_item_id, None)
            return SearchExecution("retention_rejected")
        return SearchExecution("retained")

    def _new_late_delivery_context(
        self,
        *,
        turn_id: str,
        work_item_id: str,
        origin_epoch: int,
        accepted_turn_sequence: int | None = None,
        parent_work_item_id: str | None = None,
    ) -> LateDeliveryContext:
        """Build the immutable context captured at late-task dispatch time.

        ``accepted_turn_sequence`` defaults to the live ``_turn_sequence``
        counter, but callers dispatching from a coroutine that may span
        other turns being accepted concurrently pass an explicit snapshot
        taken before any ``await``. ``parent_work_item_id`` is supplied only
        by multi-intent dispatch, where the child work item is not itself
        the turn's work-status parent.
        """
        return LateDeliveryContext(
            turn_id=turn_id,
            work_item_id=work_item_id,
            origin_epoch=origin_epoch,
            parent_work_item_id=parent_work_item_id,
            ack_timestamp=time.monotonic(),
            accepted_turn_sequence=(
                accepted_turn_sequence
                if accepted_turn_sequence is not None
                else self._turn_sequence
            ),
        )

    def _late_result_disposition(
        self, context: LateDeliveryContext, *, origin: ConnectionPipeline
    ) -> LateDeliveryDisposition:
        """Autoplay-vs-display-only verdict for one still-committable late result.

        ``enable_autoplay_policy=False`` is the strict rollback path: every
        valid, non-cancelled result on its still-active connection is
        spoken, exactly reproducing pre-v0.1.3 behavior. When the policy is
        enabled, autoplay requires *all* of: promotion-eligible evidence
        (schema validity alone is never sufficient -- see
        ``server.config.load_promotion_manifest``), the originating epoch
        still active, no newer same-epoch turn accepted since dispatch, and
        no explicit pause in effect. Any predicate failing degrades to
        ``"display_only"``; the result was already committed exactly once
        by the caller regardless of this verdict.
        """
        if not self.feature_policy.enable_autoplay_policy:
            return "autoplay"
        manifest = self._promotion_manifest
        if manifest is None or not manifest.promotion_eligible:
            return "display_only"
        if context.origin_epoch != origin.epoch:
            return "display_only"
        if self._turn_sequence != context.accepted_turn_sequence:
            return "display_only"
        if origin.scheduler.paused() is not None:
            return "display_only"
        return "autoplay"

    async def commit_late_result_once(self, context: LateDeliveryContext, late: LateResult) -> None:
        """The sole host-owned atomic API for a coordinator late-result callback.

        Commits every valid result exactly once and separately computes the
        autoplay-vs-display-only delivery disposition; a valid commit on an
        otherwise-speakable connection additionally consults
        ``_late_result_disposition`` before enqueuing speech.

        Branch precedence is: worker error, then the three structural
        fences -- not a ``GroundedResult``, foreign ``origin_epoch``,
        already-committed ``result_id`` -- and only then cancellation,
        followed by the normal commit-and-speak path. Cancellation is a
        classification input rather than a fence of its own: a cancelled
        work item whose result clears every fence still commits exactly
        once (display-only, no speech), while a cancelled result that is
        stale or duplicate is suppressed like any other, keeping
        ``work_outcome="cancelled"`` alongside the suppressing
        ``commit_outcome``. Ordering cancellation ahead of the fences would
        let a foreign-epoch result reach authoritative state and would make
        ``suppressed_duplicate`` unreachable for cancelled work.
        Synchronous
        and non-awaiting through the disposition decision itself, so
        nothing can interleave between context capture and that verdict;
        the network-facing enqueue/start_next calls below are the only
        awaiting steps, matching the surrounding coordinator-callback
        contract.
        """
        origin_epoch = context.origin_epoch
        # Popping (rather than peeking) makes a callback arriving after a
        # dispatch-registered recorder's finalization a structural no-op for
        # that recorder. Every retained work item registers its provisional
        # recorder at dispatch, so a missing recorder means this work item's
        # background metric is already closed (or was never retained): the
        # remainder of this method stays telemetry-silent rather than opening a
        # replacement recorder, whose start instant would be completion time
        # and whose `background_ms` would therefore be near zero.
        recorder = self._retained_recorders.pop(late.work_item_id, None)
        if recorder is not None and late.terminal_kind is not None:
            recorder.claim(late.terminal_kind)

        work_outcome: WorkOutcome | None = None
        commit_outcome: CommitOutcome | None = None
        speech_outcome: SpeechOutcome | None = None
        delivery_disposition: LateDeliveryDisposition | None = None
        result_id: str | None = None
        pending_exception: Exception | None = None

        # Cancellation is a *classification* input for every non-error branch
        # below, never a branch of its own that could bypass the structural
        # fences (invalid result / foreign origin_epoch / duplicate
        # result_id). Snapshotting and clearing the marker once, here, keeps
        # the marker's consumption independent of branch order; the error
        # path keeps its pre-existing behavior of leaving the marker
        # untouched.
        was_cancelled = late.error is None and late.work_item_id in self._cancelled_work_items
        if was_cancelled:
            self._cancelled_work_items.discard(late.work_item_id)
            if isinstance(late.result, GroundedResult):
                self._clarification_candidates.pop(late.result.result_id, None)

        # The recorder was already popped above, so it is only reachable from
        # this stack frame from here on. A CancelledError delivered during the
        # ``await origin.scheduler.start_next()`` below (e.g. from
        # WorkItemCoordinator.shutdown cancelling this coordinator-owned
        # callback task) must not unwind past finalization: that would both
        # skip the work_item_background record AND leave no still-registered
        # recorder for SessionHost.shutdown's sweep to catch as a backstop.
        # The try/finally makes finalization unconditional regardless of how
        # this block exits; RetainedRecorder.finalize supplies terminal
        # defaults for any outcome field still unset at that point.
        try:
            if late.error is not None:
                self._known_work_items.discard(late.work_item_id)
                # A worker task cancellation reaching this normal completion path
                # is always a live "suppressed_cancelled" outcome. Pure
                # shutdown-triggered cancellation never reaches here: the
                # coordinator's shutdown guard suppresses this callback entirely,
                # so that case is finalized separately by
                # ``SessionHost.shutdown``'s still-open-recorder sweep.
                work_outcome = late.terminal_kind or "failed"
                if late.terminal_kind == "cancelled":
                    commit_outcome, speech_outcome = "suppressed_cancelled", "cancelled"
                    delivery_disposition = "suppressed"
                else:
                    commit_outcome, speech_outcome = "not_applicable", "not_applicable"
                    delivery_disposition = "not_applicable"
                logger.warning(
                    f"Late worker result failed for work_item={late.work_item_id} "
                    f"worker={late.worker_id}"
                )
            elif not isinstance(late.result, GroundedResult):
                self._known_work_items.discard(late.work_item_id)
                if was_cancelled:
                    work_outcome, commit_outcome, speech_outcome = (
                        "cancelled",
                        "suppressed_cancelled",
                        "cancelled",
                    )
                    delivery_disposition = "suppressed"
                else:
                    work_outcome, commit_outcome, speech_outcome = (
                        "invalid_result",
                        "not_applicable",
                        "not_applicable",
                    )
                    delivery_disposition = "not_applicable"
            elif late.result.origin_epoch != origin_epoch:
                self._known_work_items.discard(late.work_item_id)
                work_outcome, commit_outcome, speech_outcome = (
                    "cancelled" if was_cancelled else "completed",
                    "suppressed_stale",
                    "not_applicable",
                )
                delivery_disposition = "not_applicable"
            elif any(
                item.result_id == late.result.result_id for item in self.state.results.results
            ):
                self._known_work_items.discard(late.work_item_id)
                work_outcome, commit_outcome, speech_outcome = (
                    "cancelled" if was_cancelled else "completed",
                    "suppressed_duplicate",
                    "not_applicable",
                )
                delivery_disposition = "not_applicable"
            elif was_cancelled:
                # Cancellation before/while-queued/while-admitted suppresses
                # or reclassifies *speech delivery* only -- it no longer
                # suppresses a valid canonical commit (dev plan Phase 2,
                # cancellation matrix): a cancelled work item whose late
                # result is otherwise valid *and past every structural fence
                # above* is still committed exactly once, display-only, with
                # no speech attempt.
                self._known_work_items.discard(late.work_item_id)
                cancelled_result = late.result
                try:
                    self._commit_result_state(cancelled_result)
                except Exception as exc:  # noqa: BLE001 - preserves existing commit-failure re-raise behavior
                    work_outcome, commit_outcome, speech_outcome = (
                        "cancelled",
                        "failed",
                        "cancelled",
                    )
                    delivery_disposition = "not_applicable"
                    pending_exception = exc
                else:
                    worker = self.state.workers.get(cancelled_result.worker_id)
                    if worker is not None and worker.origin_epoch == origin_epoch:
                        self.state.set_worker(
                            worker.model_copy(
                                update={
                                    "status": "idle",
                                    "latest_result_id": cancelled_result.result_id,
                                }
                            )
                        )
                    work_outcome, commit_outcome, speech_outcome = (
                        "cancelled",
                        "committed",
                        "cancelled",
                    )
                    delivery_disposition = "display_only"
                    result_id = cancelled_result.result_id
            else:
                result = late.result
                try:
                    self._commit_result_state(result)
                except Exception as exc:  # noqa: BLE001 - preserves existing commit-failure re-raise behavior
                    self._known_work_items.discard(late.work_item_id)
                    work_outcome, commit_outcome, speech_outcome = (
                        "completed",
                        "failed",
                        "not_applicable",
                    )
                    delivery_disposition = "not_applicable"
                    pending_exception = exc
                else:
                    self._known_work_items.discard(late.work_item_id)
                    worker = self.state.workers.get(result.worker_id)
                    if worker is not None and worker.origin_epoch == origin_epoch:
                        self.state.set_worker(
                            worker.model_copy(
                                update={
                                    "status": "idle",
                                    "latest_result_id": result.result_id,
                                }
                            )
                        )
                    origin = self.connection
                    speakable: ConnectionPipeline | None = None
                    if origin is None:
                        speech_outcome = "disconnected"
                    elif origin.tts is None:
                        speech_outcome = "no_tts"
                    elif (
                        not origin.active
                        or origin.epoch != origin_epoch
                        or not self.accepts(origin_epoch)
                    ):
                        speech_outcome = "stale_connection"
                    else:
                        speech_outcome = None
                        speakable = origin
                    work_outcome, commit_outcome = "completed", "committed"
                    result_id = result.result_id
                    if speakable is not None:
                        # Exactly-once commit is already done above; this
                        # verdict only decides whether the committed result
                        # is additionally spoken. A committed-but-not-spoken
                        # result is still a fully valid, terminal outcome.
                        policy_disposition = self._late_result_disposition(
                            context, origin=speakable
                        )
                        if policy_disposition == "display_only":
                            delivery_disposition = "display_only"
                            speech_outcome = "not_applicable"
                        else:
                            delivery_disposition = "autoplay"
                            try:
                                # A retained result supersedes only its own
                                # queued timeout notice, not other same-work
                                # queued speech, and never an utterance that has
                                # already been admitted to the transport slot.
                                speakable.scheduler.discard_queued_notice(late.work_item_id)
                                speakable.scheduler.enqueue(
                                    result_id=result.result_id,
                                    work_item_id=late.work_item_id,
                                    run_id=f"run-{result.turn_id}",
                                    text=result.spoken_text,
                                    origin_epoch=origin_epoch,
                                    role=ROLE_RESULT,
                                )
                            except Exception as exc:  # noqa: BLE001 - preserves existing enqueue-failure re-raise behavior
                                speech_outcome = "enqueue_failed"
                                pending_exception = exc
                            else:
                                try:
                                    await speakable.scheduler.start_next()
                                except Exception as exc:  # noqa: BLE001 - preserves existing start-failure re-raise behavior
                                    speech_outcome = "start_failed"
                                    pending_exception = exc
                                else:
                                    speech_outcome = "queued"
                    else:
                        delivery_disposition = "display_only"
        finally:
            if recorder is not None:
                recorder.finalize(
                    work_outcome=work_outcome,
                    commit_outcome=commit_outcome,
                    speech_outcome=speech_outcome,
                    delivery_disposition=delivery_disposition,
                    result_id=result_id,
                )
            derived = _work_status_for_outcome(
                work_outcome,
                cancelled=work_outcome == "cancelled",
                committed=commit_outcome == "committed",
                terminal_kind=late.terminal_kind,
            )
            if derived is not None:
                status_state, status_reason = derived
                self._emit_work_status(
                    turn_id=context.turn_id,
                    work_item_id=context.work_item_id,
                    parent_work_item_id=context.parent_work_item_id,
                    state=status_state,
                    origin_epoch=origin_epoch,
                    terminal_reason=status_reason,
                )
        if pending_exception is not None:
            raise pending_exception

    def _commit_result_state(self, result: GroundedResult) -> None:
        self.state.append_transcript(
            TranscriptEntry(
                role="assistant",
                text=result.ui_text,
                turn_id=result.turn_id,
                origin_epoch=result.origin_epoch,
            )
        )
        self.state.append_result(result, origin_epoch=result.origin_epoch)
        candidate = self._clarification_candidates.pop(result.result_id, None)
        if candidate is not None and self.accepts(result.origin_epoch):
            self._require_coordinator().add_worker_clarification(
                session_id=self.state.session_id,
                result_id=result.result_id,
                **candidate,
            )

    async def _commit_and_speak(
        self,
        result: GroundedResult,
        origin: ConnectionPipeline,
        *,
        role: SpeechRole = ROLE_RESULT,
        require_tts: bool = True,
    ) -> GroundedResult:
        """Commit a result and speak only when its originating epoch is active.

        ``require_tts`` gates the no-TTS short-circuit: delegated worker
        results stay history-only without a TTS lane (``require_tts=True``,
        the default), while the main-responder direct/unsupported/clarify
        replies still enqueue through the scheduler's no-TTS pre-admission
        terminal path so their progress is recorded even with no transport.
        """
        origin_epoch = result.origin_epoch
        work_item_id = f"work-{result.turn_id}"
        if work_item_id in self._cancelled_work_items:
            self._cancelled_work_items.discard(work_item_id)
            self._known_work_items.discard(work_item_id)
            self._clarification_candidates.pop(result.result_id, None)
            return result
        self._commit_result_state(result)
        self._known_work_items.discard(work_item_id)
        if (
            (require_tts and origin.tts is None)
            or self.connection is not origin
            or not origin.active
            or not self.accepts(origin_epoch)
        ):
            return result
        origin.scheduler.enqueue(
            result_id=result.result_id,
            work_item_id=work_item_id,
            run_id=f"run-{result.turn_id}",
            text=result.spoken_text,
            origin_epoch=origin_epoch,
            role=role,
        )
        try:
            await origin.scheduler.start_next(work_item_id)
        except Exception:  # noqa: BLE001 - result already committed; speech failure must not propagate
            # A no-TTS/submission-failed result may still reach
            # result_ready: that state describes canonical commit, not
            # audio. start_next's own except-path already recorded
            # DELIVERY_UNKNOWN and released the coordinator slot.
            logger.opt(exception=True).debug("speech submission failed after commit; result stands")
        return result

    def session_handshake(self) -> dict[str, Any]:
        """Return the next browser handshake without mutating session state."""
        self._prune_handshake_tokens()
        token = uuid4().hex
        self._handshake_tokens[token] = (
            self.arbiter.epoch + 1,
            time.monotonic() + 60,
            False,
        )
        while len(self._handshake_tokens) > self._MAX_HANDSHAKE_TOKENS:
            oldest = min(self._handshake_tokens, key=lambda item: self._handshake_tokens[item][1])
            self._handshake_tokens.pop(oldest, None)
        return {
            "contract_version": "v1.0",
            "session_id": self.state.session_id,
            "resume_token": token,
            "proposed_epoch": self.arbiter.epoch + 1,
            "snapshot_sequence": self.state.sequence,
        }

    def _prune_handshake_tokens(self) -> None:
        now = time.monotonic()
        expired = [
            token
            for token, (_epoch, expires_at, _redeemed) in self._handshake_tokens.items()
            if expires_at <= now
        ]
        for token in expired:
            self._handshake_tokens.pop(token, None)

    def _dispatch(self, decision: Any, catalogue: Any = None) -> Any:
        coordinator = self._require_coordinator()
        if catalogue is None:
            return coordinator.dispatch(decision)
        return coordinator.dispatch(decision, catalogue=catalogue)

    def _track_work_task(self, work_item_id: str, task: asyncio.Task[Any]) -> None:
        self._known_work_items.add(work_item_id)
        self._inflight_work_tasks.setdefault(work_item_id, set()).add(task)

        def completed(completed_task: asyncio.Task[Any]) -> None:
            tasks = self._inflight_work_tasks.get(work_item_id)
            if tasks is None:
                return
            tasks.discard(completed_task)
            if not tasks:
                self._inflight_work_tasks.pop(work_item_id, None)

        task.add_done_callback(completed)

    def _track_turn_task(self, work_item_id: str, task: asyncio.Task[Any]) -> None:
        self._known_work_items.add(work_item_id)
        self._inflight_turn_tasks[work_item_id] = task

        def completed(completed_task: asyncio.Task[Any]) -> None:
            if self._inflight_turn_tasks.get(work_item_id) is not completed_task:
                return
            self._inflight_turn_tasks.pop(work_item_id, None)
            if work_item_id not in self._inflight_work_tasks:
                self._known_work_items.discard(work_item_id)
                self._cancelled_work_items.discard(work_item_id)

        task.add_done_callback(completed)

    def _cancel_work(
        self,
        work_item_id: str | None,
        *,
        exclude_work_item_id: str | None = None,
    ) -> tuple[str, ...]:
        selected = tuple(
            item_id
            for item_id in dict.fromkeys((*self._inflight_turn_tasks, *self._inflight_work_tasks))
            if (work_item_id is None or item_id == work_item_id) and item_id != exclude_work_item_id
        )
        coordinator_cancel = getattr(self.coordinator, "cancel", None)
        if coordinator_cancel is not None:
            selected = tuple(dict.fromkeys((*selected, *coordinator_cancel(work_item_id))))
        if work_item_id is None:
            selected = tuple(dict.fromkeys((*selected, *self._known_work_items)))
        selected = tuple(item for item in selected if item != exclude_work_item_id)
        self._cancelled_work_items.update(selected)
        for item_id in selected:
            turn_task = self._inflight_turn_tasks.get(item_id)
            if turn_task is not None:
                turn_task.cancel()
            for task in self._inflight_work_tasks.get(item_id, ()):
                task.cancel()
        return selected

    def _project_worker(
        self,
        worker: Any,
        *,
        origin_epoch: int,
        status: str,
        latest_result_id: str | None = None,
    ) -> None:
        metadata = getattr(worker, "metadata", None)
        if metadata is None:
            return
        worker_id = getattr(metadata, "worker_id", None)
        topic = getattr(metadata, "topic", None)
        model_policy = getattr(metadata, "model_policy", None)
        if (
            not (isinstance(worker_id, str) and worker_id)
            or not (isinstance(topic, str) and topic)
            or not (isinstance(model_policy, str) and model_policy)
        ):
            return
        previous = self.state.workers.get(worker_id)
        self.state.set_worker(
            WorkerState(
                worker_id=worker_id,
                topic=topic,
                model_policy=model_policy,
                status=status,
                latest_result_id=(
                    latest_result_id
                    if latest_result_id is not None
                    else previous.latest_result_id
                    if previous is not None
                    else None
                ),
                origin_epoch=origin_epoch,
            )
        )

    def _emit_work_status(
        self,
        *,
        turn_id: str,
        work_item_id: str,
        parent_work_item_id: str | None = None,
        worker_id: str | None = None,
        state: WorkStatusState,
        origin_epoch: int,
        terminal_reason: TerminalReason | None = None,
    ) -> None:
        """Record one delegated child's coarse status (Phase 3).

        A no-op unless ``enable_background_status`` is on; the wire
        emission itself is further capability-gated per connection inside
        the observer, so this call site does not need to branch on client
        capability -- only on whether the feature is enabled at all.
        """
        if not self.feature_policy.enable_background_status:
            return
        self.state.set_child_work_status(
            turn_id=turn_id,
            work_item_id=work_item_id,
            parent_work_item_id=parent_work_item_id,
            worker_id=worker_id,
            state=state,
            origin_epoch=origin_epoch,
            terminal_reason=terminal_reason,
        )

    def _cancel_child_work_statuses(
        self,
        *,
        turn_id: str,
        origin_epoch: int,
        children: Mapping[str, str | None],
        parent_work_item_id: str | None = None,
    ) -> None:
        """Terminalize every still-non-terminal delegated child as ``cancelled``.

        A turn cancelled mid-flight would otherwise strand a capable client on
        a non-terminal ``routing``/``searching`` record forever. ``children``
        maps each delegated child's ``work_item_id`` to its worker id.

        Idempotent by construction, so callers do not have to track which
        children already settled: ``legal_work_status_transition`` rejects any
        move out of a terminal child state, rejects ``cancelled`` as a
        cold-start state (a child that never had a status is a no-op), and
        ``_reaggregate_parent`` never regresses an already-terminal parent.
        """
        for work_item_id, worker_id in children.items():
            self._emit_work_status(
                turn_id=turn_id,
                work_item_id=work_item_id,
                parent_work_item_id=parent_work_item_id,
                worker_id=worker_id,
                state="cancelled",
                origin_epoch=origin_epoch,
            )

    def accepts(self, epoch: int | None) -> bool:
        return (
            not self._closing
            and self.arbiter.accepts(epoch)
            and self.connection is not None
            and self.connection.active
        )

    async def shutdown(self) -> None:
        self._closing = True
        turn_tasks = tuple(self._inflight_turn_tasks.values())
        for task in turn_tasks:
            task.cancel()
        if turn_tasks:
            await asyncio.gather(*turn_tasks, return_exceptions=True)
        registrations = tuple(self._runner_registrations.values())
        for task in registrations:
            task.cancel()
        if registrations:
            await asyncio.gather(*registrations, return_exceptions=True)
        shutdowns = set(self._background_shutdowns)
        if self.connection is not None:
            connection = self.connection
            connection.deactivate(reconnect=False)
            self.connection = None
            shutdowns.add(asyncio.create_task(connection.shutdown(reason="session shutdown")))
        if shutdowns:
            done, pending = await asyncio.wait(
                shutdowns,
                timeout=self.config.shutdown_grace_seconds,
            )
            for task in pending:
                task.cancel()
                task.add_done_callback(
                    lambda completed: None if completed.cancelled() else completed.exception()
                )
            for task in done:
                if not task.cancelled():
                    task.exception()
        coordinator_shutdown = getattr(self.coordinator, "shutdown", None)
        if coordinator_shutdown is not None:
            result = coordinator_shutdown()
            if inspect.isawaitable(result):
                await result
        # Coordinator work/callback tasks have now settled. Finalize every
        # retained recorder still open: unclaimed work is shutdown-cancelled,
        # while claimed work uses its recorded terminal kind and whatever
        # commit/speech stage it had already reached.
        for work_item_id, recorder in tuple(self._retained_recorders.items()):
            recorder.finalize()
            self._retained_recorders.pop(work_item_id, None)
        stop = getattr(self.runner, "stop", None)
        if stop is not None:
            result = stop()
            if hasattr(result, "__await__"):
                await result
        elif self.runner is not None:
            cancel = getattr(self.runner, "cancel", None)
            if cancel is not None:
                result = cancel("session shutdown")
                if hasattr(result, "__await__"):
                    await result
        if self._runner_task is not None:
            self._runner_task.cancel()
            try:
                await self._runner_task
            except asyncio.CancelledError:
                pass
            finally:
                self._runner_task = None
        self.started = False
        self.state.active_epoch = None
