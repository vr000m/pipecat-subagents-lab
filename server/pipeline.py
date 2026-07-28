"""Pipecat lifecycle adapter around durable workers and a connection pipeline."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from loguru import logger
from pipecat.frames.frames import InterruptionFrame, TTSSpeakFrame
from pipecat.processors.frameworks.rtvi.frames import RTVIServerMessageFrame
from pydantic import ValidationError

from .connection_arbiter import ConnectionArbiter
from .contracts import (
    GroundedResult,
    RoutingDecision,
    RoutingState,
    TranscriptEntry,
    WorkerState,
)
from .observers import RuntimeObserver
from .perf_metrics import (
    AppTurnRecorder,
    ConsoleMeasurementSink,
    MeasurementSink,
    RetainedRecorder,
    WorkItemRecorder,
)
from .registry import UnsupportedWorkerType, WorkerRegistry
from .results import canonical_result
from .router import RoutingValidationError
from .rtvi_messages import RTVIMessage
from .session_state import SessionState
from .speech_scheduler import SpeechScheduler
from .work_item_coordinator import FAILURE_KINDS, LateResult, WorkItemFailure
from .workers.web_search import ClarificationContext, WorkerClarify, WorkerDeclined

try:
    from pipecat.bus.bridge_processor import (
        BusBridgeProcessor as BusBridgeProcessor,  # noqa: PLC0414  # explicit re-export: name must match the except-branch fallback class below
    )
    from pipecat.bus.bus import WorkerBus
except ImportError:  # pragma: no cover - only for dependency-free contract tests

    class BusBridgeProcessor:
        """Dependency-free marker used only when the pinned bridge API is absent."""

        framework_fallback = True

    WorkerBus = None  # type: ignore[assignment,misc]


if WorkerBus is not None:

    class _ProbeBus(WorkerBus):
        """No-op bus used only to construct dependency-free contract pipelines."""

        async def publish(self, _message: Any) -> None:
            return None
else:  # pragma: no cover
    _ProbeBus = None


def framework_bridge(*, bus: Any, worker_name: str, **kwargs: Any) -> Any:
    """Construct the pinned framework bridge with connection-local output frames."""
    if getattr(BusBridgeProcessor, "framework_fallback", False):
        return BusBridgeProcessor()
    excluded = kwargs.pop("exclude_frames", ())
    kwargs["exclude_frames"] = tuple(dict.fromkeys((TTSSpeakFrame, *excluded)))
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


def build_pipeline(*, transport: Any, stt: Any, tts: Any) -> LabPipeline:
    """Compose the connection-local bridge, canonical adapter, and speech seams."""
    bus = _ProbeBus() if _ProbeBus is not None else None
    bridge = (
        framework_bridge(bus=bus, worker_name="contract-pipeline")
        if bus is not None
        else BusBridgeProcessor()
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
    stt: Any | None = None
    tts: Any | None = None
    transport: Any | None = None
    worker: Any | None = None
    worker_task: asyncio.Task[Any] | None = None
    on_transcript: Callable[[str], Any] | None = None
    active: bool = True

    def deactivate(self, *, reconnect: bool = True) -> None:
        self.active = False
        self.scheduler.interrupt(epoch=self.epoch, reconnect=reconnect)

    async def shutdown(self, *, reason: str = "connection replaced") -> None:
        """Fence this connection and stop its Pipecat worker, if attached."""
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


@dataclass(frozen=True)
class SearchExecution:
    """Result of a foreground search and any ownership transfer."""

    status: str
    result: GroundedResult | None = None


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
    ) -> None:
        self.state = SessionState()
        self.arbiter = ConnectionArbiter(self.state.session_id, self.state.resume_token)
        coordinator_registry = getattr(coordinator, "registry", None)
        if registry is None and coordinator_registry is not None:
            registry = coordinator_registry
        self.registry = registry or WorkerRegistry()
        if coordinator_registry is not None and coordinator_registry is not self.registry:
            raise ValueError("SessionHost and coordinator must share one WorkerRegistry")
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
            registration.add_done_callback(
                lambda completed, registered_id=worker_id: (
                    self._runner_registrations.pop(registered_id, None)
                    if self._runner_registrations.get(registered_id) is completed
                    else None
                )
            )
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
            del item
            if connection_tts is None or self.connection is not pipeline or pipeline.worker is None:
                return
            await pipeline.worker.queue_frame(InterruptionFrame())

        pipeline = ConnectionPipeline(
            connection.epoch,
            RuntimeObserver(self.state, connection.epoch),
            SpeechScheduler(
                self.state,
                speak=queue_speech if connection_tts is not None else None,
                stop=stop_speech if connection_tts is not None else None,
            ),
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
                if event == "synthesis_started":
                    pipeline.scheduler.provider_started(context_id)
                elif event == "synthesis_ended" and current:
                    matched = pipeline.scheduler.provider_synthesis_ended(context_id)
                    # The pinned local service exposes synthesis completion, not
                    # browser playout completion. Release conservatively as
                    # unknown so later utterances cannot be starved.
                    if matched:
                        pipeline.scheduler.provider_delivery_unknown(context_id)
                elif event == "delivery_completed" and current:
                    pipeline.scheduler.provider_delivery_completed(context_id)
                elif event == "delivery_unknown" and current:
                    pipeline.scheduler.provider_delivery_unknown(context_id)
                if (
                    event in {"synthesis_ended", "delivery_completed", "delivery_unknown"}
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
    ) -> Callable[[str, str], None]:
        """Build a coordinator ``on_late_terminal`` callback that claims the
        matching retained recorder, if any, for a late-completing work item."""

        def on_late_terminal(item_id: str, terminal_kind: str) -> None:
            recorder = recorders.get(item_id)
            if recorder is not None:
                recorder.claim(terminal_kind)

        return on_late_terminal

    @staticmethod
    def _failure_child_outcome(failure: WorkItemFailure) -> str:
        """Classify a work-item failure from its structured ``failure_kind``.

        ``error_type`` is free-text diagnostic and must never drive
        classification: renaming a worker exception class would otherwise
        silently reclassify its telemetry outcome.
        """
        kind = getattr(failure, "failure_kind", None)
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

    def abort_connection(self, pipeline: ConnectionPipeline) -> None:
        """Fence a promoted connection whose transport setup did not complete."""
        if self.connection is pipeline:
            pipeline.deactivate()
            self.connection = None
            self.state.active_epoch = None

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
                control_action: str | None = None
                control_outcome: str | None = None
                if outcome.kind == "control":
                    action = getattr(outcome, "control_action", None)
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
                        cancelled_work = self._cancel_work(
                            target,
                            exclude_work_item_id=work_item_id,
                        )
                        cancelled_speech = origin.scheduler.cancel(target)
                        await origin.scheduler.wait_for_stops()
                        if not cancelled_work and not cancelled_speech:
                            action = "unknown_target" if target is not None else "no_active"
                            control_outcome = action
                        else:
                            control_outcome = "applied"
                    elif action == "consent":
                        control_outcome = "no_pending"
                    text = {
                        "pause": "Pausing the active response.",
                        "resume": "Resuming the paused response.",
                        "cancel": "Cancelling the active response.",
                        "stop": "Stopping the active response.",
                        "unknown_target": "I could not find that active work item.",
                        "no_active": "There is no active response to cancel.",
                    }.get(action, "Control request noted.")
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
                return outcome
            self._project_worker(worker, origin_epoch=origin_epoch, status="running")
            search = getattr(worker, "search", None)
            worker_id = getattr(getattr(worker, "metadata", None), "worker_id", "main")
            if search is None:
                child = turn_recorder.new_child(work_item_id=work_item_id)
                child.finalize(outcome="missing_search", app_worker_id=worker_id)
                turn_recorder.finalize(outcome="failed")
                return outcome
            child = turn_recorder.new_child(work_item_id=work_item_id)
            try:
                search_started = time.perf_counter()
                execution = await self._search_with_timeout(
                    search,
                    transcript,
                    turn_id=turn_id,
                    origin_epoch=origin_epoch,
                    timeout=getattr(
                        getattr(self.coordinator, "config", None)
                        or getattr(self.registry, "config", None),
                        "foreground_search_timeout_seconds",
                        15.0,
                    ),
                    worker_id=worker_id,
                    work_item_id=work_item_id,
                )
                search_ms = (time.perf_counter() - search_started) * 1000
                if execution.status == "completed" and execution.result is not None:
                    result = execution.result
                    child_outcome_label = "completed"
                elif execution.status == "retained":
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
            committed = await self._commit_and_speak(result, origin)
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
            if not turn_recorder.finalized:
                turn_recorder.finalize(outcome="cancelled")
            raise
        except Exception:
            if not turn_recorder.finalized:
                turn_recorder.finalize(outcome="failed")
            raise

    async def _handle_pending(
        self,
        outcome: Any,
        transcript: str,
        origin: Any,
        turn_id: str,
        turn_recorder: AppTurnRecorder,
    ) -> Any:
        try:
            pending = getattr(outcome, "pending_dialogue", None)
            owner_id = pending.owner_id if pending is not None else None
            if owner_id is None:
                owner_id = outcome.work_items[0] if outcome.work_items else None
            registered = self.coordinator.registry.get(owner_id) if owner_id else None
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
            clarification_context = self._clarification_context(pending, transcript)
            child = turn_recorder.new_child(work_item_id=work_item_id)
            retained_recorder = self._new_retained_recorder(
                origin_epoch=origin.epoch,
                turn_id=turn_id,
                work_item_id=work_item_id,
                app_worker_id=worker_id,
            )
            outcome_label = "completed"
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

            submitted = await self.coordinator.submit(
                work_item_id,
                [(worker_id, transcript)],
                execute,
                on_late_complete=lambda late: self._commit_late_result(late, origin.epoch),
                work_item_ids=[work_item_id],
                on_late_terminal=on_late_terminal,
            )
            if submitted.results:
                result = submitted.results[0]
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
                child.finalize(outcome="retained", app_worker_id=worker_id)
                self._register_retained_recorder_if_open(work_item_id, retained_recorder)
            else:
                failure_outcome = (
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
                child.finalize(outcome=failure_outcome, app_worker_id=worker_id)
            committed = await self._commit_and_speak(result, origin)
            turn_recorder.finalize()
            return committed
        except asyncio.CancelledError:
            if not turn_recorder.finalized:
                turn_recorder.finalize(outcome="cancelled")
            raise
        except Exception:
            if not turn_recorder.finalized:
                turn_recorder.finalize(outcome="failed")
            raise

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
                    registered = self.coordinator.registry.get(pending.owner_id)
                    worker = registered.worker if registered is not None else None
                else:
                    catalogue = self.coordinator.registry.catalogue()
                    try:
                        envelope = await asyncio.to_thread(
                            self.coordinator.router.route_envelope,
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
                runnable.append((worker_id, item_text))
                runnable_indexes.append(index)
                runnable_workers[index] = worker
                contexts[index] = (
                    self._clarification_context(pending, item_text)
                    if index == 0 and pending is not None
                    else None
                )

            index_to_worker_id = dict(zip(runnable_indexes, runnable, strict=True))

            execution_indexes: dict[tuple[str, str], list[int]] = {}
            for item_index, item in zip(runnable_indexes, runnable, strict=True):
                execution_indexes.setdefault(item, []).append(item_index)

            outcome_labels: dict[int, str] = {}

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
                    original_query = (
                        contexts[item_index].original_query
                        if contexts[item_index] is not None
                        else query
                    )
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

            submitted = await self.coordinator.submit(
                f"work-{turn_id}",
                runnable,
                execute,
                on_late_complete=lambda late: self._commit_late_result(late, origin.epoch),
                work_item_ids=work_item_ids,
                on_late_terminal=on_late_terminal,
            )
            result_indexes = {
                result.turn_id: index
                for index in runnable_indexes
                for result in submitted.results
                if result.turn_id == f"{turn_id}-{index}"
            }
            for result in submitted.results:
                index = result_indexes[result.turn_id]
                results[index] = result
                worker_id = index_to_worker_id[index][0]
                child_recorders[index].finalize(
                    outcome=outcome_labels.get(index, "completed"),
                    app_worker_id=worker_id,
                    result_id=result.result_id,
                )
            for work_item_id in submitted.pending_work_item_ids:
                item_index = int(work_item_id.rsplit("-", 1)[1])
                worker_id = index_to_worker_id[item_index][0]
                results[item_index] = canonical_result(
                    worker_id=worker_id,
                    turn_id=f"{turn_id}-{item_index}",
                    text="That item is taking longer than expected; I will continue in the background.",
                    origin_epoch=origin.epoch,
                )
                child_recorders[item_index].finalize(outcome="retained", app_worker_id=worker_id)
                recorder = retained_recorders.get(work_item_id)
                if recorder is not None:
                    self._register_retained_recorder_if_open(work_item_id, recorder)
            for failure in submitted.failures:
                item_index = int(failure.work_item_id.rsplit("-", 1)[1])
                results[item_index] = canonical_result(
                    worker_id=failure.worker_id,
                    turn_id=f"{turn_id}-{item_index}",
                    text="The web search is temporarily unavailable.",
                    origin_epoch=origin.epoch,
                )
                child_recorders[item_index].finalize(
                    outcome=self._failure_child_outcome(failure),
                    app_worker_id=failure.worker_id,
                )
            committed = []
            for index in sorted(results):
                committed.append(await self._commit_and_speak(results[index], origin))
            turn_recorder.finalize()
            return tuple(committed)
        except asyncio.CancelledError:
            if not turn_recorder.finalized:
                turn_recorder.finalize(outcome="cancelled")
            raise
        except Exception:
            if not turn_recorder.finalized:
                turn_recorder.finalize(outcome="failed")
            raise

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
    ) -> SearchExecution:
        kwargs: dict[str, Any] = {
            "turn_id": turn_id,
            "origin_epoch": origin_epoch,
        }
        if clarification_context is not None:
            kwargs["clarification_context"] = clarification_context
        starter = getattr(self.coordinator, "start_task", None)
        task = (
            starter(search(query, **kwargs))
            if starter is not None
            else asyncio.create_task(search(query, **kwargs))
        )
        if task is None:
            return SearchExecution("capacity_rejected")
        work_item_id = work_item_id or f"work-{turn_id}"
        self._track_work_task(work_item_id, task)
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
        accepted = self.coordinator.retain_late_task(
            task,
            work_item_id=work_item_id,
            worker_id=worker_id,
            on_complete=lambda late: self._commit_late_result(late, origin_epoch),
            on_late_terminal=self._make_late_terminal_handler({work_item_id: retained_recorder}),
        )
        if not accepted:
            self._retained_recorders.pop(work_item_id, None)
            return SearchExecution("retention_rejected")
        return SearchExecution("retained")

    async def _commit_late_result(self, late: LateResult, origin_epoch: int) -> None:
        """Commit a late result and defer speech on its still-active TTS epoch."""
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

        work_outcome: str | None = None
        commit_outcome: str | None = None
        speech_outcome: str | None = None
        result_id: str | None = None
        pending_exception: Exception | None = None

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
                else:
                    commit_outcome, speech_outcome = "not_applicable", "not_applicable"
                logger.warning(
                    f"Late worker result failed for work_item={late.work_item_id} "
                    f"worker={late.worker_id}"
                )
            elif late.work_item_id in self._cancelled_work_items:
                self._cancelled_work_items.discard(late.work_item_id)
                self._known_work_items.discard(late.work_item_id)
                if isinstance(late.result, GroundedResult):
                    self._clarification_candidates.pop(late.result.result_id, None)
                work_outcome, commit_outcome, speech_outcome = (
                    "cancelled",
                    "suppressed_cancelled",
                    "cancelled",
                )
            elif not isinstance(late.result, GroundedResult):
                self._known_work_items.discard(late.work_item_id)
                work_outcome, commit_outcome, speech_outcome = (
                    "invalid_result",
                    "not_applicable",
                    "not_applicable",
                )
            elif late.result.origin_epoch != origin_epoch:
                self._known_work_items.discard(late.work_item_id)
                work_outcome, commit_outcome, speech_outcome = (
                    "completed",
                    "suppressed_stale",
                    "not_applicable",
                )
            elif any(
                item.result_id == late.result.result_id for item in self.state.results.results
            ):
                self._known_work_items.discard(late.work_item_id)
                work_outcome, commit_outcome, speech_outcome = (
                    "completed",
                    "suppressed_duplicate",
                    "not_applicable",
                )
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
                    work_outcome, commit_outcome = "completed", "committed"
                    result_id = result.result_id
                    if speech_outcome is None:
                        try:
                            origin.scheduler.enqueue(
                                result_id=result.result_id,
                                work_item_id=late.work_item_id,
                                run_id=f"run-{result.turn_id}",
                                text=result.spoken_text,
                                origin_epoch=origin_epoch,
                            )
                        except Exception as exc:  # noqa: BLE001 - preserves existing enqueue-failure re-raise behavior
                            speech_outcome = "enqueue_failed"
                            pending_exception = exc
                        else:
                            try:
                                await origin.scheduler.start_next()
                            except Exception as exc:  # noqa: BLE001 - preserves existing start-failure re-raise behavior
                                speech_outcome = "start_failed"
                                pending_exception = exc
                            else:
                                speech_outcome = "queued"
        finally:
            if recorder is not None:
                recorder.finalize(
                    work_outcome=work_outcome,
                    commit_outcome=commit_outcome,
                    speech_outcome=speech_outcome,
                    result_id=result_id,
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
            self.coordinator.add_worker_clarification(
                session_id=self.state.session_id,
                result_id=result.result_id,
                **candidate,
            )

    async def _commit_and_speak(self, result: GroundedResult, origin: Any) -> GroundedResult:
        """Commit a result and speak only when its originating epoch is active."""
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
            origin.tts is None
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
        )
        await origin.scheduler.start_next(work_item_id)
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
        if catalogue is None:
            return self.coordinator.dispatch(decision)
        return self.coordinator.dispatch(decision, catalogue=catalogue)

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
        if not all(isinstance(value, str) and value for value in (worker_id, topic, model_policy)):
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

    def accepts(self, epoch: int) -> bool:
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
                timeout=getattr(
                    getattr(self.coordinator, "config", None),
                    "shutdown_grace_seconds",
                    2.0,
                ),
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
