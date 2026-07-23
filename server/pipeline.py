"""Pipecat lifecycle adapter around durable workers and a connection pipeline."""

from __future__ import annotations

import asyncio
import inspect
import time
from dataclasses import dataclass, field
from typing import Any, Callable
from uuid import uuid4

from loguru import logger
from pydantic import ValidationError

from pipecat.frames.frames import TTSSpeakFrame
from pipecat.processors.frameworks.rtvi.frames import RTVIServerMessageFrame

from .connection_arbiter import ConnectionArbiter
from .contracts import (
    CONTRACT_VERSION,
    GroundedResult,
    RoutingDecision,
    RoutingState,
    TranscriptEntry,
    WorkerState,
)
from .observers import RuntimeObserver
from .registry import UnsupportedWorkerType, WorkerRegistry
from .results import canonical_result
from .router import RoutingValidationError
from .session_state import SessionState
from .speech_scheduler import SpeechScheduler
from .workers.web_search import WorkerDeclined


try:
    from pipecat.bus.bridge_processor import BusBridgeProcessor as BusBridgeProcessor
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

    _PUBLIC_RTVI_KINDS = frozenset(
        {
            "runtime_snapshot",
            "result",
            "speech",
            "worker",
            "speech_progress",
            "routing",
            "user_transcript",
            "bot_transcript",
        }
    )

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
        sequence = data.get("sequence")
        origin_epoch = data.get("origin_epoch")
        return (
            data.get("contract_version") == CONTRACT_VERSION
            and isinstance(data.get("session_id"), str)
            and isinstance(sequence, int)
            and not isinstance(sequence, bool)
            and sequence >= 0
            and data.get("kind") in CanonicalResultAdapter._PUBLIC_RTVI_KINDS
            and isinstance(data.get("data"), dict)
            and (
                origin_epoch is None
                or (isinstance(origin_epoch, int) and not isinstance(origin_epoch, bool))
            )
        )

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

    def deactivate(self) -> None:
        self.active = False
        self.scheduler.interrupt(epoch=self.epoch, reconnect=True)

    async def shutdown(self, *, reason: str = "connection replaced") -> None:
        """Fence this connection and stop its Pipecat worker, if attached."""
        self.deactivate()
        if self.worker is not None:
            cancel = getattr(self.worker, "cancel", None)
            if cancel is not None:
                try:
                    result = cancel(reason=reason)
                    if hasattr(result, "__await__"):
                        await result
                except Exception:
                    pass
            self.worker = None
        if self.worker_task is not None:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except BaseException:
                pass
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
            except Exception:
                pass


class SessionHost:
    """Process-lifetime host; persistent workers outlive connection pipelines."""

    def __init__(
        self,
        registry: WorkerRegistry | None = None,
        runner_factory: Callable[[], Any] | None = None,
        stt: Any | None = None,
        tts: Any | None = None,
        coordinator: Any | None = None,
    ) -> None:
        self.state = SessionState()
        self.arbiter = ConnectionArbiter(self.state.session_id, self.state.resume_token)
        self.registry = registry or WorkerRegistry()
        self.runner_factory = runner_factory
        self.stt, self.tts = stt, tts
        self.coordinator = coordinator
        self._tts_on_event = getattr(tts, "on_event", None)
        self.runner: Any = None
        self._runner_handles: dict[str, Any] = {}
        self._runner_registered: set[str] = set()
        self._runner_task: asyncio.Task[Any] | None = None
        self.connection: ConnectionPipeline | None = None
        self._background_shutdowns: set[asyncio.Task[None]] = set()
        self._handshake_tokens: dict[str, tuple[int, float, bool]] = {}
        self._turn_sequence = 0
        self.started = False

    async def start(self) -> None:
        if self.started:
            return
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
            if registered.worker_id in self._runner_registered:
                continue
            handle = registered.worker
            if isinstance(handle, BaseWorker):
                result = add_workers(handle)
                if inspect.isawaitable(result):
                    await result
                self._runner_registered.add(registered.worker_id)

    async def connect(self, handshake: Any) -> ConnectionPipeline:
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
            await pipeline.worker.queue_frame(
                TTSSpeakFrame(text=item.text, append_to_context=False)
            )

        pipeline = ConnectionPipeline(
            connection.epoch,
            RuntimeObserver(self.state, connection.epoch),
            SpeechScheduler(self.state, speak=queue_speech if connection_tts is not None else None),
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
                if (
                    event == "synthesis_ended"
                    and self.connection is pipeline
                    and pipeline.active
                    and pipeline.scheduler.active is not None
                ):
                    # Pipecat 1.6.0 creates its own TTSSpeakFrame context ID.
                    # The scheduler's one-active-lease invariant is the only
                    # correlation available without claiming playout completion.
                    utterance_id = pipeline.scheduler.active.item.utterance_id
                    pipeline.scheduler.synthesis_ended(utterance_id)
                    # The pinned local service exposes synthesis completion, not
                    # browser playout completion. Release conservatively as
                    # unknown so later utterances cannot be starved.
                    pipeline.scheduler.delivery_unknown(utterance_id)
                elif (
                    event == "delivery_completed"
                    and self.connection is pipeline
                    and pipeline.active
                    and pipeline.scheduler.active is not None
                ):
                    pipeline.scheduler.delivery_completed(
                        pipeline.scheduler.active.item.utterance_id
                    )
                elif (
                    event == "delivery_unknown"
                    and self.connection is pipeline
                    and pipeline.active
                    and pipeline.scheduler.active is not None
                ):
                    pipeline.scheduler.delivery_unknown(pipeline.scheduler.active.item.utterance_id)
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

    def validate_handshake_token(self, token: str, proposed_epoch: int, *, redeem: bool) -> bool:
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

    async def _handle_transcript(
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
        self.state.append_transcript(
            TranscriptEntry(
                role="user",
                text=transcript,
                turn_id=turn_id,
                origin_epoch=origin_epoch,
            )
        )
        try:
            outcome = await asyncio.to_thread(
                self.coordinator.arbitrate, self.state.session_id, transcript
            )
        except Exception:
            logger.exception(
                f"Routing failed for {turn_id}; returning a safe result without provider details"
            )
            return await self._commit_and_speak(
                canonical_result(
                    worker_id="main",
                    turn_id=turn_id,
                    text="Routing is temporarily unavailable. Please try that request again.",
                    origin_epoch=origin_epoch,
                ),
                origin,
            )
        if outcome.kind != "routed" or outcome.decision is None:
            if outcome.kind == "control":
                action = getattr(outcome, "control_action", None)
                if action == "pause" and origin.scheduler.active is not None:
                    target = (
                        outcome.work_items[0]
                        if outcome.work_items
                        else origin.scheduler.active.item.work_item_id
                    )
                    origin.scheduler.pause(target)
                elif action in {"cancel", "stop"}:
                    origin.scheduler.interrupt(epoch=origin_epoch)
                text = {
                    "pause": "Pausing the active response.",
                    "resume": "Resume requested; no audio was replayed automatically.",
                    "cancel": "Cancelling the active response.",
                    "stop": "Stopping the active response.",
                }.get(action, "Control request noted.")
            elif outcome.kind == "multi_intent":
                return await self._handle_multi_intent(outcome, transcript, origin, turn_id)
            elif outcome.kind == "continue_pending":
                return await self._handle_pending(outcome, transcript, origin, turn_id)
            else:
                text = None
            if text is None:
                return outcome
            return await self._commit_and_speak(
                canonical_result(
                    worker_id="main",
                    turn_id=turn_id,
                    text=text,
                    origin_epoch=origin_epoch,
                ),
                origin,
            )
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
            router = getattr(self.coordinator, "router", None)
            text = getattr(router, "last_prose", None)
            if not text:
                text = {
                    "direct": "I could not produce a direct answer yet.",
                    "unsupported": "I cannot access that capability here.",
                    "clarify": "Could you clarify what you want me to search for?",
                }[action]
            return await self._commit_and_speak(
                canonical_result(
                    worker_id="main",
                    turn_id=turn_id,
                    text=text,
                    origin_epoch=origin_epoch,
                ),
                origin,
            )
        try:
            worker = self._dispatch(outcome.decision, getattr(outcome, "catalogue", None))
        except (RoutingValidationError, UnsupportedWorkerType):
            return await self._commit_and_speak(
                canonical_result(
                    worker_id="main",
                    turn_id=turn_id,
                    text="I cannot access that capability here.",
                    origin_epoch=origin_epoch,
                ),
                origin,
            )
        if worker is None:
            return outcome
        self._project_worker(worker, origin_epoch=origin_epoch, status="running")
        search = getattr(worker, "search", None)
        if search is None:
            return outcome
        try:
            result = await search(transcript, turn_id=turn_id, origin_epoch=origin_epoch)
        except WorkerDeclined:
            result = canonical_result(
                worker_id=getattr(getattr(worker, "metadata", None), "worker_id", "main"),
                turn_id=turn_id,
                text="I could not find a reliable result for that request.",
                origin_epoch=origin_epoch,
            )
        except Exception:
            result = canonical_result(
                worker_id=getattr(getattr(worker, "metadata", None), "worker_id", "main"),
                turn_id=turn_id,
                text="The web search is temporarily unavailable.",
                origin_epoch=origin_epoch,
            )
        committed = await self._commit_and_speak(result, origin)
        self._project_worker(
            worker,
            origin_epoch=origin_epoch,
            status="idle",
            latest_result_id=result.result_id,
        )
        return committed

    async def _handle_pending(
        self, outcome: Any, transcript: str, origin: Any, turn_id: str
    ) -> Any:
        owner_id = outcome.work_items[0] if outcome.work_items else None
        registered = self.coordinator.registry.get(owner_id) if owner_id else None
        worker = registered.worker if registered is not None else None
        search = getattr(worker, "search", None)
        if search is None:
            return outcome
        try:
            result = await search(
                transcript,
                turn_id=turn_id,
                origin_epoch=origin.epoch,
            )
        except Exception:
            result = canonical_result(
                worker_id=owner_id or "main",
                turn_id=turn_id,
                text="The pending web request could not be completed.",
                origin_epoch=origin.epoch,
            )
        return await self._commit_and_speak(result, origin)

    async def _handle_multi_intent(
        self, outcome: Any, transcript: str, origin: Any, turn_id: str
    ) -> tuple[Any, ...]:
        """Execute bounded compound work in the user's stated order."""
        del transcript
        results: list[Any] = []
        pending = self.coordinator.pending(self.state.session_id)
        for index, item_text in enumerate(outcome.work_items):
            worker = None
            if index == 0 and pending is not None:
                registered = self.coordinator.registry.get(pending.owner_id)
                worker = registered.worker if registered is not None else None
            else:
                catalogue = self.coordinator.registry.catalogue()
                decision = await asyncio.to_thread(
                    self.coordinator.router.route,
                    item_text,
                    catalogue,
                )
                try:
                    worker = await asyncio.to_thread(self._dispatch, decision, catalogue)
                except (RoutingValidationError, UnsupportedWorkerType):
                    results.append(
                        await self._commit_and_speak(
                            canonical_result(
                                worker_id="main",
                                turn_id=f"{turn_id}-{index + 1}",
                                text="I cannot access that capability here.",
                                origin_epoch=origin.epoch,
                            ),
                            origin,
                        )
                    )
                    continue
            search = getattr(worker, "search", None)
            if search is None:
                continue
            try:
                result = await search(
                    item_text,
                    turn_id=f"{turn_id}-{index + 1}",
                    origin_epoch=origin.epoch,
                )
            except WorkerDeclined:
                result = canonical_result(
                    worker_id=getattr(getattr(worker, "metadata", None), "worker_id", "main"),
                    turn_id=f"{turn_id}-{index + 1}",
                    text="I could not find a reliable result for that request.",
                    origin_epoch=origin.epoch,
                )
            except Exception:
                result = canonical_result(
                    worker_id=getattr(getattr(worker, "metadata", None), "worker_id", "main"),
                    turn_id=f"{turn_id}-{index + 1}",
                    text="The web search is temporarily unavailable.",
                    origin_epoch=origin.epoch,
                )
            results.append(await self._commit_and_speak(result, origin))
        return tuple(results)

    async def _commit_and_speak(self, result: GroundedResult, origin: Any) -> GroundedResult:
        """Commit a result and speak only when its originating epoch is active."""
        origin_epoch = result.origin_epoch
        self.state.append_transcript(
            TranscriptEntry(
                role="assistant",
                text=result.ui_text,
                turn_id=result.turn_id,
                origin_epoch=origin_epoch,
            )
        )
        self.state.append_result(result, origin_epoch=origin_epoch)
        if (
            origin.tts is None
            or self.connection is not origin
            or not origin.active
            or not self.accepts(origin_epoch)
        ):
            return result
        origin.scheduler.enqueue(
            result_id=result.result_id,
            work_item_id=f"work-{result.turn_id}",
            run_id=f"run-{result.turn_id}",
            text=result.spoken_text,
            origin_epoch=origin_epoch,
        )
        await origin.scheduler.start_next()
        return result

    def session_handshake(self) -> dict[str, Any]:
        """Return the next browser handshake without mutating session state."""
        token = uuid4().hex
        self._handshake_tokens[token] = (
            self.arbiter.epoch + 1,
            time.monotonic() + 60,
            False,
        )
        return {
            "contract_version": "v1.0",
            "session_id": self.state.session_id,
            "resume_token": token,
            "proposed_epoch": self.arbiter.epoch + 1,
            "snapshot_sequence": self.state.sequence,
        }

    def _dispatch(self, decision: Any, catalogue: Any = None) -> Any:
        if catalogue is None:
            return self.coordinator.dispatch(decision)
        return self.coordinator.dispatch(decision, catalogue=catalogue)

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
            self.arbiter.accepts(epoch) and self.connection is not None and self.connection.active
        )

    async def shutdown(self) -> None:
        if self.connection is not None:
            await self.connection.shutdown(reason="session shutdown")
            self.connection = None
        if self._background_shutdowns:
            await asyncio.gather(*self._background_shutdowns, return_exceptions=True)
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
