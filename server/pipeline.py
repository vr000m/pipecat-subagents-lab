"""Pipecat lifecycle adapter around durable workers and a connection pipeline."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable

from pipecat.frames.frames import TTSSpeakFrame
from pipecat.processors.frameworks.rtvi.frames import RTVIServerMessageFrame

from .connection_arbiter import ConnectionArbiter
from .contracts import CONTRACT_VERSION
from .observers import RuntimeObserver
from .registry import WorkerRegistry
from .session_state import SessionState
from .speech_scheduler import SpeechScheduler


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
    """Construct the pinned framework bridge; fallback is intentionally explicit."""
    if getattr(BusBridgeProcessor, "framework_fallback", False):
        return BusBridgeProcessor()
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
    """Admit only normalized result frames to the transport side of the pipeline."""

    _PUBLIC_RTVI_KINDS = frozenset(
        {"runtime_snapshot", "result", "speech", "worker", "speech_progress"}
    )

    def __init__(self) -> None:
        if FrameProcessor is not object:
            super().__init__()

    @staticmethod
    def accepts(frame: Any) -> bool:
        data = getattr(frame, "data", frame)
        if isinstance(data, dict) and data.get("kind") == "canonical_result":
            data = data.get("data", data)
        if isinstance(data, dict) and all(
            isinstance(data.get(field), str)
            for field in ("result_id", "text", "worker_id", "turn_id")
        ):
            return True
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
        """Keep the adapter a real Pipecat processor; canonical dicts use the app gate."""
        if direction != getattr(FrameDirection, "DOWNSTREAM", direction) or not self.accepts(frame):
            return
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
        if not CanonicalResultAdapter.accepts(frame):
            return False
        frames = getattr(self.transport, "frames", None)
        if frames is None:
            raise TypeError("transport must expose a frames collection for canonical output")
        frames.append(frame)
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
    transport: Any | None = None
    worker: Any | None = None
    worker_task: asyncio.Task[Any] | None = None
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
                result = cancel(reason=reason)
                if hasattr(result, "__await__"):
                    await result
            self.worker = None
        if self.worker_task is not None:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass
            finally:
                self.worker_task = None
        self.observer.unsubscribe()


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

        Pipecat 1.4.0 does not expose the planned ``LLMContextWorker`` module;
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

        async def queue_speech(item: Any) -> None:
            if (
                self.tts is None
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
            SpeechScheduler(self.state, speak=queue_speech if self.tts is not None else None),
        )
        if self.stt is not None and self.coordinator is not None:
            self.stt.on_final = self._handle_transcript
        if self.tts is not None and hasattr(self.tts, "on_event"):

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
                    # Pipecat 1.4.0 creates its own TTSSpeakFrame context ID.
                    # The scheduler's one-active-lease invariant is the only
                    # correlation available without claiming playout completion.
                    pipeline.scheduler.synthesis_ended(pipeline.scheduler.active.item.utterance_id)
                return callback_result

            self.tts.on_event = on_tts_event
        self.connection = pipeline
        if old_connection is not None:
            await old_connection.shutdown()
        await self._register_persistent_workers()
        return pipeline

    async def _handle_transcript(self, transcript: str) -> Any:
        """Route a final local-STT turn through the application coordinator."""
        if self.coordinator is None or self.connection is None:
            return transcript
        origin = self.connection
        origin_epoch = origin.epoch
        outcome = await asyncio.to_thread(
            self.coordinator.arbitrate, self.state.session_id, transcript
        )
        if outcome.kind != "routed" or outcome.decision is None:
            return outcome
        worker = self.coordinator.dispatch(outcome.decision)
        if worker is None:
            return outcome
        search = getattr(worker, "search", None)
        if search is None:
            return outcome
        result = await search(
            transcript,
            turn_id=f"turn-{self.state.sequence + 1}",
            origin_epoch=origin_epoch,
        )
        self.state.append_result(result, origin_epoch=origin_epoch)
        if (
            self.tts is None
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
        return {
            "contract_version": "v1.0",
            "session_id": self.state.session_id,
            "resume_token": self.state.resume_token,
            "proposed_epoch": self.arbiter.epoch + 1,
            "snapshot_sequence": self.state.sequence,
        }

    def accepts(self, epoch: int) -> bool:
        return (
            self.arbiter.accepts(epoch) and self.connection is not None and self.connection.active
        )

    async def shutdown(self) -> None:
        if self.connection is not None:
            await self.connection.shutdown(reason="session shutdown")
            self.connection = None
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
