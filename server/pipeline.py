"""Pipecat lifecycle adapter around durable workers and a connection pipeline."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable

from .connection_arbiter import ConnectionArbiter
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

    def __init__(self) -> None:
        if FrameProcessor is not object:
            super().__init__()

    @staticmethod
    def accepts(frame: Any) -> bool:
        return (
            isinstance(frame, dict)
            and frame.get("kind") == "canonical_result"
            and isinstance(frame.get("result_id"), str)
            and isinstance(frame.get("text"), str)
        )

    async def process_frame(self, frame: Any, direction: Any) -> None:
        """Keep the adapter a real Pipecat processor; canonical dicts use the app gate."""
        if FrameProcessor is not object:
            await super().process_frame(frame, direction)


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
        processors=(bridge, CanonicalResultAdapter()),
    )


@dataclass
class ConnectionPipeline:
    epoch: int
    observer: RuntimeObserver
    scheduler: SpeechScheduler
    transport: Any | None = None
    worker: Any | None = None
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


class SessionHost:
    """Process-lifetime host; persistent workers outlive connection pipelines."""

    def __init__(
        self,
        registry: WorkerRegistry | None = None,
        runner_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.state = SessionState()
        self.arbiter = ConnectionArbiter(self.state.session_id, self.state.resume_token)
        self.registry = registry or WorkerRegistry()
        self.runner_factory = runner_factory
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
        registry contexts therefore remain the application context owners. A
        runner-owned wait handle still gives each durable context a real bus/
        lifecycle registration without pretending the missing worker API exists.
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
            worker = registered.worker
            if isinstance(worker, BaseWorker):
                handle = worker
            else:
                handle = self._runner_handles.get(registered.worker_id)
                if handle is None:
                    owner = worker
                    worker_id = registered.worker_id

                    class DurableContextHandle(BaseWorker):
                        def __init__(self) -> None:
                            super().__init__(name=worker_id)
                            self.owner = owner
                            self._stop = asyncio.Event()

                        async def run(self) -> None:
                            await self._stop.wait()

                    handle = DurableContextHandle()
                    self._runner_handles[registered.worker_id] = handle
            if registered.worker_id not in self._runner_handles:
                self._runner_handles[registered.worker_id] = handle
            if getattr(self.runner, "registry", None) is not None:
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
        pipeline = ConnectionPipeline(
            connection.epoch,
            RuntimeObserver(self.state, connection.epoch),
            SpeechScheduler(self.state),
        )
        self.connection = pipeline
        if old_connection is not None:
            await old_connection.shutdown()
        await self._register_persistent_workers()
        return pipeline

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
