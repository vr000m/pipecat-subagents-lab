"""Pipecat lifecycle adapter around durable workers and a connection pipeline."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable

from .connection_arbiter import ConnectionArbiter
from .observers import RuntimeObserver
from .registry import WorkerRegistry
from .session_state import SessionState
from .speech_scheduler import SpeechScheduler


class BusBridgeProcessor:
    """Small named seam for the Pipecat bus bridge used by the connection pipeline.

    Pipecat 1.4 exposes the framework bridge in ``pipecat.bus.bridge_processor``;
    keeping this connection-facing marker injectable also lets contract tests run
    without opening a media transport.
    """


class CanonicalResultAdapter:
    """Admit only normalized result frames to the transport side of the pipeline."""

    @staticmethod
    def accepts(frame: Any) -> bool:
        return (
            isinstance(frame, dict)
            and frame.get("kind") == "canonical_result"
            and isinstance(frame.get("result_id"), str)
            and isinstance(frame.get("text"), str)
        )


@dataclass
class LabPipeline:
    transport: Any
    stt: Any
    tts: Any
    processors: tuple[Any, ...] = field(
        default_factory=lambda: (BusBridgeProcessor(), CanonicalResultAdapter())
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
    return LabPipeline(transport=transport, stt=stt, tts=tts)


@dataclass
class ConnectionPipeline:
    epoch: int
    observer: RuntimeObserver
    scheduler: SpeechScheduler
    active: bool = True

    def deactivate(self) -> None:
        self.active = False
        self.scheduler.interrupt(epoch=self.epoch, reconnect=True)


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

    async def connect(self, handshake: Any) -> ConnectionPipeline:
        if not self.started:
            await self.start()
        connection = self.arbiter.promote(handshake)
        if self.connection is not None:
            self.connection.deactivate()
        self.state.active_epoch = connection.epoch
        pipeline = ConnectionPipeline(
            connection.epoch,
            RuntimeObserver(self.state, connection.epoch),
            SpeechScheduler(self.state),
        )
        self.connection = pipeline
        return pipeline

    def accepts(self, epoch: int) -> bool:
        return (
            self.arbiter.accepts(epoch) and self.connection is not None and self.connection.active
        )

    async def shutdown(self) -> None:
        if self.connection is not None:
            self.connection.deactivate()
            self.connection = None
        stop = getattr(self.runner, "stop", None)
        if stop is not None:
            result = stop()
            if hasattr(result, "__await__"):
                await result
        elif self.runner is not None:
            cancel = getattr(self.runner, "cancel", None)
            if cancel is not None:
                cancel("session shutdown")
        if self._runner_task is not None:
            try:
                await self._runner_task
            except asyncio.CancelledError:
                pass
            self._runner_task = None
        self.started = False
        self.state.active_epoch = None
