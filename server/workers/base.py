"""Persistent context-owner worker primitives."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

try:  # Pipecat versions with distributed worker support can provide this class.
    from pipecat.processors.frameworks.llm_context import LLMContextWorker as _NativeWorker
except ImportError:  # pragma: no cover - exercised by the pinned 1.4.0 fallback runtime
    _NativeWorker = object


@dataclass(frozen=True)
class WorkerMetadata:
    worker_id: str
    worker_type: str
    topic: str
    topic_summary: str
    model_policy: str
    capabilities: dict[str, bool] = field(default_factory=dict)


class ContextWorker(_NativeWorker):
    """A small durable worker with one causal mailbox per context owner."""

    def __init__(self, metadata: WorkerMetadata) -> None:
        if _NativeWorker is not object:
            try:
                super().__init__()
            except TypeError:
                pass
        self.metadata = metadata
        self.status = "idle"
        self.history: list[dict[str, Any]] = []
        self._tail: asyncio.Future[Any] | None = None

    async def submit(self, operation: Callable[[], Awaitable[Any] | Any]) -> Any:
        previous = self._tail
        loop = asyncio.get_running_loop()

        async def run() -> Any:
            if previous is not None:
                await previous
            self.status = "running"
            try:
                value = operation()
                if asyncio.iscoroutine(value):
                    value = await value
                return value
            finally:
                self.status = "idle"

        task = loop.create_task(run())
        self._tail = task
        return await task

    def append_context(self, entry: dict[str, Any]) -> None:
        self.history.append(dict(entry))
