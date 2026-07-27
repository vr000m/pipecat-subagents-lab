"""Persistent context-owner worker primitives."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

try:  # Pipecat versions with distributed worker support can provide this class.
    from pipecat.processors.frameworks.llm_context import LLMContextWorker as _NativeWorker
except ImportError:  # pragma: no cover - exercised by the pinned 1.6.0 fallback runtime
    from pipecat.workers.base_worker import BaseWorker as _NativeWorker


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
                super().__init__(name=metadata.worker_id)
            except TypeError:
                pass
        self.metadata = metadata
        self.status = "idle"
        self.history: list[dict[str, Any]] = []
        self._tail: asyncio.Future[Any] | None = None

    async def run(self, params: Any) -> None:
        """Use Pipecat's real bus-worker lifecycle for durable context owners."""
        await super().run(params)

    async def submit(self, operation: Callable[[], Awaitable[Any] | Any]) -> Any:
        previous = self._tail
        loop = asyncio.get_running_loop()

        async def run() -> Any:
            if previous is not None:
                try:
                    await previous
                except BaseException:  # noqa: BLE001  # intentional catch-all: a failed
                    # operation must not poison the durable mailbox for every later
                    # submission. The failed task still propagates to its own caller;
                    # later tasks may start a fresh run.
                    logger.debug(
                        f"{self.metadata.worker_id}: previous submission raised while awaiting tail"
                    )
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
