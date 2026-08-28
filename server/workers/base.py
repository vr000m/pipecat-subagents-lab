"""Persistent context-owner worker primitives."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

try:  # Pipecat versions with distributed worker support can provide this class.
    from pipecat.processors.frameworks.llm_context import LLMContextWorker as _NativeWorker
except ImportError:  # pragma: no cover - exercised by the pinned 1.8.0 fallback runtime
    from pipecat.workers.base_worker import BaseWorker as _NativeWorker


@dataclass(frozen=True)
class WorkerMetadata:
    worker_id: str
    worker_type: str
    topic: str
    topic_summary: str
    model_policy: str
    capabilities: dict[str, bool] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.worker_id or not self.worker_id.strip():
            raise ValueError("worker_id must not be empty")


@dataclass(frozen=True)
class ClarificationContext:
    """Typed continuation data rendered only at the provider boundary.

    Lives beside ``WorkerMetadata`` rather than in ``workers/web_search.py``:
    nothing about original_query/question/answer is web-search-specific, and
    ``server/worker_projection.py`` -- a session-level slice of the SessionHost
    decomposition, whose siblings depend only on .contracts/.config/
    .session_state/.perf_metrics -- needs this type. Importing it from a
    concrete worker implementation coupled that slice to web_search, and a
    second worker type offering clarification would have had to either import
    from web_search or grow a parallel type (round-5 restart, Architecture
    Minor).
    """

    original_query: str
    question: str
    answer: str

    def provider_query(self) -> str:
        def bounded(value: str, limit: int) -> str:
            return " ".join(value.strip().split())[:limit]

        return (
            f"Original request: {bounded(self.original_query, 650)}\n"
            f"Clarification asked: {bounded(self.question, 400)}\n"
            f"User answer: {bounded(self.answer, 800)}"
        )


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
