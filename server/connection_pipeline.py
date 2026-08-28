"""``ConnectionPipeline``: per-connection runtime state for a live session.

Extracted mechanically from ``server/pipeline.py`` per Phase 2 of
``docs/dev_plans/20260824-refactor-sessionhost-decomposition.md`` (Requirement
3: "``ConnectionPipeline`` moves to its own module with no re-export from
``pipeline.py``"). This is a pure move -- no behavior change -- of the class
body that previously lived at ``server/pipeline.py:362``.

Dependency direction: this module is imported by ``server/pipeline.py`` and
must never import back from it (see the plan's Architecture & Call Flow
section).
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from loguru import logger

from .observers import RuntimeObserver
from .speech_lifecycle import SpeechLifecycleCoordinator
from .speech_scheduler import SpeechScheduler


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
        """Whether this connection negotiated the ``work_status_v1`` capability.

        Delegates to the ``RuntimeObserver`` rather than testing
        ``capabilities`` directly, so the single capability-gate predicate in
        ``resolve_work_status_wire_presence`` stays the sole arbiter of
        entitlement (Phase 3).
        """
        return self.observer.supports_work_status

    def deactivate(self, *, reconnect: bool = True) -> None:
        self.active = False
        # full_stop=True: deactivate() always means this connection is being
        # retired for good (shutdown or reconnect-promotion), so its queued
        # and paused speech items must be swept regardless of `reconnect`'s
        # value -- not just on a genuine reconnect (server/speech_scheduler.py
        # SpeechScheduler.interrupt docstring).
        self.scheduler.interrupt(epoch=self.epoch, reconnect=reconnect, full_stop=True)

    async def shutdown(self, *, reason: str = "connection replaced", reconnect: bool) -> None:
        """Fence this connection and stop its Pipecat worker, if attached.

        Always forces scheduler cleanup, even if something upstream (e.g. a
        failed output teardown) already set ``active = False`` directly
        without releasing the scheduler's active lease.

        ``reconnect`` is an explicit, caller-supplied classification -- not
        inferred from ``reason``. ``reason`` is a free-text diagnostic string
        with no stable set of reconnect-implying values (e.g. "connection
        replaced during setup" is just as much a reconnect as "connection
        replaced", but would not match an exact-string check); inferring
        ``reconnect`` from it silently mis-classifies any call site whose
        wording doesn't match exactly, dropping a still-active utterance's
        origin-epoch fencing (see ``SpeechScheduler.interrupt``).
        """
        self.deactivate(reconnect=reconnect)
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
