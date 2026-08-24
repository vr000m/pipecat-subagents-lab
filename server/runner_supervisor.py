"""Persistent-worker-runner lifecycle owned by a SessionHost.

Extracted from SessionHost (deep-review Architecture finding #1: SessionHost
god class) as the "runner supervision" slice: constructing (or accepting a
test-injected) ``WorkerRunner``, starting/running it, registering durable
workers with it once, and tearing it down in shutdown.

Session/turn/coordinator state stays on SessionHost -- this class only knows
about the runner instance and the worker registrations it owns. Callers pass
in whatever runner-agnostic inputs it needs (e.g. the registry's worker list
to ``start``); it does not reach back into SessionHost for them.

This module is a pure extraction of SessionHost's existing runner-lifecycle
logic -- semantics, including registration de-duplication and shutdown
ordering, are unchanged from the code that previously lived directly on
SessionHost. ``SessionHost.shutdown`` still controls *when* each half of this
class's teardown runs relative to its other cleanup (registration
cancellation happens early, the final runner stop/cancel happens last) --
this class only owns *how* each half is performed.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Iterable
from typing import Any


class RunnerSupervisor:
    """Owns a SessionHost's persistent ``WorkerRunner`` and its worker registrations.

    ``runner_factory`` lets tests and callers inject a fake/lightweight
    runner in place of the real ``pipecat.pipeline.runner.WorkerRunner``; when
    absent, ``start`` constructs the real one. Worker registration
    (``register_worker``) is idempotent per ``worker_id`` and coalesces
    concurrent registration attempts for the same worker onto one in-flight
    task, exactly as the pre-extraction SessionHost code did.
    """

    def __init__(self, runner_factory: Callable[[], Any] | None = None) -> None:
        self.runner_factory = runner_factory
        self.runner: Any = None
        self._runner_registered: set[str] = set()
        self._runner_registrations: dict[str, asyncio.Task[None]] = {}
        self._runner_task: asyncio.Task[Any] | None = None

    async def start(self, workers: Callable[[], Iterable[Any]]) -> None:
        """Construct (or invoke the injected factory for) the runner and start it.

        ``workers`` is a thunk returning the owning SessionHost's
        ``registry.workers`` -- deferred so it is only evaluated once the
        runner is confirmed to accept registrations (some test registries
        provide no ``.workers`` at all, matching the pre-extraction code's
        early return before ever touching the registry). Each entry's
        ``.worker`` is registered with the runner when it is a ``BaseWorker``
        (see ``register_persistent_workers``).
        """
        if self.runner_factory is not None:
            self.runner = self.runner_factory()
        else:
            from pipecat.pipeline.runner import WorkerRunner

            self.runner = WorkerRunner(name="websearch-session", handle_sigint=False)
        await self.register_persistent_workers(workers)
        start = getattr(self.runner, "start", None)
        if start is not None:
            result = start()
            if hasattr(result, "__await__"):
                await result
        else:
            run = getattr(self.runner, "run", None)
            if run is not None:
                self._runner_task = asyncio.create_task(run(auto_end=False))

    async def register_persistent_workers(self, workers: Callable[[], Iterable[Any]]) -> None:
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

        for registered in workers():
            if isinstance(registered.worker, BaseWorker):
                await self.register_worker(registered.worker)

    async def register_worker(self, worker: Any) -> None:
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
                self._runner_registered.add(worker_id)

            registration = asyncio.create_task(register())
            self._runner_registrations[worker_id] = registration

            def completed(completed_task: asyncio.Task[Any]) -> None:
                if self._runner_registrations.get(worker_id) is completed_task:
                    self._runner_registrations.pop(worker_id, None)

            registration.add_done_callback(completed)
        await asyncio.shield(registration)

    async def cancel_registrations(self) -> None:
        """Cancel and await any still-pending worker registrations.

        Called early in ``SessionHost.shutdown`` -- before the final runner
        stop/cancel -- so a stalled ``add_workers`` call cannot hold shutdown
        open past the rest of the session's teardown.
        """
        registrations = tuple(self._runner_registrations.values())
        for task in registrations:
            task.cancel()
        if registrations:
            await asyncio.gather(*registrations, return_exceptions=True)

    async def shutdown(self) -> None:
        """Stop/cancel the runner and cancel any owned run task.

        Called last in ``SessionHost.shutdown``, after every other cleanup
        has settled, matching the pre-extraction ordering.
        """
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
