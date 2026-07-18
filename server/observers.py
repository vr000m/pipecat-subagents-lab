"""Connection-scoped projection of authoritative session events."""

from __future__ import annotations

import asyncio
import inspect
from typing import Any, Callable

from .contracts import RuntimeSnapshot
from .session_state import SessionState, StateEvent


class RuntimeObserver:
    def __init__(self, state: SessionState, epoch: int) -> None:
        self.state, self.epoch = state, epoch
        self._unsubscribe: Callable[[], None] | None = None

    def subscribe(self, emit: Callable[[Any], Any]) -> Callable[[], None]:
        """Forward future authoritative events as epoch-filtered framework frames."""

        def on_event(event: StateEvent) -> None:
            frame = self.frame(event)
            if frame is None:
                return
            result = emit(frame)
            # A connection emitter is normally a coroutine scheduled by the
            # transport callback. Do not make state mutation await a network send.
            if inspect.isawaitable(result):
                asyncio.create_task(result)

        self.unsubscribe()
        self._unsubscribe = self.state.subscribe(on_event)
        return self._unsubscribe

    def unsubscribe(self) -> None:
        if self._unsubscribe is not None:
            self._unsubscribe()
            self._unsubscribe = None

    def snapshot(self) -> RuntimeSnapshot:
        return self.state.snapshot(origin_epoch=self.epoch)

    def messages(self, after_sequence: int = 0) -> tuple[dict[str, Any], ...]:
        return tuple(
            {
                "contract_version": "v1.0",
                "session_id": self.state.session_id,
                "sequence": event.sequence,
                "kind": event.kind,
                "data": event.payload,
                "origin_epoch": (
                    event.payload["origin_epoch"]
                    if event.payload.get("origin_epoch") is not None
                    else self.epoch
                ),
            }
            for event in self.state.events
            if event.sequence > after_sequence and event.payload.get("origin_epoch") == self.epoch
        )

    def frame(self, event: StateEvent) -> Any:
        """Build the framework frame when available, while keeping tests dependency-free."""
        if event.payload.get("origin_epoch") != self.epoch:
            return None
        payload = {
            "contract_version": "v1.0",
            "session_id": self.state.session_id,
            "sequence": event.sequence,
            "kind": event.kind,
            "data": event.payload,
            "origin_epoch": (
                event.payload["origin_epoch"]
                if event.payload.get("origin_epoch") is not None
                else self.epoch
            ),
        }
        try:
            from pipecat.processors.frameworks.rtvi.frames import RTVIServerMessageFrame
        except ImportError:
            return payload
        return RTVIServerMessageFrame(data=payload)
