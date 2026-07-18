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

    def subscribe(self, emit: Callable[[Any], Any]) -> None:
        """Forward future authoritative events as epoch-filtered framework frames."""

        def on_event(event: StateEvent) -> None:
            frame = self.frame(event)
            if frame is None or event.kind not in {"result", "speech_progress"}:
                return
            result = emit(frame)
            # A connection emitter is normally a coroutine scheduled by the
            # transport callback. Do not make state mutation await a network send.
            if inspect.isawaitable(result):
                asyncio.create_task(result)

        self.state.subscribe(on_event)

    def snapshot(self) -> RuntimeSnapshot:
        return self.state.snapshot(origin_epoch=self.epoch)

    def messages(self, after_sequence: int = 0) -> tuple[dict[str, Any], ...]:
        return tuple(
            {
                "contract_version": "v1.0",
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
            if event.sequence > after_sequence
            and event.payload.get("origin_epoch") in (None, self.epoch)
        )

    def frame(self, event: StateEvent) -> Any:
        """Build the framework frame when available, while keeping tests dependency-free."""
        if event.payload.get("origin_epoch") not in (None, self.epoch):
            return None
        payload = {
            "contract_version": "v1.0",
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
