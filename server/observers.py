"""Connection-scoped projection of authoritative session events."""

from __future__ import annotations

from typing import Any

from .contracts import RuntimeSnapshot
from .session_state import SessionState, StateEvent


class RuntimeObserver:
    def __init__(self, state: SessionState, epoch: int) -> None:
        self.state, self.epoch = state, epoch

    def snapshot(self) -> RuntimeSnapshot:
        return self.state.snapshot(origin_epoch=self.epoch)

    def messages(self, after_sequence: int = 0) -> tuple[dict[str, Any], ...]:
        return tuple(
            {
                "contract_version": "v1.0",
                "session_sequence": event.sequence,
                "type": event.kind,
                "data": event.payload,
                "origin_epoch": self.epoch,
            }
            for event in self.state.events
            if event.sequence > after_sequence
        )

    def frame(self, event: StateEvent) -> Any:
        """Build the framework frame when available, while keeping tests dependency-free."""
        payload = {
            "contract_version": "v1.0",
            "session_sequence": event.sequence,
            "type": event.kind,
            "data": event.payload,
            "origin_epoch": self.epoch,
        }
        try:
            from pipecat.frames.frames import RTVIServerMessageFrame
        except ImportError:
            return payload
        return RTVIServerMessageFrame(data=payload)
