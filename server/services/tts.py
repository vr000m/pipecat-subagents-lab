"""Small, injectable local TTS service seam."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class TTSEndpoint:
    transport: str
    address: str


class LocalTTS:
    def __init__(
        self, endpoint: TTSEndpoint, on_event: Callable[[str, str], Any] | None = None
    ) -> None:
        self.endpoint, self.on_event = endpoint, on_event
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def synthesize(self, text: str, utterance_id: str) -> Any:
        if not self.started:
            raise RuntimeError("TTS service is not started")
        if self.on_event is not None:
            return self.on_event("synthesis_ended", utterance_id)
        return text
