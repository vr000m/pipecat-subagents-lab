"""Small, injectable local STT service seam."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class STTEndpoint:
    transport: str
    address: str


class LocalSTT:
    def __init__(self, endpoint: STTEndpoint, on_final: Callable[[str], Any] | None = None) -> None:
        self.endpoint, self.on_final = endpoint, on_final
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def handle_final_transcript(self, text: str) -> Any:
        if not self.started:
            raise RuntimeError("STT service is not started")
        if self.on_final is not None:
            return self.on_final(text)
        return text
