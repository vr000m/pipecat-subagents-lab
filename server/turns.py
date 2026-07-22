"""Logical user-turn processing for segmented local speech recognition."""

from __future__ import annotations

import asyncio
import inspect
from typing import Any, Callable

from loguru import logger
from pipecat.audio.turn.smart_turn.local_smart_turn_v3 import LocalSmartTurnAnalyzerV3
from pipecat.frames.frames import (
    CancelFrame,
    EndFrame,
    TranscriptionFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.turns.user_stop import TurnAnalyzerUserTurnStopStrategy
from pipecat.turns.user_turn_processor import UserTurnProcessor
from pipecat.turns.user_turn_strategies import UserTurnStrategies


def smart_turn_processor() -> UserTurnProcessor:
    """Build the Pipecat 1.6 semantic end-of-turn processor explicitly."""
    return UserTurnProcessor(
        user_turn_strategies=UserTurnStrategies(
            stop=[
                TurnAnalyzerUserTurnStopStrategy(
                    turn_analyzer=LocalSmartTurnAnalyzerV3(),
                )
            ]
        )
    )


class FinalTurnTranscriptProcessor(FrameProcessor):
    """Route finalized STT fragments only after Smart Turn closes the turn."""

    def __init__(self, on_final: Callable[[str], Any] | None) -> None:
        super().__init__()
        self._on_final = on_final
        self._fragments: list[str] = []
        self._dispatch_tasks: set[asyncio.Task[None]] = set()

    async def _dispatch(self, text: str) -> None:
        if self._on_final is None:
            return
        try:
            result = self._on_final(text)
            if inspect.isawaitable(result):
                await result
        except Exception:
            logger.exception(f"{self}: failed to route completed user turn")

    def _dispatch_in_background(self, text: str) -> None:
        task = asyncio.create_task(self._dispatch(text))
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_tasks.discard)

    async def process_frame(self, frame: Any, direction: FrameDirection) -> None:
        """Collect final STT fragments and route once per semantic user turn."""
        await super().process_frame(frame, direction)

        if direction == FrameDirection.DOWNSTREAM:
            if isinstance(frame, TranscriptionFrame):
                text = frame.text.strip()
                if text:
                    self._fragments.append(text)
            elif isinstance(frame, UserStoppedSpeakingFrame):
                text = " ".join(self._fragments)
                self._fragments.clear()
                if text:
                    self._dispatch_in_background(text)
            elif isinstance(frame, (CancelFrame, EndFrame)):
                self._fragments.clear()

        await self.push_frame(frame, direction)
