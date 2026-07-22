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
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
    VADUserStartedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.turns.user_stop import TurnAnalyzerUserTurnStopStrategy
from pipecat.turns.user_turn_processor import UserTurnProcessor
from pipecat.turns.user_turn_strategies import UserTurnStrategies


def smart_turn_processor(*, timeout_seconds: float = 5.0) -> UserTurnProcessor:
    """Build the Pipecat 1.6 semantic end-of-turn processor explicitly."""
    logger.debug(f"Smart Turn fallback timeout set to {timeout_seconds:g}s")
    return UserTurnProcessor(
        user_turn_strategies=UserTurnStrategies(
            stop=[
                TurnAnalyzerUserTurnStopStrategy(
                    turn_analyzer=LocalSmartTurnAnalyzerV3(),
                )
            ]
        ),
        user_turn_stop_timeout=timeout_seconds,
    )


class FinalTurnTranscriptProcessor(FrameProcessor):
    """Debounce Smart Turn stops and route one combined application turn."""

    def __init__(
        self,
        on_final: Callable[[str], Any] | None,
        *,
        complete_grace_seconds: float = 1.5,
    ) -> None:
        super().__init__()
        self._on_final = on_final
        self._complete_grace_seconds = complete_grace_seconds
        self._fragments: list[str] = []
        self._dispatch_tasks: set[asyncio.Task[None]] = set()
        self._completion_task: asyncio.Task[None] | None = None

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

    def _cancel_completion(self) -> None:
        if self._completion_task is not None and not self._completion_task.done():
            self._completion_task.cancel()
        self._completion_task = None

    async def _complete_after_grace(self) -> None:
        try:
            await asyncio.sleep(self._complete_grace_seconds)
        except asyncio.CancelledError:
            return
        text = " ".join(self._fragments)
        fragment_count = len(self._fragments)
        self._fragments.clear()
        self._completion_task = None
        if not text:
            return
        logger.info(f"{self}: routing {fragment_count} STT fragment(s) as one completed user turn")
        self._dispatch_in_background(text)
        await self.push_frame(UserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)

    def _arm_completion(self) -> None:
        self._cancel_completion()
        logger.debug(
            f"{self}: Smart Turn completion armed for {self._complete_grace_seconds:g}s grace"
        )
        self._completion_task = asyncio.create_task(self._complete_after_grace())

    async def process_frame(self, frame: Any, direction: FrameDirection) -> None:
        """Collect STT fragments and debounce premature semantic stops."""
        await super().process_frame(frame, direction)

        if direction == FrameDirection.DOWNSTREAM:
            if isinstance(frame, TranscriptionFrame):
                text = frame.text.strip()
                if text:
                    self._fragments.append(text)
                # Raw local-STT segments are not application turns and must not
                # leak into the browser transcript as separate messages.
                return
            if isinstance(frame, (UserStartedSpeakingFrame, VADUserStartedSpeakingFrame)):
                if self._completion_task is not None:
                    logger.debug(f"{self}: speech resumed during completion grace")
                    self._cancel_completion()
            elif isinstance(frame, UserStoppedSpeakingFrame):
                self._arm_completion()
                return
            elif isinstance(frame, (CancelFrame, EndFrame)):
                self._cancel_completion()
                self._fragments.clear()

        await self.push_frame(frame, direction)
