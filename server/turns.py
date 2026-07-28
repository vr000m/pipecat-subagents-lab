"""Logical user-turn processing for segmented local speech recognition."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from typing import Any

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
        max_pending_dispatches: int = 8,
        max_transcript_chars: int = 16_000,
    ) -> None:
        super().__init__()
        if max_pending_dispatches < 1:
            raise ValueError("max_pending_dispatches must be positive")
        if max_transcript_chars < 1:
            raise ValueError("max_transcript_chars must be positive")
        self._on_final = on_final
        self._complete_grace_seconds = complete_grace_seconds
        self._max_transcript_chars = max_transcript_chars
        self._fragments: list[str] = []
        self._fragment_chars = 0
        self._dispatch_slots = asyncio.Semaphore(max_pending_dispatches)
        self._dispatch_tasks: set[asyncio.Task[None]] = set()
        self._completion_task: asyncio.Task[None] | None = None
        self._completion_tasks: set[asyncio.Task[None]] = set()
        self._completion_committed = False
        self._closing = False

    async def _dispatch(self, text: str) -> None:
        if self._on_final is None:
            return
        try:
            result = self._on_final(text)
            if inspect.isawaitable(result):
                await result
        except Exception:  # noqa: BLE001  # intentional catch-all: the caller-provided completion handler can raise arbitrary errors that must not crash turn dispatch
            logger.exception(f"{self}: failed to route completed user turn")

    async def _dispatch_with_slot(self, text: str) -> None:
        try:
            await self._dispatch(text)
        finally:
            self._dispatch_slots.release()

    async def _dispatch_in_background(self, text: str) -> None:
        await self._dispatch_slots.acquire()
        if self._closing:
            self._dispatch_slots.release()
            return
        task = asyncio.create_task(self._dispatch_with_slot(text))
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_tasks.discard)

    def _cancel_completion(self) -> None:
        if self._completion_task is not None and not self._completion_task.done():
            self._completion_task.cancel()
        self._completion_task = None
        self._completion_committed = False

    def _append_fragment(self, text: str) -> None:
        separator_chars = 1 if self._fragments else 0
        remaining = self._max_transcript_chars - self._fragment_chars
        if remaining <= separator_chars:
            return
        fragment = text[: remaining - separator_chars]
        if fragment:
            self._fragments.append(fragment)
            self._fragment_chars += separator_chars + len(fragment)

    async def _complete_after_grace(self) -> None:
        try:
            await asyncio.sleep(self._complete_grace_seconds)
        except asyncio.CancelledError:
            return
        text = " ".join(self._fragments)
        fragment_count = len(self._fragments)
        self._fragments.clear()
        self._fragment_chars = 0
        if not text:
            self._completion_task = None
            return
        self._completion_committed = True
        logger.info(f"{self}: routing {fragment_count} STT fragment(s) as one completed user turn")
        try:
            await self._dispatch_in_background(text)
            if not self._closing:
                await self.push_frame(UserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
        finally:
            if self._completion_task is asyncio.current_task():
                self._completion_task = None
                self._completion_committed = False

    async def _arm_completion(self) -> None:
        if self._completion_task is not None and not self._completion_task.done():
            if self._completion_committed:
                try:
                    await self._completion_task
                except asyncio.CancelledError:
                    if self._closing:
                        return
                    raise
            else:
                self._cancel_completion()
        if self._closing:
            return
        logger.debug(
            f"{self}: Smart Turn completion armed for {self._complete_grace_seconds:g}s grace"
        )
        self._completion_task = asyncio.create_task(self._complete_after_grace())
        self._completion_tasks.add(self._completion_task)
        self._completion_task.add_done_callback(self._completion_tasks.discard)

    async def cleanup(self) -> None:
        """Cancel and drain every application-turn task before processor teardown."""
        self._closing = True
        tasks = [*self._completion_tasks, *self._dispatch_tasks]
        for task in tasks:
            if not task.done():
                task.cancel()
        self._completion_task = None
        self._completion_committed = False
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._completion_tasks.clear()
        self._dispatch_tasks.clear()
        self._fragments.clear()
        self._fragment_chars = 0
        await super().cleanup()

    async def process_frame(self, frame: Any, direction: FrameDirection) -> None:
        """Collect STT fragments and debounce premature semantic stops."""
        await super().process_frame(frame, direction)

        if direction == FrameDirection.DOWNSTREAM:
            if isinstance(frame, TranscriptionFrame):
                text = frame.text.strip()
                if text:
                    self._append_fragment(text)
                # Raw local-STT segments are not application turns and must not
                # leak into the browser transcript as separate messages.
                return
            if isinstance(frame, (UserStartedSpeakingFrame, VADUserStartedSpeakingFrame)):
                if self._completion_task is not None and not self._completion_committed:
                    logger.debug(f"{self}: speech resumed during completion grace")
                    self._cancel_completion()
            elif isinstance(frame, UserStoppedSpeakingFrame):
                await self._arm_completion()
                return
            elif isinstance(frame, (CancelFrame, EndFrame)):
                self._cancel_completion()
                self._fragments.clear()
                self._fragment_chars = 0

        await self.push_frame(frame, direction)
