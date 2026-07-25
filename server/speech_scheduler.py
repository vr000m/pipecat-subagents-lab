"""Task-local speech scheduling with one active utterance lease."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from .contracts import DeliveryState
from .session_state import SessionState


@dataclass(frozen=True)
class SpeechItem:
    result_id: str
    work_item_id: str
    run_id: str
    text: str
    utterance_id: str
    origin_epoch: int | None = None


@dataclass(frozen=True)
class UtteranceLease:
    item: SpeechItem
    token: str


class SpeechScheduler:
    """Queues speech per work item and fences state through SessionState."""

    def __init__(
        self,
        state: SessionState | None = None,
        speak: Callable[[SpeechItem], Any] | None = None,
        stop: Callable[[SpeechItem], Any] | None = None,
    ) -> None:
        self.state = state or SessionState()
        self.speak = speak
        self.stop = stop
        self._queues: dict[str, list[SpeechItem]] = {}
        self._active: UtteranceLease | None = None
        self._paused: dict[str, SpeechItem] = {}
        self._awaiting_provider_context: deque[str] = deque()
        self._provider_contexts: dict[str, str] = {}
        self._stop_tasks: set[asyncio.Future[Any]] = set()

    def _signal_stop(self, item: SpeechItem) -> None:
        if self.stop is None:
            return
        try:
            outcome = self.stop(item)
        except BaseException:
            return
        if isinstance(outcome, Awaitable):
            try:
                task = asyncio.ensure_future(outcome)
            except RuntimeError:
                return
            self._stop_tasks.add(task)
            task.add_done_callback(self._stop_tasks.discard)

    async def wait_for_stops(self) -> None:
        """Wait until every stop signal scheduled so far has reached the pipeline."""
        pending = tuple(self._stop_tasks)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    @property
    def active(self) -> UtteranceLease | None:
        return self._active

    def paused(self, work_item_id: str | None = None) -> SpeechItem | None:
        if work_item_id is not None:
            return self._paused.get(work_item_id)
        if not self._paused:
            return None
        return self._paused[next(reversed(self._paused))]

    @staticmethod
    def _progress(item: SpeechItem) -> dict[str, Any]:
        return {
            "result_id": item.result_id,
            "work_item_id": item.work_item_id,
            "run_id": item.run_id,
            "utterance_id": item.utterance_id,
            "origin_epoch": item.origin_epoch,
        }

    def enqueue(
        self,
        *,
        result_id: str,
        work_item_id: str,
        run_id: str,
        text: str,
        origin_epoch: int | None = None,
        utterance_id: str | None = None,
    ) -> SpeechItem:
        item = SpeechItem(
            result_id,
            work_item_id,
            run_id,
            text,
            utterance_id or f"utt-{uuid4().hex}",
            origin_epoch,
        )
        self._queues.setdefault(work_item_id, []).append(item)
        self.state.speech_progress(**self._progress(item), state=DeliveryState.DISPLAYED)
        self.state.speech_progress(**self._progress(item), state=DeliveryState.QUEUED)
        return item

    async def start_next(self, work_item_id: str | None = None) -> SpeechItem | None:
        if self._active is not None:
            return None
        keys = [work_item_id] if work_item_id else list(self._queues)
        item = next((self._queues[key][0] for key in keys if self._queues.get(key)), None)
        if item is None:
            return None
        self._queues[item.work_item_id].pop(0)
        lease = UtteranceLease(item, uuid4().hex)
        self._active = lease
        if self.speak is not None:
            self._awaiting_provider_context.append(lease.token)
        self.state.speech_progress(**self._progress(item), state=DeliveryState.STARTED)
        try:
            if self.speak is not None:
                outcome = self.speak(item)
                if isinstance(outcome, Awaitable):
                    await outcome
        except BaseException:
            if self.speak is not None:
                try:
                    self._awaiting_provider_context.remove(lease.token)
                except ValueError:
                    pass
            self.state.speech_progress(**self._progress(item), state=DeliveryState.DELIVERY_UNKNOWN)
            self._release(item.utterance_id)
            raise
        return item

    def provider_started(self, context_id: str) -> None:
        """Bind a Pipecat-generated TTS context to the lease that queued it."""
        if context_id in self._provider_contexts or not self._awaiting_provider_context:
            return
        self._provider_contexts[context_id] = self._awaiting_provider_context.popleft()

    def provider_synthesis_ended(self, context_id: str) -> bool:
        item = self._provider_item(context_id)
        if item is None:
            return False
        self.synthesis_ended(item.utterance_id)
        return True

    def provider_delivery_completed(self, context_id: str) -> bool:
        item = self._provider_item(context_id)
        if item is None:
            return False
        self.delivery_completed(item.utterance_id)
        self._provider_contexts.pop(context_id, None)
        return True

    def provider_delivery_unknown(self, context_id: str) -> bool:
        item = self._provider_item(context_id)
        if item is None:
            return False
        self.delivery_unknown(item.utterance_id)
        self._provider_contexts.pop(context_id, None)
        return True

    def synthesis_ended(self, utterance_id: str) -> None:
        item = self._active_item(utterance_id)
        if item is None:
            return
        self.state.speech_progress(**self._progress(item), state=DeliveryState.SYNTHESIS_ENDED)

    def delivery_completed(self, utterance_id: str) -> None:
        item = self._active_item(utterance_id)
        if item is None:
            return
        self.state.speech_progress(**self._progress(item), state=DeliveryState.DELIVERY_COMPLETED)
        self._release(utterance_id)

    def delivery_unknown(self, utterance_id: str) -> None:
        item = self._active_item(utterance_id)
        if item is None:
            return
        self.state.speech_progress(**self._progress(item), state=DeliveryState.DELIVERY_UNKNOWN)
        self._release(utterance_id)

    def interrupt(self, *, epoch: int | None = None, reconnect: bool = False) -> SpeechItem | None:
        state = DeliveryState.INTERRUPTED_BY_RECONNECT if reconnect else DeliveryState.INTERRUPTED
        active_item = self._active.item if self._active is not None else None
        if active_item is None and not reconnect:
            return None
        if active_item is not None:
            self._signal_stop(active_item)
            self.state.speech_progress(
                result_id=active_item.result_id,
                work_item_id=active_item.work_item_id,
                run_id=active_item.run_id,
                utterance_id=active_item.utterance_id,
                state=state,
                origin_epoch=epoch if epoch is not None else active_item.origin_epoch,
                allow_stale_reconnect=reconnect,
            )
            self._release(active_item.utterance_id)
        if reconnect:
            for queue in self._queues.values():
                for item in queue:
                    self.state.speech_progress(
                        result_id=item.result_id,
                        work_item_id=item.work_item_id,
                        run_id=item.run_id,
                        utterance_id=item.utterance_id,
                        state=state,
                        origin_epoch=epoch if epoch is not None else item.origin_epoch,
                        allow_stale_reconnect=True,
                    )
            self._queues.clear()
        return active_item

    def pause(self, work_item_id: str) -> None:
        if self._active and self._active.item.work_item_id == work_item_id:
            item = self._active.item
            self._paused[work_item_id] = item
            self._signal_stop(item)
            self.state.speech_progress(**self._progress(item), state=DeliveryState.PAUSED)
            # Pausing releases the lease without recording a terminal
            # interruption; resume must be able to represent the next state.
            self._release(item.utterance_id)

    def resume(self, target: str | SpeechItem | None = None) -> SpeechItem | None:
        work_item_id = target.work_item_id if isinstance(target, SpeechItem) else target
        item = self.paused(work_item_id)
        if item is None and isinstance(target, SpeechItem):
            item = target
        if item is None:
            return None
        self._paused.pop(item.work_item_id, None)
        replay = self.enqueue(
            result_id=item.result_id,
            work_item_id=item.work_item_id,
            run_id=item.run_id,
            text=item.text,
            origin_epoch=item.origin_epoch,
        )
        self.state.speech_progress(**self._progress(replay), state=DeliveryState.RESUMED)
        return replay

    def cancel(self, work_item_id: str | None = None) -> tuple[SpeechItem, ...]:
        """Cancel active, queued, and paused speech for one work item or all work."""
        cancelled: list[SpeechItem] = []
        if self._active is not None and (
            work_item_id is None or self._active.item.work_item_id == work_item_id
        ):
            item = self._active.item
            self._signal_stop(item)
            self.state.speech_progress(**self._progress(item), state=DeliveryState.INTERRUPTED)
            self._release(item.utterance_id)
            cancelled.append(item)
        keys = [work_item_id] if work_item_id is not None else list(self._queues)
        for key in keys:
            for item in self._queues.pop(key, []):
                self.state.speech_progress(**self._progress(item), state=DeliveryState.INTERRUPTED)
                cancelled.append(item)
        paused_keys = [work_item_id] if work_item_id is not None else list(self._paused)
        for key in paused_keys:
            item = self._paused.pop(key, None)
            if item is not None:
                self.state.speech_progress(**self._progress(item), state=DeliveryState.INTERRUPTED)
                cancelled.append(item)
        return tuple(cancelled)

    def _active_item(self, utterance_id: str) -> SpeechItem | None:
        if self._active is None or self._active.item.utterance_id != utterance_id:
            return None
        return self._active.item

    def _provider_item(self, context_id: str) -> SpeechItem | None:
        if self._active is None:
            return None
        if self._provider_contexts.get(context_id) != self._active.token:
            return None
        return self._active.item

    def _release(self, utterance_id: str) -> None:
        if self._active and self._active.item.utterance_id == utterance_id:
            token = self._active.token
            try:
                self._awaiting_provider_context.remove(token)
            except ValueError:
                pass
            self._provider_contexts = {
                context_id: lease_token
                for context_id, lease_token in self._provider_contexts.items()
                if lease_token != token
            }
            self._active = None
