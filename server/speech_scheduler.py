"""Task-local speech scheduling with one active utterance lease."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal
from uuid import uuid4

from .contracts import DeliveryState
from .session_state import SessionState
from .speech_lifecycle import GenerationIdentity, SpeechLifecycleCoordinator

SpeechRole = Literal["result", "timeout_notice"]
ROLE_RESULT: SpeechRole = "result"
ROLE_TIMEOUT_NOTICE: SpeechRole = "timeout_notice"


@dataclass(frozen=True)
class SpeechItem:
    result_id: str
    work_item_id: str
    run_id: str
    text: str
    utterance_id: str
    origin_epoch: int | None = None
    role: SpeechRole = ROLE_RESULT


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
        lifecycle: SpeechLifecycleCoordinator | None = None,
    ) -> None:
        self.state = state or SessionState()
        self.speak = speak
        self.stop = stop
        self.lifecycle = lifecycle
        self._queues: dict[str, list[SpeechItem]] = {}
        self._active: UtteranceLease | None = None
        self._paused: dict[str, SpeechItem] = {}
        self._provider_contexts: dict[str, str] = {}
        self._stop_tasks: set[asyncio.Future[Any]] = set()

    def _signal_stop(self, item: SpeechItem) -> None:
        if self.stop is None:
            return
        try:
            outcome = self.stop(item)
        except BaseException:  # noqa: BLE001  # intentional catch-all: a failing stop callback must not block scheduler cleanup
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
        role: SpeechRole = ROLE_RESULT,
    ) -> SpeechItem:
        item = SpeechItem(
            result_id,
            work_item_id,
            run_id,
            text,
            utterance_id or f"utt-{uuid4().hex}",
            origin_epoch,
            role,
        )
        self._queues.setdefault(work_item_id, []).append(item)
        self.state.speech_progress(**self._progress(item), state=DeliveryState.DISPLAYED)
        self.state.speech_progress(**self._progress(item), state=DeliveryState.QUEUED)
        return item

    async def start_next(self, work_item_id: str | None = None) -> SpeechItem | None:
        if self._active is not None or (self.lifecycle is not None and self.lifecycle.occupied):
            return None
        keys = [work_item_id] if work_item_id else list(self._queues)
        item = next((self._queues[key][0] for key in keys if self._queues.get(key)), None)
        if item is None:
            return None
        generation = None
        if self.lifecycle is not None:
            generation = self.lifecycle.try_admit(
                GenerationIdentity(item.utterance_id, item.work_item_id, item.origin_epoch)
            )
            if generation is None:
                return None
        self._queues[item.work_item_id].pop(0)
        token = generation.token if generation is not None else uuid4().hex
        lease = UtteranceLease(item, token)
        self._active = lease
        self.state.speech_progress(**self._progress(item), state=DeliveryState.STARTED)
        if self.lifecycle is not None:
            self.lifecycle.mark_handed_to_tts(token)
        try:
            if self.speak is not None:
                outcome = self.speak(item)
                if isinstance(outcome, Awaitable):
                    await outcome
        except BaseException:
            self.state.speech_progress(**self._progress(item), state=DeliveryState.DELIVERY_UNKNOWN)
            self._release(item.utterance_id)
            raise
        return item

    def provider_started(self, context_id: str) -> None:
        """Bind a correlated Pipecat TTS context to its active scheduler lease."""
        if (
            context_id in self._provider_contexts
            or self._active is None
            or context_id != self._active.item.utterance_id
        ):
            return
        self._provider_contexts[context_id] = self._active.token

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
        active_token = self._active.token if self._active is not None else None
        if active_item is None and not reconnect:
            return None
        if active_item is not None:
            if self.lifecycle is not None and active_token is not None:
                self.lifecycle.record_interruption(active_token, pause=False)
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
            if self.lifecycle is not None and active_token is not None:
                self.lifecycle.release_flushed_lane(active_token)
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
            token = self._active.token
            if self.lifecycle is not None:
                self.lifecycle.record_interruption(token, pause=True)
            self._paused[work_item_id] = item
            self._signal_stop(item)
            self.state.speech_progress(**self._progress(item), state=DeliveryState.PAUSED)
            if self.lifecycle is not None:
                self.lifecycle.release_flushed_lane(token)
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
            role=item.role,
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
            token = self._active.token
            if self.lifecycle is not None:
                self.lifecycle.record_interruption(token, pause=False)
            self._signal_stop(item)
            self.state.speech_progress(**self._progress(item), state=DeliveryState.INTERRUPTED)
            if self.lifecycle is not None:
                self.lifecycle.release_flushed_lane(token)
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

    def discard_queued(self, work_item_id: str) -> tuple[SpeechItem, ...]:
        """Discard speech for one work item only if it has not started."""
        discarded = tuple(self._queues.pop(work_item_id, ()))
        for item in discarded:
            self.state.speech_progress(**self._progress(item), state=DeliveryState.INTERRUPTED)
        return discarded

    def discard_queued_notice(self, work_item_id: str) -> tuple[SpeechItem, ...]:
        """Discard only a still-queued timeout notice for one work item.

        A notice is supersedable while it remains in its own per-work queue.
        Other queued items in the same work-item queue (non-supersedable
        speech, or a notice already admitted to the transport slot) are left
        untouched and keep their relative order.
        """
        queue = self._queues.get(work_item_id)
        if not queue:
            return ()
        remaining = [item for item in queue if item.role != ROLE_TIMEOUT_NOTICE]
        discarded = tuple(item for item in queue if item.role == ROLE_TIMEOUT_NOTICE)
        if not discarded:
            return ()
        if remaining:
            self._queues[work_item_id] = remaining
        else:
            self._queues.pop(work_item_id, None)
        for item in discarded:
            self.state.speech_progress(**self._progress(item), state=DeliveryState.INTERRUPTED)
        return discarded

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
            self._provider_contexts = {
                context_id: lease_token
                for context_id, lease_token in self._provider_contexts.items()
                if lease_token != token
            }
            self._active = None
