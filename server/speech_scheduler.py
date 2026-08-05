"""Task-local speech scheduling with one active utterance lease."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal
from uuid import uuid4

from loguru import logger

from .contracts import DeliveryState
from .session_state import SessionState
from .speech_lifecycle import (
    GenerationIdentity,
    PreAdmissionTerminal,
    PreAdmissionTerminalReason,
    SpeechLifecycleCoordinator,
)

SpeechRole = Literal["result", "timeout_notice", "ack"]
ROLE_RESULT: SpeechRole = "result"
ROLE_TIMEOUT_NOTICE: SpeechRole = "timeout_notice"
ROLE_ACK: SpeechRole = "ack"

AckTerminalCallback = Callable[[GenerationIdentity, PreAdmissionTerminalReason], Any]


@dataclass(frozen=True)
class SpeechItem:
    result_id: str | None
    work_item_id: str
    run_id: str
    text: str
    utterance_id: str
    origin_epoch: int | None = None
    role: SpeechRole = ROLE_RESULT
    ack_id: str | None = None
    turn_id: str | None = None

    def __post_init__(self) -> None:
        if self.role == ROLE_ACK:
            if self.ack_id is None:
                raise ValueError("an ack SpeechItem requires ack_id")
            if self.result_id is not None:
                raise ValueError("an ack SpeechItem must not carry result_id")
        elif self.result_id is None:
            raise ValueError(f"a {self.role!r} SpeechItem requires result_id")


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
        *,
        lifecycle: SpeechLifecycleCoordinator,
        on_ack_terminal: AckTerminalCallback | None = None,
    ) -> None:
        self.state = state or SessionState()
        self.speak = speak
        self.stop = stop
        self.lifecycle = lifecycle
        self._on_ack_terminal = on_ack_terminal
        self._queues: dict[str, list[SpeechItem]] = {}
        self._active: UtteranceLease | None = None
        self._paused: dict[str, SpeechItem] = {}
        self._provider_contexts: dict[str, str] = {}
        self._stop_tasks: set[asyncio.Future[Any]] = set()
        self._ack_index: dict[str, str] = {}

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

    def pending_work_item_ids(self, *, exclude: str | None = None) -> frozenset[str]:
        """Work-item keys that still have at least one queued speech item.

        The public read for "is any other work still pending?": callers must
        not inspect ``_queues`` directly, and the truthiness filter here means
        a queue key left behind empty by a future regression cannot be
        mistaken for pending work.
        """
        return frozenset(key for key, queue in self._queues.items() if queue and key != exclude)

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

    def _emit_progress(self, item: SpeechItem, state: DeliveryState, **extra: Any) -> None:
        """The sole choke point for public speech-progress emission.

        Ack items are wire-invisible and server-log/observability-only,
        keyed by ``ack_id``: this is the one place that gate is enforced, so
        no other call site needs its own role check before calling
        ``SessionState.speech_progress`` / (indirectly)
        ``RTVIMessagePublisher.speech_progress``.
        """
        if item.role == ROLE_ACK:
            return
        fields = {**self._progress(item), **extra}
        self.state.speech_progress(state=state, **fields)

    def enqueue(
        self,
        *,
        result_id: str | None = None,
        work_item_id: str,
        run_id: str,
        text: str,
        origin_epoch: int | None = None,
        utterance_id: str | None = None,
        role: SpeechRole = ROLE_RESULT,
        ack_id: str | None = None,
        turn_id: str | None = None,
    ) -> SpeechItem:
        if role == ROLE_ACK and ack_id is None:
            # The synthetic ack queue key (``ack-{turn_id}``) and the ack's
            # identity are the same string throughout production call
            # sites; default it here so every ack enqueue satisfies
            # SpeechItem's ack_id requirement without every caller having
            # to repeat work_item_id as ack_id explicitly.
            ack_id = work_item_id
        item = SpeechItem(
            result_id=result_id,
            work_item_id=work_item_id,
            run_id=run_id,
            text=text,
            utterance_id=utterance_id or f"utt-{uuid4().hex}",
            origin_epoch=origin_epoch,
            role=role,
            ack_id=ack_id,
            turn_id=turn_id,
        )
        self._queues.setdefault(work_item_id, []).append(item)
        if role == ROLE_ACK and ack_id is not None:
            self._ack_index[ack_id] = work_item_id
        self._emit_progress(item, DeliveryState.DISPLAYED)
        self._emit_progress(item, DeliveryState.QUEUED)
        return item

    def discard_queued_ack(self, ack_id: str) -> SpeechItem | None:
        """Atomically drop one still-queued ack, clearing its index entry.

        Only the still-queued ack for ``ack_id`` is removed; other queued
        items for the same work-item key (there should be none, since the
        synthetic ack queue key is disjoint from real work-item keys) and
        every other queued/paused/active item are left untouched. An
        already-admitted ack cannot be discarded this way.
        """
        work_item_id = self._ack_index.get(ack_id)
        if work_item_id is None:
            return None
        queue = self._queues.get(work_item_id, [])
        discarded = next(
            (item for item in queue if item.role == ROLE_ACK and item.ack_id == ack_id), None
        )
        if discarded is None:
            self._ack_index.pop(ack_id, None)
            return None
        remaining = [item for item in queue if item is not discarded]
        if remaining:
            self._queues[work_item_id] = remaining
        else:
            self._queues.pop(work_item_id, None)
        self._ack_index.pop(ack_id, None)
        return discarded

    async def start_next(self, work_item_id: str | None = None) -> SpeechItem | None:
        if self._active is not None or self.lifecycle.occupied:
            return None
        keys = [work_item_id] if work_item_id else list(self._queues)
        item = next((self._queues[key][0] for key in keys if self._queues.get(key)), None)
        if item is None:
            return None
        identity = GenerationIdentity(
            item.utterance_id,
            item.work_item_id,
            item.origin_epoch,
            role=item.role if item.role == ROLE_ACK else "result",
            turn_id=item.turn_id,
            ack_id=item.ack_id,
        )
        disposition = self.lifecycle.pre_admission_disposition(identity)
        if disposition is None:
            return None
        if isinstance(disposition, PreAdmissionTerminal):
            self._discard_from_queue(item)
            if item.role == ROLE_ACK:
                if item.ack_id is not None:
                    self._ack_index.pop(item.ack_id, None)
                if self._on_ack_terminal is not None:
                    self._on_ack_terminal(identity, disposition.reason)
            else:
                self._emit_progress(item, DeliveryState.DELIVERY_UNKNOWN)
            return None
        generation = disposition.generation
        self._queues[item.work_item_id].pop(0)
        if not self._queues[item.work_item_id]:
            # Drop the now-empty queue key, exactly as every other queue
            # mutator does. A retained empty list is indistinguishable from
            # pending work to any caller that inspects the queue keys.
            self._queues.pop(item.work_item_id, None)
        token = generation.token
        lease = UtteranceLease(item, token)
        self._active = lease
        self._emit_progress(item, DeliveryState.STARTED)
        self.lifecycle.mark_handed_to_tts(token)
        try:
            if self.speak is not None:
                outcome = self.speak(item)
                if isinstance(outcome, Awaitable):
                    await outcome
        except BaseException:
            self._emit_progress(item, DeliveryState.DELIVERY_UNKNOWN)
            try:
                await self.lifecycle.provider_error(generation.token)
            except BaseException:  # noqa: BLE001 - preserve the original submission failure below
                logger.opt(exception=True).debug(
                    "lifecycle.provider_error failed while handling submission failure"
                )
            # The submission itself raised, synchronously, before anything
            # could have reached a real TTS lane: no marker/TTSSpeakFrame
            # traversal is in flight for provider_error's flush-ack barrier
            # to ever wait on, so the slot frees immediately rather than
            # waiting on an acknowledgement that structurally cannot arrive.
            # notify=False: this call is still inside start_next's own
            # admission attempt, so firing on_terminal here would re-enter
            # admission for the same still-failing item before the caller
            # that's about to receive this exception has a chance to react.
            self.lifecycle.release_generation(token, notify=False)
            self._release(item.utterance_id)
            raise
        return item

    def _discard_from_queue(self, item: SpeechItem) -> None:
        queue = self._queues.get(item.work_item_id)
        if queue and queue[0] is item:
            queue.pop(0)
            if not queue:
                self._queues.pop(item.work_item_id, None)

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
        self._emit_progress(item, DeliveryState.SYNTHESIS_ENDED)

    def delivery_completed(self, utterance_id: str) -> None:
        item = self._active_item(utterance_id)
        if item is None:
            return
        token = self._active.token if self._active is not None else None
        self._emit_progress(item, DeliveryState.DELIVERY_COMPLETED)
        if token is not None:
            self.lifecycle.release_generation(token)
        self._release(utterance_id)

    def delivery_unknown(self, utterance_id: str) -> None:
        item = self._active_item(utterance_id)
        if item is None:
            return
        token = self._active.token if self._active is not None else None
        self._emit_progress(item, DeliveryState.DELIVERY_UNKNOWN)
        if token is not None:
            self.lifecycle.release_generation(token)
        self._release(utterance_id)

    def interrupt(self, *, epoch: int | None = None, reconnect: bool = False) -> SpeechItem | None:
        state = DeliveryState.INTERRUPTED_BY_RECONNECT if reconnect else DeliveryState.INTERRUPTED
        active_item = self._active.item if self._active is not None else None
        active_token = self._active.token if self._active is not None else None
        if active_item is None and not reconnect:
            return None
        if active_item is not None:
            if active_token is not None:
                self.lifecycle.record_interruption(active_token, pause=False)
            self._signal_stop(active_item)
            self._emit_progress(
                active_item,
                state,
                allow_stale_reconnect=reconnect,
                **({"origin_epoch": epoch} if epoch is not None else {}),
            )
            self._release(active_item.utterance_id)
        if reconnect:
            for queue in self._queues.values():
                for item in queue:
                    self._emit_progress(
                        item,
                        state,
                        allow_stale_reconnect=True,
                        **({"origin_epoch": epoch} if epoch is not None else {}),
                    )
            self._queues.clear()
            self._ack_index.clear()
        return active_item

    def pause(self, work_item_id: str) -> None:
        if self._active and self._active.item.work_item_id == work_item_id:
            item = self._active.item
            token = self._active.token
            self.lifecycle.record_interruption(token, pause=True)
            self._paused[work_item_id] = item
            self._signal_stop(item)
            self._emit_progress(item, DeliveryState.PAUSED)
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
            ack_id=item.ack_id,
            turn_id=item.turn_id,
        )
        self._emit_progress(replay, DeliveryState.RESUMED)
        return replay

    def cancel(self, work_item_id: str | None = None) -> tuple[SpeechItem, ...]:
        """Cancel active, queued, and paused speech for one work item or all work."""
        cancelled: list[SpeechItem] = []
        if self._active is not None and (
            work_item_id is None or self._active.item.work_item_id == work_item_id
        ):
            item = self._active.item
            token = self._active.token
            self.lifecycle.record_interruption(token, pause=False)
            self._signal_stop(item)
            self._emit_progress(item, DeliveryState.INTERRUPTED)
            self._release(item.utterance_id)
            cancelled.append(item)
        keys = [work_item_id] if work_item_id is not None else list(self._queues)
        for key in keys:
            for item in self._queues.pop(key, []):
                self._emit_progress(item, DeliveryState.INTERRUPTED)
                if item.role == ROLE_ACK and item.ack_id is not None:
                    self._ack_index.pop(item.ack_id, None)
                cancelled.append(item)
        paused_keys = [work_item_id] if work_item_id is not None else list(self._paused)
        for key in paused_keys:
            item = self._paused.pop(key, None)
            if item is not None:
                self._emit_progress(item, DeliveryState.INTERRUPTED)
                cancelled.append(item)
        return tuple(cancelled)

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
            self._emit_progress(item, DeliveryState.INTERRUPTED)
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
