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
    DeliveryDisposition,
    GenerationIdentity,
    PreAdmissionAdmit,
    PreAdmissionBusy,
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
        self._advance_tasks: set[asyncio.Future[Any]] = set()
        # Set once by interrupt(full_stop=True): this scheduler is being
        # discarded for good and never admits again. _schedule_queue_advance
        # consults it so a task that is already past this check when
        # full_stop's own cancel() sweep runs -- and whose in-flight
        # start_next() then fails and re-schedules itself from inside a
        # caught CancelledError -- cannot re-populate _advance_tasks after
        # the sweep already ran (task.cancel() only requests cancellation;
        # start_next's own broad except clause can observe and swallow it).
        self._discarded = False

    def _notify_ack_swept(self, item: SpeechItem) -> None:
        """Route an ack discarded by ``interrupt()``'s queued/paused sweep
        through the same ack-terminal notification ``start_next`` uses for
        its ``PreAdmissionTerminal`` branch.

        ``_emit_progress`` is a deliberate no-op for acks (they are
        wire-invisible), so without this an ack swept here would vanish
        silently instead of reaching ``_on_ack_terminal`` -- the codebase's
        stated sole mutator for the pre-admission-terminal ack path
        (``server/pipeline.py``'s ``ConnectionPipelineHost.on_ack_terminal``,
        which clears the owning turn's ack latch). ``CONNECTION_CLOSED`` is
        reused here rather than adding a new enum member: this sweep only
        runs when the scheduler itself is being discarded for good
        (``reconnect``/``full_stop``), the same "this connection is gone"
        condition that reason already models, and the callback does not
        branch on the reason value today regardless.
        """
        if item.role != ROLE_ACK or self._on_ack_terminal is None:
            return
        identity = GenerationIdentity(
            item.utterance_id,
            item.work_item_id,
            item.origin_epoch,
            role=ROLE_ACK,
            turn_id=item.turn_id,
            ack_id=item.ack_id,
        )
        self._on_ack_terminal(identity, PreAdmissionTerminalReason.CONNECTION_CLOSED)

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

    def queued_roles(self) -> tuple[SpeechRole, ...]:
        """Roles of every currently queued (not yet active) speech item.

        Public counterpart to pending_work_item_ids() for callers that need
        per-item role rather than just work-item-key presence; keeps queue
        internals encapsulated per this class's private-_queues contract.
        """
        return tuple(item.role for queue in self._queues.values() for item in queue)

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
        if role == ROLE_ACK:
            if ack_id is None:
                # The synthetic ack queue key (``ack-{turn_id}``) and the
                # ack's identity are the same string throughout production
                # call sites; default it here so every ack enqueue satisfies
                # SpeechItem's ack_id requirement without every caller
                # having to repeat work_item_id as ack_id explicitly.
                ack_id = work_item_id
            elif ack_id != work_item_id:
                # discard_queued_ack indexes self._queues[ack_id] directly,
                # correct only while the two are equal. A caller passing a
                # divergent ack_id would make discards on this item silently
                # miss (read as "nothing queued") instead of failing loudly,
                # so enforce the invariant here rather than let it drift.
                raise ValueError(
                    f"ack_id ({ack_id!r}) must equal work_item_id ({work_item_id!r}) or be omitted"
                )
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
        self._emit_progress(item, DeliveryState.DISPLAYED)
        self._emit_progress(item, DeliveryState.QUEUED)
        return item

    def _set_queue(self, work_item_id: str, items: list[SpeechItem]) -> None:
        """Write back a work item's queue, dropping the key entirely if empty.

        A retained empty list is indistinguishable from pending work to any
        caller that inspects the queue keys (see ``pending_work_item_ids``),
        so every queue mutator funnels its write-back through here.
        """
        if items:
            self._queues[work_item_id] = items
        else:
            self._queues.pop(work_item_id, None)

    def _discard_queued(
        self, work_item_id: str, *, matches: Callable[[SpeechItem], bool]
    ) -> tuple[SpeechItem, ...]:
        """Drop every still-queued item matching ``matches`` from one queue.

        Other queued items in the same queue, and every queued item under a
        different work-item key, are left untouched and keep their relative
        order.
        """
        queue = self._queues.get(work_item_id, [])
        discarded = tuple(item for item in queue if matches(item))
        if not discarded:
            return ()
        self._set_queue(work_item_id, [item for item in queue if not matches(item)])
        return discarded

    def discard_queued_ack(self, ack_id: str) -> SpeechItem | None:
        """Atomically drop one still-queued ack.

        ``ack_id`` is also the synthetic ack queue key: every production ack
        enqueue passes ``work_item_id=ack_id`` (``enqueue`` defaults
        ``ack_id`` to ``work_item_id`` when omitted), so the queue key is
        resolved directly rather than through a separate identity index.
        Only the still-queued ack for ``ack_id`` is removed; other queued
        items for the same work-item key (there should be none, since the
        synthetic ack queue key is disjoint from real work-item keys) and
        every other queued/paused/active item are left untouched. An
        already-admitted ack cannot be discarded this way.
        """
        discarded = self._discard_queued(
            ack_id, matches=lambda item: item.role == ROLE_ACK and item.ack_id == ack_id
        )
        return discarded[0] if discarded else None

    async def start_next(self, work_item_id: str | None = None) -> SpeechItem | None:
        if self._active is not None or self.lifecycle.occupied:
            return None
        keys = [work_item_id] if work_item_id else list(self._queues)
        item: SpeechItem | None = None
        identity: GenerationIdentity | None = None
        disposition: PreAdmissionAdmit | None = None
        for key in keys:
            while True:
                candidate = self._queues[key][0] if self._queues.get(key) else None
                if candidate is None:
                    break
                candidate_identity = GenerationIdentity(
                    candidate.utterance_id,
                    candidate.work_item_id,
                    candidate.origin_epoch,
                    role=candidate.role if candidate.role == ROLE_ACK else "result",
                    turn_id=candidate.turn_id,
                    ack_id=candidate.ack_id,
                )
                candidate_disposition = self.lifecycle.pre_admission_disposition(candidate_identity)
                if isinstance(candidate_disposition, PreAdmissionTerminal):
                    # The transport slot is still provably free (checked
                    # above, and nothing here awaits) -- a terminal
                    # disposition rejects only this candidate, not the slot
                    # itself, so this same key's newly-exposed next item
                    # deserves its own admission attempt before moving on to
                    # another key, instead of stalling until an unrelated
                    # caller happens to call start_next again.
                    self._discard_from_queue(candidate)
                    if candidate.role == ROLE_ACK:
                        if self._on_ack_terminal is not None:
                            self._on_ack_terminal(candidate_identity, candidate_disposition.reason)
                    else:
                        self._emit_progress(candidate, DeliveryState.DELIVERY_UNKNOWN)
                    continue
                if isinstance(candidate_disposition, PreAdmissionBusy):
                    # The slot is occupied, not terminal -- unreachable today
                    # (the top-of-method guard already confirmed the slot is
                    # free before this loop starts, and nothing here awaits),
                    # but a genuine "retry later" outcome, not a candidate
                    # rejection: nothing else in this call will admit either,
                    # so stop scanning. This is only true because a closed
                    # connection is its own PreAdmissionTerminal reason
                    # (CONNECTION_CLOSED) checked before try_admit ever runs
                    # -- try_admit's own _connection_closed guard would
                    # otherwise also surface here as PreAdmissionBusy, a
                    # permanent condition this branch's contract assumes
                    # is always worth retrying.
                    return None
                item, identity, disposition = candidate, candidate_identity, candidate_disposition
                break
            if item is not None:
                break
        if item is None or identity is None or disposition is None:
            return None
        generation = disposition.generation
        queue = self._queues[item.work_item_id]
        queue.pop(0)
        self._set_queue(item.work_item_id, queue)
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
            # admission attempt, and on_terminal re-probes every queue key --
            # including this item's own key, whose owner is about to re-queue
            # a replacement item it expects to still be queued. The freed slot
            # must still be re-probed for the *other* pending keys, which
            # nothing else would do until an unrelated caller happened to call
            # start_next again; _schedule_queue_advance does exactly that,
            # deferred, so the original exception propagates first.
            self.lifecycle.release_generation(token, notify=False)
            self._release(item.utterance_id)
            self._schedule_queue_advance(exclude=item.work_item_id)
            raise
        return item

    def _schedule_queue_advance(self, *, exclude: str) -> None:
        """Re-probe the queue once, on a later tick, after a submission
        failure freed the slot.

        The coordinator's ``on_terminal`` callback is the only other reactive
        re-probe when the slot frees, and the submission-failure path
        deliberately suppresses it (see ``release_generation``). Without this,
        every item queued under another work-item key stalls until some
        unrelated caller happens to call ``start_next`` again.

        ``exclude`` is the failed item's own key: its owner catches the
        propagating exception and decides whether to re-queue, so this must
        not race that decision. Admission is deferred to a task so the
        original exception reaches that owner first, and any failure of the
        advance itself is swallowed -- it is a best-effort re-probe, and the
        newly admitted item's own progress bookkeeping already recorded it.

        A no-op once ``interrupt(reconnect=True)``/``interrupt(full_stop=True)``
        has run: this scheduler is being discarded for good at that point, so
        there is nothing left to advance onto, and a caller reached via a
        submission failure that was itself caused by a race against that
        same interrupt (``start_next``'s except clause can observe and
        swallow the ``CancelledError`` interrupt's own cleanup sweep sent it,
        then land here) must not re-populate ``_advance_tasks`` after that
        sweep already ran.
        """
        if self._discarded:
            return

        async def advance() -> None:
            try:
                key = next(
                    (
                        candidate
                        for candidate, queue in self._queues.items()
                        if queue and candidate != exclude
                    ),
                    None,
                )
                if key is None:
                    return
                await self.start_next(key)
            except BaseException:  # noqa: BLE001 - best-effort re-probe; never surfaces to a caller
                logger.opt(exception=True).debug(
                    "queue advance after a speech submission failure did not start an item"
                )

        try:
            task = asyncio.ensure_future(advance())
        except RuntimeError:  # no running loop: nothing to advance onto
            return
        self._advance_tasks.add(task)
        task.add_done_callback(self._advance_tasks.discard)

    def _discard_from_queue(self, item: SpeechItem) -> None:
        queue = self._queues.get(item.work_item_id)
        if queue and queue[0] is item:
            queue.pop(0)
            self._set_queue(item.work_item_id, queue)

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
            self.lifecycle.release_generation(
                token, disposition=DeliveryDisposition.DELIVERY_COMPLETED
            )
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

    def interrupt(
        self, *, epoch: int | None = None, reconnect: bool = False, full_stop: bool = False
    ) -> SpeechItem | None:
        """Interrupt the active lease -- task-locally by default, leaving
        other work items' queues untouched (``test_scheduler_stop_is_task_
        local_when_other_work_is_queued``); a genuine reconnect (or an
        explicit ``full_stop``) additionally sweeps every queued and paused
        item across every work-item key, because in both cases this
        scheduler is being discarded for good, and a queued/paused item
        left behind is neither in ``session_state._TERMINAL`` nor reachable
        by any future caller -- it leaks into every later connection's
        ``RuntimeSnapshot.speech_progress`` forever.

        ``full_stop`` is for a teardown (e.g. ``ConnectionPipeline.shutdown``'s
        "speech output teardown"/"session shutdown" paths, or
        ``deactivate()``) where this scheduler is being discarded for good
        but the disposition recorded should stay plain ``INTERRUPTED`` (not
        ``INTERRUPTED_BY_RECONNECT``) unless ``reconnect`` is also set.

        ``reconnect`` changes the recorded disposition (``INTERRUPTED`` vs.
        ``INTERRUPTED_BY_RECONNECT``); a genuine reconnect has already
        advanced ``active_epoch`` past these items' origin epoch by the time
        this runs.

        Both ``reconnect`` and ``full_stop`` bypass ``SessionState``'s
        active-epoch fence (``allow_stale_reconnect=reconnect or
        full_stop``): a same-epoch teardown is *usually* not expected to
        have advanced ``active_epoch``, but a teardown dispatched as a
        separate task after an ``await`` (``dispatch_lifecycle_teardown`` ->
        ``schedule_pipeline_shutdown`` -> ``shutdown(reconnect=False)`` ->
        ``deactivate`` -> here) is not guaranteed same-epoch by
        construction -- a promotion can land in that window and advance
        ``active_epoch`` first. Since this scheduler is gone either way once
        ``full_stop`` is set, the bypass must track "is this scheduler being
        discarded" rather than "was this a reconnect", or a same-epoch-only
        assumption silently drops every queued/paused terminal record
        ``full_stop`` exists to emit.
        """
        state = DeliveryState.INTERRUPTED_BY_RECONNECT if reconnect else DeliveryState.INTERRUPTED
        active_item = self._active.item if self._active is not None else None
        active_token = self._active.token if self._active is not None else None
        if active_item is None and not reconnect and not full_stop:
            return None
        if active_item is not None:
            if active_token is not None:
                self.lifecycle.record_interruption(active_token, pause=False)
            self._signal_stop(active_item)
            self._emit_progress(
                active_item,
                state,
                allow_stale_reconnect=reconnect or full_stop,
                **({"origin_epoch": epoch} if epoch is not None else {}),
            )
            self._release(active_item.utterance_id)
        if reconnect or full_stop:
            # Set before the cancel() sweep below: a task already past
            # _schedule_queue_advance's own discarded-check (i.e. already
            # running) can still have task.cancel() swallowed by start_next's
            # broad except clause, which then calls _schedule_queue_advance
            # again from inside its own exception handler. That re-entrant
            # call must see this scheduler as already discarded, or it
            # re-populates _advance_tasks right after this loop empties it.
            self._discarded = True
            for queue in self._queues.values():
                for item in queue:
                    self._emit_progress(
                        item,
                        state,
                        allow_stale_reconnect=reconnect or full_stop,
                        **({"origin_epoch": epoch} if epoch is not None else {}),
                    )
                    self._notify_ack_swept(item)
            self._queues.clear()
            # A fire-and-forget queue-advance task (_schedule_queue_advance)
            # re-probes a queue key on a later tick after a submission
            # failure. With the queues just cleared above, any such task
            # still pending has nothing left to advance onto -- cancel it
            # rather than let it run against a scheduler being discarded.
            for task in tuple(self._advance_tasks):
                task.cancel()
            # A paused item holds a non-terminal PAUSED record in
            # SessionState.speech, and only resume() can clear it. Once this
            # scheduler is discarded no resume can ever arrive, and the
            # snapshot would ship that PAUSED entry forever. Terminalize it
            # like any queued item.
            for item in self._paused.values():
                self._emit_progress(
                    item,
                    state,
                    allow_stale_reconnect=reconnect or full_stop,
                    **({"origin_epoch": epoch} if epoch is not None else {}),
                )
                self._notify_ack_swept(item)
            self._paused.clear()
        return active_item

    def pause(self, work_item_id: str) -> None:
        """Pause the active lease if it matches ``work_item_id``, else no-op.

        Calls ``self.lifecycle.record_interruption(token, pause=True)`` for
        the active lease when it matches -- callers must not also call it
        themselves, or an uncontexted generation's cleanup coroutine gets
        double-scheduled.
        """
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
        was_paused = item is not None
        if item is None and isinstance(target, SpeechItem):
            item = target
        if item is None:
            return None
        self._paused.pop(item.work_item_id, None)
        if was_paused:
            # A paused item holds a non-terminal PAUSED record in
            # SessionState.speech keyed by its own utterance_id. The replay
            # below mints a NEW utterance_id (matching the interrupted-replay
            # path below), so nothing else ever terminalizes the OLD id --
            # terminalize it here the same way interrupt() already does for
            # every other paused item (see the reconnect branch above).
            self._emit_progress(item, DeliveryState.INTERRUPTED)
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
        """Cancel active, queued, and paused speech for one work item or all work.

        Calls ``self.lifecycle.record_interruption(token, pause=False)`` for
        the active lease when it matches (or when ``work_item_id`` is
        ``None``) -- callers must not also call it themselves, or an
        uncontexted generation's cleanup coroutine gets double-scheduled.
        """
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
        discarded = self._discard_queued(
            work_item_id, matches=lambda item: item.role == ROLE_TIMEOUT_NOTICE
        )
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
