"""Turn-ack lifecycle bookkeeping owned by a single SessionHost.

Extracted from SessionHost (deep-review Architecture finding #1: SessionHost
god class) as the "turn-ack" slice: enqueuing and admitting a turn's one
early acknowledgement, latching/settling that ack's lifecycle, generating
turn ids, and tracking which delegated children belong to which turn for the
ack-ownership and sole-remaining-child cancellation decisions those turns
require.

This is the most heavily interconnected slice in the whole SessionHost
decomposition -- ``_settle_turn_ack``, ``_emit_early_ack``, and
``_schedule_ack_admission`` call each other -- and it has a real external
dependency: ``on_ack_terminal`` is wired as a callback straight into
``SpeechScheduler`` from ``SessionHost.connect()`` (server/pipeline.py). That
wiring passes ``SessionHost.on_ack_terminal``, a one-line forwarder onto this
method, rather than the bound ledger method itself; the forwarder is retained
so a test that patches the host's method still intercepts the callback.

``SessionHost.connection``/``accepts()``/``feature_policy``/``early_ack_text``
are all read through narrow callables rather than a full host reference,
mirroring ``RunnerSupervisor``'s ``Callable[[], Iterable[Any]]`` pattern: this
ledger observes them at call time and never owns them. ``connection`` is
swapped out on every reconnect/promotion, and ``config``/``feature_policy``
are reassigned on the host after construction (``scripts/smoke_conversation.py``
replaces ``host.config`` with a tuned copy), so a construction-time snapshot
would silently serve stale values.

``cancel_turn_or_child`` (SessionHost's atomic ack+work cancellation) and the
turn handlers stay on SessionHost -- both out of scope for this branch's
decomposition -- but read/mutate this ledger's turn-work-item registry and
ack latch through its public methods, exactly as they read
``self._turn_work_items``/``self._ack_emitted_turns`` before extraction.

This module is a pure extraction of SessionHost's existing ack-lifecycle
logic -- semantics, including the settle-before-admission ordering (the
Critical fix earlier on this branch) and the bounded admission retry/abandon
chain, are unchanged from the code that previously lived directly on
SessionHost.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from loguru import logger

from .speech_lifecycle import GenerationIdentity, PreAdmissionTerminalReason
from .speech_scheduler import ROLE_ACK

if TYPE_CHECKING:
    from .config import FeaturePolicy
    from .connection_pipeline import ConnectionPipeline

_ACK_ADMISSION_RETRY_DELAY_SECONDS = 0.25
"""Backoff before re-attempting a failed early-ack admission (e.g. the
connection's Pipecat worker has not attached yet). Bounded and real-time so a
persistently unavailable worker retries at a steady, low-frequency cadence
instead of a tight loop, while a short-lived unavailability window still
gets the ack delivered without waiting on an unrelated speech admission."""

_ACK_ADMISSION_MAX_ATTEMPTS = 4
"""Cap on early-ack admission retries. A worker that never attaches for a
turn's whole lifetime would otherwise retry forever at
``_ACK_ADMISSION_RETRY_DELAY_SECONDS`` cadence; past this many attempts the
ack is abandoned (its queued item discarded, its latch cleared) rather than
retried indefinitely."""

_MAX_ACK_GENERATION_TURNS = 2048
"""Cap on retained per-turn admission-chain generation counters.

``_ack_admission_generation`` cannot be popped when a turn's latch clears
(see its own comment: absence must keep meaning "no chain holds this turn",
which is what lets a stale chain fall through to the liveness path and
release its own queued ack), so it needs an explicit bound instead -- one
``TurnAckLedger`` lives for the whole ``SessionHost``, across every session
the process serves. Eviction is oldest-turn-first, and turn ids are minted
from a strictly increasing sequence, so the entries dropped are always the
least recent.

Eviction cannot resurrect the generation collision this dict exists to
prevent: generations come from one ledger-wide sequence
(``_ack_generation_sequence``), so a turn whose entry was evicted and then
re-latched claims a fresh number rather than restarting at 1 beside a chain
that still holds 1."""


class TurnAckLedger:
    """Owns a SessionHost's turn-ack latch, turn-work-item registry, and
    turn-id sequence.

    ``_ack_emitted_turns`` is the one-ephemeral-ack-per-semantic-turn latch.
    ``_turn_work_items`` is the authoritative turn -> delegated-child-work-item
    registry that ack ownership and the sole-remaining-child cancellation
    decision are answered from, rather than re-deriving the
    ``work-{turn_id}[-{index}]`` naming convention at each call site.
    ``_ack_admission_tasks`` keeps deferred ack-admission tasks referenced so
    they aren't garbage-collected mid-flight. ``_turn_sequence`` is the
    monotonic counter behind every generated turn id and every
    ``accepted_turn_sequence`` staleness check in the turn handlers.
    """

    def __init__(
        self,
        *,
        feature_policy: Callable[[], FeaturePolicy],
        early_ack_text: Callable[[], str],
        connection: Callable[[], ConnectionPipeline | None],
        accepts: Callable[[int | None], bool],
    ) -> None:
        # Thunks, not snapshots: ``host.config``/``host.feature_policy`` are
        # reassignable after this ledger is constructed, and the pre-extraction
        # code read both live off SessionHost on every ack.
        self._feature_policy = feature_policy
        self._early_ack_text = early_ack_text
        self._connection = connection
        self._accepts = accepts
        self._turn_sequence = 0
        # Turn-scoped acknowledgement latch (Requirements: one ephemeral ack
        # per semantic turn, including mixed multi-intent turns). Cleared in
        # each turn's normal completion/cancellation/failure cleanup -- never
        # retained as an unbounded process-lifetime set.
        self._ack_emitted_turns: set[str] = set()
        # One monotonic admission-chain generation counter per turn_id ever
        # latched. ``ack_work_item_id`` is derived from ``turn_id`` alone, so
        # a later eligible sibling that re-latches the same turn after an
        # earlier chain abandoned starts a brand-new chain under the
        # identical scheduler key. Without this, a stale chain's belated
        # retry timer would find the latch live again (set by the *new*
        # chain) and wrongly treat that as proof its own admission is still
        # current -- see ``_retry_or_abandon``'s generation check.
        # Deliberately *not* popped on latch clear (unlike
        # ``_ack_emitted_turns``): an absent entry means "no chain holds this
        # turn", the premise on which a stale chain falls through to the
        # liveness path and releases its own queued ack. Popping on clear
        # would make a superseded chain read the live one's turn as unheld,
        # so it would resume against the *newer* chain's latch and queued
        # item. Because it cannot be popped per turn, it is instead explicitly
        # capped at ``_MAX_ACK_GENERATION_TURNS`` with oldest-turn-first
        # eviction -- see that constant for why the cap cannot resurrect the
        # collision it exists to prevent.
        self._ack_admission_generation: OrderedDict[str, int] = OrderedDict()
        # The ledger-wide source of every generation number above. Strictly
        # increasing and never reset (not even by ``clear_all``), so no two
        # admission chains -- for the same turn or different ones -- can ever
        # hold the same number. See _claim_ack_admission_generation.
        self._ack_generation_sequence = 0
        # Authoritative turn -> delegated-child-work-item registry. Ack
        # ownership and the sole-delegated-child cancellation decision are
        # answered from this map rather than by re-deriving the
        # ``work-{turn_id}[-{index}]`` naming convention at each call site.
        # Populated at each delegated child's dispatch and dropped in the
        # owning turn handler's cleanup, alongside the ack latch.
        self._turn_work_items: dict[str, set[str]] = {}
        # Keep deferred ack-admission tasks referenced so they aren't
        # garbage-collected mid-flight; see _schedule_ack_admission.
        self._ack_admission_tasks: set[asyncio.Task[Any]] = set()

    @property
    def turn_sequence(self) -> int:
        return self._turn_sequence

    def next_turn_id(self) -> str:
        self._turn_sequence += 1
        return f"turn-{self._turn_sequence}"

    @staticmethod
    def ack_work_item_id(turn_id: str) -> str:
        """The one synthetic scheduler key this turn's ack is enqueued under."""
        return f"ack-{turn_id}"

    def clear_ack_latch(self, turn_id: str) -> None:
        self._ack_emitted_turns.discard(turn_id)

    def has_emitted_ack(self, turn_id: str) -> bool:
        """Whether ``turn_id`` currently holds the one-ack-per-turn latch.

        Public read for tests that previously asserted
        ``turn_id in ledger._ack_emitted_turns`` directly.
        """
        return turn_id in self._ack_emitted_turns

    def emitted_ack_turns(self) -> frozenset[str]:
        """Every turn id currently holding the ack latch.

        Public read for tests that previously asserted
        ``ledger._ack_emitted_turns == set()`` (or enumerated it) directly.
        """
        return frozenset(self._ack_emitted_turns)

    def admission_generation(self, turn_id: str) -> int | None:
        """The current admission-retry generation for ``turn_id``, if any.

        Public read for tests that previously read
        ``ledger._ack_admission_generation.get(turn_id)`` directly.
        """
        return self._ack_admission_generation.get(turn_id)

    def admission_generation_count(self) -> int:
        """How many turns currently hold an admission-retry generation.

        Public read for tests that previously asserted
        ``len(ledger._ack_admission_generation) == ...`` directly.
        """
        return len(self._ack_admission_generation)

    def settle_turn_ack(
        self, scheduler: Any, turn_id: str, *, cancel_admitted: bool = False
    ) -> None:
        """Retract this turn's ack and close its admission-retry chain.

        Clearing the latch is the half a bare ``discard_queued_ack`` misses:
        ``_schedule_ack_admission``'s failure path re-enqueues a fresh ack
        whenever it still reads the turn as live, so an admission failure
        landing after the turn already discarded its ack would put one back
        with nothing left to remove it -- to be spoken after the canonical
        result. The retry reads the same latch this clears.

        ``cancel_admitted`` additionally retracts an ack that already reached
        the transport, for the turns that end with nothing to say at all.
        """
        ack_work_item_id = self.ack_work_item_id(turn_id)
        if cancel_admitted:
            scheduler.cancel(ack_work_item_id)
        scheduler.discard_queued_ack(ack_work_item_id)
        self.clear_ack_latch(turn_id)

    def register_turn_work_item(self, turn_id: str, work_item_id: str) -> None:
        """Record one delegated child as belonging to ``turn_id``.

        This registry -- not the ``work-{turn_id}[-{index}]`` naming
        convention -- is the authority for ack ownership and the
        sole-delegated-child cancellation decision, so a turn id containing a
        hyphen can never make a prefix match ambiguous.
        """
        self._turn_work_items.setdefault(turn_id, set()).add(work_item_id)

    def turn_work_items(self, turn_id: str) -> set[str]:
        """This turn's currently registered delegated children.

        Returns the live set, not a copy -- read-only snapshot for callers
        like ``cancel_turn_or_child`` that only inspect membership.
        """
        return self._turn_work_items.get(turn_id, set())

    def release_all_turn_work_items(self, turn_id: str) -> None:
        """Release every delegated child of ``turn_id`` and settle its ack latch.

        Unlike ``release_turn_work_item`` (singular), which keeps the latch
        alive until the *last* child is released, this drops the whole set at
        once, so the latch is settled unconditionally alongside it.
        """
        self._turn_work_items.pop(turn_id, None)
        self.clear_ack_latch(turn_id)

    def release_turn_work_item(self, turn_id: str, work_item_id: str) -> None:
        """Release one delegated child, keeping the turn's ack alive for siblings.

        The turn's ack latch is settled only once the *last* registered child
        is released: a turn whose other children are still running in the
        background must stay resolvable by ``ack_turn_for_work_item`` so a
        later cancel can still find and settle its ack.
        """
        items = self._turn_work_items.get(turn_id)
        if items is None:
            return
        items.discard(work_item_id)
        if not items:
            self._turn_work_items.pop(turn_id, None)
            self.clear_ack_latch(turn_id)

    def ack_turn_for_work_item(self, work_item_id: str) -> str | None:
        """The latched semantic turn that owns ``work_item_id``, if any.

        A control message ("stop that") arrives as its own semantic turn, so
        the turn being cancelled is never the control turn: the ack that must
        be settled belongs to the *target child's* turn. Resolved from the
        explicit turn -> delegated-child registry rather than by re-deriving
        the work-item naming convention here; only a turn that still holds an
        ack latch can own an ack to settle.
        """
        for candidate, work_items in self._turn_work_items.items():
            if work_item_id in work_items and candidate in self._ack_emitted_turns:
                return candidate
        return None

    def on_ack_terminal(
        self, identity: GenerationIdentity, reason: PreAdmissionTerminalReason
    ) -> None:
        """Idempotent latch clear for acks terminalized before admission.

        Injected into ``SpeechScheduler`` at connection setup; invoked only
        when an ack is terminalized before admission (``no_tts`` /
        ``unavailable_transport`` / ``connection_closed``), never for a normal
        admitted completion.

        Scope note: this is the sole mutator for *this* path, not for the ack
        latch as a whole. The latch is also cleared by ``settle_turn_ack``
        (every turn-handler retraction site), by ``clear_ack_latch`` /
        ``release_turn_work_item`` / ``release_all_turn_work_items`` (turn
        cleanup), and by the retry chain's not-live and abandon branches.
        Terminal notifications likewise have two producers, only one of which
        originates in the lifecycle: ``SpeechScheduler.start_next`` forwards
        ``SpeechLifecycleCoordinator.pre_admission_disposition``'s reason,
        while ``SpeechScheduler._notify_ack_swept`` synthesizes its own for
        the interrupt sweep. Collapsing those into a single settling entry
        point that names the cause is recorded as a follow-up in the dev plan,
        not claimed here.
        """
        del reason
        if identity.turn_id is not None:
            self.clear_ack_latch(identity.turn_id)

    async def emit_early_ack(
        self,
        origin: ConnectionPipeline,
        *,
        turn_id: str,
        origin_epoch: int,
        dispatched: bool,
        search_task: asyncio.Task[Any] | None = None,
    ) -> None:
        """Enqueue this turn's one delegation-confirmed ack.

        Latches at the moment the ack is enqueued, so later eligible
        multi-intent children in the same turn are no-ops (Requirements: one
        ephemeral ack per semantic turn, never one per child, never claiming
        progress that hasn't happened). A child that bails before enqueueing
        does not consume the turn's one ack slot -- otherwise a turn whose
        first child had no TTS (or resolved instantly) would be permanently
        ack-less.

        ``dispatched`` is required at every call site and states whether the
        caller has *already* handed this turn's search to the coordinator:

        * ``dispatched=True`` -- the caller dispatched the search itself and
          passes the resulting handle as ``search_task``. A ``None`` handle
          means the coordinator refused the work for lack of capacity, so
          there is no delegation to acknowledge and no ack is queued: the
          turn is about to speak "the search service is busy", which an ack
          would directly contradict. A live handle yields exactly one
          scheduling tick first, so a search that already resolved has no
          real delegation latency to acknowledge either.
        * ``dispatched=False`` -- the pending-dialogue and multi-intent paths,
          where dispatch happens inside ``coordinator.submit`` and the plan
          requires the ack at the delegation *decision* (dev plan Phase 1:
          "invoke it immediately after the first eligible multi-intent child
          decision"). There is no handle to inspect at this point, and
          ``search_task`` must be ``None``.
        """
        if dispatched and search_task is None:
            # Capacity-rejected dispatch: nothing was delegated, so nothing
            # may be acknowledged. Not latched either -- a later eligible
            # child of the same turn can still claim the turn's one ack.
            return
        if not dispatched and search_task is not None:
            raise ValueError("search_task may only be supplied on an already-dispatched path")
        if not self._feature_policy().enable_early_ack or turn_id in self._ack_emitted_turns:
            return
        if (
            origin.tts is None
            or self._connection() is not origin
            or not origin.active
            or not self._accepts(origin_epoch)
        ):
            return
        if search_task is not None:
            await asyncio.sleep(0)
            if search_task.done():
                return
        ack_work_item_id = self.ack_work_item_id(turn_id)

        def enqueue_ack() -> None:
            origin.scheduler.enqueue(
                result_id=None,
                work_item_id=ack_work_item_id,
                run_id=f"run-{ack_work_item_id}",
                text=self._early_ack_text(),
                origin_epoch=origin_epoch,
                role=ROLE_ACK,
                ack_id=ack_work_item_id,
                turn_id=turn_id,
            )

        # Latch only on the path that actually enqueues: a child that bailed
        # above (no TTS, superseded origin, stale epoch, or a search that
        # resolved within one tick) never spent this turn's single ack slot,
        # so a later genuinely-slow child of the same turn can still claim it.
        enqueue_ack()
        self._ack_emitted_turns.add(turn_id)
        self._schedule_ack_admission(
            origin,
            ack_work_item_id,
            enqueue_ack,
            turn_id=turn_id,
            origin_epoch=origin_epoch,
            attempt=1,
        )

    def _claim_ack_admission_generation(self, turn_id: str) -> int:
        """Supersede whatever admission chain previously held ``turn_id``.

        The returned generation is strictly greater than any previously
        issued by this ledger -- for this turn or any other -- which is what
        lets ``_retry_or_abandon`` and ``admit`` tell a stale chain's belated
        retry from the live one.
        Enforces ``_MAX_ACK_GENERATION_TURNS`` here rather than at every
        mutation site, so the bound has exactly one definition.
        """
        # Drawn from ONE ledger-wide sequence, not from this turn's own
        # counter + 1: a per-turn counter restarts at 1 for a turn whose entry
        # _MAX_ACK_GENERATION_TURNS evicted, and most turns only ever have one
        # chain -- so the evicted-but-still-live chain also held generation 1
        # and would read itself as current, which is exactly the collision the
        # bail-out exists to prevent (round 6 confirm pass 3, Logic Minor). A
        # ledger-wide sequence is never reused by any turn, so "the entry
        # holds a number that is not mine" always means superseded and
        # "there is no entry" always means evicted.
        self._ack_generation_sequence += 1
        generation = self._ack_generation_sequence
        self._ack_admission_generation[turn_id] = generation
        # Newest-first retention: a turn claiming a fresh chain becomes the
        # most recently used entry, so the eviction below only ever discards
        # turns whose chains are long finished.
        self._ack_admission_generation.move_to_end(turn_id)
        while len(self._ack_admission_generation) > _MAX_ACK_GENERATION_TURNS:
            self._ack_admission_generation.popitem(last=False)
        return generation

    def _schedule_ack_admission(
        self,
        origin: ConnectionPipeline,
        ack_work_item_id: str,
        enqueue_ack: Callable[[], None],
        *,
        turn_id: str,
        origin_epoch: int,
        attempt: int = 1,
        generation: int | None = None,
    ) -> None:
        """Admit a just-enqueued ack on a later scheduling tick, not inline.

        The caller's enqueue is synchronous and immediate -- the ack is
        visible in the scheduler's own bookkeeping the instant delegation is
        confirmed. Admission is deferred so a same-turn reconnect, or a
        result that is already ready, can still discard the ack while it is
        merely queued (``discard_queued_ack`` / ``SpeechScheduler.interrupt``)
        before it is ever handed to the transport.

        ``attempt`` counts this call as one admission attempt (the caller's
        own initial ``enqueue_ack()`` plus this scheduling call is attempt 1).
        A persistently unavailable worker (e.g. never attaches for the whole
        turn) would otherwise retry forever at
        ``_ACK_ADMISSION_RETRY_DELAY_SECONDS`` cadence; past
        ``_ACK_ADMISSION_MAX_ATTEMPTS`` the ack is abandoned instead --
        its queued item discarded and the turn's ack latch cleared -- rather
        than left retrying indefinitely.

        Clearing the turn's ack latch on abandonment is a deliberate
        at-most-one-per-attempt-chain guarantee, not an at-most-one-
        delivered guarantee: it also makes the latch available again, so a
        later eligible sibling in the same multi-intent turn can still
        enqueue its own ack after this one is abandoned. That is safe --
        the abandoned ack was discarded, never spoken -- but "one ack per
        turn" should be read as "one ack per admission attempt chain," not
        as a promise that at most one ack is ever queued for a turn.

        ``generation`` identifies this call as belonging to one particular
        admission chain. ``None`` (the default, and what every entry point
        outside this method's own retry recursion passes) means "start a
        fresh chain": a new generation number is claimed here, superseding
        whatever chain previously held ``turn_id``. That covers both
        ``emit_early_ack`` (a later eligible sibling re-latching the same
        turn after an earlier chain abandoned) and callers that schedule
        admission directly without any generation bookkeeping of their own.
        Only this method's own ``retry_after_delay`` continuation passes an
        explicit ``generation``, to prove *its* retry still belongs to the
        chain that scheduled it -- see ``_retry_or_abandon``.
        """
        if generation is None:
            generation = self._claim_ack_admission_generation(turn_id)

        def _retry_or_abandon(*, log_reason: str, needs_requeue: bool) -> None:
            # Shared by both admission-failure modes: an exception from
            # start_next (e.g. the worker never attached) and a clean
            # ``None`` return (the transport slot is occupied by another
            # utterance). Both leave the ack still needing a home, so both
            # must feed the same bounded retry/abandon chain instead of one
            # of them silently doing nothing. ``needs_requeue`` distinguishes
            # them: the exception path's own except-arm already discarded
            # the item from scheduler bookkeeping, so it needs a fresh
            # ``enqueue_ack()`` before the retry; the busy-slot ``None``
            # path never dequeued the item in the first place (start_next
            # returns early, before popping, when the slot is occupied), so
            # calling ``enqueue_ack()`` there would queue a duplicate.
            current_generation = self._ack_admission_generation.get(turn_id)
            if current_generation is not None and current_generation != generation:
                # This chain has been superseded: the turn's latch was
                # cleared (settle/abandon) and a later eligible sibling
                # already started a fresh chain under the same
                # ``ack_work_item_id``. The liveness check below cannot
                # tell the two apart -- it would see the latch live again
                # and wrongly treat that as proof this stale chain is still
                # current -- so bail out here first, before touching the
                # newer chain's queued item or latch at all.
                #
                # ``current_generation is None`` is deliberately NOT this
                # branch: absence means the entry was EVICTED by
                # _MAX_ACK_GENERATION_TURNS, not that a newer chain exists.
                # Bailing out on absence took the "a newer chain will settle
                # it" exit under a premise that no longer holds -- no chain
                # existed -- so the queued ack and its latch were never
                # released. Fall through to the normal liveness/retry path
                # instead (round-5 restart, Logic Minor).
                logger.debug(
                    "early ack admission chain for {} is stale (superseded by "
                    "a newer chain); dropping it silently",
                    turn_id,
                )
                return
            if (
                turn_id not in self._ack_emitted_turns
                or self._connection() is not origin
                or not origin.active
                or origin.epoch != origin_epoch
                or not self._accepts(origin_epoch)
            ):
                logger.debug(
                    "early ack {} and its turn/epoch is no longer live; "
                    "discarding it instead of re-queueing",
                    log_reason,
                )
                # Both terminal exits of the retry chain must leave the latch
                # in the same state. The abandon branch below settles it; this
                # one used to return with the latch still set, and on the
                # ``needs_requeue=True`` path there is nothing queued for
                # ``SpeechScheduler.interrupt``'s reconnect sweep to find and
                # route through ``on_ack_terminal``, so the latch survived
                # until the owning turn handler's ``finally``. Inside that
                # window a later eligible multi-intent sibling calling
                # ``emit_early_ack`` short-circuits on the latch and the turn
                # loses its only remaining chance at an ack.
                #
                # Clearing the latch alone is not enough, though: on the
                # ``needs_requeue=False`` (busy-slot) path the item was never
                # dequeued, so a bare latch clear would leave the turn with a
                # queued ack AND a free latch -- the same asymmetry in the
                # opposite direction, letting a sibling enqueue a second ack
                # under the same ``ack_work_item_id`` on a scheduler that
                # still holds the first. ``settle_turn_ack`` does both halves,
                # mirroring the abandon branch below, and its
                # ``discard_queued_ack`` is idempotent so it is a no-op on the
                # ``needs_requeue=True`` path where nothing is queued.
                self.settle_turn_ack(origin.scheduler, turn_id)
                return
            if attempt >= _ACK_ADMISSION_MAX_ATTEMPTS:
                logger.debug(
                    "early ack {} after {} attempts; abandoning it "
                    "instead of retrying indefinitely",
                    log_reason,
                    attempt,
                )
                self.settle_turn_ack(origin.scheduler, turn_id)
                return
            logger.debug("early ack {}; leaving it queued for a later retry", log_reason)
            if needs_requeue:
                enqueue_ack()

            async def retry_after_delay() -> None:
                await asyncio.sleep(_ACK_ADMISSION_RETRY_DELAY_SECONDS)
                self._schedule_ack_admission(
                    origin,
                    ack_work_item_id,
                    enqueue_ack,
                    turn_id=turn_id,
                    origin_epoch=origin_epoch,
                    generation=generation,
                    attempt=attempt + 1,
                )

            retry_task = asyncio.create_task(retry_after_delay())
            self._ack_admission_tasks.add(retry_task)
            retry_task.add_done_callback(self._ack_admission_tasks.discard)

        async def admit() -> None:
            current_generation = self._ack_admission_generation.get(turn_id)
            if current_generation is not None and current_generation != generation:
                # Stale before ever calling start_next: the turn's latch was
                # cleared and a later sibling already started a fresh chain
                # under this same key while this attempt sat scheduled. The
                # check inside ``_retry_or_abandon`` alone is not enough --
                # it only runs *after* start_next has already been called
                # once for this (dead) chain, which is one live admission
                # attempt this chain has no business making. Catching it
                # here, before that call, means a superseded chain never
                # touches the scheduler at all.
                #
                # As in ``_retry_or_abandon``, absence means evicted, not
                # superseded -- an evicted-but-live chain proceeds normally
                # rather than dropping its ack and latch on the floor.
                logger.debug(
                    "early ack admission chain for {} is stale (superseded by "
                    "a newer chain) before starting; dropping it silently",
                    turn_id,
                )
                return
            try:
                admitted = await origin.scheduler.start_next(ack_work_item_id)
            except Exception:  # noqa: BLE001 - ack submission failure must never crash the turn
                # The ack is ephemeral and best-effort: a submission failure
                # (e.g. the connection's Pipecat worker has not attached
                # yet) must never crash the turn's real delegated work.
                # start_next's own except-path already released the
                # coordinator slot and discarded the item from scheduler
                # bookkeeping entirely, so re-queue a fresh ack item rather
                # than leaving none at all -- but only while the ack is still
                # live. A cancellation, reconnect, or newer promoted epoch
                # racing this failure would otherwise leave a stale ack queued
                # on a turn that no longer exists, to be spoken later.
                logger.opt(exception=True).debug("early ack failed to start")
                _retry_or_abandon(log_reason="failed to start", needs_requeue=True)
                return
            if admitted is None:
                # start_next returns ``None`` without raising whenever the
                # transport slot is occupied -- the common case when a prior
                # utterance is still speaking at delegation time. That is
                # not success: the ack item was never dequeued (start_next
                # returns before popping when the slot is occupied), so it
                # must feed the same bounded retry/abandon chain the
                # exception path uses, or it stays queued indefinitely past
                # _ACK_ADMISSION_MAX_ATTEMPTS.
                _retry_or_abandon(log_reason="found the transport slot busy", needs_requeue=False)

        task = asyncio.create_task(admit())
        self._ack_admission_tasks.add(task)
        task.add_done_callback(self._ack_admission_tasks.discard)

    def pending_admission_tasks(self) -> tuple[asyncio.Task[Any], ...]:
        """Every currently in-flight ack-admission task, for shutdown sweeps."""
        return tuple(self._ack_admission_tasks)

    def clear_all(self) -> None:
        """Drop every tracked turn-work-item registration and ack latch.

        For shutdown, alongside the pending-admission-task sweep -- not for
        per-turn cleanup, which goes through ``release_all_turn_work_items``/
        ``clear_ack_latch`` instead.
        """
        self._turn_work_items.clear()
        self._ack_emitted_turns.clear()
        self._ack_admission_generation.clear()
