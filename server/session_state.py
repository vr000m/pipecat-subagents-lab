"""Authoritative process-lifetime session state and delivery state machine."""

from __future__ import annotations

import json
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NamedTuple
from uuid import uuid4

from loguru import logger

from .contracts import (
    WORK_STATUS_TERMINAL,
    DeliveryState,
    GroundedResult,
    RoutingState,
    RuntimeSnapshot,
    SpeechProgress,
    TerminalReason,
    TranscriptEntry,
    WorkerState,
    WorkStatus,
    WorkStatusState,
    legal_work_status_transition,
)
from .results import ResultLog

# Both retention numbers below are the single source of truth
# shared/work-status-retention.json -- web/src/state.js loads the same file
# for its WORK_STATUS_TERMINAL_TTL_MS / WORK_STATUS_MAX_KEYS. Only the
# numeric bounds live there; the eligibility/ordering rules (terminal-only
# eviction, terminal-first/oldest-first tiebreak) are hand-implemented per
# language below and in web/src/state.js's evictOldestWorkStatus, and are
# pinned by parity tests in tests/test_session_state.py and
# web/test/state.test.js against shared/protocol.md's "Progressive work
# status" retention section.
_RETENTION_CONFIG_PATH = Path(__file__).resolve().parents[1] / "shared/work-status-retention.json"
# server/config.py documents that the shared/ tree is deliberately excluded
# from the deployable package (a packaged/standalone install has no
# docs/benchmarks or shared/schemas tree either) and every caller there
# degrades gracefully rather than raising. This module previously assumed
# shared/ is always a sibling of server/ and crashed at import time
# otherwise; fall back to the values shared/work-status-retention.json
# itself documents as defaults so a packaged install can still import.
#: The packaged-install fallback. This is a *second copy* of numbers whose
#: single source of truth is shared/work-status-retention.json, so it is
#: pinned by ``tests/test_session_state.py``'s
#: ``test_retention_fallback_matches_shared_config``: editing the JSON without
#: editing this dict fails CI rather than shipping a packaged install that
#: silently disagrees with dev/CI (and with web/src/state.js) about
#: terminal-record retention.
_RETENTION_FALLBACK = {"ttl_seconds": 300, "max_keys": 256}

try:
    _retention_config = json.loads(_RETENTION_CONFIG_PATH.read_text())
except (OSError, ValueError):
    _retention_config = dict(_RETENTION_FALLBACK)

# Terminal work_status records remain in capable-client snapshots for a fixed
# five-minute session-clock TTL (Requirements). SessionState has no timer
# source, so pruning is lazy: a record is excluded from the projection once
# its age is >= this TTL, never removed by a background timer.
WORK_STATUS_TTL_SECONDS = float(_retention_config["ttl_seconds"])

_TERMINAL = {
    DeliveryState.DELIVERY_COMPLETED,
    DeliveryState.DELIVERY_UNKNOWN,
    DeliveryState.INTERRUPTED,
    DeliveryState.INTERRUPTED_BY_RECONNECT,
}
_RANK = {
    DeliveryState.DISPLAYED: 0,
    DeliveryState.QUEUED: 1,
    DeliveryState.STARTED: 2,
    DeliveryState.SYNTHESIS_ENDED: 3,
    DeliveryState.PAUSED: 3,
    # A replay may transition from RESUMED back to STARTED.
    DeliveryState.RESUMED: 2,
    DeliveryState.DELIVERY_UNKNOWN: 5,
    DeliveryState.DELIVERY_COMPLETED: 6,
    DeliveryState.INTERRUPTED: 7,
    DeliveryState.INTERRUPTED_BY_RECONNECT: 8,
}


def _now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class StateEvent:
    sequence: int
    kind: str
    payload: dict[str, Any]


class WorkStatusKey(NamedTuple):
    """Identity of one client-visible parent work-status record.

    ``parent_key`` is the delegated-child grouping key -- the parent
    ``work_item_id`` for a mixed multi-intent turn, or the sole
    ``work_item_id`` for a single-delegation turn. Only an
    ``existing_worker``/``new_worker`` routing *decision* ever allocates a
    key; a ``direct``/``unsupported``/``clarify`` routing action never does
    (Requirements). That gate is about how the key comes to exist, not which
    states it may later reach: a worker-raised ``clarify``/``declined`` on an
    already-delegated child settles that child terminal at ``result_ready``
    once its canonical result commits -- not a non-terminal state -- and at
    ``failed`` instead if that commit fails or is stale.
    """

    origin_epoch: int | None
    turn_id: str
    parent_key: str


@dataclass
class _WorkStatusRecord:
    status: WorkStatus
    terminal_at: float | None = None


class SessionState:
    """The single source of truth projected to each active connection."""

    # Hard ceiling on distinct WorkStatusKeys retained by one process-lifetime
    # SessionState. TTL pruning alone is not a bound: a session that never
    # requests a work-status snapshot would otherwise accumulate one ledger
    # entry per delegated turn forever. Mirrors the handshake-token cap in
    # pipeline.py (evict oldest-first once over the cap). Sourced from
    # shared/work-status-retention.json -- see the module-level comment above
    # _RETENTION_CONFIG_PATH.
    _MAX_WORK_STATUS_KEYS = int(_retention_config["max_keys"])

    # Hard ceiling on the raw event log `_emit` appends to. Nothing else in
    # this class prunes it -- unlike the work-status ledger above, it has no
    # TTL of its own -- so a long-running session (or one deliberately kept
    # alive across many reconnects/turns) would otherwise grow it for the
    # process lifetime. The only reader is `RuntimeObserver.messages()`
    # (server/observers.py), an explicitly diagnostic, non-network API that
    # replays from the start of this log; a `deque(maxlen=...)` bounds
    # memory at the cost of that replay silently starting from whatever the
    # oldest retained event is once a session exceeds the cap, the same
    # "diagnostic view degrades gracefully" trade the work-status TTL/cap
    # above already makes for its own ledger.
    _MAX_EVENTS = 2000

    def __init__(self, session_id: str | None = None, resume_token: str | None = None) -> None:
        self.session_id = session_id or f"session-{uuid4().hex}"
        RuntimeSnapshot.reset_monotonicity(self.session_id)
        self.resume_token = resume_token or uuid4().hex
        self.sequence = 0
        self.workers: dict[str, WorkerState] = {}
        self.results = ResultLog()
        self.speech: dict[str, SpeechProgress] = {}
        self.routing: RoutingState | None = None
        self.transcript: list[TranscriptEntry] = []
        self._events: deque[StateEvent] = deque(maxlen=self._MAX_EVENTS)
        self.active_epoch: int | None = None
        self._speech_history: dict[str, list[SpeechProgress]] = {}
        self._listeners: list[Callable[[StateEvent], Any]] = []
        # Phase 3 work-status ledger. Children are keyed per delegated work
        # item; the parent ledger holds the one client-visible aggregate per
        # WorkStatusKey. Both hold the per-key event_sequence independently
        # of the global SessionState sequence (Requirements/Sequence
        # namespaces).
        # Ledger invariant: _work_status_children and _work_status_parents are
        # keyed in lockstep, and every insertion touches all three dicts
        # together. Eviction is deliberately *not* symmetric:
        # _work_status_sequence outlives the record it counts for. An evicted
        # key can still be written again -- a sibling multi-intent child, or a
        # late commit's finalization, may call set_child_work_status for the
        # same key after its parent went terminal -- and dropping the counter
        # would restart event_sequence at 1, which the client reducer rejects
        # as stale. Unlike the records, this counter is never bounded: a
        # NamedTuple key plus one int is cheap enough that unbounded growth
        # for the process lifetime is preferable to any eviction policy that
        # could resurrect a key at sequence 1. Round-8 review re-raised the
        # growth of this map and of _work_status_terminal_keys as a
        # resource bound; it is a knowingly accepted trade, pinned by
        # tests/test_session_state.py::
        # test_work_status_sequence_survives_high_volume_eviction_and_never_restarts_at_one.
        self._work_status_children: dict[WorkStatusKey, dict[str, WorkStatus]] = {}
        self._work_status_parents: dict[WorkStatusKey, _WorkStatusRecord] = {}
        self._work_status_sequence: dict[WorkStatusKey, int] = {}
        # Keys whose parent record was terminal when it was dropped (TTL
        # expiry or overflow eviction). Bounded by the same reasoning as
        # _work_status_sequence -- a NamedTuple key in a set is cheap -- and
        # kept for the same reason: without it a post-drop child record
        # cold-starts the key and re-derives a parent state from a single
        # child, resurrecting a record clients already applied as terminal.
        self._work_status_terminal_keys: set[WorkStatusKey] = set()
        # Keys whose parent aggregate was rehydrated by ``from_snapshot``. The
        # wire carries the parent aggregate but never the child set it was
        # derived from, so such a key's children map is known-incomplete and
        # may never be terminalized from (see ``_reaggregate_parent``).
        #
        # Non-authoritativeness is a property of the *key*, not of one record
        # -- re-aggregating over a still-partial child set does not make that
        # set complete, and neither does dropping the record. So it lives
        # here, alongside _work_status_sequence and _work_status_terminal_keys,
        # which already have exactly this key-outlives-record lifetime, rather
        # than being hand-copied onto each successive _WorkStatusRecord. It is
        # a tombstone in the same sense _work_status_terminal_keys is: once a
        # key has been restored from the wire, no later child write can prove
        # its child set complete, so a record TTL-pruned and then cold-started
        # by a fresh child report is still non-authoritative and still must
        # not terminalize. Bounded by the same accepted trade as the other two
        # key-scoped maps.
        self._work_status_nonauthoritative_keys: set[WorkStatusKey] = set()
        # The *record-scoped* expiry clock for those keys: the monotonic
        # instant the current record for a non-authoritative key started
        # being retained. A non-authoritative record can never reach a
        # terminal state, so it never gets a ``terminal_at`` -- the TTL-prune
        # and overflow-eviction paths both key off ``terminal_at``, and
        # without this it would be retained and re-shipped on every snapshot
        # for the process lifetime. Both paths consult this map (via
        # ``_work_status_expiry_stamp``) so the record expires on the same
        # five-minute TTL a terminal one does. Unlike the key set above, this
        # is dropped with the record it clocks and re-stamped when a new
        # record is written for the key, so a cold-started record gets a full
        # fresh TTL instead of inheriting an already-expired one.
        self._work_status_nonauthoritative_at: dict[WorkStatusKey, float] = {}

    @property
    def events(self) -> tuple[StateEvent, ...]:
        return tuple(self._events)

    def subscribe(self, listener: Callable[[StateEvent], Any]) -> Callable[[], None]:
        """Register a live projection listener.

        Listeners are deliberately observational: they cannot replace the
        authoritative state mutation and are expected to fence by epoch.
        """
        self._listeners.append(listener)

        def unsubscribe() -> None:
            try:
                self._listeners.remove(listener)
            except ValueError:
                pass

        return unsubscribe

    def _emit(self, kind: str, payload: dict[str, Any]) -> StateEvent:
        self.sequence += 1
        event = StateEvent(self.sequence, kind, payload)
        self._events.append(event)
        for listener in tuple(self._listeners):
            listener(event)
        return event

    def set_worker(self, worker: WorkerState) -> StateEvent | None:
        if self.active_epoch is not None and (
            worker.origin_epoch is None or worker.origin_epoch != self.active_epoch
        ):
            return None
        self.workers[worker.worker_id] = worker
        return self._emit("worker", worker.model_dump(mode="json"))

    def set_routing(self, routing: RoutingState) -> StateEvent | None:
        if self.active_epoch is not None and (
            routing.origin_epoch is None or routing.origin_epoch != self.active_epoch
        ):
            return None
        self.routing = routing
        return self._emit("routing", routing.model_dump(mode="json"))

    def append_transcript(self, entry: TranscriptEntry) -> StateEvent | None:
        if self.active_epoch is not None and (
            entry.origin_epoch is None or entry.origin_epoch != self.active_epoch
        ):
            return None
        if any(
            item.role == entry.role and item.turn_id == entry.turn_id and item.text == entry.text
            for item in self.transcript
        ):
            return None
        self.transcript.append(entry)
        kind = "user_transcript" if entry.role == "user" else "bot_transcript"
        return self._emit(kind, entry.model_dump(mode="json"))

    def append_result(
        self,
        result: GroundedResult,
        sequence: int | None = None,
        origin_epoch: int | None = None,
    ) -> StateEvent | None:
        if self.active_epoch is not None and origin_epoch is None:
            return None
        if (
            origin_epoch is not None
            and result.origin_epoch is not None
            and origin_epoch != result.origin_epoch
        ):
            return None
        if origin_epoch is not None and result.origin_epoch is None:
            result = result.model_copy(update={"origin_epoch": origin_epoch})
        if any(item.result_id == result.result_id for item in self.results.results):
            # Duplicate suppression is an internal idempotency decision. It is
            # not a public event because consuming a sequence number for it
            # would create a gap in the authoritative browser projection.
            return None
        effective_epoch = origin_epoch if origin_epoch is not None else result.origin_epoch
        if self.active_epoch is not None and effective_epoch != self.active_epoch:
            # Late provider work is still an immutable result commit, but it cannot
            # update active worker pointers or speech state.
            self.results.append(result)
            return self._emit("result", result.model_dump(mode="json"))
        if sequence is not None and sequence > self.sequence:
            self.sequence = sequence - 1
        self.results.append(result)
        worker = self.workers.get(result.worker_id)
        if worker:
            self.workers[result.worker_id] = worker.model_copy(
                update={"latest_result_id": result.result_id}
            )
        return self._emit("result", result.model_dump(mode="json"))

    # `commit_result` and `append_result` are the same operation under two
    # names: this ledger has no distinct "append" vs "commit" semantics
    # (there is no staging/undo step between them), so the alias exists only
    # for call-site readability (`SessionHost.commit_late_result_once` and
    # `pipeline.py`'s `_commit_result_state` read naturally as committing a
    # result, not merely appending one). Prefer `append_result` in new code.
    commit_result = append_result

    def result_history(self, worker_id: str) -> tuple[GroundedResult, ...]:
        return self.results.for_worker(worker_id)

    def speech_progress(
        self,
        *,
        result_id: str,
        work_item_id: str,
        run_id: str,
        utterance_id: str,
        state: DeliveryState,
        origin_epoch: int | None = None,
        allow_stale_reconnect: bool = False,
    ) -> SpeechProgress:
        if (
            self.active_epoch is not None
            and not allow_stale_reconnect
            and (origin_epoch is None or origin_epoch != self.active_epoch)
        ):
            return self.speech.get(
                utterance_id,
                SpeechProgress(
                    result_id=result_id,
                    work_item_id=work_item_id,
                    run_id=run_id,
                    utterance_id=utterance_id,
                    state=state,
                    origin_epoch=origin_epoch,
                ),
            )
        previous = self.speech.get(utterance_id)
        if previous is not None and previous.state in _TERMINAL:
            return previous
        if previous is not None and _RANK[state] < _RANK[previous.state]:
            return previous
        progress = SpeechProgress(
            result_id=result_id,
            work_item_id=work_item_id,
            run_id=run_id,
            utterance_id=utterance_id,
            state=state,
            origin_epoch=origin_epoch,
        )
        self.speech[utterance_id] = progress
        self._speech_history.setdefault(utterance_id, []).append(progress)
        self._emit("speech_progress", progress.model_dump(mode="json"))
        return progress

    def apply_speech_progress(
        self,
        progress: SpeechProgress,
        origin_epoch: int | None = None,
        allow_stale_reconnect: bool = False,
    ) -> SpeechProgress:
        return self.speech_progress(
            result_id=progress.result_id,
            work_item_id=progress.work_item_id,
            run_id=progress.run_id,
            utterance_id=progress.utterance_id,
            state=progress.state,
            origin_epoch=origin_epoch if origin_epoch is not None else progress.origin_epoch,
            allow_stale_reconnect=allow_stale_reconnect,
        )

    def speech_history(self, utterance_id: str) -> tuple[SpeechProgress, ...]:
        return tuple(self._speech_history.get(utterance_id, ()))

    # -- Phase 3: progressive work status --------------------------------

    def set_child_work_status(
        self,
        *,
        turn_id: str,
        work_item_id: str,
        parent_work_item_id: str | None = None,
        worker_id: str | None = None,
        state: WorkStatusState,
        origin_epoch: int | None,
        terminal_reason: TerminalReason | None = None,
    ) -> StateEvent | None:
        """Record one delegated child's status and re-emit the parent aggregate.

        Only delegated children participate in the client-visible parent
        ledger. The parent join is exhaustive: routing while any child is
        routing; searching while any child is searching and none routing;
        background once no child is active but at least one remains
        retained; and, once every child is terminal, failed wins over
        cancelled (all-cancelled) which wins over result_ready (Requirements).
        """
        key = WorkStatusKey(origin_epoch, turn_id, parent_work_item_id or work_item_id)
        # Validate before touching the ledger, and validate unconditionally.
        # A missing prior record is a *cold start*, not "no rule to apply":
        # legal_work_status_transition(None, ...) is what rejects inventing a
        # child directly at `cancelled`, which is exactly the guarantee
        # SessionHost._cancel_child_work_statuses documents for its blind
        # whole-child-set sweep. Short-circuiting on `previous_child is not
        # None` made that branch unreachable from every production caller.
        # Nothing is inserted on the reject path either: a bare
        # setdefault() would leave an empty children dict keyed without a
        # matching _work_status_parents/_work_status_sequence entry, breaking
        # the three-dict lockstep invariant documented in __init__.
        existing_children = self._work_status_children.get(key)
        previous_child = existing_children.get(work_item_id) if existing_children else None
        if not legal_work_status_transition(
            previous_child.state if previous_child is not None else None, state
        ):
            return None
        children = (
            existing_children
            if existing_children is not None
            else self._work_status_children.setdefault(key, {})
        )
        children[work_item_id] = WorkStatus(
            turn_id=turn_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            state=state,
            event_sequence=0,
            terminal_reason=terminal_reason,
            origin_epoch=origin_epoch,
        )
        return self._reaggregate_parent(key)

    def _reaggregate_parent(self, key: WorkStatusKey) -> StateEvent | None:
        children = list(self._work_status_children.get(key, {}).values())
        if not children:
            return None
        parent_state, parent_reason = self._aggregate(children)
        record = self._work_status_parents.get(key)
        if record is None and key in self._work_status_terminal_keys:
            # The record is gone (TTL expiry or overflow eviction) but it was
            # terminal when it went. Re-deriving a state here would resurrect
            # the key below a terminal state clients have already applied,
            # which the live-record guard below would have refused.
            #
            # A cold-start child write still legally lands in
            # _work_status_children before this method runs (that rejection
            # is set_child_work_status's job, not this one's), but since no
            # parent record is ever (re)written for a tombstoned key, that
            # children entry would otherwise never be visited by TTL pruning
            # or overflow eviction -- both scan only _work_status_parents --
            # and would leak for the process lifetime. Drop it here so the
            # three-dict lockstep invariant (children keys == parents keys)
            # holds: a tombstoned key has neither.
            self._work_status_children.pop(key, None)
            return None
        # ``terminal_reason`` is first-write-wins by construction, not by
        # oversight: WorkStatus only permits a reason on ``failed``, and
        # ``failed`` is terminal, so the no-regression guard below already
        # forbids a second reason from ever landing on the same key.
        if record is not None and record.status.state == parent_state:
            return None
        # The parent's "state" is a pure recomputation over the current child
        # set on every call, not a step in its own forward state machine, so
        # legal_work_status_transition (built for one entity's sequential
        # progression) does not apply here. The only invariant to preserve is
        # that a terminal parent state never regresses.
        if record is not None and record.status.state in WORK_STATUS_TERMINAL:
            return None
        if key in self._work_status_nonauthoritative_keys and parent_state in WORK_STATUS_TERMINAL:
            # A restored record's children map holds only the children that
            # have reported *since* the restore, not the set the restored
            # aggregate was computed over (child records are server-internal
            # and never cross the wire). Terminalizing off that partial set
            # would announce ``result_ready`` for a turn whose siblings are
            # still searching. There is no way to prove completeness from the
            # wire, so the conservative reading wins: a restored parent keeps
            # its restored non-terminal state and expires on the TTL measured
            # from its restore instant (``_work_status_nonauthoritative_at``,
            # honoured by both ``prune_expired_work_status`` and
            # ``_evict_work_status_overflow``) rather than claiming a result
            # that may not exist. Non-terminal re-aggregation still flows
            # through normally.
            #
            # This holds whether or not a record is currently live: the key's
            # non-authoritativeness is a tombstone, so a record already
            # TTL-pruned and now cold-started by a fresh child report is just
            # as unable to prove its child set complete. With no record to
            # write, that cold-started children entry would never be visited
            # by TTL pruning or overflow eviction (both scan only
            # _work_status_parents), so drop it here for the same reason the
            # terminal-tombstone branch above does.
            if record is None:
                self._work_status_children.pop(key, None)
            return None
        next_sequence = self._work_status_sequence.get(key, 0) + 1
        self._work_status_sequence[key] = next_sequence
        status = WorkStatus(
            turn_id=key.turn_id,
            work_item_id=key.parent_key,
            worker_id=None,
            state=parent_state,
            event_sequence=next_sequence,
            terminal_reason=parent_reason,
            origin_epoch=key.origin_epoch,
        )
        now = time.monotonic()
        terminal_at = now if parent_state in WORK_STATUS_TERMINAL else None
        # Non-authoritativeness itself needs no re-threading: it lives on the
        # key in ``_work_status_nonauthoritative_keys`` and so survives every
        # rewrite -- and every drop -- of the record by construction. Its
        # *retention clock* is record-scoped, though: ``setdefault`` leaves a
        # live restored record clocked from its original restore instant while
        # giving a record cold-started after a prune a full fresh TTL rather
        # than an already-expired one.
        if key in self._work_status_nonauthoritative_keys:
            self._work_status_nonauthoritative_at.setdefault(key, now)
        self._work_status_parents[key] = _WorkStatusRecord(status=status, terminal_at=terminal_at)
        self._evict_work_status_overflow(protect=key)
        return self._emit("work_status", status.model_dump(mode="json"))

    def _forget_work_status(self, key: WorkStatusKey) -> None:
        """Drop one ledger key's records, keeping its event_sequence counter.

        See the eviction note in ``__init__``: the counter must survive so a
        later record for the same key continues monotonically instead of
        restarting at 1. It is never pruned, by design. A terminal record
        additionally leaves a tombstone so a later child write cannot
        cold-start the key back below the terminal state clients applied.

        Only the *record-scoped* non-authoritative clock is dropped here. The
        key-scoped ``_work_status_nonauthoritative_keys`` tombstone survives,
        exactly like the counter and the terminal-key tombstone: dropping a
        record does not make a restored key's child set any more complete, so
        a record cold-started after this prune must still refuse to
        terminalize.
        """
        self._work_status_children.pop(key, None)
        self._work_status_nonauthoritative_at.pop(key, None)
        record = self._work_status_parents.pop(key, None)
        if record is not None and record.status.state in WORK_STATUS_TERMINAL:
            self._work_status_terminal_keys.add(key)

    def _work_status_expiry_stamp(self, key: WorkStatusKey) -> float | None:
        """When ``key``'s retention clock started, or None while it is live.

        Two records are subject to expiry: a terminal one (clock starts when
        it went terminal) and a non-authoritative restored one, which can
        never *become* terminal (``_reaggregate_parent``) and so would
        otherwise have no clock at all -- its clock starts at the restore.
        """
        record = self._work_status_parents.get(key)
        if record is None:
            return None
        if record.terminal_at is not None:
            return record.terminal_at
        return self._work_status_nonauthoritative_at.get(key)

    def _evict_work_status_overflow(self, *, protect: WorkStatusKey) -> None:
        """Bound the ledger, evicting the oldest expiry-eligible record first.

        Only terminal records and non-authoritative restored ones are
        evictable. A live authoritative non-terminal record is a parent
        aggregate whose children map is the sole memory of which children
        exist; dropping it makes the next ``set_child_work_status`` for that
        key a cold start, which turns a whole-child-set cancel sweep into a
        no-op and strands the parent non-terminal forever. A restored record
        carries no such memory -- its children map is empty by construction --
        so there is nothing to lose by evicting it, and it must be evictable
        or it would pin a ledger slot for the process lifetime. When every
        retained record is live the ledger deliberately exceeds the cap
        instead. ``protect`` is the key just written and is never the
        eviction victim.
        """
        while len(self._work_status_parents) > self._MAX_WORK_STATUS_KEYS:
            candidates = [
                (item, stamp)
                for item in self._work_status_parents
                if item != protect and (stamp := self._work_status_expiry_stamp(item)) is not None
            ]
            if not candidates:
                return
            oldest = min(candidates, key=lambda pair: pair[1])[0]
            self._forget_work_status(oldest)

    @staticmethod
    def _aggregate(children: list[WorkStatus]) -> tuple[WorkStatusState, TerminalReason | None]:
        if any(child.state == "routing" for child in children):
            return "routing", None
        if any(child.state == "searching" for child in children):
            return "searching", None
        if any(child.state not in WORK_STATUS_TERMINAL for child in children):
            return "background", None
        failed = [child for child in children if child.state == "failed"]
        if failed:
            reason = next(
                (child.terminal_reason for child in failed if child.terminal_reason), None
            )
            return "failed", reason
        if all(child.state == "cancelled" for child in children):
            return "cancelled", None
        return "result_ready", None

    def prune_expired_work_status(self) -> None:
        """Forget every expiry-eligible record past the five-minute TTL.

        Split out of ``work_status_snapshot`` so the projection stays a pure
        read: expiry is inclusive at the boundary, and an expired record is
        forgotten entirely (leaving only its tombstone and event_sequence) so
        the ledger does not grow for the process lifetime.

        Eligibility is ``_work_status_expiry_stamp``: a terminal record's
        ``terminal_at``, or a non-authoritative restored record's restore
        instant. The latter matters because such a record can never become
        terminal, so keying purely off ``terminal_at`` would retain it -- and
        re-ship it on every snapshot -- forever.
        """
        now = time.monotonic()
        expired = [
            key
            for key in self._work_status_parents
            if (stamp := self._work_status_expiry_stamp(key)) is not None
            and (now - stamp) >= WORK_STATUS_TTL_SECONDS
        ]
        for key in expired:
            # Collected first: deleting during iteration would mutate the
            # dict being walked.
            self._forget_work_status(key)

    def work_status_snapshot(self) -> tuple[WorkStatus, ...]:
        """Return live parent work-status records, pruning expired terminals first."""
        self.prune_expired_work_status()
        return tuple(record.status for record in self._work_status_parents.values())

    @classmethod
    def from_snapshot(cls, snapshot: RuntimeSnapshot) -> SessionState:
        state = cls(snapshot.session_id)
        state.sequence = snapshot.snapshot_sequence
        for worker in snapshot.workers:
            state.workers[worker.worker_id] = worker
        for result in snapshot.results:
            state.results.append(result)
        for progress in snapshot.speech_progress:
            state.speech[progress.utterance_id] = progress
            state._speech_history[progress.utterance_id] = [progress]
        state.routing = snapshot.routing
        state.transcript.extend(snapshot.transcript)
        # Rehydrate the work-status ledger. The wire carries only the parent
        # aggregate per WorkStatusKey (children are server-internal), so the
        # children map starts empty and the restored record is marked
        # ``children_authoritative=False``: a later child record re-aggregates
        # against a set that is known to be incomplete, so it may refine the
        # parent's non-terminal state but may never terminalize it (see
        # ``_reaggregate_parent``). The per-key event_sequence MUST be
        # restored too: restarting it at 1 would make every post-restore
        # record look stale to the browser reducer and be dropped.
        restored_at = time.monotonic()
        for status in snapshot.work_status:
            key = WorkStatusKey(status.origin_epoch, status.turn_id, status.work_item_id or "")
            if key in state._work_status_parents:
                # Every current producer stamps a real work_item_id onto
                # every WorkStatus it emits (see _reaggregate_parent), so
                # this should be unreachable -- but the wire schema still
                # declares work_item_id Optional, and two distinct records
                # that both restore to `work_item_id or ""` would otherwise
                # silently overwrite each other's record and event sequence
                # with no warning (round 1 gauntlet logic finding). Surface
                # it instead of losing data quietly.
                logger.warning(
                    "from_snapshot: overwriting existing work-status record for "
                    f"{key} -- multiple restored records collapsed onto the same "
                    f"WorkStatusKey (work_item_id={status.work_item_id!r})"
                )
            state._work_status_children.setdefault(key, {})
            state._work_status_sequence[key] = status.event_sequence
            state._work_status_parents[key] = _WorkStatusRecord(
                status=status,
                # The original terminal instant is not on the wire (monotonic
                # clocks are not comparable across processes anyway), so the
                # five-minute TTL restarts from the restore point rather than
                # being silently treated as already expired.
                terminal_at=restored_at if status.state in WORK_STATUS_TERMINAL else None,
            )
            # The key is non-authoritative for the process lifetime; the stamp
            # is this record's retention clock. A non-terminal restored record
            # never gets a ``terminal_at``, so the stamp is its only clock --
            # same TTL, measured from the same instant.
            state._work_status_nonauthoritative_keys.add(key)
            state._work_status_nonauthoritative_at[key] = restored_at
        return state

    def snapshot(
        self, origin_epoch: int | None = None, *, include_work_status: bool = False
    ) -> RuntimeSnapshot:
        # ``include_work_status`` gates snapshot *content* only (whether the
        # ledger is computed); wire *presence* of the field is decided later
        # by ``RuntimeSnapshot.wire_payload(include_work_status=...)``.
        return RuntimeSnapshot(
            contract_version="v1.0",
            session_id=self.session_id,
            snapshot_sequence=self.sequence,
            workers=list(self.workers.values()),
            results=list(self.results.results),
            speech_progress=list(self.speech.values()),
            routing=self.routing,
            transcript=list(self.transcript),
            origin_epoch=origin_epoch,
            work_status=list(self.work_status_snapshot()) if include_work_status else [],
        )
