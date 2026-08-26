"""Shared turn-epilogue abstraction for SessionHost's turn handlers.

Extracted per the Phase 1 characterization in
``docs/dev_plans/20260824-refactor-sessionhost-decomposition.md`` (Findings:
"Phase 1 characterization"): the three turn handlers
(``_handle_transcript_impl``, ``_handle_pending``, ``_handle_multi_intent``)
share a duplicated-but-diverging epilogue -- settle the turn's ack, derive
and emit a terminal work status around the canonical commit, finalize the
child/turn recorders, and (for single-intent only) project the worker back
to ``idle``. This module holds that logic once, parameterized by an explicit
``TurnEpilogueContext`` instead of reaching into ``SessionHost`` via
``self``.

Dependency direction: this module is imported by ``server/pipeline.py``, and
must never import back from it (see the plan's Architecture & Call Flow
section). Collaborator types are pulled from their owning modules
(``contracts``, ``perf_metrics``, ``speech_scheduler``, ``work_status_publisher``)
instead.

Sub-step C1 covers the single-child epilogue shape
(``finalize_single_child_turn``), used by ``_handle_transcript_impl`` and (as
of C2) ``_handle_pending``. Sub-step C3 adds the fan-out shape
(``finalize_fan_out_turn`` and ``release_fan_out_turn_work_items``), used by
``_handle_multi_intent``: unlike the single-child shape, the fan-out
handler's per-index result/pending/failure/reconcile fan-in loops that BUILD
the per-item state remain the handler's own distinct middle -- only the
tail that CONSUMES that already-built state (ack settle, the per-item commit
loop, the deferred-status sweep, and the partial-release decision) lives
here.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import AbstractSet, Any

from loguru import logger

from .contracts import GroundedResult, TerminalReason, WorkStatusState
from .perf_metrics import AppTurnRecorder, WorkItemOutcome, WorkItemRecorder
from .speech_scheduler import ROLE_RESULT, SpeechRole
from .work_status_publisher import work_status_after_commit_failure, work_status_for_outcome


@dataclass(frozen=True)
class TurnEpilogueContext:
    """Explicit collaborators the epilogue needs -- no ``SessionHost`` self-reads.

    ``worker_projection`` is optional: only the single-intent call site
    passes one (via ``project_idle=True`` on `finalize_single_child_turn`),
    matching row 9 of the Phase 1 differences table, which is present only
    for ``_handle_transcript_impl``.
    """

    settle_turn_ack: Callable[..., None]
    cancelled_ids: AbstractSet[str]
    emit_work_status: Callable[..., None]
    release_turn_work_item: Callable[[str, str], None]
    release_all_turn_work_items: Callable[[str], None]
    finalize_turn_exception: Callable[..., None]
    worker_projection: Any | None = None


@dataclass(frozen=True)
class SingleChildEpilogueOutcome:
    """What `finalize_single_child_turn` hands back to its caller.

    ``retained_still_open`` is the caller's row-11 "should I release this
    turn's work items in my `finally`?" signal -- the caller keeps that
    release call itself (a thin call-through), since it is also reached by
    early-exit paths (row 8's retained-and-capable short-circuit among them)
    this function never sees.
    """

    result: GroundedResult
    retained_still_open: bool


async def finalize_single_child_turn(
    ctx: TurnEpilogueContext,
    *,
    origin: Any,
    turn_id: str,
    work_item_id: str,
    origin_epoch: int,
    worker: Any,
    worker_id: str,
    child: WorkItemRecorder,
    turn_recorder: AppTurnRecorder,
    result: GroundedResult,
    child_outcome_label: WorkItemOutcome,
    speech_role: SpeechRole = ROLE_RESULT,
    search_ms: float = 0.0,
    commit_and_speak: Callable[..., Awaitable[GroundedResult]],
    project_idle: bool = False,
    cancel_admitted: bool = False,
) -> SingleChildEpilogueOutcome:
    """Rows 1-7, 9, 11 of the Phase 1 differences table, single-child shape.

    Covers the tail shared (with divergent details already folded in via
    parameters) by a turn that delegates exactly one child -- single-intent
    today, pending-dialogue as of sub-step C2. Row 8 (the retained
    work-item capability-gated short-circuit) is decided by the caller
    before a ``result`` even exists and stays in the handler's distinct
    middle -- this function is only reached once a ``result`` to commit has
    been produced.

    ``cancel_admitted`` is row 2's variance: the single-intent caller settles
    its ack unconditionally (the default, ``False``), while pending-dialogue
    queues its ack at the delegation *decision*, before it knows whether
    ``coordinator.submit`` accepted or retained anything, and so must pass
    whether nothing was accepted/retained here instead.
    """
    was_cancelled = work_item_id in ctx.cancelled_ids
    commit_started = time.perf_counter()
    ctx.settle_turn_ack(origin.scheduler, turn_id, cancel_admitted=cancel_admitted)
    # A retained child is not terminal: its truthful `background` status was
    # already emitted by the caller and the coordinator terminalizes it when
    # the late result lands. Only an actual cancellation settles it here.
    retained_still_open = child_outcome_label == "retained" and not was_cancelled
    derived = (
        None
        if retained_still_open
        else work_status_for_outcome(
            child_outcome_label,
            cancelled=was_cancelled,
            terminal_kind=child_outcome_label,
        )
    )
    # Terminal status is emitted only *after* the canonical commit succeeds.
    # Emitting `result_ready` first would tell a capable client the result
    # was committed and display-ready even when the commit then raised or
    # the turn was cancelled before it ran; a commit failure settles the
    # child to `failed` instead.
    try:
        committed = await commit_and_speak(result, origin, role=speech_role)
    except Exception:
        failure_status = work_status_after_commit_failure(derived)
        if failure_status is not None:
            ctx.emit_work_status(
                turn_id=turn_id,
                work_item_id=work_item_id,
                worker_id=worker_id,
                state=failure_status[0],
                origin_epoch=origin_epoch,
                terminal_reason=failure_status[1],
            )
        raise
    if derived is not None:
        status_state, status_reason = derived
        ctx.emit_work_status(
            turn_id=turn_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            state=status_state,
            origin_epoch=origin_epoch,
            terminal_reason=status_reason,
        )
    commit_ms = (time.perf_counter() - commit_started) * 1000
    child.finalize(
        outcome=child_outcome_label,
        app_worker_id=worker_id,
        result_id=result.result_id,
        search_ms=search_ms,
        commit_ms=commit_ms,
    )
    turn_recorder.record_commit(commit_ms)
    turn_recorder.finalize()
    if project_idle and ctx.worker_projection is not None:
        ctx.worker_projection.project(
            worker,
            origin_epoch=origin_epoch,
            status="idle",
            latest_result_id=None if was_cancelled else result.result_id,
        )
    return SingleChildEpilogueOutcome(result=committed, retained_still_open=retained_still_open)


async def finalize_fan_out_turn(
    ctx: TurnEpilogueContext,
    *,
    origin: Any,
    turn_id: str,
    parent_work_item_id: str,
    turn_recorder: AppTurnRecorder,
    results: dict[int, GroundedResult],
    attributed_indexes: AbstractSet[int],
    retained_work_items: AbstractSet[str],
    deferred_status: dict[int, tuple[str, str | None, WorkStatusState, TerminalReason | None]],
    speech_roles: dict[int, SpeechRole],
    commit_and_speak: Callable[..., Awaitable[GroundedResult]],
) -> tuple[GroundedResult, ...]:
    """Rows 2, 4, 5, 7 of the Phase 1 differences table, fan-out shape.

    Covers the tail of ``_handle_multi_intent`` that *consumes* per-index
    state already built by the handler's own fan-in loops (results/pending/
    failures/reconcile) -- ``results``, ``attributed_indexes``,
    ``retained_work_items``, ``deferred_status``, and ``speech_roles`` are
    all inputs here, never derived inside this function. Row 1 (per-child
    ``was_cancelled``), row 3 (per-index status derivation), row 6 (per-item
    child-recorder finalize at up to 4 fan-in sites), and row 8 (the
    per-item retained/capability-gated legacy result) all stay in that
    middle -- they run before any of the state this function consumes
    exists in final form.

    Row 9 (post-commit worker-``idle`` projection) is deliberately absent
    here, unlike the single-child shape: no fan-out call site ever passed
    one, matching the Phase 1 characterization's "absent" cell for this row.

    Row 11's *decision* (release-all vs partial-release) is a separate
    function, ``release_fan_out_turn_work_items`` below -- it must run from
    the caller's own ``finally`` so it still fires when this function's
    commit loop re-raises, or when an earlier exception in the handler's
    middle skips this function entirely.
    """
    if not attributed_indexes and not retained_work_items:
        # No *delegated* child was accepted or retained: the ack promised a
        # search result that is never coming, so retract it outright (see
        # the caller's own comment on this branch for the full rationale).
        ctx.settle_turn_ack(origin.scheduler, turn_id, cancel_admitted=True)
    else:
        ctx.settle_turn_ack(origin.scheduler, turn_id)
    committed: list[GroundedResult] = []
    commit_exceptions: list[Exception] = []
    for index in sorted(results):
        # Each item is isolated so one item's speak-time failure cannot drop
        # already-computed sibling results; only the first exception is
        # re-raised, and only after every item has been committed.
        suppressed: set[str] = set()
        try:
            committed.append(
                await commit_and_speak(
                    results[index],
                    origin,
                    role=speech_roles.get(index, ROLE_RESULT),
                    suppressed_out=suppressed,
                )
            )
        except Exception as exc:  # noqa: BLE001  # isolate one item's speak failure from its siblings; re-raised below once all items are committed
            logger.exception(
                f"multi-intent commit for {turn_id}: item {index} failed after its "
                f"state was already committed; continuing with remaining items"
            )
            committed.append(results[index])
            commit_exceptions.append(exc)
            pending_status = deferred_status.pop(index, None)
            if pending_status is not None:
                failed_item_id, failed_worker_id, _state, _reason = pending_status
                ctx.emit_work_status(
                    turn_id=turn_id,
                    work_item_id=failed_item_id,
                    parent_work_item_id=parent_work_item_id,
                    worker_id=failed_worker_id,
                    state="failed",
                    origin_epoch=origin.epoch,
                )
        else:
            pending_status = deferred_status.pop(index, None)
            if pending_status is not None:
                item_id, item_worker_id, status_state, status_reason = pending_status
                if item_id in suppressed:
                    status_state, status_reason = "cancelled", None
                ctx.emit_work_status(
                    turn_id=turn_id,
                    work_item_id=item_id,
                    parent_work_item_id=parent_work_item_id,
                    worker_id=item_worker_id,
                    state=status_state,
                    origin_epoch=origin.epoch,
                    terminal_reason=status_reason,
                )
    # A deferred status whose index never reached the commit loop (its result
    # was dropped from `results` between derivation and commit) still has to
    # terminalize rather than strand the parent.
    for item_id, item_worker_id, status_state, status_reason in deferred_status.values():
        ctx.emit_work_status(
            turn_id=turn_id,
            work_item_id=item_id,
            parent_work_item_id=parent_work_item_id,
            worker_id=item_worker_id,
            state=status_state,
            origin_epoch=origin.epoch,
            terminal_reason=status_reason,
        )
    deferred_status.clear()
    if commit_exceptions:
        raise commit_exceptions[0]
    turn_recorder.finalize()
    return tuple(committed)


def release_fan_out_turn_work_items(
    ctx: TurnEpilogueContext,
    *,
    turn_id: str,
    delegated_children: Mapping[str, str | None],
    retained_work_items: AbstractSet[str],
) -> None:
    """Row 11 of the Phase 1 differences table, fan-out shape.

    Unlike the single-child shape's release decision (a bool the handler
    keeps as a thin call-through in its own ``finally``), the fan-out
    handler's retained state is per-child, not per-turn: some children may
    be retained while their siblings are not, so releasing "all turn work
    items" outright would release a still-live retained sibling's work item
    out from under it. This runs from the caller's own ``finally`` (not from
    inside ``finalize_fan_out_turn``) so it still fires when the commit loop
    re-raises, or when an earlier exception skips ``finalize_fan_out_turn``
    entirely.
    """
    if retained_work_items:
        for item_work_item_id in delegated_children:
            if item_work_item_id not in retained_work_items:
                ctx.release_turn_work_item(turn_id, item_work_item_id)
    else:
        ctx.release_all_turn_work_items(turn_id)
