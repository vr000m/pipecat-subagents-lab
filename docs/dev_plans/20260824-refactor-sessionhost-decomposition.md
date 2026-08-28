# Task: SessionHost Decomposition (P2)

**Status**: Complete
**Component**: server
**Assigned to**: Claude
**Priority**: High
**Branch**: refactor/sessionhost-decomposition
**Created**: 2026-08-24

**Review Gates:** full

Program: `20260824-design-v013-followup-program.md` (items 1-12).

## Objective

Execute the structural decomposition that ~20 rounds of v0.1.3 review gauntlets
repeatedly flagged and could not perform in-round: shrink `SessionHost`
(`server/pipeline.py:594`, ~2856 lines) from a facade-plus-forwarders god-class
into real modules, deduplicate the three turn handlers' epilogue logic, unify
the fire-and-forget task idiom, and resolve the deferred ack-latch question —
**verify-first**: current `TurnAckLedger` code already carries an
admission-generation counter (`server/turn_ack_ledger.py:74-139`, checked by
`_retry_or_abandon`) that may already close the round-10 race; Phase 4 proves
it one way or the other before writing any fix.

## Context

Every in-gauntlet structural attempt died on test coupling: duck-typed
`Coordinator` test doubles (77 `class *Coordinator` classes across `tests/` by
current grep — the round-5 record's "~60" was scoped to `tests/test_pipeline.py`
alone; a round-5 strictness change broke 67 tests and was reverted) and ~56
test call sites reading extracted collaborators directly (AD-round figure;
re-inventoried in Phase 0 — the branch absorbed ~156 later fixes, so treat all
counts here as approximate until the Phase 0 sweep records current ones).
That is why Phase 0 is test infrastructure, not production code — the reviews
proved the extraction is impossible without it.

Sequencing within this plan is strictly linear. Cross-plan (program matrix,
corrected 2026-08-24): **parallel-safe with P3 only** (modulo the
`server/config.py` region caveat); **serialized with P1 on both paths** — P1 ∥
P2 is "No on either path". The P1 gate sits at the top of Phase 2 (not Phase 3):
on P1-retire, P1's retire commit deletes the `server/pipeline.py:2837` gate
region that Phase 2's `connect()` decomposition rewrites, so it must land first.

Prior-decision pins to respect (do NOT re-litigate silently):
- `_work_status_sequence`/`_work_status_terminal_keys` stay unbounded — bounding breaks the `event_sequence` restart guarantee; a pinning test exists (`tests/test_session_state.py:558`).
- Decomposition-by-facade was a deliberate v0.1.3 strategy to keep diffs mechanically verifiable; this plan is the increment that gets to collapse the facades.
- The coordinator boundary is declared four times **on purpose** (`server/work_item_coordinator.py:248-263`: `OptionalCoordinator`, `CoordinatorDefaults`, `CoordinatorView`, `coordinator_view` — "each says something the others cannot, so they are not collapsible", roster frozenset pinned by a test). All four are live in production paths (`server/pipeline.py:1317`, `work_item_coordinator.py:401`, `pipeline.py:1324`) — none is production-dead. Requirement 1 re-litigates this pin **in the open** (see below), never silently.

## Requirements

1. `SessionHost` loses its pass-through forwarders (~19 per AD-r10; enumerated in Phase 6 before removal). The four coordinator-boundary declarations get an explicit, recorded re-litigation — NOT a silent collapse: either (a) the Phase 0 contract-checked doubles are shown to remove the getattr-fallback rationale and the declarations collapse with the roster frozenset + its pinning test updated in the same commit, with call-site evidence for any declaration retired, or (b) the four-declaration pin is re-affirmed with the rationale recorded in Findings and the boundary-consolidation ambition dropped from this plan.
2. Shared turn-epilogue abstraction replaces the duplicated logic in the three ~250-615-line turn handlers, with each row of the Phase 1 differences table backed by a cited existing test or a new characterization test before extraction.
3. `connect()` (210 lines, 9 nested closures — recounted; the AD report said 8) decomposed; `ConnectionPipeline` (`server/pipeline.py:356`) moves to its own module with **no re-export** from `pipeline.py` (importers such as `server/turn_ack_ledger.py:55` update in the move commit).
4. Promotion-manifest logic (~400 lines: `server/config.py` ~340-470 + 667-770) **leaves `config.py` on either decided P1 branch**: deleted wholesale by P1's retire commit (default expectation), or extracted to `server/promotion_manifest.py` here on P1-invest. Evidence in both branches: grep-zero manifest logic in `config.py`. **On P1-blocked/escalated** this requirement stays open — recorded as such in Findings and in the acceptance carve-out below; program row 7 remains open and this plan may still complete around it.
5. `_OWNED_CONFIG_FIELDS` permissive-vs-strict default (`server/work_item_coordinator.py:131`) gets a *decision* (5th-round deferral ends here): either strict — a sanctioned behavior change under Requirement 9, with the modernized doubles absorbing it and the tests that now enforce strictness cited — or permissive pinned with a rationale comment + pinning test.
6. One shared fire-and-forget helper (supporting both `Task` and `Future` retention) replaces the hand-rolled add/discard idiom at **every site an inventory sweep finds** (`rg -n 'add_done_callback' server/` — current grep, 2026-08-25: **22 hits across 10 files**, including `server/app.py`, `server/runner_supervisor.py`, and `server/work_task_ledger.py` beyond the originally listed seven; hits ≠ convertible sites — the Phase 5 inventory classifies every hit at execution time); any site left unconverted is listed in Findings with a reason (e.g. `SessionHost.shutdown`'s cancel-then-retrieve-exception idiom at `pipeline.py:3421` is a different pattern).
7. Ack-latch race **verified before fixed**: Phase 4 first attempts to reproduce the round-10 quarantined interleaving against current code. If it cannot be made to fail — the existing `_ack_admission_generation` mechanism already covers it — item 10 closes as already-fixed with the reproduction attempt as evidence. Only if it fails does the fix land, by **extending** the existing generation mechanism (`_retry_or_abandon`'s generation check), never by adding a parallel identity/state machine.
8. Client-side eviction policy (item 12): **verify and document** the existing arrangement — numeric bounds already shared via `shared/work-status-retention.json` (loaded by both `server/session_state.py` and `web/src/state.js:179-187`) and pinned by dual parity tests (`web/test/state.test.js:554,585`, `tests/test_session_state.py`). If verification confirms coverage, close item 12 with the evidence recorded; only a concrete named gap justifies new work. The "share via schema field" option is out of scope here — it changes the client contract, which Requirement 9 forbids.
9. Zero behavior change outside item 7 (the race fix, if the reproduction fails) and item 5 (only if strict is chosen — explicitly sanctioned above); full suite + `ruff format`/`ruff check` + `mypy` green at **every phase boundary**, not just at the end.

## Architecture & Call Flow

Target module topology after decomposition (arrows = imports/calls):

```
server/app.py
   └─> server/composition.py  (WorkerRegistry → Router → WorkItemCoordinator → SessionHost)
          └─> server/pipeline.py            SessionHost (slimmed: session orchestration only)
                 ├─> server/connection_pipeline.py   ConnectionPipeline (Phase 2; no re-export back)
                 ├─> server/turn_epilogue.py          shared epilogue (Phase 1, if module scope chosen)
                 │     receives an explicit context object (ack ledger, speech scheduler,
                 │     recorder handles) — never reads SessionHost internals via self
                 ├─> server/task_retention.py         fire-and-forget helper (Phase 5)
                 │     also imported by: speech_scheduler, turn_ack_ledger, observers,
                 │     speech_lifecycle, work_item_coordinator, turns
                 └─> server/work_item_coordinator.py  coordinator boundary
                       (four declarations today; Requirement 1 decides collapse vs re-affirm)
server/config.py — after P1: no promotion-manifest logic (deleted on retire /
                   extracted to server/promotion_manifest.py on invest)
web/src/state.js — untouched; eviction parity stays pinned via
                   shared/work-status-retention.json + dual parity tests (Requirement 8)
```

Dependency direction: new modules import from collaborators, never back into
`pipeline.py`; `task_retention.py` is a leaf utility with no server imports.

## Implementation Checklist

### Phase 0: Test-double modernization

**Impl files:** tests/_doubles.py, server/speech_scheduler.py, server/turn_ack_ledger.py, server/observers.py, server/speech_lifecycle.py, server/work_item_coordinator.py (narrow public read accessors only)
**Test files:** all of tests/ found by the inventory sweep (at minimum tests/test_pipeline.py, tests/test_work_item_coordinator.py, tests/test_session_host.py, tests/test_smoke_conversation.py, tests/test_speech_scheduler.py, tests/test_work_status.py)
**Test command:** `uv run pytest -q && uv run ruff check . && uv run mypy .`
**Goal:** A single shared, contract-checked Coordinator double (and public read API for extracted collaborators) replaces every duck-typed double and collaborator-read call site found by the inventory (77 doubles across tests/ by current grep; ~56 call sites per AD rounds), so later phases can change internals without mass test breakage.

- [ ] Inventory across **all of tests/**: duck-typed Coordinator doubles and collaborator-read call sites (grep sweep; record current counts in Findings — the file list above follows the counts, not vice versa).
- [ ] Introduce `tests/_doubles.py` with a canonical Coordinator fake validated against the real interface (Protocol or ABC — decide and document).
- [ ] Migrate test files to the shared double mechanically, one test module per commit. **Assertion-parity exit check per commit**: test count identical before/after the module's migration (record both), and migration commits change arrange/double wiring only — never assert lines — spot-checked via `git diff -- tests/ | grep '^[-+].*assert'` and recorded in Findings.
- [ ] Add narrow public read accessors on extracted collaborators where tests currently reach into privates; migrate the inventoried call sites.

### Phase 1: Shared turn-epilogue abstraction

**Impl files:** server/pipeline.py (+ server/turn_epilogue.py only if module scope is chosen below)
**Test files:** tests/test_session_host.py, tests/test_pipeline.py
**Test command:** `uv run pytest -q && uv run ruff check . && uv run mypy .`
**Goal:** The three turn handlers' duplicated epilogue logic lives in exactly one place; handler bodies shrink to their genuinely distinct middles.

- [ ] Characterize the three handlers' epilogues; write a differences table in Findings before extracting. **For each table row, cite the existing test exercising that behavior, or add a characterization test first** — the prose table alone cannot fail.
- [ ] Decide the extraction scope from the differences table (new module vs SessionHost-internal class — smallest correct scope); record the decision, then create `server/turn_epilogue.py` only if module scope wins. Either way the epilogue receives an explicit context object (per Architecture & Call Flow), not `self`-reads.
- [ ] Convert each handler in its own commit; suite green after each.

### Phase 2: connect() + ConnectionPipeline extraction

**Impl files:** server/pipeline.py, server/connection_pipeline.py
**Test files:** tests/test_pipeline.py, tests/integration/test_browser_session.py
**Test command:** `uv run pytest -q && uv run ruff check . && uv run mypy .`
**Validation cmd:** `uv run python scripts/smoke_server.py`
**Goal:** `ConnectionPipeline` is its own module; `connect()` is a short orchestration of named steps with no nested closures holding hidden state.

- [ ] **P1 gate (blocks this phase and Phase 3's manifest bullet):** do not start before P1's Phase 1 decision is recorded. On P1-retire (default expectation): wait for P1's retire commit to land — it deletes the `server/pipeline.py:2837` gate region this phase rewrites, and P1 owns that deletion (fail-closed test first; never absorb it here). On P1-invest or P1-blocked/escalated: proceed with the promotion machinery left fully intact.
- [ ] Move `ConnectionPipeline` to `server/connection_pipeline.py` (mechanical, own commit). **No re-export from pipeline.py** — update all importers (`server/turn_ack_ledger.py:55`, tests) in the move commit, mirroring Phase 3's config.py rule.
- [ ] Decompose `connect()`'s 9 nested closures (recounted; enumerate before starting) into methods/functions with explicit parameters; document any closure that captured mutable state.

### Phase 3: Config/boundary consolidation

**Impl files:** server/config.py, server/promotion_manifest.py (invest branch only), server/work_item_coordinator.py, server/contracts.py
**Test files:** tests/test_config.py, tests/test_work_item_coordinator.py, tests/test_work_status.py
**Test command:** `uv run pytest -q && uv run ruff check . && uv run mypy .`
**Validation cmd:** `uv run python scripts/smoke_server.py`
**Goal:** The coordinator-boundary question decided in the open, and the OWNED_CONFIG_FIELDS + client-eviction items decided rather than deferred a sixth time.

> **Dropped (superseded):** Promotion-manifest logic in `config.py`, by P1 outcome — P1 retired the machinery outright (commit `c67da7f`); `load_promotion_manifest`/`PromotionManifest` and all consumers are deleted, so there is nothing left in `config.py` to extract. This bullet drops. Rebase note: P3 Phase 1 edited `server/config.py:1689-1716` (landed 60e0f36) — land order per program matrix.
- [ ] Re-litigate the four-declaration boundary pin in the open (Requirement 1): enumerate current call sites for all four declarations; argue collapse from the Phase 0 doubles (roster frozenset + pinning test updated in the same commit) or re-affirm the pin with rationale in Findings. No silent deletion — the "two production-dead" premise was checked and is false.
- [ ] Decide `_OWNED_CONFIG_FIELDS` strictness (Requirement 5): **if permissive** — pin with rationale comment + pinning test; **if strict** — cite the tests that now enforce strictness via the modernized doubles, and record it as the Requirement 9-sanctioned behavior change.
- [ ] Verify the client-eviction arrangement (Requirement 8): confirm `shared/work-status-retention.json` + the dual parity tests cover program item 12; record the evidence and close, or name the concrete gap found.

### Phase 4: Ack-latch race — verify, then fix

**Impl files:** server/turn_ack_ledger.py (only if the reproduction fails)
**Test files:** tests/test_work_task_ledger.py, tests/test_session_host.py
**Test command:** `uv run pytest -q tests/test_work_task_ledger.py tests/test_session_host.py && uv run pytest -q && uv run ruff check . && uv run mypy .`
**Goal:** The round-10 race is either proven already-fixed by the existing `_ack_admission_generation` mechanism (with the reproduction attempt as evidence) or fixed by extending that mechanism, pinned by a regression test that failed first. Runs **before** the fire-and-forget conversion touches this file, so the reproduction targets the shipped code path.

- [ ] Attempt to reproduce the round-10 quarantined interleaving (documented at `20260728-feature-early-ack-background-delivery-v0.1.3.md:731`) as a test against **current, unmodified** `turn_ack_ledger.py`. First characterize why the existing generation check (`turn_ack_ledger.py:74-139`, `:466-467`) would or would not catch it.
- [ ] If the test cannot be made to fail: close Requirement 7 / program item 10 as **already-fixed**, keep the reproduction test as a pin, record the evidence in Findings. Phase ends here.
- [ ] If it fails: extend `_ack_admission_generation` / `_retry_or_abandon` (no parallel identity mechanism); make the test pass; re-run the exactly-once ack pinning tests and confirm no ordering guarantee regressed.

### Phase 5: Fire-and-forget helper

**Impl files:** server/task_retention.py, server/speech_scheduler.py, server/turn_ack_ledger.py, server/observers.py, server/pipeline.py, server/speech_lifecycle.py, server/work_item_coordinator.py, server/turns.py, server/app.py, server/runner_supervisor.py, server/work_task_ledger.py
**Test files:** tests/test_task_retention.py, tests/test_speech_scheduler.py, tests/test_work_task_ledger.py, tests/test_observers.py, tests/test_speech_lifecycle.py, tests/test_work_item_coordinator.py, tests/test_app.py
**Test command:** `uv run pytest -q && uv run ruff check . && uv run mypy .`
**Goal:** The add/discard task-retention idiom exists once, used by every inventoried site, so the next fire-and-forget task cannot be written subtly differently.

- [ ] Inventory: `rg -n 'add_done_callback' server/` — record every hit in Findings (current grep, 2026-08-25: 22 hits across 10 files; re-run at execution time — the counts drift); classify **every hit** as convert / exclude-with-reason (the `SessionHost.shutdown` cancel-then-retrieve idiom at `pipeline.py:3421` is expected to be excluded), no hit left unclassified.
- [ ] Implement the shared helper (module-level utility; keep it tiny; accepts both `Task` and `Future`) **with its own tests first**: `tests/test_task_retention.py` covering retention-until-done, discard-after-done, and exception handling.
- [ ] Convert the inventoried sites, one commit each.

### Phase 6: Facade collapse

**Impl files:** server/pipeline.py, docs/architecture.md, AGENTS.md, docs/dev_plans/20260824-design-v013-followup-program.md, docs/dev_plans/20260824-refactor-sessionhost-decomposition.md, docs/dev_plans/README.md
**Test files:** tests/test_session_host.py, tests/test_pipeline.py
**Test command:** `uv run pytest -q && uv run ruff check . && uv run mypy .`
**Validation cmd:** `uv run python scripts/smoke_server.py`
**Goal:** The pass-through forwarders are gone; callers reach collaborators through the boundary Requirement 1 decided on; SessionHost's line count reflects a real decomposition; the program map records the closures.

- [ ] Enumerate the forwarders first (list them in Findings — "~19" is the AD-r10 figure, treat as approximate) before removing any.
- [ ] Remove forwarders; update call sites (Phase 0's accessors make this mechanical).
- [ ] Record before/after line counts for `server/pipeline.py` and `SessionHost` in Findings.
- [ ] Update `docs/architecture.md` and AGENTS.md layout to the new module set.
- [ ] Program closure commit: flip this plan's status header, update the program's Subordinate Plans row and provenance rows 1-12 (per the acceptance carve-outs), and create or update `docs/dev_plans/README.md` — all in the same commit, per the program's same-commit rule.

## Acceptance Criteria

- [ ] All nine Requirements resolved — met, or closed by their built-in decision branch (R1 collapse-or-re-affirm, R4 either decided P1 branch, R7 fixed-or-already-fixed, R8 verified-or-gap-named) — each with concrete evidence (line counts, grep-zero for forwarders and for manifest logic in config.py, the reproduction attempt or failing-then-passing race test). **Sole carve-out**: on P1-blocked, R4 stays open with the blocked state recorded in Findings; every other requirement still resolves.
- [ ] Phase 0/5 migration evidence recorded, not assumed: the Phase 0 inventory's before/after counts; grep-zero duck-typed `class *Coordinator` doubles outside `tests/_doubles.py`; grep-zero private collaborator reads outside the new accessors (allowlist any exception with reason); and an allowlisted zero-result check that no hand-rolled `add_done_callback` retention idiom remains outside `server/task_retention.py` plus the Phase 5 excluded sites.
- [ ] Full suite + `ruff format`/`ruff check` + mypy green at every phase boundary; smoke script passes at Phases 2, 3, and 6.
- [ ] No behavior change outside Phase 4 (race fix, if taken) and the R5-strict branch (if chosen) — pinned by the assertion-parity-checked public-contract tests.
- [ ] Program provenance map rows 1-12 closed here, with two exceptions: row 7 closes via P1's retire commit on retire, via this plan's Phase 3 extraction on invest, and **stays open on P1-blocked**; any row a carve-out leaves open is noted in the program doc in the same commit.

<!-- reviewed: 2026-08-27 @ e7fd020899e2b46678381cb987f3b54d65fad46c -->

## Progress

- [x] Phase 0: Test-double modernization
- [x] Phase 1: Shared turn-epilogue abstraction
- [x] Phase 2: connect() + ConnectionPipeline extraction
- [x] Phase 3: Config/boundary consolidation
- [x] Phase 4: Ack-latch race — verify, then fix
- [x] Phase 5: Fire-and-forget helper
- [x] Phase 6: Facade collapse

## Findings

### Phase 1 characterization (2026-08-26): turn-epilogue differences table

Sub-step A (analysis only, no extraction). Scope: `server/pipeline.py` on `SessionHost`.

## 1. The three turn handlers

| Handler | Lines | Size (lines) | Entry point |
|---|---|---|---|
| `_handle_transcript_impl` (single-intent) | 1350–1819 | 470 | routed here from `_handle_transcript` for every final STT turn |
| `_handle_pending` (continue-pending-dialogue) | 1820–2068 | 249 | called from `_handle_transcript_impl` when `outcome.kind == "continue_pending"` |
| `_handle_multi_intent` (compound/fan-out) | 2069–2681 | 613 | called from `_handle_transcript_impl` when `outcome.kind == "multi_intent"` |

These match the plan's "~250–615-line turn handlers" (249 / 470 / 613).

Each has the same `try / except asyncio.CancelledError / except Exception / finally` skeleton wrapping a distinct middle (routing/dispatch/search) that ends in a common tail — the epilogue this phase targets.

## 2. Epilogue differences table

Rows = epilogue steps, in the order they execute. "Same" = identical call shape and semantics across handlers (ignoring cosmetic naming). "Different" = same intent, divergent mechanics. "Absent" = the handler skips this step entirely.

| # | Epilogue behavior | `_handle_transcript_impl` | `_handle_pending` | `_handle_multi_intent` |
|---|---|---|---|---|
| 1 | Compute `was_cancelled` / cancellation set membership before settling | Same — `work_item_id in self._work_ledger.cancelled_ids` (:1732) | Same — identical line (:2008) | **Different** — no single `was_cancelled`; cancellation is checked per-child inline at each fan-in site (:2387, :2408, :2434, :2473, :2508) because there are N children, not one |
| 2 | Settle this turn's ack (`_settle_turn_ack`) | Same — unconditional call, no `cancel_admitted` (:1735) | **Different** — conditional `cancel_admitted=not submitted.results and not submitted.pending_work_item_ids` (:2003–2007), because the ack was queued at delegation-decision time before dispatch outcome is known | **Different** — branches on `attributed_indexes`/`retained_work_items`: `cancel_admitted=True` only if *no* child was accepted/retained (:2546–2567); pinned by `test_multi_intent_retracts_an_admitted_ack_when_no_delegated_child_was_accepted` |
| 3 | Derive terminal work-status before commit (`work_status_for_outcome` / `child_work_status_after_dispatch`) | Same shape, single child (:1741–1749) | Same shape, single child (:2012–2016) | **Different** — derived per-index into a `deferred_status` dict across three separate fan-in loops (results/pending/failures/reconcile), not one derivation (:2385–2483) |
| 4 | Commit-and-speak wrapped in try/except, with `work_status_after_commit_failure` fallback on exception | Same (:1755–1768) | Same (:2019–2032) | **Different** — same fallback function is used, but per-item inside a loop over `sorted(results)`, isolating one item's speak failure from siblings and re-raising only the first exception after the loop (:2570–2645); single/pending abort the whole handler on the first exception |
| 5 | Emit terminal work-status *after* successful commit | Same (:1769–1778) | Same (:2033–2042) | **Different** — emitted per item inside/after the same commit loop, plus a final sweep of any `deferred_status` entries that never reached the loop (:2630–2643) |
| 6 | Finalize child recorder (`child.finalize(...)`) | Same, one call (:1780–1786) | Same, one call (:1945-1947 / 1951 / 1995) | **Different** — one `child_recorders[index].finalize(...)` call per item, at up to 4 different call sites (normal, retained, failure, unattributed-reconcile) (:2378, :2428, :2467, :2498) |
| 7 | Finalize `turn_recorder` | Same — one `turn_recorder.finalize()` (or `.finalize(outcome=...)` on early-exit branches) (:1788, :1678, :1508/1510) | Same shape (:2043, :1974) | Same call shape — one `turn_recorder.finalize()` at the end of the happy path (:2646); early-exit-per-child does **not** finalize `turn_recorder` early (unlike single/pending's retained-early-return, which does) — **absent** the early finalize-and-return-None variant |
| 8 | Retained-work-item capability gate (`_work_status_publisher.replaces_legacy_result_for`) short-circuit: skip legacy canonical result + early `turn_recorder.finalize()` + `return None` | Same (:1665–1680) | Same (:1967–1975) | **Absent** — multi-intent's per-item retained branch always still emits a per-item legacy "taking longer" result unless the connection is capable (:2411–2427), but there is no early `return None` for the whole handler since other items may still need to commit |
| 9 | Post-commit worker projection to `idle` (`self._worker_projection.project(worker, ..., status="idle", latest_result_id=...)`) | **Present**, one call after the single commit (:1789–1794) | **Absent** — no equivalent call anywhere in `_handle_pending` | **Absent** — no equivalent call anywhere in `_handle_multi_intent` |
| 10 | `except CancelledError` / `except Exception` → shared `_finalize_turn_exception(...)` | Same (:1796–1815) | Same (:2045–2064) | Same call shape, only extra `parent_work_item_id=parent_work_item_id` kwarg and a filtered `children` dict excluding `retained_work_items` (:2648–2673) — **already a shared helper today** |
| 11 | `finally`: release turn work items (retain vs release-all) | Same pattern — `if not retained_still_open: self._release_all_turn_work_items(turn_id)` (:1816–1818) | Same pattern, identical shape (:2065–2067) | **Different** — partial release: `_release_turn_work_item` per non-retained child when `retained_work_items` is non-empty, else `_release_all_turn_work_items` (:2674–2680), because retained state is per-child, not per-turn |

Row 10 (`_finalize_turn_exception`) is **already** a shared SessionHost method (`server/pipeline.py:3337`) called identically (modulo one extra kwarg) by all three handlers — it is not duplicated code today, it is the one epilogue step already extracted. The remaining rows (1–9, 11) are the duplicated-but-diverging logic Requirement 2 targets.

## 3. Test citations per row

| # | Row | Citation | Why it pins this row |
|---|---|---|---|
| 1 | `was_cancelled` computation | `tests/test_pipeline.py::test_search_cancellation_cancels_child_without_retaining_it` (single); `tests/test_pipeline.py::test_pending_turn_cancelled_mid_submit_sweeps_the_child_as_cancelled` (pending); `tests/test_pipeline.py::test_multi_intent_whole_turn_cancel_between_children_reaches_the_already_acked_child` (multi) | Each asserts the terminal state a cancelled work item lands on, which depends on this handler correctly reading `_work_ledger.cancelled_ids` at the right granularity (turn-wide vs per-child) |
| 2 | Ack settle / `cancel_admitted` | single: `tests/test_pipeline.py::test_ack_admission_failure_after_the_turn_settled_does_not_requeue_it` (exercises the unconditional settle path). pending: `tests/test_pipeline.py::test_pending_turn_retracts_its_ack_when_submit_accepts_nothing`. multi: `tests/test_pipeline.py::test_multi_intent_retracts_an_admitted_ack_when_no_delegated_child_was_accepted` and `test_multi_intent_rejection_retracts_an_already_admitted_ack` | These directly assert the ack is retracted (or not) under the exact "nothing accepted" condition each handler computes differently |
| 3 | Terminal-status derivation | single/pending: `tests/test_pipeline.py::test_result_ready_is_not_emitted_when_the_canonical_commit_raises` (asserts derived status is not published early). multi: `tests/test_pipeline.py::test_multi_intent_cancel_racing_the_commit_downgrades_the_pre_derived_status` | Directly exercises "derive before commit, only publish after" for both the single-child and per-item multi-intent shapes |
| 4 | Commit wrapped in try/except with fallback | single: `tests/test_pipeline.py::test_commit_exception_after_a_completed_search_sweeps_the_child_as_failed`. pending: **NEEDS-CHARACTERIZATION-TEST** — no pending-path-specific commit-exception test found (only single- and multi-intent variants exist); add a test asserting `_handle_pending`'s commit failure emits the correct fallback status via `work_status_after_commit_failure` and re-raises. multi: `tests/test_pipeline.py::test_multi_intent_commit_failure_does_not_drop_sibling_results` and `test_multi_intent_commit_exception_still_emits_failed_parent` | Multi/single are directly pinned; pending's own commit-failure branch (:2019–2032) has no dedicated exercising test today — `test_result_ready_is_not_emitted_when_the_canonical_commit_raises` covers the single-intent flavor of the assertion only |
| 5 | Post-commit terminal status emission | single: `test_result_ready_is_not_emitted_when_the_canonical_commit_raises`. pending: **NEEDS-CHARACTERIZATION-TEST** — same gap as row 4; no pending-specific "status emitted only after commit succeeds" test. multi: `test_multi_intent_cancel_racing_the_commit_downgrades_the_pre_derived_status`, `test_multi_intent_sibling_commit_exception_does_not_fail_a_retained_child` | Same reasoning as row 4 — pending shares the derivation-then-emit shape but has no direct pin |
| 6 | Child recorder `.finalize(...)` | single: `test_commit_exception_after_a_completed_search_sweeps_the_child_as_failed` (asserts child record outcome). pending: `test_pending_turn_cancelled_mid_submit_sweeps_the_child_as_cancelled`. multi: `test_multi_intent_reconciled_unattributed_child_still_honours_a_subsequent_whole_turn_cancel` (exercises the reconcile-path finalize call, one of multi-intent's 4 call sites) | Each asserts a specific child-record outcome that only lands if the matching `.finalize()` call fired |
| 7 | `turn_recorder.finalize()` | single: broad coverage via any happy-path test, e.g. the transcript-flow test at `tests/test_pipeline.py:700` area (asserts `event.kind` sequence including terminal events implying recorder finalize ran). pending: `tests/test_pipeline.py::test_pending_turn_retracts_its_ack_when_submit_accepts_nothing` (reaches the end-of-handler finalize). multi: happy path in `test_multi_intent_commit_failure_does_not_drop_sibling_results` (asserts `turn_recorder.finalize()` still runs after partial failure) | Confirms the shared "finalize once at the end" contract holds under each handler's distinct control flow |
| 8 | Capability-gated retained short-circuit (`return None`) | single: `tests/test_pipeline.py::test_multi_intent_retained_child_on_capable_connection_stays_status_only` is multi-intent's variant; the single-intent equivalent is the retained/background branch exercised by `test_retained_late_result_cancelled_during_speech_start_still_emits_background_record`. pending: **NEEDS-CHARACTERIZATION-TEST** — no test found that specifically drives `_handle_pending`'s capability-gated `return None` branch (:1967–1975) versus its legacy-result branch; add one asserting a capable connection gets status-only with no legacy canonical result for the pending-dialogue path | Row 8 is explicitly marked absent for multi-intent (different shape, no whole-handler early return) and only weakly covered for pending |
| 9 | Worker `idle` projection post-commit | single: the transcript happy-path test asserting `worker.status == "idle"` and `worker.latest_result_id == result.result_id` (`tests/test_pipeline.py` ~line 737-739, in the multi-turn transcript flow test). pending/multi: **NEEDS-CHARACTERIZATION-TEST** — absence is not itself pinned; no test asserts that `_handle_pending`/`_handle_multi_intent` leave `worker.status`/`latest_result_id` unchanged (or in what state) after their own commit. Add characterization tests asserting the actual post-commit worker-state left behind by each, so extraction cannot silently start calling `.project(idle, ...)` for them | This is the one row most likely to be lost in a naive extraction: if the shared epilogue calls `.project(idle, ...)` unconditionally, it silently changes pending/multi-intent behavior with nothing failing today |
| 10 | Shared `_finalize_turn_exception` | `tests/test_pipeline.py::test_cancel_status_sweep_is_idempotent_for_already_terminal_children` (exercises the sweep logic inside the already-shared helper directly) | Already a shared, single-definition method; this test pins its internal idempotency, applicable to all three call sites |
| 11 | Finally: release-all vs partial release | single/pending: `test_pending_turn_retracts_its_ack_when_submit_accepts_nothing` and the retained-path tests (`test_retained_user_cancelled_work_item_suppresses_commit_and_speech`) exercise the "retain, don't release" branch. multi: `test_cancel_of_one_coordinator_retained_child_leaves_the_ack_for_a_still_live_sibling` | The multi-intent citation specifically exercises the *partial*-release branch (some children retained, others released) that single/pending never need, because they have at most one child |

**Tally**: 11 rows total. 8 rows have an existing pinning test (1, 2, 3, 6, 7, 10, 11, and partially 4/5/8 for the single/multi flavors). **4 sub-cells are marked NEEDS-CHARACTERIZATION-TEST**, all on the `_handle_pending` column plus one cross-cutting gap:
- Row 4 (pending): commit-exception fallback status, pending-specific.
- Row 5 (pending): post-commit status emission, pending-specific.
- Row 8 (pending): capability-gated `return None` short-circuit, pending-specific.
- Row 9 (pending + multi-intent): absence of worker-`idle` projection — needs a test that pins the *current* (no projection) behavior for both non-single handlers before extraction, or a naive shared epilogue will start calling it for them.

## 4. Extraction-scope recommendation

**Recommendation: a new module, `server/turn_epilogue.py`, not a SessionHost-internal class.**

Justification (from the table):
1. The genuinely shared logic (rows 2–7, 10, 11 — ack settlement, status derivation/emission ordering around commit, child/turn recorder finalization, exception cleanup, work-item release) is substantial: 7 of 11 rows are "same" or "same with a parameterizable variance" across at least two handlers, and row 10 is *already* a standalone method proving this kind of extraction is viable and testable in isolation.
2. The plan's own Architecture & Call Flow section already names `server/turn_epilogue.py` and mandates it "receives an explicit context object (ack ledger, speech scheduler, recorder handles) — never reads SessionHost internals via `self`" — that constraint alone argues against a `SessionHost`-internal class, since an internal class nested in/attached to `SessionHost` would be tempted to reach `self` directly the moment a corner case needs one more collaborator.
3. Collaborators touched by the epilogue are already numerous and cross-cutting (`_turn_ack_ledger`/`_settle_turn_ack`, `origin.scheduler`, `_work_ledger` cancellation set, `_emit_work_status`, `_recorder_factory`/child+turn recorders, `_worker_projection`, `_release_*_turn_work_item`) — a free module with an explicit context dataclass makes every one of these an auditable parameter rather than an implicit `self` reach, which is exactly what Requirement 2/Architecture demand.
4. The divergence is real but boundable: rows 1, 3, 4, 5, 6, 8, 9, 11 all diverge *only* in cardinality (one child vs N children) or in one extra gating condition (`cancel_admitted`, capability gate, worker-idle projection). This is naturally expressed as a small entry-point per cardinality (`finalize_single_child_turn(...)` for single/pending, `finalize_fan_out_turn(...)` for multi-intent) sharing internal helpers, not as one all-purpose god-function or three near-duplicate methods bolted onto `SessionHost`.
5. Row 9 (worker-idle projection) is the sharpest argument for an explicit, opt-in parameter (e.g. `project_idle: bool = False`) rather than folding it in unconditionally — the module boundary makes that toggle a visible, tested contract instead of a silent `self._worker_projection` reach that only single-intent happens to exercise today.

### Phase 1 outcome (2026-08-26, commits 8875fdc…d1226e2)

- **Scope decision: new module `server/turn_epilogue.py`** (rationale in the characterization section above). Two entry points: `finalize_single_child_turn` (single-intent + pending; per-caller variance via `cancel_admitted`, `project_idle`, `record_commit_ms` params) and `finalize_fan_out_turn` + sync `release_fan_out_turn_work_items` (multi-intent). `TurnEpilogueContext` carries explicit collaborators; no `self`-reads, no imports from `pipeline.py` (verified by review).
- Line drawn: the three fan-in loops that BUILD per-index state stay in `_handle_multi_intent`'s distinct middle; the epilogue starts at ack-settle. Row 8 (capability-gated retained short-circuit) stays in the single/pending middles — it returns before a result exists. Row 10 (`_finalize_turn_exception`) remains the pre-existing shared method, called from thin except blocks.
- Handler deltas: `_handle_transcript_impl` 470→~435, `_handle_pending` 249→239, `_handle_multi_intent` 613→~543 lines.
- Requirement 2 evidence: 11-row table above; 4 NEEDS-CHARACTERIZATION-TEST cells got tests in 8875fdc (397→401) before any extraction.
- **Advisory review (opus) findings, all fixed in d1226e2 with fail-first regression pins (401→405):** Critical — `retained_still_open` was derived inside the awaited epilogue, so except/finally saw a stale value when commit/speak raised on the retained path (single-intent; sweep + release-all fired where pre-extraction skipped); Important — pending mirror (`and not was_cancelled` adjustment lost pre-await); Important — pending turns gained an unsanctioned `commit_ms` measurement (now gated off via `record_commit_ms=False`); Minor — dead `finalize_turn_exception`/`cancelled_ids` ctx fields dropped; Minor — `work_status_after_commit_failure` test re-export removed (test imports the real home).

### Phase 1 row-3 correction (2026-08-27, review round 1)

The differences table above records row 3 (terminal-status derivation) as
"Same shape, single child" for both `_handle_transcript_impl` and
`_handle_pending`. That was wrong in one detail: single-intent gated the
retained-and-not-cancelled case to "emit nothing", while `_handle_pending`
re-derived it through `child_work_status_after_dispatch`, which republishes
`background` after the commit. The first extraction collapsed both onto the
single-intent policy. The two are observably different only when
`enable_background_status` is off when the dispatch-time `background` emit
runs (suppressing it) and back on by the time `_commit_and_speak` returns --
in that window pending's second derivation is the only thing that moves the
child off its stale `searching` record before the late result lands, so the
collapse was an unsanctioned behavior change under Requirement 9. Fixed by
making the policy an explicit `derive_status` parameter on
`finalize_single_child_turn` (default `status_omitted_while_retained` for
single-intent; `_handle_pending` passes `child_work_status_after_dispatch`),
pinned fail-first by
`tests/test_pipeline.py::test_pending_retained_child_recovers_background_when_the_status_gate_reopens`.

### Review Waivers

- 2026-08-26 marker refresh at conduct start: recorded marker (2026-08-25 @ 55db787a…) was stale because commit `0c3320e` (merged PR #9) corrected one Phase 3 line citation (`server/config.py` edit region + landed sha). Sole contract drift; landed via reviewed PR, so the marker was refreshed in place (`e3e5ad2c…`) instead of re-running `/review-plan`.
- Test-command override (all phases): the plan's literal `uv run mypy .` fails with 723 pre-existing errors because `.` overrides the pyproject mypy scope (`files = ["server", "evals", "scripts"]`; tests/ carries legacy debt by design). Effective gate used by conduct: `uv run pytest -q && uv run ruff check . && uv run mypy` (bare mypy = repo-intended scope, green at baseline).

### Phase 3 contract update (2026-08-27, merged from main / P1 PR #10)

P1's retire commit (`c67da7f`) deleted `load_promotion_manifest`/
`PromotionManifest` and all consumers from `server/config.py` outright, so
Phase 3's "extract promotion-manifest logic to `server/promotion_manifest.py`"
bullet is dropped as superseded — there is no manifest logic left in
`config.py` to extract; program row 7 closed as `fixed c67da7f` (machinery
deleted rather than extracted) rather than via this plan's Phase 3.

This contract edit intentionally stales this plan's review marker (branch
marker at merge time: `2026-08-26 @ e3e5ad2c…`). This plan is mid-run —
Phases 0-1 are completed in its conduct state file — so its next conduct
invocation is a `--resume`, which per conduct's documented resume rule
auto-refreshes a stale marker itself; no `/review-plan` re-run is needed for
this superseded-bullet drop.

### Phase 0 inventory (2026-08-26)

Baseline test counts (assertion-parity reference): full suite 2101 collected; tests/test_pipeline.py 397, tests/test_smoke_conversation.py 6, tests/test_work_item_coordinator.py 58, tests/test_session_host.py 45.

Duck-typed Coordinator doubles (`rg -n 'class \w*Coordinator' tests/`): 77 total — test_pipeline.py 71, test_smoke_conversation.py 3, test_work_item_coordinator.py 2, test_session_host.py 1.

Private collaborator-read call sites (patterns per collaborator):
- speech_scheduler: `\.scheduler\._\w+` → 47 hits (43 `_queues`, 3 `_active`, 1 `_provider_contexts`) in test_pipeline.py + test_session_host.py.
- turn_ack_ledger: `(ack_)?ledger\._\w+` → 29 hits in test_pipeline.py (12 `_ack_emitted_turns`; 12 `_claim_ack_admission_generation`/`_ack_admission_generation`; 5 `_schedule_ack_admission` — private-method setup calls, excluded from read-accessor scope).
- observers: 3 hits in tests/test_app.py (`_paused`, `_buffer`).
- speech_lifecycle: 21 hits in tests/test_speech_lifecycle.py (`_transition_tasks`, `_context_tombstones`, `_context_tokens`, `_timers`, `_timer_handles`, `_teardown_generation`, `_generations`, plus private-method setup calls excluded).
- work_item_coordinator: ~21 hits in tests/test_work_item_coordinator.py (`_submission_tasks`, `_shutdown`, `_owned_tasks`, `_late_tasks`, `_cancelling_tasks`, `_submit_tasks`, `_provider_tasks`, `_mandatory_tasks`, `_background_task_order`; setup *writes* like `coordinator._shutdown = True` excluded from read-accessor scope).

#### Phase 0 migration evidence (2026-08-26, commits b0860a7…e67b422)

- Doubles: 77 `class *Coordinator` matches → 43 genuine duck-typed doubles migrated to `FakeCoordinator` (test_pipeline.py 17-via-`RoutedCoordinator`-base + 27 standalone — one standalone also counted in the base group; test_smoke_conversation.py 3; test_session_host.py 1). **Allowlisted non-doubles (with reason):** test_pipeline.py 28-29 `class *Coordinator(WorkItemCoordinator)` subclasses of the *real* class (production-seam tests, not duck-typed impersonators); test_work_item_coordinator.py `BareCoordinator` (deliberately member-less — tests `coordinator_view`'s getattr-fallback path; migrating would defeat the test) and `RecordingCoordinator` (real-class subclass). Post-migration base-class census: 33 FakeCoordinator + 15 RoutedCoordinator(→FakeCoordinator) + 1 CoordinatorDefaults (in `_doubles.py` itself) + 29 WorkItemCoordinator (allowlisted).
- Assertion parity: every doubles-migration commit verified `git diff | grep -cE '^[-+].*assert'` == 0 and collect-only counts unchanged (397/45/6/58/80/77/50/18 per module). Call-site migration commits rewrote assert lines only under semantic equivalence (same predicate strength; never count-for-contents), recorded per commit message.
- Call sites: 38 (sub-step c) + 70 (test_pipeline + leftover) + 30 (residual sweep: bare-local-variable reads the dot-prefixed inventory grep missed in test_speech_scheduler/test_observers/test_pipeline) migrated. Accessors added beyond b0860a7: `speech_lifecycle.uses_event_loop_timers_with_clock`, `speech_scheduler.has_any_queue/has_any_paused/all_queued_items`.
- Residual private-read allowlist (final sweep clean otherwise): test_pipeline.py:4923 `coordinator._callback` (test-owned attribute on an inline double); test_perf_metrics.py:170-219 `observer._event_handlers` (upstream `pipecat.observers.*` classes, outside the 5 collaborator modules); test_observers.py:502 `observer._emit is None` (white-box emitter-wiring check, no accessor); test_pipeline.py:7255 (docstring mention). Sanctioned non-reads retained: private setup writes (`_ack_emitted_turns.add/update`, `_shutdown =`, `_owned_tasks`, `_queues[...] =`) and private-method setup calls (`_claim_ack_admission_generation`, `_schedule_ack_admission`, `_schedule`, `_dispatch_cleanup`).

#### Phase 0 advisory review (opus, one-shot) — findings & resolutions

- Fixed in the review-fix commit: 3 predicate-strength weakenings (test_pipeline.py:785/786/2287 `pending_work_item_ids()`→`has_any_queue()`; :10184 `has_queue`→`queued_items` truthiness); residual reads the sweep missed (`_timer_handles`→`timer_handle_count`, `_advance_tasks`→new `advance_task_count`/`advance_tasks` accessors, `_turn_work_items`→existing `turn_work_items`); `FakeCoordinator.submit`/`retain_late_task` now raise `NotImplementedError` without a hook (loud-failure consistency with `dispatch`); `EventLoopTimerScheduler` gained a public `clock` property (no production private read from the lifecycle accessor); dead `is_shutdown` accessor removed.
- **Finding 4 fixed (follow-up commit):** `FakeCoordinator.config` now defaults to unset (falling through to `CoordinatorDefaults.config = None`), matching the production fallback surface, so `coordinator_view`'s `getattr(coordinator, "config", None)` branch is exercised again post-migration (pinned by `test_foreground_search_timeout_comes_from_the_host_config_not_the_coordinator` and the `direct_delegated` early-ack residue test). `RoutedCoordinator`'s hand-written config workaround was removed as redundant; no test site needed an explicit `Config()` added (parity 452 collected unchanged; suite matches baseline). Phase 3's Requirement 1 re-litigation should still weigh `BareCoordinator` as the deliberate member-absence fallback test — doubles conformance alone doesn't prove the getattr-fallbacks dead.

Doubles mechanism decision: **Protocol** (reuse of existing `server.work_item_coordinator.Coordinator` Protocol as sole interface source; ABC rejected — would force a shared base class the suite deliberately avoids). `tests/_doubles.py` provides `FakeCoordinator` (subclasses `CoordinatorDefaults`, overridable per-member) plus structural conformance checks (`conformance_problems` / `assert_conforms_to_coordinator` comparing Protocol annotations + method signatures); pinned by tests/test_doubles.py against both the fake and the real `WorkItemCoordinator` (4 new tests). Note: tests/test_doubles.py imports `from _doubles import ...` (bare) — `tests.`-prefixed import collides in `mypy .` runs because tests/ has no `__init__.py`.

### Phase 2 outcome (2026-08-27): connect() + ConnectionPipeline extraction

- Sub-step A (commit 56b5d70): `ConnectionPipeline` moved verbatim to new `server/connection_pipeline.py`; no re-export shim from pipeline.py. Real importers updated: `turn_ack_ledger.py` TYPE_CHECKING import, `handshake_gate.py` docstring ref.
- Sub-step B: `connect()` (210 lines, 9 nested closures) decomposed into ~63-line orchestration + 9 `_connect_*` methods bound via `functools.partial`, plus helpers `_build_connection_lifecycle` / `_wire_connection_transcript_and_tts` / `_retire_old_connection`. Late-binding closure semantics preserved via module-level `_PendingConnectionPipeline` holder dataclass read at call time; per-connect holder keeps reconnect callbacks isolated. Zero behavior change (Requirement 9).
- Test-writer added 2 pin tests: call-time (not registration-time) holder reads; reconnect gives each connection's transport callback its own fresh holder.
- Advisory review (opus): all 9 closures verified binding-equivalent, side-effect ordering line-for-line identical, coordinator-boundary declarations untouched. 3 Minor findings, all fixed: unfilled-holder paths in `_connect_stop_speech` / `_connect_on_lifecycle_terminal` / `_connect_transport_acceptable` now `assert pipeline is not None` (loud failure matching original NameError behavior); reconnect test pins `PreAdmissionTerminalReason.UNAVAILABLE_TRANSPORT` instead of bare isinstance; call-time test asserts the empty-holder invocations raise and notes its scope (method-body semantics, not connect() wiring).
- Gate: 1868 passed + 1 skipped, ruff format/check clean, bare mypy clean, smoke passed.

### Phase 3 outcome (2026-08-27): Config/boundary consolidation

- **Requirement 1 — RE-AFFIRMED, four-declaration pin stands (no collapse).** Call-site enumeration: `OptionalCoordinator` is the Protocol spec pinned as the roster's Protocol leg (`test_all_four_coordinator_boundary_declarations_carry_the_same_members`); `CoordinatorDefaults` supplies every fallback `coordinator_view` returns (work_item_coordinator.py:401) and is `FakeCoordinator`'s base (tests/_doubles.py); `CoordinatorView` is `SessionHost._coordinator_view`'s return type (pipeline.py:1336) consumed at pipeline.py:547/577/1315/2621/3164/3319; `coordinator_view()` is the one production resolution function (pipeline.py:1354). Decisive evidence: `BareCoordinator` (deliberately member-less) still exercises the getattr-fallback path post-Phase-0 — doubles conformance does NOT prove the fallbacks dead. Roster frozenset + pinning test unchanged; dated rationale addendum added to `OPTIONAL_COORDINATOR_MEMBERS`' docstring. The boundary-consolidation ambition is dropped from this plan per Requirement 1(b).
- **Requirement 5 — DECIDED: PERMISSIVE.** Sole production consumer of `OWNED_CONFIG_FIELDS` is SessionHost.__init__'s config-conflict exclusion (pipeline.py:577-585); strictness would single out one optional member for validation none of the other six get, and risks repeating round-5's 67-test revert. Rationale comment added at the definition. Coverage gap closed: new pin `test_coordinator_view_honours_a_coordinators_own_owned_config_fields` (non-vacuous — asserts the coordinator's own differing frozenset is honored AND differs from canonical); absent-member half was already pinned. New pinning test for unchanged behavior — zero behavior change.
- **Requirement 8 — VERIFIED, program item 12 closed, no gap.** `shared/work-status-retention.json` is the single numeric source, loaded by server/session_state.py (`_RETENTION_CONFIG_PATH`:43) and web/src/state.js (import :1, consumed ~:180); dual parity pins: `tests/test_session_state.py::test_retention_constants_match_the_shared_config_file` (+ `::test_retention_fallback_matches_shared_config` guarding the packaged-install fallback) and web/test/state.test.js "retention constants match the shared config file". No new work.
- Promotion-manifest bullet: dropped as superseded (P1 retire c67da7f) — server/config.py and server/contracts.py read, verified clean, untouched.
- Test-writer audit: all three decisions enforced by tests that fail on silent revert; no gaps, no tests added. Gate: 1869 passed + 1 skipped, ruff format/check clean, bare mypy clean, smoke validation passed. Warning: plan's Test command says `mypy .`; gate ran bare `mypy` (established substitution — `mypy .` collides on tests/ bare imports, per Phase 0 note).

### Phase 4 outcome (2026-08-27): ack-latch race — ALREADY-FIXED, Requirement 7 / program item 10 closed

- Characterization: the round-10 quarantined interleaving (20260728-feature-early-ack-background-delivery-v0.1.3.md:731, logic #23) — a stale admission-retry chain's belated timer re-entering `start_next` after a later sibling re-latches the same turn under the identical `ack_work_item_id` — is caught by the existing `_ack_admission_generation` mechanism: each admission chain claims a strictly-increasing ledger-wide generation (`_claim_ack_admission_generation`, turn_ack_ledger.py ~:126-149), and both `_retry_or_abandon` (~:498-522) and `admit()` (~:588-608) compare their captured generation against the live entry and bail when superseded — exactly the documented event order.
- Provenance: the mechanism was added specifically for this finding in commit 390b764, which post-dates round 10's terminal cap — quarantined at round 10, fixed in a later confirm round.
- Regression pin: new deterministic test `tests/test_session_host.py::test_stale_ack_admission_retry_recognizes_a_newer_chain_under_the_same_key` drives the full interleaving (chain A latch → fail → settle → chain B latch+admit → chain A's stale retry fires) and asserts chain A never re-enters start_next and chain B's admitted ack survives.
- Non-vacuity evidence: with both generation early-return guards temporarily disabled (`if False: return`), the test fails exactly as the round-10 finding describes (stale retry re-enters start_next, exhausts the busy-slot chain, abandons the ack); server/turn_ack_ledger.py then reverted byte-identical (`git diff` empty). No production code changed in the final state.
- Gate: 1870 passed + 1 skipped (baseline +1 new pin), ruff format/check clean, bare mypy clean.

### Phase 5 inventory & outcome (2026-08-27): fire-and-forget helper

Execution-time inventory (`rg -n 'add_done_callback' server/`): 22 hits across 10 files (no drift from the 2026-08-25 count).

**Converted (13 sites, 7 files → `retain_until_done`):** observers.py:189 `_emit_tasks`; pipeline.py:1039 `_background_shutdowns`; speech_lifecycle.py:796 `_transition_tasks`; speech_scheduler.py:145 `_stop_tasks`, :517 `_advance_tasks`; turn_ack_ledger.py:585/:638 `_ack_admission_tasks` (retry + admit chains); turns.py:92 `_dispatch_tasks`, :151 `_completion_tasks`; work_item_coordinator.py:567 `_owned_tasks`, :592 `_mandatory_tasks`, :597 `_cancelling_tasks` (via `on_done=_consume_task_exception`, preserving discard-then-consume ordering), :1111 `_submit_tasks`.

**Excluded with reason (9 sites, 5 files):** app.py:720 (pure exception-observation, no retention set — "tracked, not fire-and-forget"); pipeline.py:3307 (cancel-then-retrieve idiom, the plan's named exclusion — line drifted from cited ~3421 via earlier phases); runner_supervisor.py:121 (identity-guarded dict cleanup; unconditional discard would evict a newer registration); work_item_coordinator.py:637 and :825 (dual set+ordered-deque retention via `_discard_ordered_task`, plus bespoke handling); :846 (shutdown cancel-then-retrieve sweep); :1113 (`_work_task_cleanup` closure — separate per-work-item dict cleanup registered alongside the converted `_submit_tasks` discard); work_task_ledger.py:84 (nested dict-of-sets with empty-bucket pop); :99 (identity-guarded dict assignment + known/cancelled-ids side effects).

**Grep-zero exit check:** post-conversion `rg -n 'add_done_callback' server/` = 13 hits = 4 in task_retention.py itself (1 docstring + 3 implementation) + the 9 allowlisted exclusions, one-for-one. No hand-rolled add/discard retention idiom remains outside the helper.

**Helper:** `server/task_retention.py::retain_until_done(task, tracking, *, on_done=None)` — leaf module, no server imports; generic over Task/Future (TypeVar bound `asyncio.Future[Any]`); adds before registration exactly as the old sites did; `on_done` runs after the discard. 8 pin tests in tests/test_task_retention.py (retention/discard/exception/on_done-ordering/Future-input/Task-input/idempotent-discard/cancellation-observation).

**Advisory review (opus):** verified per-site behavior equivalence for all 13 conversions (no identity-vs-argument mismatches; callback registration order preserved incl. the :1112 second callback; unobserved-exception semantics unchanged where no observation existed) and the 9 exclusions as genuinely non-plain shapes. Findings resolved: inventory recorded here (Important); cancellation pin test added + one test renamed to match its body (Minors). **Knowingly-accepted non-observable difference (Requirement 9 note):** the old `_adopt_task`/`_track_cancelling_task` closures held a strong ref to the coordinator via `self`; the helper's callbacks reference only the tracking set, so an in-flight task no longer keeps its coordinator alive on its own — accepted because SessionHost strongly holds the coordinator in every production path.

**Commits:** helper+tests 27a9b20, then one conversion commit per file (9e91ade, c7bb143, 6d05aca, 2c4a74a, 368ac64, 74a0f08, 91db5ef) per the plan's one-commit-each item.

**Gate:** 1878 passed + 1 skipped (baseline 1870 + 8 helper tests), ruff format/check clean, bare mypy clean.

### Phase 6 outcome (2026-08-27): facade collapse + program closure

- Forwarder enumeration: 66 SessionHost members classified. **Removed (10, all pure TurnAckLedger pass-throughs):** `_next_turn_id`, `_turn_sequence`, `_ack_work_item_id`, `_clear_ack_latch`, `_settle_turn_ack`, `_register_turn_work_item`, `_release_all_turn_work_items`, `_release_turn_work_item`, `_ack_turn_for_work_item`, `_emit_early_ack` — every production and test call site rewritten to reach `self._turn_ack_ledger` / `TurnAckLedger` directly (tests/test_pipeline.py ~45 sites + 2 monkeypatch rewires, test_session_host.py ~5, test_work_status.py 1). **Kept-with-reason:** `runner`/`runner_factory`/`validate_handshake_token`/`validate_patch_handshake` (app.py public contract), `on_ack_terminal` (test-pinned sole turn-latch mutator, scheduler-wired), `_dispatch` (bound-method to_thread target + catalogue branch), `_emit_work_status`/`_terminalize_child_work_statuses` (work-status publisher facade, call-site volume), coordinator boundary untouched per Phase 3 re-affirmation. The AD-r10 "~19" over-counted: 10 were pure pass-throughs; the rest carry logic or contract.
- Line counts: server/pipeline.py 3336 → 3263; SessionHost class span 2812 → 2739. (Original AD-r8 figure: ~2856-line class in a ~3400-line file before Phases 0-6's extractions: turn_epilogue.py, connection_pipeline.py, task_retention.py.)
- Advisory review (opus): behavior equivalence verified for all 10 removals — the 3 callback-reference wirings bind the same runtime target (ledger assigned once in __init__, never rebound); monkeypatch rewires intercept the identical 3 call paths (grep-complete); no test-predicate weakened; coordinator boundary confirmed untouched. Findings fixed: architecture.md keep-list extended (+`_emit_work_status`/`_terminalize_child_work_statuses`/`_dispatch`) and removal claim scoped to "ledger-facing"; task_retention.py user list corrected; 6 stale test comments/docstrings requalified to `TurnAckLedger.<method>`.
- Docs: docs/architecture.md gained a `server/` module-layout subsection; AGENTS.md a layout pointer + no-new-forwarders bullet.
- Program closure (this commit, same-commit rule): P2 Status → Complete; program Subordinate Plans row flipped; provenance rows 1-6, 8-12 closed; docs/dev_plans/README.md updated.
- Gate: 1878 passed + 1 skipped, ruff format/check clean, bare mypy clean, smoke validation passed.
- Marker note: the Status-header flip above the review marker intentionally stales it at run end (same accepted pattern as P1's completion flip); recorded here as the run's final contract edit.

### Post-completion review gauntlet (2026-08-27/28, `skein:review-gauntlet`, 6 rounds)

Auto-chained by conduct (`Review Gates: full`). Target `branch:refactor/sessionhost-decomposition`; gates per round: adversarial Codex review, deep-review (5 lenses), security-review. Finding trajectory 7 → 5 → 1 → 0 → 2 → 0. Five fix commits landed after Phase 6 closure:

- `a214c57` — the one real behavioral finding (codex + logic + architecture converged): `_handle_pending`'s post-commit `background` re-emit for retained children was dropped by the Phase 1 epilogue extraction; live only when `enable_background_status` toggles off→on mid-turn. Restored as an explicit `derive_status` policy parameter (matching the existing per-caller variance knobs) + fail-first pin test `test_pending_retained_child_recovers_background_when_the_status_gate_reopens`. Recorded above in "Phase 1 row-3 correction".
- `f158272` — CHANGELOG `[Unreleased]` P2 entry (docs lens finding).
- `ad1c126` — `TurnEpilogueContext` collaborators typed as Protocol call shapes (`SettleTurnAck`, `EmitWorkStatus`, `CommitAndSpeak`, `DeriveWorkStatus`); annotation-only, zero runtime change (verified by round-2 security pass).
- `423ca75` — `work_status_publisher` docstring enumeration completed (`late_commit_work_status` added).
- `8e925c2` — narrowed docstring/doc overclaims (Protocol wording in `turn_epilogue.py`; dropped nonexistent "publisher" from architecture.md's ConnectionPipeline bullet).

Quarantined as plan-recorded design (never fixed, per Guardrail 1): the 32 Phase-0 test accessors; the Phase-2 `_connect_*`-on-SessionHost shape; the Phase-6 `host._turn_ack_ledger` test reach-through; the single shared `TurnEpilogueContext`.

Terminal ledger decision: `non-converge` — a stall-counter artifact (round 6's clean confirm pass tied, rather than beat, the epoch minimum of 0). All three gates approved with zero findings in round 6; operator accepted the state as converged and proceeded to `/update-docs`. Final gate at `8e925c2`: 1879 passed + 1 skipped, ruff format/check clean, bare mypy clean.
