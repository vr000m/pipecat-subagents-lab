# Task: SessionHost Decomposition (P2)

**Status**: Not Started
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
into real modules, deduplicate the three turn handlers' epilogue logic, extract
promotion-manifest loading out of `server/config.py`, unify the fire-and-forget
task idiom, and fix the one real deferred bug (ack-latch sibling re-latch race).

## Context

Every in-gauntlet structural attempt died on test coupling: ~60 duck-typed
`Coordinator` test doubles (a round-5 strictness change broke 67 tests and was
reverted) and 56 test call sites reading extracted collaborators directly.
That is why Phase 0 is test infrastructure, not production code — the reviews
proved the extraction is impossible without it. Sequencing within this plan is
strictly linear (each phase rebuilds on the previous extraction); the plan as a
whole is worktree-parallel-safe with P1 (promote path) and P3 except for the
`server/config.py` caveat in the program's matrix.

Prior-decision pins to respect (do NOT re-litigate silently):
- `_work_status_sequence`/`_work_status_terminal_keys` stay unbounded — bounding breaks the `event_sequence` restart guarantee; a pinning test exists.
- Decomposition-by-facade was a deliberate v0.1.3 strategy to keep diffs mechanically verifiable; this plan is the increment that gets to collapse the facades.

## Requirements

1. `SessionHost` loses the ~19 pass-through forwarders and its four redundant coordinator-boundary declarations; one canonical boundary remains.
2. Shared turn-epilogue abstraction replaces the duplicated logic in the three ~250-600-line turn handlers.
3. `connect()` (210 lines, 8 nested closures) decomposed; `ConnectionPipeline` (`server/pipeline.py:356`) moves to its own module.
4. Promotion-manifest logic (~400 lines: `server/config.py` ~340-470 + 667-770) moves to a new `server/promotion_manifest.py`; `config.py` keeps only the config-shaped surface.
5. `_OWNED_CONFIG_FIELDS` permissive-vs-strict default (`server/work_item_coordinator.py:131`) gets a *decision* (5th-round deferral ends here): either strict with the modernized doubles absorbing it, or permissive pinned with a rationale comment + test.
6. One shared fire-and-forget helper replaces the hand-rolled idiom in `SpeechScheduler._advance_tasks`, `TurnAckLedger._ack_admission_tasks`, `RuntimeObserver._emit_tasks`, `SessionHost._background_shutdowns`, `SpeechLifecycleCoordinator._transition_tasks`.
7. Ack-latch race fixed: the retry chain can distinguish its own latch from a sibling's re-latch via an explicit state-machine exit path, with a regression test that reproduces the race first.
8. Client-side eviction-policy reimplementation (item 12) gets a documented decision: share the policy (server-authoritative constant or schema field) or pin the duplication with a parity test.
9. Zero behavior change outside item 7; full suite green at every phase boundary.

## Implementation Checklist

### Phase 0: Test-double modernization

**Impl files:** tests/_doubles.py
**Test files:** tests/test_work_item_coordinator.py, tests/test_session_host.py, tests/test_pipeline.py
**Test command:** `uv run pytest -q`
**Goal:** A single shared, contract-checked Coordinator double (and public read API for extracted collaborators) replaces the ~60 duck-typed doubles and 56 direct-attribute call sites, so later phases can change internals without mass test breakage.

- [ ] Inventory the duck-typed doubles and the 56 collaborator-read call sites (grep sweep; record counts in Findings).
- [ ] Introduce `tests/_doubles.py` with a canonical Coordinator fake validated against the real interface (Protocol or ABC — decide and document).
- [ ] Migrate test files to the shared double mechanically, one test module per commit.
- [ ] Add narrow public read accessors on extracted collaborators where tests currently reach into privates; migrate the 56 call sites.

### Phase 1: Shared turn-epilogue abstraction

**Impl files:** server/pipeline.py, server/turn_epilogue.py
**Test files:** tests/test_session_host.py, tests/test_pipeline.py
**Test command:** `uv run pytest -q`
**Goal:** The three turn handlers' duplicated epilogue logic lives in exactly one place; handler bodies shrink to their genuinely distinct middles.

- [ ] Characterize the three handlers' epilogues; write a differences table in Findings before extracting.
- [ ] Extract the common epilogue (new module or SessionHost-internal class — smallest correct scope).
- [ ] Convert each handler in its own commit; suite green after each.

### Phase 2: connect() + ConnectionPipeline extraction

**Impl files:** server/pipeline.py, server/connection_pipeline.py
**Test files:** tests/test_pipeline.py, tests/integration/test_browser_session.py
**Test command:** `uv run pytest -q`
**Goal:** `ConnectionPipeline` is its own module; `connect()` is a short orchestration of named steps with no nested closures holding hidden state.

- [ ] Move `ConnectionPipeline` to `server/connection_pipeline.py` (mechanical, own commit).
- [ ] Decompose `connect()`'s 8 closures into methods/functions with explicit parameters; document any closure that captured mutable state.
- [ ] Coordinate with P1: if P1 chose retirement and hasn't landed it, absorb the `_promotion_eligible` gating deletion here (program matrix rule).

### Phase 3: Config/boundary consolidation

**Impl files:** server/config.py, server/promotion_manifest.py, server/work_item_coordinator.py, server/contracts.py
**Test files:** tests/test_config.py, tests/test_work_item_coordinator.py, tests/test_work_status.py
**Test command:** `uv run pytest -q`
**Goal:** One coordinator-boundary declaration, promotion-manifest logic out of config.py, and the OWNED_CONFIG_FIELDS + client-eviction questions decided rather than deferred a sixth time.

- [ ] Promotion-manifest logic in `config.py`: **check P1's Phase 1 decision first.** On P1-retire (the default expectation) the machinery is deleted by P1 Phase 2 and this bullet drops entirely; only if P1 chose invest does this phase extract it to `server/promotion_manifest.py` (callers update imports, `config.py` re-exports nothing). Do not start this phase before P1's retire commit has landed (program matrix). Rebase note: P3 Phase 1 edits `server/config.py:1676-1708` — land order per program matrix.
- [ ] Delete the two production-dead boundary declarations; collapse the remaining two into one.
- [ ] Decide `_OWNED_CONFIG_FIELDS` strictness (Requirement 5) — the Phase 0 doubles make the strict option testable for the first time.
- [ ] Decide client eviction duplication (Requirement 8); implement the chosen form or add the parity test + rationale.

### Phase 4: Fire-and-forget helper

**Impl files:** server/task_retention.py, server/speech_scheduler.py, server/turn_ack_ledger.py, server/observers.py, server/pipeline.py, server/speech_lifecycle.py
**Test files:** tests/test_speech_scheduler.py, tests/test_work_task_ledger.py, tests/test_observers.py, tests/test_speech_lifecycle.py
**Test command:** `uv run pytest -q`
**Goal:** The add/discard task-retention idiom exists once, used by all five current sites, so the next fire-and-forget task cannot be written subtly differently.

- [ ] Implement the shared helper (module-level utility; keep it tiny).
- [ ] Convert the five sites, one commit each.

### Phase 5: Ack-latch sibling re-latch race

**Impl files:** server/turn_ack_ledger.py
**Test files:** tests/test_work_task_ledger.py, tests/test_session_host.py
**Test command:** `uv run pytest -q tests/test_work_task_ledger.py tests/test_session_host.py && uv run pytest -q`
**Goal:** The retry chain observes an explicit latch identity/epoch so it can never act on a sibling's re-latch; the race is pinned by a regression test that fails on the old code.

- [ ] Write the regression test reproducing the race first (round-10 quarantine described the interleaving); confirm it fails against pre-fix code.
- [ ] Add the state-machine exit path / latch identity; make the test pass.
- [ ] Re-run the exactly-once ack pinning tests; confirm no ordering guarantee regressed.

### Phase 6: Facade collapse

**Impl files:** server/pipeline.py
**Test files:** tests/test_session_host.py, tests/test_pipeline.py
**Test command:** `uv run pytest -q`
**Validation cmd:** `uv run python scripts/smoke_server.py`
**Goal:** The ~19 one-line forwarders are gone; callers reach collaborators through the one canonical boundary; SessionHost's line count reflects a real decomposition.

- [ ] Remove forwarders; update call sites (Phase 0's accessors make this mechanical).
- [ ] Record before/after line counts for `server/pipeline.py` and `SessionHost` in Findings.
- [ ] Update `docs/architecture.md` and AGENTS.md layout to the new module set.

## Acceptance Criteria

- [ ] All nine Requirements met; each with concrete evidence (line counts, grep-zero for forwarders, the failing-then-passing race test).
- [ ] Full suite + `ruff format`/`ruff check` + mypy green; smoke script passes.
- [ ] No behavior change outside Phase 5 (bug fix) — pinned by the unchanged public-contract tests.
- [ ] Program provenance map rows 1-12 closed.

<!-- reviewed: YYYY-MM-DD @ <hash> -->

## Progress

- [ ] Phase 0: Test-double modernization
- [ ] Phase 1: Shared turn-epilogue abstraction
- [ ] Phase 2: connect() + ConnectionPipeline extraction
- [ ] Phase 3: Config/boundary consolidation
- [ ] Phase 4: Fire-and-forget helper
- [ ] Phase 5: Ack-latch sibling re-latch race
- [ ] Phase 6: Facade collapse

## Findings
