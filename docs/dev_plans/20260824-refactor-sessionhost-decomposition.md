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
**Goal:** The coordinator-boundary question decided in the open, promotion-manifest logic out of config.py (via whichever P1 branch), and the OWNED_CONFIG_FIELDS + client-eviction items decided rather than deferred a sixth time.

- [ ] Promotion-manifest logic in `config.py`, by P1 outcome (decision recorded before this phase per the Phase 2 gate): **retire** → deleted by P1's landed commit, this bullet drops; **invest** → extract to `server/promotion_manifest.py` (callers update imports, `config.py` re-exports nothing); **blocked/escalated** → hold this bullet, proceed with the rest of the phase. Rebase note: P3 Phase 1 edited `server/config.py:1689-1716` (landed 60e0f36) — land order per program matrix.
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

<!-- reviewed: 2026-08-26 @ e3e5ad2cd85425bedcc9a3b43a386169616611c2 -->

## Progress

- [x] Phase 0: Test-double modernization
- [ ] Phase 1: Shared turn-epilogue abstraction
- [ ] Phase 2: connect() + ConnectionPipeline extraction
- [ ] Phase 3: Config/boundary consolidation
- [ ] Phase 4: Ack-latch race — verify, then fix
- [ ] Phase 5: Fire-and-forget helper
- [ ] Phase 6: Facade collapse

## Findings

### Review Waivers

- 2026-08-26 marker refresh at conduct start: recorded marker (2026-08-25 @ 55db787a…) was stale because commit `0c3320e` (merged PR #9) corrected one Phase 3 line citation (`server/config.py` edit region + landed sha). Sole contract drift; landed via reviewed PR, so the marker was refreshed in place (`e3e5ad2c…`) instead of re-running `/review-plan`.
- Test-command override (all phases): the plan's literal `uv run mypy .` fails with 723 pre-existing errors because `.` overrides the pyproject mypy scope (`files = ["server", "evals", "scripts"]`; tests/ carries legacy debt by design). Effective gate used by conduct: `uv run pytest -q && uv run ruff check . && uv run mypy` (bare mypy = repo-intended scope, green at baseline).

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

Doubles mechanism decision: **Protocol** (reuse of existing `server.work_item_coordinator.Coordinator` Protocol as sole interface source; ABC rejected — would force a shared base class the suite deliberately avoids). `tests/_doubles.py` provides `FakeCoordinator` (subclasses `CoordinatorDefaults`, overridable per-member) plus structural conformance checks (`conformance_problems` / `assert_conforms_to_coordinator` comparing Protocol annotations + method signatures); pinned by tests/test_doubles.py against both the fake and the real `WorkItemCoordinator` (4 new tests). Note: tests/test_doubles.py imports `from _doubles import ...` (bare) — `tests.`-prefixed import collides in `mypy .` runs because tests/ has no `__init__.py`.
