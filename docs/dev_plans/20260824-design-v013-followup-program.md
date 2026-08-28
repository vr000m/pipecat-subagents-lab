# Task: v0.1.3 Follow-up Program (super plan — tracking only, NOT conductable)

**Status**: Active — program tracking
**Component**: program
**Assigned to**: Claude + Varun
**Priority**: High
**Branch**: n/a (this document is never implemented directly)
**Created**: 2026-08-24

**Review Gates:** none

> **This plan is deliberately not conductable.** It has no Implementation Checklist,
> no `Test command:` slots, and no review marker. Work happens only in the
> subordinate dev plans listed below. This document owns three things the child
> plans must not duplicate: (1) the canonical provenance map of every open item
> left behind by the v0.1.3 review gauntlets, (2) the dependency/parallelization
> rules between child plans, and (3) the program exit criterion. New follow-up
> work gets added here first (a row in the provenance map + an owning plan),
> then implemented via a child dev plan.

## Objective

Close out everything the v0.1.3 release (PR #7, merged 2026-08-24, tag `v0.1.3`)
left open: ~20 review-gauntlet rounds across two plans converged with a stable
backlog of quarantined structural findings, deferred correctness items, three
Restart Gauntlet round-9 caveats, deferred eval-suite minors, and one unshipped
feature decision (query-context narrowing, Phase 4). Every item below ends the
program either **fixed** (commit recorded) or **retired** (reason recorded).

## Subordinate Plans

| Plan | File | Status | Review Gates |
|------|------|--------|--------------|
| P1 — Query-context promotion decision | `20260824-feature-query-context-promotion.md` | Complete (retired) | none (hand-run reviews) |
| P2 — SessionHost decomposition | `20260824-refactor-sessionhost-decomposition.md` | Complete (rows 1-6, 8-12 closed) | full |
| P3 — Post-release hardening | `20260824-chore-post-release-hardening.md` | Complete (rows 13-19 closed) | none (hand-run reviews) |

Child-plan completion updates this table and the provenance map below in the
same commit that flips the child's own status header.

**Conditional fourth plan (P1-invest only):** if P1's Phase 1 decision is
*invest*, P1 creates a dedicated promote plan (its Appendix A) and, in the same
commit, adds it to this table and the parallelization matrix, repoints row 20
at it, and closes itself as handed-off. Until that happens, no such plan
exists and the matrix governs only P1-P3.

## Provenance Map (canonical — the single source for "why is this still open")

Origin key: **AD-rN** = ack-delivery plan (`20260728-feature-early-ack-background-delivery-v0.1.3.md`) gauntlet round N; **RG-r9** = its Restart Gauntlet round 9 caveats; **ES** = eval-suite plan (`20260817-feature-router-worker-model-eval-suite.md`) phase reviews.

| # | Item | Origin | Owner | Resolution |
|---|------|--------|-------|------------|
| 1 | `SessionHost` god-class (~2856 lines, `server/pipeline.py:594`), decomposition so far facade-only | AD-r8/r10 | P2 Ph1-2,6 | fixed — decomposed across P2 Ph0-6: turn_epilogue.py (ca981bb), connection_pipeline.py (56b5d70), connect() decomposition (877003e), task_retention.py (27a9b20), forwarder collapse (P2 Ph6 boundary commit) |
| 2 | ~19 one-line pass-through forwarders left by prior extractions | AD-r10 | P2 Ph6 | fixed — 10 pure pass-throughs removed, remainder kept-with-recorded-reason (P2 Ph6 boundary commit; \"~19\" over-counted) |
| 3 | Three ~250-600-line turn handlers duplicating epilogue logic | AD-r8/r10 | P2 Ph1 | fixed ca981bb (shared turn-epilogue abstraction, characterization-table-backed) |
| 4 | `SessionHost.connect` — 210 lines, 8 nested closures (AD-r10 figure; 9 by P2's current recount) | AD-r8/r10 | P2 Ph2 | fixed 877003e (9 closures → _connect_* methods, late-binding holder, zero behavior change) |
| 5 | `ConnectionPipeline` should live in its own module (`server/pipeline.py:356`) | AD-r10 | P2 Ph2 | fixed 56b5d70 (moved verbatim, no re-export) |
| 6 | `SessionHost`↔coordinator boundary declared 4× — all four live in production paths (the AD-r10 "two production-dead" claim was disproven in P2's plan review); re-litigation of the pin stays open, in P2 Ph3 | AD-r8/r10 | P2 Ph3 | retired: pin RE-AFFIRMED in the open — all four live in production, BareCoordinator proves getattr-fallback live; rationale recorded (f32e0b2) |
| 7 | ~400 lines of promotion-manifest logic belong outside `server/config.py` | AD-r10 | retire → P1 Ph2 (deleted); invest → P2 Ph3 (extracted); P1-blocked → stays open | fixed c67da7f (machinery deleted rather than extracted) |
| 8 | `_OWNED_CONFIG_FIELDS` permissive default (`server/work_item_coordinator.py:131`) — deferred 5 consecutive rounds | AD-r5/7/8/9/10 | P2 Ph3 | retired: PERMISSIVE pinned with rationale comment + non-vacuous honored-own-value test (f32e0b2) — 6th-round deferral ended |
| 9 | Fire-and-forget task-reference idiom hand-rolled in ~11 sites (P2 review corrected the original "5 classes" count) | AD-r9/r10 | P2 Ph5 | fixed 27a9b20..91db5ef — retain_until_done helper; 13 sites converted, 9 excluded with recorded reasons, grep-zero exit check (d1fa8c1) |
| 10 | Ack-retry latch sibling re-latch race — verify-first: `_ack_admission_generation` (turn_ack_ledger.py:74-139) may already cover it; P2 Ph4 reproduces before fixing | AD-r10 | P2 Ph4 | retired: already-fixed by _ack_admission_generation (390b764, post-round-10); deterministic reproduction pin with demonstrated non-vacuity (8474933) |
| 11 | ~60 duck-typed `Coordinator` test doubles + 56 test call sites reading extracted collaborators — blocked every in-gauntlet structural attempt | AD-r5/r8/r10 | P2 Ph0 | fixed b1f6921 (+review-fix commits) — FakeCoordinator Protocol doubles, 43 migrated, ~138 private-read call sites moved to public accessors, assertion parity verified |
| 12 | Client re-implements server's work-status eviction policy — P2 review found the "pin" option already implemented (`shared/work-status-retention.json` + dual parity tests); P2 Ph3 verifies and documents | AD-r10 | P2 Ph3 (verify) | retired: verified — shared/work-status-retention.json + dual parity tests (Python tests/test_session_state.py, JS web/test/state.test.js); no gap (f32e0b2) |
| 13 | TTS half-pair guard hand-written, not registry-driven (`server/config.py:1676-1708`) | RG-r9 | P3 Ph1 | fixed 60e0f36 |
| 14 | `check_release_metadata.py` manifest-verify false-negative on indirect shell invocation (fail-closed, not live in ci.yml) | RG-r9 | P3 Ph2 | fixed 0f3da39 |
| 15 | ci.yml drift-gate class structurally open-ended (YAML-intent parsing vs observed-behavior assertion) | RG-r9 | P3 Ph2 | fixed 8e2090c |
| 16 | Eval manifest-staleness heuristic: exact `source_commit` equality trips after every commit — P3 review found this is a documented fail-closed whole-tree-attestation decision with the override flag as its sanctioned valve; P3 Ph3 records a go/no-go (retire vs explicit reversal), not a presumed relaxation | ES | P3 Ph3 (decision) | retired: no-go on relaxation — fail-closed whole-tree attestation stands, override flag is the sanctioned valve (730db3f) |
| 17 | Effort-policy labels not cross-validated (typos silently accepted) — premise stale: P3 review verified the validation already exists (`server/config.py:236-259` + tests); P3 Ph3 verifies and retires with citation | ES | P3 Ph3 (verify) | retired: verified — `server/config.py:236-259` + `tests/test_config.py:313`/`:329` (730db3f) |
| 18 | Env-var coverage gaps: 2 reasoning-effort env vars untested; README stale — premise stale: P3 review verified both vars are tested incl. the empty-string pin (`tests/test_config.py:2019-2030`); P3 Ph3 verifies README against the pinned semantics and retires with citation | ES | P3 Ph3 (verify) | retired: verified — worker var `tests/test_config.py:1955-2132` (empty-string pin `:2019-2030`), router var `:376`; README matches (730db3f) |
| 19 | Policy-dict wholesale-replacement semantics + effort-display-vs-effective-value + truncated-cell reporting (Phase-2 minors) | ES | P3 Ph3 | fixed 730db3f (effort-display gap fixed with test; policy-dict retired as intended design; truncated-cell retired as not-reproducible) |
| 20 | Query-context narrowing implemented but `promotion_eligible=false` (`reason=real_stratum_missing`) — no paid evidence ever collected | AD Phase 4 | P1 (all) | retired: operator decision after feasibility pre-flight — promote required the full Appendix A effort incl. paid collection against an experiment that never demonstrated value (c67da7f) |
| 21 | `docs/benchmarks/` misnomer: 8 of 9 committed files are v0.1.3 release-gate evidence, not benchmarks — load-bearing for the frozen release record even though the runtime loader is gone: ci.yml's `promotion-manifest-drift` job (read-only drift check), `justfile:40-41`'s `verify-manifest` recipe, and `check_release_metadata.py`'s manifest-path check all still bind against these files, and they're frozen in place (manifest embeds paths; re-stamping requires `--source-commit` = `740b364`, impossible from later HEAD). Fix forward only: v0.1.4+ evidence lands in `docs/evidence/`, `check_release_metadata.py`'s path pattern updated then. Never delete/move the v0.1.3 artifacts | evidence audit 2026-08-25 | first v0.1.4 release plan (deferred — not P1/P2/P3) | open |

When an item closes, replace `open` with `fixed <short-sha>` or `retired: <one-line reason>`.

## Dependency & Parallelization Rules

**Worktree parallelization matrix** (can these run concurrently in separate worktrees?):

| Pair | Parallel-safe? | Why |
|------|----------------|-----|
| P1 ∥ P3 | **Resolved — executed in order.** P3 Phase 2 landed before P1's retire commit (`c67da7f`), per rule. | P1's Phases 0-1 touched its own plan file, this program doc (Phase 1 recorded the decision and updated row 20 — P3 Phase 3 also edited this doc; mechanical merge, last lander rebased), and one probe. P1's *retire* Phase 2 removed the `config.toml` manifest key and the runtime loader chain (`load_promotion_manifest`/`PromotionManifest` in `server/config.py` and its consumers) — it did not touch the `justfile` recipe, `check_release_metadata.py`'s manifest-path check, or any ci.yml step, since P3 Phase 2 had already converted those into read-only drift-check machinery beforehand; that machinery survives P1's retire unchanged. P3's other files (`server/config.py` guard region, `scripts/eval_*.py`) stayed disjoint. |
| P1 ∥ P2 | **Resolved — P1's retire commit (`c67da7f`) landed before P2 Phase 2, per rule.** | *Retire* was a full-chain removal of the machinery (pipeline gate → unconditional `commit_display_only`, `load_promotion_manifest`/`PromotionManifest` out of `server/config.py`, `app.py` call, plus the manifest's CI/release-check consumers; the committed manifest and evidence files stay as frozen records — row 21) overlapping P2 Phases 2-3; P2 Phase 3's manifest-extraction bullet has been dropped accordingly (see P2 plan Findings). The invest path was never exercised (operator chose retire). |
| P2 ∥ P3 | **Mostly — one same-file caveat.** | P3 Ph1 (TTS half-pair guard) and P2 Ph3 (promotion-manifest extraction) both edit `server/config.py`, in different regions (~1689-1716 vs ~340-470/667-770). Semantically independent; merge conflict is mechanical. Rule: whichever lands second rebases; prefer landing P3 Ph1 first since it's small. Everything else is disjoint. |

**Recommended schedule**: start P1 and P3 immediately in parallel worktrees.
P2 may also start immediately — its Phase 0 touches tests/ plus narrow public
read accessors on server collaborator modules (speech_scheduler, turn_ack_ledger,
observers, speech_lifecycle, work_item_coordinator), all disjoint from P1's and
P3's footprints — but its Phase 2 is gated on P1's Phase 1 decision (retire
commit must land first on the retire path; P2 plan Phase 2 gate).

**Retire-path landing order** (explicit, since three plans touch shared files):
P2 Phase 0 anytime → P3 Phase 2 → P1 retire commit → P2 Phase 2 onward. This
order was followed: P3 Phase 2 landed, then P1's retire commit (`c67da7f`),
unblocking P2 Phase 2. Invariant 1 below governs plans **outside** this
program; inside it, this order and the matrix governed — P1's
`server/pipeline.py` gate-region edits and P2 Phase 1's epilogue work touched
different regions of that file, whichever landed second rebased.

**Cross-plan invariants**:
1. No *new* feature plan may touch `SessionHost` until P2 Phases 0-1 have merged — otherwise its review gauntlet re-reports items 1-4 for a third time.
2. Each child plan's Files-to-Modify must stay inside its footprint above; expanding it requires updating the matrix here first.
3. Review-gauntlet quarantines arising *during* a child plan get a new row in the provenance map, owned by that plan or explicitly re-assigned.

## Exit Criterion

The program closes when every provenance-map row **1-20** reads `fixed` or
`retired` (row 21 is deferred by construction to the first v0.1.4 release plan
— it closes as `deferred: v0.1.4` here and does not block program exit; rows a
recorded P1-blocked carve-out leaves open — 7, and 20 if blocked persists —
block exit until the blocked state is resolved by an operator decision), all
child plans' status headers read Complete/Shipped, and `docs/dev_plans/README.md`
reflects the final states. The index is **created by the first child plan to
complete** (each child's final phase carries the create-or-update task) — it
does not exist today.

## Findings

- 2026-08-24: Program created. Backlog snapshot taken from AD rounds 5-10, Restart Gauntlet rounds 4-9 (156 fixes, 0 new quarantines — backlog stable across ~20 total rounds), and eval-suite phase reviews.
- 2026-08-25: Evidence audit of `docs/benchmarks/` added row 21 and corrected the P1 ∥ P3 matrix row: P1's retire path must remove the manifest's CI/release-check consumers (overlapping P3 Phase 2) while keeping every committed v0.1.3 artifact as a frozen release record. P1 and P3 plans updated in the same pass.
- 2026-08-25: Codex adversarial review of all four plan docs (20 findings, 2 Critical) fixed across the set. Headlines: P1's retire footprint gained the remaining `PromotionManifest` consumers (`server/composition.py`, `scripts/eval_common.py`, `scripts/smoke_conversation.py`) and the regression test must cover the fail-open `enable_autoplay_policy=False → "autoplay"` branch at `server/pipeline.py:2835`; P2's fire-and-forget inventory recounted (22 hits / 10 files); rows 4/6/7 corrected here; blocked-branch carve-outs, invest handoff, retire-path landing order, and index-creation ownership made explicit.
- 2026-08-27: P2 complete — all 7 phases executed via /skein:conduct (Ph0 b1f6921, Ph1 ca981bb, Ph2 877003e, Ph3 f32e0b2, Ph4 8474933, Ph5 d1fa8c1, Ph6 this commit). Rows 1-6, 8-12 closed (5 fixed, 4 retired-with-evidence + row 2 fixed); Subordinate Plans row flipped to Complete. Row 21 remains the program's only open row (deferred to first v0.1.4 release plan).
- 2026-08-27: P1 complete — operator chose retire (Phase 1 decision) after the Phase 0 feasibility pre-flight found the model available but promote gated behind the full Appendix A effort against an experiment that never demonstrated value; full-chain removal executed in `c67da7f`. Rows 20 and 7 closed; P1 ∥ P2 and P1 ∥ P3 matrix rows and the retire-path landing order marked resolved; P1's row in Subordinate Plans flipped to Complete (retired); P2 plan's Phase 3 manifest-extraction bullet dropped (P2 Findings).
