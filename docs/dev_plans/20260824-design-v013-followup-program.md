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
| P1 — Query-context promotion decision | `20260824-feature-query-context-promotion.md` | Not Started | none (hand-run reviews) |
| P2 — SessionHost decomposition | `20260824-refactor-sessionhost-decomposition.md` | Not Started | full |
| P3 — Post-release hardening | `20260824-chore-post-release-hardening.md` | Not Started | none (hand-run reviews) |

Child-plan completion updates this table and the provenance map below in the
same commit that flips the child's own status header.

## Provenance Map (canonical — the single source for "why is this still open")

Origin key: **AD-rN** = ack-delivery plan (`20260728-feature-early-ack-background-delivery-v0.1.3.md`) gauntlet round N; **RG-r9** = its Restart Gauntlet round 9 caveats; **ES** = eval-suite plan (`20260817-feature-router-worker-model-eval-suite.md`) phase reviews.

| # | Item | Origin | Owner | Resolution |
|---|------|--------|-------|------------|
| 1 | `SessionHost` god-class (~2856 lines, `server/pipeline.py:594`), decomposition so far facade-only | AD-r8/r10 | P2 Ph1-2,6 | open |
| 2 | ~19 one-line pass-through forwarders left by prior extractions | AD-r10 | P2 Ph6 | open |
| 3 | Three ~250-600-line turn handlers duplicating epilogue logic | AD-r8/r10 | P2 Ph1 | open |
| 4 | `SessionHost.connect` — 210 lines, 8 nested closures | AD-r8/r10 | P2 Ph2 | open |
| 5 | `ConnectionPipeline` should live in its own module (`server/pipeline.py:356`) | AD-r10 | P2 Ph2 | open |
| 6 | `SessionHost`↔coordinator boundary declared 4× (two production-dead) | AD-r8/r10 | P2 Ph3 | open |
| 7 | ~400 lines of promotion-manifest logic belong outside `server/config.py` | AD-r10 | P2 Ph3 | open |
| 8 | `_OWNED_CONFIG_FIELDS` permissive default (`server/work_item_coordinator.py:131`) — deferred 5 consecutive rounds | AD-r5/7/8/9/10 | P2 Ph3 | open |
| 9 | Fire-and-forget task-reference idiom hand-rolled in 5 classes | AD-r9/r10 | P2 Ph4 | open |
| 10 | Ack-retry latch cannot distinguish its own latch from a sibling's re-latch (real, narrow race; needs new state-machine exit path) | AD-r10 | P2 Ph5 | open |
| 11 | ~60 duck-typed `Coordinator` test doubles + 56 test call sites reading extracted collaborators — blocked every in-gauntlet structural attempt | AD-r5/r8/r10 | P2 Ph0 | open |
| 12 | Client re-implements server's work-status eviction policy (open design question) | AD-r10 | P2 Ph3 (decide) | open |
| 13 | TTS half-pair guard hand-written, not registry-driven (`server/config.py:1676-1708`) | RG-r9 | P3 Ph1 | open |
| 14 | `check_release_metadata.py` manifest-verify false-negative on indirect shell invocation (fail-closed, not live in ci.yml) | RG-r9 | P3 Ph2 | open |
| 15 | ci.yml drift-gate class structurally open-ended (YAML-intent parsing vs observed-behavior assertion) | RG-r9 | P3 Ph2 | open |
| 16 | Eval manifest-staleness heuristic: exact `source_commit` equality trips after every commit (override flag exists) | ES | P3 Ph3 | open |
| 17 | Effort-policy labels not cross-validated against registered model labels (typos silently accepted) | ES | P3 Ph3 | open |
| 18 | Env-var coverage gaps: 2 reasoning-effort env vars untested; README env-var docs stale | ES | P3 Ph3 | open |
| 19 | Policy-dict wholesale-replacement semantics + effort-display-vs-effective-value + truncated-cell reporting (Phase-2 minors) | ES | P3 Ph3 | open |
| 20 | Query-context narrowing implemented but `promotion_eligible=false` (`reason=real_stratum_missing`) — no paid evidence ever collected | AD Phase 4 | P1 (all) | open |

When an item closes, replace `open` with `fixed <short-sha>` or `retired: <one-line reason>`.

## Dependency & Parallelization Rules

**Worktree parallelization matrix** (can these run concurrently in separate worktrees?):

| Pair | Parallel-safe? | Why |
|------|----------------|-----|
| P1 ∥ P3 | **Yes** | Disjoint files. P1 touches `scripts/{run_query_context_experiment,collect_query_context_latency,analyze_query_context_latency,query_context_common}.py` + `docs/benchmarks/*`; P3 touches `server/config.py` (guard region), `scripts/check_release_metadata.py`, `tests/test_justfile_ci_parity.py`, `scripts/eval_*.py`. No overlap. |
| P1 ∥ P2 | **Yes on the promote path; No on the retire path.** | Promotion is data-driven (manifest + evidence files only — no server code change). Retirement deletes gating code in `server/pipeline.py`, which P2 is actively restructuring. Rule: if P1's decision is *retire*, either land the deletion before P2 Phase 2 starts, or hand the deletion to P2 as an added phase — never delete in parallel. |
| P2 ∥ P3 | **Mostly — one same-file caveat.** | P3 Ph1 (TTS half-pair guard) and P2 Ph3 (promotion-manifest extraction) both edit `server/config.py`, in different regions (~1676-1708 vs ~340-470/667-770). Semantically independent; merge conflict is mechanical. Rule: whichever lands second rebases; prefer landing P3 Ph1 first since it's small. Everything else is disjoint. |

**Recommended schedule**: start P1 and P3 immediately in parallel worktrees.
P2 may also start immediately (its Phase 0 test-double work conflicts with
nothing), but its Phase 2+ should not merge before P1's decision lands if that
decision is retirement.

**Cross-plan invariants**:
1. No *new* feature plan may touch `SessionHost` until P2 Phases 0-1 have merged — otherwise its review gauntlet re-reports items 1-4 for a third time.
2. Each child plan's Files-to-Modify must stay inside its footprint above; expanding it requires updating the matrix here first.
3. Review-gauntlet quarantines arising *during* a child plan get a new row in the provenance map, owned by that plan or explicitly re-assigned.

## Exit Criterion

The program closes when every provenance-map row reads `fixed` or `retired`,
all three child plans' status headers read Complete/Shipped, and
`docs/dev_plans/README.md` (index — created alongside the first child-plan PR)
reflects the final states.

## Findings

- 2026-08-24: Program created. Backlog snapshot taken from AD rounds 5-10, Restart Gauntlet rounds 4-9 (156 fixes, 0 new quarantines — backlog stable across ~20 total rounds), and eval-suite phase reviews.
