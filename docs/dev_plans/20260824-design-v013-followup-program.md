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
| 7 | ~400 lines of promotion-manifest logic belong outside `server/config.py` | AD-r10 | P1 Ph2 (deleted on retire) / P2 Ph3 (extracted, only if P1 chooses invest) | open |
| 8 | `_OWNED_CONFIG_FIELDS` permissive default (`server/work_item_coordinator.py:131`) — deferred 5 consecutive rounds | AD-r5/7/8/9/10 | P2 Ph3 | open |
| 9 | Fire-and-forget task-reference idiom hand-rolled in ~11 sites (P2 review corrected the original "5 classes" count) | AD-r9/r10 | P2 Ph5 | open |
| 10 | Ack-retry latch sibling re-latch race — verify-first: `_ack_admission_generation` (turn_ack_ledger.py:74-139) may already cover it; P2 Ph4 reproduces before fixing | AD-r10 | P2 Ph4 | open |
| 11 | ~60 duck-typed `Coordinator` test doubles + 56 test call sites reading extracted collaborators — blocked every in-gauntlet structural attempt | AD-r5/r8/r10 | P2 Ph0 | open |
| 12 | Client re-implements server's work-status eviction policy — P2 review found the "pin" option already implemented (`shared/work-status-retention.json` + dual parity tests); P2 Ph3 verifies and documents | AD-r10 | P2 Ph3 (verify) | open |
| 13 | TTS half-pair guard hand-written, not registry-driven (`server/config.py:1676-1708`) | RG-r9 | P3 Ph1 | open |
| 14 | `check_release_metadata.py` manifest-verify false-negative on indirect shell invocation (fail-closed, not live in ci.yml) | RG-r9 | P3 Ph2 | open |
| 15 | ci.yml drift-gate class structurally open-ended (YAML-intent parsing vs observed-behavior assertion) | RG-r9 | P3 Ph2 | open |
| 16 | Eval manifest-staleness heuristic: exact `source_commit` equality trips after every commit — P3 review found this is a documented fail-closed whole-tree-attestation decision with the override flag as its sanctioned valve; P3 Ph3 records a go/no-go (retire vs explicit reversal), not a presumed relaxation | ES | P3 Ph3 (decision) | open |
| 17 | Effort-policy labels not cross-validated (typos silently accepted) — premise stale: P3 review verified the validation already exists (`server/config.py:236-259` + tests); P3 Ph3 verifies and retires with citation | ES | P3 Ph3 (verify) | open |
| 18 | Env-var coverage gaps: 2 reasoning-effort env vars untested; README stale — premise stale: P3 review verified both vars are tested incl. the empty-string pin (`tests/test_config.py:2019-2030`); P3 Ph3 verifies README against the pinned semantics and retires with citation | ES | P3 Ph3 (verify) | open |
| 19 | Policy-dict wholesale-replacement semantics + effort-display-vs-effective-value + truncated-cell reporting (Phase-2 minors) | ES | P3 Ph3 | open |
| 20 | Query-context narrowing implemented but `promotion_eligible=false` (`reason=real_stratum_missing`) — no paid evidence ever collected | AD Phase 4 | P1 (all) | open |
| 21 | `docs/benchmarks/` misnomer: 8 of 9 committed files are v0.1.3 release-gate evidence, not benchmarks — load-bearing (`config.toml:54` loader, ci.yml:174 + `justfile:41` re-hash, `check_release_metadata.py:429` path check) and frozen in place (manifest embeds paths; re-stamping requires `--source-commit` = `740b364`, impossible from later HEAD). Fix forward only: v0.1.4+ evidence lands in `docs/evidence/`, `check_release_metadata.py`'s path pattern updated then. Never delete/move the v0.1.3 artifacts | evidence audit 2026-08-25 | first v0.1.4 release plan (deferred — not P1/P2/P3) | open |

When an item closes, replace `open` with `fixed <short-sha>` or `retired: <one-line reason>`.

## Dependency & Parallelization Rules

**Worktree parallelization matrix** (can these run concurrently in separate worktrees?):

| Pair | Parallel-safe? | Why |
|------|----------------|-----|
| P1 ∥ P3 | **Phases 0-1 yes; retire path no** (corrected 2026-08-25 after the evidence audit). | P1's Phases 0-1 touch only its own plan file + one probe. Its *retire* Phase 2 removes the manifest's out-of-server consumers — the ci.yml release-metadata manifest-write step, `justfile:41`, `check_release_metadata.py:429`, `config.toml:54` — overlapping P3 Phase 2 (`scripts/check_release_metadata.py`, `tests/test_justfile_ci_parity.py`). Rule: land P3 Phase 2 before P1's retire commit; whichever lands second rebases. P3's other files (`server/config.py` guard region, `scripts/eval_*.py`) stay disjoint. |
| P1 ∥ P2 | **No on either path** (corrected 2026-08-24 after P1's plan review — the original "promote is data-only" claim was false). | *Retire* is a full-chain removal of the machinery (pipeline gate → unconditional `commit_display_only`, `load_promotion_manifest`/`PromotionManifest` out of `server/config.py`, `app.py` call, plus the manifest's CI/release-check consumers; the committed manifest and evidence files stay as frozen records — row 21) overlapping P2 Phases 2-3 — land P1's retire commit before P2 Phase 2 starts; P2 Phase 3's manifest-extraction bullet then drops. *Promote* (invest path, separate plan) requires serialization: the manifest's identity binding (`source_commit`/`tree_hash`) means it must be stamped after all concurrent work merges, or re-stamped post-merge. |
| P2 ∥ P3 | **Mostly — one same-file caveat.** | P3 Ph1 (TTS half-pair guard) and P2 Ph3 (promotion-manifest extraction) both edit `server/config.py`, in different regions (~1676-1708 vs ~340-470/667-770). Semantically independent; merge conflict is mechanical. Rule: whichever lands second rebases; prefer landing P3 Ph1 first since it's small. Everything else is disjoint. |

**Recommended schedule**: start P1 and P3 immediately in parallel worktrees.
P2 may also start immediately — its Phase 0 touches tests/ plus narrow public
read accessors on server collaborator modules (speech_scheduler, turn_ack_ledger,
observers, speech_lifecycle, work_item_coordinator), all disjoint from P1's and
P3's footprints — but its Phase 2 is gated on P1's Phase 1 decision (retire
commit must land first on the retire path; P2 plan Phase 2 gate).

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
- 2026-08-25: Evidence audit of `docs/benchmarks/` added row 21 and corrected the P1 ∥ P3 matrix row: P1's retire path must remove the manifest's CI/release-check consumers (overlapping P3 Phase 2) while keeping every committed v0.1.3 artifact as a frozen release record. P1 and P3 plans updated in the same pass.
