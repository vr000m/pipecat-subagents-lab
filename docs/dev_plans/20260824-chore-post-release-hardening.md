# Task: Post-Release Hardening — Gauntlet Caveats + Eval Minors (P3)

**Status**: Not Started
**Component**: tooling
**Assigned to**: Claude
**Priority**: Medium
**Branch**: chore/post-release-hardening
**Created**: 2026-08-24

**Review Gates:** none

Program: `20260824-design-v013-followup-program.md` (items 13-19).

## Objective

Clear the small, independent items the v0.1.3 gauntlets accepted as follow-ups:
the three Restart Gauntlet round-9 caveats (TTS half-pair guard, release-metadata
manifest-verify false-negative, ci.yml drift-gate scope) and the eval-suite's
deferred Minor findings. All are half-day-sized, no ordering constraints among
them, worktree-parallel-safe with P1 and (modulo one `server/config.py` region —
see program matrix) with P2.

## Requirements

1. TTS half-pair guard (`server/config.py:1676-1708`) derives its pairing knowledge from the registry instead of a hand-maintained constant, or the hand-written form is pinned with a parity test against the registry.
2. `scripts/check_release_metadata.py`'s manifest-verify check gets an explicit decision on indirect invocation (`VAR=path; --verify-manifest "$VAR"`): support it, or pin fail-closed with a test + comment naming the accepted false-negative.
3. ci.yml drift-gate: stop the bypass whack-a-mole by adding one behavior-level assertion (fixture-commit or equivalent observed-CI check) OR explicitly scope the YAML-intent parser's covered axes in a comment + test, closing the "structurally open-ended" caveat as a documented boundary.
4. Eval manifest-staleness heuristic relaxed from exact `source_commit` equality to something that survives unrelated commits (e.g., staleness = candidate-defining inputs changed), keeping the explicit override flag.
5. Effort-policy labels cross-validated against registered model labels at config load (typo → hard error).
6. The two reasoning-effort env vars get direct tests (including the empty-string case, whose semantics get decided and pinned); README env-var docs refreshed.
7. Phase-2 eval minors addressed or explicitly retired with reasons: policy-dict wholesale-replacement semantics, effort-display-vs-effective-value, truncated-cell status reporting.

## Implementation Checklist

### Phase 1: Registry-driven TTS half-pair guard

**Impl files:** server/config.py
**Test files:** tests/test_config.py
**Test command:** `uv run pytest -q tests/test_config.py`
**Goal:** The guard cannot silently drift from the registry — either it reads the registry, or a test fails when the two disagree.

- [ ] Read the guard and the registry's pairing source; pick derivation vs parity-test (smaller correct change wins).
- [ ] Implement; add the drift test.
- [ ] Land-order note: P2 Phase 3 also edits `server/config.py` — prefer landing this first (program matrix).

### Phase 2: Release-metadata + drift-gate closure

**Impl files:** scripts/check_release_metadata.py, tests/test_justfile_ci_parity.py
**Test files:** tests/test_release_metadata.py, tests/test_justfile_ci_parity.py
**Test command:** `uv run pytest -q tests/test_release_metadata.py tests/test_justfile_ci_parity.py`
**Goal:** Both round-9 caveats end as decided boundaries, not open-ended TODOs — each with a test that pins the decision.

- [ ] Manifest-verify: decide indirect-invocation handling (Requirement 2); implement + test.
- [ ] Drift-gate: decide behavior-assertion vs scoped-parser (Requirement 3); implement + test; document the covered/uncovered axes where the check lives.

### Phase 3: Eval-suite minors

**Impl files:** scripts/verify_eval_candidates.py, scripts/eval_common.py, scripts/eval_model_comparison.py, server/config.py, README.md
**Test files:** tests/test_verify_eval_candidates.py, tests/test_eval_common.py, tests/test_eval_model_comparison.py, tests/test_config.py
**Test command:** `uv run pytest -q tests/test_verify_eval_candidates.py tests/test_eval_common.py tests/test_eval_model_comparison.py tests/test_config.py`
**Goal:** Requirements 4-7 each end fixed-with-test or retired-with-reason; none stays silently deferred.

- [ ] Staleness heuristic (Requirement 4) — define "candidate-defining inputs" precisely before coding.
- [ ] Effort-label cross-validation at config load (Requirement 5).
- [ ] Env-var tests + empty-string semantics + README (Requirement 6).
- [ ] Phase-2 minors triage (Requirement 7) — record each disposition in Findings.

## Acceptance Criteria

- [ ] Requirements 1-7 each resolved with a test or a recorded retirement reason.
- [ ] Full pytest suite, `ruff format`/`ruff check` green.
- [ ] Program provenance map rows 13-19 closed.

<!-- reviewed: YYYY-MM-DD @ <hash> -->

## Progress

- [ ] Phase 1: Registry-driven TTS half-pair guard
- [ ] Phase 2: Release-metadata + drift-gate closure
- [ ] Phase 3: Eval-suite minors

## Findings
