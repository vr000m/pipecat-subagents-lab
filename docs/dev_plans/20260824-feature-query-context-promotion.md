# Task: Query-Context Narrowing — Promote or Retire (P1)

**Status**: Not Started
**Component**: evals
**Assigned to**: Claude
**Priority**: High
**Branch**: feature/query-context-promotion
**Created**: 2026-08-24

**Review Gates:** none

Program: `20260824-design-v013-followup-program.md` (item 20).

## Objective

Close the last open thread of the v0.1.3 plan: the query-context narrowing
experiment shipped implemented-but-gated (`promotion_eligible=false`,
`reason=real_stratum_missing`) because no paid-provider evidence was ever
collected. Collect that evidence, make the promote-or-retire decision, and
carry it out. Retirement is a legitimate outcome — code behind a permanently
false gate is debt.

## Context

- Experiment code: `scripts/run_query_context_experiment.py`, `scripts/collect_query_context_latency.py`, `scripts/analyze_query_context_latency.py`, `scripts/query_context_common.py`.
- Evidence + manifest: `docs/benchmarks/v0.1.3-query-context*.json{,l}`, `docs/benchmarks/v0.1.3-promotion-manifest.json`, written via `scripts/validate_v013_evidence.py --write-manifest`.
- Server consult path: `server/app.py:757` loads the manifest at startup (`load_promotion_manifest`, `server/config.py`); `server/pipeline.py:670` stores `SessionHost._promotion_eligible`; gated again at `server/pipeline.py:2837`.
- Promotion requires **no server code change** — it is manifest-data-driven. Retirement deletes gating code in `server/pipeline.py` and the experiment scripts.
- **Parallelization**: promote path is worktree-parallel-safe with P2/P3. If the decision is *retire*, coordinate with P2 per the program's matrix (land deletion before P2 Phase 2, or hand it to P2).

## Requirements

1. A real-stratum paid-provider evidence run, recorded under `docs/benchmarks/`, schema-valid per `shared/schemas/v013-query-context-raw.json` and the post-change analysis schema.
2. An explicit, documented promote-or-retire decision with the analysis numbers that justify it.
3. If promote: regenerated promotion manifest with `promotion_eligible=true`, passing `scripts/validate_v013_evidence.py`'s gates; live smoke confirming autoplay behavior change.
4. If retire: experiment scripts and the pipeline gating branch removed (or deletion delegated to P2 with a provenance-map note), manifest left fail-closed, CHANGELOG entry.
5. Program provenance map row 20 updated in the same PR.

## Implementation Checklist

### Phase 0: Collect real-stratum evidence (paid)

**Impl files:** docs/benchmarks/v0.1.3-query-context.jsonl, docs/benchmarks/v0.1.3-query-context-analysis.json
**Test files:** tests/test_query_context_latency.py
**Test command:** `uv run pytest -q tests/test_query_context_latency.py tests/test_v013_evidence_validator.py`
**Validation cmd:** `uv run python scripts/analyze_query_context_latency.py --input docs/benchmarks/v0.1.3-query-context.jsonl`
**Goal:** The missing real stratum exists as committed, schema-valid evidence; no analysis or decision yet.

- [ ] Confirm the collection protocol from the v0.1.3 plan's Phase 4 section (sample size, strata, query set) before spending on the paid provider.
- [ ] Run `scripts/collect_query_context_latency.py` / `scripts/run_query_context_experiment.py` against the live paid provider for the real stratum.
- [ ] Commit raw evidence; verify schema validity via the existing validator tests.

### Phase 1: Analyze and decide

**Impl files:** docs/benchmarks/v0.1.3-query-context-analysis.json, docs/benchmarks/v0.1.3-promotion-manifest.json
**Test files:** tests/test_v013_phase4c_manifest.py
**Test command:** `uv run pytest -q tests/test_v013_phase4c_manifest.py tests/test_v013_evidence_validator.py`
**Goal:** One documented decision — promote or retire — backed by the analysis output; the manifest reflects it.

- [ ] Run the analysis; record the latency/quality deltas in this plan's Findings.
- [ ] Decision gate: promote only if the analysis meets the v0.1.3 plan's stated promotion thresholds; otherwise retire.
- [ ] Promote path: `scripts/validate_v013_evidence.py --write-manifest` with the full evidence set; confirm `promotion_eligible=true`; run a live `scripts/smoke_conversation.py` pass confirming behavior.
- [ ] Retire path: mark the decision here and in the program doc; deletion executes in Phase 2 (or is delegated to P2 if P2 Phase 2 has started — check the program matrix).

### Phase 2: Carry out the decision + docs

**Impl files:** CHANGELOG.md, README.md, docs/dev_plans/20260824-design-v013-followup-program.md
**Test files:** tests/test_release_metadata.py
**Test command:** `uv run pytest -q`
**Goal:** Repo state, docs, and the program provenance map all agree with the decision; no dead gate remains ambiguous.

- [ ] Promote: CHANGELOG + README note the newly active behavior.
- [ ] Retire: remove `scripts/{run,collect,analyze}_query_context*.py`, `scripts/query_context_common.py`, the `server/pipeline.py:2837` gating branch and `_promotion_eligible` plumbing (coordinate with P2), and their tests; CHANGELOG "Removed" entry.
- [ ] Update program provenance map row 20 (`fixed <sha>` or `retired: <reason>`).

## Acceptance Criteria

- [ ] Real-stratum evidence committed and schema-valid.
- [ ] Decision documented with numbers in Findings.
- [ ] Manifest state matches the decision; `scripts/validate_v013_evidence.py` and full pytest suite pass.
- [ ] Program doc row 20 closed.

<!-- reviewed: YYYY-MM-DD @ <hash> -->

## Progress

- [ ] Phase 0: Collect real-stratum evidence (paid)
- [ ] Phase 1: Analyze and decide
- [ ] Phase 2: Carry out the decision + docs

## Findings
