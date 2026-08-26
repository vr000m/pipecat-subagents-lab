# Task: Query-Context Narrowing — Promote or Retire (P1)

**Status**: Not Started
**Component**: evals
**Assigned to**: Claude
**Priority**: High
**Branch**: feature/query-context-promotion
**Created**: 2026-08-24

**Review Gates:** none

Program: `20260824-design-v013-followup-program.md` (item 20; a retire outcome also closes item 7).

## Objective

Close the last open thread of the v0.1.3 plan: the query-context narrowing
experiment shipped implemented-but-gated (`promotion_eligible=false`,
`reason=real_stratum_missing`). Plan review established that the promote path
is **currently unreachable** for three independent reasons (see Context), so
this plan is **feasibility-gated with retire as the default outcome**: Phase 0
verifies the blockers cheaply with no material paid spend, Phase 1 escalates
the decision, and the retire path executes as a full-chain removal that leaves
the late-result disposition unconditionally fail-closed. Promotion survives
only as an explicit "invest" decision that spawns a dedicated follow-up plan
(scoped in the appendix); it is not promised by this plan.

## Context

**Why promote is currently unreachable** (verified against code during plan review):

1. **The real-stratum gate never reads query-context evidence.** `has_real_provider_stratum` (`scripts/validate_v013_evidence.py:305-315`, consumed at ~673-677) is computed exclusively from the `--phase0-input`/`--phase1-input` artifacts (`docs/benchmarks/v0.1.3-phase0-transport-baseline.jsonl`, `v0.1.3-phase1-ack-evidence.jsonl`), whose records are all `provider/model = unavailable/unavailable`. The query-context jsonl is not a manifest input; the writer CLI takes only `--phase0/1/2/3/4c-input`. The only allowlisted real stratum is `("openai", "gpt-4o-search-preview")` (`REAL_PROVIDER_ALLOWLIST`, :150-155).
2. **A second, independent gate is also false.** The verdict is `real_stratum_present AND transport_eligible` (`_promotion_verdict`, :543-568); the committed phase2 transport artifact has `promotion_eligible: false` (no named browser/device check has ever run). Fixing the real stratum alone yields `audibility_unverified`. `write_manifest` additionally hard-fails unless phase2/phase3 artifacts' `source_commit`/`source_tree_hash` match the passed identity (:596-608).
3. **The live collector is a stub.** `run_live()` in `scripts/run_query_context_experiment.py:396-401` always exits 1 with "BLOCKED: live query-context collection is not wired to a responses client in this runner".

**Identity binding**: promotion is NOT "data-only". `load_promotion_manifest`
(`server/config.py:822-838`) accepts the manifest only when `source_commit`,
`source_tree_hash`, `release_version`, and `feature_policy_fingerprint` all
match the running server and — when the optional `deployed_at_utc` is
configured (`server/config.py:145`; the stale check is skipped otherwise) —
`generated_at` postdates it,
fail-closed to `source_mismatch`/`identity_unbound` otherwise. Any promotion
manifest must therefore be stamped against the final deployed commit
(manifest-written-last), and the committed manifest pins each input file's
sha256, so evidence files cannot be appended to without a manifest rebind.

**Experiment code**: `scripts/run_query_context_experiment.py`, `scripts/collect_query_context_latency.py`, `scripts/analyze_query_context_latency.py`, `scripts/query_context_common.py`; schemas `shared/schemas/v013-query-context-raw.json` (note: it names the raw file `v0.1.3-query-context-raw.jsonl`; the committed `v0.1.3-query-context.jsonl` is the collector-normalized output) and `shared/schemas/v013-query-context-post-change-analysis.json` (governs the phase4c artifact only; its schema pins `status="promoted"`/`promotion_eligible=true`, so it can only exist after a promote decision).

**Parallelization (corrected from the first draft — the original note was
inverted)**: NEITHER outcome is freely parallel with P2.
- *Retire* (full-chain removal) deletes `load_promotion_manifest`/`PromotionManifest` from `server/config.py`, the `server/app.py:757` call, and the `server/pipeline.py:2837` gate region — overlapping P2 Phases 2-3. **Land P1's retire commit before P2 Phase 2 starts** (P2 Phase 3's manifest-extraction bullet then drops; program row 7 closes via this plan).
- *Promote* (if ever chosen) requires serialization with P2/P3: the manifest must be stamped after all concurrent work has merged (identity binding above), or explicitly re-stamped post-merge.
- P1 ∥ P3 is safe on Phases 0-1 but **not on the retire path**: Phase 2's consumer removal touches `scripts/check_release_metadata.py`, `.github/workflows/ci.yml`, and `justfile` — inside P3 Phase 2's footprint. Rule: land P3 Phase 2 before P1's retire commit, or whichever lands second rebases (program matrix, corrected 2026-08-25).
- **Retire-path landing order across the program**: P2 Phase 0 (tests + accessors) may run anytime → P3 Phase 2 → **P1 retire** → P2 Phase 2 onward. P1's `server/pipeline.py` edits (`:2835-2837` gate region, `:670` plumbing) and P2 Phase 1's epilogue work touch different regions of the same file — whichever lands second rebases; the super plan's invariant 1 ("no new feature plan touches SessionHost before P2 Phases 0-1 merge") governs plans *outside* this program, while inside it this explicit order governs.

**Evidence artifacts are frozen release records — retire deletes machinery,
never the committed artifacts** (2026-08-25 evidence audit): the v0.1.3 files
in `docs/benchmarks/` (phase0-3 evidence, transport contract, promotion
manifest, query-context dry-run/analysis/jsonl) are load-bearing release-gate
records with live consumers beyond the server loader — `config.toml:54` names
the manifest path, `.github/workflows/ci.yml:174` and `justfile:41` re-hash the
phase inputs against it, and `scripts/check_release_metadata.py:440` requires
the versioned manifest path to exist. Renaming/moving the folder is blocked for
v0.1.3: the manifest embeds the input paths, and regenerating it requires
`--source-commit` to match the one stamped in `phase3-completion.json`
(`740b364`), which cannot honestly be produced from a later HEAD. Verdict:
keep all committed artifacts in place; fix the folder-name misnomer forward by
pointing v0.1.4+ artifacts at `docs/evidence/` (program row 21).

## Architecture & Call Flow

```
collectors (run_query_context_experiment / collect_query_context_latency)
      │  [live path BLOCKED: run_live() stub]
      ▼
phase0/phase1/phase2/phase3 evidence artifacts + optional phase4c artifact
      │  (real stratum must appear IN phase0/phase1 records;
      │   phase2 supplies transport_eligible; sha256 of every input pinned)
      ▼
scripts/validate_v013_evidence.py --write-manifest  ── hard-fails on
      │                                                source identity mismatch
      ▼
docs/benchmarks/v0.1.3-promotion-manifest.json
      │
      ▼  (server startup)
server/app.py:757 → load_promotion_manifest (identity + eligibility gates,
      │              fail-closed: source_mismatch / identity_unbound)
      ▼
SessionHost._promotion_eligible (server/pipeline.py:670)
      │
      ▼
late-result disposition (server/pipeline.py:2835-2837):
  if not enable_autoplay_policy → "autoplay"    ← legacy FAIL-OPEN branch: flag
      │                                            False bypasses every gate below;
      │                                            must be removed with the gate
      ▼
  if not _promotion_eligible → commit_display_only   ← the fail-closed guard
```

Retire = remove this entire chain and make the disposition unconditionally
`commit_display_only` (decision: full-chain removal — the gate must not be
deleted without replacing it, or autoplay fails open).

## Requirements

1. **Phase 0 is a no-material-spend feasibility pre-flight**: verify (a) gate wiring — `real_stratum_present` derives from phase0/phase1 records and `transport_eligible` from the phase2 artifact, both currently false-producing; (b) `run_live()` stub status; (c) credential resolution plus at most one live probe call against `("openai", "gpt-4o-search-preview")` to confirm model availability (the single permitted paid call). Committed raw evidence, if any is ever produced later, must be schema-valid per `shared/schemas/v013-query-context-raw.json` only — post-change-analysis-schema validity applies solely to a promote branch's phase4c artifact.
2. Phase 1 is an **escalation checkpoint, not an automatic gate**: the feasibility report goes to the operator. *Invest* (promote) requires an explicit operator decision and spawns a dedicated follow-up plan per Appendix A — this plan then stops. *Retire* proceeds here. A blocked/non-decisive state anywhere (missing credentials, retired model, provider outage) also escalates — it is never auto-mapped to retire; retire is chosen, not defaulted into by an outage.
3. Retire path executes **full-chain removal of the machinery**, fail-closed: the late-result disposition becomes unconditionally `commit_display_only` (autoplay unreachable) with a regression test pinning that; then remove the experiment scripts, both query-context schemas, the phase4c validator path, `load_promotion_manifest`/`PromotionManifest`, the `app.py` startup call, **and the manifest's out-of-server consumers in the same PR** — the ci.yml release-metadata manifest-write step, the `justfile:41` re-hash recipe, `check_release_metadata.py:440`'s manifest-path requirement, and the `config.toml:54` manifest key (removing the loader without its CI step and release check leaves zombie machinery re-stamping a manifest nothing reads) — plus the `PromotionManifest` type's remaining consumers: `server/composition.py:26/:41`, `scripts/eval_common.py:52/:455`, `scripts/smoke_conversation.py:86/:117`, with their tests. The regression test must pin `commit_display_only` under **both** `enable_autoplay_policy` values: the legacy branch at `server/pipeline.py:2835` returns `"autoplay"` outright when the flag is False — a fail-open bypass of every gate — and must be removed with the gate, or retire leaves autoplay reachable. The committed `docs/benchmarks/` v0.1.3 artifacts — manifest and evidence/dry-run/analysis files included — are **kept in place** as frozen release records (see Context; don't delete, don't move).
4. Retire completeness is verified, not assumed: post-deletion `rg query_context` and `rg -i promotion_manifest\|promotion_eligible` sweeps resolve every remaining reference (known consumers beyond the script set: `scripts/eval_common.py:855`, `scripts/evidence_common.py:87/516`, `scripts/verify_eval_candidates.py:401`, `tests/test_perf_metrics.py:1402/1454`); and a check that no `pytest.importorskip`-skipped test files remain (a partial deletion must fail the suite, not silently skip).
5. The decision (either way) is documented with the feasibility evidence, and program provenance rows 20 (and, on retire, 7) are updated in the same PR, plus the P2 plan's Phase 3 manifest-extraction bullet.

## Implementation Checklist

### Phase 0: Feasibility pre-flight (no material paid spend)

**Impl files:** docs/dev_plans/20260824-feature-query-context-promotion.md
**Test files:** tests/test_v013_evidence_validator.py
**Test command:** `uv run pytest -q tests/test_v013_evidence_validator.py`
**Goal:** The three promote blockers are confirmed or refuted against live repo/provider state and recorded in Findings; zero evidence collection, at most one paid probe call.

- [ ] Confirm gate wiring from code: `real_stratum_present` reads phase0/phase1 records only; `transport_eligible` reads the phase2 artifact; record current values of both.
- [ ] Confirm `run_live()` stub status in `scripts/run_query_context_experiment.py`.
- [ ] Resolve credentials and make **exactly one** probe call against `gpt-4o-search-preview`; record availability (model retirement forces either an allowlist policy decision or retire). Evidence contract (the probe is not `run_live()` — that is a stub): record in Findings the exact command run, the credential env var it resolved, and the redacted result (model id + HTTP status only, no response body); confirm `git status --short` shows no new evidence artifacts (the probe collects nothing).
- [ ] Record the promotion thresholds from the v0.1.3 plan verbatim in Findings for the appendix: ≥10% median latency improvement, bootstrap lower bound ≥5%, quality ≥0.90, quality drop ≤0.02, ≥30 paired samples per cell, ≥10 baseline repeats, baseline SD ≤0.01.
- [ ] Write the feasibility verdict (per-blocker status) into this plan's Findings.

### Phase 1: Decision checkpoint (escalation)

**Impl files:** docs/dev_plans/20260824-feature-query-context-promotion.md, docs/dev_plans/20260824-design-v013-followup-program.md
**Test files:** tests/test_v013_evidence_validator.py (no-regression guard only — the decision phase adds no tests)
**Test command:** `uv run pytest -q tests/test_v013_evidence_validator.py`
**Goal:** One explicit operator decision — invest or retire — recorded with the feasibility evidence; nothing auto-decided, blocked states escalate rather than defaulting to retire.

- [ ] Present the Phase 0 feasibility verdict to the operator with the Appendix A invest-scope estimate.
- [ ] If **invest**: record the decision, create the dedicated promote plan per Appendix A, mark this plan's remaining phases N/A, update program row 20 to point at the new plan, **and add the new plan to the program's Subordinate Plans table and parallelization matrix in the same commit** (the program has no standing entry for it; P1 then closes as handed-off). **This plan stops here.**
- [ ] If **retire**: record the decision and rationale; proceed to Phase 2.
- [ ] If **blocked** (credentials absent, model retired, provider outage): stop and escalate — do not proceed to Phase 2 on a blocked verdict without an explicit operator retire decision.

### Phase 2: Retire — fail-closed gate replacement, then full-chain removal

**Impl files:** server/pipeline.py, server/config.py, server/app.py, scripts/run_query_context_experiment.py, scripts/collect_query_context_latency.py, scripts/analyze_query_context_latency.py, scripts/query_context_common.py, shared/schemas/v013-query-context-raw.json, shared/schemas/v013-query-context-post-change-analysis.json, scripts/validate_v013_evidence.py, scripts/check_release_metadata.py, .github/workflows/ci.yml, justfile, config.toml, server/composition.py, scripts/eval_common.py, scripts/smoke_conversation.py
**Test files:** tests/test_pipeline.py, tests/test_session_host.py, tests/test_query_context_latency.py, tests/test_v013_phase4c_manifest.py, tests/test_v013_evidence_validator.py, tests/test_app.py, tests/test_config.py, tests/test_release_metadata.py, tests/test_justfile_ci_parity.py, tests/test_eval_common.py, tests/test_smoke_conversation.py
**Test command:** `uv run pytest -q`
**Validation cmd:** `uv run python scripts/smoke_server.py`
**Goal:** Autoplay is structurally unreachable (unconditional commit_display_only, pinned by a regression test that lands BEFORE any deletion), and the entire manifest/experiment chain is gone with no dead or silently-skipped remnants.

- [ ] **First, before any deletion**: add the regression test pinning that the late-result disposition is `commit_display_only` under **both** `enable_autoplay_policy` values and regardless of manifest state — the legacy branch at `server/pipeline.py:2835` returns `"autoplay"` when the flag is False, bypassing every gate, so a flag=True-only test would pass while autoplay stays reachable. Then replace the whole `server/pipeline.py:2835-2837` gate region (legacy branch included) so the disposition is unconditional.
- [ ] Remove `SessionHost._promotion_eligible` plumbing (`server/pipeline.py:670`), the `server/app.py:757` `load_promotion_manifest` call, and `load_promotion_manifest`/`_load_promotion_manifest`/`PromotionManifest` from `server/config.py` (coordinate with P2: this must land before P2 Phase 2 starts, per the program matrix).
- [ ] Remove the `PromotionManifest` type's remaining consumers: the `server/composition.py:26` import and `:41` parameter, `scripts/eval_common.py:52` import and `:455` parameter, and `scripts/smoke_conversation.py:86/:117` load-and-pass; update their tests (`tests/test_eval_common.py`, `tests/test_smoke_conversation.py`).
- [ ] Decide `scripts/validate_v013_evidence.py`'s residual manifest machinery explicitly: with the CI write step gone, either keep its `--verify-manifest` read path as a historical validator of the frozen committed manifest, or delete the validator's manifest read/write paths entirely — record which in Findings and update `tests/test_v013_evidence_validator.py` to match (the phase4c branches are deleted in either case).
- [ ] Delete the four experiment scripts, both query-context schemas, and the phase4c branches of `scripts/validate_v013_evidence.py` (`--phase4c-input`, `_validate_phase4c_artifact`, `_bind_phase4c_artifact`). **Keep every committed `docs/benchmarks/` artifact in place** — manifest, phase evidence, dry-run/analysis JSONs — as frozen v0.1.3 release records (Requirement 3).
- [ ] Remove the manifest's out-of-server consumers in the same PR: the ci.yml release-metadata manifest-write step, the `justfile:41` re-hash recipe, `check_release_metadata.py:440`'s manifest-path requirement, and the `config.toml:54` manifest key; update `tests/test_release_metadata.py` and `tests/test_justfile_ci_parity.py`. **Coordinate with P3 Phase 2 (same files) — land P3 Phase 2 first, or rebase.**
- [ ] Delete `tests/test_query_context_latency.py` and `tests/test_v013_phase4c_manifest.py`; update `tests/test_v013_evidence_validator.py` for the removed manifest/phase4c paths.
- [ ] Sweep (runtime/dead-code scope): `rg -i "query_context|promotion_manifest|promotion_eligible|phase4c"` across **scripts/, server/, tests/, shared/, web/** — resolve every hit (update the `scripts/eval_common.py:52/:455` import/parameter, `scripts/evidence_common.py`, `scripts/verify_eval_candidates.py:401` helper references; keep the `tests/test_perf_metrics.py` work-item provider/model forwarding tests with reworded rationale — they test live telemetry, not the experiment; the validator hits resolve per the previous bullet's recorded decision). **Exempt from the sweep**: the committed `docs/benchmarks/` artifacts (frozen records) and `docs/dev_plans/`/`CHANGELOG.md` (historical provenance — the program map and this plan legitimately keep naming the removed machinery). `docs/architecture.md`/README are in scope via Phase 3.
- [ ] Verify no `pytest.importorskip`-skipped test files remain (`uv run pytest -q -rs` and inspect skips); full suite + smoke pass.

### Phase 3: Docs + program closure

**Impl files:** CHANGELOG.md, README.md, docs/architecture.md, docs/dev_plans/20260824-design-v013-followup-program.md, docs/dev_plans/20260824-refactor-sessionhost-decomposition.md, docs/dev_plans/README.md
**Test files:** tests/test_release_metadata.py
**Test command:** `uv run pytest -q tests/test_release_metadata.py`
**Goal:** Repo docs and the program map agree with the executed decision; no stale reference to the removed chain anywhere.

- [ ] CHANGELOG "Removed" entry (experiment + promotion-manifest chain) under the next unreleased version.
- [ ] README/architecture.md: remove promotion-manifest/query-context references.
- [ ] Program doc: row 20 `retired: <reason>`; row 7 `fixed <sha>` (machinery deleted rather than extracted); update the parallelization matrix per the corrected Context note; flip this plan's row in the Subordinate Plans table.
- [ ] Create or update `docs/dev_plans/README.md` (the program's exit criterion requires it; the first child plan to complete creates it, later ones update their row).
- [ ] P2 plan: drop Phase 3's "extract promotion-manifest to server/promotion_manifest.py" bullet (superseded by deletion); note the change in P2's Findings.

## Acceptance Criteria

- [ ] Phase 0 feasibility verdict recorded in Findings with per-blocker evidence (code line refs + probe result).
- [ ] An explicit operator decision recorded; blocked states escalated, never auto-retired.
- [ ] On retire: regression test proves `commit_display_only` is unconditional; `rg` sweeps and importorskip check clean; full pytest suite + `scripts/smoke_server.py` pass.
- [ ] Program rows 20 (and 7 on retire) closed; P2 plan updated.
- [ ] On invest: dedicated promote plan exists per Appendix A and this plan's stop is recorded.

## Appendix A: Invest (promote) scope — executed only via a dedicated follow-up plan

If the operator chooses invest at Phase 1, the follow-up plan must cover, at minimum:

1. Implement `run_live()` (wire the responses client) with tests; distinguish raw (`v0.1.3-query-context-raw.jsonl`, per the raw schema's stated path) from normalized output and commit both.
2. Collect real-stratum records `("openai", "gpt-4o-search-preview")` **into the phase0/phase1 evidence artifacts** (the gate's actual inputs; the `("unavailable","unavailable")` stratum must remain present per `PHASE_MINIMUMS`), meeting the sample thresholds recorded in Phase 0 Findings; add a committed-file schema test (mirror `test_the_repo_committed_manifest_verifies_clean`, `tests/test_v013_evidence_validator.py:1074`).
3. Run the named browser/device transport check and regenerate the phase2 artifact `promotion_eligible=true` at the target commit; regenerate the phase3 completion record (`scripts/record_phase3_completion.py`).
4. Produce the phase4c post-change analysis artifact conforming to `v013-query-context-post-change-analysis.json` and pass it via `--phase4c-input`; run `tests/test_v013_phase4c_manifest.py` against it.
5. Ordering: all identity-bearing artifacts regenerated first, docs committed, then `--write-manifest` stamped against the **final** commit as the last change of the PR; serialize with P2/P3 merges or re-stamp post-merge. Evidence appends require rebinding the manifest's pinned sha256 inputs in the same commit.
6. Live smoke with a named observable: force a late-result delivery (extend the `--ack-ordering` smoke, commit `840a360`) and assert the autoplay branch at `server/pipeline.py:2837` is taken (log line / session-record disposition), run at the manifest-bound commit with matching `PIPECAT_SOURCE_COMMIT`/`PIPECAT_SOURCE_TREE_HASH`.
7. Decision gate evaluated against the thresholds verbatim (≥10% median, bootstrap LB ≥5%, quality ≥0.90, drop ≤0.02, ≥30 paired samples/cell, ≥10 baseline repeats, baseline SD ≤0.01), with non-decisive outcomes escalating per Requirement 2.

<!-- reviewed: 2026-08-25 @ dd50a54c29552ef9c13957d9ac801cba3d058811 -->

## Progress

- [ ] Phase 0: Feasibility pre-flight (no material paid spend)
- [ ] Phase 1: Decision checkpoint (escalation)
- [ ] Phase 2: Retire — fail-closed gate replacement, then full-chain removal
- [ ] Phase 3: Docs + program closure

## Findings

### Review Waivers

(none)
