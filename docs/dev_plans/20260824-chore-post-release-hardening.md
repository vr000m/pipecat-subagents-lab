# Task: Post-Release Hardening — Gauntlet Caveats + Eval Minors (P3)

**Status**: Complete — all three phases landed; program provenance rows 13-19 closed
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
deferred Minor findings. Each item is estimated (not measured) at roughly half a
day; Requirement 3 is the likely overrun — its drift-gate design surface has
consumed multiple gauntlet rounds before. Phases run **sequentially within this
plan** (Phases 1 and 3 share `tests/test_config.py`; only Phase 1 edits
`server/config.py`); the plan as a whole is worktree-parallel-safe with P1's
Phases 0-1 and (modulo the guard region of `server/config.py` — see program
matrix) with P2. **P1's retire path (its Phase 2) also touches
`scripts/check_release_metadata.py`, ci.yml, and `justfile`** — this plan's
Phase 2 footprint — so land P3 Phase 2 before P1's retire commit, or whichever
lands second rebases (program matrix, corrected 2026-08-25).

## Context (verified 2026-08-25, P3 plan review)

The original requirement list was drafted against a pre-round-8 snapshot of the
branch. The plan review verified the current state; two requirements' work
already exists and one proposal would reverse a documented decision:

- **Effort-label cross-validation already implemented** (original R5): unknown
  effort-policy labels hard-error at `Config` construction
  (`server/config.py:236-259`, raises `ConfigError`), with both the failure-path
  test (`tests/test_config.py:313`) and the no-false-positive test
  (`test_reasoning_effort_policy_allows_a_subset_of_model_policy_labels`,
  `tests/test_config.py:329`) in place.
- **Reasoning-effort env vars already tested** (original R6): both vars have
  direct tests including the empty-string pin
  (`test_explicitly_empty_effort_override_does_not_clear_the_toml_effort`,
  `tests/test_config.py:2019-2030`; suite at `:1955-2132`), and `README.md:191`
  documents them. The empty-string semantics ("empty means no override") are
  already decided and pinned — re-deciding them would contradict the existing
  test.
- **Eval staleness check is a documented fail-closed decision** (R4's target):
  `scripts/eval_model_comparison.py:762` enforces exact `source_commit`
  equality plus a dirty-tree check over whole-tree attestation
  (`_MANIFEST_ATTESTED_PATHS = ("server/", "scripts/", "evals/")`, `:486-504`),
  chosen after four failed enumerated-input-list rounds each missed a file;
  `--i-know-the-manifest-is-stale` (`:3085`) is the sanctioned override valve.
  Any "staleness = candidate-defining inputs changed" relaxation would
  re-introduce the retired enumerated-list anti-pattern.
- **TTS guard is already mostly registry-driven** (R1's target): the guard
  (`server/config.py:1676-1708`) consumes registry-stored pair members; the
  hand-maintained residue is the hardcoded key-string literals at
  `server/config.py:1689-1690` and `:1705-1706` (registry row at `:1151`), not
  a whole constant.

## Architecture & Call Flow

Four independently-executing surfaces, no shared runtime state:

```
config.toml + env ──> load_config() ── Config.__post_init__
                          │   ├─ effort-label cross-validation (:236-259, exists)
                          │   └─ TTS half-pair guard (:1676-1708)
                          │        └─ reads registry rows (:1151) + key-string
                          │           literals (:1689-1690, :1705-1706)  <- Phase 1
                          v
                      server runtime (unchanged by this plan)

scripts/check_release_metadata.py         tests/test_justfile_ci_parity.py
  argv scan: _verifies_manifest             static YAML parse of ci.yml (:37)
  single-argv co-occurrence (:265-280)      vs justfile recipe intent
  <- Phase 2 (R2)                           <- Phase 2 (R3)

scripts/verify_eval_candidates.py ──writes──> candidates manifest
scripts/eval_model_comparison.py ──reads───> manifest; staleness gate (:762)
  <- Phase 3 (R4 go/no-go only; no code change unless reversal is chosen)
```

The seams are file-level (manifest JSON, ci.yml, config.toml) — no cross-phase
runtime coupling, which is why sequential phases with per-phase test runs are
sufficient.

## Requirements

1. TTS half-pair guard: eliminate the remaining hand-maintained residue — the
   hardcoded key-string literals (`server/config.py:1689-1690`, `:1705-1706`)
   either become registry-derived, or the hand-written form is pinned with a
   parity test that fails when the guard and the registry row (`:1151`)
   disagree. (The guard already reads registry-stored members; this closes the
   literal-string gap, not a rewrite.)
2. `scripts/check_release_metadata.py`'s manifest-verify check: pin
   **fail-closed** on indirect invocation (`VAR=path; --verify-manifest "$VAR"`)
   with a test plus a comment naming the accepted false-negative. Supporting
   indirect invocation is rejected: it would add partial shell-expansion
   semantics (`export`, `${VAR}`, command substitution, …) to a checker whose
   single-argv co-occurrence design (`:265-280`) is deliberate — the same
   structurally-open-ended shape Requirement 3 exists to close. Additionally,
   add a regression test pinning that **direct** `--verify-manifest <path>`
   invocation is still detected — invocation-parsing changes are exactly where
   the existing behavior could silently break.
3. ci.yml drift-gate: close the "structurally open-ended" caveat via one of two
   branches, decided before implementation (Phase 2 decision-gate bullet):
   - **Scoped-parser branch (default)**: explicitly enumerate the YAML-intent
     parser's covered axes in a comment + test at the check
     (`tests/test_justfile_ci_parity.py:37`), documenting uncovered axes as an
     accepted boundary. Stays inside P3's declared footprint. The enumeration
     must **name the axes concretely** (at minimum: command text, flags,
     working directory, env, step presence/order — finalize the list from the
     check's code at implementation), and each covered axis gets a positive
     test plus at least one representative rejected/uncovered form pinned as a
     negative test — a comment plus an always-passing test does not satisfy
     this requirement.
   - **Behavior-assertion branch**: a fixture-commit or observed-CI check. This
     branch touches `.github/workflows/ci.yml` and/or fixtures — outside
     Phase 2's impl files and P3's program-matrix footprint — so choosing it
     requires updating the program matrix **first** (cross-plan invariant 2)
     and explicitly deciding the new dependency direction (an observed-CI
     assertion makes tests depend on live GitHub state; a fixture-commit
     approach edits ci.yml itself). Do not start this branch mid-phase without
     the matrix update landing.
4. Eval manifest-staleness heuristic: **go/no-go decision, not a presumed
   relaxation.** The current exact-equality + dirty-tree check is a documented
   fail-closed decision (see Context) with the override flag as its sanctioned
   noise valve. Either:
   - **Retire** (default expectation): record that the override flag is the
     accepted mechanism and false-positive staleness on unrelated commits is
     the documented cost of whole-tree attestation; no code change.
   - **Reverse**: explicitly record why the fail-closed decision is being
     reversed, define the replacement without re-introducing an enumerated
     input list, and only then pin tests asserting the newly chosen behavior
     (unrelated commit not stale; override flag still honored). Never pin both
     the relaxed and the fail-closed assertions — they are mutually exclusive
     on the same input.
5. Effort-policy label cross-validation: **verify-and-retire.** Confirm the
   existing implementation (`server/config.py:236-259` + tests at
   `tests/test_config.py:313`, `:329`) covers the original eval-suite finding
   (typo → hard error at load, no false positives), and record the retirement
   citation in Findings and the program provenance map. No new validation path
   — a second implementation would duplicate the `__post_init__` mechanism.
6. Reasoning-effort env vars: **verify-and-retire.** Confirm the existing tests
   (`tests/test_config.py:1955-2132`, empty-string pin at `:2019-2030`) cover
   both vars, and verify `README.md`'s env-var documentation against the
   pinned test behavior (the README check is the verification path — if the
   docs have drifted from the tested semantics, fix the README; otherwise no
   edit). Record the retirement citation.
7. Phase-2 eval minors addressed or explicitly retired with reasons:
   policy-dict wholesale-replacement semantics, effort-display-vs-effective-value,
   truncated-cell status reporting.

## Implementation Checklist

### Phase 1: Registry-driven TTS half-pair guard

**Impl files:** server/config.py
**Test files:** tests/test_config.py
**Test command:** `uv run pytest -q tests/test_config.py`
**Goal:** The guard cannot silently drift from the registry — the key-string literals become registry-derived, or a parity test fails when guard and registry disagree.

- [x] Read the guard (`:1676-1708`) and the registry row (`:1151`); pick derivation vs parity-test for the literal residue (smaller correct change wins).
- [x] Implement; add the drift test.
- [x] Land-order note: P2 Phase 3 also edits `server/config.py` — prefer landing this first (program matrix). This is P3's **only** `server/config.py` edit (Phase 3 makes none), so no later same-file race with P2 re-opens.

### Phase 2: Release-metadata + drift-gate closure

**Impl files:** scripts/check_release_metadata.py, tests/test_justfile_ci_parity.py
**Test files:** tests/test_release_metadata.py, tests/test_justfile_ci_parity.py
**Test command:** `uv run pytest -q tests/test_release_metadata.py tests/test_justfile_ci_parity.py`
**Goal:** Both round-9 caveats end as decided boundaries, not open-ended TODOs — each with a test that pins the decision.

- [x] Manifest-verify (Requirement 2): pin fail-closed on indirect invocation — test + comment naming the accepted false-negative; add the direct-invocation-detection regression test.
- [x] Land-order note: P1's retire path later removes the ci.yml manifest-write step, `justfile:41`, and `check_release_metadata.py:429`'s manifest requirement — land this phase first so P1 rebases over pinned, decided behavior rather than racing it.
- [x] Drift-gate decision gate (Requirement 3): choose scoped-parser (default, in-footprint) vs behavior-assertion (requires program-matrix update landing **first**, per invariant 2 — record the decision in Findings before writing code).
- [x] Implement the chosen branch + test; document the covered/uncovered axes where the check lives.

### Phase 3: Eval-suite verification + minors

**Impl files:** scripts/verify_eval_candidates.py, scripts/eval_common.py, scripts/eval_model_comparison.py, README.md, docs/dev_plans/20260824-design-v013-followup-program.md, docs/dev_plans/20260824-chore-post-release-hardening.md, docs/dev_plans/README.md
**Test files:** tests/test_verify_eval_candidates.py, tests/test_eval_common.py, tests/test_eval_model_comparison.py, tests/test_config.py (verification target of R5/R6 citations — no edits expected)
**Test command:** `uv run pytest -q tests/test_verify_eval_candidates.py tests/test_eval_common.py tests/test_eval_model_comparison.py tests/test_config.py`
**Goal:** Requirements 4-7 each end fixed-with-test, verified-and-retired-with-citation, or retired-with-reason; none stays silently deferred. No `server/config.py` edits in this phase (R5/R6 are verification-only; see Requirements).

- [x] Staleness go/no-go (Requirement 4): record retire-or-reverse in Findings first; code + tests only on the reverse branch, per the requirement's constraints.
- [x] Verify-and-retire Requirement 5 (effort-label validation) with citations; update the provenance map row.
- [x] Verify-and-retire Requirement 6 (env-var tests): check README against the pinned test semantics; fix README only if drifted; citations + provenance row.
- [x] Phase-2 minors triage (Requirement 7) — record each disposition in Findings.
- [x] Program closure commit: flip this plan's status header, update the program's Subordinate Plans row and provenance rows 13-19, and create or update `docs/dev_plans/README.md` — all in the same commit, per the program's same-commit rule.

## Acceptance Criteria

- [x] Requirements 1-3 end **implemented with tests** (retirement is not a valid resolution for them; for R3, "implemented" means the chosen branch's assertion + boundary documentation landed).
- [x] Requirement 4 ends with a recorded go/no-go decision — retire-with-reason or reverse-with-tests.
- [x] Requirements 5-6 end **verified-and-retired with citations** to the existing implementation and tests (or, if verification finds a gap, the gap is fixed with a test and the citation updated).
- [x] Requirement 7's three minors each carry a recorded disposition.
- [x] Full pytest suite, `ruff format`/`ruff check` green.
- [x] Program provenance map rows 13-19 closed (fixed or retired per row).

<!-- reviewed: 2026-08-25 @ 034a1fc4ce4512d0bc87f0326e45f4150e09cf7c -->

## Progress

- [x] Phase 1: Registry-driven TTS half-pair guard
- [x] Phase 2: Release-metadata + drift-gate closure
- [x] Phase 3: Eval-suite verification + minors

## Findings

- 2026-08-26 Phase 1: chose **derivation** over parity-test (smaller correct change): the guard now pulls the pair's key names from the registry row's `host_port` member (already stored in `endpoint_resolution`) — literals at old `:1689-1690/:1706/:1710` gone; unpack fails loudly on arity drift; guard skips when no `host_port` member exists. Drift test `test_half_pair_guard_names_the_registry_pair_keys` reads the pair from `_TTS_ENDPOINT_MEMBERS` and matches the error message against those keys. `tests/test_config.py` 174 passed; ruff + mypy clean. Requirement 1 met.
- 2026-08-26 Phase 2, R2: **fail-closed on indirect invocation is the pinned decision.** `_verifies_manifest`'s docstring now names the accepted false-negative (a shell-variable invocation like `--verify-manifest "$MANIFEST"` does not literal-match, so the gate reports the drift step as missing — fails closed, never open) and why no partial shell-expansion interpreter is added. Paired regression test `test_direct_literal_invocation_is_detected_and_variable_indirection_fails_closed` in `tests/test_release_metadata.py` pins both directions on synthetic ci.yml fixtures; existing direct-detection assertions retained. `tests/test_release_metadata.py`: 45 passed, 1 skipped. Requirement 2 met.
- 2026-08-26 Phase 2, R3: **decision recorded before implementation — scoped-parser branch** (the in-footprint default; behavior-assertion would have required a program-matrix update landing first, per invariant 2, for no added coverage). The round-9 "structurally open-ended" caveat closes as a documented boundary: a covered-axes comment in `tests/test_justfile_ci_parity.py` enumerates exactly what the parity check compares (1 command identity via `&&`-split + tracked prefixes, 2 working directory via `working-directory:` + literal `cd`, 3 job scope via the push-to-main marker substring, 4 step exemption by name, 5 justfile recipe closure), and `TestParityCheckScopeBoundaries` pins each uncovered form as a negative test (untracked prefixes invisible; `;` chains unsplit; `cd "$VAR"` literal; `pushd` not a cwd change; compound-`if` over-exclusion; exemption-by-name). Positive coverage per axis already existed (`test_ci_run_lines_are_split_on_ampersands`, `test_working_directory_disambiguates_identical_commands`, `test_a_job_with_no_if_defaults_to_in_scope`, `test_uv_sync_flags_match`, `test_promotion_manifest_drift_job_is_covered_by_the_parity_check`). Main test now shares the extracted `_tracked_only` filter, so the boundary tests exercise the same code path. `tests/test_justfile_ci_parity.py`: 17 passed. Requirement 3 met.
- 2026-08-26 Phase 3, R4: **retired (go/no-go: no-go on relaxation).** The exact-`source_commit`-equality + dirty-tree staleness check (`scripts/eval_model_comparison.py:762`, attested-path set `_MANIFEST_ATTESTED_PATHS` `:486-504`) stays fail-closed by decision: `--i-know-the-manifest-is-stale` (`:3085`) is the sanctioned noise valve, and false-positive staleness after unrelated commits is the documented cost of whole-tree attestation — any narrower heuristic would re-introduce an enumerated input list, the exact shape this program retires elsewhere. No code change.
- 2026-08-26 Phase 3, R5: **verified-and-retired.** `Config.__post_init__` (`server/config.py:236-259`) diffs each effort-policy's label set against its model-policy's and raises `ConfigError` on any leftover — a typo hard-errors at load. Tests: `tests/test_config.py:313` (`test_reasoning_effort_policy_rejects_a_label_not_in_model_policy`) and `:329` (`test_reasoning_effort_policy_allows_a_subset_of_model_policy_labels`, the no-false-positive direction). No new validation path added.
- 2026-08-26 Phase 3, R6: **verified-and-retired, with one citation correction.** Both env vars are tested, but not both inside the `:1955-2132` range this plan cited: `WEBSEARCH_WORKER_REASONING_EFFORT` is covered there (`:1961` effort-only override, `:2019-2030` empty-string pin — an explicit empty override does NOT clear the inherited TOML effort), while `WEBSEARCH_ROUTER_REASONING_EFFORT`'s coverage lives at `:376` (`test_websearch_router_reasoning_effort_env_applies_to_the_registered_label`). `README.md:191-198` matches the pinned semantics (override behavior, unset-preserves, model-only-override clears inherited effort) — no README edit needed.
- 2026-08-26 Phase 3, R7 triage: (a) **policy-dict wholesale replacement — retired as intended design**: `_per_run_config()` (`scripts/eval_model_comparison.py:1619-1653`) deliberately builds a single-entry policy per run so each per-run `Config` carries exactly the candidate under test; no caller relies on merged base-config policy labels. (b) **effort-display-vs-effective-value — residual gap fixed with test**: preview and missing-candidate paths already printed `effective_effort_for_manifest_lookup()`, but `_shipped_config_cells_annotation` still recorded the raw declared effort while matching cells on `candidate_wire_key()` (effective) — the baseline router annotated `@None` for cells matched at `minimal`. Fixed to record the effective value; regression test `test_annotation_records_the_effective_effort_not_the_raw_declared_value`. (c) **truncated-cell status reporting — retired as not-reproducible**: the finding text was never persisted beyond its five-word deferral label; searched both current code and the Phase-2-era runner (`d2a3f91`) for any cell/status truncation (slicing, width formatting) — none exists; `print_report_summary`/`build_report` emit full status/error/reason strings. Re-raise with a concrete location if the original concern resurfaces.
- 2026-08-26 Codex adversarial review (post-PR): two findings, both fail-**open** gaps the pinned fail-closed decisions never sanctioned; both fixed. (a) **R2 predicate accepted mention as execution** (high): `uv run echo <verifier> --verify-manifest <path>` passed `_verifies_manifest` — the reporting-command denylist + anywhere-in-argv co-occurrence never required the verifier to be the program actually invoked. Replaced with a positional allowlist: assignment/wrapper stepping shared with `_command_name` via extracted `_program_index`, an optional `uv run [python[3]]`/bare-`python` runner prefix, the verifier required in program position, and `expected` required immediately after the flag. The fail-closed-on-indirection decision is unchanged. Unit-test matrix: 5 rejected mention-forms, 3 accepted execution-forms. (b) **R3 boundary was silent, not loud** (medium): a tracked command hidden behind unmodeled `;`/`|` syntax (`echo setup; uv run pytest`) vanished from the parity comparison. The scoped-parser decision stands; the boundary now fails closed — `_hidden_tracked_commands` flags any piece containing a tracked prefix at a non-start position, asserted empty in the main parity test; the `;`-chain boundary test now pins loud detection, and a genuinely untracked chain (`echo a; echo b`) stays invisible by design.
