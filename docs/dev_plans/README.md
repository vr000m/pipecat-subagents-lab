# Dev Plans Index

One row per plan; the linked plan's `**Status**:` header is canonical — this
index derives from it. Update this file in the same commit that flips a plan's
status (the v0.1.3 follow-up program's same-commit rule).

## In Progress / Planned

| Plan | Status |
|------|--------|
| [v0.1.3 follow-up program (super plan)](20260824-design-v013-followup-program.md) | Active — program tracking; provenance rows 13-20 closed, rows 1-6/8-12 open (P2, Not Started), row 7 fixed (P1 retire), row 21 deferred to v0.1.4 |
| [P2 — SessionHost decomposition](20260824-refactor-sessionhost-decomposition.md) | Not Started |

## Complete

| Plan | Status |
|------|--------|
| [P1 — Query-context promotion decision](20260824-feature-query-context-promotion.md) | Complete — retired (operator decision); full-chain removal `c67da7f`; program rows 7 and 20 closed |
| [P3 — Post-release hardening](20260824-chore-post-release-hardening.md) | Complete — program rows 13-19 closed |
| [Router/worker model eval suite](20260817-feature-router-worker-model-eval-suite.md) | Complete |
| [Early-ack background delivery (v0.1.3)](20260728-feature-early-ack-background-delivery-v0.1.3.md) | Complete through review-gauntlet terminal status; Phase 4 unpromoted (see plan's caveats section) |
| [Transport-aware speech supersession](20260728-bug-transport-aware-speech-supersession.md) | Complete |
| [Latency observability](20260727-feature-latency-observability.md) | Complete (PR #3) |
| [Websearch subagent (Electron)](20260711-feature-websearch-subagent-electron.md) | Complete |
