# Router/Worker Model Shortlist — 2026-08-20

Source plan: `docs/dev_plans/20260817-feature-router-worker-model-eval-suite.md`
Mechanism: `scripts/eval_model_comparison.py --repeat 3` (default one-at-a-time
sweep, majority-vote aggregation across 3 live repetitions per cell — see that
plan's Findings for why `--repeat` replaced single-sample runs as the primary
comparison method).

## Runs

1. **2026-08-20 20:14 UTC** — 5-pair sweep (baseline×baseline, `luna-high`,
   `terra-low`, `terra-medium`, `sol-low`), `--repeat 3`.
   Report: `.review-plan/eval-reports/eval-report-20260820T201442Z-3971f183.json`.
   `overall: PASS` — every cell, every repeat, clean (judge=yes,
   citations_pass=True, worker_presence_pass=True throughout).
2. **2026-08-20 20:54 UTC** — 6-pair sweep, same as above plus `luna-medium`
   (added after run 1 because `luna-high` underperformed `terra-low` on
   latency despite its higher reasoning effort — hypothesis: effort=high was
   costing pure latency without a quality payoff at `medium`).
   Report: `.review-plan/eval-reports/eval-report-20260820T205426Z-604f3b7f.json`.
   `overall: FAIL` — one failure: `baseline/worker=sol-low`'s
   `single-turn-default` turn majority-voted `judge=no` (1/3 yes). All 3
   repeats agreed on the core fact (`v1.7.0`); the 2 "no" votes objected to
   hallucinated *ancillary* metadata (a mismatched PyPI URL in one repeat, an
   impossible future date in another) — the same known judge-noise pattern
   recorded earlier in the parent plan's Findings, not a new failure mode.

Both runs used a manifest freshly re-verified against the commit that
produced the run (`scripts/verify_eval_candidates.py`, 12/12 then 13/13
probes accepted).

## Results — router candidates (worker held at baseline `gpt-5`)

Latency is the mean `total_ms` across the 3 repeats, from run 2 (includes
`luna-medium`); all values are `judge=yes` / no semantic failures.

| Router candidate | single-turn-default | weather turns (avg) | greeting (routing-only) |
|---|---|---|---|
| baseline (`gpt-5-mini`@minimal) | 34,281 ms | 30,439 ms | 1,896 ms |
| `luna-high` (`gpt-5.6-luna`@high) | 39,940 ms | 29,242 ms | 2,749 ms |
| **`luna-medium` (`gpt-5.6-luna`@medium)** | **27,255 ms** | **21,745 ms** | **1,437 ms** |
| `terra-low` (`gpt-5.6-terra`@low) | 27,746 ms | 28,235 ms | 1,703 ms |

`luna-medium` wins on all 3 metrics — it is the fastest router candidate
overall, beating even `terra-low`. This confirms the hypothesis: `luna` at
`effort=high` was paying a real latency cost for reasoning depth the router's
classification task doesn't need; `effort=medium` recovers that cost while
keeping (in fact improving on run 1's numbers for `terra-low`) comparable or
better latency. `luna-high` is *worse* than baseline on `single-turn-default`
latency — effort=high on this task is a net loss.

## Results — worker candidates (router held at baseline `gpt-5-mini`)

| Worker candidate | single-turn-default | weather turns (avg) | Semantic result |
|---|---|---|---|
| baseline (`gpt-5`, no reasoning param) | 34,281–35,047 ms | 27,527–30,439 ms | clean, both runs |
| **`terra-medium` (`gpt-5.6-terra`@medium)** | **6,713–7,369 ms** | **6,020–7,219 ms** | clean, both runs |
| `sol-low` (`gpt-5.6-sol`@low) | 16,406–17,260 ms | 7,734–8,679 ms | **flaky**: run 1 clean, run 2 majority `judge=no` (hallucinated ancillary metadata; core fact correct in all 3 repeats) |

`terra-medium` is consistently ~5x faster than baseline with zero semantic
failures across both runs (6/6 turn-checks clean). `sol-low` is faster than
baseline (~2x) but has now shown a semantic miss under repeat-averaging —
one data point, but enough to deprioritize it below `terra-medium`.

## Shortlist recommendation

- **Router: `luna-medium`** — `gpt-5.6-luna` @ `effort=medium`. Best or
  tied-best on every latency metric measured; no semantic issues observed.
- **Worker: `terra-medium`** — `gpt-5.6-terra` @ `effort=medium`. ~5x faster
  than the current baseline with a clean semantic record across both repeat
  runs; the clear standout of every worker candidate tried.
- **Not shortlisted**: `luna-high` (worse than baseline on latency),
  `terra-low` (solid but not better than `luna-medium` on any axis measured),
  `sol-low` (faster than baseline but showed a semantic miss).

## To apply as the new default

No code change required — `server/config.py` already exposes this via
environment variables (`WEBSEARCH_ROUTER_MODEL`/`WEBSEARCH_WORKER_MODEL` for
the model, `WEBSEARCH_ROUTER_REASONING_EFFORT`/`WEBSEARCH_WORKER_REASONING_EFFORT`
for effort):

```bash
WEBSEARCH_ROUTER_MODEL=gpt-5.6-luna
WEBSEARCH_ROUTER_REASONING_EFFORT=medium
WEBSEARCH_WORKER_MODEL=gpt-5.6-terra
WEBSEARCH_WORKER_REASONING_EFFORT=medium
```

These override `config.toml`'s `[models]` defaults (`router_model =
"gpt-5-mini"`, `worker_model = "gpt-5"`) without touching the repo's
checked-in non-secret defaults — set them in the deployment environment
(or `~/.secrets/ai.env`-style env file for local runs).

`config.toml`'s `[models]` section has no TOML key for reasoning effort today
(only the model ID); effort is env-var-only. If this shortlist is adopted
as the checked-in default rather than an env override, `config.toml`'s
`[models]` section would need a `router_reasoning_effort`/
`worker_reasoning_effort` key added (mirroring the existing `router_model`/
`worker_model` TOML→env mapping in `server/config.py`'s `_load_toml_values`)
— not yet done, since this shortlist hasn't been confirmed as final.

## Caveats before locking this in

- n=3 per cell. This is enough to wash out the single-sample judge-noise seen
  in earlier full-matrix runs, but is not a large sample — `luna-medium`'s
  win margin over `terra-low` (single-turn: 27,255 vs 27,746 ms) is within
  plausible run-to-run variance at this sample size.
- `--full-matrix` (or a targeted single-pair repeat run) on the
  `luna-medium` × `terra-medium` pair specifically has not yet been run —
  per the parent plan's Findings, the router cannot influence worker output,
  so this is a "confirm they behave the same together as apart" check, not
  expected to change the pairwise recommendation.
- `sol-low`'s one semantic failure is a single data point; a `--repeat 5`+ run
  on just that cell would sharpen whether it's a real quality gap or noise
  before ruling it out permanently (currently moot since `terra-medium`
  already wins on both latency and reliability).
