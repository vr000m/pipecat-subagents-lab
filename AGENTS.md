# Repository Instructions

## Scope

- Keep the Python runtime close to Pipecat-native workers, pipelines, transports, frames, and RTVI patterns.
- Keep browser clients Bun-based and use plain JavaScript unless a framework is explicitly approved; defer Electron packaging to a separately reviewed plan.
- Treat Python-to-client RTVI message shapes as versioned contracts and document them under `shared/`.
- Never commit API keys, credentials, secret files, or values loaded from `~/.secrets/ai.env`.
- `server/pipeline.py` holds `SessionHost` and connection/turn orchestration
  only; extracted collaborators (connection pipeline, turn-ack ledger, turn
  epilogue, fire-and-forget task retention, speech scheduling/lifecycle,
  observers, the coordinator boundary, and more) live in their own modules —
  see `docs/architecture.md`'s "`server/` module layout" for the current set.
  Reach a collaborator directly rather than adding a new pass-through
  forwarder on `SessionHost`.

## Workflow

- Work on feature branches; never commit directly to `main`.
- Plan and review multi-component changes before implementation.
- Use `uv` for Python dependency management and commands.
- Use Bun for browser or future desktop dependency management and commands.
- Stage explicit paths only; never use `git add .`, `git add -A`, or `git add --all`.
- Run formatting, linting, tests, and a secret scan before pushing.
- `just check` is the recommended local entrypoint (lint, types, tests, web
  build, web tests, and a real-process smoke in one command); see
  `README.md`'s "Verification commands" section for the underlying `uv`/`bun`
  commands it wraps.

## Review-gauntlet round labels

Code comments and dev-plan status logs cite "round N" (e.g. "round 10
gauntlet, Logic finding 3") to attribute a fix to the review pass that found
it. These labels are **per-session**, not a single global index: multiple
independent review-gauntlet sessions have run against this branch over its
history, each restarting its own round count from 1. Two comments citing
"round 3" may therefore refer to different sessions' round 3, with different
findings and different commits — do not assume a shared timeline across
comments, and do not renumber existing citations to force one (round 10
gauntlet, Architecture finding 8: the citations are accurate to the session
that wrote them, and renumbering would falsify that attribution). When
writing a new comment, cite the round it was written in and leave earlier
citations untouched.

## Router/worker model evaluation

`scripts/verify_eval_candidates.py` (candidate verification, writes a
manifest) then `scripts/eval_model_comparison.py` (paid comparison runner)
are the eval suite; see `README.md`'s "Scripts" section for flags and the
one-at-a-time-sweep vs `--full-matrix` rationale. Two conventions worth
knowing before touching this code:
- Model/reasoning-effort resolution follows a 3-layer precedence
  (`config.toml` → env file → process env, see `README.md`'s env var section
  for the exact override semantics) — overriding a role's model at a
  higher-precedence layer clears that role's inherited effort default rather
  than silently carrying it forward.
- `scripts/eval_common.py`'s `ROUTER_BASELINE`/`WORKER_BASELINE` name a fixed
  historical comparison anchor, not "config.toml's live default" — the two
  can diverge (see `tests/test_eval_common.py::TestShippedConfigHasAnEvalCandidateCell`).
