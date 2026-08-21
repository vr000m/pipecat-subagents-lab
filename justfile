# Pipecat Subagents Lab — local dev tasks
# Usage: just <recipe>   (list all with `just --list`)
#
# Mirrors .github/workflows/ci.yml's command list (sync/build/lint/test
# flags kept identical on purpose -- round-10 gauntlet confirming pass,
# Architecture finding: a flag mismatch here let a lockfile-desyncing
# dependency change pass `just check` locally while CI's `--frozen`
# sync/install would reject it). Neither file consumes the other;
# tests/test_justfile_ci_parity.py asserts CI's `test` job cannot run a
# command `just check` does not reach.

set shell := ["bash", "-cu"]

# Install Python deps
sync:
    uv sync --frozen

# Build the browser bundle (dist/ is gitignored; required before serving web/index.html)
build:
    cd web && bun install --frozen-lockfile && bun run build

# Credential-free Python gates: formatting, lint, types, tests
py-check:
    uv run ruff format --check .
    uv run ruff check .
    uv run mypy
    uv run pytest

# Web-side checks
web-test:
    cd web && bun test && bun run lint

# Full compile+test: python checks + web build + web checks + real-process smoke
check: py-check build web-test smoke

# Values-redacted provider preflight (same probe backing /api/readyz)
preflight:
    uv run python -m server.preflight

# Credential-free process smoke: starts real FastAPI/Uvicorn on an ephemeral port,
# verifies liveness, assets, session handshake, clean shutdown.
smoke:
    uv run python scripts/smoke_server.py

# Run the server for real (loads secrets, requires web/dist built and local STT/TTS
# sockets up if config.toml still points at provider = "local"). AI_ENV_FILE
# overrides the default path -- round-10 gauntlet confirming pass, Architecture
# +Security findings: a hardcoded ~/.secrets/ai.env baked one developer's
# machine layout into a repo-tracked file. Only the path half of that
# finding was fixed. `set -a` + `source` remains a deliberate, accepted
# tradeoff for a local-developer recipe: it executes the env file as bash
# and exports EVERY variable in it, not just the keys the server reads.
# Re-examined and accepted at round 3's security gate (AI_ENV_FILE and the
# file it names are developer-controlled; there is no attacker-controlled
# input path). Do not read this recipe as a least-privilege boundary --
# keep unrelated credentials out of the file it points at. A failed `source`
# (malformed file, or a directory) is fatal, not silently ignored -- round-4
# restart, Codex P2: `just` runs each non-continued line in its own shell, so
# a guard on one line does not protect a `source` on the next.
run:
    env_file="${AI_ENV_FILE:-$HOME/.secrets/ai.env}"; \
      test -r "$env_file" || { echo "just run: env file not readable: $env_file (set AI_ENV_FILE to override)" >&2; exit 1; }; \
      set -a; \
      source "$env_file" || { echo "just run: failed to source env file: $env_file (malformed, or a directory?)" >&2; exit 1; }; \
      set +a; \
      uv run python -m server.app

# One-shot: install, check (which builds as a dependency), then run
all: sync check run
