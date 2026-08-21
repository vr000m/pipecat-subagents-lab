# Pipecat Subagents Lab — local dev tasks
# Usage: just <recipe>   (list all with `just --list`)
#
# Mirrors .github/workflows/ci.yml's command list (sync/build/lint/test
# flags kept identical on purpose -- round-10 gauntlet confirming pass,
# Architecture finding: a flag mismatch here let a lockfile-desyncing
# dependency change pass `just check` locally while CI's `--frozen`
# sync/install would reject it). Neither file consumes the other; keep
# both in sync by hand if either's command list changes.

set shell := ["bash", "-cu"]

# Install Python deps
sync:
    uv sync --frozen

# Build the browser bundle (dist/ is gitignored; required before serving web/index.html)
build:
    cd web && bun install --frozen-lockfile && bun run build

# Credential-free checks: formatting, lint, types, python tests
test:
    uv run ruff format --check .
    uv run ruff check .
    uv run mypy
    uv run pytest

# Web-side checks
web-test:
    cd web && bun test && bun run lint

# Full compile+test: python checks + web build + web checks
check: test build web-test

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
# machine layout into a repo-tracked file and exported the whole file into the
# process env rather than just what the server needs.
run:
    set -a; source "${AI_ENV_FILE:-$HOME/.secrets/ai.env}"; set +a; uv run python -m server.app

# One-shot: install, check (which builds as a dependency), then run
all: sync check run
