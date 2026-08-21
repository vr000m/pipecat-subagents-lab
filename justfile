# Pipecat Subagents Lab — local dev tasks
# Usage: just <recipe>   (list all with `just --list`)

set shell := ["bash", "-cu"]

# Install Python deps
sync:
    uv sync

# Build the browser bundle (dist/ is gitignored; required before serving web/index.html)
build:
    cd web && bun install && bun run build

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
# sockets up if config.toml still points at provider = "local")
run:
    set -a; source ~/.secrets/ai.env; set +a; uv run python -m server.app

# One-shot: install, build, check, then run
all: sync build check run
