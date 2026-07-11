# Repository Instructions

## Scope

- Keep the Python runtime close to Pipecat-native workers, pipelines, transports, frames, and RTVI patterns.
- Keep browser clients Bun-based and use plain JavaScript unless a framework is explicitly approved; defer Electron packaging to a separately reviewed plan.
- Treat Python-to-client RTVI message shapes as versioned contracts and document them under `shared/`.
- Never commit API keys, credentials, secret files, or values loaded from `~/.secrets/ai.env`.

## Workflow

- Work on feature branches; never commit directly to `main`.
- Plan and review multi-component changes before implementation.
- Use `uv` for Python dependency management and commands.
- Use Bun for browser or future desktop dependency management and commands.
- Stage explicit paths only; never use `git add .`, `git add -A`, or `git add --all`.
- Run formatting, linting, tests, and a secret scan before pushing.
