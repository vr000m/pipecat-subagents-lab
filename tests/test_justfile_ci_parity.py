"""Guards the justfile/ci.yml parity invariant.

Round 3 confirming pass, Architecture findings 1-2: a prose comment
("keep both in sync by hand") is not a check, and it demonstrably drifted
twice -- round 10 raised the flag-mismatch risk, round 2's fix restated the
comment, and `just check` still didn't reach `smoke` afterwards. This test
replaces the comment with an executable, directional invariant:

    Every `uv run ...` or `bun ...` command CI's `test` job executes is
    reachable from `just check`'s transitive recipe closure.

Directional on purpose -- CI is the merge gate, so CI subset-of `just check`
is what protects a developer from a green local run and a red CI run. The
reverse is not wanted: the justfile legitimately holds `preflight`, `run`,
and `all`, which CI must never execute.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

_REPO = Path(__file__).resolve().parents[1]
_JUSTFILE = _REPO / "justfile"
_CI_YML = _REPO / ".github" / "workflows" / "ci.yml"

# Steps with no local shell equivalent: pinned composite actions and the
# Docker-daemon-dependent secret scan. Everything else must be reachable
# from `just check`.
_CI_STEPS_WITHOUT_A_JUST_EQUIVALENT = frozenset({"Scan repository history for secrets"})
_TRACKED_PREFIXES = ("uv run", "bun ")

_RECIPE_HEADER_RE = re.compile(r"^([a-z][a-z-]*):(.*)$")


def _parse_justfile(text: str) -> dict[str, tuple[list[str], list[str]]]:
    """Parse `{recipe_name: (deps, body_lines)}` out of a justfile.

    Recipe headers are `^([a-z][a-z-]*):(.*)$` at column 0; everything
    indented that follows is a body line. `set shell := [...]` never matches
    the header regex (it has a space before its `:=`), so it is skipped for
    free, same as blank lines and comments.
    """
    recipes: dict[str, tuple[list[str], list[str]]] = {}
    current: str | None = None
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if line.startswith((" ", "\t")):
            if current is not None:
                recipes[current][1].append(line.strip())
            continue
        match = _RECIPE_HEADER_RE.match(line)
        if match:
            name = match.group(1)
            deps_part = match.group(2).strip()
            recipes[name] = (deps_part.split() if deps_part else [], [])
            current = name
        else:
            current = None
    return recipes


def _closure_body_lines(recipes: dict[str, tuple[list[str], list[str]]], start: str) -> list[str]:
    """DFS over `start`'s deps, collecting every reachable body line."""
    assert start in recipes, f"recipe {start!r} not found in justfile"
    seen: set[str] = set()
    body_lines: list[str] = []
    stack = [start]
    while stack:
        name = stack.pop()
        if name in seen:
            continue
        seen.add(name)
        if name not in recipes:
            continue
        deps, body = recipes[name]
        body_lines.extend(body)
        stack.extend(deps)
    return body_lines


def _split_commands(line: str) -> list[str]:
    """Split a `&&`-chained body line into its individual sub-commands.

    `cd web && bun install --frozen-lockfile && bun run build` becomes three
    pieces so it compares equal against CI's `working-directory: web` +
    separate `run:` lines for the same commands.
    """
    return [piece.strip() for piece in line.split("&&") if piece.strip()]


def _just_check_command_closure() -> set[str]:
    recipes = _parse_justfile(_JUSTFILE.read_text())
    body_lines = _closure_body_lines(recipes, "check")
    commands: set[str] = set()
    for line in body_lines:
        commands.update(_split_commands(line))
    return commands


def _ci_test_job_run_lines() -> list[str]:
    data = yaml.safe_load(_CI_YML.read_text())
    steps = data["jobs"]["test"]["steps"]
    lines: list[str] = []
    for step in steps:
        if step.get("name") in _CI_STEPS_WITHOUT_A_JUST_EQUIVALENT:
            continue
        run = step.get("run")
        if not run:
            continue
        lines.extend(stripped for raw in run.splitlines() if (stripped := raw.strip()))
    return lines


def test_ci_test_job_commands_are_reachable_from_just_check() -> None:
    closure = _just_check_command_closure()
    assert closure, "`check`'s recipe closure resolved to zero commands -- parser likely broken"

    tracked = [line for line in _ci_test_job_run_lines() if line.startswith(_TRACKED_PREFIXES)]
    assert tracked, "no `uv run`/`bun` commands found in ci.yml's test job -- parser likely broken"

    missing = [line for line in tracked if line not in closure]
    assert not missing, (
        "CI's `test` job runs commands `just check` cannot reach: "
        f"{missing}. Add them to a recipe `check` depends on."
    )


def test_uv_sync_flags_match() -> None:
    """Pins `uv sync --frozen` in both files (round 10's flag-mismatch finding)."""
    recipes = _parse_justfile(_JUSTFILE.read_text())
    sync_body = recipes["sync"][1]
    assert "uv sync --frozen" in sync_body

    ci_lines = _ci_test_job_run_lines()
    assert "uv sync --frozen" in ci_lines
