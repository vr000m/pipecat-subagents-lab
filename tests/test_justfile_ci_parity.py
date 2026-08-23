"""Guards the justfile/ci.yml parity invariant.

Round 3 confirming pass, Architecture findings 1-2: a prose comment
("keep both in sync by hand") is not a check, and it demonstrably drifted
twice -- round 10 raised the flag-mismatch risk, round 2's fix restated the
comment, and `just check` still didn't reach `smoke` afterwards. This test
replaces the comment with an executable, directional invariant:

    Every `uv run ...` or `bun ...` command any CI job that can run on
    `pull_request` executes is reachable from `just check`'s transitive
    recipe closure.

Directional on purpose -- CI is the merge gate, so CI subset-of `just check`
is what protects a developer from a green local run and a red CI run. The
reverse is not wanted: the justfile legitimately holds `preflight`, `run`,
and `all`, which CI must never execute.

Originally scoped to only `jobs['test']`. Widened to every job that can run
on `pull_request` (round-5 restart, Architecture finding #7): the
`promotion-manifest-drift` job was split out specifically so it blocks pull
requests (no push-to-main `if:` guard), but a check hardcoded to
`jobs['test']` never saw it, so its `uv run` command had no `just`
equivalent and no test would have caught one going missing.
"""

from __future__ import annotations

import re
import stat
import subprocess
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


_CD_RE = re.compile(r"^cd\s+(\S+)$")


def _qualified_commands(line: str, cwd: str = ".") -> set[tuple[str, str]]:
    """Split ``line`` on ``&&`` and pair each resulting command with the
    working directory it actually runs in.

    A piece matching ``cd <dir>`` updates ``cwd`` for the REST of this line's
    pieces and is not itself emitted (it's a directory change, not a
    command); every other piece is emitted as ``(cwd, piece)``. Threading
    ``cwd`` through here -- instead of only applying ``_split_commands`` --
    closes two holes in the parity check (round-4 restart, Architecture
    Minor #4 / Logic finding #3): a CI ``run: cd web && bun test`` line
    previously produced the single unsplit string ``"cd web && bun test"``,
    which failed the ``_TRACKED_PREFIXES`` filter and was silently DROPPED
    rather than reported missing; and a justfile ``cd web && ...`` command
    compared equal (by bare command text) to an unrelated repo-root command
    of the same name.
    """
    commands: set[tuple[str, str]] = set()
    for piece in _split_commands(line):
        match = _CD_RE.match(piece)
        if match:
            cwd = match.group(1)
            continue
        commands.add((cwd, piece))
    return commands


def _just_check_command_closure() -> set[tuple[str, str]]:
    recipes = _parse_justfile(_JUSTFILE.read_text())
    body_lines = _closure_body_lines(recipes, "check")
    commands: set[tuple[str, str]] = set()
    for line in body_lines:
        commands.update(_qualified_commands(line))
    return commands


def _qualified_commands_from_steps(steps: list[dict]) -> set[tuple[str, str]]:
    """Extracted from ``_ci_test_job_commands`` so a test can feed it a
    synthetic step list instead of only ever exercising the real ci.yml
    (round-4 restart verification, closing a tautological-test gap)."""
    commands: set[tuple[str, str]] = set()
    for step in steps:
        if step.get("name") in _CI_STEPS_WITHOUT_A_JUST_EQUIVALENT:
            continue
        run = step.get("run")
        if not run:
            continue
        cwd = step.get("working-directory", ".")
        for raw in run.splitlines():
            if stripped := raw.strip():
                commands.update(_qualified_commands(stripped, cwd))
    return commands


# A job is excluded only when its `if:` explicitly restricts it to the
# push-to-main event -- i.e. it cannot run on `pull_request` and is
# therefore not a merge gate a developer needs to reproduce locally before
# opening a PR. `release-metadata` is the only such job today
# (`if: github.event_name == 'push' && github.ref == 'refs/heads/main'`);
# everything else -- including jobs added later with no `if:` at all --
# defaults to IN scope, which is the fail-safe direction (round-5 restart,
# Architecture finding #7).
_PUSH_TO_MAIN_ONLY_MARKER = "event_name == 'push'"


def _pull_request_job_names(data: dict) -> list[str]:
    return [
        name
        for name, job in data["jobs"].items()
        if _PUSH_TO_MAIN_ONLY_MARKER not in str(job.get("if", ""))
    ]


def _ci_pull_request_job_commands() -> set[tuple[str, str]]:
    data = yaml.safe_load(_CI_YML.read_text())
    commands: set[tuple[str, str]] = set()
    for name in _pull_request_job_names(data):
        commands.update(_qualified_commands_from_steps(data["jobs"][name]["steps"]))
    return commands


# Follow-up (deliberately deferred, round-4 restart, architecture Minor #3):
# inverting the relationship so CI does `run: just py-check` / `run: just
# smoke` (one list consumed twice) instead of two lists kept equal by a
# parser is the better long-term design, but it changes CI workflow
# structure for a green invariant and is out of proportion to this round.
# Same applies to exempting CI steps by an in-workflow marker instead of by
# human-readable step name (`_CI_STEPS_WITHOUT_A_JUST_EQUIVALENT` above).


def test_ci_pull_request_job_commands_are_reachable_from_just_check() -> None:
    closure = _just_check_command_closure()
    assert closure, "`check`'s recipe closure resolved to zero commands -- parser likely broken"

    tracked = [
        (cwd, cmd)
        for cwd, cmd in _ci_pull_request_job_commands()
        if cmd.startswith(_TRACKED_PREFIXES)
    ]
    assert tracked, (
        "no `uv run`/`bun` commands found in any pull_request-triggered CI job -- "
        "parser likely broken"
    )

    missing = [pair for pair in tracked if pair not in closure]
    assert not missing, (
        "A CI job that runs on pull_request executes commands `just check` cannot reach: "
        f"{missing}. Add them to a recipe `check` depends on."
    )


def test_promotion_manifest_drift_job_is_covered_by_the_parity_check() -> None:
    """Reproduction for round-5 restart, Architecture finding #7: this job
    runs on `pull_request` (no push-to-main `if:` guard) and was invisible
    to the parity check while it was hardcoded to `jobs['test']`."""
    data = yaml.safe_load(_CI_YML.read_text())
    assert "promotion-manifest-drift" in _pull_request_job_names(data)


def test_release_metadata_job_is_excluded_as_push_to_main_only() -> None:
    data = yaml.safe_load(_CI_YML.read_text())
    assert "release-metadata" not in _pull_request_job_names(data)


def test_a_job_with_no_if_defaults_to_in_scope() -> None:
    """The fail-safe direction: a future job added without an `if:` at all
    must not silently fall outside the parity check the way `test` and
    `promotion-manifest-drift` did before this job was hardcoded in."""
    data = {"jobs": {"new-job": {"steps": []}}}
    assert _pull_request_job_names(data) == ["new-job"]


def test_uv_sync_flags_match() -> None:
    """Pins `uv sync --frozen` in both files (round 10's flag-mismatch finding)."""
    recipes = _parse_justfile(_JUSTFILE.read_text())
    sync_body = recipes["sync"][1]
    assert "uv sync --frozen" in sync_body

    assert (".", "uv sync --frozen") in _ci_pull_request_job_commands()


def test_ci_run_lines_are_split_on_ampersands() -> None:
    """Reproduction: pre-fix, a CI step written `run: cd web && bun test`
    yielded the unsplit `"cd web && bun test"`, which `_TRACKED_PREFIXES`'
    `.startswith()` filter dropped instead of reporting missing."""
    assert _qualified_commands("cd web && bun test") == {("web", "bun test")}


def test_working_directory_disambiguates_identical_commands() -> None:
    """Reproduction: pre-fix, `working-directory:` was discarded, so a
    `uv run pytest` step under `working-directory: web` matched the
    justfile's repo-root `uv run pytest` -- a false parity match. Feeds a
    synthetic step through the real extractor rather than hand-building a
    set, so this actually exercises `_qualified_commands_from_steps`."""
    steps = [{"working-directory": "web", "run": "uv run pytest"}]
    commands = _qualified_commands_from_steps(steps)
    assert commands == {("web", "uv run pytest")}
    assert (".", "uv run pytest") not in commands


class TestRunRecipeGuardsItsEnvFile:
    """Round 3 confirming pass, Architecture finding 7.

    `set shell := ["bash", "-cu"]` enables `-u` but not `-e`, and the `run`
    recipe's body was one `;`-separated line, so a failed `source` fell
    through to `uv run python -m server.app` and `just` reported success.
    This exercises the recipe's actual body text (not a hand-copied stand-in)
    through a real `bash -cu`, so a future edit to the guard is caught here
    too.
    """

    @staticmethod
    def _run_recipe_body() -> str:
        recipes = _parse_justfile(_JUSTFILE.read_text())
        assert "run" in recipes, "recipe `run` not found in justfile"
        return "\n".join(recipes["run"][1])

    @staticmethod
    def _stub_path(tmp_path: Path) -> str:
        """A `uv` stub on PATH that touches a marker file instead of booting a server."""
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        uv_stub = bin_dir / "uv"
        uv_stub.write_text(f"#!/bin/bash\ntouch {tmp_path / 'marker'}\n")
        uv_stub.chmod(uv_stub.stat().st_mode | stat.S_IEXEC)
        return str(bin_dir)

    def test_missing_env_file_is_a_hard_stop(self, tmp_path: Path) -> None:
        body = self._run_recipe_body()
        stub_dir = self._stub_path(tmp_path)
        marker = tmp_path / "marker"
        env = {
            "AI_ENV_FILE": str(tmp_path / "does-not-exist.env"),
            "PATH": f"{stub_dir}:/usr/bin:/bin",
            "HOME": str(tmp_path),
        }

        result = subprocess.run(["bash", "-cu", body], env=env, capture_output=True, text=True)

        # returncode alone is not load-bearing under `bash -cu`, which exits
        # with the LAST command's status -- the marker's absence is the real
        # assertion that the server-boot command never ran.
        assert result.returncode != 0
        assert not marker.exists()

    def test_readable_env_file_still_boots(self, tmp_path: Path) -> None:
        body = self._run_recipe_body()
        stub_dir = self._stub_path(tmp_path)
        marker = tmp_path / "marker"
        env_file = tmp_path / "ai.env"
        env_file.write_text("FOO=bar\n")
        env = {
            "AI_ENV_FILE": str(env_file),
            "PATH": f"{stub_dir}:/usr/bin:/bin",
            "HOME": str(tmp_path),
        }

        result = subprocess.run(["bash", "-cu", body], env=env, capture_output=True, text=True)

        assert result.returncode == 0, result.stderr
        assert marker.exists()

    def test_malformed_env_file_stops_the_recipe(self, tmp_path: Path) -> None:
        """The reproduction: round-4 restart, Codex P2. `just` runs each
        non-continued recipe line in its own shell, so line 60's old
        `source "${AI_ENV_FILE:-...}"` -- a SEPARATE shell from the readable
        guard above it -- discarded `source`'s exit status via the following
        `;`. A malformed env file (unterminated quote) previously exited 0
        with the server-boot command still running.
        """
        body = self._run_recipe_body()
        stub_dir = self._stub_path(tmp_path)
        marker = tmp_path / "marker"
        env_file = tmp_path / "ai.env"
        env_file.write_text('FOO="bar\n')  # unterminated quote -- malformed
        env = {
            "AI_ENV_FILE": str(env_file),
            "PATH": f"{stub_dir}:/usr/bin:/bin",
            "HOME": str(tmp_path),
        }

        result = subprocess.run(["bash", "-cu", body], env=env, capture_output=True, text=True)

        assert result.returncode != 0
        assert not marker.exists()

    def test_directory_env_file_stops_the_recipe(self, tmp_path: Path) -> None:
        """A directory passes the readability guard (`test -r` is true for a
        readable directory) but `source` on it fails -- must still be fatal.
        """
        body = self._run_recipe_body()
        stub_dir = self._stub_path(tmp_path)
        marker = tmp_path / "marker"
        env_file = tmp_path / "ai_env_dir"
        env_file.mkdir()
        env = {
            "AI_ENV_FILE": str(env_file),
            "PATH": f"{stub_dir}:/usr/bin:/bin",
            "HOME": str(tmp_path),
        }

        result = subprocess.run(["bash", "-cu", body], env=env, capture_output=True, text=True)

        assert result.returncode != 0
        assert not marker.exists()
