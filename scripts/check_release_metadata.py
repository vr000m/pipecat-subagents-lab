#!/usr/bin/env python3
"""Release-finalization check: ``pyproject.toml`` and ``CHANGELOG.md`` agree.

See the v0.1.3 dev plan's Testing Notes: "Release finalization is a separate
post-Phase-4 task: first implement and test ``scripts/check_release_metadata.py``,
run it against the pre-release tree, then update ``CHANGELOG.md`` and
``pyproject.toml`` for 0.1.3." This script is intentionally version-agnostic:
it reads the canonical version from ``pyproject.toml`` and requires
``CHANGELOG.md`` to carry a matching, non-``Unreleased``, non-empty dated
release section as its first release heading -- so it also gates future
releases, not only 0.1.3.
"""

from __future__ import annotations

import argparse
import re
import shlex
import sys
import tomllib
from pathlib import Path

import yaml

from scripts.evidence_common import REPO_ROOT, EvidenceGateError, read_bytes_no_follow

#: The CI job whose step command this script pins to the current release
#: version. Named once: the error messages and the lookup must not drift.
_MANIFEST_DRIFT_JOB = "promotion-manifest-drift"

# The date group requires an actual `YYYY-MM-DD` token, not merely
# `(\S+)`: this repo's own CHANGELOG.md convention (see every dated heading
# below `[Unreleased]`) always carries a real date, never a placeholder like
# "TBD" -- and `(\S+)` would let `## [0.1.3] - TBD` pass a gate whose error
# message promises a YYYY-MM-DD date.
CHANGELOG_HEADING_RE = re.compile(r"(?m)^##\s*\[([^\]]+)\]\s*(?:-\s*(\d{4}-\d{2}-\d{2}))?\s*$")


class ReleaseMetadataError(ValueError):
    """Raised when pyproject.toml and CHANGELOG.md disagree or are malformed."""


def read_pyproject_version(pyproject_path: Path) -> str:
    # Hardened read like every other evidence read on this branch -- the sweep
    # that migrated the CHANGELOG/scripts/ci.yml reads skipped this one
    # (round 7 confirm pass 4, Architecture Minor).
    try:
        data = tomllib.loads(read_bytes_no_follow(pyproject_path).decode("utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise ReleaseMetadataError(f"{pyproject_path}: invalid TOML: {exc}") from exc
    except UnicodeDecodeError as exc:
        raise ReleaseMetadataError(f"{pyproject_path}: not valid UTF-8: {exc}") from exc
    project = data.get("project")
    version = project.get("version") if isinstance(project, dict) else None
    if not isinstance(version, str) or not version:
        raise ReleaseMetadataError(f"{pyproject_path}: no `[project] version` string found")
    return version


def read_changelog_release(changelog_path: Path) -> tuple[str, str | None]:
    """Return (version, date) for the first non-Unreleased CHANGELOG heading."""
    # Hardened read, like every other evidence read on this branch: a
    # predictable repo-relative path that a plain `read_text` would follow
    # through a symlink or block on against a FIFO.
    try:
        text = read_bytes_no_follow(changelog_path).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReleaseMetadataError(f"{changelog_path}: not valid UTF-8: {exc}") from exc
    headings = CHANGELOG_HEADING_RE.findall(text)
    if not headings:
        raise ReleaseMetadataError(f"{changelog_path}: no `## [version]` headings found")
    for name, date in headings:
        if name.strip().lower() == "unreleased":
            continue
        return name.strip(), (date.strip() if date else None)
    raise ReleaseMetadataError(
        f"{changelog_path}: only an [Unreleased] heading is present; no dated release section"
    )


def check_no_hardcoded_version_literals(scripts_dir: Path, version: str) -> None:
    """Assert no ``scripts/*.py`` file hand-maintains its own copy of ``version``.

    ``server/config.py``'s ``_installed_release_version()`` and
    ``pyproject.toml`` are the two places the release version is allowed to
    live; a third hardcoded copy (as ``validate_v013_evidence.py``'s
    ``write_manifest`` once carried) would silently emit a stale version on
    a version bump instead of failing loudly. A quoted string literal
    matching the current version anywhere under ``scripts/`` is treated as
    exactly that hand-maintained copy.
    """
    pattern = re.compile(rf'["\']{re.escape(version)}["\']')
    offending: list[str] = []
    for path in sorted(scripts_dir.glob("*.py")):
        try:
            source = read_bytes_no_follow(path).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ReleaseMetadataError(f"{path}: not valid UTF-8: {exc}") from exc
        if pattern.search(source):
            offending.append(str(path))
    if offending:
        raise ReleaseMetadataError(
            f"scripts/ hardcodes release version {version!r} in {offending} -- derive it from "
            "pyproject.toml/server.config instead of duplicating the literal"
        )


def _shell_words(command: str) -> set[str]:
    """The comment-stripped shell words of a workflow ``run:`` script.

    Tokenising rather than substring-matching is what stops a ``#`` comment or
    a quoted ``echo`` message from satisfying the manifest-path gate: shlex
    drops comments outright, and a path mentioned inside a quoted sentence
    comes back as one long word that cannot equal the path.

    A ``run:`` script that shlex cannot tokenise (an unbalanced quote, which
    is a shell syntax error anyway) contributes no words rather than raising,
    so one malformed step cannot mask a well-formed sibling that does
    reference the manifest.
    """
    words: set[str] = set()
    for line in command.splitlines():
        try:
            words.update(shlex.split(line, comments=True))
        except ValueError:
            continue
    return words


def check_ci_promotion_manifest_path_matches_version(ci_yml_path: Path, version: str) -> None:
    """Assert ``.github/workflows/ci.yml``'s ``promotion-manifest-drift`` job
    still points at the manifest for the CURRENT release version.

    ``check_no_hardcoded_version_literals`` above forbids scripts/ from
    hand-maintaining a copy of the version at all, because scripts/ can
    always derive it dynamically instead. ci.yml cannot: the manifest path
    (``docs/benchmarks/v{version}-promotion-manifest.json``) names a real,
    checked-in file, so a literal is the only option and that scan is scoped
    to ``scripts/*.py`` and never looked at ci.yml (round-5 restart,
    Architecture finding #11). What this checks instead is that the literal
    stays in sync with the version pyproject.toml/CHANGELOG.md just agreed
    on: on a version bump that renames the committed manifest but forgets to
    update ci.yml (or vice versa), this fails loudly instead of ci.yml
    silently keeping the previous release's manifest green.

    The workflow is PARSED, and the expected path looked for in the
    ``promotion-manifest-drift`` job's own step commands. A whole-file
    substring test was satisfied by the path appearing anywhere at all --
    a ``#`` comment, a ``name:`` field, a disabled job -- while the step that
    actually runs pointed at the previous release's manifest, which is
    strictly weaker than this docstring's claim. A missing job now fails
    loudly for the same reason (round 6 confirm pass 3, Security Minor).

    Parsing alone did not close two of those three bypasses, though, so this
    also (round 7 confirm pass 4, Security Minor):

    * rejects a job or matching step carrying an ``if:``. ``if: false`` (or
      any never-true expression) leaves the job declared and this gate green
      while the drift check never executes -- exactly the "disabled job"
      evasion the paragraph above claims to have closed.
    * matches the path as a whole SHELL WORD of a comment-stripped command,
      not as a free substring of one. ``run: echo "regenerate
      docs/benchmarks/...json when bumping"`` satisfied the substring form
      while the step that actually diffs pointed at the previous release's
      manifest.

    Not checked, deliberately: the workflow's ``on:`` triggers. Which events
    run CI is a routine, legitimately-varying choice (this repo's gates run on
    push and pull_request; a fork may reasonably differ), so pinning it here
    would fail for reasons unrelated to release-version drift. The ``if:``
    rejection is narrow because a conditional on *this* job has no legitimate
    use: the gate is cheap and must run whenever CI runs.
    """
    try:
        text = read_bytes_no_follow(ci_yml_path).decode("utf-8")
    except (OSError, EvidenceGateError, UnicodeDecodeError) as exc:
        raise ReleaseMetadataError(f"{ci_yml_path}: cannot read CI workflow: {exc}") from exc
    try:
        workflow = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ReleaseMetadataError(f"{ci_yml_path}: invalid YAML: {exc}") from exc
    jobs = workflow.get("jobs") if isinstance(workflow, dict) else None
    job = jobs.get(_MANIFEST_DRIFT_JOB) if isinstance(jobs, dict) else None
    if not isinstance(job, dict):
        raise ReleaseMetadataError(
            f"{ci_yml_path}: no `{_MANIFEST_DRIFT_JOB}` job -- the gate that proves the "
            "committed promotion manifest still matches its inputs is gone"
        )
    if "if" in job:
        raise ReleaseMetadataError(
            f"{ci_yml_path}: the `{_MANIFEST_DRIFT_JOB}` job carries an `if:` "
            f"({job['if']!r}) -- a conditional job can be switched off while still "
            "satisfying this gate, so the drift check must run unconditionally"
        )
    steps = job.get("steps")
    expected = f"docs/benchmarks/v{version}-promotion-manifest.json"
    matching = [
        step
        for step in (steps if isinstance(steps, list) else [])
        if isinstance(step, dict)
        and isinstance(step.get("run"), str)
        and expected in _shell_words(step["run"])
    ]
    if not matching:
        raise ReleaseMetadataError(
            f"{ci_yml_path}: the `{_MANIFEST_DRIFT_JOB}` job does not reference {expected!r} "
            "as a shell word of any step it runs -- the release version changed without "
            "updating the manifest path here (and renaming/regenerating the committed "
            "manifest file to match)"
        )
    # One unconditional referencing step is enough; a *sibling* step may
    # legitimately carry an `if:`. Only a roster where every reference is
    # conditional means the gate can be switched off while staying green.
    if not any("if" not in step for step in matching):
        names = [str(step.get("name", "<unnamed>")) for step in matching]
        raise ReleaseMetadataError(
            f"{ci_yml_path}: every `{_MANIFEST_DRIFT_JOB}` step referencing {expected!r} "
            f"carries an `if:` ({names}) -- a conditional step can be switched off while "
            "still satisfying this gate"
        )


def check(
    pyproject_path: Path,
    changelog_path: Path,
    *,
    ci_yml_path: Path | None = None,
) -> str:
    """Return the matched version string, or raise ReleaseMetadataError."""
    pyproject_version = read_pyproject_version(pyproject_path)
    changelog_version, changelog_date = read_changelog_release(changelog_path)
    if pyproject_version != changelog_version:
        raise ReleaseMetadataError(
            f"pyproject.toml version {pyproject_version!r} does not match CHANGELOG.md's "
            f"first release heading {changelog_version!r}"
        )
    if not changelog_date:
        raise ReleaseMetadataError(
            f"CHANGELOG.md's [{changelog_version}] heading has no release date "
            "(expected `## [x.y.z] - YYYY-MM-DD`)"
        )
    check_no_hardcoded_version_literals(pyproject_path.parent / "scripts", pyproject_version)
    # Unlike the scripts/ scan above (relative to pyproject_path, so it works
    # against synthetic tmp_path fixtures for free), ci_yml_path has no
    # sensible tmp_path-relative default -- callers that want this check
    # (the real CLI, and tests exercising it directly) pass it explicitly;
    # callers that only care about pyproject/CHANGELOG agreement omit it and
    # this step is skipped.
    if ci_yml_path is not None:
        check_ci_promotion_manifest_path_matches_version(ci_yml_path, pyproject_version)
    return pyproject_version


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pyproject", type=Path, default=REPO_ROOT / "pyproject.toml")
    parser.add_argument("--changelog", type=Path, default=REPO_ROOT / "CHANGELOG.md")
    parser.add_argument(
        "--ci-yml", type=Path, default=REPO_ROOT / ".github" / "workflows" / "ci.yml"
    )
    parser.add_argument("--version", default=None)
    args = parser.parse_args(argv)

    try:
        version = check(args.pyproject, args.changelog, ci_yml_path=args.ci_yml)
        if args.version is not None and version != args.version:
            raise ReleaseMetadataError(
                f"--version {args.version!r} does not match the version read from "
                f"pyproject.toml/CHANGELOG.md {version!r}"
            )
    # EvidenceGateError as well as OSError: the hardened reads collapse a
    # symlinked/FIFO/oversized input into that one type instead of an OSError.
    # UnicodeDecodeError is converted to ReleaseMetadataError at each decode
    # site; it is listed here too so a decode site added later without that
    # conversion still prints FAIL rather than a traceback.
    except (ReleaseMetadataError, EvidenceGateError, OSError, UnicodeDecodeError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    print(f"OK: pyproject.toml and CHANGELOG.md both identify release {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
