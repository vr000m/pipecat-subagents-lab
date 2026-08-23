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
import sys
import tomllib
from pathlib import Path

from scripts.evidence_common import REPO_ROOT


# The date group requires an actual `YYYY-MM-DD` token, not merely
# `(\S+)`: this repo's own CHANGELOG.md convention (see every dated heading
# below `[Unreleased]`) always carries a real date, never a placeholder like
# "TBD" -- and `(\S+)` would let `## [0.1.3] - TBD` pass a gate whose error
# message promises a YYYY-MM-DD date.
CHANGELOG_HEADING_RE = re.compile(r"(?m)^##\s*\[([^\]]+)\]\s*(?:-\s*(\d{4}-\d{2}-\d{2}))?\s*$")


class ReleaseMetadataError(ValueError):
    """Raised when pyproject.toml and CHANGELOG.md disagree or are malformed."""


def read_pyproject_version(pyproject_path: Path) -> str:
    try:
        with pyproject_path.open("rb") as handle:
            data = tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        raise ReleaseMetadataError(f"{pyproject_path}: invalid TOML: {exc}") from exc
    project = data.get("project")
    version = project.get("version") if isinstance(project, dict) else None
    if not isinstance(version, str) or not version:
        raise ReleaseMetadataError(f"{pyproject_path}: no `[project] version` string found")
    return version


def read_changelog_release(changelog_path: Path) -> tuple[str, str | None]:
    """Return (version, date) for the first non-Unreleased CHANGELOG heading."""
    text = changelog_path.read_text(encoding="utf-8")
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
        if pattern.search(path.read_text(encoding="utf-8")):
            offending.append(str(path))
    if offending:
        raise ReleaseMetadataError(
            f"scripts/ hardcodes release version {version!r} in {offending} -- derive it from "
            "pyproject.toml/server.config instead of duplicating the literal"
        )


def check(pyproject_path: Path, changelog_path: Path) -> str:
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
    return pyproject_version


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pyproject", type=Path, default=REPO_ROOT / "pyproject.toml")
    parser.add_argument("--changelog", type=Path, default=REPO_ROOT / "CHANGELOG.md")
    parser.add_argument("--version", default=None)
    args = parser.parse_args(argv)

    try:
        version = check(args.pyproject, args.changelog)
        if args.version is not None and version != args.version:
            raise ReleaseMetadataError(
                f"--version {args.version!r} does not match the version read from "
                f"pyproject.toml/CHANGELOG.md {version!r}"
            )
    except (ReleaseMetadataError, OSError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    print(f"OK: pyproject.toml and CHANGELOG.md both identify release {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
