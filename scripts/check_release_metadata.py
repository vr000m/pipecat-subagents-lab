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
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

PYPROJECT_VERSION_RE = re.compile(r'(?m)^version\s*=\s*"([^"]+)"')
CHANGELOG_HEADING_RE = re.compile(r"(?m)^##\s*\[([^\]]+)\]\s*(?:-\s*(\S+))?\s*$")


class ReleaseMetadataError(ValueError):
    """Raised when pyproject.toml and CHANGELOG.md disagree or are malformed."""


def read_pyproject_version(pyproject_path: Path) -> str:
    text = pyproject_path.read_text(encoding="utf-8")
    match = PYPROJECT_VERSION_RE.search(text)
    if not match:
        raise ReleaseMetadataError(f'{pyproject_path}: no `version = "..."` field found')
    return match.group(1)


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
