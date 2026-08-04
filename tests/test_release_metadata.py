"""Tests for ``scripts/check_release_metadata.py``.

Plan: docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md,
"New files to create" -- 'scripts/check_release_metadata.py -- release-
finalization check that pyproject.toml and CHANGELOG.md both identify
0.1.3; implement this script and its tests as the first post-Phase-4
release-finalization task, before changing either version file.'

The repo's actual pyproject.toml/CHANGELOG.md still say 0.1.2 as of this
test-writer's run (the version bump is an explicit later release-
finalization step per Testing Notes), so every test here operates on
synthetic copies under tmp_path rather than the real repo files -- this
suite must keep passing both before and after the real version bump lands.

The concurrent Phase 4 implementer had already landed this script by the
time this test-writer subagent read the repo: a version-agnostic
``check(pyproject_path, changelog_path) -> str`` that reads the canonical
version from ``pyproject.toml`` and requires ``CHANGELOG.md``'s first
non-``[Unreleased]`` heading to match it with a release date, plus a
``main(argv=None)`` CLI exposing ``--pyproject``/``--changelog`` path
overrides (defaulting to the real repo files). Most tests below exercise
that actual delivered API.

The plan's Testing Notes / final CI-equivalent command (line 398) also
specifies ``uv run python scripts/check_release_metadata.py --version 0.1.3``
-- an explicit ``--version`` argument the delivered CLI does not currently
accept. ``test_main_cli_supports_the_plan_mandated_version_argument`` below
holds the script to that literal contract; if it currently fails, that is a
genuine gap for the conductor's fix cycle to reconcile (add ``--version`` as
an optional cross-check against the read pyproject version), not a mistake
in this test.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "check_release_metadata.py"

VALID_PYPROJECT = """\
[project]
name = "pipecat-subagents-lab"
version = "0.1.3"
"""

VALID_CHANGELOG = """\
# Changelog

## [Unreleased]

## [0.1.3] - 2026-08-05

### Added
- Query-context narrowing experiment.

## [0.1.2] - 2026-07-28
"""


def _load_script() -> Any:
    if not SCRIPT_PATH.exists():
        pytest.skip(f"{SCRIPT_PATH} not yet implemented (Phase 4 concurrent implementer)")
    spec = importlib.util.spec_from_file_location("check_release_metadata", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_pair(
    tmp_path: Path, *, pyproject_text: str = VALID_PYPROJECT, changelog_text: str = VALID_CHANGELOG
) -> tuple[Path, Path]:
    pyproject_path = tmp_path / "pyproject.toml"
    changelog_path = tmp_path / "CHANGELOG.md"
    pyproject_path.write_text(pyproject_text, encoding="utf-8")
    changelog_path.write_text(changelog_text, encoding="utf-8")
    return pyproject_path, changelog_path


def _args(pyproject_path: Path, changelog_path: Path) -> list[str]:
    return ["--pyproject", str(pyproject_path), "--changelog", str(changelog_path)]


def test_accepts_matching_0_1_3_metadata_in_both_files(tmp_path: Path) -> None:
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)
    exit_code = module.main(_args(pyproject_path, changelog_path))
    assert exit_code == 0


def test_check_returns_the_matched_version_string(tmp_path: Path) -> None:
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)
    version = module.check(pyproject_path, changelog_path)
    assert version == "0.1.3"


def test_rejects_pyproject_changelog_version_mismatch(tmp_path: Path) -> None:
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(
        tmp_path, pyproject_text=VALID_PYPROJECT.replace("0.1.3", "0.1.2-mismatch")
    )
    exit_code = module.main(_args(pyproject_path, changelog_path))
    assert exit_code != 0
    with pytest.raises(module.ReleaseMetadataError):
        module.check(pyproject_path, changelog_path)


def test_rejects_when_only_an_unreleased_heading_is_present(tmp_path: Path) -> None:
    changelog_unreleased_only = "# Changelog\n\n## [Unreleased]\n\n- some change\n"
    pyproject_path, changelog_path = _write_pair(tmp_path, changelog_text=changelog_unreleased_only)
    module = _load_script()
    exit_code = module.main(_args(pyproject_path, changelog_path))
    assert exit_code != 0


def test_rejects_a_dated_release_heading_with_no_date(tmp_path: Path) -> None:
    changelog_no_date = VALID_CHANGELOG.replace("## [0.1.3] - 2026-08-05", "## [0.1.3]")
    pyproject_path, changelog_path = _write_pair(tmp_path, changelog_text=changelog_no_date)
    module = _load_script()
    exit_code = module.main(_args(pyproject_path, changelog_path))
    assert exit_code != 0
    with pytest.raises(module.ReleaseMetadataError, match="no release date"):
        module.check(pyproject_path, changelog_path)


def test_rejects_malformed_pyproject_missing_version_field(tmp_path: Path) -> None:
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(
        tmp_path, pyproject_text='[project]\nname = "pipecat-subagents-lab"\n'
    )
    exit_code = module.main(_args(pyproject_path, changelog_path))
    assert exit_code != 0


def test_rejects_a_changelog_with_no_release_headings_at_all(tmp_path: Path) -> None:
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(
        tmp_path, changelog_text="# Changelog\n\nNothing here.\n"
    )
    exit_code = module.main(_args(pyproject_path, changelog_path))
    assert exit_code != 0


def test_rejects_missing_pyproject_file(tmp_path: Path) -> None:
    module = _load_script()
    _, changelog_path = _write_pair(tmp_path)
    missing = tmp_path / "does-not-exist.toml"
    exit_code = module.main(_args(missing, changelog_path))
    assert exit_code != 0


def test_rejects_missing_changelog_file(tmp_path: Path) -> None:
    module = _load_script()
    pyproject_path, _ = _write_pair(tmp_path)
    missing = tmp_path / "does-not-exist.md"
    exit_code = module.main(_args(pyproject_path, missing))
    assert exit_code != 0


def test_default_paths_point_at_the_real_repo_files_and_agree_on_a_version() -> None:
    """Smoke test against the script's own default paths (no --pyproject/
    --changelog override): pyproject.toml and CHANGELOG.md must already be
    self-consistent at every point in this branch's history, including
    before the eventual 0.1.3 release-finalization commit -- the script is
    version-agnostic, so this must pass both now (0.1.2) and after the bump
    (0.1.3)."""
    module = _load_script()
    exit_code = module.main([])
    assert exit_code == 0


def test_the_bumped_0_1_3_release_section_does_not_exist_yet() -> None:
    """Documents the expected state at the time this test was written: the
    dedicated release-finalization commit (Testing Notes: 'first implement
    and test scripts/check_release_metadata.py ... then update CHANGELOG.md
    and pyproject.toml for 0.1.3') has not landed yet. If this starts
    failing because the bump landed, that is expected progress -- skip
    rather than treat it as a regression."""
    real_pyproject_text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    if '"0.1.3"' in real_pyproject_text:
        pytest.skip("release finalization has already bumped pyproject.toml to 0.1.3")
    assert '"0.1.3"' not in real_pyproject_text


def test_main_cli_supports_the_plan_mandated_version_argument(tmp_path: Path) -> None:
    """Plan Testing Notes / final CI-equivalent command (line 398):
    ``uv run python scripts/check_release_metadata.py --version 0.1.3``.
    The script should accept an optional ``--version`` cross-check that
    fails when it disagrees with the version actually read from
    pyproject.toml, even though the script is otherwise version-agnostic."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)
    matching = module.main([*_args(pyproject_path, changelog_path), "--version", "0.1.3"])
    assert matching == 0
    mismatching = module.main([*_args(pyproject_path, changelog_path), "--version", "9.9.9"])
    assert mismatching != 0
