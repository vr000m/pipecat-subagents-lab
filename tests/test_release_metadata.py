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

from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

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
    import scripts.check_release_metadata

    return scripts.check_release_metadata


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


VALID_CI_YML = """\
jobs:
  promotion-manifest-drift:
    steps:
      - name: Verify the committed promotion manifest has not drifted
        run: uv run python scripts/validate_v013_evidence.py --verify-manifest docs/benchmarks/v0.1.3-promotion-manifest.json
"""


def test_rejects_ci_yml_manifest_path_stale_after_a_version_bump(tmp_path: Path) -> None:
    """Round-5 restart regression (Architecture finding #11): ci.yml's
    promotion-manifest-drift job hardcodes a version-stamped manifest path
    that the scripts/*.py-scoped hardcoded-literal guard never scanned. Bump
    pyproject.toml/CHANGELOG.md's version without updating ci.yml's path and
    this must fail loudly instead of silently green-checking a manifest
    nothing consumes."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(
        tmp_path,
        pyproject_text=VALID_PYPROJECT.replace("0.1.3", "0.1.4"),
        changelog_text=VALID_CHANGELOG.replace("0.1.3", "0.1.4"),
    )
    ci_yml_path = tmp_path / "ci.yml"
    ci_yml_path.write_text(VALID_CI_YML, encoding="utf-8")  # still says v0.1.3

    with pytest.raises(
        module.ReleaseMetadataError, match="no .promotion-manifest-drift. step runs"
    ):
        module.check(pyproject_path, changelog_path, ci_yml_path=ci_yml_path)


def test_accepts_ci_yml_manifest_path_matching_the_current_version(tmp_path: Path) -> None:
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)  # version 0.1.3
    ci_yml_path = tmp_path / "ci.yml"
    ci_yml_path.write_text(VALID_CI_YML, encoding="utf-8")  # says v0.1.3, matches

    version = module.check(pyproject_path, changelog_path, ci_yml_path=ci_yml_path)
    assert version == "0.1.3"


def test_ci_yml_check_is_skipped_when_ci_yml_path_is_omitted(tmp_path: Path) -> None:
    """Callers that only care about pyproject/CHANGELOG agreement (every
    other test in this file that calls `check()` directly) must not be
    forced to also fixture a ci.yml -- version 0.1.4 here would fail the
    ci.yml check above if it ran, so this proves it doesn't."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(
        tmp_path,
        pyproject_text=VALID_PYPROJECT.replace("0.1.3", "0.1.4"),
        changelog_text=VALID_CHANGELOG.replace("0.1.3", "0.1.4"),
    )
    version = module.check(pyproject_path, changelog_path)
    assert version == "0.1.4"


def test_main_cli_checks_ci_yml_by_default_against_the_real_repo_file() -> None:
    """The real CLI entrypoint always passes --ci-yml (default: the repo's
    own ci.yml), so this check is live for the actual release gate even
    though `check()` itself treats it as opt-in. Overlaps with
    `test_default_paths_point_at_the_real_repo_files_and_agree_on_a_version`
    but asserts the ci.yml wiring specifically, not just an exit code."""
    module = _load_script()
    exit_code = module.main([])
    assert exit_code == 0


def test_main_cli_reports_ci_yml_drift_via_default_path() -> None:
    """`main()`'s default `--ci-yml` must actually be consulted, not just
    accepted and ignored -- point it at a ci.yml that doesn't mention the
    real repo's version and confirm the CLI fails."""
    module = _load_script()
    stale_ci_yml = REPO_ROOT / "tests" / "does-not-exist-ci.yml"
    assert not stale_ci_yml.exists()
    exit_code = module.main(["--ci-yml", str(stale_ci_yml)])
    assert exit_code != 0


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


def test_rejects_ci_yml_whose_only_mention_of_the_manifest_is_a_comment(
    tmp_path: Path,
) -> None:
    """Round 6 confirm pass 3, Security Minor: the check was a whole-file
    substring test, so a stale comment (or a `name:` field, or a disabled job)
    satisfied it while the step that actually runs pointed at the previous
    release's manifest."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)  # version 0.1.3
    ci_yml_path = tmp_path / "ci.yml"
    ci_yml_path.write_text(
        "jobs:\n"
        "  promotion-manifest-drift:\n"
        "    steps:\n"
        "      # was docs/benchmarks/v0.1.3-promotion-manifest.json\n"
        "      - name: Verify the committed promotion manifest has not drifted\n"
        "        run: uv run python scripts/validate_v013_evidence.py --verify-manifest "
        "docs/benchmarks/v0.1.2-promotion-manifest.json\n",
        encoding="utf-8",
    )

    with pytest.raises(
        module.ReleaseMetadataError, match="no .promotion-manifest-drift. step runs"
    ):
        module.check(pyproject_path, changelog_path, ci_yml_path=ci_yml_path)


def test_rejects_ci_yml_with_no_promotion_manifest_drift_job(tmp_path: Path) -> None:
    """Deleting the drift job entirely must fail loudly rather than pass: the
    substring test happened to catch this only because the path string lived
    inside that job."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)
    ci_yml_path = tmp_path / "ci.yml"
    ci_yml_path.write_text("jobs:\n  test:\n    steps: []\n", encoding="utf-8")

    with pytest.raises(module.ReleaseMetadataError, match="promotion-manifest-drift"):
        module.check(pyproject_path, changelog_path, ci_yml_path=ci_yml_path)


def test_rejects_a_ci_yml_whose_drift_job_is_disabled_by_an_if_condition(
    tmp_path: Path,
) -> None:
    """Round 7 confirm pass 4, Security Minor: parsing the workflow closed the
    comment bypass but not the disabled-job one the docstring also claimed --
    `if: false` leaves the job declared, this gate green, and the drift check
    never executed."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)  # version 0.1.3
    ci_yml_path = tmp_path / "ci.yml"
    ci_yml_path.write_text(
        VALID_CI_YML.replace(
            "  promotion-manifest-drift:\n",
            "  promotion-manifest-drift:\n    if: false\n",
        ),
        encoding="utf-8",
    )

    with pytest.raises(module.ReleaseMetadataError, match="carries an `if:`"):
        module.check(pyproject_path, changelog_path, ci_yml_path=ci_yml_path)


def test_rejects_a_ci_yml_whose_only_referencing_step_is_conditional(tmp_path: Path) -> None:
    """The step-level spelling of the same evasion."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)
    ci_yml_path = tmp_path / "ci.yml"
    ci_yml_path.write_text(
        VALID_CI_YML.replace(
            "      - name: Verify",
            "      - if: ${{ github.ref == 'refs/heads/never' }}\n        name: Verify",
        ),
        encoding="utf-8",
    )

    with pytest.raises(module.ReleaseMetadataError, match="carries an `if:`"):
        module.check(pyproject_path, changelog_path, ci_yml_path=ci_yml_path)


def test_accepts_a_conditional_sibling_step_alongside_an_unconditional_one(
    tmp_path: Path,
) -> None:
    """Only a roster where EVERY reference is conditional means the gate can
    be switched off -- an extra conditional step is not itself a bypass."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)
    ci_yml_path = tmp_path / "ci.yml"
    ci_yml_path.write_text(
        VALID_CI_YML + "      - if: ${{ failure() }}\n"
        "        name: Re-verify on failure\n"
        "        run: uv run python scripts/validate_v013_evidence.py --verify-manifest "
        "docs/benchmarks/v0.1.3-promotion-manifest.json\n",
        encoding="utf-8",
    )

    assert module.check(pyproject_path, changelog_path, ci_yml_path=ci_yml_path) == "0.1.3"


def test_rejects_a_ci_yml_that_only_echoes_the_manifest_path_in_a_message(
    tmp_path: Path,
) -> None:
    """`expected in command` was an unanchored substring test at the command
    level, so a quoted reminder inside an `echo` satisfied the gate while the
    step that actually diffs pointed at the previous release's manifest
    (round 7 confirm pass 4, Security Minor)."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)  # version 0.1.3
    ci_yml_path = tmp_path / "ci.yml"
    ci_yml_path.write_text(
        "jobs:\n"
        "  promotion-manifest-drift:\n"
        "    steps:\n"
        "      - name: Remind\n"
        '        run: echo "regenerate docs/benchmarks/v0.1.3-promotion-manifest.json '
        'when bumping"\n'
        "      - name: Verify the committed promotion manifest has not drifted\n"
        "        run: uv run python scripts/validate_v013_evidence.py --verify-manifest "
        "docs/benchmarks/v0.1.2-promotion-manifest.json\n",
        encoding="utf-8",
    )

    with pytest.raises(
        module.ReleaseMetadataError, match="no .promotion-manifest-drift. step runs"
    ):
        module.check(pyproject_path, changelog_path, ci_yml_path=ci_yml_path)


def test_rejects_a_ci_yml_mentioning_the_manifest_only_in_a_run_script_comment(
    tmp_path: Path,
) -> None:
    """A `#` comment INSIDE a run script, not just above the step -- shlex
    drops it, so it cannot satisfy the gate either."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)
    ci_yml_path = tmp_path / "ci.yml"
    ci_yml_path.write_text(
        "jobs:\n"
        "  promotion-manifest-drift:\n"
        "    steps:\n"
        "      - name: Verify the committed promotion manifest has not drifted\n"
        "        run: |\n"
        "          # docs/benchmarks/v0.1.3-promotion-manifest.json\n"
        "          uv run python scripts/validate_v013_evidence.py --verify-manifest "
        "docs/benchmarks/v0.1.2-promotion-manifest.json\n",
        encoding="utf-8",
    )

    with pytest.raises(
        module.ReleaseMetadataError, match="no .promotion-manifest-drift. step runs"
    ):
        module.check(pyproject_path, changelog_path, ci_yml_path=ci_yml_path)


def test_rejects_a_ci_yml_that_only_echoes_the_manifest_path_unquoted(
    tmp_path: Path,
) -> None:
    """Round 8 confirm pass 5, Security Minor: whole-shell-word matching only
    defeated the QUOTED echo bypass. Unquoted, shlex hands the path back as
    its own word, so the same reminder message satisfied the gate while the
    step that actually diffs pointed at the previous release's manifest."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)  # version 0.1.3
    ci_yml_path = tmp_path / "ci.yml"
    ci_yml_path.write_text(
        "jobs:\n"
        "  promotion-manifest-drift:\n"
        "    steps:\n"
        "      - name: Remind\n"
        "        run: echo Regenerate docs/benchmarks/v0.1.3-promotion-manifest.json "
        "when bumping\n"
        "      - name: Verify the committed promotion manifest has not drifted\n"
        "        run: uv run python scripts/validate_v013_evidence.py --verify-manifest "
        "docs/benchmarks/v0.1.2-promotion-manifest.json\n",
        encoding="utf-8",
    )

    with pytest.raises(
        module.ReleaseMetadataError, match="no .promotion-manifest-drift. step runs"
    ):
        module.check(pyproject_path, changelog_path, ci_yml_path=ci_yml_path)


def test_rejects_a_ci_yml_whose_drift_job_is_neutered_by_continue_on_error(
    tmp_path: Path,
) -> None:
    """Round 8 confirm pass 5, Security Minor: `continue-on-error: true`
    disables the gate more completely than `if: false` -- the drift check
    runs, reports drift, and the workflow stays green."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)
    ci_yml_path = tmp_path / "ci.yml"
    ci_yml_path.write_text(
        VALID_CI_YML.replace(
            "  promotion-manifest-drift:\n",
            "  promotion-manifest-drift:\n    continue-on-error: true\n",
        ),
        encoding="utf-8",
    )

    with pytest.raises(module.ReleaseMetadataError, match="continue-on-error"):
        module.check(pyproject_path, changelog_path, ci_yml_path=ci_yml_path)


def test_rejects_a_ci_yml_whose_only_verifying_step_is_continue_on_error(
    tmp_path: Path,
) -> None:
    """The step-level spelling of the same evasion."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)
    ci_yml_path = tmp_path / "ci.yml"
    ci_yml_path.write_text(
        VALID_CI_YML.replace(
            "      - name: Verify",
            "      - continue-on-error: true\n        name: Verify",
        ),
        encoding="utf-8",
    )

    with pytest.raises(module.ReleaseMetadataError, match="continue-on-error"):
        module.check(pyproject_path, changelog_path, ci_yml_path=ci_yml_path)


def test_accepts_a_continue_on_error_sibling_alongside_an_armed_step(tmp_path: Path) -> None:
    """`continue-on-error: false` leaves a step armed, and a switchable
    SIBLING is not itself a bypass."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)
    ci_yml_path = tmp_path / "ci.yml"
    ci_yml_path.write_text(
        VALID_CI_YML.replace(
            "      - name: Verify",
            "      - continue-on-error: false\n        name: Verify",
        )
        + "      - continue-on-error: true\n"
        "        name: Re-verify, advisory\n"
        "        run: uv run python scripts/validate_v013_evidence.py --verify-manifest "
        "docs/benchmarks/v0.1.3-promotion-manifest.json\n",
        encoding="utf-8",
    )

    assert module.check(pyproject_path, changelog_path, ci_yml_path=ci_yml_path) == "0.1.3"


def test_accepts_a_verifying_step_written_with_line_continuations(tmp_path: Path) -> None:
    """Round 8 confirm pass 5, Logic Minor: `_shell_words` shlex-split each
    line independently, and a line ending in `\\` raises `ValueError: No
    escaped character` -- so a CORRECT workflow written in the standard
    continuation style lost the line carrying the manifest path and was
    rejected."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)
    ci_yml_path = tmp_path / "ci.yml"
    ci_yml_path.write_text(
        "jobs:\n"
        "  promotion-manifest-drift:\n"
        "    steps:\n"
        "      - name: Verify the committed promotion manifest has not drifted\n"
        "        run: |\n"
        "          uv run python scripts/validate_v013_evidence.py \\\n"
        "            --verify-manifest \\\n"
        "            docs/benchmarks/v0.1.3-promotion-manifest.json\n",
        encoding="utf-8",
    )

    assert module.check(pyproject_path, changelog_path, ci_yml_path=ci_yml_path) == "0.1.3"


def test_a_reporting_command_after_a_separator_does_not_hide_a_real_invocation(
    tmp_path: Path,
) -> None:
    """Dropping `echo`-owned words is per-command, not per-line: a real
    verifier invocation joined to an `echo` by `&&` must still count."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)
    ci_yml_path = tmp_path / "ci.yml"
    ci_yml_path.write_text(
        "jobs:\n"
        "  promotion-manifest-drift:\n"
        "    steps:\n"
        "      - name: Verify the committed promotion manifest has not drifted\n"
        "        run: echo checking && uv run python scripts/validate_v013_evidence.py "
        "--verify-manifest docs/benchmarks/v0.1.3-promotion-manifest.json\n",
        encoding="utf-8",
    )

    assert module.check(pyproject_path, changelog_path, ci_yml_path=ci_yml_path) == "0.1.3"


def test_the_real_repo_ci_yml_still_satisfies_the_tightened_gate() -> None:
    """The tightening must not have made the live workflow fail: the real
    `promotion-manifest-drift` job carries no `if:` and names the manifest as
    a bare shell word."""
    module = _load_script()
    module.check_ci_promotion_manifest_path_matches_version(
        REPO_ROOT / ".github" / "workflows" / "ci.yml",
        module.read_pyproject_version(REPO_ROOT / "pyproject.toml"),
    )


def test_rejects_a_symlinked_pyproject(tmp_path: Path) -> None:
    """The pyproject read is hardened like every other evidence read on this
    branch -- the round 6 sweep migrated the CHANGELOG/scripts/ci.yml reads and
    skipped this one (round 7 confirm pass 4, Architecture Minor)."""
    module = _load_script()
    _, changelog_path = _write_pair(tmp_path)
    real_pyproject = tmp_path / "real-pyproject.toml"
    real_pyproject.write_text(VALID_PYPROJECT, encoding="utf-8")
    linked = tmp_path / "linked-pyproject.toml"
    linked.symlink_to(real_pyproject)

    with pytest.raises(module.EvidenceGateError):
        module.check(linked, changelog_path)


def test_non_utf8_changelog_returns_fail_rather_than_a_traceback(tmp_path: Path) -> None:
    """Only one of three decode sites converted UnicodeDecodeError, and main's
    except tuple did not list it, so the other two tracebacked out of the CLI
    (round 7 confirm pass 4, Architecture Minor)."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)
    changelog_path.write_bytes(b"\xff\xfe## [0.1.3] - 2026-07-28\n")

    assert module.main(_args(pyproject_path, changelog_path)) != 0


def test_non_utf8_pyproject_returns_fail_rather_than_a_traceback(tmp_path: Path) -> None:
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)
    pyproject_path.write_bytes(b'\xff\xfe[project]\nversion = "0.1.3"\n')

    assert module.main(_args(pyproject_path, changelog_path)) != 0


def test_rejects_a_symlinked_ci_yml(tmp_path: Path) -> None:
    """The ci.yml read is hardened like every other evidence read on this
    branch: a predictable repo-relative path a plain `read_text` would follow
    through a symlink."""
    module = _load_script()
    pyproject_path, changelog_path = _write_pair(tmp_path)
    real_ci_yml = tmp_path / "real-ci.yml"
    real_ci_yml.write_text(VALID_CI_YML, encoding="utf-8")
    linked = tmp_path / "ci.yml"
    linked.symlink_to(real_ci_yml)

    with pytest.raises(module.ReleaseMetadataError, match="cannot read CI workflow"):
        module.check(pyproject_path, changelog_path, ci_yml_path=linked)
