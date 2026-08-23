"""Producer-to-writer-to-loader tests for release deployment metadata.

Plan: docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md,
Phase 2 bullet 193. ``scripts/emit_v013_deployment_metadata.py`` always
derives its values from its own repository root (``REPO_ROOT``, a module
constant), not the process ``cwd``, so these tests import the module by path
and monkeypatch ``REPO_ROOT`` to a scratch git checkout rather than
shelling out against the real (often mid-edit) working tree.

``--shell-export`` refuses a dirty/untracked tree and prints exactly four
bare ``KEY=VALUE`` lines (``PIPECAT_SOURCE_COMMIT``,
``PIPECAT_SOURCE_TREE_HASH``, ``PIPECAT_DEPLOYED_AT_UTC``,
``PIPECAT_FEATURE_POLICY_FINGERPRINT``) with no ``export `` prefix, because
``.github/workflows/ci.yml`` appends that stdout to ``$GITHUB_ENV``, which
GitHub Actions parses literally rather than evaluating as shell.
``--check-release-inputs`` does *not* require a clean tree (per the script's
own docstring: "local dev/test may leave them unset and remains
display-only") -- it only proves the four values are derivable from the
current checkout, so it fails only when the checkout cannot produce them at
all (e.g. not a git repository).
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

EXPORT_RE = re.compile(r"^(PIPECAT_[A-Z_]+)=(.*)$", re.MULTILINE)

EXPECTED_NAMES = {
    "PIPECAT_SOURCE_COMMIT",
    "PIPECAT_SOURCE_TREE_HASH",
    "PIPECAT_DEPLOYED_AT_UTC",
    "PIPECAT_FEATURE_POLICY_FINGERPRINT",
}

# Runtime-identity files that must be inside the tree-hash allowlist, and
# files that must stay outside it (evidence and tooling commits may never
# drift the deployment identity).
RUNTIME_IDENTITY_FILES = (
    "server/marker.py",
    "web/src/app.js",
    "web/src/styles.css",
    "web/index.html",
    "web/package.json",
    "web/bun.lock",
    "shared/protocol.md",
    "pyproject.toml",
    "uv.lock",
    "shared/schemas/rtvi-message.json",
    "shared/schemas/snapshot-handshake.json",
    "shared/schemas/work-status.json",
    "shared/schemas/runtime-snapshot.json",
    "shared/schemas/grounded-result.json",
)
NON_RUNTIME_FILES = (
    "docs/benchmarks/v0.1.3-phase0-transport-baseline.jsonl",
    "scripts/validate_v013_evidence.py",
    "shared/schemas/v013-transport-browser-contract.json",
    "tests/test_marker.py",
)


def _load_module() -> Any:
    import scripts.emit_v013_deployment_metadata

    return scripts.emit_v013_deployment_metadata


def _init_clean_release_tree(tmp_path: Path) -> Path:
    repo = tmp_path / "release-tree"
    repo.mkdir()
    for rel in (*RUNTIME_IDENTITY_FILES, *NON_RUNTIME_FILES):
        target = repo / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"release marker for {rel}\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "ci@example.invalid"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "CI"], cwd=repo, check=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "release tree"], cwd=repo, check=True)
    return repo


def test_shell_export_emits_exactly_four_pipecat_export_lines_on_a_clean_tree(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    module = _load_module()
    monkeypatch.setattr(module, "REPO_ROOT", _init_clean_release_tree(tmp_path))

    exit_code = module.main(["--shell-export"])

    assert exit_code == 0
    exports = dict(EXPORT_RE.findall(capsys.readouterr().out))
    assert set(exports) == EXPECTED_NAMES
    assert all(value for value in exports.values())


def test_shell_export_rejects_a_dirty_tracked_tree(tmp_path: Path, monkeypatch) -> None:
    module = _load_module()
    repo = _init_clean_release_tree(tmp_path)
    (repo / "server" / "marker.py").write_text("# dirty edit\n", encoding="utf-8")
    monkeypatch.setattr(module, "REPO_ROOT", repo)

    assert module.main(["--shell-export"]) != 0


def test_shell_export_rejects_an_untracked_file_in_the_tree(tmp_path: Path, monkeypatch) -> None:
    module = _load_module()
    repo = _init_clean_release_tree(tmp_path)
    (repo / "server" / "untracked.py").write_text("# untracked\n", encoding="utf-8")
    monkeypatch.setattr(module, "REPO_ROOT", repo)

    assert module.main(["--shell-export"]) != 0


def test_shell_export_fails_structurally_outside_a_git_checkout(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Round-3 restart gauntlet, Logic finding: ``_is_dirty()`` was called
    *outside* ``shell_export()``'s try/except.

    It uses ``_run_git`` with ``check=True``, so running outside a git
    checkout (or against a corrupt index) raised an uncaught
    ``subprocess.CalledProcessError`` -- a raw traceback and a Python exit
    code, not the ``FAIL:`` message and return-1 the script promises. CI pipes
    this into ``$GITHUB_ENV``, where a traceback is a far worse signal than
    the documented refusal.
    """
    not_a_repo = tmp_path / "no-git-here"
    not_a_repo.mkdir()
    module = _load_module()
    monkeypatch.setattr(module, "REPO_ROOT", not_a_repo)

    exit_code = module.main(["--shell-export"])

    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err.startswith("FAIL: could not derive deployment metadata")
    # And nothing partial was emitted for CI to source.
    assert not EXPORT_RE.findall(captured.out)


def test_check_release_inputs_does_not_require_a_clean_tree(tmp_path: Path, monkeypatch) -> None:
    """Plan/script docstring: local dev/test may leave metadata unset and
    remains display-only -- --check-release-inputs is a lightweight
    preflight, not the release gate; a dirty scratch tree must still let it
    derive values."""
    module = _load_module()
    repo = _init_clean_release_tree(tmp_path)
    (repo / "server" / "marker.py").write_text("# dirty edit\n", encoding="utf-8")
    monkeypatch.setattr(module, "REPO_ROOT", repo)

    assert module.main(["--check-release-inputs"]) == 0


def test_check_release_inputs_refuses_when_the_checkout_cannot_derive_metadata_at_all(
    tmp_path: Path, monkeypatch
) -> None:
    """A tree that is not a git checkout at all cannot derive
    PIPECAT_SOURCE_COMMIT; the preflight must fail rather than boot on
    fabricated identity."""
    module = _load_module()
    not_a_repo = tmp_path / "not-a-git-repo"
    not_a_repo.mkdir()
    monkeypatch.setattr(module, "REPO_ROOT", not_a_repo)

    assert module.main(["--check-release-inputs"]) != 0


def test_shell_export_and_check_release_inputs_are_mutually_exclusive() -> None:
    module = _load_module()
    with pytest.raises(SystemExit):
        module.main(["--shell-export", "--check-release-inputs"])


def test_shell_export_values_are_shell_safe_for_eval_capture(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """End-to-end handoff shape: the four exported values must be
    single-line and whitespace-free so ``eval "$(... --shell-export)"``
    binds exactly four variables, matching the CI capture the writer
    consumes (--source-commit/--source-tree-hash/--deployed-at-utc/
    --feature-policy-fingerprint)."""
    module = _load_module()
    monkeypatch.setattr(module, "REPO_ROOT", _init_clean_release_tree(tmp_path))

    assert module.main(["--shell-export"]) == 0
    exports = dict(EXPORT_RE.findall(capsys.readouterr().out))
    for name, value in exports.items():
        assert "\n" not in value, f"{name} export value must be single-line"
        assert " " not in value, f"{name} export value must not contain whitespace"


def test_shell_export_lines_carry_no_export_prefix(tmp_path: Path, monkeypatch, capsys) -> None:
    """Regression (M6): ``.github/workflows/ci.yml`` appends this stdout to
    ``$GITHUB_ENV``, which GitHub Actions parses as literal ``KEY=VALUE``
    pairs without shell evaluation. An ``export `` prefix would be absorbed
    into the *name*, producing a variable called ``export
    PIPECAT_SOURCE_COMMIT`` and leaving ``$PIPECAT_SOURCE_COMMIT`` empty in
    every downstream step."""
    module = _load_module()
    monkeypatch.setattr(module, "REPO_ROOT", _init_clean_release_tree(tmp_path))

    assert module.main(["--shell-export"]) == 0
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]

    assert len(lines) == 4
    for line in lines:
        assert not line.startswith("export "), f"{line!r} must not carry an export prefix"
        assert re.fullmatch(r"[A-Z][A-Z0-9_]*=[A-Za-z0-9:.+-]+", line), line


def test_shell_export_output_round_trips_through_github_env_parsing(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Regression (M6): parse the appended file exactly the way GitHub
    Actions does -- split each line on the first ``=``, no shell -- and
    require the four expected names with non-empty values."""
    module = _load_module()
    monkeypatch.setattr(module, "REPO_ROOT", _init_clean_release_tree(tmp_path))

    assert module.main(["--shell-export"]) == 0
    github_env = tmp_path / "github_env"
    github_env.write_text(capsys.readouterr().out, encoding="utf-8")

    parsed = {}
    for line in github_env.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        key, sep, value = line.partition("=")
        assert sep == "=", f"{line!r} is not a KEY=VALUE pair"
        parsed[key] = value

    assert set(parsed) == EXPECTED_NAMES
    for name in EXPECTED_NAMES:
        assert parsed[name], f"{name} must not resolve to an empty string"


@pytest.mark.parametrize("rel", RUNTIME_IDENTITY_FILES)
def test_runtime_tree_hash_changes_when_a_runtime_file_changes(
    tmp_path: Path, monkeypatch, rel: str
) -> None:
    """Regression (M13): ``web/index.html`` is served, ``web/src/styles.css``
    is bundled, and ``uv.lock`` pins the Python runtime -- editing any of
    them changes what is deployed, so it must change the identity hash."""
    module = _load_module()
    repo = _init_clean_release_tree(tmp_path)
    monkeypatch.setattr(module, "REPO_ROOT", repo)

    before = module.source_tree_hash()
    (repo / rel).write_text("mutated runtime content\n", encoding="utf-8")

    assert module.source_tree_hash() != before, f"{rel} must be inside the runtime tree hash"


@pytest.mark.parametrize("rel", NON_RUNTIME_FILES)
def test_runtime_tree_hash_ignores_evidence_and_tooling_files(
    tmp_path: Path, monkeypatch, rel: str
) -> None:
    """Existing invariant: evidence, tooling, evidence schemas and tests are
    outside the deployable runtime set, so regenerating them must never
    drift the deployment identity."""
    module = _load_module()
    repo = _init_clean_release_tree(tmp_path)
    monkeypatch.setattr(module, "REPO_ROOT", repo)

    before = module.source_tree_hash()
    (repo / rel).write_text("mutated non-runtime content\n", encoding="utf-8")

    assert module.source_tree_hash() == before, f"{rel} must stay outside the runtime tree hash"


def test_runtime_tree_hash_ignores_a_gitignored_file_matching_a_runtime_glob(
    tmp_path: Path, monkeypatch
) -> None:
    """Regression: every `RUNTIME_TREE_GLOBS` match was hashed with no
    `git ls-files` filter, despite the docstring's "currently-tracked" claim
    -- a gitignored file matching the glob (e.g. a local build artifact
    under `web/src/`) would silently change the computed release identity on
    an otherwise clean tree."""
    module = _load_module()
    repo = _init_clean_release_tree(tmp_path)
    monkeypatch.setattr(module, "REPO_ROOT", repo)

    before = module.source_tree_hash()
    untracked_runtime_file = repo / "web" / "src" / "untracked-build-artifact.js"
    untracked_runtime_file.write_text("// never committed\n", encoding="utf-8")

    assert module.source_tree_hash() == before, (
        "an untracked file matching a runtime glob must not change the release identity"
    )
