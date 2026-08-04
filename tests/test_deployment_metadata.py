"""Producer-to-writer-to-loader tests for release deployment metadata.

Plan: docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md,
Phase 2 bullet 193. ``scripts/emit_v013_deployment_metadata.py`` always
derives its values from its own repository root (``REPO_ROOT``, a module
constant), not the process ``cwd``, so these tests import the module by path
and monkeypatch ``REPO_ROOT`` to a scratch git checkout rather than
shelling out against the real (often mid-edit) working tree.

``--shell-export`` refuses a dirty/untracked tree and prints exactly four
``export`` lines (``PIPECAT_SOURCE_COMMIT``, ``PIPECAT_SOURCE_TREE_HASH``,
``PIPECAT_DEPLOYED_AT_UTC``, ``PIPECAT_FEATURE_POLICY_FINGERPRINT``).
``--check-release-inputs`` does *not* require a clean tree (per the script's
own docstring: "local dev/test may leave them unset and remains
display-only") -- it only proves the four values are derivable from the
current checkout, so it fails only when the checkout cannot produce them at
all (e.g. not a git repository).
"""

from __future__ import annotations

import importlib.util
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "emit_v013_deployment_metadata.py"

EXPORT_RE = re.compile(r"^export (PIPECAT_[A-Z_]+)=(.*)$", re.MULTILINE)


def _load_module() -> Any:
    if not SCRIPT_PATH.exists():
        pytest.skip(f"{SCRIPT_PATH} not yet implemented (Phase 2 concurrent implementer)")
    spec = importlib.util.spec_from_file_location("emit_v013_deployment_metadata", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _init_clean_release_tree(tmp_path: Path) -> Path:
    repo = tmp_path / "release-tree"
    repo.mkdir()
    (repo / "server").mkdir()
    (repo / "server" / "marker.py").write_text("# release marker\n", encoding="utf-8")
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
    assert set(exports) == {
        "PIPECAT_SOURCE_COMMIT",
        "PIPECAT_SOURCE_TREE_HASH",
        "PIPECAT_DEPLOYED_AT_UTC",
        "PIPECAT_FEATURE_POLICY_FINGERPRINT",
    }
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
