#!/usr/bin/env python3
"""Derive and emit the v0.1.3 deployment identity metadata.

Concrete metadata ownership (see the dev plan's Phase 2 section): CI's
release/manifest job invokes this script with ``--shell-export`` against a
clean, checked-out release tree and appends its four ``KEY=VALUE`` lines to
``$GITHUB_ENV`` before invoking
``scripts/validate_v013_evidence.py --write-manifest``.

``--shell-export`` prints bare ``NAME=value`` lines with **no** ``export``
prefix, because GitHub Actions parses ``$GITHUB_ENV`` as literal
``KEY=VALUE`` pairs and never evaluates it as shell; an ``export `` prefix
would be absorbed into the variable *name*. The same bare form remains
usable locally via ``eval "$(... --shell-export)"``, where each line is an
ordinary shell assignment (shell-local rather than exported -- prefix the
consuming command with ``export`` or use ``set -a`` if the child process
needs them). Every emitted line is validated against
``SHELL_EXPORT_LINE_RE`` before printing so no value can smuggle in
whitespace, a newline, or shell metacharacters.

Local dev/test never runs the release job, so ``--check-release-inputs`` (used
by every phase's test command as a lightweight preflight) only proves the
four values *can* be derived from the current checkout -- it does not require
a clean tree, matching the plan's "local dev/test may leave them unset and
remains display-only" rule.

``PIPECAT_SOURCE_TREE_HASH`` is a deterministic filtered hash of the
committed deployable runtime set: ``server/**``, ``web/src/**`` (scripts and
stylesheets), ``web/index.html``, ``shared/protocol.md``, the five runtime
JSON schemas, package/build metadata, and both lockfiles (``web/bun.lock``
and ``uv.lock``) -- explicitly excluding ``docs/benchmarks/**``,
evidence ``v013-*`` schemas, test fixtures, scripts, and generated evidence.
`server/config.py`'s loader and this emitter must use the same allowlist so
neither can drift the runtime identity independently.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

from scripts.evidence_common import REPO_ROOT, now_utc, read_bytes_no_follow, sha256_bytes

# The deployable runtime identity surface. Deliberately excludes
# docs/benchmarks/**, shared/schemas/v013-*.json, tests/**, and scripts/**,
# so evidence-collection and tooling commits never drift the runtime hash.
RUNTIME_TREE_GLOBS = (
    "server/**/*.py",
    "web/src/**/*.js",
    "web/src/**/*.css",
    "web/index.html",
    "shared/protocol.md",
    "shared/schemas/rtvi-message.json",
    "shared/schemas/snapshot-handshake.json",
    "shared/schemas/work-status.json",
    "shared/schemas/runtime-snapshot.json",
    "shared/schemas/grounded-result.json",
    "web/package.json",
    "web/bun.lock",
    "pyproject.toml",
    "uv.lock",
)

# The exact shape every ``--shell-export`` line must have. GitHub Actions
# reads ``$GITHUB_ENV`` as literal ``KEY=VALUE`` pairs (no shell evaluation),
# so the line must carry no ``export `` prefix, no quoting and no
# whitespace. Values here are hex digests and an ISO-8601 UTC timestamp, so
# this charset is a defensive assertion rather than an expected rejection.
SHELL_EXPORT_LINE_RE = re.compile(r"^[A-Z][A-Z0-9_]*=[A-Za-z0-9:.+-]+$")


def _run_git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def source_commit() -> str:
    return _run_git("rev-parse", "HEAD")


def _tracked_files() -> set[str]:
    return set(_run_git("ls-files").splitlines())


def source_tree_hash() -> str:
    """A deterministic filtered hash over `RUNTIME_TREE_GLOBS`'s tracked files.

    Reads each matched, currently-tracked file's on-disk bytes (not the git
    blob) so an uncommitted-but-staged phase-in-progress checkout still
    produces a value, matching `--check-release-inputs`'s no-clean-tree
    requirement; only `--shell-export`'s dirty-tree rejection enforces that
    the release build is exact.

    Every glob match is filtered through `git ls-files` before hashing: a
    gitignored file that happens to match one of `RUNTIME_TREE_GLOBS` (e.g. a
    local build artifact under `web/src/`) would otherwise silently change
    the computed release identity on an otherwise clean tree, contradicting
    the "currently-tracked" claim below.
    """
    tracked = _tracked_files()
    paths: set[Path] = set()
    for pattern in RUNTIME_TREE_GLOBS:
        paths.update(
            p
            for p in REPO_ROOT.glob(pattern)
            if p.is_file() and str(p.relative_to(REPO_ROOT)) in tracked
        )
    digest_input = bytearray()
    for path in sorted(paths, key=lambda p: str(p.relative_to(REPO_ROOT))):
        rel = str(path.relative_to(REPO_ROOT)).encode("utf-8")
        digest_input += len(rel).to_bytes(4, "big") + rel
        # read_bytes_no_follow (scripts/evidence_common.py), not
        # `path.read_bytes()`: `is_file()` above follows symlinks, so a
        # tracked symlink matching one of `RUNTIME_TREE_GLOBS` would
        # otherwise have its out-of-tree target's bytes silently folded into
        # this release-identity digest, with no size cap. Every other
        # evidence read on this branch is already hardened the same way; this
        # one feeds a promotion manifest's identity hash, so it must fail
        # closed too.
        data = read_bytes_no_follow(path)
        digest_input += len(data).to_bytes(8, "big") + data
    return sha256_bytes(bytes(digest_input))


def deployed_at_utc() -> str:
    return now_utc()


def feature_policy_fingerprint_value() -> str:
    from server.config import effective_feature_policy_fingerprint, load_config

    return effective_feature_policy_fingerprint(load_config())


def _is_dirty() -> bool:
    status = _run_git("status", "--porcelain")
    return bool(status.strip())


def check_release_inputs() -> int:
    try:
        commit = source_commit()
        tree_hash = source_tree_hash()
        deployed_at = deployed_at_utc()
        fingerprint = feature_policy_fingerprint_value()
    except Exception as exc:  # noqa: BLE001 - report and fail the preflight, not a boot path
        print(f"FAIL: could not derive deployment metadata: {exc}", file=sys.stderr)
        return 1
    print(
        "OK: deployment metadata derivable "
        f"(source_commit={commit[:12]}, source_tree_hash={tree_hash[:12]}, "
        f"deployed_at_utc={deployed_at}, feature_policy_fingerprint={fingerprint[:12]})"
    )
    return 0


def shell_export() -> int:
    # ``_is_dirty()`` runs *inside* the guard (round-3 restart gauntlet, Logic
    # finding). It calls ``_run_git`` with ``check=True``, so outside a git
    # checkout -- or against a corrupt index -- it raised an uncaught
    # ``CalledProcessError``: a raw traceback and a Python exit code, not the
    # structured ``FAIL:`` message and return-1 this function promises every
    # caller. CI pipes this straight into ``$GITHUB_ENV``, so the failure mode
    # mattered: an unhandled traceback there is a much worse signal than the
    # documented refusal.
    try:
        dirty = _is_dirty()
        commit = source_commit()
        tree_hash = source_tree_hash()
        deployed_at = deployed_at_utc()
        fingerprint = feature_policy_fingerprint_value()
    except Exception as exc:  # noqa: BLE001 - report and fail rather than emit partial exports
        print(f"FAIL: could not derive deployment metadata: {exc}", file=sys.stderr)
        return 1
    if dirty:
        print(
            "FAIL: refusing to emit release metadata from a dirty/untracked tree", file=sys.stderr
        )
        return 1
    lines = [
        f"PIPECAT_SOURCE_COMMIT={commit}",
        f"PIPECAT_SOURCE_TREE_HASH={tree_hash}",
        f"PIPECAT_DEPLOYED_AT_UTC={deployed_at}",
        f"PIPECAT_FEATURE_POLICY_FINGERPRINT={fingerprint}",
    ]
    for line in lines:
        if not SHELL_EXPORT_LINE_RE.fullmatch(line):
            name = line.split("=", 1)[0]
            print(
                f"FAIL: refusing to emit unsafe $GITHUB_ENV line for {name}",
                file=sys.stderr,
            )
            return 1
    for line in lines:
        print(line)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--shell-export", action="store_true")
    mode.add_argument("--check-release-inputs", action="store_true")
    args = parser.parse_args(argv)

    if args.shell_export:
        return shell_export()
    return check_release_inputs()


if __name__ == "__main__":
    raise SystemExit(main())
