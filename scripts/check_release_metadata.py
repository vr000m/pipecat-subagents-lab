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
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

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


#: Commands that report rather than consume: a path passed to one of these is
#: a message about the manifest, never a use of it. Such a command is skipped
#: entirely, because tokenising alone cannot tell a mention from a use --
#: ``echo Regenerate docs/benchmarks/...json when bumping`` (unquoted) yields
#: the path as its own shell word and satisfied the gate
#: (round 8 confirm pass 5, Security Minor). This is now defence in depth
#: rather than the primary decision: :func:`_verifies_manifest` requires the
#: verifier, its flag, and the path to belong to ONE command's argv, which an
#: ``echo`` cannot satisfy without literally being handed all three
#: (round 9 confirm pass 6, Security Important).
_REPORTING_COMMANDS = frozenset({"echo", "printf", ":", "true", "false"})

#: Wrapper programs that run another command: the *effective* command name is
#: the first word past them (``env echo x`` is an ``echo``). Matched together
#: with basename normalisation and ``VAR=value`` prefix skipping so
#: ``/bin/echo``, ``env echo``, and ``LC_ALL=C echo`` are all recognised as
#: ``echo`` (round 9 confirm pass 6, Security Important).
_COMMAND_WRAPPERS = frozenset({"env", "command", "builtin", "exec", "nohup", "time", "sudo"})

#: Tokens ``shlex`` emits (with ``punctuation_chars``) that end one command and
#: begin another, so ``echo hi && uv run x.py path`` keeps ``path``.
_COMMAND_SEPARATORS = frozenset({";", "|", "||", "&", "&&", "(", ")"})

#: Redirection operators: the word after one names a file the shell opens, not
#: an argument the command was invoked with, so neither is part of the argv.
_REDIRECTIONS = frozenset({"<", ">", ">>", "<<", "<<<", ">|", "<>", "&>", "&>>"})

#: A heredoc introducer (``<<EOF``, ``<<-'EOF'``). Its BODY is data the command
#: reads, not commands the shell runs -- a body line spelling out a verifier
#: invocation would otherwise tokenise into a qualifying argv
#: (round 9 confirm pass 6, Security Important).
_HEREDOC_RE = re.compile(r"<<-?\s*(['\"]?)([A-Za-z_][A-Za-z0-9_]*)\1")


def _tokenise(text: str) -> list[str]:
    """Comment-stripped, quote-aware tokens, with shell punctuation separated.

    Raises ``ValueError`` on input shlex cannot tokenise.
    """
    lexer = shlex.shlex(text, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    return list(lexer)


def _without_heredoc_bodies(lines: list[str]) -> list[str]:
    """``lines`` with every heredoc body dropped, introducer lines kept."""
    kept: list[str] = []
    delimiter: str | None = None
    for line in lines:
        if delimiter is not None:
            if line.strip() == delimiter:
                delimiter = None
            continue
        match = _HEREDOC_RE.search(line)
        if match:
            delimiter = match.group(2)
        kept.append(line)
    return kept


def _commands(command: str) -> list[list[str]]:
    """The argv lists of every command in a workflow ``run:`` script.

    Tokenising rather than substring-matching is what stops a ``#`` comment or
    a *quoted* ``echo`` message from satisfying the manifest-path gate: shlex
    drops comments outright, and a path mentioned inside a quoted sentence
    comes back as one long word that cannot equal the path.

    Returning one argv list PER COMMAND rather than a step-wide union of words
    is what stops the three tokens the gate looks for from being contributed by
    three different commands: verifying the *previous* release's manifest in
    one command and merely naming the current path in a second (``ls``,
    ``test -f``, ``/bin/echo``) satisfied the union form while the drift check
    ran against the wrong file (round 9 confirm pass 6, Security Important).

    Line continuations are joined before tokenising: a ``run: |`` script
    written in the standard ``\\``-continued style put the manifest path on a
    line shlex could not tokenise on its own (``ValueError: No escaped
    character``), so a *correct* workflow failed the gate
    (round 8 confirm pass 5, Logic Minor).

    Lines are then tokenised independently, because shlex treats a newline as
    plain whitespace and would otherwise let one line's ``echo`` swallow the
    next line's command. A line shlex still cannot tokenise (an unbalanced
    quote -- including one legitimately opened on one line and closed on the
    next) contributes nothing rather than raising, so one malformed step
    cannot mask a well-formed sibling that does reference the manifest.
    """
    commands: list[list[str]] = []
    lines = _without_heredoc_bodies(command.replace("\\\n", " ").splitlines())
    for line in lines:
        try:
            tokens = _tokenise(line)
        except ValueError:
            continue
        argv: list[str] = []
        drop_next = False
        for token in [*tokens, ";"]:
            if drop_next:
                drop_next = False
                continue
            if token in _REDIRECTIONS:
                drop_next = True
                continue
            if token in _COMMAND_SEPARATORS:
                if argv:
                    commands.append(argv)
                argv = []
            else:
                argv.append(token)
    return commands


def _command_name(argv: list[str]) -> str:
    """The basename of the program ``argv`` actually invokes, or ``""``.

    ``VAR=value`` assignment prefixes, wrapper programs (:data:`_COMMAND_WRAPPERS`)
    and their flags are stepped over, and the result is basenamed, so
    ``/bin/echo``, ``env -i echo`` and ``LC_ALL=C echo`` all answer ``echo``.
    """
    for token in argv:
        if "=" in token and token.split("=", 1)[0].isidentifier():
            continue
        if token.startswith("-"):
            continue
        name = PurePosixPath(token).name
        if name in _COMMAND_WRAPPERS:
            continue
        return name
    return ""


#: The script (and its flag) whose invocation is what actually proves the
#: committed manifest has not drifted. Requiring the manifest path to appear
#: alongside these -- rather than merely as some shell word somewhere -- is
#: what makes the gate check a USE of the path rather than a mention of it
#: (round 8 confirm pass 5, Security Minor).
_MANIFEST_VERIFIER = "scripts/validate_v013_evidence.py"
_VERIFY_MANIFEST_FLAG = "--verify-manifest"


def _is_truthy(raw: object) -> bool:
    """Whether a workflow field YAML-parses to something GitHub treats as true.

    ``continue-on-error: true`` parses to a bool; ``continue-on-error:
    ${{ ... }}`` parses to a non-empty string, which is switchable at will and
    so is treated as truthy here too. Only an explicit ``false``/absent value
    leaves the gate armed.
    """
    if raw is None:
        return False
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() not in ("", "false", "0")


def _verifies_manifest(command: str, expected: str) -> bool:
    """Whether ``command`` actually runs the drift verifier against ``expected``.

    The verifier, its flag, and the expected path must co-occur in ONE
    command's argv. Testing set-membership against the step-wide union of
    words let the three be supplied by three different commands: a step that
    verified the *previous* release's manifest and then merely named the
    current path in a second command passed, while the drift check ran against
    the wrong file (round 9 confirm pass 6, Security Important).

    Fail-closed on indirect invocation, by decision (post-release hardening
    P3, Requirement 2 -- program row 14): argv words are compared literally,
    so ``M=docs/...json`` followed by ``--verify-manifest "$M"`` is NOT
    recognized even though the shell would expand it into a real
    verification. That false-negative is the accepted cost: recognizing it
    would require a partial shell-expansion interpreter (assignments,
    ``${VAR}``, command substitution -- an open-ended axis this checker
    deliberately avoids by requiring all three words literally in one argv).
    CI must spell the manifest path out; otherwise this gate fails closed
    with "no step runs the verifier".
    """
    required = {_MANIFEST_VERIFIER, _VERIFY_MANIFEST_FLAG, expected}
    return any(
        _command_name(argv) not in _REPORTING_COMMANDS
        and required <= {word.removeprefix("./") for word in argv}
        for argv in _commands(command)
    )


def _conditional_needs_ancestor(jobs: Mapping[str, object], job_name: str) -> str | None:
    """The name of a transitive ``needs:`` ancestor of ``job_name`` that is
    itself switchable (carries an ``if:``), or is missing from the workflow.

    GitHub skips a job when any job in its ``needs:`` is skipped, so an
    ancestor's ``if: false`` disables the drift gate as completely as the drift
    job's own would -- and the job still exists, unconditional, so the
    job-level checks stay green (round 9 confirm pass 6, Security Minor).

    ``needs:`` itself is emphatically NOT rejected: the real workflow's
    ``needs: test`` is legitimate and load-bearing. Only ancestor
    *conditionality* is. ``continue-on-error`` on an ancestor is likewise
    fine -- it makes the ancestor succeed, which lets the dependent run.
    """
    seen: set[str] = set()
    pending = [job_name]
    while pending:
        current = pending.pop()
        job = jobs.get(current)
        needs = job.get("needs") if isinstance(job, dict) else None
        for name in (
            [needs] if isinstance(needs, str) else (needs if isinstance(needs, list) else [])
        ):
            if not isinstance(name, str) or name in seen:
                continue
            seen.add(name)
            ancestor = jobs.get(name)
            if not isinstance(ancestor, dict) or "if" in ancestor:
                return name
            pending.append(name)
    return None


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

    Neither of those was enough either, so (round 8 confirm pass 5, Security
    Minor):

    * ``continue-on-error`` is rejected on the job, and disqualifies a step,
      exactly as ``if:`` is. It disables the gate more completely than
      ``if: false``: the drift check runs, reports drift, and CI stays green.
    * a step qualifies only when it runs ``_MANIFEST_VERIFIER`` with
      ``_VERIFY_MANIFEST_FLAG`` and the expected path, not when it merely
      names the path as a word. Whole-word matching only defeated the
      *quoted* echo bypass; ``run: echo Regenerate docs/benchmarks/...json
      when bumping`` yields the path as its own word. Requiring the verifier
      invocation checks a USE of the path rather than a mention (and
      :func:`_command_name` additionally identifies ``echo``-like reporting
      commands through wrappers and path spellings).

    None of THAT was enough either, so (round 9 confirm pass 6, Security
    Important + Minor):

    * the verifier, its flag, and the expected path must co-occur in ONE
      command's argv (:func:`_commands`). Set-membership over the step-wide
      union of words let a step verify the PREVIOUS release's manifest in one
      command and merely name the current path in a second (``ls``,
      ``test -f``, ``/bin/echo``, a ``cat <<EOF`` body) -- the exact
      mention-vs-use bypass the paragraph above claims to have closed.
    * a transitive ``needs:`` ancestor carrying an ``if:`` is rejected. GitHub
      skips a job when any job it needs is skipped, so an ancestor's
      ``if: false`` disables this gate while the drift job itself stays
      unconditional and green. ``needs:`` itself is not rejected -- the real
      workflow's ``needs: test`` is legitimate.

    Not checked, deliberately: the workflow's ``on:`` triggers. Which events
    run CI is a routine, legitimately-varying choice (this repo's gates run on
    push and pull_request; a fork may reasonably differ), so pinning it here
    would fail for reasons unrelated to release-version drift. The ``if:``/
    ``continue-on-error`` rejections are narrow because neither has a
    legitimate use on *this* job: the gate is cheap, must run whenever CI
    runs, and exists only to fail.
    """
    try:
        text = read_bytes_no_follow(ci_yml_path).decode("utf-8")
    except (OSError, EvidenceGateError, UnicodeDecodeError) as exc:
        raise ReleaseMetadataError(f"{ci_yml_path}: cannot read CI workflow: {exc}") from exc
    try:
        workflow = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ReleaseMetadataError(f"{ci_yml_path}: invalid YAML: {exc}") from exc
    raw_jobs = workflow.get("jobs") if isinstance(workflow, dict) else None
    jobs: dict[str, object] = raw_jobs if isinstance(raw_jobs, dict) else {}
    job = jobs.get(_MANIFEST_DRIFT_JOB)
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
    if _is_truthy(job.get("continue-on-error")):
        raise ReleaseMetadataError(
            f"{ci_yml_path}: the `{_MANIFEST_DRIFT_JOB}` job carries "
            f"`continue-on-error: {job['continue-on-error']!r}` -- the drift check would run, "
            "report drift, and leave the workflow green, which disables the gate as "
            "completely as `if: false`"
        )
    switchable_ancestor = _conditional_needs_ancestor(jobs, _MANIFEST_DRIFT_JOB)
    if switchable_ancestor is not None:
        raise ReleaseMetadataError(
            f"{ci_yml_path}: the `{_MANIFEST_DRIFT_JOB}` job transitively `needs:` "
            f"`{switchable_ancestor}`, which is itself conditional or missing -- GitHub skips a "
            "job when any job it needs is skipped, so that ancestor can switch the drift check "
            "off while this gate stays green"
        )
    steps = job.get("steps")
    expected = f"docs/benchmarks/v{version}-promotion-manifest.json"
    matching = [
        step
        for step in (steps if isinstance(steps, list) else [])
        if isinstance(step, dict)
        and isinstance(step.get("run"), str)
        and _verifies_manifest(step["run"], expected)
    ]
    if not matching:
        raise ReleaseMetadataError(
            f"{ci_yml_path}: no `{_MANIFEST_DRIFT_JOB}` step runs "
            f"{_MANIFEST_VERIFIER!r} with {_VERIFY_MANIFEST_FLAG} against {expected!r} -- "
            "either the release version changed without updating the manifest path here "
            "(and renaming/regenerating the committed manifest file to match), or the "
            "step that actually verifies it is gone"
        )
    # One step that is neither conditional nor continue-on-error is enough; a
    # *sibling* step may legitimately carry either. Only a roster where every
    # reference is switchable means the gate can be turned off while staying
    # green (round 8 confirm pass 5, Security Minor).
    if not any(
        "if" not in step and not _is_truthy(step.get("continue-on-error")) for step in matching
    ):
        names = [str(step.get("name", "<unnamed>")) for step in matching]
        raise ReleaseMetadataError(
            f"{ci_yml_path}: every `{_MANIFEST_DRIFT_JOB}` step verifying {expected!r} "
            f"carries an `if:` or `continue-on-error:` ({names}) -- such a step can be "
            "switched off, or fail harmlessly, while still satisfying this gate"
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
