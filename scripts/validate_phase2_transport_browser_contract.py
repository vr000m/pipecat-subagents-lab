#!/usr/bin/env python3
"""Validate the Phase 2 transport/browser contract artifact.

This is the credential-free gate for the v0.1.3 media-routability spike
(see the "Phase 2: Background-delivery policy tuning" section of
``docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md``).
It hand-checks a JSON object against
``shared/schemas/v013-transport-browser-contract.json`` (no ``jsonschema``
dependency, matching ``validate_v013_evidence.py``'s style), then applies the
promotion predicate: schema validity alone never proves promotion-eligible.

Only an artifact whose pinned ``@pipecat-ai/small-webrtc-transport`` source
anchor matches ``web/package.json``/``web/bun.lock`` *and* whose
``audibility.state`` is ``audibility_verified`` (from a named browser/device
check bound to the exact source/dependency/fake-route artifact) may be
``promotion_eligible``. Every other artifact -- including one that is
otherwise perfectly schema-valid but only exercised fake transport/audio
fakes -- is ``audibility_unverified`` and ``promotion_eligible=false``.
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from scripts.evidence_common import (
    REPO_ROOT,
    EvidenceGateError,
    closed_object,
    load_json,
    require_hex64,
    require_nonempty_str,
    require_type,
)

SCHEMA_PATH = REPO_ROOT / "shared" / "schemas" / "v013-transport-browser-contract.json"
PACKAGE_JSON_PATH = REPO_ROOT / "web" / "package.json"
BUN_LOCK_PATH = REPO_ROOT / "web" / "bun.lock"
PACKAGE_NAME = "@pipecat-ai/small-webrtc-transport"

TOP_REQUIRED = frozenset(
    {
        "status",
        "reason",
        "promotion_eligible",
        "source_commit",
        "source_tree_hash",
        "package_version",
        "package_integrity",
        "source_anchor",
        "fake_route_artifact_sha256",
        "audibility",
    }
)
TOP_ALLOWED = TOP_REQUIRED
AUDIBILITY_REQUIRED = frozenset({"state"})
AUDIBILITY_ALLOWED = frozenset(
    {
        "state",
        "browser_name",
        "browser_version",
        "os_device_name",
        "output_route",
        "prior_user_gesture",
        "check_method",
        "checked_at_utc",
        "runner_identity",
        "checked_source_commit",
        "checked_source_tree_hash",
        "route_artifact_sha256",
        "package_version",
        "package_integrity",
        "play_result",
    }
)
AUDIBILITY_VERIFIED_REQUIRED = frozenset(
    {
        "browser_name",
        "browser_version",
        "os_device_name",
        "output_route",
        "prior_user_gesture",
        "check_method",
        "checked_at_utc",
        "runner_identity",
        "checked_source_commit",
        "checked_source_tree_hash",
        "route_artifact_sha256",
        "package_version",
        "package_integrity",
        "play_result",
    }
)
STATUSES = frozenset(
    {
        "promotion_eligible",
        "evidence_unavailable",
        "blocked",
        "not_run",
        "real_stratum_missing",
        "contaminated",
        "malformed",
        "unverified_source",
        "invalid_fake_route",
        "audibility_unverified",
    }
)


def _require_utc_timestamp(value: str, field: str) -> None:
    """Require ``value`` to parse as an ISO-8601 timestamp.

    The schema (``shared/schemas/v013-transport-browser-contract.json``)
    declares ``checked_at_utc`` as ``format: date-time``, but this validator
    hand-checks the artifact instead of running a JSON Schema library (see
    module docstring), and the hand-check previously only asked whether the
    field was *present* -- not whether its value meant anything. A string
    with no relation to a timestamp (``"not-a-timestamp"``) satisfied that
    presence check and reached a promotion-eligible manifest.
    """
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = f"{text[:-1]}+00:00"
    try:
        datetime.fromisoformat(text)
    except ValueError as exc:
        raise EvidenceGateError(f"{field} must be an ISO-8601 timestamp, got {value!r}") from exc


def _lockfile_dependency_anchor() -> tuple[str, str]:
    """Read the pinned package version/integrity from web/bun.lock.

    Anchored to a line that *starts* with ``"<package>":`` followed
    immediately by an array (``:\\s*\\[``): a dependency reference nested
    inside another package's own ``dependencies``/``peerDependencies`` object
    (e.g. ``"@pipecat-ai/small-webrtc-transport": "^1.10.6"``) has no leading
    array bracket, so it cannot be mistaken for this package's own top-level
    lockfile entry -- the previous unanchored ``re.search`` over the whole
    file could match whichever occurrence came first. The body between the
    version and the integrity hash is matched lazily (``.*?``) rather than
    with ``[^\\]]*``, since a dependency object containing a literal ``]``
    (e.g. an array-valued field) would otherwise stop the match early or fail
    to find the real closing bracket at all.
    """
    for line in BUN_LOCK_PATH.read_text(encoding="utf-8").splitlines():
        stripped = line.strip().rstrip(",")
        if not stripped.startswith(f'"{PACKAGE_NAME}":'):
            continue
        match = re.match(
            rf'"{re.escape(PACKAGE_NAME)}":\s*\["{re.escape(PACKAGE_NAME)}@([^"]+)".*?'
            r'"(sha512-[^"]+)"\]$',
            stripped,
        )
        if match:
            return match.group(1), match.group(2)
    raise EvidenceGateError(f"{PACKAGE_NAME} not found in {BUN_LOCK_PATH}")


def _package_json_declared_version() -> str:
    """The version `@pipecat-ai/small-webrtc-transport` is pinned to in
    web/package.json's own dependency declaration.

    `PACKAGE_JSON_PATH` was previously declared but never read, despite the
    module docstring's claim that it checks "web/package.json/web/bun.lock"
    -- only the lockfile was actually consulted. Reading it here lets
    `validate_artifact` catch package.json/bun.lock drift (an updated
    dependency declaration whose lockfile was never regenerated) instead of
    silently trusting the lockfile alone.

    Read through ``evidence_common.load_json``, not ``read_text`` +
    ``json.loads`` (round-3 restart gauntlet, Logic finding). A malformed or
    truncated web/package.json raised ``json.JSONDecodeError``, which
    subclasses ``ValueError`` and *not* ``OSError`` -- so it sailed past
    ``main()``'s ``except (EvidenceGateError, OSError)`` and surfaced as a raw
    traceback with a nonstandard exit code instead of the gate's structured
    failure. ``load_json`` collapses every read and parse failure into one
    ``EvidenceGateError``, and hardens the read while it is there.
    """
    data = load_json(PACKAGE_JSON_PATH)
    dependencies = data.get("dependencies") if isinstance(data, dict) else None
    version = dependencies.get(PACKAGE_NAME) if isinstance(dependencies, dict) else None
    if not isinstance(version, str) or not version:
        raise EvidenceGateError(f"{PACKAGE_NAME} not found in {PACKAGE_JSON_PATH} dependencies")
    return version


#: The npm range operators this gate understands. Anything outside this set
#: (``||`` unions, hyphen ranges, ``*``/``x`` wildcards, ``workspace:``/URL/
#: git specifiers) is refused rather than guessed at -- see
#: :func:`_declared_range_admits`.
#:
#: The prerelease/build tail is *captured*, not swallowed: an earlier version
#: matched it with a non-capturing ``(?:[-+].*)?`` and discarded it, so
#: ``1.10.6`` "admitted" the lockfile pin ``1.10.6-evil.0`` (and ``1.10.6+evil``,
#: and ``v1.10.6``) even though npm semver satisfies none of them -- a
#: dependency-anchor gate that had been exact string equality before round 3
#: silently loosened into one that accepts a different package build (round-4
#: confirm pass, Security finding). Both sides are now rejected outright when
#: a suffix is present, which is what the docstring below always claimed.
_RANGE_OPERATOR_RE = re.compile(
    r"^(?P<op>\^|~|>=|<=|>|<|=)?\s*(?P<v>v)?"
    r"(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)(?P<suffix>[-+].*)?$"
)


def _declared_range_admits(declared: str, locked: str) -> bool:
    """Does the package.json range `declared` admit the lockfile pin `locked`?

    Round-3 restart gauntlet, Logic finding: this comparison used to be
    ``declared != locked`` string equality. Any range prefix -- ``^1.10.6``,
    ``~1.10.6``, ``>=1.10.6`` -- then failed the gate even when the lockfile
    held a perfectly correct resolution of that range, producing a false
    "regenerate the lockfile" failure. The defect was latent only because
    both files currently hold the bare string ``1.10.6``; it fires on the next
    dependency edit that introduces a caret.

    Deliberately implements a *subset* of npm's range grammar -- exact,
    ``^``, ``~``, ``>=``, ``>``, ``<=``, ``<``, ``=`` against a plain
    ``MAJOR.MINOR.PATCH`` -- and raises for anything else rather than
    guessing. This is an evidence gate: a range shape it cannot reason about
    must fail closed with "pin it exactly", never be waved through. Prerelease
    and build suffixes are likewise refused rather than half-ordered, since
    correct prerelease precedence is more machinery than this gate needs --
    on *both* sides. A suffix on the locked side is the one that matters
    security-wise: ``1.10.6-evil.0`` and ``1.10.6+evil`` are different package
    builds from ``1.10.6``, and npm semver satisfies neither.
    """
    declared_match = _RANGE_OPERATOR_RE.fullmatch(declared.strip())
    locked_match = _RANGE_OPERATOR_RE.fullmatch(locked.strip())
    if declared_match is None or declared_match.group("suffix"):
        raise EvidenceGateError(
            f"web/package.json declares {PACKAGE_NAME}@{declared!r}, a version range this gate "
            "cannot evaluate -- pin an exact version or a simple ^/~/>= range "
            "with no prerelease/build suffix"
        )
    # The locked side must be a bare MAJOR.MINOR.PATCH: no operator, no
    # ``v`` prefix, and no prerelease/build tail. Each of those was previously
    # accepted and then thrown away, so a lockfile pinned to a *different
    # package build* (``1.10.6-evil.0``, ``1.10.6+evil``) compared equal to the
    # declared ``1.10.6`` (round-4 confirm pass, Security finding).
    if (
        locked_match is None
        or locked_match.group("op")
        or locked_match.group("v")
        or locked_match.group("suffix")
    ):
        raise EvidenceGateError(
            f"web/bun.lock resolved {PACKAGE_NAME} to {locked!r}, which is not an exact "
            "MAJOR.MINOR.PATCH version"
        )

    operator = declared_match.group("op") or "="
    want = tuple(int(declared_match.group(name)) for name in ("major", "minor", "patch"))
    have = tuple(int(locked_match.group(name)) for name in ("major", "minor", "patch"))

    if operator == "=":
        return have == want
    if operator == ">=":
        return have >= want
    if operator == ">":
        return have > want
    if operator == "<=":
        return have <= want
    if operator == "<":
        return have < want
    if operator == "~":
        # ~X.Y.Z admits >=X.Y.Z and <X.(Y+1).0.
        return have >= want and have[:2] == want[:2]
    # ^X.Y.Z admits >=X.Y.Z up to the next *left-most non-zero* bump: ^0.2.3
    # is <0.3.0 and ^0.0.3 is <0.0.4, not <1.0.0.
    if want[0] != 0:
        return have >= want and have[0] == want[0]
    if want[1] != 0:
        return have >= want and have[:2] == want[:2]
    return have == want


def validate_artifact(record: dict[str, Any]) -> None:
    closed_object(record, required=TOP_REQUIRED, allowed=TOP_ALLOWED)
    if record["status"] not in STATUSES:
        raise EvidenceGateError(f"invalid status {record['status']!r}")
    require_type(record["reason"], (str, type(None)), "reason")
    require_type(record["promotion_eligible"], (bool,), "promotion_eligible")
    for field in (
        "source_commit",
        "source_tree_hash",
        "package_version",
        "package_integrity",
        "source_anchor",
    ):
        require_nonempty_str(record[field], field)
    # A 64-character string is not a digest: `"z" * 64` satisfies a length
    # check while being no kind of hash at all, so require exact lowercase hex.
    fake_route_hash = require_hex64(
        record["fake_route_artifact_sha256"], "fake_route_artifact_sha256"
    )

    audibility = record["audibility"]
    require_type(audibility, (dict,), "audibility")
    closed_object(audibility, required=AUDIBILITY_REQUIRED, allowed=AUDIBILITY_ALLOWED)
    if audibility["state"] not in {"audibility_verified", "audibility_unverified"}:
        raise EvidenceGateError(f"invalid audibility.state {audibility['state']!r}")

    if audibility["state"] == "audibility_verified":
        missing = AUDIBILITY_VERIFIED_REQUIRED - set(audibility)
        if missing:
            raise EvidenceGateError(f"audibility_verified requires field(s) {sorted(missing)}")
        # Presence alone is not a claim: an empty string, the wrong type, or
        # (for `checked_at_utc`) a value with no relation to a timestamp all
        # satisfied the `missing` check above while carrying no real evidence.
        # Match the schema's per-field type/format so a malformed claim can't
        # flow into a promotion-eligible manifest.
        for field in (
            "browser_name",
            "browser_version",
            "os_device_name",
            "output_route",
            "check_method",
            "runner_identity",
            "checked_source_commit",
            "checked_source_tree_hash",
            "route_artifact_sha256",
            "package_version",
            "package_integrity",
        ):
            require_nonempty_str(audibility[field], f"audibility.{field}")
        require_type(audibility["prior_user_gesture"], (bool,), "audibility.prior_user_gesture")
        checked_at_utc = require_nonempty_str(
            audibility["checked_at_utc"], "audibility.checked_at_utc"
        )
        _require_utc_timestamp(checked_at_utc, "audibility.checked_at_utc")
        if audibility["play_result"] != "resolved":
            raise EvidenceGateError("audibility_verified requires play_result == 'resolved'")
        if audibility["checked_source_commit"] != record["source_commit"]:
            raise EvidenceGateError("audibility.checked_source_commit must match source_commit")
        # A commit alone does not identify the tree that was actually checked:
        # a browser check run from a dirty or different working tree at the
        # same commit must not be attachable to this release.
        if audibility["checked_source_tree_hash"] != record["source_tree_hash"]:
            raise EvidenceGateError(
                "audibility.checked_source_tree_hash must match source_tree_hash"
            )
        if audibility["route_artifact_sha256"] != fake_route_hash:
            raise EvidenceGateError(
                "audibility.route_artifact_sha256 must match fake_route_artifact_sha256"
            )
        if (
            audibility["package_version"] != record["package_version"]
            or audibility["package_integrity"] != record["package_integrity"]
        ):
            raise EvidenceGateError(
                "audibility package version/integrity must match the top-level anchor"
            )

    # Dependency-anchor drift check: the artifact's declared package
    # version/integrity must match the lockfile's pinned dependency exactly.
    locked_version, locked_integrity = _lockfile_dependency_anchor()
    if (
        record["package_version"] != locked_version
        or record["package_integrity"] != locked_integrity
    ):
        raise EvidenceGateError(
            "package_version/package_integrity do not match web/bun.lock's pinned dependency"
        )
    # web/package.json is the declared dependency; web/bun.lock is its
    # resolved lock. If a package.json version bump was never followed by a
    # lockfile regeneration, the artifact could still match the (now stale)
    # lockfile pin while silently describing a dependency that package.json
    # itself no longer declares.
    #
    # Compared as "does the declared range admit the locked pin", not as
    # string equality: the lockfile stores a resolved exact version while
    # package.json may legitimately declare a range.
    declared_version = _package_json_declared_version()
    if not _declared_range_admits(declared_version, locked_version):
        raise EvidenceGateError(
            f"web/package.json declares {PACKAGE_NAME}@{declared_version} but web/bun.lock "
            f"is pinned to {locked_version}, which that range does not admit -- "
            "regenerate the lockfile"
        )

    # `source_anchor` names the source the check was actually run against.
    # Validating it only as "a non-empty string" let a verified artifact claim
    # an arbitrary anchor (a fork, a different package, an unrelated URL)
    # while still passing the version/integrity checks above, so it must name
    # the pinned package *and* the locked version. The exact anchor spelling
    # is deliberately free-form (a lockfile reference or an upstream source
    # URL are both legitimate), so this binds the two identifying substrings
    # rather than one fixed format.
    #
    # Matching only the bare leaf name (`"small-webrtc-transport"`) let an
    # anchor for a completely unrelated repository that merely happens to
    # share that leaf segment -- e.g. `https://evil.example/
    # small-webrtc-transport/tree/v1.10.6` -- pass as if it named the real
    # `@pipecat-ai/small-webrtc-transport` package. Require the scoped
    # org/leaf pair together, which both the npm-scoped spelling
    # (`@pipecat-ai/small-webrtc-transport`) and the GitHub org/repo URL
    # spelling (`.../pipecat-ai/small-webrtc-transport/...`) satisfy, but an
    # anchor naming only the leaf does not.
    anchor = record["source_anchor"]
    package_org_leaf = PACKAGE_NAME.lstrip("@")
    if package_org_leaf not in anchor or locked_version not in anchor:
        raise EvidenceGateError(
            f"source_anchor {anchor!r} does not name the pinned package "
            f"{PACKAGE_NAME!r} at the locked version {locked_version!r}"
        )

    # The promotion predicate: schema validity alone is never sufficient.
    eligible = (
        record["status"] == "promotion_eligible" and audibility["state"] == "audibility_verified"
    )
    if record["promotion_eligible"] != eligible:
        raise EvidenceGateError(
            f"promotion_eligible={record['promotion_eligible']!r} does not match the computed "
            f"predicate ({eligible!r}) for status={record['status']!r}/"
            f"audibility.state={audibility['state']!r}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    args = parser.parse_args(argv)

    try:
        record = load_json(args.input)
        if not isinstance(record, dict):
            raise EvidenceGateError("artifact must be a JSON object")
        validate_artifact(record)
    except (EvidenceGateError, OSError) as exc:
        # `load_json` now wraps read failures, but `validate_artifact` also
        # reads `web/bun.lock` directly; an unreadable lockfile must still be
        # a controlled non-zero exit, not a traceback.
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    status = record["status"]
    eligible = record["promotion_eligible"]
    print(
        f"OK: transport/browser contract validated (status={status}, promotion_eligible={eligible})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
