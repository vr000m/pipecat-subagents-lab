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
from pathlib import Path
from typing import Any

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from _evidence_common import (
    EvidenceGateError,
    closed_object,
    load_json,
    require_nonempty_str,
    require_type,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
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


def _lockfile_dependency_anchor() -> tuple[str, str]:
    """Read the pinned package version/integrity from web/bun.lock."""
    text = BUN_LOCK_PATH.read_text(encoding="utf-8")
    match = re.search(
        rf'"{re.escape(PACKAGE_NAME)}":\s*\["{re.escape(PACKAGE_NAME)}@([^"]+)"[^\]]*"(sha512-[^"]+)"\]',
        text,
    )
    if not match:
        raise EvidenceGateError(f"{PACKAGE_NAME} not found in {BUN_LOCK_PATH}")
    return match.group(1), match.group(2)


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
    fake_route_hash = require_nonempty_str(
        record["fake_route_artifact_sha256"], "fake_route_artifact_sha256"
    )
    if len(fake_route_hash) != 64:
        raise EvidenceGateError("fake_route_artifact_sha256 must be a 64-hex-char SHA-256 digest")

    audibility = record["audibility"]
    require_type(audibility, (dict,), "audibility")
    closed_object(audibility, required=AUDIBILITY_REQUIRED, allowed=AUDIBILITY_ALLOWED)
    if audibility["state"] not in {"audibility_verified", "audibility_unverified"}:
        raise EvidenceGateError(f"invalid audibility.state {audibility['state']!r}")

    if audibility["state"] == "audibility_verified":
        missing = AUDIBILITY_VERIFIED_REQUIRED - set(audibility)
        if missing:
            raise EvidenceGateError(f"audibility_verified requires field(s) {sorted(missing)}")
        if audibility["play_result"] != "resolved":
            raise EvidenceGateError("audibility_verified requires play_result == 'resolved'")
        if audibility["checked_source_commit"] != record["source_commit"]:
            raise EvidenceGateError("audibility.checked_source_commit must match source_commit")
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
    except EvidenceGateError as exc:
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
