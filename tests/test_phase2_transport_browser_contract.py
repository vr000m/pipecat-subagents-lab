"""Negative and blocked-artifact tests for the Phase 2 transport/browser
contract gate (``scripts/validate_phase2_transport_browser_contract.py``).

Plan: docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md,
Phase 2 bullets 185-188. The validator checks a single JSON artifact
(``docs/benchmarks/v0.1.3-phase2-transport-browser-contract.json``-shaped)
against ``shared/schemas/v013-transport-browser-contract.json`` plus the
cross-check against ``web/package.json``/``web/bun.lock`` for the pinned
``@pipecat-ai/small-webrtc-transport`` dependency, then applies the
promotion predicate: an artifact is ``promotion_eligible`` only when
``status == "promotion_eligible"`` *and* ``audibility.state ==
"audibility_verified"`` -- schema validity alone never proves eligibility.

This module imports the script and ``scripts/_evidence_common.py`` by path
(matching the ``_load_validator()`` pattern already used in
tests/test_v013_perf_scenarios.py) since ``scripts/`` is not a package.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
BUN_LOCK_PATH = REPO_ROOT / "web" / "bun.lock"
PACKAGE_NAME = "@pipecat-ai/small-webrtc-transport"


def _locked_dependency_anchor() -> tuple[str, str]:
    import re

    text = BUN_LOCK_PATH.read_text(encoding="utf-8")
    match = re.search(
        rf'"{re.escape(PACKAGE_NAME)}":\s*\["{re.escape(PACKAGE_NAME)}@([^"]+)"[^\]]*"(sha512-[^"]+)"\]',
        text,
    )
    assert match is not None, f"{PACKAGE_NAME} not found in {BUN_LOCK_PATH}"
    return match.group(1), match.group(2)


PINNED_PACKAGE_VERSION, PINNED_PACKAGE_INTEGRITY = _locked_dependency_anchor()
SOURCE_COMMIT = "a" * 40
SOURCE_TREE_HASH = "b" * 64
FAKE_ROUTE_SHA256 = "c" * 64
SOURCE_ANCHOR = "https://github.com/pipecat-ai/small-webrtc-transport/tree/v1.10.6"


def _load_validator() -> Any:
    import scripts.validate_phase2_transport_browser_contract

    return scripts.validate_phase2_transport_browser_contract


def _valid_audibility(**overrides: Any) -> dict[str, Any]:
    payload = {
        "state": "audibility_verified",
        "browser_name": "Chrome",
        "browser_version": "128.0.6613.0",
        "os_device_name": "macOS / MacBook Pro",
        "output_route": "default",
        "prior_user_gesture": True,
        "check_method": "manual-named-browser",
        "checked_at_utc": "2026-08-04T00:00:00Z",
        "runner_identity": "varun@varunsingh.net",
        "checked_source_commit": SOURCE_COMMIT,
        "checked_source_tree_hash": SOURCE_TREE_HASH,
        "route_artifact_sha256": FAKE_ROUTE_SHA256,
        "package_version": PINNED_PACKAGE_VERSION,
        "package_integrity": PINNED_PACKAGE_INTEGRITY,
        "play_result": "resolved",
    }
    payload.update(overrides)
    return payload


def _valid_artifact(**overrides: Any) -> dict[str, Any]:
    payload = {
        "status": "promotion_eligible",
        "reason": None,
        "promotion_eligible": True,
        "source_commit": SOURCE_COMMIT,
        "source_tree_hash": SOURCE_TREE_HASH,
        "package_version": PINNED_PACKAGE_VERSION,
        "package_integrity": PINNED_PACKAGE_INTEGRITY,
        "source_anchor": SOURCE_ANCHOR,
        "fake_route_artifact_sha256": FAKE_ROUTE_SHA256,
        "audibility": _valid_audibility(),
    }
    payload.update(overrides)
    return payload


def _write(tmp_path: Path, payload: Any, name: str = "artifact.json") -> Path:
    path = tmp_path / name
    if isinstance(payload, str):
        path.write_text(payload, encoding="utf-8")
    else:
        path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_validator_rejects_malformed_json(tmp_path: Path) -> None:
    module = _load_validator()
    path = _write(tmp_path, "{not valid json")
    exit_code = module.main(["--input", str(path)])
    assert exit_code != 0


def test_validator_rejects_a_missing_artifact_file(tmp_path: Path) -> None:
    module = _load_validator()
    missing = tmp_path / "does-not-exist.json"
    exit_code = module.main(["--input", str(missing)])
    assert exit_code != 0


def test_validator_rejects_a_blocked_status_claiming_promotion_eligible(tmp_path: Path) -> None:
    module = _load_validator()
    payload = _valid_artifact(
        status="blocked", reason="fake route unverified", promotion_eligible=True
    )
    path = _write(tmp_path, payload)
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_accepts_a_blocked_status_correctly_marked_promotion_ineligible(
    tmp_path: Path,
) -> None:
    module = _load_validator()
    payload = _valid_artifact(
        status="blocked",
        reason="fake route unverified",
        promotion_eligible=False,
        audibility=_valid_audibility(state="audibility_unverified"),
    )
    path = _write(tmp_path, payload)
    module.validate_artifact(json.loads(path.read_text()))  # must not raise


def test_validator_rejects_unavailable_only_strata_claiming_eligibility(tmp_path: Path) -> None:
    """Plan: 'A schema-valid artifact with only unavailable provider/model
    values is valid evidence for credential-free lifecycle tests but is not
    a promotion input.'"""
    module = _load_validator()
    payload = _valid_artifact(
        status="evidence_unavailable",
        reason="unavailable-only strata",
        promotion_eligible=True,
    )
    path = _write(tmp_path, payload)
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_rejects_real_stratum_missing_claiming_eligibility(tmp_path: Path) -> None:
    module = _load_validator()
    payload = _valid_artifact(
        status="real_stratum_missing", reason="no real strata", promotion_eligible=True
    )
    path = _write(tmp_path, payload)
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_rejects_contaminated_artifacts_claiming_eligibility(tmp_path: Path) -> None:
    module = _load_validator()
    payload = _valid_artifact(
        status="contaminated", reason="cache contamination", promotion_eligible=True
    )
    path = _write(tmp_path, payload)
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_rejects_an_unverified_source_contract(tmp_path: Path) -> None:
    """Plan: 'An unverified source contract or fake route is a blocking
    blocked result and leaves the policy disabled.'"""
    module = _load_validator()
    payload = _valid_artifact(
        status="unverified_source", reason="source anchor drifted", promotion_eligible=True
    )
    path = _write(tmp_path, payload)
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_rejects_an_invalid_fake_route_hash_shape(tmp_path: Path) -> None:
    module = _load_validator()
    payload = _valid_artifact(fake_route_artifact_sha256="not-a-sha256")
    path = _write(tmp_path, payload)
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_rejects_a_64_char_non_hex_fake_route_digest(tmp_path: Path) -> None:
    """Regression: the digest check was a *length* check, so a 64-character
    string that is no kind of hash (``"z" * 64``) passed it."""
    module = _load_validator()
    payload = _valid_artifact(fake_route_artifact_sha256="z" * 64)
    path = _write(tmp_path, payload)
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_rejects_an_uppercase_hex_fake_route_digest(tmp_path: Path) -> None:
    module = _load_validator()
    payload = _valid_artifact(fake_route_artifact_sha256="C" * 64)
    path = _write(tmp_path, payload)
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_rejects_a_source_anchor_that_names_a_foreign_source(tmp_path: Path) -> None:
    """Regression: ``source_anchor`` was validated only as a non-empty string,
    so a verified artifact could name an arbitrary source while still passing
    the lockfile version/integrity checks."""
    module = _load_validator()
    path = _write(tmp_path, _valid_artifact(source_anchor="https://example.invalid/some-fork"))
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_rejects_a_source_anchor_naming_a_different_version(tmp_path: Path) -> None:
    module = _load_validator()
    path = _write(
        tmp_path,
        _valid_artifact(
            source_anchor="https://github.com/pipecat-ai/small-webrtc-transport/tree/v0.0.1"
        ),
    )
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_rejects_a_verified_claim_missing_the_checked_source_tree_hash(
    tmp_path: Path,
) -> None:
    """Regression: ``audibility_verified`` bound only ``checked_source_commit``,
    so a browser check run from a different working tree at the same commit
    could be attached to this release."""
    module = _load_validator()
    audibility = _valid_audibility()
    del audibility["checked_source_tree_hash"]
    path = _write(tmp_path, _valid_artifact(audibility=audibility))
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_rejects_a_checked_source_tree_hash_from_a_different_tree(
    tmp_path: Path,
) -> None:
    module = _load_validator()
    audibility = _valid_audibility(checked_source_tree_hash="e" * 64)
    path = _write(tmp_path, _valid_artifact(audibility=audibility))
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_main_cli_returns_nonzero_for_an_unreadable_input_file(tmp_path: Path) -> None:
    """Regression: refactoring onto the shared ``load_json`` helper narrowed
    the CLI's exception handling to ``EvidenceGateError`` only, so a
    permission-denied input raised an uncaught traceback instead of the
    validator's controlled non-zero exit."""
    module = _load_validator()
    path = _write(tmp_path, _valid_artifact())
    path.chmod(0o000)
    try:
        exit_code = module.main(["--input", str(path)])
    finally:
        path.chmod(0o644)
    assert exit_code == 1


def test_validator_records_audibility_unverified_when_no_named_browser_device_check_exists(
    tmp_path: Path,
) -> None:
    module = _load_validator()
    payload = _valid_artifact(
        status="audibility_unverified",
        reason="not-done",
        promotion_eligible=False,
        audibility={"state": "audibility_unverified"},
    )
    path = _write(tmp_path, payload)
    module.validate_artifact(json.loads(path.read_text()))  # must not raise


def test_validator_rejects_a_forged_audibility_verified_claim_missing_required_fields(
    tmp_path: Path,
) -> None:
    """Plan: 'Add validator cases for forged or under-specified verified
    claims.' A claimed audibility_verified state with an incomplete evidence
    object (missing browser_name) is schema-invalid."""
    module = _load_validator()
    audibility = _valid_audibility()
    del audibility["browser_name"]
    path = _write(tmp_path, _valid_artifact(audibility=audibility))
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_rejects_a_verified_claim_whose_play_result_did_not_resolve(
    tmp_path: Path,
) -> None:
    module = _load_validator()
    path = _write(tmp_path, _valid_artifact(audibility=_valid_audibility(play_result="rejected")))
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_rejects_dependency_version_integrity_mismatch_against_the_lockfile(
    tmp_path: Path,
) -> None:
    """Plan: 'The validator reads web/package.json and web/bun.lock as the
    source of truth and rejects an artifact whose dependency version/
    integrity/source anchor differs from the lockfile or installed pinned
    source.'"""
    module = _load_validator()
    path = _write(
        tmp_path,
        _valid_artifact(
            package_version="0.0.1",
            package_integrity="sha512-forgedforgedforgedforgedforgedforgedforgedforgedforgedforge==",
        ),
    )
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_rejects_unknown_top_level_fields_because_the_schema_is_closed(
    tmp_path: Path,
) -> None:
    """Plan: 'v013-transport-browser-contract.json is closed
    (additionalProperties=false).'"""
    module = _load_validator()
    payload = _valid_artifact()
    payload["unexpected_field"] = "surprise"
    path = _write(tmp_path, payload)
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_accepts_a_fully_valid_named_browser_device_artifact_as_promotion_eligible(
    tmp_path: Path,
) -> None:
    module = _load_validator()
    path = _write(tmp_path, _valid_artifact())
    record = json.loads(path.read_text())
    module.validate_artifact(record)  # must not raise
    assert record["status"] == "promotion_eligible"
    assert record["audibility"]["state"] == "audibility_verified"
    assert record["promotion_eligible"] is True


def test_main_cli_returns_nonzero_for_an_artifact_that_misreports_eligibility(
    tmp_path: Path,
) -> None:
    module = _load_validator()
    path = _write(tmp_path, _valid_artifact(status="blocked", promotion_eligible=True))
    exit_code = module.main(["--input", str(path)])
    assert exit_code != 0


def test_main_cli_returns_zero_for_a_valid_promotion_eligible_artifact(tmp_path: Path) -> None:
    module = _load_validator()
    path = _write(tmp_path, _valid_artifact())
    exit_code = module.main(["--input", str(path)])
    assert exit_code == 0
