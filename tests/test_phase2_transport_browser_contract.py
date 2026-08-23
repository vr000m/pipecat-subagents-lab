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

This module imports the script and ``scripts/evidence_common.py`` by path
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


def test_validator_rejects_an_empty_browser_name_in_a_verified_claim(tmp_path: Path) -> None:
    """Regression: ``audibility_verified`` required-field checking only
    checked *presence* (``AUDIBILITY_VERIFIED_REQUIRED - set(audibility)``),
    so an empty ``browser_name`` -- present but content-free -- satisfied the
    check and reached a promotion-eligible manifest."""
    module = _load_validator()
    audibility = _valid_audibility(browser_name="")
    path = _write(tmp_path, _valid_artifact(audibility=audibility))
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_rejects_a_non_bool_prior_user_gesture_in_a_verified_claim(
    tmp_path: Path,
) -> None:
    """Regression: ``prior_user_gesture`` is documented/schema'd as a
    boolean, but only its presence was checked, so the string ``"true"``
    (truthy, but not the boolean the schema requires) passed."""
    module = _load_validator()
    audibility = _valid_audibility(prior_user_gesture="true")
    path = _write(tmp_path, _valid_artifact(audibility=audibility))
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_rejects_an_unparseable_checked_at_utc_in_a_verified_claim(
    tmp_path: Path,
) -> None:
    """Regression: ``checked_at_utc`` is schema'd as ``format: date-time``,
    but the Python validator never parsed it, so any non-empty string --
    including a value with no relation to a timestamp -- passed."""
    module = _load_validator()
    audibility = _valid_audibility(checked_at_utc="not-a-timestamp")
    path = _write(tmp_path, _valid_artifact(audibility=audibility))
    with pytest.raises(module.EvidenceGateError):
        module.validate_artifact(json.loads(path.read_text()))


def test_validator_rejects_a_source_anchor_matching_only_the_bare_leaf_name(
    tmp_path: Path,
) -> None:
    """Regression: the source-anchor check matched an unscoped substring of
    the package leaf name (``"small-webrtc-transport" in anchor``), so a
    lookalike URL for a completely different, unrelated repository that
    merely contains the leaf name and the locked version passed as if it
    named the real ``@pipecat-ai/small-webrtc-transport`` source."""
    module = _load_validator()
    forged_anchor = f"https://evil.example/small-webrtc-transport/tree/v{PINNED_PACKAGE_VERSION}"
    path = _write(tmp_path, _valid_artifact(source_anchor=forged_anchor))
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


def test_malformed_package_json_is_a_gate_error_not_a_raw_traceback(
    tmp_path: Path, monkeypatch
) -> None:
    """Round-3 restart gauntlet, Logic finding: ``_package_json_declared_version``
    used ``json.loads(PACKAGE_JSON_PATH.read_text(...))`` with no error
    handling.

    ``json.JSONDecodeError`` subclasses ``ValueError``, not ``OSError``, so a
    truncated or malformed web/package.json sailed past ``main()``'s
    ``except (EvidenceGateError, OSError)`` and produced a raw traceback and a
    nonstandard exit code instead of the gate's structured failure. It now
    reads through ``load_json``, which collapses every read/parse failure into
    one ``EvidenceGateError``.
    """
    module = _load_validator()
    broken = tmp_path / "package.json"
    broken.write_text('{"dependencies": {', encoding="utf-8")
    monkeypatch.setattr(module, "PACKAGE_JSON_PATH", broken)

    with pytest.raises(module.EvidenceGateError):
        module._package_json_declared_version()

    # And end-to-end through the CLI: a controlled exit code, not a traceback.
    artifact = _write(tmp_path, _valid_artifact())
    assert module.main(["--input", str(artifact)]) == 1


class TestDeclaredRangeAdmitsTheLockedPin:
    """Round-3 restart gauntlet, Logic finding: package.json's dependency spec
    was compared to the lockfile's *resolved* version with ``!=``.

    Any range prefix (``^1.10.6``, ``~1.10.6``, ``>=1.10.6``) failed that
    equality even when the lockfile pin was a correct resolution of the range,
    producing a false "regenerate the lockfile" gate failure. Latent only
    because both files currently hold the bare string ``1.10.6``; it fires on
    the next dependency edit that introduces a caret.
    """

    def _admits(self, declared: str, locked: str) -> bool:
        return _load_validator()._declared_range_admits(declared, locked)

    def test_exact_pin_still_requires_equality(self) -> None:
        assert self._admits("1.10.6", "1.10.6") is True
        assert self._admits("1.10.6", "1.10.7") is False

    def test_caret_admits_a_later_minor_and_patch_in_the_same_major(self) -> None:
        assert self._admits("^1.10.6", "1.10.6") is True
        assert self._admits("^1.10.6", "1.11.0") is True
        assert self._admits("^1.10.6", "1.10.5") is False
        assert self._admits("^1.10.6", "2.0.0") is False

    def test_caret_on_a_zero_major_is_bounded_by_the_minor(self) -> None:
        """``^0.2.3`` is ``<0.3.0``, not ``<1.0.0`` -- the left-most non-zero
        rule. Getting this wrong would wave through a breaking 0.x bump."""
        assert self._admits("^0.2.3", "0.2.9") is True
        assert self._admits("^0.2.3", "0.3.0") is False
        assert self._admits("^0.0.3", "0.0.3") is True
        assert self._admits("^0.0.3", "0.0.4") is False

    def test_tilde_is_bounded_by_the_minor(self) -> None:
        assert self._admits("~1.10.6", "1.10.9") is True
        assert self._admits("~1.10.6", "1.11.0") is False

    def test_comparison_operators(self) -> None:
        assert self._admits(">=1.10.6", "2.0.0") is True
        assert self._admits(">=1.10.6", "1.10.5") is False
        assert self._admits(">1.10.6", "1.10.6") is False
        assert self._admits("<2.0.0", "1.99.0") is True
        assert self._admits("<=1.10.6", "1.10.6") is True

    def test_an_unevaluatable_range_fails_closed(self) -> None:
        """This is an evidence gate: a range shape it cannot reason about must
        raise "pin it exactly", never be silently admitted."""
        module = _load_validator()
        for spec in ("*", "1.x", "^1.0.0 || ^2.0.0", "1.0.0 - 2.0.0", "workspace:*", "latest"):
            with pytest.raises(module.EvidenceGateError):
                self._admits(spec, "1.10.6")

    def test_a_prerelease_or_build_suffixed_pin_fails_closed(self) -> None:
        """Round-4 confirm pass, Security finding (reproduced before the fix):
        the range regex matched the prerelease/build tail with a
        non-capturing ``(?:[-+].*)?`` and threw it away, so ``1.10.6``
        "admitted" the lockfile pins ``1.10.6-evil.0``, ``1.10.6+evil`` and
        ``v1.10.6``. npm semver satisfies none of those -- each names a
        different package build -- and the pre-round-3 string equality had
        caught every one, so the range support silently loosened a dependency
        anchor into a bypass.
        """
        module = _load_validator()
        for declared, locked in (
            ("1.10.6", "1.10.6-evil.0"),
            ("1.10.6", "1.10.6+evil"),
            ("1.10.6", "v1.10.6"),
            ("^1.10.6", "1.10.6-attack"),
            ("^1.10.6", "1.11.0+build"),
            (">=1.10.6", "2.0.0-rc.1"),
            # The declared side too, per the docstring's stated intent.
            ("1.10.6-x", "1.10.6"),
            ("^1.10.6+build", "1.10.6"),
        ):
            with pytest.raises(module.EvidenceGateError):
                self._admits(declared, locked)

    def test_the_repo_as_it_stands_passes(self) -> None:
        """The real web/package.json and web/bun.lock must still agree -- the
        new comparison must not have loosened or broken the live check."""
        module = _load_validator()
        declared = module._package_json_declared_version()
        locked, _integrity = module._lockfile_dependency_anchor()
        assert module._declared_range_admits(declared, locked) is True


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
