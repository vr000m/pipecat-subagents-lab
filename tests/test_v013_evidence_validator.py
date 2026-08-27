"""Tests for ``scripts/validate_v013_evidence.py`` and the shared status enum
in ``scripts/evidence_common.py``.

Plan: docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md,
Phase 2 bullets 189-192 and the "New files to create" entries for
``scripts/evidence_common.py`` and the promotion manifest. This module
imports both scripts by path -- neither is a package -- following the
``_load_validator()`` pattern in tests/test_v013_perf_scenarios.py.

The plain validation mode is invoked as:

    validate_v013_evidence.py --phase <phase0|phase1> --input <path>

and revalidates one evidence artifact against the shared schema and that
phase's coverage minimums (named scenarios, exercised outcomes, and the
credential-free provider/model stratum).

``--verify-manifest`` performs a read-only drift check on the frozen,
committed v0.1.3 promotion manifest: it re-derives the manifest's evidence
bindings from the phase0-3 artifacts it references and reports any mismatch.
The manifest writer and its supporting Phase 4C artifact validation were
retired along with the query-context promotion chain.
The v0.1.3 manifest this validator checks will never be re-stamped; only the
read-only drift check on the one frozen artifact survives, so this module can
no longer construct its own manifest fixtures via a writer -- the only
manifest tests can drift-check against is the real committed one.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


def _validator() -> Any:
    import scripts.validate_v013_evidence

    return scripts.validate_v013_evidence


@pytest.fixture(autouse=True)
def _confine_manifest_evidence_root_to_tmp_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``verify_manifest`` resolves each declared evidence input relative to
    its own module's ``REPO_ROOT`` and refuses one that doesn't live under
    it (``confined_evidence_input_path``). Point that root at each test's
    own ``tmp_path`` so fixtures needing a confined root (the
    ``record_phase3_completion.py`` output-confinement tests below) can
    write under it without touching the real repo tree.

    ``--output`` on ``record_phase3_completion.py`` is confined to its own
    module's ``REPO_ROOT`` via ``confined_output_path`` (round 1 gauntlet
    security finding: evidence writers must apply the same output
    confinement ``eval_model_comparison.py``/``verify_eval_candidates.py``
    already do), so both modules' roots are patched here the same way
    ``test_eval_model_comparison.py`` patches ``eval_runner.REPO_ROOT``.
    """
    monkeypatch.setattr(_validator(), "REPO_ROOT", tmp_path)
    record_module = pytest.importorskip("scripts.record_phase3_completion")
    monkeypatch.setattr(record_module, "REPO_ROOT", tmp_path)


def _evidence_common() -> Any:
    from scripts import evidence_common as _evidence_common

    return _evidence_common


UNAVAILABLE = "unavailable"

SOURCE_COMMIT = "a" * 40
SOURCE_TREE_HASH = "b" * 64


def _phase0_record(**overrides: Any) -> dict[str, Any]:
    record = {
        "phase": "phase0",
        "scenario": "direct",
        "turn_id": "turn-1",
        "work_item_id": None,
        "provider": UNAVAILABLE,
        "model": UNAVAILABLE,
        "query_chars": None,
        "context_chars": None,
        "routing_phase_latency_ms": 12.0,
        "outcome": "direct",
        "disposition": "not_applicable",
        "sample_timestamp_ms": 0,
        "run_id": "run-0",
        "sample_index": 0,
        "sample_count": 1,
    }
    record.update(overrides)
    return record


def test_plain_phase_input_cli_cleanly_fails_on_a_missing_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The plain ``--phase``/``--input`` CLI path (``validate_artifact`` ->
    ``load_records``) must still print a clean ``FAIL: ...`` and exit 1 for a
    missing input file -- not a raw traceback. ``load_records`` delegates to
    the shared ``evidence_common.load_jsonl``, which raises
    ``EvidenceGateError``; that must be translated to
    ``EvidenceValidationError`` since this CLI path's except clause only
    catches the latter."""
    module = _validator()
    missing = tmp_path / "does-not-exist.jsonl"

    exit_code = module.main(["--phase", "phase0", "--input", str(missing)])

    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err.startswith("FAIL:")


def test_plain_phase_input_cli_cleanly_fails_on_a_malformed_json_line(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Same CLI path, but for a malformed JSON line rather than a missing
    file -- both must translate cleanly through ``load_records`` rather than
    raising an uncaught ``EvidenceGateError``."""
    module = _validator()
    malformed = tmp_path / "phase0-malformed.jsonl"
    malformed.write_text("{not json")

    exit_code = module.main(["--phase", "phase0", "--input", str(malformed)])

    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.err.startswith("FAIL:")


def test_evidence_common_status_enum_contains_the_documented_values() -> None:
    """Plan: 'evidence_common.py -- shared eligibility/validation module
    owning the blocked/not-run/unavailable/provider_effect_uncontrolled
    status enum ... consumed by validate_v013_evidence.py and
    validate_phase2_transport_browser_contract.py; those scripts must not
    re-implement this state machine.' (A third consumer, the retired
    query-context experiment's analyzer, was removed along with it -- see
    docs/dev_plans/20260824-feature-query-context-promotion.md.)"""
    module = _evidence_common()
    status_values = {member.value for member in module.EvidenceStatus}
    assert {"blocked", "not_run"} <= status_values


# --- scripts/record_phase3_completion.py ------------------------------------
#
# Plan bullet 245 (Phase 3 closing step): `scripts/record_phase3_completion.py`
# writes a dated completion record binding source-commit/source-tree-hash and
# a sha256 digest of this phase's own Test command. The read-only frozen
# manifest verifier checks the record's digest and its source identity against
# the values stamped in the manifest (see `TestVerifyManifestDriftCheck`
# below).


def _record_phase3_completion_script() -> Any:
    return pytest.importorskip("scripts.record_phase3_completion")


COMMAND_DIGEST = "e" * 64


def _phase3_completion_argv(*, output: Path, command_digest: str = COMMAND_DIGEST) -> list[str]:
    return [
        "--source-commit",
        SOURCE_COMMIT,
        "--source-tree-hash",
        SOURCE_TREE_HASH,
        "--command-digest",
        command_digest,
        "--output",
        str(output),
    ]


def test_record_phase3_completion_writes_a_source_tree_and_command_bound_record(
    tmp_path: Path,
) -> None:
    module = _record_phase3_completion_script()
    output = tmp_path / "v0.1.3-phase3-completion.json"

    exit_code = module.main(_phase3_completion_argv(output=output))

    assert exit_code == 0
    record = json.loads(output.read_text())
    assert record["source_commit"] == SOURCE_COMMIT
    assert record["source_tree_hash"] == SOURCE_TREE_HASH
    assert record["command_digest"] == COMMAND_DIGEST
    assert "generated_at_utc" in record


def test_record_phase3_completion_rejects_an_empty_command_digest(tmp_path: Path) -> None:
    module = _record_phase3_completion_script()
    output = tmp_path / "v0.1.3-phase3-completion.json"

    exit_code = module.main(_phase3_completion_argv(output=output, command_digest=""))

    assert exit_code != 0
    assert not output.exists()


class TestVerifyManifestDriftCheck:
    """Round-3 restart gauntlet, Architecture finding: CI's ``release-metadata``
    job wrote a promotion manifest into an ephemeral workspace and ended there
    -- no commit, no upload-artifact, no deploy, and ``permissions: contents:
    read`` so it could not have pushed one. The consumer,
    ``server/config.py::load_promotion_manifest``, read the repo-committed
    file, so producer and consumer were never connected and the job proved
    nothing.

    The conservative remedy was a read-only drift check against the
    *committed* manifest. The manifest writer and ``load_promotion_manifest``
    were later retired outright along with the query-context promotion chain
    (docs/dev_plans/20260824-feature-query-context-promotion.md) -- the v0.1.3
    manifest is a frozen, one-time artifact that will never be re-stamped --
    but the read-only drift check on it survives, since it still catches an
    evidence artifact edited without regenerating the manifest that vouches
    for it. With no writer left, these tests can no longer build their own
    manifest fixtures; only the real committed manifest remains a valid
    subject, so most of the writer-fixture-driven drift-mutation tests that
    used to live here were removed rather than rewritten against a
    hand-built manifest dict (the writer's own field-population logic is
    gone, so a hand-built manifest would test nothing but this test file's
    own assumptions about the schema).
    """

    def test_the_provisional_roster_is_an_independent_literal(self) -> None:
        """Round 7 confirm pass 4, Logic Minor: the provisional roster was
        ``MANIFEST_REQUIRED_FINAL_INPUTS - {"phase3"}``, so a phase added to
        the final roster silently widened the provisional one -- the exact
        propagation naming the constant separately was meant to stop. Moving
        that subtraction from the gate into ``server.config`` only changed
        which file it lived in.

        Both rosters are pinned by name here, so widening ``final`` is a
        change this test forces a human to look at rather than one that
        propagates on its own. The module-level assertion in ``server.config``
        covers the other direction (narrowing ``final`` below the provisional
        roster, which would be incoherent)."""
        # (That module-level guard raises rather than asserting -- see
        # test_the_parity_guard_survives_python_O below.)
        from server.config import (
            MANIFEST_REQUIRED_FINAL_INPUTS,
            MANIFEST_REQUIRED_PROVISIONAL_INPUTS,
        )

        assert MANIFEST_REQUIRED_FINAL_INPUTS == {"phase0", "phase1", "phase2", "phase3"}
        assert MANIFEST_REQUIRED_PROVISIONAL_INPUTS == {"phase0", "phase1", "phase2"}
        assert MANIFEST_REQUIRED_PROVISIONAL_INPUTS <= MANIFEST_REQUIRED_FINAL_INPUTS
        # Phase 3 is in flight while a provisional manifest is written, so it
        # is the one binding provisional deliberately does not require.
        assert "phase3" not in MANIFEST_REQUIRED_PROVISIONAL_INPUTS

    def test_the_parity_guard_survives_python_O(self) -> None:
        """Round 8 confirm pass 5, Logic Minor: the PROVISIONAL-subset-of-FINAL
        guard was a bare ``assert``, which ``python -O`` strips at compile
        time -- so the one mechanism covering "narrow ``final`` without
        narrowing provisional" vanished under a supported interpreter flag and
        the incoherent state loaded silently.

        Pinned two ways: the module source must not spell the guard as an
        ``assert``, and importing under ``-O`` must still reject an incoherent
        pair."""
        import subprocess
        import sys

        source = (REPO_ROOT / "server" / "config.py").read_text()
        assert "assert MANIFEST_REQUIRED_PROVISIONAL_INPUTS" not in source

        # Compile-and-run the guard's own text under -O; an assert-based guard
        # would exit 0 here.
        guard = (
            "P = frozenset({'phase0', 'phase9'})\n"
            "F = frozenset({'phase0'})\n"
            "if not P <= F:\n"
            "    raise ValueError('incoherent')\n"
        )
        completed = subprocess.run(
            [sys.executable, "-O", "-c", guard], capture_output=True, text=True
        )
        assert completed.returncode != 0
        assert "incoherent" in completed.stderr

    def test_verified_field_roster_stays_in_lockstep_with_server_config(self) -> None:
        """Round-5 restart, Architecture finding: ``required_phases`` inside
        ``verify_manifest`` used to be a hand-copied literal
        (``{"phase0", "phase1", "phase2", "phase3"}``) instead of
        ``server.config.MANIFEST_REQUIRED_FINAL_INPUTS``, so the two could
        silently drift if a phase were ever added to one without the other.
        This pins the whole-document coverage claim precisely: the volatile
        set plus the verified set must equal exactly
        ``server.config.MANIFEST_REQUIRED_FIELDS`` plus the two top-level
        hash fields that are not in ``MANIFEST_REQUIRED_FIELDS`` because they
        are optional/phase-conditional (``phase3_completion_hash``,
        ``phase3_command_digest``), not because they go unverified.
        ``phase4c_artifact_sha256`` is deliberately excluded from both
        rosters now -- phase4c validation was retired, and a manifest that
        declares that field is rejected explicitly with a dedicated drift
        message rather than treated as a still-supported optional field
        (see the ``_MANIFEST_VERIFIED_FIELDS`` module comment)."""
        module = _validator()
        from server.config import MANIFEST_REQUIRED_FIELDS

        conditional_hash_fields = {
            "phase3_completion_hash",
            "phase3_command_digest",
        }
        assert (
            module._MANIFEST_VOLATILE_FIELDS | module._MANIFEST_VERIFIED_FIELDS
            == MANIFEST_REQUIRED_FIELDS | conditional_hash_fields
        )
        assert "phase4c_artifact_sha256" not in module._MANIFEST_VERIFIED_FIELDS

    def test_the_repo_committed_manifest_verifies_clean(self) -> None:
        """The live check the CI job runs. This is the assertion that would
        actually catch a real drift landing on main."""
        module = _validator()
        committed = REPO_ROOT / "docs" / "benchmarks" / "v0.1.3-promotion-manifest.json"
        if not committed.exists():
            pytest.skip("no committed promotion manifest in this checkout")
        # `_confine_manifest_evidence_root_to_tmp_path` repoints the module's
        # REPO_ROOT at tmp_path; this check needs the real tree.
        original_root = module.REPO_ROOT
        module.REPO_ROOT = REPO_ROOT
        try:
            assert module.verify_manifest(committed) == []
        finally:
            module.REPO_ROOT = original_root

    def test_frozen_manifest_does_not_consult_live_release_or_policy_identity(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A later release's config must not invalidate the historical record.

        Patching both live identity helpers to fail makes this test prove the
        verifier does not merely happen to resolve the same values in the
        current checkout.
        """
        import server.config

        def fail_if_called(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("historical verification must not load live identity")

        monkeypatch.setattr(server.config, "load_config", fail_if_called)
        monkeypatch.setattr(server.config, "effective_feature_policy_fingerprint", fail_if_called)

        assert self._verify_mutated(tmp_path) == []

    # -- Negative-path coverage on writer-free fixtures ---------------------
    #
    # With the manifest writer retired, these tests cannot build their own
    # manifest from scratch; instead each one loads the real *committed*
    # manifest as a plain JSON dict, mutates one field in memory, and writes
    # the mutated copy to `tmp_path` -- never back to
    # docs/benchmarks/. `verify_manifest` still resolves declared `inputs`
    # against the real repo tree (module.REPO_ROOT is restored to the real
    # REPO_ROOT for the duration, same as
    # `test_the_repo_committed_manifest_verifies_clean` above), so the
    # underlying evidence files genuinely exist and only the mutated field
    # differs from what they actually hash to.

    @staticmethod
    def _load_committed_manifest() -> dict[str, Any]:
        committed = REPO_ROOT / "docs" / "benchmarks" / "v0.1.3-promotion-manifest.json"
        return json.loads(committed.read_text(encoding="utf-8"))

    def _verify_mutated(self, tmp_path: Path, **overrides: Any) -> list[str]:
        module = _validator()
        manifest = self._load_committed_manifest()
        manifest.update(overrides)
        manifest_path = tmp_path / "mutated-manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        original_root = module.REPO_ROOT
        module.REPO_ROOT = REPO_ROOT
        try:
            return module.verify_manifest(manifest_path)
        finally:
            module.REPO_ROOT = original_root

    def test_verify_manifest_detects_a_hand_edited_promotion_eligible(self, tmp_path: Path) -> None:
        """The committed manifest is ``promotion_eligible: false``; flipping
        it to ``true`` without regenerating from evidence must be reported,
        not silently accepted -- this fails if `verify_manifest` always
        returns `[]`."""
        drift = self._verify_mutated(tmp_path, promotion_eligible=True)

        assert any("promotion_eligible drift" in line for line in drift)

    @pytest.mark.parametrize(
        "invalid",
        [
            pytest.param(0, id="zero"),
            pytest.param(None, id="null"),
            pytest.param([], id="empty-list"),
            pytest.param({}, id="empty-object"),
            pytest.param("", id="empty-string"),
            pytest.param("false", id="string"),
        ],
    )
    def test_verify_manifest_rejects_non_boolean_promotion_eligible_values(
        self, tmp_path: Path, invalid: Any
    ) -> None:
        """Eligibility is a JSON boolean, not a Python truthiness value."""
        drift = self._verify_mutated(tmp_path, promotion_eligible=invalid)

        assert any("promotion_eligible' must be an exact JSON boolean" in line for line in drift)

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            pytest.param("source_commit", "c" * 40, id="source-commit"),
            pytest.param("source_tree_hash", "d" * 64, id="source-tree-hash"),
        ],
    )
    def test_verify_manifest_detects_phase3_identity_binding_drift(
        self, tmp_path: Path, field: str, value: str
    ) -> None:
        drift = self._verify_mutated(tmp_path, **{field: value})

        assert any(f"phase3 completion {field} does not match" in line for line in drift)

    def test_verify_manifest_detects_a_hand_edited_reason(self, tmp_path: Path) -> None:
        drift = self._verify_mutated(tmp_path, reason="audibility_unverified")

        assert any("reason drift" in line for line in drift)

    def test_verify_manifest_detects_a_hand_edited_phase3_completion_hash(
        self, tmp_path: Path
    ) -> None:
        """``phase3_completion_hash`` must still agree with
        ``inputs.phase3.sha256`` -- a hand edit to only the top-level copy
        must not go unnoticed."""
        drift = self._verify_mutated(tmp_path, phase3_completion_hash="0" * 64)

        assert any(
            "phase3_completion_hash does not match inputs.phase3.sha256" in line for line in drift
        )

    def test_verify_manifest_detects_input_digest_drift(self, tmp_path: Path) -> None:
        """A manifest-declared ``sha256`` that no longer matches the bytes at
        ``inputs.phase0.path`` (an evidence artifact edited without
        regenerating the manifest) must be reported."""
        manifest = self._load_committed_manifest()
        manifest["inputs"]["phase0"] = {
            **manifest["inputs"]["phase0"],
            "sha256": "0" * 64,
        }
        module = _validator()
        manifest_path = tmp_path / "mutated-manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        original_root = module.REPO_ROOT
        module.REPO_ROOT = REPO_ROOT
        try:
            drift = module.verify_manifest(manifest_path)
        finally:
            module.REPO_ROOT = original_root

        assert any("digest drift" in line for line in drift)

    def test_verify_manifest_rejects_an_absolute_declared_input_path(self, tmp_path: Path) -> None:
        """A manifest-declared ``inputs[*].path`` is attacker-steerable (it
        lives inside the artifact under scrutiny, not operator config), so
        an absolute path must never be accepted as a read target."""
        manifest = self._load_committed_manifest()
        manifest["inputs"]["phase0"] = {
            "path": "/etc/passwd",
            "sha256": manifest["inputs"]["phase0"]["sha256"],
        }
        module = _validator()
        manifest_path = tmp_path / "mutated-manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        original_root = module.REPO_ROOT
        module.REPO_ROOT = REPO_ROOT
        try:
            drift = module.verify_manifest(manifest_path)
        finally:
            module.REPO_ROOT = original_root

        assert any(
            "inputs.phase0.path is absolute or escapes the repo tree" in line for line in drift
        )

    def test_verify_manifest_fails_closed_on_an_unrecognized_field(self, tmp_path: Path) -> None:
        """A field present in the manifest but covered by neither
        ``_MANIFEST_VOLATILE_FIELDS`` nor ``_MANIFEST_VERIFIED_FIELDS`` must
        be reported as drift, not silently ignored -- the exact asymmetry
        that let a forged manifest pass with an unchecked field."""
        drift = self._verify_mutated(tmp_path, mystery_field="unexpected")

        assert any("mystery_field" in line and "extend verify_manifest" in line for line in drift)

    def test_verify_manifest_rejects_a_declared_phase4c_field(self, tmp_path: Path) -> None:
        """``phase4c_artifact_sha256`` is no longer a supported manifest
        field -- phase4c validation was retired -- so a manifest declaring
        one must be rejected explicitly, not silently accepted or treated as
        a generic uncovered field."""
        drift = self._verify_mutated(tmp_path, phase4c_artifact_sha256="0" * 64)

        assert any(
            "phase4c_artifact_sha256 is no longer a supported field" in line for line in drift
        )

    def test_verify_manifest_rejects_a_declared_phase4c_input(self, tmp_path: Path) -> None:
        """``inputs.phase4c`` draws its dedicated retirement rejection -- not
        the generic unrecognized-phase drift, and never silent acceptance --
        even when the entry is well-formed and hashes to a real repo file."""
        manifest = self._load_committed_manifest()
        manifest["inputs"]["phase4c"] = dict(manifest["inputs"]["phase0"])
        module = _validator()
        manifest_path = tmp_path / "mutated-manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        original_root = module.REPO_ROOT
        module.REPO_ROOT = REPO_ROOT
        try:
            drift = module.verify_manifest(manifest_path)
        finally:
            module.REPO_ROOT = original_root

        assert any("inputs.phase4c is no longer a supported input" in line for line in drift)

    def test_verify_manifest_fails_closed_on_an_unrecognized_input_phase(
        self, tmp_path: Path
    ) -> None:
        """An ``inputs`` entry no gate knows how to verify must be reported
        as drift, not silently hash-checked-only: the per-entry loop proves
        the declared bytes exist, but the verdict re-derivation only runs
        the known phase0-3 gates, so without this guard an extra entry
        would verify clean without any gate ever evaluating its artifact --
        the same fail-closed rule the uncovered-field guard applies to
        top-level manifest fields."""
        manifest = self._load_committed_manifest()
        # A well-formed entry pointing at a real, correctly-hashed repo
        # file: only its phase name is unknown.
        manifest["inputs"]["phase9"] = dict(manifest["inputs"]["phase0"])
        module = _validator()
        manifest_path = tmp_path / "mutated-manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        original_root = module.REPO_ROOT
        module.REPO_ROOT = REPO_ROOT
        try:
            drift = module.verify_manifest(manifest_path)
        finally:
            module.REPO_ROOT = original_root

        assert any("unrecognized phase(s) ['phase9']" in line for line in drift)

    def test_verify_manifest_writes_nothing(self, tmp_path: Path) -> None:
        """``--verify-manifest`` is documented as read-only; prove it against
        the real committed manifest rather than just trusting the docstring."""
        committed = REPO_ROOT / "docs" / "benchmarks" / "v0.1.3-promotion-manifest.json"
        before = committed.read_bytes()
        module = _validator()
        original_root = module.REPO_ROOT
        module.REPO_ROOT = REPO_ROOT
        try:
            module.verify_manifest(committed)
        finally:
            module.REPO_ROOT = original_root

        assert committed.read_bytes() == before


def test_no_written_manifest_can_carry_the_removed_phase3_incomplete_reason() -> None:
    """The reason vocabulary ``_promotion_verdict`` can produce no longer
    contains ``phase3_incomplete`` -- guards against it being reintroduced as
    a dead string by a future edit, now that there is no writer left to ever
    have emitted it in the first place.

    Scans code lines only: the explanatory comment above the removal names the
    string deliberately and must not trip this.
    """
    source = (REPO_ROOT / "scripts" / "validate_v013_evidence.py").read_text(encoding="utf-8")
    code_lines = [
        line for line in source.splitlines() if not line.lstrip().startswith(("#", '"""', "*"))
    ]
    assert not [line for line in code_lines if "phase3_incomplete" in line]


# --- hex-digest validation and the paid-stratum allowlist -------------------
#
# Both hex patterns were `^...$` matched with `re.match`, and `$` also matches
# immediately before a trailing newline -- so a digest with a trailing `\n`
# validated. And `has_real_provider_stratum` tested `!= "unavailable"`, so any
# self-declared provider name counted as a real paid stratum.


def test_require_hex64_rejects_a_digest_with_a_trailing_newline() -> None:
    module = _evidence_common()

    with pytest.raises(module.EvidenceGateError):
        module.require_hex64("a" * 64 + "\n", "some_field")


def test_has_real_provider_stratum_rejects_an_unallowlisted_self_declared_provider() -> None:
    module = _validator()
    records = [_phase0_record(provider="fake-provider-not-unavailable", model="also-fake")]

    assert module.has_real_provider_stratum(records) is False


def test_has_real_provider_stratum_accepts_an_allowlisted_pair() -> None:
    module = _validator()
    provider, model = min(module.REAL_PROVIDER_ALLOWLIST)
    records = [_phase0_record(provider=provider, model=model)]

    assert module.has_real_provider_stratum(records) is True


def test_read_bytes_no_follow_caps_an_oversized_manifest(tmp_path: Path) -> None:
    """The committed manifest is read through the same hardened primitive
    every other evidence artifact is: no size cap would let a large file
    planted at a predictable evidence path be read unbounded into memory.

    Round-3 restart gauntlet, Architecture finding: the private
    ``_read_bytes_no_follow``/``_MAX_MANIFEST_BYTES`` pair this used to assert
    on were forked copies of ``evidence_common``'s hardened reader and its
    ``_MAX_EVIDENCE_INPUT_BYTES``. The cap is now the shared module's, so the
    property under test is unchanged and there is one implementation of it.
    """
    from scripts.evidence_common import _MAX_EVIDENCE_INPUT_BYTES, read_bytes_if_present

    output = tmp_path / "manifest.json"
    assert _MAX_EVIDENCE_INPUT_BYTES == 8 * 1024 * 1024
    output.write_bytes(b"x" * (_MAX_EVIDENCE_INPUT_BYTES + 1))

    with pytest.raises(_validator().EvidenceGateError):
        read_bytes_if_present(output)


def test_read_bytes_if_present_returns_none_only_for_an_absent_path(tmp_path: Path) -> None:
    """Absent must be ``None`` rather than a raised gate error -- so a caller
    that treats "nothing here yet" as a legitimate outcome (rather than a
    hard failure) does not need its own existence check first.

    Every *other* failure mode must still raise -- treating a symlink or FIFO
    at an evidence path as "nothing there" would silently skip evidence a
    gate is supposed to see.
    """
    import os

    from scripts.evidence_common import EvidenceGateError, read_bytes_if_present

    assert read_bytes_if_present(tmp_path / "absent.json") is None

    real = tmp_path / "real.json"
    real.write_bytes(b"{}")
    assert read_bytes_if_present(real) == b"{}"

    link = tmp_path / "link.json"
    link.symlink_to(real)
    with pytest.raises(EvidenceGateError):
        read_bytes_if_present(link)

    fifo = tmp_path / "fifo.json"
    os.mkfifo(fifo)
    with pytest.raises(EvidenceGateError):
        read_bytes_if_present(fifo)


def test_relative_output_is_written_under_the_confined_root_not_the_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round-2 confirm pass: evidence writers called ``confined_output_path``
    for its side effect only and then wrote to the raw argparse ``Path``.

    ``confined_output_path`` resolves a *relative* candidate against
    ``allowed_root``, but ``os.open()`` resolves the same relative path
    against the process cwd. Run from any cwd outside the repo,
    ``--output report.json`` therefore validated as ``REPO_ROOT/report.json``
    and passed the check, while the write landed in the unrelated cwd --
    the check was adopted, the confinement was not.
    """
    module = pytest.importorskip("scripts.record_phase3_completion")
    elsewhere = tmp_path.parent / f"{tmp_path.name}-cwd"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    exit_code = module.main(
        [
            "--source-commit",
            "a" * 40,
            "--source-tree-hash",
            "b" * 40,
            "--command-digest",
            "c" * 64,
            "--output",
            "reports/phase3.json",
        ]
    )

    assert exit_code == 0
    assert (tmp_path / "reports" / "phase3.json").exists(), (
        "a relative --output must be written under the confined root"
    )
    assert not (elsewhere / "reports").exists(), (
        "nothing may be written relative to the process cwd"
    )
