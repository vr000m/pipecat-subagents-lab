#!/usr/bin/env python3
"""Validate a v0.1.3 evidence JSONL artifact against its schema and phase minimums.

Each line of ``--input`` must be one JSON object matching
``shared/schemas/v013-evidence.json``. This module hand-checks the schema's
required fields, types, and enum vocabularies directly (no ``jsonschema``
dependency) and then enforces phase-specific coverage minimums: one record
per named scenario, one record per outcome exercised by that phase's fixture
matrix, and one record per provider/model stratum (the credential-free
stratum is ``unavailable``/``unavailable``).

``--verify-manifest`` performs a read-only drift check on a previously
committed promotion manifest: it re-derives the manifest's evidence bindings
from the phase0-3 artifacts it references and reports any mismatch. The
manifest-writing path and its supporting Phase 4C artifact validation were
retired along with the query-context promotion chain. The v0.1.3 manifest
this validator checks is a one-time frozen artifact that will never be
re-stamped; this module only validates it and the evidence it binds.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from scripts.evidence_common import (
    REPO_ROOT,
    EvidenceGateError,
    confined_evidence_input_path,
    load_json,
    load_jsonl,
    require_type,
    sha256_file,
)
from scripts.validate_phase2_transport_browser_contract import (
    validate_artifact as validate_transport_browser_artifact,
)
from server.config import (
    MANIFEST_REQUIRED_FIELDS,
    MANIFEST_REQUIRED_FINAL_INPUTS,
    MANIFEST_REQUIRED_PROVISIONAL_INPUTS,
    MANIFEST_STRING_FIELDS,
)

SCHEMA_PATH = REPO_ROOT / "shared" / "schemas" / "v013-evidence.json"
TRANSPORT_SCHEMA_PATH = REPO_ROOT / "shared" / "schemas" / "v013-transport-browser-contract.json"

REQUIRED_FIELDS = (
    "phase",
    "scenario",
    "turn_id",
    "provider",
    "model",
    "routing_phase_latency_ms",
    "outcome",
    "disposition",
    "sample_timestamp_ms",
    "run_id",
    "sample_index",
    "sample_count",
)
OPTIONAL_FIELDS = ("work_item_id", "query_chars", "context_chars", "wall_clock_time")
# Phase 1 extends the record shape with the ack-path fields described in the
# dev plan's Phase 1 evidence-artifact bullet. They are optional at the
# schema level (so phase0 records are unaffected) but required-though-nullable
# for phase1 records specifically -- enforced in `validate_record` below.
PHASE1_FIELDS = ("ack_id", "ack_enqueued", "ack_enqueued_ms", "ack_terminal_state")
ALL_FIELDS = frozenset(REQUIRED_FIELDS) | frozenset(OPTIONAL_FIELDS) | frozenset(PHASE1_FIELDS)

PHASES = frozenset({"phase0", "phase1", "phase2", "phase3", "phase4"})
OUTCOMES = frozenset(
    {
        "direct",
        "unsupported",
        "control",
        "clarify",
        "completed",
        "mixed",
        "retained",
        "declined",
        "failed",
        "cancelled",
    }
)
DISPOSITIONS = frozenset({"autoplay", "display_only", "suppressed", "not_applicable"})
ACK_TERMINAL_STATES = frozenset(
    {"discarded", "admitted_completed", "no_tts", "unavailable_transport", "not_applicable"}
)

# Phase-specific fields required in addition to REQUIRED_FIELDS. Values may
# be null (nullable-required); presence is what's enforced here, and the
# per-field type/enum checks below reject a wrong-typed non-null value.
PHASE_EXTRA_REQUIRED_FIELDS: Mapping[str, tuple[str, ...]] = {
    "phase1": ("work_item_id", "ack_id", "ack_enqueued", "ack_enqueued_ms", "ack_terminal_state"),
}

# Phase-specific coverage minimums. Each phase declares the scenario names,
# outcomes, and provider/model strata its fixture matrix must exercise at
# least once. Later phases extend this table; they do not replace phase0's.
PHASE_MINIMUMS: Mapping[str, Mapping[str, frozenset[Any]]] = {
    "phase0": {
        "scenarios": frozenset(
            {
                "direct",
                "delegated_complete",
                "retained_late",
                "cancellation",
                "reconnect",
                "same_epoch_newer_turn",
            }
        ),
        "outcomes": frozenset({"direct", "completed", "retained", "cancelled"}),
        "provider_model_strata": frozenset({("unavailable", "unavailable")}),
    },
    "phase1": {
        "scenarios": frozenset(
            {
                "ack_enqueued_timing",
                "exactly_once",
                "queued_discard",
                "admitted_completion",
                "no_tts",
            }
        ),
        "outcomes": frozenset({"completed", "cancelled"}),
        "provider_model_strata": frozenset({("unavailable", "unavailable")}),
    },
}
# The (provider, model) pairs this release actually supports for paid, real
# evidence collection. `has_real_provider_stratum` checks membership here
# rather than "is not the string 'unavailable'": the negative check let an
# artifact self-declare any provider name (e.g. "fake-provider") into the
# paid stratum. The only real pair the (now-retired) query-context experiment
# collector could produce -- see
# docs/dev_plans/20260824-feature-query-context-promotion.md -- is the one
# pair below; its dry-run stratum ("synthetic"/"dry-run-fixture") was
# deliberately absent from this allowlist. Adding a provider/model here is
# what admits it to promotion.
REAL_PROVIDER_ALLOWLIST: frozenset[tuple[str, str]] = frozenset(
    {
        ("openai", "gpt-4o-search-preview"),
    }
)
# "two records each for the queued-discard and admitted-completion race
# scenarios" -- a minimum *count*, not just presence, so it is checked
# separately from PHASE_MINIMUMS' presence-only sets.
PHASE_SCENARIO_MIN_COUNT: Mapping[str, Mapping[str, int]] = {
    "phase1": {"queued_discard": 2, "admitted_completion": 2},
}


def _require_type(value: Any, kinds: tuple[type, ...], field: str, index: int) -> None:
    """``evidence_common.require_type`` with this file's per-record prefix.

    Round-3 restart gauntlet, Architecture finding: this was a forked copy of
    the shared check (same bool-is-not-an-int rule, same isinstance test),
    differing only in prefixing the message with the record index. Two copies
    of a type predicate can harden independently, so the predicate now lives
    in one place and only the *message* is local -- which is the actual
    difference.
    """
    try:
        require_type(value, kinds, field)
    except EvidenceGateError as exc:
        raise EvidenceGateError(f"record {index}: {exc}") from exc


def validate_record(record: Mapping[str, Any], index: int) -> None:
    unknown = set(record) - ALL_FIELDS
    if unknown:
        raise EvidenceGateError(f"record {index}: unknown field(s) {sorted(unknown)}")
    missing = [name for name in REQUIRED_FIELDS if name not in record]
    if missing:
        raise EvidenceGateError(f"record {index}: missing required field(s) {missing}")

    if record["phase"] not in PHASES:
        raise EvidenceGateError(f"record {index}: invalid phase {record['phase']!r}")

    phase_extra = PHASE_EXTRA_REQUIRED_FIELDS.get(record["phase"], ())
    missing_extra = [name for name in phase_extra if name not in record]
    if missing_extra:
        raise EvidenceGateError(
            f"record {index}: phase={record['phase']!r} missing required field(s) {missing_extra}"
        )

    for field in ("scenario", "turn_id", "provider", "model", "run_id"):
        _require_type(record[field], (str,), field, index)
        if not record[field]:
            raise EvidenceGateError(f"record {index}: {field} must be non-empty")
    if record["outcome"] not in OUTCOMES:
        raise EvidenceGateError(f"record {index}: invalid outcome {record['outcome']!r}")
    if record["disposition"] not in DISPOSITIONS:
        raise EvidenceGateError(f"record {index}: invalid disposition {record['disposition']!r}")

    latency = record["routing_phase_latency_ms"]
    if latency is not None:
        _require_type(latency, (int, float), "routing_phase_latency_ms", index)
        if latency < 0:
            raise EvidenceGateError(f"record {index}: routing_phase_latency_ms must be >= 0")

    _require_type(record["sample_timestamp_ms"], (int,), "sample_timestamp_ms", index)
    if record["sample_timestamp_ms"] < 0:
        raise EvidenceGateError(f"record {index}: sample_timestamp_ms must be >= 0")
    _require_type(record["sample_index"], (int,), "sample_index", index)
    if record["sample_index"] < 0:
        raise EvidenceGateError(f"record {index}: sample_index must be >= 0")
    _require_type(record["sample_count"], (int,), "sample_count", index)
    if record["sample_count"] < 1:
        raise EvidenceGateError(f"record {index}: sample_count must be >= 1")

    if "work_item_id" in record and record["work_item_id"] is not None:
        _require_type(record["work_item_id"], (str,), "work_item_id", index)
        if not record["work_item_id"]:
            raise EvidenceGateError(f"record {index}: work_item_id must be non-empty")
    for field in ("query_chars", "context_chars"):
        if field in record and record[field] is not None:
            _require_type(record[field], (int,), field, index)
            if record[field] < 0:
                raise EvidenceGateError(f"record {index}: {field} must be >= 0")
    if "wall_clock_time" in record and record["wall_clock_time"] is not None:
        _require_type(record["wall_clock_time"], (str,), "wall_clock_time", index)

    if "ack_id" in record and record["ack_id"] is not None:
        _require_type(record["ack_id"], (str,), "ack_id", index)
        if not record["ack_id"]:
            raise EvidenceGateError(f"record {index}: ack_id must be non-empty")
    if "ack_enqueued" in record and record["ack_enqueued"] is not None:
        _require_type(record["ack_enqueued"], (bool,), "ack_enqueued", index)
    if "ack_enqueued_ms" in record and record["ack_enqueued_ms"] is not None:
        _require_type(record["ack_enqueued_ms"], (int,), "ack_enqueued_ms", index)
        if record["ack_enqueued_ms"] < 0:
            raise EvidenceGateError(f"record {index}: ack_enqueued_ms must be >= 0")
    if (
        "ack_terminal_state" in record
        and record["ack_terminal_state"] is not None
        and record["ack_terminal_state"] not in ACK_TERMINAL_STATES
    ):
        raise EvidenceGateError(
            f"record {index}: invalid ack_terminal_state {record['ack_terminal_state']!r}"
        )


def load_records(input_path: Path) -> list[dict[str, Any]]:
    return load_jsonl(input_path)


def check_phase_minimums(phase: str, records: Sequence[Mapping[str, Any]]) -> None:
    minimums = PHASE_MINIMUMS.get(phase)
    if minimums is None:
        raise EvidenceGateError(f"no coverage minimums declared for phase {phase!r}")

    phase_records = [r for r in records if r["phase"] == phase]
    if not phase_records:
        raise EvidenceGateError(f"no records found for phase {phase!r}")

    seen_scenarios = {r["scenario"] for r in phase_records}
    missing_scenarios = minimums["scenarios"] - seen_scenarios
    if missing_scenarios:
        raise EvidenceGateError(
            f"phase {phase!r}: missing scenario coverage for {sorted(missing_scenarios)}"
        )

    seen_outcomes = {r["outcome"] for r in phase_records}
    missing_outcomes = minimums["outcomes"] - seen_outcomes
    if missing_outcomes:
        raise EvidenceGateError(
            f"phase {phase!r}: missing outcome coverage for {sorted(missing_outcomes)}"
        )

    seen_strata = {(r["provider"], r["model"]) for r in phase_records}
    missing_strata = minimums["provider_model_strata"] - seen_strata
    if missing_strata:
        raise EvidenceGateError(
            f"phase {phase!r}: missing provider/model stratum coverage for {sorted(missing_strata)}"
        )

    min_counts = PHASE_SCENARIO_MIN_COUNT.get(phase, {})
    for scenario, minimum in min_counts.items():
        count = sum(1 for r in phase_records if r["scenario"] == scenario)
        if count < minimum:
            raise EvidenceGateError(
                f"phase {phase!r}: scenario {scenario!r} requires >= {minimum} record(s), got {count}"
            )


def validate_artifact(phase: str, input_path: Path) -> list[dict[str, Any]]:
    records = load_records(input_path)
    for index, record in enumerate(records):
        validate_record(record, index)
    check_phase_minimums(phase, records)
    return records


def has_real_provider_stratum(records: list[Mapping[str, Any]]) -> bool:
    """True if any record uses an allowlisted real provider/model stratum.

    The credential-free ``unavailable``/``unavailable`` stratum can validate
    schema/lifecycle correctness but can never make a manifest
    promotion-eligible; this is the check that enforces that split. Membership
    in ``REAL_PROVIDER_ALLOWLIST`` is required rather than merely differing
    from ``"unavailable"``, so an artifact cannot self-declare an arbitrary
    provider name into the paid stratum.
    """
    return any((r["provider"], r["model"]) in REAL_PROVIDER_ALLOWLIST for r in records)


def _validate_transport_contract(input_path: Path) -> dict[str, Any]:
    """Revalidate the Phase 2 transport/browser contract artifact.

    Delegates to `scripts.validate_phase2_transport_browser_contract` so this
    writer never re-implements or drifts from that gate's own logic.
    """
    record = load_json(input_path)
    if not isinstance(record, dict):
        raise EvidenceGateError("transport contract artifact must be a JSON object")
    validate_transport_browser_artifact(record)
    return record


PHASE3_COMPLETION_REQUIRED_FIELDS = (
    "source_commit",
    "source_tree_hash",
    "command_digest",
    "generated_at_utc",
)


def _read_phase3_completion(input_path: Path) -> dict[str, Any]:
    """Shape-validate the Phase 3 completion artifact.

    :func:`verify_manifest` separately binds the source identity in this
    record to the values stamped in the frozen manifest. It deliberately does
    not compare either value with the current checkout: the manifest records
    the historical release identity, while this validator is a historical
    drift check.
    """
    record = load_json(input_path)
    if not isinstance(record, dict):
        raise EvidenceGateError("phase3 completion artifact must be a JSON object")
    missing = [
        field
        for field in PHASE3_COMPLETION_REQUIRED_FIELDS
        if not isinstance(record.get(field), str) or not record[field]
    ]
    if missing:
        raise EvidenceGateError(f"phase3 completion artifact missing field(s) {missing}")
    return record


def _promotion_verdict(
    *, manifest_phase: str, real_stratum_present: bool, transport_eligible: bool
) -> tuple[bool, str | None]:
    """The ``(promotion_eligible, reason)`` pair the current evidence supports.

    Used by :func:`verify_manifest` to *re-derive* the verdict the (now
    retired) manifest writer used to *stamp*. Kept as one function rather than
    two independently-drifting copies (round-4 confirm pass, Logic + Security
    + Architecture findings): the writer's own call site is gone along with
    the query-context promotion chain, but the shared selection logic is what
    stops ``reason`` from silently disagreeing with ``promotion_eligible``.
    """
    if manifest_phase == "provisional":
        # A provisional manifest is diagnostic-only and is permanently
        # ineligible; Phase 3 replaces it with a "final" manifest that could
        # actually pass the (now-retired) runtime eligibility gate.
        if not real_stratum_present:
            return False, "real_stratum_missing"
        if not transport_eligible:
            return False, "audibility_unverified"
        return False, "provisional_manifest"
    promotion_eligible = real_stratum_present and transport_eligible
    if promotion_eligible:
        return True, None
    return False, ("real_stratum_missing" if not real_stratum_present else "audibility_unverified")


#: Manifest fields that describe *when and where* the manifest was produced
#: rather than *what evidence it binds*. Excluded from the drift check in
#: :func:`verify_manifest`, because a manifest committed at one commit can
#: never match a regeneration at a later one: ``deployed_at_utc`` and
#: ``generated_at_utc`` are wall-clock stamps, and ``source_commit`` /
#: ``source_tree_hash`` name the checkout the manifest was written *from*,
#: which every subsequent commit on the branch changes.
#:
#: ``release_version`` and ``feature_policy_fingerprint`` are historical
#: identity values stamped in the manifest. They are structurally validated
#: below, but intentionally are not derived from the current checkout: a
#: later release is expected to have different live identity values.
#:
#: The source identity is also kept volatile relative to the current checkout,
#: but its internal binding to the final Phase 3 completion record is checked
#: below. That catches a manifest whose stamped source identity no longer
#: agrees with the historical record it claims to bind.
#:
#: Everything *not* listed here is evidence binding, and must match.
_MANIFEST_VOLATILE_FIELDS = frozenset(
    {
        "deployed_at_utc",
        "generated_at_utc",
        "source_commit",
        "source_tree_hash",
    }
)

#: The evidence-binding fields :func:`verify_manifest` knows how to re-derive.
#: Together with ``_MANIFEST_VOLATILE_FIELDS`` this must cover every top-level
#: manifest key -- ``verify_manifest`` reports anything left over as drift, so
#: a field the retired writer once emitted cannot slip through unverified.
#:
#: Membership here is a claim that the field is explicitly covered by the
#: structural or binding checks in ``verify_manifest``. The release and
#: feature-policy identity fields are covered structurally only: their values
#: are historical facts and have no live authority to compare against.
#: ``tests/test_v013_evidence_validator.py`` pins the roster so a newly added
#: manifest field cannot silently escape review.
#:
#: ``phase4c_artifact_sha256`` is deliberately absent: the committed v0.1.3
#: manifest never declared it (phase4c was a promote-only artifact and this
#: manifest's ``promotion_eligible`` is ``false``), and phase4c validation
#: was fully retired along with the rest of the query-context promotion
#: chain -- see docs/dev_plans/20260824-feature-query-context-promotion.md.
#: A manifest that declares it now is a roster the writer never produced;
#: the explicit check below fails closed on it with a dedicated message
#: rather than falling through to the generic "uncovered field" drift.
_MANIFEST_VERIFIED_FIELDS = frozenset(
    {
        "inputs",
        "schema_hash",
        "manifest_phase",
        "promotion_eligible",
        "reason",
        "release_version",
        "feature_policy_fingerprint",
        "phase3_completion_hash",
        "phase3_command_digest",
    }
)


def verify_manifest(manifest_path: Path) -> list[str]:
    """Re-derive the committed v0.1.3 manifest's evidence bindings and report drift.

    This is now the sole surviving half of what used to be a writer/verifier
    pair: the manifest writer (and the server-side runtime consumer that
    read the manifest it produced, ``server.config.load_promotion_manifest``)
    were retired along with the query-context promotion chain -- see
    docs/dev_plans/20260824-feature-query-context-promotion.md. The v0.1.3
    manifest this function checks is a frozen, one-time-committed artifact
    that will never be re-stamped, so keeping ongoing drift protection on it
    (rather than deleting this function too) still has value: it catches an
    evidence artifact edited without regenerating the manifest that vouches
    for it, even with no writer left to regenerate it.

    What is checked, and why each one is drift worth failing on:

    * every field ``server.config.MANIFEST_REQUIRED_FIELDS`` requires is
      present, and every field ``MANIFEST_STRING_FIELDS`` names is a JSON
      string -- the completeness guard at the end of this function only ever
      caught an *extra* field CI didn't recognize, never a required field
      that was missing or wrongly typed;
    * every declared ``inputs[*].sha256`` still matches the bytes at
      ``inputs[*].path`` -- i.e. nobody edited an evidence artifact without
      regenerating the manifest that vouches for it;
    * ``schema_hash`` still matches ``shared/schemas/v013-evidence.json``;
    * ``release_version`` and ``feature_policy_fingerprint`` remain valid
      stamped identity strings. They are not compared with the current
      release/config because this is a historical v0.1.3 check;
    * ``manifest_phase`` is one of the two known values and the ``inputs``
      *it* implies are all declared -- a ``final`` manifest must carry
      phase0-3 and a ``phase3_completion_hash``. It is a writer input, not
      something re-derivable from evidence, so this presence-and-consistency
      check plus its role in the verdict below is the whole of its coverage;
    * every *required* declared input still resolves and *passes its gate* --
      a manifest must not outlive the validity of the evidence it points at;
    * ``promotion_eligible`` **and** ``reason`` still equal what those gates
      now conclude, re-derived through :func:`_promotion_verdict` -- the
      check that stops a stale manifest from vouching for evidence the
      current tree no longer supports;
    * the Phase 3 completion artifact's ``source_commit`` and
      ``source_tree_hash`` still match the identity stamped in the manifest,
      and its ``command_digest`` still matches the manifest's copied
      ``phase3_command_digest`` provenance binding;

    **The empty return is the security property.** An empty list must mean
    every one of the above was actually evaluated, never that some of them
    were skipped: the pre-fix guard was ``if {"phase0","phase1","phase2"} <=
    resolved.keys()``, so a manifest that simply *omitted* an ``inputs`` entry
    never entered the branch and a forged ``promotion_eligible=true`` passed
    with exit 0 (round-4 confirm pass, Logic + Security findings). Every
    "cannot check this" path below therefore appends drift rather than
    falling through.

    Returns a list of human-readable drift descriptions; empty means clean.
    Raises ``EvidenceGateError`` only when the manifest itself cannot be read
    or is structurally unusable.
    """
    manifest = load_json(manifest_path)
    if not isinstance(manifest, dict):
        raise EvidenceGateError(f"{manifest_path}: manifest must be a JSON object")

    drift: list[str] = []

    # Presence + type of every field the frozen manifest format requires.
    # The uncovered-field guard at the end of this function only ever caught
    # an *extra*, unrecognized field -- never a required field that was
    # absent or wrongly typed -- so this gate could report clean
    # (drift == []) on a manifest shape that structurally violates the
    # format: delete `deployed_at_utc`, or set `source_commit` to a
    # non-string, and this function had nothing that would notice (round-5
    # restart, Architecture finding -- the exact asymmetry the round-4
    # `confined_evidence_input_path` fix closed for the declared-input-path
    # rule, now closed here for the top-level field roster). Reusing
    # `server.config.MANIFEST_REQUIRED_FIELDS`/`MANIFEST_STRING_FIELDS`
    # rather than a second hand-copied roster keeps this check aligned with
    # the same constants the (now-retired) runtime loader used to enforce
    # (`load_promotion_manifest`; see
    # docs/dev_plans/20260824-feature-query-context-promotion.md).
    missing_required = MANIFEST_REQUIRED_FIELDS - set(manifest)
    if missing_required:
        drift.append(
            f"manifest is missing required field(s) {sorted(missing_required)} -- "
            "violates the frozen manifest format"
        )
    wrongly_typed_fields = sorted(
        name
        for name in MANIFEST_STRING_FIELDS
        if name in manifest and not isinstance(manifest[name], str)
    )
    if wrongly_typed_fields:
        drift.append(
            f"manifest field(s) {wrongly_typed_fields} must be JSON strings -- "
            "violates the frozen manifest format"
        )
    promotion_eligible = manifest.get("promotion_eligible")
    if type(promotion_eligible) is not bool:
        drift.append(
            "manifest field 'promotion_eligible' must be an exact JSON boolean -- "
            f"got {promotion_eligible!r}"
        )

    manifest_phase = manifest.get("manifest_phase")
    if manifest_phase not in ("provisional", "final"):
        drift.append(
            f"manifest_phase {manifest_phase!r} is neither 'provisional' nor 'final'; "
            "the verdict it governs cannot be re-derived"
        )
        manifest_phase = None

    inputs = manifest.get("inputs")
    if not isinstance(inputs, dict) or not inputs:
        raise EvidenceGateError(f"{manifest_path}: manifest declares no inputs")

    # The input set the manifest's own `manifest_phase` implies: phase0/1/2
    # for either phase, additionally phase3 for `final`; phase3-on-a-
    # provisional is an optional extra. `phase4c` is not in this roster at
    # all -- it is rejected explicitly below, not merely left unrequired --
    # because phase4c validation was fully retired (see the
    # `_MANIFEST_VERIFIED_FIELDS` comment above). The frozen v0.1.3
    # manifest was produced under this same roster by the now-retired
    # writer (`write_manifest`; see
    # docs/dev_plans/20260824-feature-query-context-promotion.md). Derived
    # from `server.config.MANIFEST_REQUIRED_FINAL_INPUTS` -- the named
    # constant the (also retired) runtime loader `load_promotion_manifest`
    # required a `final` manifest's `inputs` to cover -- rather than a
    # hand-copied `{"phase0", "phase1", "phase2"}` literal, so this
    # historical-drift gate stays aligned with the format the writer and
    # loader once shared (round-5 restart, Architecture finding). The
    # provisional roster is likewise a named `server.config` constant -- and
    # an independent literal there, not `FINAL - {"phase3"}`: either
    # spelling of that subtraction, here or in `server.config`, makes a
    # phase added to the `final` roster start being required of provisional
    # manifests automatically (round 7 confirm pass 4, Logic Minor).
    required_phases = set(
        MANIFEST_REQUIRED_FINAL_INPUTS
        if manifest_phase == "final"
        else MANIFEST_REQUIRED_PROVISIONAL_INPUTS
    )

    if "phase4c" in inputs:
        drift.append(
            "inputs.phase4c is no longer a supported input -- phase4c validation was "
            "retired along with the rest of the query-context promotion chain"
        )

    # Fail closed on an input phase nothing below knows how to gate-check:
    # the per-entry loop only hash-verifies a declared input against the
    # bytes at its path, and the verdict re-derivation further down only
    # runs the known phase0-3 gates, so an unrecognized extra `inputs`
    # entry would otherwise verify clean without any gate ever evaluating
    # its artifact -- the same asymmetry the trailing uncovered-field guard
    # closes for top-level manifest fields. `MANIFEST_REQUIRED_FINAL_INPUTS`
    # is the complete roster of known phases (provisional is a subset);
    # `phase4c` is excluded here only because it draws its dedicated
    # retirement message above.
    unrecognized_phases = sorted(set(inputs) - MANIFEST_REQUIRED_FINAL_INPUTS - {"phase4c"})
    if unrecognized_phases:
        drift.append(
            f"inputs declares unrecognized phase(s) {unrecognized_phases} that no gate "
            "here knows how to verify"
        )

    resolved: dict[str, Path] = {}
    for phase, entry in sorted(inputs.items()):
        if phase == "phase4c":
            continue
        if not isinstance(entry, dict):
            drift.append(f"inputs.{phase} is not an object")
            continue
        declared_path = entry.get("path")
        declared_sha = entry.get("sha256")
        if not isinstance(declared_path, str) or not isinstance(declared_sha, str):
            drift.append(f"inputs.{phase} is missing a string path/sha256")
            continue
        # Confined by calling the shared `confined_evidence_input_path`
        # primitive rather than re-implementing the rule, which is how the
        # copy that used to live here came to accept absolute in-repo paths
        # the (now-retired) runtime loader `load_promotion_manifest` used to
        # reject outright (round-4 confirm pass, Architecture + Security
        # findings). The path comes from the artifact under scrutiny, so it
        # is not a trustworthy read target on its own.
        candidate = confined_evidence_input_path(declared_path, allowed_root=REPO_ROOT)
        if candidate is None:
            drift.append(
                f"inputs.{phase}.path is absolute or escapes the repo tree: {declared_path!r}"
            )
            continue
        try:
            actual_sha = sha256_file(candidate)
        except EvidenceGateError as exc:
            drift.append(f"inputs.{phase} is unreadable: {exc}")
            continue
        if actual_sha != declared_sha:
            # Only the digest prefix is echoed: `declared_path` is
            # manifest-declared and confined to the repo tree but otherwise
            # free, so printing the full digest of whatever it names would make
            # the CI log a general hashing oracle over the tree. A prefix is
            # enough to tell one artifact revision from another, which is all a
            # human debugging drift needs.
            drift.append(
                f"inputs.{phase} digest drift: manifest says {declared_sha}, "
                f"{declared_path} now hashes to {actual_sha[:12]}..."
            )
            continue
        resolved[phase] = candidate

    for phase in sorted(required_phases - set(inputs)):
        drift.append(
            f"inputs.{phase} is missing; a {manifest_phase!r} manifest must declare it "
            "and promotion_eligible cannot be re-derived without it"
        )

    if SCHEMA_PATH.exists():
        actual_schema_hash = sha256_file(SCHEMA_PATH)
        if manifest.get("schema_hash") != actual_schema_hash:
            drift.append(
                f"schema_hash drift: manifest says {manifest.get('schema_hash')}, "
                f"{SCHEMA_PATH.name} now hashes to {actual_schema_hash}"
            )
    else:
        drift.append(f"evidence schema missing, cannot verify schema_hash: {SCHEMA_PATH}")

    if manifest_phase == "final" and "phase3_completion_hash" not in manifest:
        drift.append("a 'final' manifest must carry phase3_completion_hash")

    # Re-run the gates over the still-matching inputs and re-derive the
    # verdict. `unresolved` collapses "never declared" and "declared but the
    # digest/gate check above rejected it" -- either way the verdict is not
    # re-derivable, and that must be reported, not skipped.
    unresolved = sorted(required_phases - resolved.keys())
    phase3_command_digest_checked = False
    if unresolved:
        drift.append(
            "promotion_eligible/reason could not be re-derived: required input(s) "
            f"{unresolved} are missing or did not resolve"
        )
    elif manifest_phase is None:
        drift.append(
            "promotion_eligible/reason could not be re-derived: manifest_phase is unusable"
        )
    else:
        try:
            phase0_records = validate_artifact("phase0", resolved["phase0"])
            phase1_records = validate_artifact("phase1", resolved["phase1"])
            transport_record = _validate_transport_contract(resolved["phase2"])
            phase3_record = (
                _read_phase3_completion(resolved["phase3"]) if "phase3" in resolved else None
            )
        except EvidenceGateError as exc:
            drift.append(f"a declared input no longer passes its gate: {exc}")
        else:
            real_stratum_present = has_real_provider_stratum(
                [r for r in phase0_records if r["phase"] == "phase0"]
                + [r for r in phase1_records if r["phase"] == "phase1"]
            )
            expected_eligible, expected_reason = _promotion_verdict(
                manifest_phase=manifest_phase,
                real_stratum_present=real_stratum_present,
                transport_eligible=transport_record["promotion_eligible"],
            )
            if type(promotion_eligible) is bool and promotion_eligible != expected_eligible:
                drift.append(
                    f"promotion_eligible drift: manifest says "
                    f"{promotion_eligible!r}, the current evidence supports "
                    f"{expected_eligible!r}"
                )
            if manifest.get("reason") != expected_reason:
                drift.append(
                    f"reason drift: manifest says {manifest.get('reason')!r}, "
                    f"the current evidence supports {expected_reason!r}"
                )
            if phase3_record is not None:
                phase3_command_digest_checked = True
                for field in ("source_commit", "source_tree_hash"):
                    if phase3_record[field] != manifest.get(field):
                        drift.append(
                            f"phase3 completion {field} does not match the manifest's stamped "
                            f"{field} ({phase3_record[field]!r} vs {manifest.get(field)!r})"
                        )
                if (
                    "phase3_command_digest" in manifest
                    and manifest["phase3_command_digest"] != phase3_record["command_digest"]
                ):
                    drift.append(
                        "phase3_command_digest does not match the resolved phase3 artifact's "
                        f"command_digest ({manifest['phase3_command_digest']!r} vs "
                        f"{phase3_record['command_digest']!r})"
                    )

    if "phase3_command_digest" in manifest and not phase3_command_digest_checked:
        drift.append(
            "phase3_command_digest could not be checked: the phase3 completion artifact it "
            "is copied from did not resolve"
        )

    if "phase4c_artifact_sha256" in manifest:
        drift.append(
            "phase4c_artifact_sha256 is no longer a supported field -- phase4c validation "
            "was retired along with the rest of the query-context promotion chain"
        )

    # The digests the manifest repeats at top level must agree with the
    # `inputs` entries they are copied from -- a hand-edited manifest could
    # otherwise pass every check above while its top-level binding pointed at
    # different bytes than the input it claims to summarize.
    for field, phase in (("phase3_completion_hash", "phase3"),):
        if field not in manifest:
            continue
        entry = inputs.get(phase)
        declared = entry.get("sha256") if isinstance(entry, dict) else None
        if manifest[field] != declared:
            drift.append(
                f"{field} does not match inputs.{phase}.sha256 "
                f"({manifest[field]!r} vs {declared!r})"
            )

    # Fail closed on a manifest field nothing above knows how to verify: a
    # field present in the committed manifest but absent from both rosters
    # below would otherwise be silently unchecked, which is how this job
    # came to prove nothing in the first place. The manifest is frozen and
    # has no writer anymore, so this guard exists purely to keep the two
    # rosters honest against the one manifest that will ever exist.
    uncovered = set(manifest) - _MANIFEST_VOLATILE_FIELDS - _MANIFEST_VERIFIED_FIELDS
    if uncovered:
        drift.append(
            f"manifest field(s) {sorted(uncovered)} are covered by neither the drift check "
            "nor the documented volatile set -- extend verify_manifest()"
        )

    return drift


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=sorted(PHASE_MINIMUMS))
    parser.add_argument("--input", type=Path)
    parser.add_argument(
        "--verify-manifest",
        type=Path,
        help=(
            "Read-only drift check on the committed v0.1.3 promotion manifest: re-derive "
            "its evidence bindings and exit non-zero if they no longer hold. Does not write."
        ),
    )
    args = parser.parse_args(argv)

    if args.verify_manifest is not None:
        try:
            drift = verify_manifest(args.verify_manifest)
        except (EvidenceGateError, OSError) as exc:
            print(f"FAIL: {exc}", file=sys.stderr)
            return 1
        if drift:
            print(
                f"FAIL: {args.verify_manifest} has drifted from the evidence it binds:",
                file=sys.stderr,
            )
            for item in drift:
                print(f"  - {item}", file=sys.stderr)
            return 1
        print(f"OK: {args.verify_manifest} still matches the evidence it binds")
        return 0

    if not args.phase or not args.input:
        parser.error("--phase and --input are required unless --verify-manifest is given")

    try:
        records = validate_artifact(args.phase, args.input)
    except EvidenceGateError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    print(f"OK: {len(records)} record(s) validated for phase={args.phase}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
