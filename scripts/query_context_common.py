"""Shared raw-record vocabulary and pure helpers for the Phase 4A
query-context narrowing experiment's runner, collector, and analyzer.

``scripts/run_query_context_experiment.py`` (the runner) is primarily a CLI
entry point -- argparse plus ``main()`` -- but ``scripts/collect_query_context_latency.py``
and ``scripts/analyze_query_context_latency.py`` each need three of its pure
helpers (``validate_raw_record``, ``scorer_hash``, ``load_fixture``) and the
raw-record field vocabulary those helpers close over. Importing them from the
runner module dragged that CLI entry point's whole module scope -- argparse
setup, ``main()``, ``run_live()`` -- into two scripts whose job is to
validate and analyze already-collected evidence, not to run the experiment.
This module is the dependency-light home for that shared vocabulary and
those helpers, so the collector and analyzer no longer need to import a CLI
entry-point module to get them. The runner re-exports these names so every
existing reference to them keeps working unchanged.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from scripts.evidence_common import (
    EvidenceGateError,
    closed_object,
    load_json,
    require_type,
)

SCORER_VERSION = "v1"
SELECTABLE_DIMENSIONS = frozenset({"history_count", "answer_chars"})
CONDITIONS = ("baseline", "narrowed")

RAW_REQUIRED = frozenset(
    {
        "run_id",
        "run_block",
        "run_order",
        "fixture_version",
        "fixture_turn_id",
        "condition",
        "selected_dimension",
        "selected_value",
        "context_chars",
        "query_chars",
        "provider",
        "model",
        "latency_ms",
        "outcome",
        "quality_score",
        "matched_fact_ids",
        "matched_citation_ids",
        "matched_disallowed_claim_ids",
        "scorer_version",
        "scorer_hash",
        "attempt_count",
        "retry_count",
        "rate_limit_count",
        "cache_status",
        "retrieval_snapshot_id",
        "recorded_at_utc",
    }
)
# `fixture_sha256` is populated by the collector (a sha256 of the exact
# fixture file bytes each record's matched IDs/quality_score were resolved
# against), not by the runner's raw output, so it is allowed but not
# required -- an artifact predating this field is still valid raw shape.
RAW_ALLOWED = RAW_REQUIRED | frozenset({"fixture_sha256"})

# Fields deliberately excluded from `scorer_hash`'s whole-record binding.
# `scorer_hash` is the digest itself; `fixture_sha256` is stamped by the
# collector *after* the runner has scored and hashed the record, and has its
# own binding path (compared against `sha256_file(fixture_path)`).
_UNBOUND_RECORD_FIELDS = frozenset({"scorer_hash", "fixture_sha256"})


OUTCOMES = frozenset({"success", "error", "timeout"})
CACHE_STATUSES = frozenset({"hit", "miss", "unknown"})

# (field, python types, minimum) for the numeric raw fields, mirroring
# `shared/schemas/v013-query-context-raw.json`'s type/minimum constraints.
_RAW_NUMERIC_FIELDS: tuple[tuple[str, tuple[type, ...], float], ...] = (
    ("run_block", (int,), 0),
    ("run_order", (int,), 0),
    ("selected_value", (int,), 0),
    ("context_chars", (int,), 0),
    ("query_chars", (int,), 0),
    ("latency_ms", (int, float), 0),
    ("attempt_count", (int,), 1),
    ("retry_count", (int,), 0),
    ("rate_limit_count", (int,), 0),
)
_RAW_STRING_FIELDS = (
    "run_id",
    "fixture_version",
    "fixture_turn_id",
    "provider",
    "model",
    "scorer_version",
    "recorded_at_utc",
)
_RAW_ID_LIST_FIELDS = (
    "matched_fact_ids",
    "matched_citation_ids",
    "matched_disallowed_claim_ids",
)


def validate_raw_record(record: dict[str, Any], *, where: str = "record") -> None:
    """Full type/range/enum validation of one raw record against the schema.

    Both the collector and the analyzer need this: a hand-edited or
    externally produced JSONL can reach either one, and a key-set
    (`closed_object`) check alone lets wrong numeric types, negative values,
    and invalid enums through to skew statistics or raise downstream. It
    lives here because this module already owns `RAW_REQUIRED`/`RAW_ALLOWED`,
    so the allowlist and the field contract cannot drift apart.
    """
    closed_object(record, required=RAW_REQUIRED, allowed=RAW_ALLOWED)
    for field in _RAW_STRING_FIELDS:
        value = record[field]
        require_type(value, (str,), f"{where}: {field}")
        if not value:
            raise EvidenceGateError(f"{where}: {field} must be non-empty")
    if record["condition"] not in CONDITIONS:
        raise EvidenceGateError(f"{where}: invalid condition {record['condition']!r}")
    if record["selected_dimension"] not in SELECTABLE_DIMENSIONS:
        raise EvidenceGateError(
            f"{where}: invalid selected_dimension {record['selected_dimension']!r}"
        )
    if record["outcome"] not in OUTCOMES:
        raise EvidenceGateError(f"{where}: invalid outcome {record['outcome']!r}")
    if record["cache_status"] not in CACHE_STATUSES:
        raise EvidenceGateError(f"{where}: invalid cache_status {record['cache_status']!r}")
    # A record's declared `scorer_version` was previously never checked against
    # anything -- `scorer_hash` bound the module constant `SCORER_VERSION`
    # regardless of what a record claimed, so a uniformly forged
    # `scorer_version` across an entire batch produced an identical, correctly
    # matching digest and passed every gate. Rejecting any declared value that
    # is not the actual current scorer version closes that gap independent of
    # hash binding.
    if record["scorer_version"] != SCORER_VERSION:
        raise EvidenceGateError(
            f"{where}: scorer_version {record['scorer_version']!r} does not match the "
            f"current scorer version {SCORER_VERSION!r}"
        )

    for field, kinds, minimum in _RAW_NUMERIC_FIELDS:
        value = record[field]
        require_type(value, kinds, f"{where}: {field}")
        if value < minimum:
            raise EvidenceGateError(f"{where}: {field} must be >= {minimum}, got {value!r}")

    require_type(record["quality_score"], (int, float), f"{where}: quality_score")
    if not (0.0 <= float(record["quality_score"]) <= 1.0):
        raise EvidenceGateError(f"{where}: quality_score must be within [0, 1]")

    for field in _RAW_ID_LIST_FIELDS:
        value = record[field]
        require_type(value, (list,), f"{where}: {field}")
        for item in value:
            if not isinstance(item, str) or not item:
                raise EvidenceGateError(f"{where}: {field} entries must be non-empty strings")

    digest = record["scorer_hash"]
    require_type(digest, (str,), f"{where}: scorer_hash")
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise EvidenceGateError(f"{where}: scorer_hash must be a 64-character lowercase hex digest")

    snapshot = record["retrieval_snapshot_id"]
    if snapshot is not None:
        require_type(snapshot, (str,), f"{where}: retrieval_snapshot_id")
        if not snapshot:
            raise EvidenceGateError(f"{where}: retrieval_snapshot_id must be non-empty or null")

    if "fixture_sha256" in record:
        fixture_digest = record["fixture_sha256"]
        require_type(fixture_digest, (str,), f"{where}: fixture_sha256")
        if len(fixture_digest) != 64 or any(c not in "0123456789abcdef" for c in fixture_digest):
            raise EvidenceGateError(
                f"{where}: fixture_sha256 must be a 64-character lowercase hex digest"
            )


def scorer_hash(
    fixture_version: str,
    fixture_turn_id: str | None = None,
    *,
    matched_fact_ids: list[str] | None = None,
    matched_citation_ids: list[str] | None = None,
    matched_disallowed_claim_ids: list[str] | None = None,
    quality_score: float | None = None,
    scorer_version: str = SCORER_VERSION,
    record: Mapping[str, Any] | None = None,
) -> str:
    """Provenance hash binding a scorer identity to a fixture (and, when the
    per-record fields are supplied, to one record's exact matched-ID sets and
    quality_score).

    Called two ways: ``scorer_hash(fixture_version)`` is the coarse
    scorer/fixture identity binding -- it changes whenever the scorer version
    or fixture version changes. The fully-parameterized form additionally
    binds one record's sorted matched-ID sets and quality_score, so a caller
    who forges or edits a matched ID after scoring -- without re-running the
    scorer -- changes this hash's expected value, letting
    ``collect_query_context_latency.py`` detect the forgery by recomputing
    and comparing, with no fixture file of its own to consult.

    ``scorer_version`` defaults to the module constant for internal callers
    stamping a hash for a record they are about to emit (there is no
    record yet to read a declared version from). A verifier recomputing this
    hash to check an *existing* record must instead pass
    ``record["scorer_version"]`` explicitly -- otherwise recomputing with the
    constant regardless of what the record declares lets a uniformly forged
    ``scorer_version`` produce a matching digest for every record in a batch.

    ``record`` binds the *whole* raw record, not just the scoring fields. The
    scoring-field-only payload authenticated exactly the values the scorer
    produced and nothing else, which left the measurement and stratum-identity
    fields the promotion decision actually rests on -- ``latency_ms``,
    ``condition``, ``selected_value``, ``provider``, ``model``, ``outcome``,
    ``cache_status``, ``retrieval_snapshot_id``, the attempt/retry/rate-limit
    counters -- entirely unauthenticated. Since the analyzer's synthetic guard
    is a name denylist over those same unbound strings
    (``SYNTHETIC_STRATA``), a dry-run artifact could be relabelled into the
    paid stratum with any chosen latency and still satisfy every ID, quality,
    fixture-digest, and hash check. Binding the full record closes that: any
    post-scoring edit to any field invalidates the digest.

    ``scorer_hash`` itself is excluded (it is the digest being computed) and
    so is ``fixture_sha256``, which the *collector* stamps onto a record after
    the runner has already scored and hashed it; including it would make the
    runner's stamp and every verifier's recomputation disagree by
    construction. ``fixture_sha256`` has its own binding path -- it is
    compared against ``sha256_file(fixture_path)``.

    This remains a keyless self-hash: it detects edits to a runner-produced
    artifact, not a wholly regenerated one. Fixture resolution
    (``validate_against_fixture``) is the anti-fabrication layer on top. A
    threat model that includes regeneration needs an HMAC stamped with a
    collection-time key, or a signed artifact, not a bare SHA-256.
    """
    fixture_turn_id = fixture_turn_id or ""
    matched_fact_ids = matched_fact_ids or []
    matched_citation_ids = matched_citation_ids or []
    matched_disallowed_claim_ids = matched_disallowed_claim_ids or []
    bound_record = (
        {key: value for key, value in sorted(record.items()) if key not in _UNBOUND_RECORD_FIELDS}
        if record is not None
        else None
    )
    payload = json.dumps(
        {
            "scorer_version": scorer_version,
            "fixture_version": fixture_version,
            "fixture_turn_id": fixture_turn_id,
            "matched_fact_ids": sorted(matched_fact_ids),
            "matched_citation_ids": sorted(matched_citation_ids),
            "matched_disallowed_claim_ids": sorted(matched_disallowed_claim_ids),
            "quality_score": round(float(quality_score), 6) if quality_score is not None else None,
            "record": bound_record,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def load_fixture(path: Path) -> dict[str, Any]:
    """Load and shape-check the quality fixture.

    Reads through ``evidence_common.load_json`` (round-3 restart gauntlet,
    Security finding). This used a raw ``path.open()`` while every other
    evidence-gate input in this branch -- ``validate_v013_evidence.py``,
    ``analyze_query_context_latency.py``, ``collect_query_context_latency.py``,
    ``validate_phase2_transport_browser_contract.py`` -- already routed its
    ``--input`` through ``load_json``/``read_bytes_no_follow``. The asymmetry
    mattered because this module's *write* side is already confined and
    ``O_NOFOLLOW``-guarded, and the branch's own threat model treats an
    operator-supplied CLI path as attacker-influenced (see
    ``confined_output_path``'s docstring): a FIFO or ``/dev/zero`` planted at
    the fixture path would hang or exhaust memory before the run started.
    ``load_json`` applies ``O_NOFOLLOW|O_NONBLOCK``, the ``fstat`` ``S_ISREG``
    check on the held fd, and the ``_MAX_EVIDENCE_INPUT_BYTES`` cap.

    ``--fixture`` is deliberately *not* additionally confined to ``REPO_ROOT``
    the way ``--output`` is. The two are not symmetric: output confinement
    stops a run from *writing* to an arbitrary path, which is a real
    primitive; a read path the operator names is only a disclosure risk if
    its contents reach a sink, and only the fixture's own version string and
    turn IDs ever reach the artifact. Confining it would also be wrong here in
    practice -- the collector and analyzer legitimately read the repo's
    committed fixture while writing their outputs elsewhere, so one
    ``REPO_ROOT`` cannot serve as both roots. The concrete harms the finding
    named (FIFO hang, ``/dev/zero`` memory exhaustion, symlink read-through)
    are all closed by the hardened read above.
    """
    fixture = load_json(path)
    if not isinstance(fixture, dict) or "fixture_version" not in fixture or "turns" not in fixture:
        raise EvidenceGateError(f"{path}: fixture must be an object with fixture_version/turns")
    if not fixture["turns"]:
        raise EvidenceGateError(f"{path}: fixture must declare at least one turn")
    return fixture
