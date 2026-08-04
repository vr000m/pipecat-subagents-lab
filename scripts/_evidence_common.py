"""Shared status vocabulary and helpers for the v0.1.3 evidence-gate scripts.

``scripts/validate_v013_evidence.py``, ``scripts/validate_phase2_transport_browser_contract.py``,
and ``scripts/emit_v013_deployment_metadata.py`` all need the same closed
"why isn't this promotion-eligible" vocabulary and the same JSON/JSONL
loading primitives. This module is the one place that vocabulary and those
primitives live, so the three scripts cannot silently drift from each other.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from enum import Enum
from pathlib import Path
from typing import Any


class EvidenceStatus(str, Enum):
    """Closed vocabulary for an evidence artifact's or gate's outcome.

    ``PROMOTION_ELIGIBLE`` is the only status a promotion consumer may treat
    as data-driven-tuning-enabled; every other status means "fail closed to
    display-only", regardless of whether the artifact is otherwise schema-valid.
    """

    PROMOTION_ELIGIBLE = "promotion_eligible"
    EVIDENCE_UNAVAILABLE = "evidence_unavailable"
    BLOCKED = "blocked"
    NOT_RUN = "not_run"
    UNAVAILABLE_ONLY = "unavailable_only"
    REAL_STRATUM_MISSING = "real_stratum_missing"
    CONTAMINATED = "contaminated"
    MALFORMED = "malformed"
    UNVERIFIED_SOURCE = "unverified_source"
    INVALID_FAKE_ROUTE = "invalid_fake_route"
    AUDIBILITY_UNVERIFIED = "audibility_unverified"
    STALE = "stale"
    SCHEMA_INVALID = "schema_invalid"


class EvidenceGateError(ValueError):
    """Raised when an evidence artifact or manifest fails a hard gate check."""


def sha256_file(path: Path) -> str:
    """Return the hex SHA-256 digest of a file's bytes."""
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def load_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError as exc:
        raise EvidenceGateError(f"missing evidence input: {path}") from exc
    except json.JSONDecodeError as exc:
        raise EvidenceGateError(f"malformed JSON in {path}: {exc}") from exc


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise EvidenceGateError(f"missing evidence input: {path}")
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise EvidenceGateError(f"{path}: line {line_no}: invalid JSON ({exc})") from exc
            if not isinstance(record, dict):
                raise EvidenceGateError(f"{path}: line {line_no}: expected a JSON object")
            records.append(record)
    return records


def require_type(value: Any, kinds: tuple[type, ...], field: str) -> None:
    if isinstance(value, bool) and bool not in kinds:
        raise EvidenceGateError(f"{field} must not be a bool, got {value!r}")
    if not isinstance(value, kinds):
        raise EvidenceGateError(f"{field} expected {kinds}, got {type(value).__name__}")


def require_nonempty_str(value: Any, field: str) -> str:
    require_type(value, (str,), field)
    if not value:
        raise EvidenceGateError(f"{field} must be non-empty")
    return value


def closed_object(
    record: Mapping[str, Any], *, required: frozenset[str], allowed: frozenset[str]
) -> None:
    """Assert a JSON object carries exactly the allowed/required keys (additionalProperties=false)."""
    unknown = set(record) - allowed
    if unknown:
        raise EvidenceGateError(f"unknown field(s) {sorted(unknown)}")
    missing = required - set(record)
    if missing:
        raise EvidenceGateError(f"missing required field(s) {sorted(missing)}")
