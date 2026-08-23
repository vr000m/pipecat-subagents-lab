"""Shared status vocabulary and helpers for the v0.1.3 evidence-gate scripts.

``scripts/validate_v013_evidence.py``, ``scripts/validate_phase2_transport_browser_contract.py``,
and ``scripts/emit_v013_deployment_metadata.py`` all need the same closed
"why isn't this promotion-eligible" vocabulary and the same JSON/JSONL
loading primitives. This module is the one place that vocabulary and those
primitives live, so the three scripts cannot silently drift from each other.
"""

from __future__ import annotations

import errno
import json
import os
import re
from collections.abc import Callable, Mapping
from enum import Enum
from pathlib import Path
from stat import S_ISREG
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


# Gates two halves of the same Phase 4 promotion decision (the collector's
# per-cell undersizing check and the analyzer's per-stratum pairing check);
# defined once here so the two scripts cannot drift apart on the minimum.
MIN_PAIRED_SAMPLES_PER_CELL = 30


def sha256_file(path: Path) -> str:
    """Return the hex SHA-256 digest of a file's bytes."""
    import hashlib

    return hashlib.sha256(read_bytes_no_follow(path)).hexdigest()


def sha256_bytes(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def load_json(path: Path) -> Any:
    """Read one JSON document, converting *every* read failure into a gate error.

    Routes through ``read_bytes_no_follow`` so every read failure mode --
    missing, symlinked, a FIFO/device, or oversized -- collapses to one
    controlled ``EvidenceGateError``, the same guarantee the raw-``OSError``
    handling below already gave the more ordinary failure modes.
    """
    data = read_bytes_no_follow(path)
    try:
        return json.loads(data)
    except json.JSONDecodeError as exc:
        raise EvidenceGateError(f"malformed JSON in {path}: {exc}") from exc
    except UnicodeDecodeError as exc:
        raise EvidenceGateError(f"undecodable evidence input {path}: {exc}") from exc


def load_jsonl(
    path: Path,
    *,
    validate_record: Callable[[int, dict[str, Any]], None] | None = None,
) -> list[dict[str, Any]]:
    """Read a JSONL file, converting every read/parse failure into a gate error.

    ``validate_record``, when given, is called as ``validate_record(line_no,
    record)`` immediately after each record is parsed and before it is
    appended -- so a caller needing per-line-number diagnostics (the
    collector's raw-record validation) can raise its own
    ``EvidenceGateError`` with that context, without duplicating this
    function's own read/parse handling.
    """
    data = read_bytes_no_follow(path)
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise EvidenceGateError(f"undecodable evidence input {path}: {exc}") from exc
    records: list[dict[str, Any]] = []
    for line_no, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise EvidenceGateError(f"{path}: line {line_no}: invalid JSON ({exc})") from exc
        if not isinstance(record, dict):
            raise EvidenceGateError(f"{path}: line {line_no}: expected a JSON object")
        if validate_record is not None:
            validate_record(line_no, record)
        records.append(record)
    return records


# Unanchored + `fullmatch` (the `server.config._is_hex_hash` idiom) rather than
# `^...$` + `match`: `$` also matches just before a trailing newline, so
# `"a" * 64 + "\n"` passed the anchored version.
HEX64_RE = re.compile(r"[0-9a-f]{64}")


def require_hex64(value: Any, field: str) -> str:
    """Require an exact lowercase 64-char hex SHA-256 digest.

    Length alone is not a digest check: ``"z" * 64`` is 64 characters and no
    kind of hash. Every SHA-256 binding in the evidence gates goes through
    here so none of them re-implements a weaker version of the same check.
    """
    require_type(value, (str,), field)
    if HEX64_RE.fullmatch(value) is None:
        raise EvidenceGateError(f"{field} must be a 64-character lowercase hex SHA-256 digest")
    return value


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


class FixtureIndex:
    """The versioned Phase 4 quality fixture's own view of what a record may
    legally claim.

    Neither the collector nor the analyzer can re-run `score_response`: raw
    records deliberately carry no response text (that is the point of the
    strict allowlist). What each *can* do -- and, before this was shared,
    only the collector did -- is refuse to take a record's self-reported
    matches on trust. Recomputing `scorer_hash` from fields the record
    itself supplies proves only internal consistency: an editor who invents
    unknown or duplicate matches and recomputes the hash from those
    fabricated values passes that check. Every match ID is therefore
    resolved against the versioned fixture, and `quality_score` is
    recomputed from the fixture's own denominator. Shared here so the
    collector's and analyzer's forgery checks cannot drift apart.
    """

    def __init__(self, fixture: dict[str, Any]) -> None:
        self.version: str = fixture["fixture_version"]
        self.turns: dict[str, dict[str, Any]] = {t["turn_id"]: t for t in fixture["turns"]}

    def turn_for(self, fixture_turn_id: str) -> dict[str, Any]:
        # The runner pairs repeats as "<turn_id>#<repeat_index>".
        base_id = fixture_turn_id.split("#", 1)[0]
        turn = self.turns.get(base_id)
        if turn is None:
            raise EvidenceGateError(
                f"fixture_turn_id {fixture_turn_id!r} does not resolve to a turn in fixture "
                f"version {self.version!r}"
            )
        return turn


def _check_ids_against_fixture(
    claimed: list[str], known: list[str], *, field: str, where: str
) -> None:
    unknown = [value for value in claimed if value not in set(known)]
    if unknown:
        raise EvidenceGateError(
            f"{where}: {field} contains ID(s) {sorted(unknown)} that the versioned "
            "fixture does not declare -- forged or fabricated match"
        )
    if len(claimed) != len(set(claimed)):
        raise EvidenceGateError(f"{where}: {field} contains duplicate IDs")


def validate_against_fixture(record: dict[str, Any], *, index: FixtureIndex, where: str) -> None:
    """Resolve one raw record's fixture identity, match IDs, and quality_score
    against the versioned fixture ``index`` was built from.

    Raises ``EvidenceGateError`` for a fixture-version mismatch, an unknown
    fixture-turn identity, an invented/duplicate match ID, a citation
    claimed without its fixture-expected fact, or a ``quality_score`` that
    does not equal the fixture-derived score for the record's matched IDs.
    """
    if record["fixture_version"] != index.version:
        raise EvidenceGateError(
            f"{where}: fixture_version {record['fixture_version']!r} does not match the "
            f"loaded fixture {index.version!r}"
        )
    turn = index.turn_for(record["fixture_turn_id"])
    _check_ids_against_fixture(
        record["matched_fact_ids"],
        [fact["id"] for fact in turn["required_facts"]],
        field="matched_fact_ids",
        where=where,
    )
    _check_ids_against_fixture(
        record["matched_citation_ids"],
        [cite["id"] for cite in turn["expected_citations"]],
        field="matched_citation_ids",
        where=where,
    )
    _check_ids_against_fixture(
        record["matched_disallowed_claim_ids"],
        [claim["id"] for claim in turn["disallowed_claims"]],
        field="matched_disallowed_claim_ids",
        where=where,
    )
    # A citation is valid only when the fixture-expected fact it maps to was
    # also matched -- the same binding `score_response` applies.
    matched_facts = set(record["matched_fact_ids"])
    for cite in turn["expected_citations"]:
        if cite["id"] not in record["matched_citation_ids"]:
            continue
        expected_fact_id = cite.get("fact_id")
        if expected_fact_id is not None and expected_fact_id not in matched_facts:
            raise EvidenceGateError(
                f"{where}: citation {cite['id']!r} is claimed without its "
                f"fixture-expected fact {expected_fact_id!r}"
            )

    denominator = len(turn["required_facts"]) + len(turn["expected_citations"])
    if denominator == 0:
        raise EvidenceGateError(
            f"{where}: fixture turn {turn['turn_id']!r} has a zero quality-score denominator"
        )
    numerator = (
        len(record["matched_fact_ids"])
        + len(record["matched_citation_ids"])
        - len(record["matched_disallowed_claim_ids"])
    )
    expected_score = max(0.0, min(1.0, numerator / denominator))
    if abs(float(record["quality_score"]) - expected_score) > 1e-9:
        raise EvidenceGateError(
            f"{where}: quality_score {record['quality_score']!r} is not the fixture-derived "
            f"score {expected_score!r} for its matched IDs"
        )


# Mirrors the manifest writer's own predictable-path threat model (see
# ``validate_v013_evidence.py``'s ``_MAX_MANIFEST_BYTES``): every evidence
# artifact these scripts write lives at a repo-relative, predictable path, so
# an attacker who can plant a symlink or FIFO there is a threat this module
# must close for every writer, not just the promotion-manifest one.
_MAX_EVIDENCE_OUTPUT_BYTES = 8 * 1024 * 1024


def require_regular_fd(fd: int, path: Path, verb: str) -> None:
    """Reject anything that is not a regular file, from the *open fd*.

    ``O_NOFOLLOW`` covers symlinks and nothing else. An attacker who can plant
    a symlink at this predictable, repo-relative path can equally ``mkfifo``
    it, and opening a FIFO blocks forever (which ``O_NONBLOCK`` at the open
    turns into a non-blocking open, not into a safe read) -- or plant a device
    node. ``fstat`` on the fd we actually hold, rather than a ``stat`` before
    the open, is what closes the TOCTOU window between the check and the use.
    """
    st = os.fstat(fd)
    if not S_ISREG(st.st_mode):
        raise EvidenceGateError(f"{path}: refusing to {verb} -- not a regular file")


def read_bytes_no_follow(path: Path, *, max_bytes: int = _MAX_EVIDENCE_OUTPUT_BYTES) -> bytes:
    """Read `path`'s bytes, refusing to read *through* a symlink or hang on a FIFO.

    Mirrors ``write_bytes_no_follow``'s hardening on the read side: an
    attacker who can plant a symlink at a predictable, repo-relative evidence
    path can equally ``mkfifo`` it, and a plain ``open()``/``read()`` blocks
    forever on a reader-less FIFO's write end (or never reaches EOF against a
    character device such as ``/dev/zero``). ``O_NOFOLLOW`` rejects the
    symlink case outright (``ELOOP``); ``O_NONBLOCK`` plus
    ``require_regular_fd`` -- checked against the held fd, so the type check
    and the read cannot be TOCTOU'd apart -- rejects everything else. The
    read is additionally capped at ``max_bytes`` so an oversized input is
    never fully buffered into memory.
    """
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            raise EvidenceGateError(f"missing evidence input: {path}") from exc
        if exc.errno == errno.ENXIO:
            # Read-opening the write end of a reader-less FIFO with
            # O_NONBLOCK fails here rather than blocking; same verdict as
            # the fstat check below would give a FIFO opened some other way.
            raise EvidenceGateError(f"{path}: refusing to read -- not a regular file") from exc
        raise EvidenceGateError(f"unreadable evidence input {path}: {exc}") from exc
    try:
        require_regular_fd(fd, path, "read")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(fd, 1 << 20)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise EvidenceGateError(f"{path}: evidence input exceeds {max_bytes}-byte cap")
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(fd)


def write_bytes_no_follow(path: Path, payload: bytes) -> None:
    """Write `payload` to `path`, refusing to write *through* a symlink.

    ``O_NOFOLLOW`` makes an attacker-planted symlink at this predictable path
    fail with ``ELOOP`` instead of silently redirecting evidence bytes to the
    link's target. The path may legitimately already exist as a regular file
    (a prior run's output), so ``O_EXCL`` is not usable here -- ``O_NOFOLLOW``
    is the check that matters.

    ``O_NONBLOCK`` plus the ``require_regular_fd`` check closes the sibling
    gap ``O_NOFOLLOW`` does not cover: a FIFO planted at this path would
    otherwise make the open (and then the write) block indefinitely. The flag
    is cleared implicitly by closing the fd; nothing here needs it beyond the
    open.

    ``payload`` is rejected outright above ``_MAX_EVIDENCE_OUTPUT_BYTES`` --
    the threat-model bound this module documents but, before this, never
    enforced. ``os.write`` is also looped to completion: it is permitted to
    write fewer bytes than given (a short write), and a discarded return
    value there would silently truncate the artifact with no error raised.

    Every evidence-gate script that writes to a predictable, repo-relative
    path routes through this one function -- see
    ``validate_v013_evidence.py``'s promotion-manifest writer, which this was
    lifted from, for the same hardening applied to that path.
    """
    if len(payload) > _MAX_EVIDENCE_OUTPUT_BYTES:
        raise EvidenceGateError(
            f"{path}: refusing to write {len(payload)} bytes -- exceeds "
            f"{_MAX_EVIDENCE_OUTPUT_BYTES}-byte cap"
        )
    try:
        fd = os.open(
            path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW | os.O_NONBLOCK, 0o644
        )
    except OSError as exc:
        # ELOOP/EMLINK deliberately propagates as the raw OSError it always
        # has here -- callers already pin that -- and only the newly-covered
        # non-regular-file cases become an EvidenceGateError.
        if exc.errno == errno.ENXIO:
            # Write-opening a reader-less FIFO with O_NONBLOCK fails here
            # rather than blocking; same verdict as the fstat check below.
            raise EvidenceGateError(f"{path}: refusing to write -- not a regular file") from exc
        raise
    try:
        require_regular_fd(fd, path, "write")
        view = memoryview(payload)
        written = 0
        while written < len(view):
            written += os.write(fd, view[written:])
        os.fsync(fd)
    finally:
        os.close(fd)
