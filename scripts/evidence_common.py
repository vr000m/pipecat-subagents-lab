"""Shared status vocabulary and helpers for the v0.1.3 evidence-gate scripts.

``scripts/validate_v013_evidence.py``, ``scripts/validate_phase2_transport_browser_contract.py``,
and ``scripts/emit_v013_deployment_metadata.py`` all need the same closed
"why isn't this promotion-eligible" vocabulary and the same JSON/JSONL
loading primitives. This module is the one place that vocabulary and those
primitives live, so the three scripts cannot silently drift from each other.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import re
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from stat import S_ISREG
from typing import Any

#: The repo tree every evidence artifact is written inside. Lives here rather
#: than in ``scripts/eval_common.py`` (which re-exports it) so the five
#: dependency-light evidence scripts can confine their ``--output`` without
#: importing ``server.pipeline``/pipecat -- see ``confined_output_path``.
REPO_ROOT = Path(__file__).resolve().parents[1]


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


class MissingEvidenceInput(EvidenceGateError):
    """The path does not exist -- the one read failure some callers treat as
    "nothing to do" rather than "fail the gate".

    A subclass of :class:`EvidenceGateError`, deliberately: every existing
    ``except EvidenceGateError`` handler keeps catching it, so this narrows
    the vocabulary without changing any current caller's behaviour. It exists
    so :func:`read_bytes_if_present` can tell "absent" from every other
    failure mode *from the failed open itself*, rather than re-``stat``-ing
    the path afterwards -- which would put a second, TOCTOU-able look at the
    filesystem back into a helper whose whole point is that the type check and
    the read happen against one held fd.
    """


# Gates two halves of the same Phase 4 promotion decision (the collector's
# per-cell undersizing check and the analyzer's per-stratum pairing check);
# defined once here so the two scripts cannot drift apart on the minimum.
MIN_PAIRED_SAMPLES_PER_CELL = 30


def now_utc() -> str:
    """The current UTC instant, formatted ``YYYY-MM-DDTHH:MM:SSZ``.

    Every evidence artifact's ``generated_at_utc`` field is written in this
    exact format -- ``server/config.py``'s boot-path loader parses it with
    ``_parse_utc_timestamp``, so the format is a contract between the writer
    and that parser, not a formatting preference each script was free to pick
    independently. This was previously hand-written at several call sites
    (``validate_v013_evidence.py``, ``emit_v013_deployment_metadata.py``,
    ``record_phase3_completion.py``, and ``eval_model_comparison.py`` spelled
    it inline, plus two more in the retired query-context experiment
    collectors -- see
    docs/dev_plans/20260824-feature-query-context-promotion.md), all
    producing the same string -- consolidated here so a future format
    change happens in one place, not several (round-5 restart, Architecture
    finding).
    """
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_file(path: Path) -> str:
    """Return the hex SHA-256 digest of a file's bytes.

    Streams through ``_iter_file_chunks`` rather than buffering the whole
    artifact: hashing needs the bytes only once, in order, so an evidence
    file near the ``_MAX_EVIDENCE_INPUT_BYTES`` cap is never materialized in
    memory just to be digested. The cap itself still applies -- a file this
    module refuses to *load* must not be one it will happily *vouch for*.
    """
    digest = hashlib.sha256()
    for chunk in _iter_file_chunks(path, max_bytes=_MAX_EVIDENCE_INPUT_BYTES):
        digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
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
    # ``split("\n")``, never ``splitlines()``: the latter additionally splits
    # on \v, \f, \x1c-\x1e, \x85, U+2028 and U+2029, all of which are legal
    # *unescaped* inside a JSON string. A transcript field carrying one would
    # either fail to parse (a file that read fine before) or -- worse -- split
    # one physical line into two accepted records, disagreeing with every
    # other line-oriented consumer of the same file (jq, grep, a reviewer's
    # eyes, and the byte-wise sha256_file digest that cannot see the
    # difference). Splitting on \n alone keeps this parser in step with them.
    for line_no, raw_line in enumerate(text.split("\n"), start=1):
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

# The read-side bound, named and valued separately from the write-side one
# above -- the same split ``server/config.py`` keeps between its own
# ``_MAX_EVIDENCE_INPUT_BYTES`` (config.py) and its write path. Reusing the
# output constant as the read default silently welded the two together, so
# lowering the cap on artifacts these scripts *write* would also have
# lowered the cap on evidence they are willing to *read*.
_MAX_EVIDENCE_INPUT_BYTES = 8 * 1024 * 1024


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


def _iter_file_chunks(path: Path, *, max_bytes: int) -> Iterator[bytes]:
    """Yield `path`'s bytes, refusing to read *through* a symlink or hang on a FIFO.

    The shared read primitive behind :func:`read_bytes_no_follow` and
    :func:`sha256_file` -- chunked so a caller that does not need the whole
    artifact at once (hashing) never buffers it.

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

    ``os.read`` is wrapped as well as ``os.open``: a mid-read ``OSError``
    (EIO on a failing device, EBADF, a signal-interrupted read) would
    otherwise escape raw and break :func:`load_json`'s documented promise
    that *every* read failure mode collapses to one ``EvidenceGateError``.
    ``server/config.py``'s ``read_regular_file_no_follow`` guards the same
    call for the same reason.
    """
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            raise MissingEvidenceInput(f"missing evidence input: {path}") from exc
        if exc.errno == errno.ENXIO:
            # Read-opening the write end of a reader-less FIFO with
            # O_NONBLOCK fails here rather than blocking; same verdict as
            # the fstat check below would give a FIFO opened some other way.
            raise EvidenceGateError(f"{path}: refusing to read -- not a regular file") from exc
        raise EvidenceGateError(f"unreadable evidence input {path}: {exc}") from exc
    try:
        require_regular_fd(fd, path, "read")
        total = 0
        while True:
            try:
                chunk = os.read(fd, 1 << 20)
            except OSError as exc:
                raise EvidenceGateError(f"unreadable evidence input {path}: {exc}") from exc
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise EvidenceGateError(f"{path}: evidence input exceeds {max_bytes}-byte cap")
            yield chunk
    finally:
        os.close(fd)


def read_bytes_no_follow(path: Path, *, max_bytes: int = _MAX_EVIDENCE_INPUT_BYTES) -> bytes:
    """Read `path`'s whole contents through :func:`_iter_file_chunks`'s hardening."""
    return b"".join(_iter_file_chunks(path, max_bytes=max_bytes))


def read_bytes_if_present(
    path: Path, *, max_bytes: int = _MAX_EVIDENCE_INPUT_BYTES
) -> bytes | None:
    """:func:`read_bytes_no_follow`, but ``None`` when `path` simply is not there.

    For the callers whose read is a *conditional* step rather than a gate
    input -- the promotion-manifest writer's ``.previous`` backup, which has
    nothing to back up on a first run. Every *other* failure mode still
    raises: a symlink, a FIFO, a device node, or an oversized file at this
    path all mean something is there that must not be, and treating them as
    "absent" would let the writer clobber it.

    Only ``ENOENT`` at the open produces ``None``, signalled by
    :class:`MissingEvidenceInput` from the failed open itself -- no follow-up
    ``exists()``/``stat()``, which would reopen the TOCTOU window the held-fd
    design closes.
    """
    try:
        return read_bytes_no_follow(path, max_bytes=max_bytes)
    except MissingEvidenceInput:
        return None


def confined_output_path(raw_path: str | Path, *, allowed_root: Path | None = None) -> Path:
    """Resolve a user-supplied ``--output``/``--out`` path, confined to the repo tree.

    An operator-supplied output path is still attacker-influenced surface (a
    credentialed run could be invoked with a scripted or copy-pasted value)
    -- rejects `..` traversal that escapes ``allowed_root`` and refuses to
    resolve onto an existing symlink, so a planted symlink at the target path
    cannot redirect the write to an arbitrary file. Raises ``ValueError`` on
    either violation; callers decide how to surface that to the operator.

    **Callers must write through the returned path, not the raw one.** A
    relative candidate is resolved against ``allowed_root``, but the raw
    argparse ``Path`` an ``os.open`` would see resolves against the process
    cwd instead -- so dropping the return value validates one path and writes
    a different one, which is no confinement at all.

    Defined in this module, not in ``scripts/eval_common.py`` (which
    re-exports it), so the dependency-light evidence scripts can confine
    their output without dragging ``server.pipeline`` and the whole pipecat
    runtime into a gate that must stay importable on its own.
    """
    root = (allowed_root or REPO_ROOT).resolve()
    candidate = Path(raw_path)
    resolved = candidate if candidate.is_absolute() else (root / candidate)
    if resolved.is_symlink():
        raise ValueError(f"refusing to write through an existing symlink: {resolved}")
    resolved = resolved.resolve()
    if not resolved.is_relative_to(root):
        raise ValueError(
            f"output path must stay within {root}: {raw_path!r} resolved to {resolved}"
        )
    # Case-insensitive comparison, not exact-case ``in`` membership: on a
    # case-insensitive filesystem (macOS APFS default, Windows) ``.GIT``/
    # ``.Git``/etc. name the exact same directory as ``.git`` but bypassed an
    # exact-case check, letting a path like ``--out .GIT/hooks/pre-commit``
    # plant a hook despite the check below intending to block exactly that
    # (round 9 gauntlet, Security lens finding 10). ``.github`` is denylisted
    # alongside ``.git`` for the same reason (round 9 gauntlet, Security lens
    # finding 11): a write under ``.github/workflows/*.yml`` is the same
    # class of risk this check already exists to close -- code execution
    # triggered on push to a CI-connected repo -- not the git-hook mechanism,
    # but an equivalent one.
    denylisted_dirs = {".git", ".github"}
    if any(part.lower() in denylisted_dirs for part in resolved.parts):
        raise ValueError(f"output path must not write under .git/ or .github/: {raw_path!r}")
    return resolved


def confine_output_arg(raw_output: str | Path, *, allowed_root: Path) -> Path:
    """Confine an operator-supplied ``--output``/``--out`` CLI argument to the repo tree.

    A thin wrapper around :func:`confined_output_path` that raises
    :class:`EvidenceGateError` instead of ``ValueError``, so a caller can fold
    the confinement check into the same broad ``except (EvidenceGateError,
    OSError)`` block it already wraps the rest of the gate in, rather than
    needing a second, earlier ``try/except ValueError`` block just for this
    one call.

    ``allowed_root`` is a required keyword, not a default of this module's own
    ``REPO_ROOT``: every calling script keeps its own module-level
    ``REPO_ROOT`` import specifically so its tests can ``monkeypatch.setattr``
    that name to a ``tmp_path`` and exercise the real confinement logic
    without writing into the actual repo tree (see
    ``tests/test_v013_evidence_validator.py``'s
    ``_confine_manifest_evidence_root_to_tmp_path`` fixture and its siblings
    across the other evidence-gate test modules). Defaulting this helper to
    ``evidence_common.REPO_ROOT`` would silently stop honouring that
    per-module patch and point every caller back at the real repo root.

    Several evidence-gate scripts (``record_phase3_completion.py`` and
    ``validate_v013_evidence.py`` among them; the retired query-context
    experiment collectors were two more before their removal -- see
    docs/dev_plans/20260824-feature-query-context-promotion.md) used to
    copy-paste the same prose comment plus ``try/except ValueError: print
    FAIL; return 1`` block, and the invariant that comment restated in each
    place -- **the confined return value MUST be bound back onto
    ``args.output``, or a caller validates one path and writes a different
    one** -- is exactly the kind of thing a signature enforces better than a
    comment: returning the confined path here means the caller has nothing
    else to remember (round-5 restart, Architecture finding).
    """
    try:
        return confined_output_path(raw_output, allowed_root=allowed_root)
    except ValueError as exc:
        raise EvidenceGateError(str(exc)) from exc


def confined_evidence_input_path(raw_path: str, *, allowed_root: Path | None = None) -> Path | None:
    """Resolve a *manifest-declared* evidence input path, confined to the repo tree.

    The read-side counterpart to :func:`confined_output_path`. (It used to be
    the CLI-side mirror of ``server.config._resolve_confined_evidence_path``,
    which was deleted along with the runtime loader it served -- see the
    retirement note below.) ``raw_path``
    comes from ``manifest["inputs"][phase]["path"]`` -- a value the artifact
    under scrutiny declares about *itself*, not an operator setting -- so it is
    attacker-steerable and must never be used as a read target unconfined.
    Returns ``None`` when the path is absolute or escapes ``allowed_root``
    after resolution.

    **The absolute-path rejection is load-bearing, not belt-and-braces.**
    ``Path(root) / "/etc/passwd"`` discards ``root`` entirely in pathlib, so a
    confinement written as "join, then ``is_relative_to``" accepts every
    absolute path that happens to sit inside the tree. This used to matter
    because the runtime loader (``server.config.load_promotion_manifest``,
    retired along with the rest of the query-context promotion chain -- see
    docs/dev_plans/20260824-feature-query-context-promotion.md) rejected
    those outright: a CI gate that accepted a manifest the consumer it spoke
    for would refuse was worse than no gate, reporting clean while the server
    degraded to ``evidence_unresolvable``/display-only with nothing surfaced
    anywhere (round-4 confirm pass, Architecture + Security findings). The
    rejection is retained directly now that the runtime consumer is gone --
    :func:`verify_manifest` is the sole remaining caller, and it must not
    read outside the repo tree on an attacker-steerable manifest-declared
    path. ``tests/test_evidence_common.py`` pins the rejection cases
    directly.

    Deliberately does *not* also check "is a regular file within the size cap":
    doing that with a ``stat()`` here and reading the path separately at the
    caller is exactly the TOCTOU gap :func:`read_bytes_no_follow` /
    :func:`sha256_file` exist to close, so callers must route the read through
    one of those, which re-derive both facts from the fd they open.
    """
    if not raw_path:
        # `Path("") / candidate` resolves to `root` itself -- a directory,
        # not a file -- which `is_relative_to(root)` below would accept as
        # "confined" even though it can never name a real evidence artifact.
        # Reject explicitly rather than handing callers a directory that
        # only fails later, and less directly, inside `sha256_file`.
        return None
    root = (allowed_root or REPO_ROOT).resolve()
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return None
    resolved = (root / candidate).resolve()
    if not resolved.is_relative_to(root):
        return None
    return resolved


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
