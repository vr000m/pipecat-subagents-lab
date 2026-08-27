"""Regression tests for scripts/evidence_common.py's I/O hardening.

Round-1 review-gauntlet findings fixed here:
  - write_bytes_no_follow never enforced its own documented
    _MAX_EVIDENCE_OUTPUT_BYTES bound.
  - write_bytes_no_follow issued a single os.write with no short-write loop,
    so a partial write silently truncated an evidence artifact.
  - load_json/load_jsonl/sha256_file read through plain path.open()/
    read_bytes(), lacking the O_NOFOLLOW/regular-file/TOCTOU guard the
    module's own writers enforce.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from scripts.evidence_common import (
    _MAX_EVIDENCE_OUTPUT_BYTES,
    EvidenceGateError,
    confined_evidence_input_path,
    load_json,
    load_jsonl,
    read_bytes_no_follow,
    sha256_file,
    write_bytes_no_follow,
)


class TestConfinedEvidenceInputPathRejections:
    """Direct rejection tests for :func:`confined_evidence_input_path`'s
    traversal guard, restored after the retirement of the manifest writer
    and the runtime loader it used to be parity-checked against
    (``server.config._resolve_confined_evidence_path``, deleted along with
    ``load_promotion_manifest`` -- see
    docs/dev_plans/20260824-feature-query-context-promotion.md). The sole
    remaining caller is ``scripts.validate_v013_evidence.verify_manifest``,
    reading a manifest-declared, attacker-steerable path -- these tests pin
    the guard directly rather than via a second implementation to compare
    against."""

    def test_rejects_an_absolute_in_repo_path(self, tmp_path: Path) -> None:
        (tmp_path / "docs").mkdir()
        in_repo_absolute = tmp_path / "docs" / "evidence.jsonl"
        in_repo_absolute.write_text("{}\n")

        assert confined_evidence_input_path(str(in_repo_absolute), allowed_root=tmp_path) is None

    def test_rejects_a_relative_traversal_escape(self, tmp_path: Path) -> None:
        outside = tmp_path.parent / "outside-secret.txt"
        outside.write_text("do-not-read-me\n")

        assert confined_evidence_input_path(f"../{outside.name}", allowed_root=tmp_path) is None

    def test_rejects_an_empty_string_path(self, tmp_path: Path) -> None:
        # An empty path would otherwise resolve to `allowed_root` itself (a
        # directory, not a file), so it must never be returned as a usable
        # read target.
        assert confined_evidence_input_path("", allowed_root=tmp_path) is None


def test_write_bytes_no_follow_rejects_payload_over_the_documented_cap(tmp_path: Path) -> None:
    target = tmp_path / "oversized.json"
    oversized = b"x" * (_MAX_EVIDENCE_OUTPUT_BYTES + 1)
    with pytest.raises(EvidenceGateError, match="exceeds"):
        write_bytes_no_follow(target, oversized)
    assert not target.exists()


def test_write_bytes_no_follow_loops_past_a_short_write(tmp_path: Path) -> None:
    target = tmp_path / "artifact.json"
    payload = b"0123456789" * 10  # 100 bytes
    real_os_write = os.write
    calls: list[int] = []

    def short_write(fd: int, data: Any) -> int:
        # Simulate the kernel accepting only part of the buffer per call.
        calls.append(len(data))
        chunk = bytes(data)[:7]
        return real_os_write(fd, chunk)

    with patch("scripts.evidence_common.os.write", side_effect=short_write):
        write_bytes_no_follow(target, payload)

    assert len(calls) > 1  # the short-write loop actually looped
    assert target.read_bytes() == payload


def test_read_bytes_no_follow_rejects_symlink(tmp_path: Path) -> None:
    real = tmp_path / "real.json"
    real.write_bytes(b'{"a": 1}')
    link = tmp_path / "link.json"
    link.symlink_to(real)

    with pytest.raises(EvidenceGateError):
        read_bytes_no_follow(link)


def test_read_bytes_no_follow_rejects_fifo(tmp_path: Path) -> None:
    fifo = tmp_path / "evidence.fifo"
    os.mkfifo(fifo)
    try:
        with pytest.raises(EvidenceGateError, match="not a regular file"):
            read_bytes_no_follow(fifo)
    finally:
        fifo.unlink()


def test_read_bytes_no_follow_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(EvidenceGateError, match="missing evidence input"):
        read_bytes_no_follow(tmp_path / "does-not-exist.json")


def test_read_bytes_no_follow_enforces_max_bytes(tmp_path: Path) -> None:
    target = tmp_path / "big.json"
    target.write_bytes(b"x" * 100)
    with pytest.raises(EvidenceGateError, match="exceeds"):
        read_bytes_no_follow(target, max_bytes=10)


def test_load_json_routes_through_read_bytes_no_follow_and_rejects_symlink(
    tmp_path: Path,
) -> None:
    real = tmp_path / "real.json"
    real.write_text('{"a": 1}')
    link = tmp_path / "link.json"
    link.symlink_to(real)

    with pytest.raises(EvidenceGateError):
        load_json(link)


def test_load_json_still_parses_a_regular_file(tmp_path: Path) -> None:
    target = tmp_path / "doc.json"
    target.write_text('{"a": 1}')
    assert load_json(target) == {"a": 1}


def test_load_jsonl_routes_through_read_bytes_no_follow_and_rejects_symlink(
    tmp_path: Path,
) -> None:
    real = tmp_path / "real.jsonl"
    real.write_text('{"a": 1}\n')
    link = tmp_path / "link.jsonl"
    link.symlink_to(real)

    with pytest.raises(EvidenceGateError):
        load_jsonl(link)


def test_load_jsonl_still_parses_regular_records(tmp_path: Path) -> None:
    target = tmp_path / "records.jsonl"
    target.write_text('{"a": 1}\n{"b": 2}\n')
    assert load_jsonl(target) == [{"a": 1}, {"b": 2}]


def test_sha256_file_routes_through_read_bytes_no_follow_and_rejects_symlink(
    tmp_path: Path,
) -> None:
    real = tmp_path / "real.bin"
    real.write_bytes(b"hello")
    link = tmp_path / "link.bin"
    link.symlink_to(real)

    with pytest.raises(EvidenceGateError):
        sha256_file(link)


def test_sha256_file_still_hashes_a_regular_file(tmp_path: Path) -> None:
    import hashlib

    target = tmp_path / "real.bin"
    target.write_bytes(b"hello")
    assert sha256_file(target) == hashlib.sha256(b"hello").hexdigest()


# ---------------------------------------------------------------------------
# Round-2 confirm-pass findings: regressions introduced by the round-1 fixes.
# ---------------------------------------------------------------------------


# Only the separators JSON permits *unescaped* inside a string are asserted
# here: \v, \f and \x1c-\x1e are control characters below U+0020, which
# JSON forbids raw regardless of how the file is split.
@pytest.mark.parametrize("separator", ["\u2028", "\u2029", "\x85"])
def test_load_jsonl_treats_unicode_line_separators_as_record_content(
    tmp_path: Path, separator: str
) -> None:
    """Round-2 confirm pass: the round-1 rewrite split on ``str.splitlines()``,
    which also breaks on \\v, \\f, \\x1c-\\x1e, \\x85, U+2028 and U+2029 --
    all legal *unescaped* inside a JSON string. A single physical line
    carrying one either failed to parse (a file that read fine before) or
    split into two separately-accepted records, disagreeing with every other
    line-oriented consumer of the same bytes.
    """
    target = tmp_path / "records.jsonl"
    target.write_text(f'{{"text": "before{separator}after"}}\n', encoding="utf-8")

    records = load_jsonl(target)

    assert records == [{"text": f"before{separator}after"}], (
        "a Unicode line separator inside a JSON string must stay one record"
    )


def test_load_jsonl_line_numbers_are_not_shifted_by_a_unicode_separator(tmp_path: Path) -> None:
    """The same bug shifted every reported line number after the offending
    record, so a caller's per-line diagnostics pointed at the wrong line."""
    target = tmp_path / "records.jsonl"
    target.write_text('{"text": "a b"}\nnot json\n', encoding="utf-8")

    with pytest.raises(EvidenceGateError, match=r"line 2: invalid JSON"):
        load_jsonl(target)


def test_load_json_wraps_a_mid_read_oserror(tmp_path: Path) -> None:
    """``load_json``'s docstring promises *every* read failure collapses to
    one ``EvidenceGateError``; only ``os.open`` was guarded, so an ``OSError``
    raised by ``os.read`` (EIO, EBADF, an interrupted read) escaped raw."""
    target = tmp_path / "record.json"
    target.write_text('{"a": 1}')

    with patch("scripts.evidence_common.os.read", side_effect=OSError(5, "Input/output error")):
        with pytest.raises(EvidenceGateError, match="unreadable evidence input"):
            load_json(target)


def test_read_and_write_caps_are_separately_named_constants() -> None:
    """Round-2 confirm pass: ``read_bytes_no_follow`` defaulted its input cap
    to ``_MAX_EVIDENCE_OUTPUT_BYTES``, silently welding the artifact-write
    bound to the evidence-read bound. ``server/config.py`` keeps the two
    apart under exactly these names."""
    import inspect as _inspect

    from scripts.evidence_common import _MAX_EVIDENCE_INPUT_BYTES

    default = _inspect.signature(read_bytes_no_follow).parameters["max_bytes"].default
    assert default == _MAX_EVIDENCE_INPUT_BYTES
    assert "_MAX_EVIDENCE_INPUT_BYTES" in _inspect.getsource(read_bytes_no_follow)


def test_sha256_file_streams_instead_of_buffering_the_whole_artifact(tmp_path: Path) -> None:
    """Round-2 confirm pass: routing ``sha256_file`` through
    ``read_bytes_no_follow`` made it buffer the entire artifact in memory
    just to digest it. It now streams the same hardened fd in chunks."""
    import hashlib

    payload = b"z" * (3 * (1 << 20) + 7)
    target = tmp_path / "big.bin"
    target.write_bytes(payload)

    with patch(
        "scripts.evidence_common.read_bytes_no_follow",
        side_effect=AssertionError("sha256_file must not buffer the whole file"),
    ):
        digest = sha256_file(target)

    assert digest == hashlib.sha256(payload).hexdigest()
