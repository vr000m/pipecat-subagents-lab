"""Regression tests for scripts/_evidence_common.py's I/O hardening.

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

from scripts._evidence_common import (
    _MAX_EVIDENCE_OUTPUT_BYTES,
    EvidenceGateError,
    load_json,
    load_jsonl,
    read_bytes_no_follow,
    sha256_file,
    write_bytes_no_follow,
)


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

    with patch("scripts._evidence_common.os.write", side_effect=short_write):
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
