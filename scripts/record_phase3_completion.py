#!/usr/bin/env python3
"""Record the exact Phase 3 command digest, source commit, and tree hash.

Run only after Phase 3's own Test command (see the dev plan's closing Phase
3 bullet) has passed against a clean checkout. The resulting artifact is a
required input to ``scripts/validate_v013_evidence.py --write-manifest
--manifest-phase final``: a final promotion manifest cannot be produced
without a matching Phase 3 completion record.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

from scripts._evidence_common import EvidenceGateError, require_nonempty_str, write_bytes_no_follow
from scripts.eval_common import REPO_ROOT, confined_output_path


def build_record(
    *, source_commit: str, source_tree_hash: str, command_digest: str
) -> dict[str, str]:
    require_nonempty_str(source_commit, "source_commit")
    require_nonempty_str(source_tree_hash, "source_tree_hash")
    require_nonempty_str(command_digest, "command_digest")
    return {
        "source_commit": source_commit,
        "source_tree_hash": source_tree_hash,
        "command_digest": command_digest,
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-tree-hash", required=True)
    parser.add_argument("--command-digest", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    import json

    # Sibling eval scripts (eval_model_comparison.py, verify_eval_candidates.py)
    # confine every operator-supplied --out/--output to the repo tree before
    # writing; this evidence writer previously skipped that, so --output
    # could point at an arbitrary destination such as .github/workflows/ci.yml
    # despite write_bytes_no_follow already blocking symlink/FIFO redirection
    # at the resolved path.
    try:
        confined_output_path(args.output, allowed_root=REPO_ROOT)
    except ValueError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    try:
        record = build_record(
            source_commit=args.source_commit,
            source_tree_hash=args.source_tree_hash,
            command_digest=args.command_digest,
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        # write_bytes_no_follow (scripts/_evidence_common.py): this predictable,
        # repo-relative output path gets the same symlink/FIFO hardening as the
        # promotion-manifest writer, instead of a plain write_text that would
        # follow a planted symlink. Kept inside this try block, not after it,
        # so a symlinked --output fails closed with the same FAIL/exit-1
        # contract as every other gate error here, instead of an uncaught
        # traceback.
        write_bytes_no_follow(
            args.output, (json.dumps(record, indent=2, sort_keys=True) + "\n").encode("utf-8")
        )
    except (EvidenceGateError, OSError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    print(f"OK: wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
