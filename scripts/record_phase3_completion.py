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
from pathlib import Path

from scripts.evidence_common import (
    REPO_ROOT,
    EvidenceGateError,
    confine_output_arg,
    now_utc,
    require_nonempty_str,
    write_bytes_no_follow,
)


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
        "generated_at_utc": now_utc(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-tree-hash", required=True)
    parser.add_argument("--command-digest", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    import json

    try:
        # confine_output_arg (scripts/evidence_common.py): an operator-supplied
        # --output is still attacker-influenced surface (a credentialed run
        # could be invoked with a scripted or copy-pasted value), so it is
        # confined to the repo tree -- and rejected as EvidenceGateError, not a
        # bare ValueError, so this one call folds into the same FAIL/exit-1
        # gate-error handling as everything else below rather than needing its
        # own earlier try/except block.
        args.output = confine_output_arg(args.output, allowed_root=REPO_ROOT)
        record = build_record(
            source_commit=args.source_commit,
            source_tree_hash=args.source_tree_hash,
            command_digest=args.command_digest,
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        # write_bytes_no_follow (scripts/evidence_common.py): this predictable,
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
