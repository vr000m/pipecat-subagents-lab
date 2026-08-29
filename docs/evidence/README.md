# Release evidence

Home for release-gate evidence from v0.1.4 onward, created to resolve the
`docs/benchmarks/` misnomer (v0.1.3 follow-up program, provenance row 21).

## The split

- **`docs/benchmarks/`** — frozen. Eight of its nine files are v0.1.3
  release-gate evidence, not benchmarks (the exception is
  `20260724-speech-latency.md`, a genuine benchmark). They are load-bearing
  for the frozen v0.1.3 release record: the committed promotion manifest
  embeds their paths, and three read-only validators still bind against them
  (CI's `promotion-manifest-drift` job, the `justfile` `verify-manifest`
  recipe, and `scripts/check_release_metadata.py`'s fixed historical-record
  path check). Re-stamping is impossible from any later HEAD (the manifest
  requires `--source-commit` = `740b364`). **Never delete, move, or rename
  those files.**
- **`docs/evidence/`** (this directory) — any evidence a future release needs
  lands here, under a `v<version>-` filename prefix.

## Why this directory is empty at v0.1.4

The promotion-evidence machinery that populated `docs/benchmarks/` was
retired with the query-context experiment (P1, `c67da7f`): no current release
gate produces evidence artifacts. Row 21's original instruction to update
`check_release_metadata.py`'s path pattern is therefore superseded — the
script's only benchmarks binding is the frozen v0.1.3 historical pin, which
stays by design. If a future release reintroduces evidence-producing gates,
their outputs belong here, not in `docs/benchmarks/`.
