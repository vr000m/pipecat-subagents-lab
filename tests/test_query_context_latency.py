"""Phase 4A/4B tests for the query-context narrowing experiment.

Plan: docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md,
"### Phase 4: Query-context narrowing experiment (P2, conditional)".

By the time this test-writer subagent read the repo, the concurrent Phase 4
implementer had already landed all three scripts plus the versioned fixture
at ``tests/fixtures/query-context-quality-v1.json``, so this module tests
the actual delivered API rather than a speculative one:

- ``scripts/run_query_context_experiment.py``: ``main(argv)`` with a
  required mutually-exclusive ``--dry-run``/``--live`` mode, ``--fixture``,
  ``--output`` (single JSON artifact, not JSONL), optional ``--seed``
  (default 0) and ``--baseline-repeats`` (default 10). It hardcodes exactly
  one condition (``selected_dimension="history_count"``, baseline value 4
  matching production ``history[-4:]``, narrowed candidate value 2) -- there
  is no CLI flag to select a different dimension, so "exactly one selected
  dimension" is a structural invariant of the output, not something a test
  can defeat via conflicting flags.
- ``scripts/collect_query_context_latency.py``: ``main(argv)`` with
  ``--input``/``--output``; also exposes ``load_and_validate_raw``,
  ``normalize``, and the ``RAW_REQUIRED``/``RAW_ALLOWED`` allowlist
  (re-exported from the runner). A blocked/not-run/undersized outcome is
  written as a status line and is *not* a CLI failure (exit 0); only a
  malformed/forbidden-field/out-of-range input is a hard failure (exit 1).
- ``scripts/analyze_query_context_latency.py``: ``main(argv)`` with
  ``--input``/``--output``, plus a pure ``analyze(records)`` function. It
  reuses ``scripts/_evidence_common.py``'s ``EvidenceStatus.BLOCKED``/
  ``NOT_RUN`` values for the shared vocabulary and adds Phase-4-only
  ``"not_promoted"``/``"promoted"`` statuses for the data-driven decision.
  ``analysis`` is only ``None`` for the empty-input/passthrough-status-record
  path; every other blocked/not-promoted outcome carries a non-null
  ``analysis`` dict with a diagnostic reason.

A few tests below assert plan-stated behaviour that the delivered analyzer/
collector do not yet implement (allowlist enforcement inside the analyzer,
and re-validating scorer-derived match IDs against the fixture inside the
collector) -- each such test is clearly labelled as a known gap for the
conductor's fix cycle, not a mistake in this file.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
REAL_FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "query-context-quality-v1.json"


def _evidence_common() -> Any:
    from scripts import _evidence_common

    return _evidence_common


def _runner() -> Any:
    return pytest.importorskip("scripts.run_query_context_experiment")


def _collector() -> Any:
    return pytest.importorskip("scripts.collect_query_context_latency")


def _analyzer() -> Any:
    return pytest.importorskip("scripts.analyze_query_context_latency")


def _fixture_path() -> Path:
    if not REAL_FIXTURE_PATH.exists():
        pytest.skip(f"{REAL_FIXTURE_PATH} not yet authored (Phase 4 concurrent implementer)")
    return REAL_FIXTURE_PATH


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")


def _dry_run_records(
    tmp_path: Path, *, seed: int = 0, baseline_repeats: int = 10
) -> list[dict[str, Any]]:
    module = _runner()
    output = tmp_path / f"dry-run-{seed}-{baseline_repeats}.json"
    exit_code = module.main(
        [
            "--dry-run",
            "--fixture",
            str(_fixture_path()),
            "--output",
            str(output),
            "--seed",
            str(seed),
            "--baseline-repeats",
            str(baseline_repeats),
        ]
    )
    assert exit_code == 0
    payload = json.loads(output.read_text())
    return payload["records"]


def _real_scorer_hash(record: dict[str, Any]) -> str:
    """Compute a collector-acceptable scorer_hash via the runner's own
    provenance-binding function, so synthetic collector-facing fixtures in
    this module don't have to guess at its exact hash formula. The digest
    binds the *whole* record (minus scorer_hash/fixture_sha256), so the
    record must already be fully built before this is called. Falls back to
    a fixed placeholder if the runner (or its current signature) is
    unavailable -- callers that only need collector rejection paths that
    fail *before* the hash check (schema/condition/outcome/range) don't
    depend on this being authentic."""
    try:
        module = _runner()
        return module.scorer_hash(
            record["fixture_version"],
            record["fixture_turn_id"],
            matched_fact_ids=record["matched_fact_ids"],
            matched_citation_ids=record["matched_citation_ids"],
            matched_disallowed_claim_ids=record["matched_disallowed_claim_ids"],
            quality_score=record["quality_score"],
            scorer_version=record["scorer_version"],
            record=record,
        )
    except (TypeError, AttributeError, KeyError):
        return "0" * 64


def _restamped(record: dict[str, Any]) -> dict[str, Any]:
    """Re-stamp a record's scorer_hash after a test mutated one of its fields.

    The digest binds the whole record, so any edit invalidates it. A test that
    wants to exercise a *later* gate (stratum coverage, mixed dimension, mixed
    fixture version) has to re-stamp, or it fails at the hash check instead
    and stops testing what it claims to test.
    """
    stamped = {key: value for key, value in record.items() if key != "scorer_hash"}
    stamped["scorer_hash"] = _real_scorer_hash(stamped)
    return stamped


def _valid_raw_record(**overrides: Any) -> dict[str, Any]:
    explicit_hash = overrides.pop("scorer_hash", None)
    module = _runner()
    payload = {
        "run_id": "run-0001",
        "run_block": 0,
        "run_order": 0,
        "fixture_version": "qcl-test-v1",
        "fixture_turn_id": "turn-1",
        "condition": "baseline",
        "selected_dimension": "history_count",
        "selected_value": 4,
        "context_chars": 512,
        "query_chars": 42,
        "provider": "openai",
        "model": "gpt-test",
        "latency_ms": 850,
        "outcome": "success",
        "quality_score": 1.0,
        "matched_fact_ids": ["fact-1"],
        "matched_citation_ids": ["cite-1"],
        "matched_disallowed_claim_ids": [],
        "scorer_version": module.SCORER_VERSION,
        "attempt_count": 1,
        "retry_count": 0,
        "rate_limit_count": 0,
        "cache_status": "miss",
        "retrieval_snapshot_id": "snap-1",
        "recorded_at_utc": "2026-08-05T00:00:00Z",
    }
    payload.update(overrides)
    # Stamped last, over the finished record: the digest authenticates every
    # measurement and stratum-identity field, so an override of latency_ms /
    # provider / condition must change it.
    payload["scorer_hash"] = explicit_hash or _real_scorer_hash(payload)
    return payload


# --- 4A: runner (bounded experiment, dry-run mode) -------------------------


def test_runner_dry_run_selects_exactly_one_dimension_and_value(tmp_path: Path) -> None:
    records = _dry_run_records(tmp_path)
    assert records
    selected = {(r["selected_dimension"], r["selected_value"]) for r in records}
    assert selected == {("history_count", 4), ("history_count", 2)}


def test_runner_dry_run_does_not_import_or_mutate_web_search_module(tmp_path: Path) -> None:
    """Plan: 'leave production defaults unchanged.' The dry-run path must
    not import server.workers.web_search at all -- it synthesizes responses
    from the fixture rather than touching the production worker."""
    import sys

    before = "server.workers.web_search" in sys.modules
    _dry_run_records(tmp_path)
    after = "server.workers.web_search" in sys.modules
    assert before == after or not after, "dry-run must not newly import the production worker"


def test_runner_dry_run_produces_paired_baseline_and_narrowed_cells(tmp_path: Path) -> None:
    records = _dry_run_records(tmp_path)
    conditions = {r["condition"] for r in records}
    assert conditions == {"baseline", "narrowed"}
    by_condition: dict[str, set[str]] = {}
    for record in records:
        by_condition.setdefault(record["condition"], set()).add(record["fixture_turn_id"])
    assert by_condition["baseline"] == by_condition["narrowed"], (
        "every baseline fixture-turn identity must have a narrowed counterpart"
    )


def test_runner_deterministic_seeded_run_order_reproducible(tmp_path: Path) -> None:
    first = _dry_run_records(tmp_path, seed=7)
    second = _dry_run_records(tmp_path, seed=7)

    def _strip_wallclock(record: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in record.items() if k != "recorded_at_utc"}

    assert [_strip_wallclock(r) for r in first] == [_strip_wallclock(r) for r in second], (
        "seeded run block/order/scoring must be reproducible across identical invocations"
    )


def test_runner_different_seeds_still_select_the_same_dimension_and_value(tmp_path: Path) -> None:
    first = _dry_run_records(tmp_path, seed=1)
    second = _dry_run_records(tmp_path, seed=2)
    for records in (first, second):
        selected = {(r["selected_dimension"], r["selected_value"]) for r in records}
        assert selected == {("history_count", 4), ("history_count", 2)}


def test_runner_rejects_baseline_repeats_below_one(tmp_path: Path) -> None:
    module = _runner()
    output = tmp_path / "invalid.json"
    exit_code = module.main(
        [
            "--dry-run",
            "--fixture",
            str(_fixture_path()),
            "--output",
            str(output),
            "--baseline-repeats",
            "0",
        ]
    )
    assert exit_code != 0
    assert not output.exists() or output.read_text() == ""


def test_runner_requires_exactly_one_of_dry_run_or_live(tmp_path: Path) -> None:
    module = _runner()
    output = tmp_path / "invalid.json"
    with pytest.raises(SystemExit):
        module.main(["--fixture", str(_fixture_path()), "--output", str(output)])


def test_runner_live_mode_without_credentials_is_blocked_and_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _runner()
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    output = tmp_path / "live.jsonl"
    exit_code = module.main(["--live", "--output", str(output)])
    assert exit_code != 0
    assert not output.exists()


def test_runner_output_records_only_the_strict_raw_allowlist(tmp_path: Path) -> None:
    module = _runner()
    records = _dry_run_records(tmp_path)
    for record in records:
        unknown = set(record) - module.RAW_ALLOWED
        assert not unknown, f"record carries non-allowlisted field(s): {unknown}"


def test_runner_provenance_binds_scorer_version_hash_and_fixture_version(tmp_path: Path) -> None:
    real_fixture = json.loads(_fixture_path().read_text())
    records = _dry_run_records(tmp_path)
    for record in records:
        assert record["fixture_version"] == real_fixture["fixture_version"]
        assert record["scorer_version"]
        assert len(record["scorer_hash"]) == 64  # sha256 hex


def test_runner_scorer_hash_changes_when_fixture_version_changes(tmp_path: Path) -> None:
    """Plan: '...binds them to the fixture version and scorer version/hash.'
    Changing the fixture (a different fixture_version) must change the
    binding hash so a stale scorer identity can never silently apply."""
    module = _runner()
    hash_a = module.scorer_hash("v1")
    hash_b = module.scorer_hash("v2")
    assert hash_a != hash_b


@pytest.mark.parametrize(
    "field, forged_value",
    [
        ("latency_ms", 1),
        ("condition", "narrowed"),
        ("selected_value", 2),
        ("provider", "synthetic"),
        ("model", "gpt-4o-search-preview"),
        ("outcome", "timeout"),
        ("cache_status", "hit"),
        ("retrieval_snapshot_id", "snap-forged"),
        ("attempt_count", 3),
        ("retry_count", 2),
        ("rate_limit_count", 1),
        ("run_id", "run-forged"),
        ("context_chars", 4096),
    ],
)
def test_runner_scorer_hash_binds_every_measurement_and_stratum_field(
    field: str, forged_value: Any
) -> None:
    """Regression: scorer_hash bound only the scoring fields, so the
    measurement and stratum-identity fields the promotion decision actually
    rests on were unauthenticated. A dry-run artifact could be relabelled into
    the paid stratum (provider/model), given any chosen latency_ms, and still
    satisfy every ID, quality, fixture-digest and hash check -- the analyzer's
    synthetic guard is only a name denylist over exactly those unbound
    strings. Every one of these fields must now change the digest."""
    record = _valid_raw_record()
    assert record[field] != forged_value, f"{field} fixture value must differ from the forgery"
    forged = {key: value for key, value in record.items() if key != "scorer_hash"}
    forged[field] = forged_value
    assert _real_scorer_hash(forged) != record["scorer_hash"]


@pytest.mark.parametrize(
    "field, forged_value",
    [("latency_ms", 1), ("provider", "synthetic"), ("retrieval_snapshot_id", "snap-forged")],
)
def test_collector_rejects_a_record_edited_after_scoring(
    tmp_path: Path, field: str, forged_value: Any
) -> None:
    """The end-to-end half of the binding: a record whose measurement or
    stratum field was edited after scoring keeps a now-stale scorer_hash, and
    the collector must refuse it rather than normalizing it onward to the
    analyzer."""
    module = _collector()
    record = {**_valid_raw_record(), field: forged_value}
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    assert module.main(_collector_argv(raw_input, output, tmp_path)) != 0


def test_runner_scorer_hash_excludes_the_collector_stamped_fixture_digest() -> None:
    """`fixture_sha256` is stamped by the collector *after* the runner scores
    and hashes a record, and has its own binding path, so including it in the
    digest would make the runner's stamp and every verifier's recomputation
    disagree by construction."""
    record = _valid_raw_record()
    with_digest = {**record, "fixture_sha256": "a" * 64}
    assert _real_scorer_hash(with_digest) == _real_scorer_hash(record)


def test_runner_scorer_rejects_a_zero_denominator_turn() -> None:
    module = _runner()
    turn = {
        "turn_id": "empty-turn",
        "query": "irrelevant",
        "required_facts": [],
        "expected_citations": [],
        "disallowed_claims": [],
    }
    with pytest.raises(module.EvidenceGateError):
        module.score_response(turn, "any response text")


def test_runner_scorer_formula_matches_the_documented_rubric() -> None:
    """Plan: quality_score = (required_facts_present + valid_citations -
    disallowed_claims) / (required_facts + expected_citations), clamped to
    [0, 1]."""
    module = _runner()
    turn = {
        "turn_id": "t1",
        "query": "q",
        "required_facts": [
            {"id": "f1", "match_pattern": "alpha"},
            {"id": "f2", "match_pattern": "beta"},
        ],
        "expected_citations": [{"id": "c1", "domain": "example.com", "fact_id": "f1"}],
        "disallowed_claims": [{"id": "d1", "match_pattern": "gamma"}],
    }
    # Matches both facts and the citation, and the disallowed claim.
    score, facts, cites, disallowed = module.score_response(turn, "alpha beta example.com gamma")
    # (2 facts + 1 citation - 1 disallowed) / (2 + 1) = 2/3
    assert score == pytest.approx(2 / 3)
    assert set(facts) == {"f1", "f2"}
    assert cites == ["c1"]
    assert disallowed == ["d1"]


def test_runner_scorer_clamps_score_to_zero_when_disallowed_claims_exceed_matches() -> None:
    module = _runner()
    turn = {
        "turn_id": "t1",
        "query": "q",
        "required_facts": [{"id": "f1", "match_pattern": "alpha"}],
        "expected_citations": [],
        "disallowed_claims": [
            {"id": "d1", "match_pattern": "gamma"},
            {"id": "d2", "match_pattern": "delta"},
        ],
    }
    score, _, _, _ = module.score_response(turn, "gamma delta")  # no facts matched
    assert score == 0.0  # would be negative unclamped: (0 - 2) / 1


# --- collector fixture binding ---------------------------------------------
#
# The collector resolves every record's match IDs and quality_score against a
# versioned fixture rather than trusting the record's self-reported values, so
# these tests supply a synthetic fixture whose turns/IDs match
# `_valid_raw_record`. One matched fact + one matched citation over a
# two-item denominator scores exactly 1.0.

COLLECTOR_FIXTURE_VERSION = "qcl-test-v1"

# The analyzer's promotion-threshold tests need quality_score values at finer
# granularity (e.g. 0.945, 0.92) than the 2-item fact/citation turns below can
# express (only 0.0/0.5/1.0 are reachable with a denominator of 2). These
# "pquality-*" turns are dedicated to that use (see `_paired_cells`) and use a
# disjoint turn-id namespace so they never collide with the fact-1/cite-1
# turns other tests (and the collector's own tests) rely on.
ANALYZER_QUALITY_FACT_COUNT = 1000
ANALYZER_QUALITY_TURN_COUNT = 100


def _quality_matched_fact_ids(quality: float) -> list[str]:
    matched = round(quality * ANALYZER_QUALITY_FACT_COUNT)
    return [f"qfact-{k}" for k in range(matched)]


def _collector_fixture(tmp_path: Path) -> Path:
    path = tmp_path / "collector-fixture.json"
    if not path.exists():
        fixture = {
            "fixture_version": COLLECTOR_FIXTURE_VERSION,
            "turns": [
                {
                    "turn_id": f"turn-{i}",
                    "query": "q",
                    "prior_queries": [],
                    "required_facts": [{"id": "fact-1", "match_pattern": "alpha"}],
                    "expected_citations": [
                        {"id": "cite-1", "domain": "example.com", "fact_id": "fact-1"}
                    ],
                    "disallowed_claims": [{"id": "claim-1", "match_pattern": "zeta"}],
                }
                for i in range(64)
            ]
            + [
                {
                    "turn_id": f"pquality-{i}",
                    "query": "q",
                    "prior_queries": [],
                    "required_facts": [
                        {"id": f"qfact-{k}", "match_pattern": f"qfact-{k}-pattern"}
                        for k in range(ANALYZER_QUALITY_FACT_COUNT)
                    ],
                    "expected_citations": [],
                    "disallowed_claims": [],
                }
                for i in range(ANALYZER_QUALITY_TURN_COUNT)
            ],
        }
        path.write_text(json.dumps(fixture), encoding="utf-8")
    return path


def _collector_argv(input_path: Path, output: Path, tmp_path: Path) -> list[str]:
    return [
        "--input",
        str(input_path),
        "--output",
        str(output),
        "--fixture",
        str(_collector_fixture(tmp_path)),
    ]


def _analyzer_argv(input_path: Path, output: Path, tmp_path: Path) -> list[str]:
    """The analyzer now resolves match IDs/quality_score against a versioned
    fixture too (matching the collector's forgery gate), so every analyzer
    ``main()`` invocation in this module needs a ``--fixture`` pointing at a
    fixture whose turns/IDs match ``_valid_raw_record``/``_paired_cells``.
    ``_collector_fixture`` already builds exactly that shape (64 turns,
    fact-1/cite-1/claim-1), so it is reused here rather than duplicated."""
    return [
        "--input",
        str(input_path),
        "--output",
        str(output),
        "--fixture",
        str(_collector_fixture(tmp_path)),
    ]


# --- 4A: collector (raw allowlist enforcement) -----------------------------


def test_collector_rejects_raw_records_with_a_status_field(tmp_path: Path) -> None:
    module = _collector()
    record = _valid_raw_record(status="blocked")
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    exit_code = module.main(_collector_argv(raw_input, output, tmp_path))
    assert exit_code != 0


@pytest.mark.parametrize(
    "forbidden_field", ["prompt", "transcript", "context", "response_text", "api_key", "secret"]
)
def test_collector_rejects_raw_records_with_unknown_forbidden_fields(
    tmp_path: Path, forbidden_field: str
) -> None:
    module = _collector()
    record = _valid_raw_record(**{forbidden_field: "should not be here"})
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    exit_code = module.main(_collector_argv(raw_input, output, tmp_path))
    assert exit_code != 0


def test_collector_rejects_an_invalid_condition(tmp_path: Path) -> None:
    module = _collector()
    record = _valid_raw_record(condition="not_a_real_condition")
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    exit_code = module.main(_collector_argv(raw_input, output, tmp_path))
    assert exit_code != 0


def test_collector_rejects_quality_score_out_of_range(tmp_path: Path) -> None:
    module = _collector()
    record = _valid_raw_record(quality_score=1.5)
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    exit_code = module.main(_collector_argv(raw_input, output, tmp_path))
    assert exit_code != 0


def test_collector_rejects_scalar_only_quality_records(tmp_path: Path) -> None:
    """Plan: 'reject scalar-only quality records ... before analysis.' A
    record missing the matched-ID/scorer-provenance fields entirely (not
    just a bare quality_score) must not pass the collector's closed_object
    allowlist check, since those fields are all required."""
    module = _collector()
    record = _valid_raw_record()
    del record["matched_fact_ids"]
    del record["matched_citation_ids"]
    del record["matched_disallowed_claim_ids"]
    del record["scorer_version"]
    del record["scorer_hash"]
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    exit_code = module.main(_collector_argv(raw_input, output, tmp_path))
    assert exit_code != 0


def test_collector_rejects_a_forged_matched_id_added_after_scoring(tmp_path: Path) -> None:
    """Plan: 'reject ... any score whose normalized match IDs/scorer
    identity cannot be validated.' A record is first scored honestly (its
    scorer_hash binds the true matched_fact_ids), then one extra ID is
    appended without recomputing the hash -- exactly what a caller
    forging/editing a match after scoring would produce. The collector must
    recompute the expected hash from the record's own matched-ID fields and
    reject the mismatch."""
    module = _collector()
    record = _valid_raw_record()  # scorer_hash is bound to the original matched_fact_ids
    record["matched_fact_ids"] = [*record["matched_fact_ids"], "fact-forged-not-in-any-fixture"]
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    exit_code = module.main(_collector_argv(raw_input, output, tmp_path))
    assert exit_code != 0


def test_collector_rejects_a_scorer_hash_that_does_not_match_its_own_record(tmp_path: Path) -> None:
    module = _collector()
    record = _valid_raw_record(scorer_hash="f" * 64)
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    exit_code = module.main(_collector_argv(raw_input, output, tmp_path))
    assert exit_code != 0


def test_collector_accepts_a_fully_valid_raw_record_and_writes_a_status_artifact(
    tmp_path: Path,
) -> None:
    module = _collector()
    record = _valid_raw_record()
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    exit_code = module.main(_collector_argv(raw_input, output, tmp_path))
    assert exit_code == 0
    assert output.exists()
    common = _evidence_common()
    lines = [json.loads(line) for line in output.read_text().splitlines() if line.strip()]
    assert lines[0]["status"] == common.EvidenceStatus.BLOCKED.value
    assert lines[0]["reason"] == "undersized_cell"


def test_collector_reports_not_run_when_raw_input_file_is_missing(tmp_path: Path) -> None:
    module = _collector()
    common = _evidence_common()
    missing = tmp_path / "does-not-exist.jsonl"
    output = tmp_path / "normalized.jsonl"
    exit_code = module.main(_collector_argv(missing, output, tmp_path))
    assert exit_code == 0
    line = json.loads(output.read_text().splitlines()[0])
    assert line["status"] == common.EvidenceStatus.NOT_RUN.value
    assert line["reason"] == "no_paid_samples"


def test_collector_passes_through_a_fully_populated_cell(tmp_path: Path) -> None:
    module = _collector()
    records = [_valid_raw_record(run_id=f"run-{i}", fixture_turn_id=f"turn-{i}") for i in range(30)]
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, records)
    output = tmp_path / "normalized.jsonl"
    exit_code = module.main(_collector_argv(raw_input, output, tmp_path))
    assert exit_code == 0
    lines = [json.loads(line) for line in output.read_text().splitlines() if line.strip()]
    assert len(lines) == 30
    assert all("status" not in line for line in lines)


def test_collector_blocked_status_record_carries_source_commit_and_tree_hash(
    tmp_path: Path,
) -> None:
    """A blocked outcome is itself evidence a manifest cites, so it has to
    carry the same source_commit/source_tree_hash identity binding a
    successful run's artifacts do -- otherwise any later run's status line
    could be passed off as this checkout's."""
    module = _collector()
    common = _evidence_common()
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [_valid_raw_record()])  # one record: undersized cell
    output = tmp_path / "normalized.jsonl"
    exit_code = module.main(
        [
            *_collector_argv(raw_input, output, tmp_path),
            "--source-commit",
            "abc123",
            "--source-tree-hash",
            "def456",
        ]
    )
    assert exit_code == 0
    line = json.loads(output.read_text().splitlines()[0])
    assert line["status"] == common.EvidenceStatus.BLOCKED.value
    assert line["source_commit"] == "abc123"
    assert line["source_tree_hash"] == "def456"


# --- 4B: analyzer -----------------------------------------------------------


def _analysis_input(
    tmp_path: Path,
    records: list[dict[str, Any]],
    name: str = "normalized.jsonl",
    *,
    fixture: Path | None = None,
) -> Path:
    """Write analyzer input, stamping each record's ``fixture_sha256``.

    The analyzer requires every record to bind the exact fixture bytes it will
    be analyzed against -- ``fixture_version`` alone is self-declared and binds
    nothing -- so this mirrors what the collector stamps in production.
    ``fixture`` defaults to the same synthetic fixture ``_analyzer_argv``
    passes; a record that already declares a digest keeps it, so
    missing/mismatched-digest rejection tests can opt out.
    """
    common = _evidence_common()
    digest = common.sha256_file(fixture if fixture is not None else _collector_fixture(tmp_path))
    path = tmp_path / name
    _write_jsonl(path, [{"fixture_sha256": digest, **record} for record in records])
    return path


def test_analyzer_reports_not_run_with_no_paid_samples_when_input_is_empty(tmp_path: Path) -> None:
    module = _analyzer()
    common = _evidence_common()
    empty_input = _analysis_input(tmp_path, [])
    output = tmp_path / "analysis.json"
    module.main(["--input", str(empty_input), "--output", str(output)])
    result = json.loads(output.read_text())
    assert result["status"] == common.EvidenceStatus.NOT_RUN.value
    assert result["reason"] == "no_paid_samples"
    assert result["promotion_eligible"] is False
    assert result["analysis"] is None


def test_analyzer_passes_through_a_collector_blocked_status_record(tmp_path: Path) -> None:
    """analyze() special-cases a single collector-produced status record
    (no quality_score field) and passes its status/reason straight through
    rather than trying to run the promotion rubric against it."""
    module = _analyzer()
    common = _evidence_common()
    status_record = {
        "status": common.EvidenceStatus.BLOCKED.value,
        "reason": "undersized_cell",
        "promotion_eligible": False,
        "record_count": 1,
        "generated_at_utc": "2026-08-05T00:00:00Z",
    }
    input_path = _analysis_input(tmp_path, [status_record])
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    assert result["status"] == common.EvidenceStatus.BLOCKED.value
    assert result["reason"] == "undersized_cell"
    assert result["promotion_eligible"] is False
    assert result["analysis"] is None


def test_analyzer_rejects_a_status_record_with_an_out_of_vocabulary_status(tmp_path: Path) -> None:
    """Regression: the single-status-record passthrough copied
    ``status``/``reason`` verbatim from input with no validation against the
    closed ``EvidenceStatus`` vocabulary, so a forged status value would have
    reached a manifest reader unfiltered."""
    module = _analyzer()
    status_record = {
        "status": "not_a_real_status",
        "reason": "made up",
        "promotion_eligible": False,
        "record_count": 1,
        "generated_at_utc": "2026-08-05T00:00:00Z",
    }
    input_path = _analysis_input(tmp_path, [status_record])
    output = tmp_path / "analysis.json"
    exit_code = module.main(_analyzer_argv(input_path, output, tmp_path))
    assert exit_code != 0


def test_analyzer_rejects_a_status_record_with_an_empty_string_reason(tmp_path: Path) -> None:
    module = _analyzer()
    common = _evidence_common()
    status_record = {
        "status": common.EvidenceStatus.BLOCKED.value,
        "reason": "",
        "promotion_eligible": False,
        "record_count": 1,
        "generated_at_utc": "2026-08-05T00:00:00Z",
    }
    input_path = _analysis_input(tmp_path, [status_record])
    output = tmp_path / "analysis.json"
    exit_code = module.main(_analyzer_argv(input_path, output, tmp_path))
    assert exit_code != 0


def test_analyzer_reports_blocked_undersized_cell_below_30_paired_samples(tmp_path: Path) -> None:
    """Plan assumption (c): 'an undersized (partially collected) run
    resolves to a distinct blocked artifact with reason=undersized_cell.'"""
    module = _analyzer()
    common = _evidence_common()
    records = [
        _valid_raw_record(run_id=f"run-{i}", fixture_turn_id=f"turn-{i}", condition="baseline")
        for i in range(5)
    ] + [
        _valid_raw_record(
            run_id=f"run-narrowed-{i}",
            fixture_turn_id=f"turn-{i}",
            condition="narrowed",
            selected_value=2,
        )
        for i in range(5)
    ]
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    assert result["status"] == common.EvidenceStatus.BLOCKED.value
    assert result["reason"] == "undersized_cell"
    assert result["promotion_eligible"] is False
    assert result["analysis"] is not None
    assert result["analysis"]["undersized_strata"]


def test_analyzer_reports_blocked_provider_effect_uncontrolled_when_no_stratum_exposes_a_snapshot(
    tmp_path: Path,
) -> None:
    """Plan assumption (d): a stratum with no discoverable retrieval-
    snapshot control resolves to blocked/provider_effect_uncontrolled."""
    module = _analyzer()
    common = _evidence_common()
    records = [
        _valid_raw_record(
            run_id=f"run-{i}",
            fixture_turn_id=f"turn-{i}",
            condition="baseline",
            retrieval_snapshot_id=None,
        )
        for i in range(30)
    ] + [
        _valid_raw_record(
            run_id=f"run-narrowed-{i}",
            fixture_turn_id=f"turn-{i}",
            condition="narrowed",
            selected_value=2,
            retrieval_snapshot_id=None,
        )
        for i in range(30)
    ]
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    assert result["status"] == common.EvidenceStatus.BLOCKED.value
    assert result["reason"] == "provider_effect_uncontrolled"
    assert result["promotion_eligible"] is False


def test_analyzer_excludes_contaminated_samples_leaving_a_stratum_undersized(
    tmp_path: Path,
) -> None:
    """Plan (d): 'exclude any sample with attempt_count > 1, retry_count >
    0, rate_limit_count > 0, unknown cache state, or missing retrieval
    snapshot.' Contaminated records sharing the narrowed side's identities
    must be excluded from pairing, leaving too few clean pairs."""
    module = _analyzer()
    common = _evidence_common()
    records = (
        [
            _valid_raw_record(
                run_id=f"run-clean-{i}", fixture_turn_id=f"turn-{i}", condition="baseline"
            )
            for i in range(20)
        ]
        + [
            _valid_raw_record(
                run_id=f"run-retry-{i}",
                fixture_turn_id=f"turn-{i + 20}",
                condition="baseline",
                retry_count=1,
            )
            for i in range(15)
        ]
        + [
            _valid_raw_record(
                run_id=f"run-narrowed-{i}",
                fixture_turn_id=f"turn-{i}",
                condition="narrowed",
                selected_value=2,
            )
            for i in range(35)
        ]
    )
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    # Only 20 clean baseline records exist (the 15 retry-contaminated ones
    # are excluded), so even though 35 narrowed records exist, pairing tops
    # out at 20 -- below the 30-sample minimum.
    assert result["status"] == common.EvidenceStatus.BLOCKED.value
    assert result["reason"] == "undersized_cell"


def _paired_cells(
    *,
    n: int,
    baseline_latency: float,
    narrowed_latency: float,
    quality: float = 0.95,
) -> list[dict[str, Any]]:
    records = []
    matched_fact_ids = _quality_matched_fact_ids(quality)
    for i in range(n):
        turn_id = f"pquality-{i}"
        records.append(
            _valid_raw_record(
                run_id=f"run-base-{i}",
                fixture_turn_id=turn_id,
                condition="baseline",
                latency_ms=baseline_latency,
                quality_score=quality,
                context_chars=1000 + i,
                matched_fact_ids=matched_fact_ids,
                matched_citation_ids=[],
            )
        )
        records.append(
            _valid_raw_record(
                run_id=f"run-narrow-{i}",
                fixture_turn_id=turn_id,
                condition="narrowed",
                selected_value=2,
                latency_ms=narrowed_latency,
                quality_score=quality,
                context_chars=200 + i,
                matched_fact_ids=matched_fact_ids,
                matched_citation_ids=[],
            )
        )
    return records


def test_analyzer_promotes_when_thresholds_are_comfortably_exceeded(tmp_path: Path) -> None:
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600)
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    assert result["status"] == "promoted"
    assert result["promotion_eligible"] is True
    assert result["analysis"] is not None


def test_analyzer_does_not_promote_when_improvement_is_below_10_percent(tmp_path: Path) -> None:
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=910)
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    assert result["status"] == "not_promoted"
    assert result["promotion_eligible"] is False
    assert result["reason"] == "data_did_not_support_promotion"


def test_analyzer_promotes_at_exactly_the_10_percent_boundary_inclusive(tmp_path: Path) -> None:
    """Plan: 'promote only when the median latency improves by at least
    10%' -- inclusive boundary. With identical improvement across every
    pair, the bootstrap lower bound equals the median exactly, so an exact
    10% median must promote."""
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=900)
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    assert result["analysis"]["median_relative_latency_improvement"] == pytest.approx(0.10)
    assert result["promotion_eligible"] is True


def test_analyzer_does_not_promote_just_below_the_10_percent_boundary(tmp_path: Path) -> None:
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=901)
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    assert result["analysis"]["median_relative_latency_improvement"] < 0.10
    assert result["promotion_eligible"] is False


def test_analyzer_rejects_baseline_below_quality_floor(tmp_path: Path) -> None:
    """Plan assumption (b): baseline < 0.90 halts with reason
    'baseline_below_quality_floor', not an ordinary not-promoted result."""
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600, quality=0.85)
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    assert result["promotion_eligible"] is False
    assert result["reason"] == "baseline_below_quality_floor"


def test_analyzer_accepts_baseline_exactly_at_quality_floor(tmp_path: Path) -> None:
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600, quality=0.90)
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    assert result["reason"] != "baseline_below_quality_floor"


def test_analyzer_does_not_promote_when_quality_drop_exceeds_0_02(tmp_path: Path) -> None:
    module = _analyzer()
    records = []
    for i in range(30):
        turn_id = f"pquality-{i}"
        records.append(
            _valid_raw_record(
                run_id=f"run-base-{i}",
                fixture_turn_id=turn_id,
                condition="baseline",
                latency_ms=1000,
                quality_score=0.95,
                context_chars=1000 + i,
                matched_fact_ids=_quality_matched_fact_ids(0.95),
                matched_citation_ids=[],
            )
        )
        records.append(
            _valid_raw_record(
                run_id=f"run-narrow-{i}",
                fixture_turn_id=turn_id,
                condition="narrowed",
                selected_value=2,
                latency_ms=600,
                quality_score=0.92,
                context_chars=200 + i,
                matched_fact_ids=_quality_matched_fact_ids(0.92),
                matched_citation_ids=[],
            )
        )
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    assert result["promotion_eligible"] is False
    assert result["reason"] == "quality_drop_exceeded"
    stratum = next(iter(result["analysis"]["strata"].values()))
    assert stratum["quality_drop"] == pytest.approx(0.03)
    assert stratum["quality_ok"] is False


def test_analyzer_promotes_at_exactly_a_0_02_quality_drop_boundary(tmp_path: Path) -> None:
    module = _analyzer()
    records = []
    for i in range(30):
        turn_id = f"pquality-{i}"
        records.append(
            _valid_raw_record(
                run_id=f"run-base-{i}",
                fixture_turn_id=turn_id,
                condition="baseline",
                latency_ms=1000,
                quality_score=0.95,
                context_chars=1000 + i,
                matched_fact_ids=_quality_matched_fact_ids(0.95),
                matched_citation_ids=[],
            )
        )
        records.append(
            _valid_raw_record(
                run_id=f"run-narrow-{i}",
                fixture_turn_id=turn_id,
                condition="narrowed",
                selected_value=2,
                latency_ms=600,
                quality_score=0.93,
                context_chars=200 + i,
                matched_fact_ids=_quality_matched_fact_ids(0.93),
                matched_citation_ids=[],
            )
        )
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    assert result["reason"] != "quality_drop_exceeded"
    assert result["promotion_eligible"] is True


def test_analyzer_halts_baseline_too_noisy_above_sd_0_01(tmp_path: Path) -> None:
    """Plan assumption (a): baseline repeated-run SD > 0.01 halts with
    reason='baseline_too_noisy' rather than applying the promotion rubric
    against unmeasured provider noise.

    The noise gate measures WITHIN-turn repeat variance, not cross-turn
    variance (see the ``baseline_quality_sd`` grouping in
    scripts/analyze_query_context_latency.py) — so each pair of records
    here shares one base turn id (``pquality-<i>#0``/``#1``, two repeats
    of the same underlying fixture turn) rather than being 15 distinct
    single-sample turns, which would carry no within-turn signal at all.
    """
    module = _analyzer()
    records = []
    noisy_scores = [0.99, 0.85] * 15
    for i, score in enumerate(noisy_scores):
        turn_id = f"pquality-{i // 2}#{i % 2}"
        matched_fact_ids = _quality_matched_fact_ids(score)
        records.append(
            _valid_raw_record(
                run_id=f"run-base-{i}",
                fixture_turn_id=turn_id,
                condition="baseline",
                latency_ms=1000,
                quality_score=score,
                context_chars=1000 + i,
                matched_fact_ids=matched_fact_ids,
                matched_citation_ids=[],
            )
        )
        records.append(
            _valid_raw_record(
                run_id=f"run-narrow-{i}",
                fixture_turn_id=turn_id,
                condition="narrowed",
                selected_value=2,
                latency_ms=600,
                quality_score=score,
                context_chars=200 + i,
                matched_fact_ids=matched_fact_ids,
                matched_citation_ids=[],
            )
        )
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    assert result["promotion_eligible"] is False
    assert result["reason"] == "baseline_too_noisy"


def test_analyzer_accepts_baseline_variance_comfortably_below_sd_0_01(tmp_path: Path) -> None:
    """Boundary intent per the plan ('exactly/below/above SD 0.01'): a
    comfortably-below-threshold SD must continue past the noise halt. An
    exactly-0.01 float construction is avoided here since the analyzer's
    ``> BASELINE_NOISE_SD_THRESHOLD`` comparison is float-precision-
    sensitive at the exact boundary; see the strictly-above-threshold test
    for the other side of this boundary."""
    module = _analyzer()
    scores = [0.945, 0.955] * 15  # within-turn pstdev = 0.005, well under the 0.01 threshold
    records = []
    for i, score in enumerate(scores):
        turn_id = f"pquality-{i // 2}#{i % 2}"
        matched_fact_ids = _quality_matched_fact_ids(score)
        records.append(
            _valid_raw_record(
                run_id=f"run-base-{i}",
                fixture_turn_id=turn_id,
                condition="baseline",
                latency_ms=1000,
                quality_score=score,
                context_chars=1000 + i,
                matched_fact_ids=matched_fact_ids,
                matched_citation_ids=[],
            )
        )
        records.append(
            _valid_raw_record(
                run_id=f"run-narrow-{i}",
                fixture_turn_id=turn_id,
                condition="narrowed",
                selected_value=2,
                latency_ms=600,
                quality_score=score,
                context_chars=200 + i,
                matched_fact_ids=matched_fact_ids,
                matched_citation_ids=[],
            )
        )
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    assert result["reason"] != "baseline_too_noisy"


def test_analyzer_raises_on_duplicate_fixture_turn_identity_within_a_condition(
    tmp_path: Path,
) -> None:
    module = _analyzer()
    records = _paired_cells(n=5, baseline_latency=1000, narrowed_latency=600)
    records.append(dict(records[0]))  # exact duplicate baseline identity
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    exit_code = module.main(_analyzer_argv(input_path, output, tmp_path))
    assert exit_code != 0


def test_analyzer_excludes_unpaired_identities_from_the_paired_count(tmp_path: Path) -> None:
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600)
    records.append(
        # "turn-40" is outside `_paired_cells(n=30, ...)`'s turn-0..29 range but
        # still resolves against the shared 64-turn fixture, so it stays
        # unpaired without tripping the fixture's unknown-turn-identity check.
        _valid_raw_record(
            run_id="run-orphan",
            fixture_turn_id="turn-40",
            condition="baseline",
            latency_ms=1000,
        )
    )
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    stratum = next(iter(result["analysis"]["strata"].values()))
    assert stratum["paired_sample_count"] == 30


def test_analyzer_computes_spearman_correlation_between_context_chars_and_latency(
    tmp_path: Path,
) -> None:
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600)
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    stratum = next(iter(result["analysis"]["strata"].values()))
    correlation = stratum["spearman_context_vs_latency"]
    assert correlation is not None
    assert -1.0 <= correlation <= 1.0


def test_spearman_treats_float_round_off_ties_as_equal() -> None:
    """``_ranks`` must compare values within ``EPSILON`` tolerance, not with
    exact float equality, matching the module's own tolerance used elsewhere
    (``baseline_quality_sd``/``quality_drop``). Two latency values equal up
    to float round-off (differing by 1e-10, well under EPSILON=1e-9) must be
    treated as tied and receive the same averaged rank as if they were
    exactly equal -- an exact-equality comparison would instead rank them
    1.0/2.0, silently perturbing the Spearman correlation."""
    module = _analyzer()
    exact_tie = module._spearman([1.0, 2.0], [10.0, 10.0])
    near_tie = module._spearman([1.0, 2.0], [10.0, 10.0 + 1e-10])
    assert near_tie == exact_tie


def test_analyzer_bootstrap_is_exactly_reproducible_with_seed_0(tmp_path: Path) -> None:
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=750)
    input_path = _analysis_input(tmp_path, records)
    first_output = tmp_path / "first.json"
    second_output = tmp_path / "second.json"
    module.main(_analyzer_argv(input_path, first_output, tmp_path))
    module.main(_analyzer_argv(input_path, second_output, tmp_path))
    first = json.loads(first_output.read_text())
    second = json.loads(second_output.read_text())
    first.pop("generated_at_utc", None)
    second.pop("generated_at_utc", None)
    assert first == second, "seed-0 bootstrap must be reproducible across identical invocations"
    assert first["analysis"]["bootstrap_seed"] == 0
    assert first["analysis"]["bootstrap_iterations"] == 10_000


def test_analyzer_rejects_forbidden_raw_fields(tmp_path: Path) -> None:
    """The plan's Testing Notes list 'forbidden raw fields' among the
    analyzer's own rejection matrix: build_analysis() re-runs the strict raw
    allowlist over its input rather than trusting the collector to have done
    it, so a leaked prompt field is rejected here too."""
    module = _analyzer()
    records = _paired_cells(n=5, baseline_latency=1000, narrowed_latency=600)
    records[0]["prompt"] = "leaked prompt text"
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    exit_code = module.main(_analyzer_argv(input_path, output, tmp_path))
    assert exit_code != 0


def test_analyzer_rejects_a_missing_selected_dimension(tmp_path: Path) -> None:
    """'Analyzer tests cover missing dimensions' per the plan's Phase 4B
    testing notes: selected_dimension is a required raw field, so a record
    missing it must not reach the promotion rubric."""
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600)
    del records[0]["selected_dimension"]
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    exit_code = module.main(_analyzer_argv(input_path, output, tmp_path))
    assert exit_code != 0


def test_analyzer_rejects_a_record_missing_provider_with_evidence_gate_error_not_a_crash(
    tmp_path: Path,
) -> None:
    """Regression: the synthetic-dry-run gate used to read
    ``r["provider"]``/``r["model"]`` on every record before
    ``validate_raw_record`` ran, so a malformed record missing either key
    raised a raw KeyError instead of the documented EvidenceGateError.
    ``main()`` only catches ``(EvidenceGateError, OSError)``, so that KeyError
    would have propagated as an unhandled traceback rather than the
    documented ``FAIL: ...`` exit. Shape validation must run before the gate
    reads either field, on every record, regardless of whether that record
    is ultimately synthetic."""
    module = _analyzer()
    records = _paired_cells(n=5, baseline_latency=1000, narrowed_latency=600)
    del records[0]["provider"]
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    exit_code = module.main(_analyzer_argv(input_path, output, tmp_path))
    assert exit_code != 0
    assert not output.exists()


def test_analyzer_rejects_a_record_with_a_scorer_hash_that_does_not_match_the_fixture(
    tmp_path: Path,
) -> None:
    """The analyzer is the promotion gate, and nothing stops a hand-edited
    JSONL from reaching it after the collector ran. A scorer_hash that the
    record's own fixture identity, matched IDs, and quality_score do not
    derive is forged provenance and must be rejected, not analyzed."""
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600)
    records[0]["scorer_hash"] = "a" * 64
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    exit_code = module.main(_analyzer_argv(input_path, output, tmp_path))
    assert exit_code != 0


def test_analyzer_rejects_invented_fixture_ids_even_with_a_correctly_recomputed_scorer_hash(
    tmp_path: Path,
) -> None:
    """Regression: `build_analysis` recomputed `scorer_hash` from the
    record's OWN fields and treated a match as proof of provenance, but a
    keyless self-hash only proves internal consistency, not that the
    matched IDs/facts are real. A reviewer building records with invented
    fixture IDs/facts, a favorable latency spread, and a correctly
    recomputed `scorer_hash` got `promotion_eligible=True` from the analyzer
    while the SAME records were correctly rejected by the collector's
    fixture-binding check. This builds an all-invented-ID record set through
    the same `--fixture` gate the collector uses and asserts the analyzer
    now fails it too."""
    module = _analyzer()
    forged_fact_ids = [f"forged-fact-{i}" for i in range(3)]
    records = []
    for i in range(30):
        turn_id = f"pquality-{i}"
        for run_prefix, condition, latency in (
            ("run-base", "baseline", 1000),
            ("run-narrow", "narrowed", 600),
        ):
            records.append(
                _valid_raw_record(
                    run_id=f"{run_prefix}-{i}",
                    fixture_turn_id=turn_id,
                    condition=condition,
                    selected_value=2 if condition == "narrowed" else 4,
                    latency_ms=latency,
                    quality_score=1.0,
                    context_chars=1000 + i,
                    matched_fact_ids=forged_fact_ids,
                    matched_citation_ids=[],
                )
            )
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    exit_code = module.main(_analyzer_argv(input_path, output, tmp_path))
    assert exit_code != 0
    assert (
        not output.exists() or json.loads(output.read_text()).get("promotion_eligible") is not True
    )


def test_analyzer_blocks_promotion_when_one_stratum_is_undersized_and_another_is_complete(
    tmp_path: Path,
) -> None:
    """A complete stratum next to an undersized one used to promote on the
    complete one alone. Incomplete coverage of the collected strata blocks the
    whole decision."""
    module = _analyzer()
    common = _evidence_common()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600)
    for record in _paired_cells(n=5, baseline_latency=1000, narrowed_latency=600):
        records.append(
            _restamped({**record, "model": "gpt-undersized", "run_id": f"under-{record['run_id']}"})
        )
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    assert module.main(_analyzer_argv(input_path, output, tmp_path)) == 0
    result = json.loads(output.read_text())
    assert result["status"] == common.EvidenceStatus.BLOCKED.value
    assert result["reason"] == "incomplete_stratum_coverage"
    assert result["promotion_eligible"] is False
    assert result["analysis"]["undersized_strata"] == ["openai/gpt-undersized=5"]


def test_analyzer_rejects_mixed_selected_dimension_values(tmp_path: Path) -> None:
    """Two dimensions in one input describe two different experiments; pooling
    them would attribute one dimension's latency change to the other."""
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600)
    records[0] = _restamped({**records[0], "selected_dimension": "answer_chars"})
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    exit_code = module.main(_analyzer_argv(input_path, output, tmp_path))
    assert exit_code != 0


def test_analyzer_rejects_mixed_fixture_versions(tmp_path: Path) -> None:
    module = _analyzer()
    records = _paired_cells(n=5, baseline_latency=1000, narrowed_latency=600)
    records[0] = _restamped({**records[0], "fixture_version": "some-other-version"})
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    exit_code = module.main(_analyzer_argv(input_path, output, tmp_path))
    assert exit_code != 0


def test_analyzer_rejects_mixed_scorer_versions(tmp_path: Path) -> None:
    module = _analyzer()
    records = _paired_cells(n=5, baseline_latency=1000, narrowed_latency=600)
    records[0]["scorer_version"] = "some-other-scorer"
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    exit_code = module.main(_analyzer_argv(input_path, output, tmp_path))
    assert exit_code != 0


def test_analyzer_rejects_a_uniformly_forged_scorer_version_across_every_record(
    tmp_path: Path,
) -> None:
    """Regression: `scorer_hash` used to bind the module constant
    `SCORER_VERSION` regardless of what a record declared, so a
    *uniformly* forged `scorer_version` across an entire batch produced a
    matching digest for every record and defeated the mixed-versions check
    (which only catches divergence *within* a batch). Mutating every record
    to the same forged value must still be rejected."""
    module = _analyzer()
    runner = _runner()
    records = _paired_cells(n=5, baseline_latency=1000, narrowed_latency=600)
    forged_version = "forged-v1"
    assert forged_version != runner.SCORER_VERSION
    for record in records:
        record["scorer_version"] = forged_version
        record["scorer_hash"] = runner.scorer_hash(
            record["fixture_version"],
            record["fixture_turn_id"],
            matched_fact_ids=record["matched_fact_ids"],
            matched_citation_ids=record["matched_citation_ids"],
            matched_disallowed_claim_ids=record["matched_disallowed_claim_ids"],
            quality_score=record["quality_score"],
            scorer_version=forged_version,
        )
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    exit_code = module.main(_analyzer_argv(input_path, output, tmp_path))
    assert exit_code != 0


# --- End-to-end: dry-run -> collector-shaped analyzer input, and the -------
# --- production-defaults invariant that must hold regardless of outcome. --


def test_dry_run_records_feed_directly_into_the_analyzer_as_a_valid_shape(
    tmp_path: Path,
) -> None:
    """The runner's dry-run records use provider="synthetic" and are never
    treated as real evidence. Round 7 made that an explicit terminal gate --
    `blocked`/`synthetic_dry_run_input` -- rather than an incidental
    consequence of `retrieval_snapshot_id=None` routing the stratum into
    `provider_effect_uncontrolled`. Shape is still validated before the gate
    returns, so this remains schema-compatibility coverage: it proves 4A's
    dry-run output is shape-compatible with the analyzer's input contract
    (same field set as a collector-normalized cell) end to end, before any
    paid collection exists."""
    records = _dry_run_records(tmp_path, baseline_repeats=15)
    analyzer = _analyzer()
    common = _evidence_common()
    input_path = _analysis_input(tmp_path, records, fixture=_fixture_path())
    output = tmp_path / "analysis.json"
    # Dry-run records are scored against the real committed fixture (not the
    # synthetic qcl-test-v1 one `_analyzer_argv` uses for `_valid_raw_record`-
    # built records), so this end-to-end test resolves against that same real
    # fixture rather than `_analyzer_argv`'s default.
    exit_code = analyzer.main(
        ["--input", str(input_path), "--output", str(output), "--fixture", str(_fixture_path())]
    )
    assert exit_code == 0
    result = json.loads(output.read_text())
    assert result["status"] == common.EvidenceStatus.BLOCKED.value
    assert result["reason"] == "synthetic_dry_run_input"
    assert result["promotion_eligible"] is False


def test_production_context_window_unchanged_regardless_of_phase4_outcome() -> None:
    """The single most important Phase 4 invariant for this credential-free
    run: whatever the analyzer decides, `_contextual_input` must still use
    the existing history[-4:] window and 1200-character truncation. See
    tests/test_web_search_worker.py for the direct behavioural proof; this
    test only re-confirms the invariant from this module so a reader
    auditing Phase 4 coverage does not have to cross-reference another
    file to find it."""
    from server.workers.web_search import WebSearchWorker

    worker = WebSearchWorker.__new__(WebSearchWorker)
    worker.history = [{"query": f"q{i}", "text": "a" * 2000} for i in range(6)]
    result = worker._contextual_input("current")
    assert result.count("Previous query:") == 4
    for entry_text in result.split("Previous answer: ")[1:]:
        truncated = entry_text.split("\n", 1)[0]
        assert len(truncated) <= 1200


# --- Regression: Phase 4 experiment/collector/analyzer defects --------------


def test_runner_zero_history_condition_actually_empties_the_history() -> None:
    """Regression: `history[-0:]` returns the FULL list, so a schema-valid
    `--value 0` "zero history" narrowed condition silently included every
    prior query."""
    module = _runner()
    turn = {
        "turn_id": "t1",
        "query": "q",
        "prior_queries": ["one", "two", "three", "four"],
        "required_facts": [{"id": "f1", "match_pattern": "alpha"}],
        "expected_citations": [],
        "disallowed_claims": [],
    }
    narrowed_zero = module._context_chars_for(
        turn, "narrowed", dimension="history_count", narrowed_value=0
    )
    baseline = module._context_chars_for(
        turn, "baseline", dimension="history_count", narrowed_value=0
    )
    assert narrowed_zero == 0
    assert baseline > 0


def test_runner_rejects_a_negative_dimension_value(tmp_path: Path) -> None:
    """Regression: only the *count* of --value was validated, so `--value -1`
    reached artifact construction and produced negative selected_value /
    context fields the raw schema forbids."""
    module = _runner()
    output = tmp_path / "dry-run.json"
    exit_code = module.main(
        [
            "--dry-run",
            "--fixture",
            str(_fixture_path()),
            "--output",
            str(output),
            "--value",
            "-1",
        ]
    )
    assert exit_code != 0
    assert not output.exists()


def test_runner_scorer_rejects_a_citation_whose_expected_fact_is_unmatched() -> None:
    """Regression: citation credit was granted for a bare domain mention,
    ignoring the fixture's citation-to-fact mapping -- so a response that
    name-dropped "example.com" without supporting the fact still scored."""
    module = _runner()
    turn = {
        "turn_id": "t1",
        "query": "q",
        "required_facts": [{"id": "f1", "match_pattern": "alpha"}],
        "expected_citations": [{"id": "c1", "domain": "example.com", "fact_id": "f1"}],
        "disallowed_claims": [],
    }
    score, facts, cites, _ = module.score_response(turn, "example.com says something else")
    assert facts == []
    assert cites == [], "a citation without its expected fact is not a valid citation"
    assert score == 0.0


def test_runner_scorer_requires_a_pinned_canonical_url_when_the_fixture_declares_one() -> None:
    module = _runner()
    turn = {
        "turn_id": "t1",
        "query": "q",
        "required_facts": [{"id": "f1", "match_pattern": "alpha"}],
        "expected_citations": [
            {
                "id": "c1",
                "domain": "example.com",
                "url": "https://example.com/facts/alpha",
                "fact_id": "f1",
            }
        ],
        "disallowed_claims": [],
    }
    _, _, without_url, _ = module.score_response(turn, "alpha example.com")
    _, _, with_url, _ = module.score_response(turn, "alpha https://example.com/facts/alpha")
    assert without_url == []
    assert with_url == ["c1"]


def test_runner_randomizes_condition_order_within_blocks_and_records_it(tmp_path: Path) -> None:
    """Regression: every block ran baseline-then-narrowed, so provider/cache/
    time drift loaded entirely onto the narrowed arm. The order must vary and
    must be recorded for reproducibility."""
    module = _runner()
    output = tmp_path / "dry-run.json"
    assert (
        module.main(
            ["--dry-run", "--fixture", str(_fixture_path()), "--output", str(output), "--seed", "1"]
        )
        == 0
    )
    artifact = json.loads(output.read_text())
    orders = [tuple(entry["order"]) for entry in artifact["condition_orders"]]
    assert orders, "the chosen per-block condition order must be recorded"
    assert len(set(orders)) > 1, "condition order must vary across blocks, not be fixed"
    assert all(sorted(order) == ["baseline", "narrowed"] for order in orders)
    # The recorded order must match the order the records were actually
    # emitted in, block by block.
    records = artifact["records"]
    for entry in artifact["condition_orders"]:
        block = [r for r in records if r["run_block"] == entry["run_block"]]
        assert [r["condition"] for r in sorted(block, key=lambda r: r["run_order"])] == entry[
            "order"
        ]


def test_runner_condition_order_is_reproducible_for_a_given_seed(tmp_path: Path) -> None:
    module = _runner()
    first, second = tmp_path / "a.json", tmp_path / "b.json"
    for output in (first, second):
        assert (
            module.main(
                [
                    "--dry-run",
                    "--fixture",
                    str(_fixture_path()),
                    "--output",
                    str(output),
                    "--seed",
                    "7",
                ]
            )
            == 0
        )
    a = json.loads(first.read_text())
    b = json.loads(second.read_text())
    assert a["condition_orders"] == b["condition_orders"]


def test_collector_rejects_a_match_id_the_fixture_does_not_declare(tmp_path: Path) -> None:
    """Regression: the collector recomputed scorer_hash purely from fields the
    record itself supplied, so an editor could invent a match, recompute the
    same hash from the fabricated values, and pass validation."""
    module = _collector()
    record = _valid_raw_record(
        matched_fact_ids=["fact-1", "fact-invented"],
        matched_citation_ids=["cite-1"],
        quality_score=1.0,
    )
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    assert module.main(_collector_argv(raw_input, output, tmp_path)) != 0


def test_collector_rejects_duplicate_match_ids(tmp_path: Path) -> None:
    module = _collector()
    record = _valid_raw_record(
        matched_fact_ids=["fact-1", "fact-1"], matched_citation_ids=["cite-1"], quality_score=1.0
    )
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    assert module.main(_collector_argv(raw_input, output, tmp_path)) != 0


def test_collector_rejects_a_citation_claimed_without_its_expected_fact(tmp_path: Path) -> None:
    module = _collector()
    record = _valid_raw_record(
        matched_fact_ids=[], matched_citation_ids=["cite-1"], quality_score=0.5
    )
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    assert module.main(_collector_argv(raw_input, output, tmp_path)) != 0


def test_collector_rejects_a_quality_score_that_is_not_fixture_derived(tmp_path: Path) -> None:
    """A self-consistent record (its scorer_hash matches its own fields) whose
    quality_score is not what the fixture's denominator implies must still be
    rejected."""
    module = _collector()
    record = _valid_raw_record(quality_score=0.5)
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    assert module.main(_collector_argv(raw_input, output, tmp_path)) != 0


def test_collector_rejects_an_unknown_fixture_turn_identity(tmp_path: Path) -> None:
    module = _collector()
    record = _valid_raw_record(fixture_turn_id="turn-not-in-the-fixture")
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    assert module.main(_collector_argv(raw_input, output, tmp_path)) != 0


@pytest.mark.parametrize(
    "override",
    [
        {"latency_ms": -1},
        {"latency_ms": "fast"},
        {"context_chars": -5},
        {"attempt_count": 0},
        {"retry_count": -1},
        {"cache_status": "warm"},
        {"outcome": "maybe"},
        {"selected_dimension": "not_a_dimension"},
        {"run_block": -1},
        {"matched_fact_ids": [""]},
    ],
)
def test_analyzer_rejects_type_and_range_violations(tmp_path: Path, override: dict) -> None:
    """Regression: the analyzer re-validated only the *key set*, so a
    hand-edited record with wrong numeric types, negative values, or invalid
    enums reached the statistics."""
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600)
    records[0].update(override)
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    assert module.main(_analyzer_argv(input_path, output, tmp_path)) != 0


# --- Review-gauntlet round 7 -------------------------------------------


def test_analyzer_rejects_a_record_that_declares_no_fixture_digest(tmp_path: Path) -> None:
    """Round 7 (security + spec, corroborated independently): the collector
    stamps ``fixture_sha256`` on every record precisely so downstream binding
    can tie the artifact to those exact fixture bytes, but nothing ever
    compared it -- it was only shape-checked as 64 lowercase hex characters.
    A record that binds no fixture bytes cannot support a promotion
    decision."""
    module = _analyzer()
    records = [
        {k: v for k, v in record.items() if k != "fixture_sha256"}
        for record in _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600)
    ]
    input_path = tmp_path / "normalized.jsonl"
    _write_jsonl(input_path, records)
    output = tmp_path / "analysis.json"
    assert module.main(_analyzer_argv(input_path, output, tmp_path)) != 0


def test_analyzer_rejects_records_scored_against_different_fixture_bytes(tmp_path: Path) -> None:
    """Round 7: ``fixture_version`` is a self-declared string, so it binds
    nothing. A ``--fixture`` file carrying the same version but weaker
    required_facts/expected_citations/disallowed_claims let forged match IDs
    and an inflated quality_score resolve cleanly into
    ``promoted``/``promotion_eligible=true``. The digest is now compared
    against ``sha256_file(fixture_path)``."""
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600)
    # A digest of some other file: same shape, different bytes.
    other = tmp_path / "other-fixture.json"
    other.write_text("{}", encoding="utf-8")
    input_path = _analysis_input(tmp_path, records, fixture=other)
    output = tmp_path / "analysis.json"
    assert module.main(_analyzer_argv(input_path, output, tmp_path)) != 0


def test_analyzer_publishes_the_bound_fixture_digest_in_a_promoted_analysis(
    tmp_path: Path,
) -> None:
    """Round 7: the digest the decision was made under is emitted so a
    downstream evidence binder can verify it, rather than trusting
    ``fixture_version``."""
    module = _analyzer()
    common = _evidence_common()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600)
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(_analyzer_argv(input_path, output, tmp_path))
    result = json.loads(output.read_text())
    assert result["status"] == "promoted"
    assert result["analysis"]["fixture_sha256"] == common.sha256_file(_collector_fixture(tmp_path))


def test_analyzer_blocks_a_synthetic_stratum_that_looks_fully_controlled(
    tmp_path: Path,
) -> None:
    """Round 7 (spec): the runner documents that Phase 4B always treats
    dry-run/synthetic records as ineligible, but there was no explicit
    provider/model gate -- dry-run rows were rejected only as a side effect
    of ``retrieval_snapshot_id=None`` routing them into
    ``provider_effect_uncontrolled``. A synthetic row carrying a snapshot id
    and a hit/miss cache status would have reached the promotion rubric."""
    module = _analyzer()
    common = _evidence_common()
    records = [
        {**record, "provider": "synthetic", "model": "dry-run-fixture"}
        for record in _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600)
    ]
    assert all(r["retrieval_snapshot_id"] is not None for r in records)
    assert all(r["cache_status"] in {"hit", "miss"} for r in records)
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    assert module.main(_analyzer_argv(input_path, output, tmp_path)) == 0
    result = json.loads(output.read_text())
    assert result["status"] == common.EvidenceStatus.BLOCKED.value
    assert result["reason"] == "synthetic_dry_run_input"
    assert result["promotion_eligible"] is False
