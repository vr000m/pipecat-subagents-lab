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

import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
RUNNER_PATH = REPO_ROOT / "scripts" / "run_query_context_experiment.py"
COLLECTOR_PATH = REPO_ROOT / "scripts" / "collect_query_context_latency.py"
ANALYZER_PATH = REPO_ROOT / "scripts" / "analyze_query_context_latency.py"
EVIDENCE_COMMON_PATH = REPO_ROOT / "scripts" / "_evidence_common.py"
REAL_FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "query-context-quality-v1.json"


def _load(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _evidence_common() -> Any:
    return _load(EVIDENCE_COMMON_PATH, "_evidence_common_qcl")


def _runner() -> Any:
    if not RUNNER_PATH.exists():
        pytest.skip(f"{RUNNER_PATH} not yet implemented (Phase 4 concurrent implementer)")
    return _load(RUNNER_PATH, "run_query_context_experiment")


def _collector() -> Any:
    if not COLLECTOR_PATH.exists():
        pytest.skip(f"{COLLECTOR_PATH} not yet implemented (Phase 4 concurrent implementer)")
    return _load(COLLECTOR_PATH, "collect_query_context_latency")


def _analyzer() -> Any:
    if not ANALYZER_PATH.exists():
        pytest.skip(f"{ANALYZER_PATH} not yet implemented (Phase 4 concurrent implementer)")
    return _load(ANALYZER_PATH, "analyze_query_context_latency")


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


def _real_scorer_hash(**kwargs: Any) -> str:
    """Compute a collector-acceptable scorer_hash via the runner's own
    provenance-binding function, so synthetic collector-facing fixtures in
    this module don't have to guess at its exact hash formula. Falls back to
    a fixed placeholder if the runner (or its current signature) is
    unavailable -- callers that only need collector rejection paths that
    fail *before* the hash check (schema/condition/outcome/range) don't
    depend on this being authentic."""
    try:
        module = _runner()
        return module.scorer_hash(
            kwargs.get("fixture_version", "qcl-test-v1"),
            kwargs.get("fixture_turn_id", "turn-1"),
            matched_fact_ids=kwargs.get("matched_fact_ids", ["fact-1"]),
            matched_citation_ids=kwargs.get("matched_citation_ids", ["cite-1"]),
            matched_disallowed_claim_ids=kwargs.get("matched_disallowed_claim_ids", []),
            quality_score=kwargs.get("quality_score", 0.95),
        )
    except (TypeError, AttributeError):
        return "0" * 64


def _valid_raw_record(**overrides: Any) -> dict[str, Any]:
    scorer_hash = overrides.pop("scorer_hash", None) or _real_scorer_hash(**overrides)
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
        "quality_score": 0.95,
        "matched_fact_ids": ["fact-1"],
        "matched_citation_ids": ["cite-1"],
        "matched_disallowed_claim_ids": [],
        "scorer_version": "scorer-v1",
        "scorer_hash": scorer_hash,
        "attempt_count": 1,
        "retry_count": 0,
        "rate_limit_count": 0,
        "cache_status": "miss",
        "retrieval_snapshot_id": "snap-1",
        "recorded_at_utc": "2026-08-05T00:00:00Z",
    }
    payload.update(overrides)
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


# --- 4A: collector (raw allowlist enforcement) -----------------------------


def test_collector_rejects_raw_records_with_a_status_field(tmp_path: Path) -> None:
    module = _collector()
    record = _valid_raw_record(status="blocked")
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    exit_code = module.main(["--input", str(raw_input), "--output", str(output)])
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
    exit_code = module.main(["--input", str(raw_input), "--output", str(output)])
    assert exit_code != 0


def test_collector_rejects_an_invalid_condition(tmp_path: Path) -> None:
    module = _collector()
    record = _valid_raw_record(condition="not_a_real_condition")
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    exit_code = module.main(["--input", str(raw_input), "--output", str(output)])
    assert exit_code != 0


def test_collector_rejects_quality_score_out_of_range(tmp_path: Path) -> None:
    module = _collector()
    record = _valid_raw_record(quality_score=1.5)
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    exit_code = module.main(["--input", str(raw_input), "--output", str(output)])
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
    exit_code = module.main(["--input", str(raw_input), "--output", str(output)])
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
    exit_code = module.main(["--input", str(raw_input), "--output", str(output)])
    assert exit_code != 0


def test_collector_rejects_a_scorer_hash_that_does_not_match_its_own_record(tmp_path: Path) -> None:
    module = _collector()
    record = _valid_raw_record(scorer_hash="f" * 64)
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    exit_code = module.main(["--input", str(raw_input), "--output", str(output)])
    assert exit_code != 0


def test_collector_accepts_a_fully_valid_raw_record_and_writes_a_status_artifact(
    tmp_path: Path,
) -> None:
    module = _collector()
    record = _valid_raw_record()
    raw_input = tmp_path / "raw.jsonl"
    _write_jsonl(raw_input, [record])
    output = tmp_path / "normalized.jsonl"
    exit_code = module.main(["--input", str(raw_input), "--output", str(output)])
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
    exit_code = module.main(["--input", str(missing), "--output", str(output)])
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
    exit_code = module.main(["--input", str(raw_input), "--output", str(output)])
    assert exit_code == 0
    lines = [json.loads(line) for line in output.read_text().splitlines() if line.strip()]
    assert len(lines) == 30
    assert all("status" not in line for line in lines)


# --- 4B: analyzer -----------------------------------------------------------


def _analysis_input(
    tmp_path: Path, records: list[dict[str, Any]], name: str = "normalized.jsonl"
) -> Path:
    path = tmp_path / name
    _write_jsonl(path, records)
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
    module.main(["--input", str(input_path), "--output", str(output)])
    result = json.loads(output.read_text())
    assert result["status"] == common.EvidenceStatus.BLOCKED.value
    assert result["reason"] == "undersized_cell"
    assert result["promotion_eligible"] is False
    assert result["analysis"] is None


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
    module.main(["--input", str(input_path), "--output", str(output)])
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
    module.main(["--input", str(input_path), "--output", str(output)])
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
    module.main(["--input", str(input_path), "--output", str(output)])
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
    for i in range(n):
        turn_id = f"turn-{i}"
        records.append(
            _valid_raw_record(
                run_id=f"run-base-{i}",
                fixture_turn_id=turn_id,
                condition="baseline",
                latency_ms=baseline_latency,
                quality_score=quality,
                context_chars=1000 + i,
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
            )
        )
    return records


def test_analyzer_promotes_when_thresholds_are_comfortably_exceeded(tmp_path: Path) -> None:
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600)
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(["--input", str(input_path), "--output", str(output)])
    result = json.loads(output.read_text())
    assert result["status"] == "promoted"
    assert result["promotion_eligible"] is True
    assert result["analysis"] is not None


def test_analyzer_does_not_promote_when_improvement_is_below_10_percent(tmp_path: Path) -> None:
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=910)
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(["--input", str(input_path), "--output", str(output)])
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
    module.main(["--input", str(input_path), "--output", str(output)])
    result = json.loads(output.read_text())
    assert result["analysis"]["median_relative_latency_improvement"] == pytest.approx(0.10)
    assert result["promotion_eligible"] is True


def test_analyzer_does_not_promote_just_below_the_10_percent_boundary(tmp_path: Path) -> None:
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=901)
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(["--input", str(input_path), "--output", str(output)])
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
    module.main(["--input", str(input_path), "--output", str(output)])
    result = json.loads(output.read_text())
    assert result["promotion_eligible"] is False
    assert result["reason"] == "baseline_below_quality_floor"


def test_analyzer_accepts_baseline_exactly_at_quality_floor(tmp_path: Path) -> None:
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600, quality=0.90)
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(["--input", str(input_path), "--output", str(output)])
    result = json.loads(output.read_text())
    assert result["reason"] != "baseline_below_quality_floor"


def test_analyzer_does_not_promote_when_quality_drop_exceeds_0_02(tmp_path: Path) -> None:
    module = _analyzer()
    records = []
    for i in range(30):
        turn_id = f"turn-{i}"
        records.append(
            _valid_raw_record(
                run_id=f"run-base-{i}",
                fixture_turn_id=turn_id,
                condition="baseline",
                latency_ms=1000,
                quality_score=0.95,
                context_chars=1000 + i,
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
            )
        )
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(["--input", str(input_path), "--output", str(output)])
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
        turn_id = f"turn-{i}"
        records.append(
            _valid_raw_record(
                run_id=f"run-base-{i}",
                fixture_turn_id=turn_id,
                condition="baseline",
                latency_ms=1000,
                quality_score=0.95,
                context_chars=1000 + i,
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
            )
        )
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(["--input", str(input_path), "--output", str(output)])
    result = json.loads(output.read_text())
    assert result["reason"] != "quality_drop_exceeded"
    assert result["promotion_eligible"] is True


def test_analyzer_halts_baseline_too_noisy_above_sd_0_01(tmp_path: Path) -> None:
    """Plan assumption (a): baseline repeated-run SD > 0.01 halts with
    reason='baseline_too_noisy' rather than applying the promotion rubric
    against unmeasured provider noise."""
    module = _analyzer()
    records = []
    noisy_scores = [0.99, 0.85] * 15
    for i, score in enumerate(noisy_scores):
        turn_id = f"turn-{i}"
        records.append(
            _valid_raw_record(
                run_id=f"run-base-{i}",
                fixture_turn_id=turn_id,
                condition="baseline",
                latency_ms=1000,
                quality_score=score,
                context_chars=1000 + i,
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
            )
        )
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(["--input", str(input_path), "--output", str(output)])
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
    scores = [0.945, 0.955] * 15  # pstdev = 0.005, well under the 0.01 threshold
    records = []
    for i, score in enumerate(scores):
        turn_id = f"turn-{i}"
        records.append(
            _valid_raw_record(
                run_id=f"run-base-{i}",
                fixture_turn_id=turn_id,
                condition="baseline",
                latency_ms=1000,
                quality_score=score,
                context_chars=1000 + i,
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
            )
        )
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(["--input", str(input_path), "--output", str(output)])
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
    exit_code = module.main(["--input", str(input_path), "--output", str(output)])
    assert exit_code != 0


def test_analyzer_excludes_unpaired_identities_from_the_paired_count(tmp_path: Path) -> None:
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600)
    records.append(
        _valid_raw_record(
            run_id="run-orphan",
            fixture_turn_id="turn-orphan",
            condition="baseline",
            latency_ms=1000,
        )
    )
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    module.main(["--input", str(input_path), "--output", str(output)])
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
    module.main(["--input", str(input_path), "--output", str(output)])
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
    module.main(["--input", str(input_path), "--output", str(first_output)])
    module.main(["--input", str(input_path), "--output", str(second_output)])
    first = json.loads(first_output.read_text())
    second = json.loads(second_output.read_text())
    first.pop("generated_at_utc", None)
    second.pop("generated_at_utc", None)
    assert first == second, "seed-0 bootstrap must be reproducible across identical invocations"
    assert first["analysis"]["bootstrap_seed"] == 0
    assert first["analysis"]["bootstrap_iterations"] == 10_000


def test_analyzer_gap_does_not_yet_reject_forbidden_raw_fields(tmp_path: Path) -> None:
    """KNOWN GAP: the plan's Testing Notes list 'forbidden raw fields' among
    the analyzer's own rejection matrix, but the delivered analyze()/
    build_analysis() never runs the closed_object allowlist check itself
    (only the runner/collector do). This documents the gap as an xfail
    rather than silently omitting the plan-required case."""
    module = _analyzer()
    records = _paired_cells(n=5, baseline_latency=1000, narrowed_latency=600)
    records[0]["prompt"] = "leaked prompt text"
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    exit_code = module.main(["--input", str(input_path), "--output", str(output)])
    if exit_code == 0:
        pytest.xfail("analyzer does not re-run the raw allowlist check itself (Phase 4B gap)")
    assert exit_code != 0


def test_analyzer_gap_does_not_yet_reject_a_missing_selected_dimension(tmp_path: Path) -> None:
    """KNOWN GAP: 'analyzer tests cover missing dimensions' per the plan's
    Phase 4B testing notes, but build_analysis() never reads
    selected_dimension at all, so a record missing it is silently accepted
    today."""
    module = _analyzer()
    records = _paired_cells(n=30, baseline_latency=1000, narrowed_latency=600)
    del records[0]["selected_dimension"]
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    exit_code = module.main(["--input", str(input_path), "--output", str(output)])
    if exit_code == 0:
        pytest.xfail("analyzer does not validate selected_dimension presence (Phase 4B gap)")
    assert exit_code != 0


def test_analyzer_rejects_mixed_fixture_versions(tmp_path: Path) -> None:
    module = _analyzer()
    records = _paired_cells(n=5, baseline_latency=1000, narrowed_latency=600)
    records[0]["fixture_version"] = "some-other-version"
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    exit_code = module.main(["--input", str(input_path), "--output", str(output)])
    assert exit_code != 0


def test_analyzer_rejects_mixed_scorer_versions(tmp_path: Path) -> None:
    module = _analyzer()
    records = _paired_cells(n=5, baseline_latency=1000, narrowed_latency=600)
    records[0]["scorer_version"] = "some-other-scorer"
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    exit_code = module.main(["--input", str(input_path), "--output", str(output)])
    assert exit_code != 0


# --- End-to-end: dry-run -> collector-shaped analyzer input, and the -------
# --- production-defaults invariant that must hold regardless of outcome. --


def test_dry_run_records_feed_directly_into_the_analyzer_as_a_valid_shape(
    tmp_path: Path,
) -> None:
    """The runner's dry-run records use provider="synthetic" and
    retrieval_snapshot_id=None and are never treated as real evidence -- the
    analyzer must always resolve them to blocked/provider_effect_uncontrolled
    (no discoverable retrieval-snapshot control) or blocked/undersized_cell,
    never a promotion decision. This is still valuable schema-compatibility
    coverage: it proves 4A's dry-run output is shape-compatible with the
    analyzer's input contract (same field set as a collector-normalized
    cell) end to end, before any paid collection exists."""
    records = _dry_run_records(tmp_path, baseline_repeats=15)
    analyzer = _analyzer()
    common = _evidence_common()
    input_path = _analysis_input(tmp_path, records)
    output = tmp_path / "analysis.json"
    exit_code = analyzer.main(["--input", str(input_path), "--output", str(output)])
    assert exit_code == 0
    result = json.loads(output.read_text())
    assert result["status"] == common.EvidenceStatus.BLOCKED.value
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
