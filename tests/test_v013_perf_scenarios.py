"""Phase 0 credential-free evidence fixture for v0.1.3 (see the dev plan's
Phase 0 bullets in docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md).

Drives the six named Phase 0 scenarios (direct, delegated_complete,
retained_late, cancellation, reconnect, same_epoch_newer_turn) through the
real ``server.perf_metrics`` recorders, converts the resulting PERF_METRIC
records into ``shared/schemas/v013-evidence.json``-shaped evidence records,
writes them as dated JSONL under ``docs/benchmarks/``, and asserts
``scripts/validate_v013_evidence.py`` accepts the artifact.

All scenarios here are credential-free: provider/model are the explicit
``unavailable``/``unavailable`` stratum. A paid-provider supplement is out of
scope for this fixture (see ``scripts/smoke_conversation.py``).
"""

from __future__ import annotations

import importlib.util
import json
import time
import uuid
from pathlib import Path
from typing import Any

from server.perf_metrics import (
    AppTurnRecorder,
    CollectingMeasurementSink,
    RetainedRecorder,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
ARTIFACT_PATH = REPO_ROOT / "docs" / "benchmarks" / "v0.1.3-phase0-transport-baseline.jsonl"


def _load_validator() -> Any:
    """Import scripts/validate_v013_evidence.py by path.

    ``scripts/`` is not a package, so this loads the module directly rather
    than mutating ``sys.path`` at import time.
    """
    spec = importlib.util.spec_from_file_location(
        "validate_v013_evidence", REPO_ROOT / "scripts" / "validate_v013_evidence.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


validate_artifact = _load_validator().validate_artifact

UNAVAILABLE = "unavailable"


def _evidence_record(
    *,
    scenario: str,
    turn_id: str,
    work_item_id: str | None,
    provider: str,
    model: str,
    query_chars: int | None,
    context_chars: int | None,
    routing_phase_latency_ms: float | None,
    outcome: str,
    disposition: str,
    run_id: str,
    sample_index: int,
    sample_count: int,
    monotonic_origin_ns: int,
) -> dict[str, Any]:
    return {
        "phase": "phase0",
        "scenario": scenario,
        "turn_id": turn_id,
        "work_item_id": work_item_id,
        "provider": provider,
        "model": model,
        "query_chars": query_chars,
        "context_chars": context_chars,
        "routing_phase_latency_ms": routing_phase_latency_ms,
        "outcome": outcome,
        "disposition": disposition,
        "sample_timestamp_ms": (time.monotonic_ns() - monotonic_origin_ns) // 1_000_000,
        "run_id": run_id,
        "sample_index": sample_index,
        "sample_count": sample_count,
    }


def build_phase0_fixture(run_id: str | None = None) -> list[dict[str, Any]]:
    """Drive the Phase 0 scenario matrix through the real recorders and
    return one schema-shaped evidence record per scenario.

    Each scenario finalizes a real ``AppTurnRecorder`` (and, where the
    scenario dispatches a search, a child ``WorkItemRecorder`` or a
    ``RetainedRecorder``) against a ``CollectingMeasurementSink``, so the
    evidence record's ``routing_phase_latency_ms``/query/context/provider/
    model dimensions are pulled from real emitted ``PERF_METRIC`` fields
    rather than invented directly in the fixture.
    """
    run_id = run_id or f"phase0-fixture-{uuid.uuid4().hex[:12]}"
    monotonic_origin_ns = time.monotonic_ns()
    sink = CollectingMeasurementSink()
    records: list[dict[str, Any]] = []

    # 1. direct — router resolves without delegating; no search dispatch.
    direct_turn = AppTurnRecorder(sink, session_id="s0", origin_epoch=1, turn_id="turn-direct")
    direct_turn.record_routing(12.5)
    direct_turn.finalize(outcome="direct", scenario="direct", acknowledgement=False)
    direct_fields = sink.get("app_turn_foreground", turn_id="turn-direct")[0].fields
    records.append(
        _evidence_record(
            scenario="direct",
            turn_id="turn-direct",
            work_item_id=None,
            provider=UNAVAILABLE,
            model=UNAVAILABLE,
            query_chars=None,
            context_chars=None,
            routing_phase_latency_ms=direct_fields["routing_ms"],
            outcome=direct_fields["outcome"],
            disposition="not_applicable",
            run_id=run_id,
            sample_index=len(records),
            sample_count=6,
            monotonic_origin_ns=monotonic_origin_ns,
        )
    )

    # 2. delegated_complete — search dispatched and completes inside the
    # foreground window; delivered as autoplay.
    complete_turn = AppTurnRecorder(sink, session_id="s0", origin_epoch=1, turn_id="turn-complete")
    complete_turn.record_routing(18.0)
    child = complete_turn.new_child(work_item_id="work-complete")
    child.finalize(
        outcome="completed",
        query_chars=42,
        context_chars=128,
        provider=UNAVAILABLE,
        model=UNAVAILABLE,
    )
    complete_turn.finalize(scenario="delegated_complete", acknowledgement=True)
    complete_turn_fields = sink.get("app_turn_foreground", turn_id="turn-complete")[0].fields
    complete_child_fields = sink.get(
        "work_item_foreground", turn_id="turn-complete", work_item_id="work-complete"
    )[0].fields
    records.append(
        _evidence_record(
            scenario="delegated_complete",
            turn_id="turn-complete",
            work_item_id="work-complete",
            provider=complete_child_fields["provider"],
            model=complete_child_fields["model"],
            query_chars=complete_child_fields["query_chars"],
            context_chars=complete_child_fields["context_chars"],
            routing_phase_latency_ms=complete_turn_fields["routing_ms"],
            outcome=complete_turn_fields["outcome"],
            disposition="autoplay",
            run_id=run_id,
            sample_index=len(records),
            sample_count=6,
            monotonic_origin_ns=monotonic_origin_ns,
        )
    )

    # 3. retained_late — foreground timeout exceeded; late result delivered
    # in the background and committed exactly once, display-only.
    retained = RetainedRecorder(
        sink,
        session_id="s0",
        origin_epoch=1,
        turn_id="turn-retained",
        work_item_id="work-retained",
        app_worker_id="worker-retained",
    )
    retained.claim("completed")
    retained.record_commit("committed", result_id="result-retained")
    retained.finalize(
        speech_outcome="queued",
        provider=UNAVAILABLE,
        model=UNAVAILABLE,
        delivery_disposition="display_only",
    )
    retained_fields = sink.get(
        "work_item_background", turn_id="turn-retained", work_item_id="work-retained"
    )[0].fields
    records.append(
        _evidence_record(
            scenario="retained_late",
            turn_id="turn-retained",
            work_item_id="work-retained",
            provider=retained_fields["provider"],
            model=retained_fields["model"],
            query_chars=37,
            context_chars=96,
            routing_phase_latency_ms=21.0,
            outcome="retained",
            disposition=retained_fields["delivery_disposition"],
            run_id=run_id,
            sample_index=len(records),
            sample_count=6,
            monotonic_origin_ns=monotonic_origin_ns,
        )
    )

    # 4. cancellation — turn cancelled before routing completes; no routing
    # latency is available.
    cancelled_turn = AppTurnRecorder(
        sink, session_id="s0", origin_epoch=1, turn_id="turn-cancelled"
    )
    cancelled_turn.finalize(outcome="cancelled", scenario="cancellation", acknowledgement=False)
    cancelled_fields = sink.get("app_turn_foreground", turn_id="turn-cancelled")[0].fields
    records.append(
        _evidence_record(
            scenario="cancellation",
            turn_id="turn-cancelled",
            work_item_id=None,
            provider=UNAVAILABLE,
            model=UNAVAILABLE,
            query_chars=None,
            context_chars=None,
            routing_phase_latency_ms=None,
            outcome=cancelled_fields["outcome"],
            disposition="suppressed",
            run_id=run_id,
            sample_index=len(records),
            sample_count=6,
            monotonic_origin_ns=monotonic_origin_ns,
        )
    )

    # 5. reconnect — result committed server-side across a reconnect; wire
    # delivery is server-commit/display state only, never autoplay.
    reconnect_turn = AppTurnRecorder(
        sink, session_id="s0", origin_epoch=2, turn_id="turn-reconnect"
    )
    reconnect_turn.record_routing(15.0)
    reconnect_child = reconnect_turn.new_child(work_item_id="work-reconnect")
    reconnect_child.finalize(
        outcome="completed",
        query_chars=29,
        context_chars=64,
        provider=UNAVAILABLE,
        model=UNAVAILABLE,
    )
    reconnect_turn.finalize(scenario="reconnect", acknowledgement=True)
    reconnect_turn_fields = sink.get("app_turn_foreground", turn_id="turn-reconnect")[0].fields
    reconnect_child_fields = sink.get(
        "work_item_foreground", turn_id="turn-reconnect", work_item_id="work-reconnect"
    )[0].fields
    records.append(
        _evidence_record(
            scenario="reconnect",
            turn_id="turn-reconnect",
            work_item_id="work-reconnect",
            provider=reconnect_child_fields["provider"],
            model=reconnect_child_fields["model"],
            query_chars=reconnect_child_fields["query_chars"],
            context_chars=reconnect_child_fields["context_chars"],
            routing_phase_latency_ms=reconnect_turn_fields["routing_ms"],
            outcome=reconnect_turn_fields["outcome"],
            disposition="display_only",
            run_id=run_id,
            sample_index=len(records),
            sample_count=6,
            monotonic_origin_ns=monotonic_origin_ns,
        )
    )

    # 6. same_epoch_newer_turn — a newer same-epoch turn supersedes a
    # still-pending delivery; the superseded delivery is suppressed.
    superseded_turn = AppTurnRecorder(
        sink, session_id="s0", origin_epoch=3, turn_id="turn-superseded"
    )
    superseded_turn.record_routing(9.0)
    superseded_child = superseded_turn.new_child(work_item_id="work-superseded")
    superseded_child.finalize(
        outcome="cancelled",
        query_chars=31,
        context_chars=80,
        provider=UNAVAILABLE,
        model=UNAVAILABLE,
    )
    superseded_turn.finalize(scenario="same_epoch_newer_turn", acknowledgement=True)
    superseded_turn_fields = sink.get("app_turn_foreground", turn_id="turn-superseded")[0].fields
    superseded_child_fields = sink.get(
        "work_item_foreground", turn_id="turn-superseded", work_item_id="work-superseded"
    )[0].fields
    records.append(
        _evidence_record(
            scenario="same_epoch_newer_turn",
            turn_id="turn-superseded",
            work_item_id="work-superseded",
            provider=superseded_child_fields["provider"],
            model=superseded_child_fields["model"],
            query_chars=superseded_child_fields["query_chars"],
            context_chars=superseded_child_fields["context_chars"],
            routing_phase_latency_ms=superseded_turn_fields["routing_ms"],
            outcome=superseded_turn_fields["outcome"],
            disposition="suppressed",
            run_id=run_id,
            sample_index=len(records),
            sample_count=6,
            monotonic_origin_ns=monotonic_origin_ns,
        )
    )

    return records


def test_phase0_fixture_covers_all_six_named_scenarios() -> None:
    records = build_phase0_fixture()
    assert {r["scenario"] for r in records} == {
        "direct",
        "delegated_complete",
        "retained_late",
        "cancellation",
        "reconnect",
        "same_epoch_newer_turn",
    }


def test_phase0_fixture_covers_direct_completed_retained_cancelled_outcomes() -> None:
    records = build_phase0_fixture()
    assert {r["outcome"] for r in records} >= {"direct", "completed", "retained", "cancelled"}


def test_phase0_fixture_is_entirely_credential_free() -> None:
    records = build_phase0_fixture()
    assert {(r["provider"], r["model"]) for r in records} == {(UNAVAILABLE, UNAVAILABLE)}


def test_phase0_fixture_produces_a_schema_valid_dated_artifact(tmp_path: Path) -> None:
    records = build_phase0_fixture()
    scratch_path = tmp_path / "v0.1.3-phase0-transport-baseline.jsonl"
    scratch_path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")

    validated = validate_artifact("phase0", scratch_path)

    assert len(validated) == 6


def test_phase0_writes_the_committed_docs_benchmarks_artifact() -> None:
    """Produce the dated, credential-free artifact under docs/benchmarks/ and
    validate it in place, per the Phase 0 completion gate.

    Uses a fixed run_id (rather than build_phase0_fixture's default random
    uuid) so re-running this test does not churn the committed artifact with
    a no-op diff.
    """
    records = build_phase0_fixture(run_id="phase0-fixture-committed")
    ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT_PATH.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")

    validated = validate_artifact("phase0", ARTIFACT_PATH)

    assert len(validated) == 6
