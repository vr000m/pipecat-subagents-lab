"""Shared eval-suite plumbing: per-run session construction and hoisted metric helpers.

Hoisted out of ``scripts/smoke_conversation.py`` (which now re-exports these two
names for backward compatibility with existing imports, including
``tests/test_smoke_conversation.py``) so ``scripts/eval_model_comparison.py`` can
reuse the same latency-measurement pattern without importing a script module as
a library.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from server.config import Config
from server.perf_metrics import CollectingMeasurementSink
from server.pipeline import SessionHost
from server.registry import WorkerRegistry
from server.router import LazyRouterProvider, Router
from server.work_item_coordinator import WorkItemCoordinator

__all__ = [
    "CollectingMeasurementSink",
    "_latest_turn_stage_metrics",
    "build_session_for_run",
]


def _latest_turn_stage_metrics(sink: Any, elapsed_ms: float, turn_id: str) -> dict[str, float]:
    """Read the given ``turn_id``'s correlated PERF_METRIC records.

    Selecting by the caller's own ``turn_id`` (rather than the newest record
    of each kind) is required so a turn that emits no ``work_item_foreground``
    record -- any direct/unsupported/clarify router turn -- never silently
    inherits a preceding delegated turn's ``search_ms``/``total_ms``.
    """
    turn_records = [
        record
        for record in sink.records
        if record.event == "app_turn_foreground" and record.fields.get("turn_id") == turn_id
    ]
    if not turn_records:
        raise RuntimeError(f"no app_turn_foreground metric was emitted for turn_id={turn_id!r}")
    turn_record = turn_records[-1]
    work_records = [
        record
        for record in sink.records
        if record.event == "work_item_foreground" and record.fields.get("turn_id") == turn_id
    ]
    work_record = work_records[-1] if work_records else None
    return {
        "routing_ms": float(turn_record.fields.get("routing_ms", 0)),
        "search_ms": float(work_record.fields.get("search_ms", 0)) if work_record else 0.0,
        "total_ms": float(turn_record.fields.get("total_ms", elapsed_ms)),
    }


def build_session_for_run(
    config: Config,
    *,
    measurement_sink: Any | None = None,
    router_responses_factory: Callable[[], Any] | None = None,
    worker_responses: Any = None,
) -> SessionHost:
    """Construct a fresh Router/WorkerRegistry/WorkItemCoordinator/SessionHost,
    all bound to this exact per-run ``Config`` at construction time.

    Deliberately does **not** go through ``server.app._default_session_host()``
    (which internally calls ``load_config()`` from the environment) or the
    ``host.config = tuned`` post-hoc reassignment
    ``scripts/smoke_conversation.py``'s ``_run_child`` uses: the router
    provider (``LazyRouterProvider``) captures its ``Config`` reference at
    construction and resolves the model/effort from that captured reference at
    call time, so a post-hoc reassignment of ``host.config`` never reaches it.
    Every eval run therefore needs its own ``Router``/``LazyRouterProvider``
    constructed from *this* run's ``Config`` -- see the dev plan's
    "Architecture & Call Flow / Injection seam" note.

    Text-only: ``host.stt``/``host.tts`` are disabled the same way
    ``_run_child`` disables them for its non-speech scenarios, since every eval
    scenario here drives turns directly through ``_handle_transcript`` with no
    audio in or out.
    """
    registry = WorkerRegistry(config=config, responses=worker_responses)
    router = Router(
        call=LazyRouterProvider(config, router_responses_factory),
        config=config,
    )
    coordinator = WorkItemCoordinator(registry=registry, router=router, config=config)
    host = SessionHost(
        registry=registry,
        coordinator=coordinator,
        measurement_sink=measurement_sink,
        config=config,
    )
    host.stt = None
    host.tts = None
    return host
