"""Shared eval-suite plumbing: per-run session construction and hoisted metric helpers.

Hoisted out of ``scripts/smoke_conversation.py`` (which now re-exports these
names for backward compatibility with existing imports, including
``tests/test_smoke_conversation.py``) so ``scripts/eval_model_comparison.py``
and ``evals/scenarios.py`` can reuse the same latency-measurement pattern and
scenario constants without importing a script module as a library -- the
layering this module exists for in the first place.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

from server.config import Config
from server.perf_metrics import CollectingMeasurementSink
from server.pipeline import SessionHost
from server.registry import WorkerRegistry
from server.router import LazyRouterProvider, Router
from server.work_item_coordinator import WorkItemCoordinator

__all__ = [
    "DEFAULT_QUERY",
    "ROUTING_REGRESSION_QUERIES",
    "SAFE_FALLBACKS",
    "CollectingMeasurementSink",
    "_latest_turn_stage_metrics",
    "build_judge_llm_service",
    "build_session_for_run",
    "confined_output_path",
    "write_no_follow",
]

_REPO_ROOT = Path(__file__).resolve().parents[1]

# Shared scenario constants. Both scripts/smoke_conversation.py (the live
# smoke CLI) and evals/scenarios.py (the eval-suite matrix) need the same
# query text -- hoisted here, not left in scripts/smoke_conversation.py, so
# evals/scenarios.py and scripts/eval_model_comparison.py don't have to import
# a script module as a library to get them (the exact layering violation this
# module's docstring already warns against). scripts/smoke_conversation.py
# re-exports these names for backward compatibility with existing imports.
DEFAULT_QUERY = "What is the latest stable Pipecat release?"
ROUTING_REGRESSION_QUERIES = (
    "Hi.",
    "Tell me the weather in Riga. For today.",
    "Could you tell me the weather in Helsinki today?",
)
SAFE_FALLBACKS = {
    "Routing is temporarily unavailable. Please try that request again.",
    "The web search is temporarily unavailable.",
    "I could not find a reliable result for that request.",
}


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
    (which internally calls ``load_config()`` from the environment) or a
    post-hoc ``host.config = tuned`` reassignment: the router provider
    (``LazyRouterProvider``) captures its ``Config`` reference at construction
    and resolves the model/effort from that captured reference at call time,
    so a post-hoc reassignment of ``host.config`` never reaches it. Every
    caller that needs to vary the router/worker model+effort per run
    therefore needs its own ``Router``/``LazyRouterProvider`` constructed from
    *this* run's ``Config`` -- see the dev plan's "Architecture & Call Flow /
    Injection seam" note. ``scripts/smoke_conversation.py``'s ``_run_child``
    is migrated onto this same seam (rather than keeping its own now-obsolete
    post-hoc reassignment idiom) so the two callers never drift apart.

    Text-only by default: ``host.stt``/``host.tts`` are set to ``None`` here,
    since every eval/smoke scenario built on this helper drives turns
    directly through ``_handle_transcript`` with no audio in or out. A caller
    that needs real TTS (e.g. the ack-ordering smoke) reassigns
    ``host.tts`` itself after construction.
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


def build_judge_llm_service(model: str, api_key: str | None) -> Any:
    """Build the judge's Chat Completions LLM service with an explicit credential.

    ``pipecat.evals.services.openai_service(config)`` -- the library factory
    both callers used to go through -- only reads ``config["model"]`` and
    otherwise relies on the OpenAI SDK's own ``OPENAI_API_KEY`` environment
    lookup. That silently ignores an operator's ``WEBSEARCH_OPENAI_API_KEY``/
    ``WEBSEARCH_OPENAI_API_KEY_ENV`` override -- the credential this repo's own
    ``load_config()`` resolves -- so a judge/verifier client built through it
    can fail (or silently pick up the wrong key) for any operator not using
    the raw ``OPENAI_API_KEY`` env var. Constructing ``OpenAILLMService``
    directly, threading the already-resolved credential through explicitly,
    fixes that without needing to change the installed library.
    """
    from pipecat.services.openai.llm import OpenAILLMService

    return OpenAILLMService(settings=OpenAILLMService.Settings(model=model), api_key=api_key)


def confined_output_path(raw_path: str | Path, *, allowed_root: Path | None = None) -> Path:
    """Resolve a user-supplied ``--out`` path, confined to the repo tree.

    An operator-supplied output path is still attacker-influenced surface (a
    credentialed run could be invoked with a scripted or copy-pasted ``--out``
    value) -- rejects `..` traversal that escapes ``allowed_root`` and refuses
    to resolve onto an existing symlink, so a planted symlink at the target
    path cannot redirect the write to an arbitrary file. Raises ``ValueError``
    on either violation; callers decide how to surface that to the operator.
    """
    root = (allowed_root or _REPO_ROOT).resolve()
    candidate = Path(raw_path)
    resolved = candidate if candidate.is_absolute() else (root / candidate)
    if resolved.is_symlink():
        raise ValueError(f"refusing to write through an existing symlink: {resolved}")
    resolved = resolved.resolve()
    if not resolved.is_relative_to(root):
        raise ValueError(
            f"output path must stay within {root}: {raw_path!r} resolved to {resolved}"
        )
    return resolved


def write_no_follow(path: Path, content: str) -> None:
    """Write text to ``path`` without following an existing symlink there.

    Pairs with :func:`confined_output_path` -- that function rejects a
    symlink already resolved by the time it runs its check, but a TOCTOU
    window remains between that check and the actual write unless the write
    itself also refuses to follow a symlink planted in between. ``O_NOFOLLOW``
    closes that window at the syscall level.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o644)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(content)
