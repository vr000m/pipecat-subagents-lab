#!/usr/bin/env python3
"""Run bounded, paid router-to-web-search conversation smoke(s)."""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
from dataclasses import replace
from typing import Any

# Hoisted to scripts/_eval_common.py so scripts/eval_model_comparison.py and
# evals/scenarios.py can reuse the same latency-measurement helper, in-memory
# sink, and scenario constants without importing this script module as a
# library; re-exported here so existing imports (including
# tests/test_smoke_conversation.py) keep working unchanged.
from scripts._eval_common import (
    DEFAULT_QUERY,
    ROUTING_REGRESSION_QUERIES,
    SAFE_FALLBACKS,
    CollectingMeasurementSink,
    _latest_turn_stage_metrics,
    build_session_for_run,
)

__all__ = [
    "DEFAULT_QUERY",
    "ROUTING_REGRESSION_QUERIES",
    "SAFE_FALLBACKS",
    "CollectingMeasurementSink",
    "_latest_turn_stage_metrics",
]

RESULT_PREFIX = "SMOKE_RESULT="


async def _run_child(
    query: str,
    *,
    max_latency_seconds: float,
    max_routing_seconds: float,
    routing_regression: bool,
    ack_ordering: bool,
    max_ack_seconds: float,
) -> dict[str, Any]:
    from server.config import load_config
    from server.perf_metrics import CollectingMeasurementSink

    sink = CollectingMeasurementSink()
    # Built via build_session_for_run(), not _default_session_host() + a
    # post-hoc host.config reassignment: the router provider captures its
    # Config reference at construction, so a later `host.config = tuned`
    # never reaches it (see build_session_for_run's own docstring, and the
    # dev plan's "Architecture & Call Flow / Injection seam" note). This
    # smoke's model/effort is always the operator's configured default (no
    # per-run candidate override the way the eval-suite runner has), but it
    # shares the same construction seam so the two never drift apart.
    base_config = load_config()
    # The semantic smoke waits longer than the interactive foreground path so
    # it validates the final provider result rather than the background-work
    # acknowledgement. The total latency budget remains independently enforced.
    tuned = replace(
        base_config,
        foreground_search_timeout_seconds=max_latency_seconds,
        provider_timeout_seconds=max(
            base_config.provider_timeout_seconds,
            max_latency_seconds + 15,
        ),
    )
    host = build_session_for_run(tuned, measurement_sink=sink)
    # This smoke isolates the paid semantic path. Browser media and local
    # speech have separate deterministic/live acceptance commands.
    # build_session_for_run() already sets host.stt/host.tts = None.
    await host.start()
    try:
        if ack_ordering:
            return await _run_ack_ordering(
                host,
                query,
                max_ack_seconds=max_ack_seconds,
                max_latency_seconds=max_latency_seconds,
                sink=sink,
                max_routing_seconds=max_routing_seconds,
            )
        # The single-turn and routing-regression scenarios don't exercise
        # speech at all, so they run with TTS disabled (see _run_ack_ordering
        # for the scenario that needs it wired up).
        host.tts = None
        connection = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 1,
                "snapshot_sequence": 0,
            }
        )
        if routing_regression:
            return await _run_routing_regression(
                host,
                connection,
                sink,
                max_latency_seconds=max_latency_seconds,
                max_routing_seconds=max_routing_seconds,
            )
        started = time.perf_counter()
        value = await host._handle_transcript(query, origin=connection)
        elapsed_ms = (time.perf_counter() - started) * 1000
        results = value if isinstance(value, tuple) else (value,)
        if len(results) != 1:
            raise RuntimeError(f"expected one result, received {len(results)}")
        result = results[0]
        if result.worker_id == "main":
            raise RuntimeError("public-web smoke fell back to the main responder")
        if result.ui_text in SAFE_FALLBACKS:
            raise RuntimeError("public-web smoke returned a safe fallback")
        if not result.spoken_text or len(result.spoken_text) > 600:
            raise RuntimeError("public-web smoke returned an invalid spoken projection")
        if not result.citations:
            raise RuntimeError("public-web smoke returned no normalized citations")
        stage_metrics = _latest_turn_stage_metrics(sink, elapsed_ms, result.turn_id)
        routing_ms = stage_metrics["routing_ms"]
        search_ms = stage_metrics["search_ms"]
        total_ms = stage_metrics["total_ms"]
        if routing_ms > max_routing_seconds * 1000:
            raise RuntimeError(
                f"routing exceeded {max_routing_seconds:.1f}s budget: {routing_ms:.1f}ms"
            )
        if total_ms > max_latency_seconds * 1000:
            raise RuntimeError(
                f"conversation exceeded {max_latency_seconds:.1f}s latency budget: {total_ms:.1f}ms"
            )
        return {
            "scenario": "single",
            "worker": result.worker_id,
            "display_chars": len(result.ui_text),
            "spoken_chars": len(result.spoken_text),
            "citations": len(result.citations),
            "routing_ms": round(routing_ms, 1),
            "search_ms": round(search_ms, 1),
            "total_ms": round(total_ms, 1),
            "harness_elapsed_ms": round(elapsed_ms, 1),
        }
    finally:
        await host.shutdown()


async def _drive_lifecycle(connection: Any, lease: Any) -> None:
    """Stand in for what a real transport does to an admitted generation:
    bind the marker token, observe synthesis start/stop, then the fieldless
    upstream bot stop. Mirrors tests/test_pipeline.py's
    release_lifecycle_slot, including awaiting the coordinator's
    fire-and-forget on_terminal task so the transport slot is actually
    released (and the next queued item admitted) before this returns --
    without that, a caller that tears the connection down right after (as
    _run_ack_ordering does, via host.shutdown()) can race the coordinator's
    still-in-flight generation termination against shutdown's own interrupt,
    corrupting the recorded disposition.
    """
    lifecycle = connection.lifecycle
    utterance_id = lease.item.utterance_id
    bound = lifecycle.bind_context(lease.token, utterance_id)
    started = lifecycle.on_tts_started(utterance_id) if bound else False
    stopped = lifecycle.on_tts_stopped(utterance_id) if started else False
    if not (bound and started and stopped):
        raise RuntimeError(
            f"lifecycle wiring rejected the {lease.item.role} generation "
            f"(bind={bound}, started={started}, stopped={stopped}, token={lease.token!r})"
        )
    terminal = lifecycle.on_transport_bot_stopped()
    if terminal is not None:
        await terminal


async def _run_ack_ordering(
    host: Any,
    query: str,
    *,
    max_ack_seconds: float,
    max_latency_seconds: float,
    sink: Any,
    max_routing_seconds: float,
) -> dict[str, Any]:
    """Prove the early ack is admitted for real speech while a live,
    paid-provider delegated search is still in flight -- the externally
    observable contract ``tests/test_pipeline.py::
    test_early_ack_is_enqueued_immediately_after_delegated_search_dispatch``
    proves against a fake coordinator/worker. Uses a recording TTS/worker
    (no audio synthesis, no extra cost) so the scheduler treats this
    connection as TTS-capable and actually admits the ack, instead of the
    ``host.tts = None`` isolation the other scenarios use.
    """
    from server.services.tts import CorrelatedTTSSpeakFrame
    from server.speech_scheduler import ROLE_ACK, ROLE_RESULT

    class _RecordingTTS:
        on_event = None

        @staticmethod
        def correlated_speak_frame(
            text: str, *, correlation_id: str, append_to_context: bool
        ) -> CorrelatedTTSSpeakFrame:
            return CorrelatedTTSSpeakFrame(
                text=text, correlation_id=correlation_id, append_to_context=append_to_context
            )

    class _RecordingWorker:
        async def queue_frame(self, frame: object) -> None:
            del frame

        async def cancel(self, *, reason: str) -> None:
            del reason

    host.tts = _RecordingTTS()
    connection = await host.connect(
        {
            "session_id": host.state.session_id,
            "resume_token": host.state.resume_token,
            "proposed_epoch": 1,
            "snapshot_sequence": 0,
        }
    )
    connection.worker = _RecordingWorker()
    scheduler = connection.scheduler

    started = time.perf_counter()
    pending = asyncio.create_task(host._handle_transcript(query, origin=connection))

    admissions: list[tuple[str, float]] = []
    seen_tokens: set[str] = set()
    ack_released = False
    while True:
        elapsed = time.perf_counter() - started
        # Check admission before the done-check below: start_next() can admit
        # the final result synchronously as the very last step of the task
        # that _handle_transcript's own await resolves from, so a loop that
        # exits on pending.done() without checking active one more time here
        # would miss that admission entirely.
        lease = scheduler.active
        if lease is not None and lease.token not in seen_tokens:
            seen_tokens.add(lease.token)
            admissions.append((lease.item.role, elapsed * 1000))
            try:
                await _drive_lifecycle(connection, lease)
            except RuntimeError:
                pending.cancel()
                raise
            if lease.item.role == ROLE_ACK:
                ack_released = True
                if elapsed > max_ack_seconds:
                    pending.cancel()
                    raise RuntimeError(
                        f"early ack was admitted late, at {elapsed:.1f}s "
                        f"(budget {max_ack_seconds:.1f}s)"
                    )
        if pending.done():
            break
        if not ack_released and elapsed > max_ack_seconds:
            pending.cancel()
            raise RuntimeError(
                f"early ack was not admitted for real speech within {max_ack_seconds:.1f}s "
                f"(active={scheduler.active!r}, queued_roles={scheduler.queued_roles()})"
            )
        if elapsed > max_latency_seconds:
            pending.cancel()
            raise RuntimeError(f"ack-ordering smoke exceeded {max_latency_seconds:.1f}s budget")
        await asyncio.sleep(0.01)

    value = await pending
    results = value if isinstance(value, tuple) else (value,)
    if len(results) != 1:
        raise RuntimeError(f"expected one result, received {len(results)}")
    result = results[0]
    if result.worker_id == "main":
        raise RuntimeError("ack-ordering smoke fell back to the main responder")
    if result.ui_text in SAFE_FALLBACKS:
        raise RuntimeError("ack-ordering smoke returned a safe fallback")
    if not result.spoken_text or len(result.spoken_text) > 600:
        raise RuntimeError("ack-ordering smoke returned an invalid spoken projection")
    if not result.citations:
        raise RuntimeError("ack-ordering smoke returned no normalized citations")

    roles_seen = [role for role, _ in admissions]
    if roles_seen.count(ROLE_ACK) != 1:
        raise RuntimeError(f"expected exactly one ack admission, saw {roles_seen}")
    if ROLE_RESULT not in roles_seen:
        raise RuntimeError(f"the final result was never admitted for speech: saw {roles_seen}")
    if roles_seen.index(ROLE_ACK) > roles_seen.index(ROLE_RESULT):
        raise RuntimeError(f"the ack was admitted after the result: {admissions}")

    ack_ms = next(ms for role, ms in admissions if role == ROLE_ACK)
    result_ms = next(ms for role, ms in admissions if role == ROLE_RESULT)
    # No separate `ack_ms > max_ack_seconds * 1000` check here: the in-loop
    # "early ack was admitted late" check above already raises at the same
    # instant this value was captured, on the same elapsed-vs-max_ack_seconds
    # comparison, so a second check on the same value can never fire.
    elapsed_ms = (time.perf_counter() - started) * 1000
    stage_metrics = _latest_turn_stage_metrics(sink, elapsed_ms, result.turn_id)
    routing_ms = stage_metrics["routing_ms"]
    if routing_ms > max_routing_seconds * 1000:
        raise RuntimeError(
            f"ack-ordering routing exceeded {max_routing_seconds:.1f}s budget: {routing_ms:.1f}ms"
        )
    return {
        "scenario": "ack-ordering",
        "worker": result.worker_id,
        "ack_ms": round(ack_ms, 1),
        "result_ms": round(result_ms, 1),
        "routing_ms": round(routing_ms, 1),
    }


async def _run_routing_regression(
    host: Any,
    connection: Any,
    sink: Any,
    *,
    max_latency_seconds: float,
    max_routing_seconds: float,
) -> dict[str, Any]:
    observations: list[dict[str, Any]] = []
    for index, query in enumerate(ROUTING_REGRESSION_QUERIES):
        started = time.perf_counter()
        value = await host._handle_transcript(query, origin=connection)
        elapsed_ms = (time.perf_counter() - started) * 1000
        results = value if isinstance(value, tuple) else (value,)
        if len(results) != 1:
            raise RuntimeError(
                f"routing regression returned {len(results)} results for turn {index + 1}"
            )
        result = results[0]
        routing = getattr(host.state, "routing", None)
        action = getattr(routing, "action", None)
        if index == 0:
            if action != "direct" or result.worker_id != "main":
                raise RuntimeError("greeting was not handled as a direct main response")
            if host.registry.workers:
                raise RuntimeError("greeting created a persistent worker")
        else:
            if result.worker_id == "main" or result.ui_text in SAFE_FALLBACKS:
                raise RuntimeError("weather turn returned a routing/search fallback")
            if action not in {"new_worker", "existing_worker"}:
                raise RuntimeError(f"weather turn used unexpected routing action: {action!r}")
        stage_metrics = _latest_turn_stage_metrics(sink, elapsed_ms, result.turn_id)
        routing_ms = stage_metrics["routing_ms"]
        total_ms = stage_metrics["total_ms"]
        if routing_ms > max_routing_seconds * 1000:
            raise RuntimeError(
                f"routing regression turn {index + 1} exceeded {max_routing_seconds:.1f}s "
                f"budget: {routing_ms:.1f}ms"
            )
        if total_ms > max_latency_seconds * 1000:
            raise RuntimeError(
                f"routing regression turn {index + 1} exceeded {max_latency_seconds:.1f}s "
                f"latency budget: {total_ms:.1f}ms"
            )
        observations.append(
            {
                "action": action,
                "worker": result.worker_id,
                "routing_ms": round(routing_ms, 1),
                "total_ms": round(total_ms, 1),
            }
        )
    return {
        "scenario": "routing-regression",
        "turns": len(observations),
        "worker_count_after_greeting": 0,
        "actions": [item["action"] for item in observations],
        "workers": [item["worker"] for item in observations],
        "max_routing_ms": max(item["routing_ms"] for item in observations),
        "max_total_ms": max(item["total_ms"] for item in observations),
    }


def _child(
    query: str,
    max_latency_seconds: float,
    max_routing_seconds: float,
    routing_regression: bool,
    ack_ordering: bool,
    max_ack_seconds: float,
) -> int:
    metrics = asyncio.run(
        _run_child(
            query,
            max_latency_seconds=max_latency_seconds,
            max_routing_seconds=max_routing_seconds,
            routing_regression=routing_regression,
            ack_ordering=ack_ordering,
            max_ack_seconds=max_ack_seconds,
        )
    )
    print(RESULT_PREFIX + json.dumps(metrics, sort_keys=True))
    return 0


def _parent(
    query: str,
    timeout: float,
    max_latency_seconds: float,
    max_routing_seconds: float,
    routing_regression: bool,
    ack_ordering: bool,
    max_ack_seconds: float,
) -> int:
    command = [
        sys.executable,
        __file__,
        "--child",
        "--query",
        query,
        "--max-latency-seconds",
        str(max_latency_seconds),
        "--max-routing-seconds",
        str(max_routing_seconds),
        "--max-ack-seconds",
        str(max_ack_seconds),
    ]
    if routing_regression:
        command.append("--routing-regression")
    if ack_ordering:
        command.append("--ack-ordering")
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        diagnostic = ((exc.stdout or "") + (exc.stderr or ""))[-8_000:]
        if diagnostic:
            print(diagnostic, file=sys.stderr)
        raise RuntimeError(f"conversation smoke exceeded {timeout:.1f}s") from exc
    if completed.returncode != 0:
        print((completed.stdout + completed.stderr)[-8_000:], file=sys.stderr)
        raise RuntimeError(f"conversation smoke exited with code {completed.returncode}")
    result_line = next(
        (line for line in completed.stdout.splitlines() if line.startswith(RESULT_PREFIX)),
        None,
    )
    if result_line is None:
        print((completed.stdout + completed.stderr)[-8_000:], file=sys.stderr)
        raise RuntimeError("conversation smoke returned no result metrics")
    metrics = json.loads(result_line.removeprefix(RESULT_PREFIX))
    if metrics.get("scenario") == "routing-regression":
        print(
            "routing regression smoke passed: "
            f"turns={metrics['turns']} "
            f"actions={','.join(metrics['actions'])} "
            f"workers={','.join(metrics['workers'])} "
            f"max_routing_ms={metrics['max_routing_ms']} "
            f"max_total_ms={metrics['max_total_ms']}"
        )
    elif metrics.get("scenario") == "ack-ordering":
        print(
            "ack-ordering smoke passed: "
            f"worker={metrics['worker']} "
            f"ack_ms={metrics['ack_ms']} "
            f"result_ms={metrics['result_ms']} "
            f"routing_ms={metrics['routing_ms']}"
        )
    else:
        print(
            "conversation smoke passed: "
            f"worker={metrics['worker']} "
            f"display_chars={metrics['display_chars']} "
            f"spoken_chars={metrics['spoken_chars']} "
            f"citations={metrics['citations']} "
            f"routing_ms={metrics['routing_ms']} "
            f"search_ms={metrics['search_ms']} "
            f"total_ms={metrics['total_ms']}"
        )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--max-latency-seconds", type=float, default=60)
    parser.add_argument("--max-routing-seconds", type=float, default=15)
    parser.add_argument(
        "--routing-regression",
        action="store_true",
        help="run the live Hi-then-weather routing regression sequence",
    )
    parser.add_argument(
        "--ack-ordering",
        action="store_true",
        help="prove the early ack is admitted for real speech before the delegated result",
    )
    parser.add_argument("--max-ack-seconds", type=float, default=15)
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not args.query.strip():
        parser.error("--query must not be empty")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.max_latency_seconds <= 0:
        parser.error("--max-latency-seconds must be positive")
    if args.max_routing_seconds <= 0:
        parser.error("--max-routing-seconds must be positive")
    if args.max_ack_seconds <= 0:
        parser.error("--max-ack-seconds must be positive")
    if args.ack_ordering and args.max_ack_seconds >= args.max_latency_seconds:
        parser.error("--max-ack-seconds must be less than --max-latency-seconds")
    if args.routing_regression and args.ack_ordering:
        parser.error("--routing-regression and --ack-ordering are mutually exclusive")
    if args.timeout <= args.max_latency_seconds:
        parser.error("--timeout must exceed --max-latency-seconds")
    return (
        _child(
            args.query,
            args.max_latency_seconds,
            args.max_routing_seconds,
            args.routing_regression,
            args.ack_ordering,
            args.max_ack_seconds,
        )
        if args.child
        else _parent(
            args.query,
            args.timeout,
            args.max_latency_seconds,
            args.max_routing_seconds,
            args.routing_regression,
            args.ack_ordering,
            args.max_ack_seconds,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
