#!/usr/bin/env python3
"""Run one bounded, paid router-to-web-search conversation smoke."""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
from typing import Any


DEFAULT_QUERY = "What are the most important Pipecat releases or announcements in July 2026?"
RESULT_PREFIX = "SMOKE_RESULT="
SAFE_FALLBACKS = {
    "Routing is temporarily unavailable. Please try that request again.",
    "The web search is temporarily unavailable.",
    "I could not find a reliable result for that request.",
}


async def _run_child(query: str) -> dict[str, Any]:
    from server.app import _default_session_host

    host = _default_session_host()
    # This smoke isolates the paid semantic path. Browser media and local speech
    # have separate deterministic/live acceptance commands.
    host.stt = None
    host.tts = None
    await host.start()
    try:
        connection = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 1,
                "snapshot_sequence": 0,
            }
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
        return {
            "worker": result.worker_id,
            "display_chars": len(result.ui_text),
            "spoken_chars": len(result.spoken_text),
            "citations": len(result.citations),
            "elapsed_ms": round(elapsed_ms, 1),
        }
    finally:
        await host.shutdown()


def _child(query: str) -> int:
    metrics = asyncio.run(_run_child(query))
    print(RESULT_PREFIX + json.dumps(metrics, sort_keys=True))
    return 0


def _parent(query: str, timeout: float) -> int:
    command = [sys.executable, __file__, "--child", "--query", query]
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
    print(
        "conversation smoke passed: "
        f"worker={metrics['worker']} "
        f"display_chars={metrics['display_chars']} "
        f"spoken_chars={metrics['spoken_chars']} "
        f"citations={metrics['citations']} "
        f"elapsed_ms={metrics['elapsed_ms']}"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not args.query.strip():
        parser.error("--query must not be empty")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    return _child(args.query) if args.child else _parent(args.query, args.timeout)


if __name__ == "__main__":
    raise SystemExit(main())
