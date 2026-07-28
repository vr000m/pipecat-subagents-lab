#!/usr/bin/env python3
"""Start the real server and verify its credential-free HTTP boundary."""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_CONTRACT = "v1.0"


class SmokeFailure(RuntimeError):
    """Raised when the server does not satisfy a smoke invariant."""


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _request(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    expected_status: int = 200,
) -> tuple[int, bytes]:
    request = Request(url, headers=headers or {})
    try:
        with urlopen(request, timeout=2) as response:
            status = response.status
            body = response.read()
    except HTTPError as exc:
        status = exc.code
        body = exc.read()
    if status != expected_status:
        raise SmokeFailure(f"{url} returned HTTP {status}, expected {expected_status}")
    return status, body


def _json(body: bytes, *, source: str) -> dict[str, Any]:
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SmokeFailure(f"{source} did not return valid JSON") from exc
    if not isinstance(value, dict):
        raise SmokeFailure(f"{source} did not return a JSON object")
    return value


def _wait_until_ready(process: subprocess.Popen[str], base_url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise SmokeFailure(f"server exited before becoming ready (code {process.returncode})")
        try:
            _request(f"{base_url}/api/healthz")
            return
        except (SmokeFailure, URLError, TimeoutError):
            time.sleep(0.1)
    raise SmokeFailure(f"server did not become ready within {timeout:.1f}s")


def _verify_server(base_url: str) -> None:
    health = _json(_request(f"{base_url}/api/healthz")[1], source="/api/healthz")
    if health != {"status": "ok", "transport": "smallwebrtc"}:
        raise SmokeFailure(f"unexpected health response: {health!r}")

    index = _request(f"{base_url}/")[1]
    if b"Pipecat Subagents Lab" not in index or b"/dist/app.js" not in index:
        raise SmokeFailure("browser index is missing the application shell or bundle reference")

    styles = _request(f"{base_url}/styles.css")[1]
    if b".live-grid" not in styles:
        raise SmokeFailure("browser stylesheet is missing expected application styles")

    bundle = _request(f"{base_url}/dist/app.js")[1]
    if len(bundle) < 10_000 or b"PipecatClient" not in bundle:
        raise SmokeFailure("browser bundle is missing or does not contain the Pipecat client")

    session = _json(
        _request(f"{base_url}/api/session", headers={"Origin": base_url})[1],
        source="/api/session",
    )
    required = {
        "contract_version",
        "session_id",
        "resume_token",
        "proposed_epoch",
        "snapshot_sequence",
    }
    if set(session) != required:
        raise SmokeFailure(f"session handshake fields differ: {sorted(session)}")
    if session["contract_version"] != EXPECTED_CONTRACT:
        raise SmokeFailure(f"unexpected contract version: {session['contract_version']!r}")
    if not session["session_id"] or not session["resume_token"]:
        raise SmokeFailure("session handshake returned an empty identity or token")
    if session["proposed_epoch"] < 1 or session["snapshot_sequence"] < 0:
        raise SmokeFailure("session handshake returned an invalid epoch or sequence")

    _request(
        f"{base_url}/api/session",
        headers={"Origin": "https://evil.example"},
        expected_status=403,
    )
    _request(
        f"{base_url}/api/session",
        headers={"Sec-Fetch-Site": "same-origin"},
    )


def _lifecycle_lines(output: str) -> list[str]:
    markers = (
        "Application startup complete",
        "Uvicorn running on",
        "Application shutdown complete",
    )
    return [line for line in output.splitlines() if any(marker in line for marker in markers)]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0, help="loopback port; 0 chooses a free port")
    parser.add_argument("--startup-timeout", type=float, default=20)
    args = parser.parse_args()
    if not 0 <= args.port <= 65_535:
        parser.error("--port must be between 0 and 65535")
    if args.startup_timeout <= 0:
        parser.error("--startup-timeout must be positive")

    bundle = ROOT / "web" / "dist" / "app.js"
    if not bundle.is_file():
        raise SmokeFailure("web/dist/app.js is missing; run `cd web && bun run build` first")

    port = args.port or _free_loopback_port()
    base_url = f"http://127.0.0.1:{port}"
    env = os.environ.copy()
    env.update(
        {
            "WEBSEARCH_BIND_HOST": "127.0.0.1",
            "WEBSEARCH_BIND_PORT": str(port),
            "WEBSEARCH_KNOWN_CLIENT_URL": base_url,
        }
    )
    process = subprocess.Popen(
        [sys.executable, "-m", "server.app"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    failure: BaseException | None = None
    try:
        _wait_until_ready(process, base_url, args.startup_timeout)
        _verify_server(base_url)
    except BaseException as exc:  # noqa: BLE001  # intentional catch-all: must capture Ctrl-C/SystemExit too so the subprocess is always cleaned up, then re-raised below
        failure = exc
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        try:
            output, _ = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            output, _ = process.communicate()
            failure = failure or SmokeFailure("server did not shut down within 10 seconds")

    if process.returncode not in {0, 130}:
        failure = failure or SmokeFailure(f"server exited with code {process.returncode}")
    if "Traceback (most recent call last)" in output:
        failure = failure or SmokeFailure("server log contains a Python traceback")
    lifecycle = _lifecycle_lines(output)
    if len(lifecycle) != 3:
        failure = failure or SmokeFailure("server log is missing startup or shutdown evidence")

    if failure is not None:
        print("server smoke failed", file=sys.stderr)
        print(output[-8_000:], file=sys.stderr)
        raise failure

    for line in lifecycle:
        print(line)
    print("server smoke passed: health, assets, origin policy, session handshake, clean shutdown")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
