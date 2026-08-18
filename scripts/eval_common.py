"""Shared eval-suite plumbing: per-run session construction and hoisted metric helpers.

Hoisted out of ``scripts/smoke_conversation.py`` (which now re-exports these
names for backward compatibility with existing imports, including
``tests/test_smoke_conversation.py``) so ``scripts/eval_model_comparison.py``
and ``scripts/smoke_conversation.py`` can share the same session-construction
(``build_session_for_run``) and latency-measurement (``CollectingMeasurementSink``,
``latest_turn_stage_metrics``) helpers without importing a script module as a
library -- the layering this module exists for in the first place. The
``DEFAULT_QUERY``/``ROUTING_REGRESSION_QUERIES`` scenario constants live in
``evals/queries.py``; ``evals/scenarios.py`` imports them from there directly,
and this module re-exports them only for the same script-import
backward-compatibility reason as the rest of its ``__all__``.

This module lives at ``scripts/eval_common.py`` (not the leading-underscore
``scripts/_eval_common.py`` this repo used before round 5 of the review
gauntlet) -- three separate consumers (``scripts/smoke_conversation.py``,
``scripts/eval_model_comparison.py``, ``scripts/verify_eval_candidates.py``)
import it as a shared public seam, which a package-private-by-convention name
contradicted. ``scripts/_eval_common.py`` still exists as a one-line
backward-compatibility re-export shim for any caller (chiefly existing tests)
still importing the old path; new code should import from here directly.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

from evals.queries import DEFAULT_QUERY, ROUTING_REGRESSION_QUERIES
from server.composition import build_session_host
from server.config import Config, PromotionManifest
from server.perf_metrics import CollectingMeasurementSink
from server.pipeline import SAFE_FALLBACK_TEXTS, TIMEOUT_FALLBACK_TEXTS, SessionHost

__all__ = [
    "DEFAULT_JUDGE_MODEL",
    "DEFAULT_MANIFEST_RELATIVE_PATH",
    "DEFAULT_QUERY",
    "MANIFEST_VERSION",
    "ROUTER_MANIFEST_TOOLS",
    "ROUTING_REGRESSION_QUERIES",
    "SAFE_FALLBACKS",
    "TIMEOUT_FALLBACKS",
    "WORKER_MANIFEST_TOOLS",
    "CollectingMeasurementSink",
    "build_judge_llm_service",
    "build_session_for_run",
    "confined_output_path",
    "error_text",
    "latest_turn_stage_metrics",
    "strip_control_chars",
    "write_no_follow",
]
# NOTE: ``_latest_turn_stage_metrics`` (leading underscore) is deliberately
# NOT in ``__all__`` -- a "private by convention" name has no business in a
# module's declared public-export list, which is the self-contradiction round
# 5's Architecture lens flagged (finding 6). The canonical public name is
# ``latest_turn_stage_metrics`` above; ``_latest_turn_stage_metrics`` remains
# defined below as a plain backward-compatible alias (importable by explicit
# name, just not via ``from scripts.eval_common import *``) since existing
# call sites and tests (``scripts/smoke_conversation.py``,
# ``scripts/eval_model_comparison.py``, ``tests/test_eval_model_comparison.py``,
# ``tests/test_smoke_conversation.py``) already monkeypatch/import it under
# the old name -- renaming every one of those call sites was judged
# disproportionate churn for a Minor finding.

# Light redaction for a low-severity but real risk: an OpenAI client
# exception's str() can, under a misconfigured proxy or an unusual error
# path, echo back request metadata -- including an Authorization header or
# API key -- and this text can end up written verbatim into a git-tracked
# manifest or report. Not a complete secrets scanner, just a defense-in-depth
# backstop. Hoisted here (originally scripts/verify_eval_candidates.py-only)
# so scripts/eval_model_comparison.py's parallel error paths get the same
# protection -- see the dev plan's round-2 gauntlet findings 4/5.
_API_KEY_PATTERN = re.compile(r"sk-[A-Za-z0-9_-]{10,}")

# ASCII control characters (excluding newline/tab, which are legitimate in
# multi-line provider error text) that a raw provider-exception body could
# carry -- e.g. ANSI escape sequences -- and that would otherwise reach an
# operator's terminal unmodified via a raw ``print()`` sink. Defense-in-depth
# only: this is not a prompt-injection defense (see
# scripts/eval_model_comparison.py's ``_sanitize_for_judge``, which reuses
# this same filter for the judge-context sink), just a bound on how much raw
# control-character content reaches a terminal or git-tracked artifact
# unmodified. See round-4 gauntlet finding 7.
#
# Widened in round 5 (Security lens finding 8, optional-but-low-risk) beyond
# ASCII C0 + DEL to also strip the C1 control range (U+0080-U+009F) and the
# Unicode bidirectional-override code points (U+202A-U+202E, U+2066-U+2069):
# mainstream terminals honor both, and either could visually reorder an
# operator-facing error/report line sourced from a provider error body.
_CONTROL_CHAR_PATTERN = re.compile(
    r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f\u202a-\u202e\u2066-\u2069]"
)

_REPO_ROOT = Path(__file__).resolve().parents[1]

# Manifest producer/consumer contract, hoisted here (round 5, Architecture
# lens finding 2) so scripts/verify_eval_candidates.py (the producer) and
# scripts/eval_model_comparison.py (the consumer) reference one shared
# definition instead of two independently-maintained "must match" copies of
# the manifest schema version, default path, judge model, and per-role tools
# vocabulary.
MANIFEST_VERSION = 1
# Relative to the repo root; tracked in git (not `.review-plan/`, which is
# gitignored) -- both the verifier and the runner need to read this manifest
# from a fresh checkout.
DEFAULT_MANIFEST_RELATIVE_PATH = "docs/dev_plans/artifacts/eval-candidates-manifest.json"
DEFAULT_JUDGE_MODEL = "gpt-5-mini"
# The `tools` token each role's manifest entry is expected to carry --
# verify_eval_candidates.py's build_plan() emits these, and
# eval_model_comparison.py's load_manifest_status() re-checks membership
# against them before trusting an entry.
ROUTER_MANIFEST_TOOLS: tuple[str, ...] = ("text",)
WORKER_MANIFEST_TOOLS: tuple[str, ...] = ("web_search",)

# DEFAULT_QUERY/ROUTING_REGRESSION_QUERIES: re-exported from evals.queries
# (not defined here) so the scripts <-> evals dependency stays
# one-directional (scripts -> evals -> server), not the two-way coupling a
# local definition here plus evals/scenarios.py importing it back would
# create. scripts/smoke_conversation.py re-exports these names in turn, for
# backward compatibility with existing imports (including
# tests/test_smoke_conversation.py).
# Re-exported (not re-typed) from server.pipeline.SAFE_FALLBACK_TEXTS -- see
# round-2 gauntlet finding 9: a hand-duplicated copy here would silently
# desync from a wording change at any of pipeline.py's safe-fallback
# `text=` call sites, turning this guard into a no-op false PASS.
SAFE_FALLBACKS = SAFE_FALLBACK_TEXTS
# Re-exported (not re-typed) from server.pipeline.TIMEOUT_FALLBACK_TEXTS, for
# the same reason as SAFE_FALLBACKS above -- kept as its own set (not folded
# into SAFE_FALLBACKS) because a timeout placeholder is a distinct failure
# mode ("too slow", not "broken") a caller wants to score separately. See
# round-4 gauntlet finding 2.
TIMEOUT_FALLBACKS = TIMEOUT_FALLBACK_TEXTS


def latest_turn_stage_metrics(sink: Any, elapsed_ms: float, turn_id: str) -> dict[str, float]:
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


# Backward-compatible alias -- not in __all__ (see the note above it). Existing
# call sites (scripts/smoke_conversation.py, scripts/eval_model_comparison.py)
# and tests monkeypatch/import this old leading-underscore name directly.
_latest_turn_stage_metrics = latest_turn_stage_metrics


def build_session_for_run(
    config: Config,
    *,
    measurement_sink: Any | None = None,
    router_responses_factory: Callable[[], Any] | None = None,
    worker_responses: Any = None,
    promotion_manifest: PromotionManifest | None = None,
    stt: Any | None = None,
    tts: Any | None = None,
) -> SessionHost:
    """Construct a fresh Router/WorkerRegistry/WorkItemCoordinator/SessionHost,
    all bound to this exact per-run ``Config`` at construction time.

    Thin wrapper around ``server.composition.build_session_host()`` -- the
    single composition root also used by ``server.app._default_session_host()``
    (round 5, Architecture lens finding 1; see that module's docstring for
    the full rationale). Deliberately does **not** go through
    ``server.app._default_session_host()`` itself (which internally calls
    ``load_config()`` from the environment) or a post-hoc
    ``host.config = tuned`` reassignment: the router provider
    (``LazyRouterProvider``) captures its ``Config`` reference at construction
    and resolves the model/effort from that captured reference at call time,
    so a post-hoc reassignment of ``host.config`` never reaches it. Every
    caller that needs to vary the router/worker model+effort per run
    therefore needs its own ``Router``/``LazyRouterProvider`` constructed from
    *this* run's ``Config`` -- see the dev plan's "Architecture & Call Flow /
    Injection seam" note. ``scripts/smoke_conversation.py``'s ``_run_child``
    is migrated onto this same seam (rather than keeping its own now-obsolete
    post-hoc reassignment idiom) so the two callers never drift apart.

    ``promotion_manifest`` mirrors ``server.app._default_session_host()``'s own
    ``load_promotion_manifest(config)`` -> ``SessionHost(..., promotion_manifest=...)``
    wiring: omitting it leaves ``SessionHost._promotion_eligible`` permanently
    ``False`` (fail-closed to the ``"display_only"`` late-delivery disposition
    regardless of the caller's actual manifest), which is correct for a caller
    that has no manifest to give but silently wrong for one that does -- a
    caller exercising the promotion-eligible path (e.g. the ack-ordering
    smoke) must pass its own resolved manifest through explicitly.

    Text-only by default: ``host.stt``/``host.tts`` default to ``None`` here,
    since every eval/smoke scenario built on this helper drives turns
    directly through ``_handle_transcript`` with no audio in or out. A caller
    that needs real STT/TTS (e.g. the ack-ordering smoke's recording TTS)
    passes it through the ``stt``/``tts`` keywords -- bound here, at
    construction, rather than via a post-hoc ``host.tts = ...`` reassignment,
    since ``SessionHost.__init__`` derives ``self._tts_on_event`` from the
    constructor's ``tts`` argument only; a reassignment after construction
    leaves that derived state stale.
    """
    return build_session_host(
        config,
        router_responses_factory=router_responses_factory,
        worker_responses=worker_responses,
        measurement_sink=measurement_sink,
        promotion_manifest=promotion_manifest,
        stt=stt,
        tts=tts,
    )


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


def strip_control_chars(text: str) -> str:
    """Strip ASCII control characters (other than newline/tab) from ``text``.

    Shared by ``error_text`` below and
    ``scripts/eval_model_comparison.py``'s ``_sanitize_for_judge`` -- both
    sinks receive raw, externally-sourced (provider/exception) text and need
    the same bound on control-character content, so the filter lives here
    once rather than as two independently-maintained copies. See round-4
    gauntlet finding 7.
    """
    return _CONTROL_CHAR_PATTERN.sub("", text)


def error_text(exc: Exception, *, credential: str | None = None, max_len: int = 2000) -> str:
    """Redact-then-truncate an exception's text before it reaches a git-tracked
    manifest/report or stdout.

    Two redaction passes, in this order: (1) an exact substring replace on
    ``credential`` -- the actual resolved API key/token, when the caller has
    it in scope -- which is shape-independent and catches any configured
    credential form (an Azure OpenAI key, a gateway/proxy token, etc.), not
    just OpenAI's own ``sk-``-prefixed shape; (2) a fallback pattern match for
    the ``sk-...`` shape, in case ``credential`` wasn't available at the call
    site or the leaked text names a *different* credential than the one this
    call resolved (e.g. a proxy's own upstream key echoed back in an error).
    Redaction runs before truncation, not after, so a credential split across
    the truncation boundary can't leave a redactable fragment behind. Control
    characters (e.g. ANSI escape sequences a provider error body could carry)
    are stripped in the same pass, before truncation, for the same reason --
    every other sink already escapes/represents these safely
    (``compute_pass_fail`` via ``!r``, ``json.dumps``), but this function's
    two callers (``eval_model_comparison.py``'s summary print,
    ``verify_eval_candidates.py``'s ``_print_summary``) write raw text
    straight to a terminal.
    """
    text = f"{type(exc).__name__}: {exc}"
    if credential:
        text = text.replace(credential, "***REDACTED***")
    text = _API_KEY_PATTERN.sub("sk-***REDACTED***", text)
    text = strip_control_chars(text)
    return text[:max_len]


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
    closes that window for the final path component only: it guarantees this
    call won't follow a symlink planted at ``path`` itself between the check
    and this write, but does not protect against a *parent* directory being
    swapped for a symlink in that same window (this would require walking
    ``path.parent`` component-by-component with
    ``os.open(..., O_DIRECTORY | O_NOFOLLOW)`` and a ``dir_fd``-relative
    final open, which this function does not do). That residual window
    requires local write access to the repo tree during the run to exploit.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o644)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(content)
