#!/usr/bin/env python3
"""Phase 0 gate: verify candidate OpenAI model IDs and (model, effort, tools).

This is a standalone, paid-call verification script for the router/worker
model-comparison eval suite (see
``docs/dev_plans/20260817-feature-router-worker-model-eval-suite.md``). It
confirms, live against the OpenAI API, that:

- every candidate and baseline model ID actually exists (``models.retrieve``);
- every (model, effort) pair named in the plan's Objective candidate matrix is
  accepted using the SAME request-kwargs shape production sends -- the
  router's structured-output ``text`` format + ``store=False``
  (``server/router.py``'s ``LazyRouterProvider.__call__``), or the worker's
  ``tool_choice="required"`` + ``include`` + instructions + ``store=False``
  with the ``web_search`` tool attached (``server/workers/web_search.py``);
- the judge's Chat Completions path (``pipecat.evals.services.openai_service``)
  accepts a minimal call.

Every probe is a real, billed OpenAI API call unless ``--dry-run`` is passed,
in which case the full probe plan is printed and NO network call is made.

Results are written to a versioned JSON manifest. Phase 2's runner refuses to
run any (model, effort, tools) combination absent from, or stale relative to,
that manifest. A non-zero exit code means at least one candidate/effort/tools
combination was rejected -- do not proceed with implementation work that
assumes it would have been accepted.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.eval_common import (
    DEFAULT_JUDGE_MODEL,
    DEFAULT_MANIFEST_RELATIVE_PATH,
    MANIFEST_VERSION,
    REPO_ROOT,
    ROUTER_BASELINE,
    ROUTER_CANDIDATES,
    ROUTER_MANIFEST_TOOLS,
    WORKER_BASELINE,
    WORKER_CANDIDATES,
    WORKER_MANIFEST_TOOLS,
    build_judge_llm_service,
    confined_output_path,
    effective_effort_for_manifest_lookup,
    error_text,
    git_head,
    write_no_follow,
)

# MANIFEST_VERSION/DEFAULT_JUDGE_MODEL/REPO_ROOT hoisted from
# scripts/eval_common.py (round 5, Architecture lens finding 2; REPO_ROOT
# hoisted in round 7, Architecture finding 12): this producer and
# scripts/eval_model_comparison.py's consumer reference the same objects, not
# two independently-maintained "must match" copies.
# Tracked in git (not `.review-plan/`, which is gitignored) -- Phase 2's runner
# and CI both need to read this manifest from a fresh checkout.
DEFAULT_OUT = DEFAULT_MANIFEST_RELATIVE_PATH
_REPO_ROOT = REPO_ROOT

# Renamed from ROUTER_CANDIDATES/WORKER_CANDIDATES (round 5, Architecture
# lens finding 4): scripts/eval_model_comparison.py independently defines its
# own same-named ROUTER_CANDIDATES/WORKER_CANDIDATES with a different shape
# (tuple[Candidate, ...], baseline split out via ROUTER_BASELINE/
# WORKER_BASELINE) -- same domain concept, different type, same name is a
# naming hazard on its own regardless of any sync risk between the two.
# PHASE0_* makes clear these are this module's own probe-plan tuples.
#
# Derived from scripts.eval_common's ROUTER_BASELINE/ROUTER_CANDIDATES/
# WORKER_BASELINE/WORKER_CANDIDATES (round 7, Architecture finding 11) --
# this producer and scripts/eval_model_comparison.py's Phase 2 runner both
# need the same candidate matrix, and previously kept two independently-
# authored copies (this module's flat (model, effort) tuples vs. the
# runner's Candidate-based ones) reconciled only by convention. The
# baseline entries (router: gpt-5-mini @ minimal; worker: gpt-5 @ no
# reasoning param) still reproduce today's production behavior --
# effective_effort_for_manifest_lookup() resolves the router's
# "unset effort + gpt-5* model defaults to minimal" rule the same way
# server/router.py's LazyRouterProvider.__call__ does. `None` worker effort
# means "omit the `reasoning` kwarg entirely", not "resolve to a default".
PHASE0_ROUTER_PROBES: tuple[tuple[str, str | None], ...] = tuple(
    (c.model, effective_effort_for_manifest_lookup(c))
    for c in (ROUTER_BASELINE, *ROUTER_CANDIDATES)
)
PHASE0_WORKER_PROBES: tuple[tuple[str, str | None], ...] = tuple(
    (c.model, effective_effort_for_manifest_lookup(c))
    for c in (WORKER_BASELINE, *WORKER_CANDIDATES)
)


def _model_existence_ids(judge_model: str) -> tuple[str, ...]:
    """Every unique model ID appearing anywhere in either matrix, plus the judge
    model (which may be overridden via --judge-model), gets an existence check
    via `models.retrieve`."""
    ids = {model for model, _ in PHASE0_ROUTER_PROBES} | {
        model for model, _ in PHASE0_WORKER_PROBES
    }
    ids.add(judge_model)
    return tuple(sorted(ids))


ROUTER_PROBE_TRANSCRIPT = "Hi."
WORKER_PROBE_QUERY = "What is the current weather in London?"
JUDGE_PROBE_MESSAGE = "Reply with exactly the single word: ok"


@dataclass
class ProbeResult:
    """One verified (or rejected) tuple, ready to drop straight into the manifest."""

    kind: str  # "model_existence" | "router" | "worker" | "judge"
    model: str
    effort: str | None
    tools: list[str]
    request_kwargs: dict[str, Any]
    accepted: bool
    error: str | None
    response_id: str | None = None


def _router_timeout_seconds() -> float:
    """Read the real production default without hardcoding a second copy.

    ``Config()`` takes no required arguments and has no realistic failure
    path -- the prior ``DEFAULT_ROUTER_TIMEOUT_SECONDS`` hardcoded fallback +
    ``try/except Exception`` contradicted this function's own docstring
    ("without hardcoding a second copy") by keeping exactly that second copy
    on standby for a path that never runs; dropped, letting
    ``Config().router_timeout_seconds`` be the single source of truth (round
    7 gauntlet, Architecture finding 19b).
    """
    from server.config import Config

    return float(Config().router_timeout_seconds)


def _build_router_kwargs(
    model: str, effort: str | None, *, router_timeout_seconds: float
) -> dict[str, Any]:
    """Reproduce `LazyRouterProvider.__call__`'s request-kwargs shape exactly."""
    from server.router import RouterEnvelope, WorkerCatalogue, effective_router_reasoning_effort
    from server.structured_outputs import structured_text_format

    catalogue = WorkerCatalogue(
        version="verify-eval-candidates-1",
        workers=(),
        capability_labels=("public_web",),
        model_policies=("deep",),
    )
    prompt = catalogue.prompt(ROUTER_PROBE_TRANSCRIPT)
    kwargs: dict[str, Any] = {
        "model": model,
        "input": prompt,
        "store": False,
        "timeout": router_timeout_seconds,
        "text": structured_text_format(RouterEnvelope, "router_envelope"),
    }
    # effective_router_reasoning_effort(), not a bare `if effort is not
    # None`: the production call site (LazyRouterProvider.__call__) resolves
    # the wire-level effort through that helper (its own "unset effort + a
    # gpt-5* model defaults to minimal" rule), so a probe that bypassed it
    # would be a latent shape mismatch for any future PHASE0_ROUTER_PROBES
    # entry carrying an unset effort (round 5, Architecture lens finding 3).
    effective_effort = effective_router_reasoning_effort(model, effort)
    if effective_effort is not None:
        kwargs["reasoning"] = {"effort": effective_effort}
    return kwargs


def _build_worker_kwargs(model: str, effort: str | None) -> dict[str, Any]:
    """Reproduce `WebSearchWorker.search`'s request-kwargs shape exactly."""
    from server.structured_outputs import structured_text_format
    from server.workers.web_search import _WEB_SEARCH_INSTRUCTIONS, WebSearchAnswer

    kwargs: dict[str, Any] = {
        "model": model,
        "tools": [{"type": "web_search"}],
        "tool_choice": "required",
        "include": ["web_search_call.action.sources"],
        "instructions": _WEB_SEARCH_INSTRUCTIONS,
        "input": WORKER_PROBE_QUERY,
        "text": structured_text_format(WebSearchAnswer, "web_search_answer"),
        "store": False,
    }
    if effort is not None:
        kwargs["reasoning"] = {"effort": effort}
    return kwargs


def _judge_kwargs(judge_model: str) -> dict[str, Any]:
    return {
        "model": judge_model,
        "messages": [{"role": "user", "content": JUDGE_PROBE_MESSAGE}],
        "max_completion_tokens": 16,
    }


def build_plan(*, judge_model: str, router_timeout_seconds: float) -> list[dict[str, Any]]:
    """Build the full, deterministic list of probes this run will perform.

    Shared by --dry-run (printed, never executed) and a real run (executed in
    this order). Each entry's `request_kwargs` is the exact payload that would
    be sent to the provider for that tuple.
    """
    entries: list[dict[str, Any]] = []
    for model in _model_existence_ids(judge_model):
        entries.append({"kind": "model_existence", "model": model, "effort": None, "tools": []})
    for model, effort in PHASE0_ROUTER_PROBES:
        entries.append(
            {
                "kind": "router",
                "model": model,
                "effort": effort,
                "tools": list(ROUTER_MANIFEST_TOOLS),
                "request_kwargs": _build_router_kwargs(
                    model, effort, router_timeout_seconds=router_timeout_seconds
                ),
            }
        )
    for model, worker_effort in PHASE0_WORKER_PROBES:
        entries.append(
            {
                "kind": "worker",
                "model": model,
                "effort": worker_effort,
                "tools": list(WORKER_MANIFEST_TOOLS),
                "request_kwargs": _build_worker_kwargs(model, worker_effort),
            }
        )
    entries.append(
        {
            "kind": "judge",
            "model": judge_model,
            "effort": None,
            "tools": [],
            "request_kwargs": _judge_kwargs(judge_model),
        }
    )
    return entries


def _sync_responses_client(api_key: str) -> Any:
    from openai import OpenAI

    return OpenAI(api_key=api_key).responses


def _sync_models_client(api_key: str) -> Any:
    from openai import OpenAI

    return OpenAI(api_key=api_key).models


def probe_model_existence(models_client: Any, model: str, *, api_key: str | None) -> ProbeResult:
    try:
        models_client.retrieve(model)
    except Exception as exc:  # noqa: BLE001 - report every rejection reason, not just known types
        return ProbeResult(
            "model_existence",
            model,
            None,
            [],
            {"model": model},
            False,
            error_text(exc, credential=api_key),
        )
    return ProbeResult("model_existence", model, None, [], {"model": model}, True, None)


def probe_router(
    responses_client: Any,
    model: str,
    effort: str | None,
    *,
    router_timeout_seconds: float,
    api_key: str | None,
) -> ProbeResult:
    kwargs = _build_router_kwargs(model, effort, router_timeout_seconds=router_timeout_seconds)
    try:
        response = responses_client.create(**kwargs)
    except Exception as exc:  # noqa: BLE001
        return ProbeResult(
            "router",
            model,
            effort,
            list(ROUTER_MANIFEST_TOOLS),
            kwargs,
            False,
            error_text(exc, credential=api_key),
        )
    return ProbeResult(
        "router",
        model,
        effort,
        list(ROUTER_MANIFEST_TOOLS),
        kwargs,
        True,
        None,
        response_id=getattr(response, "id", None),
    )


def probe_worker(
    responses_client: Any, model: str, effort: str | None, *, api_key: str | None
) -> ProbeResult:
    kwargs = _build_worker_kwargs(model, effort)
    try:
        response = responses_client.create(**kwargs)
    except Exception as exc:  # noqa: BLE001
        return ProbeResult(
            "worker",
            model,
            effort,
            list(WORKER_MANIFEST_TOOLS),
            kwargs,
            False,
            error_text(exc, credential=api_key),
        )
    return ProbeResult(
        "worker",
        model,
        effort,
        list(WORKER_MANIFEST_TOOLS),
        kwargs,
        True,
        None,
        response_id=getattr(response, "id", None),
    )


def probe_judge(judge_model: str, api_key: str | None) -> ProbeResult:
    import asyncio

    kwargs = _judge_kwargs(judge_model)

    async def _call() -> Any:
        # build_judge_llm_service(), not pipecat.evals.services.openai_service():
        # that factory only reads config["model"] and otherwise relies on the
        # OpenAI SDK's own OPENAI_API_KEY env-var lookup, silently ignoring
        # the WEBSEARCH_OPENAI_API_KEY(_ENV)-resolved credential this probe
        # already went to the trouble of resolving via load_config().
        service = build_judge_llm_service(judge_model, api_key)
        # `service._client` reaches into a private pipecat attribute -- a
        # future pipecat rename would otherwise surface as a bare
        # AttributeError that the generic `except Exception` below would
        # misattribute to "judge model absent from manifest" (a rejected
        # probe) rather than "probe plumbing broke" (this script needs a
        # fix). Resolved separately, before the actual network call, so that
        # distinction survives into the reported error text (round 7
        # gauntlet, Architecture finding 19a).
        try:
            client = service._client
        except AttributeError as exc:
            raise RuntimeError(
                "judge probe plumbing broke: OpenAILLMService no longer exposes the "
                f"private `_client` attribute this probe relies on ({exc})"
            ) from exc
        return await client.chat.completions.create(**kwargs)

    try:
        response = asyncio.run(_call())
    except Exception as exc:  # noqa: BLE001
        return ProbeResult(
            "judge", judge_model, None, [], kwargs, False, error_text(exc, credential=api_key)
        )
    return ProbeResult(
        "judge",
        judge_model,
        None,
        [],
        kwargs,
        True,
        None,
        response_id=getattr(response, "id", None),
    )


# _git_head() hoisted to scripts/eval_common.py's git_head() (round 7
# gauntlet finding 12) -- was a near-identical copy of
# scripts/eval_model_comparison.py's own _current_source_commit(),
# cross-referencing it in a comment instead of sharing code. Call sites below
# use git_head() directly.


def _resolve_openai_api_key() -> str | None:
    """Resolve the OpenAI key the same way production does: TOML -> env-file ->
    process environment, honoring WEBSEARCH_OPENAI_API_KEY(_ENV) overrides --
    not a raw `os.environ["OPENAI_API_KEY"]` read, which would false-negative
    for an operator whose key only lives in config.toml or an env-file."""
    from server.config import load_config

    return load_config().openai_api_key


def run_verification(*, judge_model: str) -> tuple[list[ProbeResult], float]:
    """Execute every probe against the live API. Makes real, billed calls."""
    api_key = _resolve_openai_api_key()
    if not api_key:
        raise RuntimeError(
            "No OpenAI API key resolved via load_config() "
            "(checked config.toml, env-file, WEBSEARCH_OPENAI_API_KEY, and OPENAI_API_KEY); "
            "cannot run live verification probes"
        )

    router_timeout_seconds = _router_timeout_seconds()
    models_client = _sync_models_client(api_key)
    responses_client = _sync_responses_client(api_key)

    results: list[ProbeResult] = []
    for model in _model_existence_ids(judge_model):
        results.append(probe_model_existence(models_client, model, api_key=api_key))
    for model, effort in PHASE0_ROUTER_PROBES:
        results.append(
            probe_router(
                responses_client,
                model,
                effort,
                router_timeout_seconds=router_timeout_seconds,
                api_key=api_key,
            )
        )
    for model, worker_effort in PHASE0_WORKER_PROBES:
        results.append(probe_worker(responses_client, model, worker_effort, api_key=api_key))
    results.append(probe_judge(judge_model, api_key))
    return results, router_timeout_seconds


def write_manifest(out_path: Path, results: list[ProbeResult]) -> dict[str, Any]:
    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "source_commit": git_head(),
        "verified_at_utc": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "results": [asdict(result) for result in results],
    }
    # confined_output_path()/write_no_follow(): an operator-supplied --out
    # value is still attacker-influenced surface (see scripts/eval_common.py
    # for the rationale) -- reject `..` traversal escaping the repo tree and
    # refuse to follow an existing symlink at the target path.
    confined_path = confined_output_path(out_path, allowed_root=_REPO_ROOT)
    write_no_follow(confined_path, json.dumps(manifest, indent=2, sort_keys=False) + "\n")
    return manifest


def _print_dry_run(entries: list[dict[str, Any]]) -> None:
    print(json.dumps({"dry_run": True, "probe_count": len(entries), "probes": entries}, indent=2))


def _print_summary(results: list[ProbeResult]) -> None:
    rejected = [r for r in results if not r.accepted]
    for result in results:
        status = "OK" if result.accepted else "REJECTED"
        effort = result.effort if result.effort is not None else "(unset)"
        tools = ",".join(result.tools) if result.tools else "(none)"
        line = f"[{status}] {result.kind} model={result.model} effort={effort} tools={tools}"
        if not result.accepted:
            line += f" error={result.error}"
        print(line, file=sys.stderr if not result.accepted else sys.stdout)
    print(
        f"\n{len(results) - len(rejected)}/{len(results)} probes accepted"
        f"{'; ' + str(len(rejected)) + ' REJECTED' if rejected else ''}",
        file=sys.stderr if rejected else sys.stdout,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        default=DEFAULT_OUT,
        help=f"manifest output path (default: {DEFAULT_OUT})",
    )
    parser.add_argument(
        "--judge-model",
        default=DEFAULT_JUDGE_MODEL,
        help=f"cheap judge model to verify the Chat Completions path with (default: {DEFAULT_JUDGE_MODEL})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the full probe plan (models, efforts, exact request kwargs) and make zero network calls",
    )
    args = parser.parse_args()

    if args.dry_run:
        entries = build_plan(
            judge_model=args.judge_model, router_timeout_seconds=_router_timeout_seconds()
        )
        _print_dry_run(entries)
        return 0

    try:
        results, _ = run_verification(judge_model=args.judge_model)
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    # Printed BEFORE the write, not after: write_manifest() can raise OSError
    # (write_no_follow()'s target is a directory, unwritable, or a symlink --
    # ELOOP) as well as ValueError, and every result above is already a real,
    # billed probe outcome -- an operator must see it regardless of whether
    # the manifest write itself then fails.
    _print_summary(results)
    try:
        manifest = write_manifest(Path(args.out), results)
    except (ValueError, OSError) as exc:
        print(f"refusing to write manifest: {exc}", file=sys.stderr)
        return 2
    print(f"\nmanifest written to {args.out} (source_commit={manifest['source_commit']})")

    exit_code = 0
    if manifest["source_commit"] is None:
        # git_head() returning None (git unavailable, not a repo, or the
        # rev-parse itself failed) still let write_manifest() write a real,
        # already-billed manifest above -- that artifact must exist (the
        # probe outcomes are real and worth keeping), but a manifest with no
        # source_commit is unusable: scripts/eval_model_comparison.py's
        # manifest consumer treats a null source_commit as unverifiable and
        # therefore stale/rejected for any live run. Failing loudly here
        # (nonzero exit), instead of returning 0 as if verification fully
        # succeeded, is the only signal an operator gets that the manifest
        # they just paid for needs re-verification from a working git
        # checkout before Phase 2 can use it (round 7 gauntlet, Codex
        # adversarial gate finding 3).
        print(
            "\nWARNING: source_commit could not be resolved (git unavailable or this is not "
            "a git checkout) -- the manifest was written but is unusable until re-verified "
            "from a working git checkout: scripts/eval_model_comparison.py treats a null "
            "source_commit as stale/rejected for any live run.",
            file=sys.stderr,
        )
        exit_code = 2

    if any(not result.accepted for result in results):
        print(
            "\nOne or more candidate/effort/tools combinations were rejected. "
            "Do not proceed to Phase 1/2 implementation work that assumes these "
            "combinations are accepted.",
            file=sys.stderr,
        )
        exit_code = exit_code or 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
