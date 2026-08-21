"""Tool-free routing and snapshot-bound policy validation."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError

from .config import Config, default_reasoning_effort_for_model
from .contracts import RoutingDecision
from .structured_outputs import structured_text_format


class RoutingValidationError(ValueError):
    """Model output is not valid for the supplied catalogue."""


class RouterEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")
    decision: RoutingDecision
    prose: str | None = None


def build_openai_responses_client(api_key: str, *, timeout: float = 75.0) -> Any:
    """Construct the synchronous Responses client used by the router."""
    from openai import OpenAI

    return OpenAI(api_key=api_key, timeout=timeout).responses


def build_openai_async_responses_client(api_key: str, *, timeout: float = 75.0) -> Any:
    """Construct the cancellable async Responses client used by workers."""
    from openai import AsyncOpenAI

    return AsyncOpenAI(api_key=api_key, timeout=timeout).responses


def effective_router_reasoning_effort(model: str, resolved_effort: str | None) -> str | None:
    """The reasoning-effort value ``LazyRouterProvider.__call__`` actually sends
    for the ROUTER.

    Router-specific: it layers the router's resolved-config precedence over
    the model-naming default. A caller that needs to *predict* the router's
    wire-level effort without making the call (the eval runner's
    manifest-lookup) uses this; a caller that only wants the model-naming
    default for some OTHER role must call
    ``default_reasoning_effort_for_model`` directly rather than borrowing this
    function's router contract (round 10 gauntlet, Architecture finding 4).
    """
    if resolved_effort is not None:
        return resolved_effort
    return default_reasoning_effort_for_model(model)


def build_router_request_kwargs(
    model: str, effort: str | None, *, prompt: str, timeout: float
) -> dict[str, Any]:
    """The exact request-kwargs shape sent to the Responses API for a router
    call.

    Hoisted out of ``LazyRouterProvider.__call__`` (round 8 gauntlet,
    Architecture finding 8) so ``scripts/verify_eval_candidates.py``'s probe
    builder can call this SAME function instead of hand-mirroring the shape
    independently -- a production kwarg change previously left the manifest
    verifier free to keep reproducing the OLD shape and re-certify it as
    still production-equivalent.
    """
    kwargs: dict[str, Any] = {
        "model": model,
        "input": prompt,
        "store": False,
        "timeout": timeout,
        "text": structured_text_format(RouterEnvelope, "router_envelope"),
    }
    effective_effort = effective_router_reasoning_effort(model, effort)
    if effective_effort is not None:
        kwargs["reasoning"] = {"effort": effective_effort}
    return kwargs


def _response_text(response: Any) -> str:
    if isinstance(response, Mapping):
        value = response.get("output_text")
    else:
        value = getattr(response, "output_text", None)
    if isinstance(value, str) and value:
        return value
    raise RoutingValidationError("router provider returned no structured output")


class LazyRouterProvider:
    """Create the configured Responses provider only when routing is first used."""

    def __init__(
        self,
        config: Config,
        responses_factory: Callable[[], Any] | None = None,
    ) -> None:
        self._config = config
        self._responses_factory = responses_factory
        self._responses: Any = None

    def __repr__(self) -> str:
        return f"{type(self).__name__}(initialized={self._responses is not None})"

    def _get_responses(self) -> Any:
        if self._responses is not None:
            return self._responses
        if self._responses_factory is not None:
            self._responses = self._responses_factory()
            return self._responses
        if not self._config.openai_api_key:
            raise RuntimeError(
                "router provider is unavailable; configure an OpenAI credential "
                "or inject a router provider"
            )
        self._responses = build_openai_responses_client(
            self._config.openai_api_key,
            timeout=self._config.provider_timeout_seconds,
        )
        return self._responses

    def __call__(self, prompt: str) -> dict[str, Any]:
        model = self._config.resolve_router_model("fast")
        effort = self._config.resolve_router_reasoning_effort("fast")
        kwargs = build_router_request_kwargs(
            model, effort, prompt=prompt, timeout=self._config.router_timeout_seconds
        )
        response = self._get_responses().create(**kwargs)
        try:
            payload = json.loads(_response_text(response))
        except json.JSONDecodeError as exc:
            raise RoutingValidationError("router provider returned invalid JSON") from exc
        if not isinstance(payload, dict):
            raise RoutingValidationError("router provider returned a non-object envelope")
        return payload


@dataclass(frozen=True)
class WorkerCatalogueEntry:
    worker_id: str
    worker_type: str
    topic: str
    status: str
    capabilities: Mapping[str, bool] = field(default_factory=dict)
    model_policies: frozenset[str] = frozenset()
    topic_summary: str = ""
    model_policy: str = "deep"


@dataclass(frozen=True)
class WorkerCatalogue:
    version: str
    workers: tuple[WorkerCatalogueEntry, ...]
    capability_labels: tuple[str, ...]
    model_policies: tuple[str, ...]

    @property
    def worker_ids(self) -> tuple[str, ...]:
        return tuple(w.worker_id for w in self.workers)

    @property
    def entries(self) -> tuple[WorkerCatalogueEntry, ...]:
        return self.workers

    def prompt(self, transcript: str) -> str:
        return (
            "Route the user transcript using only this immutable catalogue and these rules. "
            "Do not use tools. Do not invent existing worker IDs. "
            "Classify conversational intent before selecting a worker. Greetings, thanks, "
            "acknowledgements, goodbyes, and casual conversation that does not need external "
            "data are direct: return no worker fields and put a concise response in prose. "
            "Do not create a worker for those requests. "
            "A public factual, current, or historical request is available through the "
            "public_web capability. Select an existing matching worker only when its topic "
            "matches the request; otherwise return action=new_worker, worker_id=null, "
            "worker_type=web_search, "
            "capability=public_web, capability_available=true, and model_policy=deep. "
            "For existing_worker, copy worker_id, worker_type, topic, and model_policy "
            "verbatim from the matching catalogue entry. The topic is immutable worker "
            "metadata, not the current transcript; never change an existing worker's topic "
            "to match a new request. "
            "Use unsupported only for a capability that is genuinely unavailable, such as "
            "private account data. For direct, unsupported, or clarify, worker_id, worker_type, "
            "topic, and model_policy must all be null. "
            f"catalogue_version={self.version}; workers={self.workers!r}; "
            f"capabilities={self.capability_labels!r}; "
            f"model_policies={self.model_policies!r}; transcript={transcript}"
        )


def validate_decision(decision: RoutingDecision, catalogue: WorkerCatalogue) -> RoutingDecision:
    if (
        decision.catalogue_version != catalogue.version
        or tuple(decision.catalogue_worker_ids) != catalogue.worker_ids
    ):
        raise RoutingValidationError("routing decision does not match the catalogue snapshot")
    if decision.action in {"direct", "unsupported", "clarify"}:
        if any(getattr(decision, f) is not None for f in ("worker_id", "worker_type", "topic")):
            raise RoutingValidationError("non-worker routes cannot select a worker")
        return decision
    if decision.action == "new_worker":
        if decision.worker_id is not None and decision.worker_id in catalogue.worker_ids:
            raise RoutingValidationError("new-worker proposals cannot select an existing worker")
        if not catalogue.workers:
            if (
                decision.worker_type != "web_search"
                or (
                    catalogue.model_policies
                    and decision.model_policy not in catalogue.model_policies
                )
                or not decision.capability
                or (
                    catalogue.capability_labels
                    and decision.capability not in catalogue.capability_labels
                )
                or not decision.capability_available
            ):
                raise RoutingValidationError("first worker must request an available capability")
            return decision
        worker_types = {entry.worker_type for entry in catalogue.workers}
        if worker_types and decision.worker_type not in worker_types:
            raise RoutingValidationError("new worker type is not allowlisted")
        if (
            decision.model_policy not in catalogue.model_policies
            or decision.capability not in catalogue.capability_labels
            or not decision.capability_available
        ):
            raise RoutingValidationError("new worker policy or capability is not allowlisted")
        return decision

    entry = next((w for w in catalogue.workers if w.worker_id == decision.worker_id), None)
    if entry is None or decision.worker_type != entry.worker_type or decision.topic != entry.topic:
        raise RoutingValidationError("worker selection does not match the catalogue snapshot")
    policies = entry.model_policies or frozenset({entry.model_policy})
    if (
        decision.model_policy not in policies
        or decision.capability not in catalogue.capability_labels
    ):
        raise RoutingValidationError("worker policy or capability is not allowlisted")
    if (
        decision.capability_available != bool(entry.capabilities.get(decision.capability, False))
        or not decision.capability_available
    ):
        raise RoutingValidationError("capability is unavailable for the selected worker")
    return decision


class Router:
    def __init__(
        self, call: Callable[[str], Any] | None = None, config: Any = None, model: Any = None
    ) -> None:
        self._call, self.config, self.model = call, config, model
        self.last_prompt: str | None = None

    def route_envelope(
        self, transcript: str, catalogue: WorkerCatalogue | tuple[WorkerCatalogueEntry, ...]
    ) -> RouterEnvelope:
        if isinstance(catalogue, tuple):
            catalogue = WorkerCatalogue(
                "catalogue-1",
                catalogue,
                tuple(sorted({k for e in catalogue for k in e.capabilities})),
                tuple(
                    sorted({p for e in catalogue for p in (e.model_policies or {e.model_policy})})
                ),
            )
        prompt = catalogue.prompt(transcript)
        self.last_prompt = prompt
        raw = (
            self.model.structured_output(transcript=transcript, catalogue=catalogue.workers)
            if self.model is not None
            else self._call(prompt)
        )
        if isinstance(raw, RouterEnvelope):
            envelope = raw
        else:
            payload = raw.model_dump() if isinstance(raw, RoutingDecision) else raw
            payload = _complete_unambiguous_new_worker_fields(payload, transcript, catalogue)
            try:
                envelope = RouterEnvelope.model_validate(
                    payload
                    if isinstance(payload, dict) and "decision" in payload
                    else {"decision": payload}
                )
            except ValidationError as exc:
                raise RoutingValidationError("router returned an invalid routing decision") from exc
        validate_decision(envelope.decision, catalogue)
        return envelope

    def route(
        self, transcript: str, catalogue: WorkerCatalogue | tuple[WorkerCatalogueEntry, ...]
    ) -> RoutingDecision:
        return self.route_envelope(transcript, catalogue).decision


def _complete_unambiguous_new_worker_fields(
    payload: Any,
    transcript: str,
    catalogue: WorkerCatalogue,
) -> Any:
    """Fill only deterministic fields omitted by a structured new-worker route.

    OpenAI strict schemas can require nullable routing fields to be present, but
    cannot express that worker fields become non-null for one action. Preserve
    fail-closed validation for arbitrary values while completing the single
    worker type and policy this registry can actually execute.
    """
    if not isinstance(payload, dict):
        return payload
    container = payload.get("decision", payload)
    if not isinstance(container, dict) or container.get("action") != "new_worker":
        return payload

    decision = dict(container)
    worker_types = {entry.worker_type for entry in catalogue.workers} or {"web_search"}
    if decision.get("worker_type") is None and len(worker_types) == 1:
        decision["worker_type"] = next(iter(worker_types))
    if decision.get("model_policy") is None and len(catalogue.model_policies) == 1:
        decision["model_policy"] = catalogue.model_policies[0]
    if (
        decision.get("capability") is None
        and decision.get("worker_type") == "web_search"
        and "public_web" in catalogue.capability_labels
    ):
        decision["capability"] = "public_web"
    if decision.get("topic") is None:
        topic = " ".join(transcript.split()).strip()
        if topic:
            decision["topic"] = topic[:200]

    completed = dict(payload)
    if "decision" in payload:
        completed["decision"] = decision
    else:
        completed = decision
    return completed


def catalogue_from_registry(registry: Any) -> WorkerCatalogue:
    return registry.catalogue()
