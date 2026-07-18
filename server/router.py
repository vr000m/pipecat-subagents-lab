"""Tool-free routing and snapshot-bound policy validation."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping
from pydantic import BaseModel, ConfigDict
from .contracts import RoutingDecision


class RoutingValidationError(ValueError):
    """Model output is not valid for the supplied catalogue."""


class RouterEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")
    decision: RoutingDecision
    prose: str | None = None


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
            "Route this transcript using only this immutable catalogue. Do not use tools or "
            f"invent IDs. catalogue_version={self.version}; workers={self.workers!r}; "
            f"capabilities={self.capability_labels!r}; transcript={transcript}"
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
                (catalogue.model_policies and decision.model_policy not in catalogue.model_policies)
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

    def route(
        self, transcript: str, catalogue: WorkerCatalogue | tuple[WorkerCatalogueEntry, ...]
    ) -> RoutingDecision:
        if isinstance(catalogue, tuple):
            catalogue = WorkerCatalogue(
                "catalogue-1",
                catalogue,
                tuple(sorted({k for e in catalogue for k in e.capabilities})),
                tuple(
                    sorted({p for e in catalogue for p in (e.model_policies or {e.model_policy})})
                ),
            )
        self.last_prompt = catalogue.prompt(transcript)
        raw = (
            self.model.structured_output(transcript=transcript, catalogue=catalogue.workers)
            if self.model is not None
            else self._call(self.last_prompt)
        )
        if isinstance(raw, RouterEnvelope):
            envelope = raw
        else:
            payload = raw.model_dump() if isinstance(raw, RoutingDecision) else raw
            envelope = RouterEnvelope.model_validate(
                payload
                if isinstance(payload, dict) and "decision" in payload
                else {"decision": payload}
            )
        validate_decision(envelope.decision, catalogue)
        return envelope.decision


def catalogue_from_registry(registry: Any) -> WorkerCatalogue:
    return registry.catalogue()
