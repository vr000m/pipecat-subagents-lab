"""Process-lifetime registry for persistent context-owning workers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .config import Config
from .router import WorkerCatalogue, WorkerCatalogueEntry
from .workers.base import ContextWorker, WorkerMetadata
from .workers.web_search import WebSearchWorker


class _UnavailableResponses:
    def create(self, **_: Any) -> Any:
        raise RuntimeError("web-search provider is unavailable; inject a Responses client")


class UnsupportedWorkerType(ValueError):
    """The first-slice registry cannot execute the requested worker type."""


@dataclass(frozen=True)
class RegisteredWorker:
    metadata: WorkerMetadata
    worker: ContextWorker

    @property
    def worker_id(self) -> str:
        return self.metadata.worker_id

    @property
    def context(self) -> ContextWorker:
        return self.worker


@dataclass(frozen=True)
class RegistryPolicy:
    eviction_enabled: bool = False


class WorkerRegistry:
    """Stable worker IDs with an explicit no-eviction policy."""

    def __init__(
        self,
        config: Config | None = None,
        worker_factory: Callable[[str], ContextWorker] | None = None,
        responses: Any = None,
    ) -> None:
        self.config = config or Config()
        self.worker_factory = worker_factory
        self.responses = responses
        self.policy = RegistryPolicy()
        self._workers: dict[str, RegisteredWorker] = {}
        self._version = 0

    @property
    def workers(self) -> tuple[RegisteredWorker, ...]:
        return tuple(self._workers.values())

    def register(
        self,
        *,
        worker_id: str,
        worker_type: str,
        topic: str,
        topic_summary: str = "",
        model_policy: str = "deep",
        capabilities: dict[str, bool] | None = None,
        worker: ContextWorker | None = None,
    ) -> RegisteredWorker:
        if worker_type != "web_search":
            raise UnsupportedWorkerType(f"unsupported worker type: {worker_type}")
        if worker_id in self._workers:
            raise ValueError(f"worker ID already registered: {worker_id}")
        self.config.resolve_worker_model(model_policy)
        metadata = WorkerMetadata(
            worker_id, worker_type, topic, topic_summary, model_policy, capabilities or {}
        )
        registered = RegisteredWorker(metadata, worker or ContextWorker(metadata))
        self._workers[worker_id] = registered
        self._version += 1
        return registered

    def get(self, worker_id: str) -> RegisteredWorker:
        try:
            return self._workers[worker_id]
        except KeyError as exc:
            raise ValueError(f"unknown worker: {worker_id}") from exc

    def get_or_create(self, *, topic: str, worker_type: str, model_policy: str) -> RegisteredWorker:
        if worker_type != "web_search":
            raise UnsupportedWorkerType(f"unsupported worker type: {worker_type}")
        for item in self._workers.values():
            if item.metadata.topic == topic and item.metadata.worker_type == worker_type:
                if item.metadata.model_policy != model_policy:
                    raise ValueError(
                        "existing worker has an incompatible model policy; refusing to reuse it"
                    )
                return item
        worker: ContextWorker | None = None
        if self.worker_factory is not None:
            worker = self.worker_factory(f"worker-{len(self._workers) + 1}")
        elif worker_type == "web_search":
            if self.responses is None:
                if self.config.openai_api_key:
                    from openai import OpenAI

                    self.responses = OpenAI(api_key=self.config.openai_api_key).responses
                else:
                    self.responses = _UnavailableResponses()
            worker = WebSearchWorker(
                model=self.config.resolve_worker_model(model_policy),
                model_policy=model_policy,
                responses=self.responses,
                worker_id=f"worker-{len(self._workers) + 1}",
                topic=topic,
            )
        worker_capabilities = getattr(getattr(worker, "metadata", None), "capabilities", None)
        if worker_capabilities is None:
            worker_capabilities = getattr(worker, "capabilities", {})
        return self.register(
            worker_id=f"worker-{len(self._workers) + 1}",
            worker_type=worker_type,
            topic=topic,
            model_policy=model_policy,
            capabilities=dict(worker_capabilities),
            worker=worker,
        )

    def validate_selection(self, snapshot: WorkerCatalogue, **kwargs: object) -> None:
        from .contracts import RoutingDecision
        from .router import validate_decision

        entry = next(e for e in snapshot.entries if e.worker_id == kwargs["worker_id"])
        validate_decision(
            RoutingDecision(
                action="existing_worker",
                worker_id=str(kwargs["worker_id"]),
                worker_type=str(kwargs["worker_type"]),
                topic=entry.topic,
                capability=str(kwargs["capability"]),
                capability_available=True,
                model_policy=str(kwargs["model_policy"]),
                catalogue_version=snapshot.version,
                catalogue_worker_ids=snapshot.worker_ids,
            ),
            snapshot,
        )

    def catalogue(self) -> WorkerCatalogue:
        entries = tuple(
            WorkerCatalogueEntry(
                worker_id=item.metadata.worker_id,
                worker_type=item.metadata.worker_type,
                topic=item.metadata.topic,
                topic_summary=item.metadata.topic_summary,
                status=getattr(item.worker, "status", "idle"),
                capabilities=dict(item.metadata.capabilities),
                model_policies=frozenset({item.metadata.model_policy}),
                model_policy=item.metadata.model_policy,
            )
            for item in self._workers.values()
        )
        labels = {label for item in entries for label in item.capabilities} or {"public_web"}
        return WorkerCatalogue(
            version=f"catalogue-{self._version}",
            workers=entries,
            capability_labels=tuple(sorted(labels)),
            model_policies=tuple(sorted(self.config.worker_model_policy)),
        )

    def remove(self, worker_id: str) -> None:
        raise RuntimeError("worker eviction is disabled by the first-slice registry policy")
