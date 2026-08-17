"""Process-lifetime registry for persistent context-owning workers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from threading import RLock
from typing import Any, Protocol, cast

from .config import Config
from .router import (
    WorkerCatalogue,
    WorkerCatalogueEntry,
    build_openai_async_responses_client,
)
from .workers.base import WorkerMetadata
from .workers.web_search import WebSearchWorker


class _UnavailableResponses:
    def create(self, **_: Any) -> Any:
        raise RuntimeError("web-search provider is unavailable; inject a Responses client")


class UnsupportedWorkerType(ValueError):
    """The first-slice registry cannot execute the requested worker type."""


class WebSearchCapableWorker(Protocol):
    """Minimum execution contract for workers published as web search."""

    def search(self, query: str, **kwargs: Any) -> Awaitable[Any]: ...


@dataclass(frozen=True)
class RegisteredWorker:
    metadata: WorkerMetadata
    worker: WebSearchCapableWorker

    @property
    def worker_id(self) -> str:
        return self.metadata.worker_id

    @property
    def context(self) -> WebSearchCapableWorker:
        return self.worker


@dataclass(frozen=True)
class RegistryPolicy:
    eviction_enabled: bool = False


class WorkerRegistry:
    """Stable worker IDs with an explicit no-eviction policy."""

    def __init__(
        self,
        config: Config | None = None,
        worker_factory: Callable[[str], WebSearchCapableWorker] | None = None,
        responses: Any = None,
    ) -> None:
        self.config = config or Config()
        self.worker_factory = worker_factory
        self.responses = responses
        self.policy = RegistryPolicy()
        self._workers: dict[str, RegisteredWorker] = {}
        self._version = 0
        self._lock = RLock()

    @property
    def workers(self) -> tuple[RegisteredWorker, ...]:
        with self._lock:
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
        worker: WebSearchCapableWorker | None = None,
    ) -> RegisteredWorker:
        with self._lock:
            if worker_type != "web_search":
                raise UnsupportedWorkerType(f"unsupported worker type: {worker_type}")
            if worker_id in self._workers:
                raise ValueError(f"worker ID already registered: {worker_id}")
            self.config.resolve_worker_model(model_policy)
            metadata = WorkerMetadata(
                worker_id, worker_type, topic, topic_summary, model_policy, capabilities or {}
            )
            if worker is None:
                worker = self._create_web_search_worker(metadata)
            worker = self._require_web_search_capability(worker)
            self._require_matching_identity(worker, metadata)
            if capabilities is None:
                worker_capabilities = getattr(worker.metadata, "capabilities", None)
                if worker_capabilities is None:
                    worker_capabilities = getattr(worker, "capabilities", {})
                metadata = WorkerMetadata(
                    worker_id,
                    worker_type,
                    topic,
                    topic_summary,
                    model_policy,
                    dict(worker_capabilities),
                )
            registered = RegisteredWorker(metadata, worker)
            self._workers[worker_id] = registered
            self._version += 1
            return registered

    @staticmethod
    def _require_web_search_capability(worker: object) -> WebSearchCapableWorker:
        if not callable(getattr(worker, "search", None)):
            raise TypeError("web_search worker must define a callable search method")
        return cast(WebSearchCapableWorker, worker)

    @staticmethod
    def _require_matching_identity(
        worker: WebSearchCapableWorker, expected: WorkerMetadata
    ) -> None:
        metadata = getattr(worker, "metadata", None)
        if metadata is None:
            raise TypeError("web_search worker must define metadata")
        for field in ("worker_id", "worker_type", "topic", "model_policy"):
            actual = getattr(metadata, field, None)
            wanted = getattr(expected, field)
            if actual != wanted:
                raise ValueError(
                    f"web_search worker metadata mismatch for {field}: "
                    f"expected {wanted!r}, got {actual!r}"
                )

    def _create_web_search_worker(self, metadata: WorkerMetadata) -> WebSearchCapableWorker:
        if self.worker_factory is not None:
            return self._require_web_search_capability(self.worker_factory(metadata.worker_id))
        if self.responses is None:
            if self.config.openai_api_key:
                self.responses = build_openai_async_responses_client(
                    self.config.openai_api_key,
                    timeout=self.config.provider_timeout_seconds,
                )
            else:
                self.responses = _UnavailableResponses()
        return WebSearchWorker(
            model=self.config.resolve_worker_model(metadata.model_policy),
            model_policy=metadata.model_policy,
            reasoning_effort=self.config.resolve_worker_reasoning_effort(metadata.model_policy),
            responses=self.responses,
            worker_id=metadata.worker_id,
            topic=metadata.topic,
            provider_timeout_seconds=self.config.provider_timeout_seconds,
            max_citations=self.config.max_citations,
        )

    def get(self, worker_id: str) -> RegisteredWorker:
        with self._lock:
            try:
                return self._workers[worker_id]
            except KeyError as exc:
                raise ValueError(f"unknown worker: {worker_id}") from exc

    def get_or_create(self, *, topic: str, worker_type: str, model_policy: str) -> RegisteredWorker:
        with self._lock:
            if worker_type != "web_search":
                raise UnsupportedWorkerType(f"unsupported worker type: {worker_type}")
            for item in self._workers.values():
                if item.metadata.topic == topic and item.metadata.worker_type == worker_type:
                    if item.metadata.model_policy != model_policy:
                        raise ValueError(
                            "existing worker has an incompatible model policy; refusing to reuse it"
                        )
                    return item
            return self.register(
                worker_id=f"worker-{len(self._workers) + 1}",
                worker_type=worker_type,
                topic=topic,
                model_policy=model_policy,
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
        with self._lock:
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
