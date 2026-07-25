"""The registry owns persistent context identities and immutable catalogues."""

from dataclasses import replace

import pytest

from server.registry import UnsupportedWorkerType, WorkerRegistry
from server.workers.base import ContextWorker, WorkerMetadata
from server.workers.web_search import WebSearchWorker


class FakeContextWorker:
    def __init__(self, worker_id: str) -> None:
        self.worker_id = worker_id
        self.turns: list[str] = []
        self.metadata = WorkerMetadata(
            worker_id=worker_id,
            worker_type="web_search",
            topic="custom",
            topic_summary="custom",
            model_policy="deep",
            capabilities={"private_calendar": True},
        )

    async def search(self, query: str, **kwargs: object) -> object:
        return {"query": query, **kwargs}


class FakeResponses:
    pass


class SearchOnlyWorker:
    async def search(self, query: str, **kwargs: object) -> object:
        return {"query": query, **kwargs}


def test_new_web_search_worker_uses_injected_provider_and_configured_model() -> None:
    provider = FakeResponses()
    registry = WorkerRegistry(responses=provider)

    item = registry.get_or_create(topic="news", worker_type="web_search", model_policy="deep")

    assert isinstance(item.worker, WebSearchWorker)
    assert item.worker.responses is provider
    assert item.worker.model == registry.config.worker_model_policy["deep"]
    assert item.metadata.model_policy == "deep"


def test_registry_rejects_unimplemented_worker_types() -> None:
    registry = WorkerRegistry()

    with pytest.raises(UnsupportedWorkerType, match="calendar"):
        registry.get_or_create(topic="calendar", worker_type="calendar", model_policy="deep")

    with pytest.raises(UnsupportedWorkerType, match="calendar"):
        registry.register(worker_id="worker-calendar", worker_type="calendar", topic="calendar")


def test_register_builds_a_search_capable_worker_by_default() -> None:
    registry = WorkerRegistry(responses=FakeResponses())

    item = registry.register(
        worker_id="worker-weather",
        worker_type="web_search",
        topic="weather",
    )

    assert isinstance(item.worker, WebSearchWorker)
    assert callable(item.worker.search)


def test_register_rejects_context_worker_without_search_before_publication() -> None:
    registry = WorkerRegistry()
    worker = ContextWorker(
        WorkerMetadata(
            worker_id="worker-weather",
            worker_type="web_search",
            topic="weather",
            topic_summary="weather",
            model_policy="deep",
        )
    )

    with pytest.raises(TypeError, match="callable search method"):
        registry.register(
            worker_id="worker-weather",
            worker_type="web_search",
            topic="weather",
            worker=worker,
        )

    assert registry.workers == ()


def test_get_or_create_rejects_factory_worker_without_search_before_publication() -> None:
    registry = WorkerRegistry(
        worker_factory=lambda worker_id: ContextWorker(
            WorkerMetadata(
                worker_id=worker_id,
                worker_type="web_search",
                topic="weather",
                topic_summary="weather",
                model_policy="deep",
            )
        )
    )

    with pytest.raises(TypeError, match="callable search method"):
        registry.get_or_create(topic="weather", worker_type="web_search", model_policy="deep")

    assert registry.workers == ()


@pytest.mark.parametrize("source", ["injected", "factory"])
def test_registry_rejects_search_worker_without_metadata_before_publication(source: str) -> None:
    worker = SearchOnlyWorker()
    registry = (
        WorkerRegistry(worker_factory=lambda worker_id: worker)
        if source == "factory"
        else WorkerRegistry()
    )

    with pytest.raises(TypeError, match="must define metadata"):
        registry.register(
            worker_id="worker-weather",
            worker_type="web_search",
            topic="weather",
            worker=None if source == "factory" else worker,
        )

    assert registry.workers == ()


@pytest.mark.parametrize("source", ["injected", "factory"])
@pytest.mark.parametrize(
    ("field", "mismatched_value"),
    [
        ("worker_id", "worker-other"),
        ("worker_type", "other"),
        ("topic", "news"),
        ("model_policy", "fast"),
    ],
)
def test_registry_rejects_mismatched_worker_identity_before_publication(
    source: str, field: str, mismatched_value: str
) -> None:
    expected = WorkerMetadata(
        worker_id="worker-weather",
        worker_type="web_search",
        topic="weather",
        topic_summary="weather",
        model_policy="deep",
        capabilities={"private_calendar": True},
    )
    worker = FakeContextWorker(expected.worker_id)
    worker.metadata = replace(expected, **{field: mismatched_value})
    registry = (
        WorkerRegistry(worker_factory=lambda worker_id: worker)
        if source == "factory"
        else WorkerRegistry()
    )

    with pytest.raises(ValueError, match=f"metadata mismatch for {field}"):
        registry.register(
            worker_id=expected.worker_id,
            worker_type=expected.worker_type,
            topic=expected.topic,
            model_policy=expected.model_policy,
            worker=None if source == "factory" else worker,
        )

    assert registry.workers == ()


def test_registry_keeps_two_workers_and_same_topic_identity_persistent() -> None:
    registry = WorkerRegistry(responses=FakeResponses())
    weather = registry.get_or_create(topic="weather", worker_type="web_search", model_policy="deep")
    news = registry.get_or_create(topic="news", worker_type="web_search", model_policy="deep")
    follow_up = registry.get_or_create(
        topic="weather", worker_type="web_search", model_policy="deep"
    )

    assert weather.worker_id == follow_up.worker_id
    assert news.worker_id != weather.worker_id
    assert registry.get(weather.worker_id).context is weather.context
    assert registry.policy.eviction_enabled is False


def test_registry_does_not_reuse_topic_with_incompatible_model_policy() -> None:
    registry = WorkerRegistry(responses=FakeResponses())
    registry.get_or_create(topic="weather", worker_type="web_search", model_policy="deep")
    with pytest.raises(ValueError, match="incompatible model policy"):
        registry.get_or_create(topic="weather", worker_type="web_search", model_policy="fast")


def test_catalogue_is_immutable_and_dispatch_revalidates_against_its_snapshot() -> None:
    def worker_factory(worker_id: str) -> FakeContextWorker:
        worker = FakeContextWorker(worker_id)
        worker.metadata = replace(
            worker.metadata,
            topic={"worker-1": "weather", "worker-2": "news"}[worker_id],
        )
        return worker

    registry = WorkerRegistry(worker_factory=worker_factory)
    worker = registry.get_or_create(topic="weather", worker_type="web_search", model_policy="deep")
    snapshot = registry.catalogue()

    assert snapshot.version
    assert tuple(entry.worker_id for entry in snapshot.entries) == (worker.worker_id,)
    registry.get_or_create(topic="news", worker_type="web_search", model_policy="deep")
    assert tuple(entry.worker_id for entry in snapshot.entries) == (worker.worker_id,)

    registry.validate_selection(
        snapshot,
        worker_id=worker.worker_id,
        worker_type="web_search",
        capability="private_calendar",
        model_policy="deep",
    )


def test_custom_factory_preserves_worker_specific_capabilities() -> None:
    registry = WorkerRegistry(worker_factory=FakeContextWorker)

    item = registry.get_or_create(topic="custom", worker_type="web_search", model_policy="deep")

    entry = registry.catalogue().entries[0]
    assert item.metadata.capabilities == {"private_calendar": True}
    assert dict(entry.capabilities) == {"private_calendar": True}
