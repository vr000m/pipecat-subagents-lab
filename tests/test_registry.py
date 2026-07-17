"""The registry owns persistent context identities and immutable catalogues."""

from server.registry import WorkerRegistry


class FakeContextWorker:
    def __init__(self, worker_id: str) -> None:
        self.worker_id = worker_id
        self.turns: list[str] = []


def test_registry_keeps_two_workers_and_same_topic_identity_persistent() -> None:
    registry = WorkerRegistry(worker_factory=FakeContextWorker)
    weather = registry.get_or_create(topic="weather", worker_type="web_search", model_policy="deep")
    news = registry.get_or_create(topic="news", worker_type="web_search", model_policy="deep")
    follow_up = registry.get_or_create(
        topic="weather", worker_type="web_search", model_policy="deep"
    )

    assert weather.worker_id == follow_up.worker_id
    assert news.worker_id != weather.worker_id
    assert registry.get(weather.worker_id).context is weather.context
    assert registry.policy.eviction_enabled is False


def test_catalogue_is_immutable_and_dispatch_revalidates_against_its_snapshot() -> None:
    registry = WorkerRegistry(worker_factory=FakeContextWorker)
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
        capability="public_web",
        model_policy="deep",
    )
