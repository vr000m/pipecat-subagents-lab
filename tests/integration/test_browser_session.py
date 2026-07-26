"""End-to-end invariants for the browser-first session seams."""

import asyncio

import pytest

from server.contracts import DeliveryState, WorkerState
from server.connection_arbiter import ConnectionEpochArbiter
from server.pipeline import SessionHost
from server.registry import WorkerRegistry
from server.results import normalize_grounded_result, project_result
from server.router import Router, WorkerCatalogueEntry
from server.rtvi_messages import RTVIMessagePublisher
from server.session_state import SessionState
from server.work_item_coordinator import WorkItemCoordinator


class FakeRouterModel:
    tools: list[object] = []

    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload
        self.catalogues: list[tuple[WorkerCatalogueEntry, ...]] = []

    def structured_output(
        self, *, transcript: str, catalogue: tuple[WorkerCatalogueEntry, ...]
    ) -> dict[str, object]:
        del transcript
        self.catalogues.append(catalogue)
        return self.payload


class FakeRunner:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


def _decision(catalogue, action: str, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "action": action,
        "worker_id": None,
        "worker_type": None,
        "topic": None,
        "capability": None,
        "capability_available": True,
        "model_policy": None,
        "catalogue_version": catalogue.version,
        "catalogue_worker_ids": catalogue.worker_ids,
    }
    if action in {"existing_worker", "new_worker"}:
        worker = catalogue.entries[0]
        payload.update(
            {
                "worker_id": worker.worker_id if action == "existing_worker" else None,
                "worker_type": worker.worker_type,
                "topic": worker.topic if action == "existing_worker" else "new-topic",
                "capability": "public_web",
                "model_policy": worker.model_policy,
            }
        )
    payload.update(overrides)
    return payload


@pytest.mark.parametrize(
    "action", ["direct", "unsupported", "clarify", "new_worker", "existing_worker"]
)
def test_routing_matrix_is_tool_free_and_catalogue_bound(action: str) -> None:
    registry = WorkerRegistry()
    registry.register(
        worker_id="worker-weather",
        worker_type="web_search",
        topic="weather",
        capabilities={"public_web": True},
    )
    catalogue = registry.catalogue()
    model = FakeRouterModel(_decision(catalogue, action))

    decision = Router(model=model).route("What is happening?", catalogue)

    assert decision.action == action
    assert model.tools == []
    assert model.catalogues == [catalogue.entries]
    if action == "new_worker":
        assert decision.worker_id is None


def test_new_worker_proposal_is_validated_then_materialized_by_dispatch() -> None:
    registry = WorkerRegistry()
    registry.register(
        worker_id="worker-weather",
        worker_type="web_search",
        topic="weather",
        capabilities={"public_web": True},
    )
    catalogue = registry.catalogue()
    model = FakeRouterModel(_decision(catalogue, "new_worker", topic="news"))
    decision = Router(model=model).route("Search today's news", catalogue)

    worker = WorkItemCoordinator(registry=registry).dispatch(decision, catalogue=catalogue)

    assert worker.metadata.topic == "news"
    assert worker.metadata.worker_type == "web_search"
    registered = registry.get_or_create(topic="news", worker_type="web_search", model_policy="deep")
    assert registered.worker is worker


def test_persistence_grounding_reconnect_and_interruption_hold_together() -> None:
    async def run() -> None:
        registry = WorkerRegistry()
        weather = registry.get_or_create(
            topic="weather", worker_type="web_search", model_policy="deep"
        )
        news = registry.get_or_create(topic="news", worker_type="web_search", model_policy="deep")
        same_weather = registry.get_or_create(
            topic="weather", worker_type="web_search", model_policy="deep"
        )

        async def record(turn_id: str) -> None:
            same_weather.context.append_context({"turn_id": turn_id})

        await weather.context.submit(lambda: record("turn-1"))
        await weather.context.submit(lambda: record("turn-2"))
        assert same_weather.context is weather.context
        assert [item["turn_id"] for item in weather.context.history] == ["turn-1", "turn-2"]
        assert news.context.history == []

        result = normalize_grounded_result(
            result_id="result-weather-1",
            worker_id=weather.worker_id,
            turn_id="turn-2",
            text="Rain is likely.",
            citations=[
                {"title": "Forecast", "url": "https://weather.example/forecast"},
                {"title": "Unsafe", "url": "javascript:alert(1)"},
                {"title": "Duplicate", "url": "https://weather.example/forecast"},
            ],
            origin_epoch=1,
        )
        spoken, ui = project_result(result)
        assert spoken.result_id == ui.result_id == result.result_id
        assert spoken.citations == ui.citations == result.citations
        assert [citation.url for citation in result.citations] == [
            "https://weather.example/forecast"
        ]

        host = SessionHost(registry=registry, runner_factory=FakeRunner)
        host.state.set_worker(
            WorkerState(
                worker_id=weather.worker_id,
                topic="weather",
                model_policy="deep",
                status="idle",
            )
        )
        first = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 1,
                "snapshot_sequence": 0,
            }
        )
        host.state.append_result(result, origin_epoch=1)
        item = first.scheduler.enqueue(
            result_id=result.result_id,
            work_item_id="work-weather-1",
            run_id="run-weather-1",
            text=result.spoken_text,
            origin_epoch=1,
        )
        assert host.state.speech[item.utterance_id].state == DeliveryState.QUEUED
        await first.scheduler.start_next()
        assert host.state.speech[item.utterance_id].state == DeliveryState.STARTED

        second = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 2,
                "snapshot_sequence": host.state.sequence,
            }
        )
        assert not host.accepts(1)
        assert host.accepts(2)
        assert host.state.speech[item.utterance_id].state == DeliveryState.INTERRUPTED_BY_RECONNECT
        assert [entry.result_id for entry in host.state.result_history(weather.worker_id)] == [
            result.result_id
        ]

        restored = SessionState.from_snapshot(second.observer.snapshot())
        assert [entry.result_id for entry in restored.result_history(weather.worker_id)] == [
            result.result_id
        ]
        assert restored.speech[item.utterance_id].state == DeliveryState.INTERRUPTED_BY_RECONNECT
        await host.shutdown()

    asyncio.run(run())


def test_replacement_fences_old_client_and_snapshot_waits_for_client_ready() -> None:
    arbiter = ConnectionEpochArbiter(session_id="session-1", resume_token="resume-1")
    old = arbiter.activate("client-a", 1)
    new = arbiter.activate("client-b", 2)

    assert arbiter.accepts_input(new.client_id, new.epoch)
    assert arbiter.snapshot_allowed(new.client_id, new.epoch)
    assert arbiter.accepts_callback(new.epoch)
    assert not arbiter.accepts_input(old.client_id, old.epoch)
    assert not arbiter.snapshot_allowed(old.client_id, old.epoch)
    assert not arbiter.accepts_callback(old.epoch)

    publisher = RTVIMessagePublisher(session_id="session-1", active_epoch=new.epoch)
    assert publisher.snapshot() is None
    publisher.client_ready(epoch=new.epoch)
    assert publisher.snapshot() is not None
