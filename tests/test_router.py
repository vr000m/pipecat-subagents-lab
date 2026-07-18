"""Routing is tool-free and bound to one immutable catalogue snapshot."""

import pytest

from server.router import Router, RoutingValidationError, WorkerCatalogue, WorkerCatalogueEntry


class FakeRouterModel:
    tools = []

    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.calls: list[dict] = []

    def structured_output(self, *, transcript: str, catalogue: tuple[WorkerCatalogueEntry, ...]):
        self.calls.append({"transcript": transcript, "catalogue": catalogue})
        return self.payload


def catalogue() -> tuple[WorkerCatalogueEntry, ...]:
    return (
        WorkerCatalogueEntry(
            worker_id="worker-weather",
            worker_type="web_search",
            topic="weather",
            status="idle",
            capabilities={"public_web": True, "private_calendar": False},
            model_policies={"deep"},
        ),
    )


def decision_payload(**overrides: object) -> dict:
    payload = {
        "action": "existing_worker",
        "worker_id": "worker-weather",
        "worker_type": "web_search",
        "topic": "weather",
        "capability": "public_web",
        "capability_available": True,
        "model_policy": "deep",
        "catalogue_version": "catalogue-1",
        "catalogue_worker_ids": ("worker-weather",),
    }
    payload.update(overrides)
    return payload


def test_router_has_no_tools_and_passes_the_same_snapshot_to_model_and_validation() -> None:
    model = FakeRouterModel(decision_payload())
    router = Router(model=model)

    decision = router.route("What is the weather in Riga?", catalogue())

    assert model.tools == []
    assert decision.action == "existing_worker"
    assert model.calls[0]["catalogue"] == catalogue()
    assert decision.worker_id == model.calls[0]["catalogue"][0].worker_id


@pytest.mark.parametrize("action", ["direct", "unsupported", "clarify"])
def test_router_accepts_tool_free_main_owned_actions(action: str) -> None:
    model = FakeRouterModel(
        decision_payload(action=action, worker_id=None, worker_type=None, topic=None)
    )

    decision = Router(model=model).route("Tell me something", catalogue())

    assert decision.action == action


def test_router_rejects_hallucinated_worker_against_snapshot() -> None:
    model = FakeRouterModel(decision_payload(worker_id="worker-hallucinated"))

    with pytest.raises((RoutingValidationError, ValueError)):
        Router(model=model).route("Search this", catalogue())


def test_router_rejects_unavailable_private_capability_even_when_topic_is_current() -> None:
    model = FakeRouterModel(
        decision_payload(
            topic="calendar",
            capability="private_calendar",
            capability_available=False,
        )
    )

    with pytest.raises((RoutingValidationError, ValueError)):
        Router(model=model).route("What is on my private calendar today?", catalogue())


def test_router_allows_first_valid_new_worker_with_empty_catalogue() -> None:
    empty = WorkerCatalogue("catalogue-empty", (), (), ())
    model = FakeRouterModel(
        decision_payload(
            action="new_worker",
            worker_id=None,
            worker_type="web_search",
            topic="news",
            capability="public_web",
            capability_available=True,
            model_policy="deep",
            catalogue_version="catalogue-empty",
            catalogue_worker_ids=(),
        )
    )

    decision = Router(model=model).route("Search the news", empty)

    assert decision.action == "new_worker"
    assert decision.worker_type == "web_search"
