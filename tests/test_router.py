"""Routing is tool-free and bound to one immutable catalogue snapshot."""

import json

import pytest

from server.config import Config
from server.router import (
    LazyRouterProvider,
    Router,
    RoutingValidationError,
    WorkerCatalogue,
    WorkerCatalogueEntry,
)


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


def test_lazy_router_provider_defers_credentials_and_provider_creation_until_route() -> None:
    calls: list[dict] = []

    class FakeResponses:
        def create(self, **kwargs: object) -> dict[str, str]:
            calls.append(kwargs)
            return {"output_text": json.dumps({"decision": decision_payload()})}

    factory_calls = 0

    def factory() -> FakeResponses:
        nonlocal factory_calls
        factory_calls += 1
        return FakeResponses()

    provider = LazyRouterProvider(Config(openai_api_key="must-not-appear"), factory)
    router = Router(call=provider)

    assert factory_calls == 0
    assert "must-not-appear" not in repr(provider)
    decision = router.route("What is the weather in Riga?", catalogue())

    assert decision.worker_id == "worker-weather"
    assert factory_calls == 1
    assert calls[0]["model"] == "gpt-5-mini"
    assert calls[0]["store"] is False
    assert "tools" not in calls[0]

    schema = calls[0]["text"]["format"]["schema"]

    def assert_strict_objects(value: object) -> None:
        if isinstance(value, dict):
            if isinstance(value.get("properties"), dict):
                assert value["required"] == list(value["properties"])
                assert "default" not in value
            for child in value.values():
                assert_strict_objects(child)
        elif isinstance(value, list):
            for child in value:
                assert_strict_objects(child)

    assert_strict_objects(schema)


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


def test_empty_catalogue_prompt_bootstraps_public_web_and_reserves_unsupported() -> None:
    prompt = WorkerCatalogue("catalogue-empty", (), ("public_web",), ("deep",)).prompt(
        "What were the capitals of India through the last two hundred years?"
    )

    assert "public factual, current, or historical" in prompt
    assert "action=new_worker" in prompt
    assert "worker_type=web_search" in prompt
    assert "Use unsupported only" in prompt


def test_router_wraps_semantically_invalid_structured_output() -> None:
    invalid = decision_payload(
        action="unsupported",
        worker_id=None,
        worker_type="web_search",
        topic="capitals of India",
    )

    with pytest.raises(RoutingValidationError, match="invalid routing decision"):
        Router(model=FakeRouterModel(invalid)).route("Search the web", catalogue())
