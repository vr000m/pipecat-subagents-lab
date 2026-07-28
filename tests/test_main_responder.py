"""Direct, unsupported, and router-owned clarification use one tool-free call."""

from typing import ClassVar

from server.main_responder import MainResponder, MainResponse
from server.results import ResultLog


class FakeStructuredModel:
    tools: ClassVar[list[object]] = []

    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.calls: list[dict] = []

    def structured_output(self, **kwargs: object) -> dict:
        self.calls.append(kwargs)
        return self.payload


def test_main_responder_emits_validated_prose_from_one_tool_free_envelope() -> None:
    model = FakeStructuredModel({"action": "direct", "prose": "The answer is 42."})
    result = MainResponder(model=model).respond(
        transcript="What is the answer?",
        decision={"action": "direct"},
    )

    assert model.tools == []
    assert len(model.calls) == 1
    assert result.text == "The answer is 42."
    assert result.spoken_text == result.ui_text == result.text


def test_main_responder_keeps_unsupported_and_clarify_distinct_from_worker_dispatch() -> None:
    for action, prose in (
        ("unsupported", "I cannot access private calendar data."),
        ("clarify", "Which city?"),
    ):
        model = FakeStructuredModel({"action": action, "prose": prose})
        result = MainResponder(model=model).respond(transcript="test", decision={"action": action})
        assert result.text == prose
        assert model.tools == []


def test_main_responder_normalizes_pydantic_main_response_before_reading_fields() -> None:
    model = FakeStructuredModel(MainResponse(action="direct", text="Structured answer."))

    result = MainResponder(model=model).respond(
        transcript="What is the answer?",
        decision={"action": "direct"},
    )

    assert result.text == "Structured answer."


def test_main_responder_validates_dict_main_response_before_reading_fields() -> None:
    model = FakeStructuredModel({"action": "direct", "text": "Validated answer."})

    result = MainResponder(model=model).respond(
        transcript="What is the answer?",
        decision={"action": "direct"},
    )

    assert result.text == "Validated answer."


def test_repeated_identical_main_prose_gets_unique_result_ids_for_result_log() -> None:
    model = FakeStructuredModel({"action": "direct", "prose": "The same answer."})
    responder = MainResponder(model=model)

    first = responder.respond("What is the answer?", {"action": "direct"})
    second = responder.respond("What is the answer?", {"action": "direct"})
    log = ResultLog()

    assert first.text == second.text
    assert first.result_id != second.result_id
    log.append(first)
    log.append(second)
    assert [result.result_id for result in log.results] == [first.result_id, second.result_id]
