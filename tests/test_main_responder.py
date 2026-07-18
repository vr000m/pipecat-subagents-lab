"""Direct, unsupported, and router-owned clarification use one tool-free call."""

from server.main_responder import MainResponder, MainResponse


class FakeStructuredModel:
    tools = []

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
