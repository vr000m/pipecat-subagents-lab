"""Tool-free main-model responses for router-owned outcomes."""

from __future__ import annotations

from typing import Any, Callable
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, model_validator

from .contracts import RoutingDecision
from .results import ProjectedResult


class MainResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    action: str
    prose: str | None = None
    text: str | None = None

    @model_validator(mode="after")
    def require_text(self) -> MainResponse:
        if not self.prose and not self.text:
            raise ValueError("main response must contain prose or text")
        return self


class MainResponder:
    def __init__(self, call: Callable[[str], Any] | None = None, model: Any = None) -> None:
        self._call, self.model, self.last_prompt = call, model, None

    def respond(
        self, transcript: str, decision: RoutingDecision | dict[str, Any]
    ) -> ProjectedResult:
        action = decision.action if isinstance(decision, RoutingDecision) else decision["action"]
        if action not in {"direct", "unsupported", "clarify"}:
            raise ValueError("main responder only handles router-owned outcomes")
        self.last_prompt = (
            f"Answer concisely for validated action {action}. Transcript: {transcript}"
        )
        raw = (
            self.model.structured_output(transcript=transcript, decision=decision)
            if self.model is not None
            else self._call(self.last_prompt)
        )
        if isinstance(raw, str):
            response = MainResponse(action=action, prose=raw)
        else:
            response = raw if isinstance(raw, MainResponse) else MainResponse.model_validate(raw)
        if response.action != action:
            raise ValueError("main response action does not match routing action")
        prose = response.prose or response.text
        assert prose is not None
        return ProjectedResult(f"main-{uuid4().hex}", prose, [])
