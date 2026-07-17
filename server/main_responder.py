"""Tool-free main-model responses for router-owned outcomes."""

from __future__ import annotations
from typing import Any, Callable
from pydantic import BaseModel, ConfigDict
from .contracts import RoutingDecision
from .results import ProjectedResult


class MainResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    text: str
    action: str


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
        prose = raw if isinstance(raw, str) else raw.get("prose", raw.get("text", ""))
        if isinstance(raw, dict) and raw.get("action", action) != action:
            raise ValueError("main response action does not match routing action")
        return ProjectedResult(f"main-{id(prose)}", prose, [])
