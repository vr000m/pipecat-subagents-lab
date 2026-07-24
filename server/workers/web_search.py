"""Persistent OpenAI hosted-web-search context worker."""

from __future__ import annotations

import asyncio
import inspect
import re
from collections.abc import Callable, Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from ..contracts import GroundedResult
from ..results import canonical_result
from ..structured_outputs import structured_text_format
from .base import ContextWorker, WorkerMetadata


class WorkerDeclined(Exception):
    """The worker determined that hosted search cannot satisfy the request."""


class WorkerClarify(Exception):
    """The worker needs a clarifying answer before it can search."""

    def __init__(self, question: str) -> None:
        super().__init__(question)
        self.question = question


def default_web_clarification(query: str) -> str | None:
    """Ask for details only for narrow, safely detectable incomplete searches."""
    normalized = " ".join(query.strip().split())
    lowered = normalized.casefold()
    if "clarification asked:" in lowered and "user answer:" in lowered:
        return None
    if re.search(r"\b(weather|forecast|temperature)\b", lowered) and not re.search(
        r"\b(?:in|for|at|near)\s+\S+", lowered
    ):
        return "Which location should I use?"
    if re.search(r"\b(?:search|find|look for)\s*$", lowered):
        return "What should I search for?"
    return None


class WebSearchAnswer(BaseModel):
    """One sourced answer with separate reading and speech projections."""

    model_config = ConfigDict(extra="forbid")

    display_text: str
    spoken_text: str

    @field_validator("display_text", "spoken_text")
    @classmethod
    def require_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("answer projections must not be empty")
        return normalized

    @field_validator("spoken_text")
    @classmethod
    def bound_spoken_text(cls, value: str) -> str:
        if len(value) > 600:
            raise ValueError("spoken projection must not exceed 600 characters")
        return value


_WEB_SEARCH_INSTRUCTIONS = (
    "Use hosted web search to answer the current request. Return a complete, normal readable "
    "answer in display_text and a concise standalone version for text-to-speech in spoken_text. "
    "spoken_text must be plain speech, use at most 80 words and 600 characters, and contain no "
    "Markdown, URLs, citation markers, source lists, tables, headings, or stage directions. "
    "Preserve names, dates, quantities, qualifications, and uncertainty needed to answer the "
    "request accurately. Every claim in spoken_text must also be supported by display_text and "
    "the same web-search sources; never introduce a fact only in the spoken projection."
)


def _value(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _response_text(response: Any) -> str:
    text = _value(response, "output_text")
    if isinstance(text, str) and text:
        return text
    chunks: list[str] = []
    for item in _value(response, "output", []) or []:
        for content in _value(item, "content", []) or []:
            value = _value(content, "text")
            if isinstance(value, str):
                chunks.append(value)
    return "\n".join(chunks).strip()


def _response_answer(response: Any) -> WebSearchAnswer:
    raw = _response_text(response)
    if not raw:
        raise WorkerDeclined("hosted web search returned no answer")
    try:
        return WebSearchAnswer.model_validate_json(raw)
    except ValidationError as exc:
        raise WorkerDeclined("hosted web search returned an invalid answer envelope") from exc


def _response_citations(response: Any) -> list[dict[str, str]]:
    citations: list[dict[str, str]] = []

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            url = value.get("url")
            if isinstance(url, str) and (
                "citation" in str(value.get("type", "")).lower() or "title" in value
            ):
                citations.append({"title": str(value.get("title") or ""), "url": url})
            for child in value.values():
                visit(child)
        elif isinstance(value, (list, tuple)):
            for child in value:
                visit(child)
        else:
            url = _value(value, "url")
            if isinstance(url, str):
                citations.append({"title": str(_value(value, "title", "") or ""), "url": url})
            for child_name in (
                "output",
                "content",
                "annotations",
                "results",
                "action",
                "sources",
            ):
                child = _value(value, child_name)
                if child is not None:
                    visit(child)

    visit(response)
    return citations


class WebSearchWorker(ContextWorker):
    def __init__(
        self,
        *,
        worker_id: str = "worker-web-search",
        topic: str = "web-search",
        model: str,
        responses: Any,
        model_policy: str | None = None,
        decline: Callable[[str], bool] | None = None,
        can_satisfy: Callable[[str], bool] | None = None,
        needs_clarification: Callable[[str], str | None] | None = None,
    ) -> None:
        metadata = WorkerMetadata(
            worker_id=worker_id,
            worker_type="web_search",
            topic=topic,
            topic_summary=topic,
            model_policy=model_policy or model,
            capabilities={"public_web": True},
        )
        super().__init__(metadata)
        self.model = model
        self.responses = responses
        self.decline = decline or (
            lambda query: can_satisfy(query) is False if can_satisfy else False
        )
        self.needs_clarification = needs_clarification or default_web_clarification

    @staticmethod
    def refine_query(query: str) -> str:
        normalized = " ".join(query.strip().split())
        return normalized.split("refined:", 1)[0].rstrip(" ;")[:2000]

    def _contextual_input(self, query: str) -> str:
        if not self.history:
            return query
        previous = self.history[-4:]
        context = "\n".join(
            f"Previous query: {entry.get('query', '')}\n"
            f"Previous answer: {entry.get('text', '')[:1200]}"
            for entry in previous
        )
        return f"Use this prior topic context when useful:\n{context}\nCurrent request: {query}"

    async def search(
        self, query: str, *, turn_id: str, origin_epoch: int | None = None
    ) -> GroundedResult:
        refined = self.refine_query(query)
        if self.decline(refined):
            raise WorkerDeclined("hosted web search cannot satisfy this request")
        question = self.needs_clarification(refined)
        if question:
            raise WorkerClarify(question)

        async def execute() -> Any:
            # Build the contextual request inside the mailbox. This keeps a
            # same-worker turn from observing a history snapshot captured
            # before an earlier queued turn has committed its result.
            kwargs = {
                "model": self.model,
                "tools": [{"type": "web_search"}],
                "tool_choice": "required",
                "include": ["web_search_call.action.sources"],
                "instructions": _WEB_SEARCH_INSTRUCTIONS,
                "input": self._contextual_input(refined),
                "text": structured_text_format(WebSearchAnswer, "web_search_answer"),
                "store": False,
            }
            create = self.responses.create
            if inspect.iscoroutinefunction(create):
                response = create(**kwargs)
            else:
                response = await asyncio.to_thread(create, **kwargs)
            if inspect.isawaitable(response):
                response = await response
            answer = _response_answer(response)
            result = canonical_result(
                worker_id=self.metadata.worker_id,
                turn_id=turn_id,
                text=answer.display_text,
                spoken_text=answer.spoken_text,
                citations=_response_citations(response),
                origin_epoch=origin_epoch,
            )
            self.append_context(
                {
                    "turn_id": turn_id,
                    "query": refined,
                    "text": answer.display_text,
                    "result_id": result.result_id,
                }
            )
            return result

        return await self.submit(execute)

    async def run(self, query: Any) -> Any:
        # WorkerRunner invokes BaseWorker.run(params); tests and the application
        # invoke this convenience method with a query string.
        if not isinstance(query, str):
            return await super().run(query)
        if self.decline(self.refine_query(query)):
            return type("WorkerOutcome", (), {"outcome": "decline", "text": "", "citations": []})()
        return await self.search(query, turn_id="turn-worker")
