"""Persistent OpenAI hosted-web-search context worker."""

from __future__ import annotations

import asyncio
import inspect
import re
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from ..contracts import GroundedResult
from ..results import canonical_result, normalize_citations
from ..structured_outputs import structured_text_format
from .base import ContextWorker, WorkerMetadata


class WorkerDeclined(Exception):
    """The worker determined that hosted search cannot satisfy the request."""


class WorkerClarify(Exception):
    """The worker needs a clarifying answer before it can search."""

    def __init__(self, question: str) -> None:
        super().__init__(question)
        self.question = question


@dataclass(frozen=True)
class ClarificationContext:
    """Typed continuation data rendered only at the provider boundary."""

    original_query: str
    question: str
    answer: str

    def provider_query(self) -> str:
        def bounded(value: str, limit: int) -> str:
            return " ".join(value.strip().split())[:limit]

        return (
            f"Original request: {bounded(self.original_query, 650)}\n"
            f"Clarification asked: {bounded(self.question, 400)}\n"
            f"User answer: {bounded(self.answer, 800)}"
        )


def default_web_clarification(query: str) -> str | None:
    """Ask for details only for narrow, safely detectable incomplete searches."""
    normalized = " ".join(query.strip().split())
    lowered = normalized.casefold()
    if re.search(r"\b(weather|forecast|temperature)\b", lowered) and not (
        _has_weather_location(lowered)
    ):
        return "Which location should I use?"
    if re.search(r"\b(?:search|find|look for)\s*$", lowered):
        return "What should I search for?"
    return None


_WEATHER_MODIFIER_WORDS = {
    "afternoon",
    "april",
    "august",
    "autumn",
    "c",
    "celsius",
    "centigrade",
    "coming",
    "december",
    "degrees",
    "evening",
    "f",
    "fahrenheit",
    "fall",
    "february",
    "friday",
    "hour",
    "hours",
    "january",
    "july",
    "june",
    "k",
    "kelvin",
    "last",
    "march",
    "may",
    "midnight",
    "monday",
    "month",
    "morning",
    "next",
    "night",
    "noon",
    "november",
    "now",
    "october",
    "saturday",
    "september",
    "spring",
    "summer",
    "sunday",
    "the",
    "this",
    "thursday",
    "today",
    "tomorrow",
    "tonight",
    "tuesday",
    "wednesday",
    "week",
    "weekend",
    "winter",
    "year",
    "yesterday",
}
_WEATHER_TEMPORAL_WORDS = _WEATHER_MODIFIER_WORDS - {
    "c",
    "celsius",
    "centigrade",
    "degrees",
    "f",
    "fahrenheit",
    "k",
    "kelvin",
    "the",
}
_WEATHER_TEMPORAL_PREFIX = re.compile(
    r"^(?:"
    r"\d{1,2}(?::\d{2})?\s*(?:am|pm)"
    r"|(?:zero|one|two|three|four|five|six|seven|eight|nine|ten|\d+)\s+"
    r"(?:minute|minutes|hour|hours|day|days|week|weeks|month|months)"
    r")\b",
    re.IGNORECASE,
)


def _has_weather_location(query: str) -> bool:
    """Return whether a weather preposition is followed by a plausible place."""
    for preposition in re.finditer(r"\b(?:in|for|at|near)\b", query):
        suffix = query[preposition.end() :]
        tokens = re.findall(r"[^\s,;?.!]+", suffix)[:3]
        if not tokens:
            continue
        phrase = " ".join(token.strip("\"'()[]{}") for token in tokens)
        if _WEATHER_TEMPORAL_PREFIX.match(phrase):
            continue
        first = tokens[0].strip("\"'()[]{}").casefold()
        second = tokens[1].strip("\"'()[]{}").casefold() if len(tokens) > 1 else ""
        if first in _WEATHER_MODIFIER_WORDS:
            if first == "the" and second not in _WEATHER_TEMPORAL_WORDS:
                return True
            continue
        if second in {"degree", "degrees", "hour", "hours"} and first.isdigit():
            continue
        if re.fullmatch(r"(?:\d{1,2}(?::\d{2})?\s*(?:am|pm)|\d{4}-\d{1,2}-\d{1,2})", first):
            continue
        if re.fullmatch(r"(?:°?[cfk]|[-+]?\d+(?:\.\d+)?°[cfk]?)", first):
            continue
        return True
    return False


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
    "the same web-search sources; never introduce a fact only in the spoken projection. Prefer "
    "three to six authoritative sources and avoid exhaustive source lists."
)


def build_worker_request_kwargs(
    model: str, effort: str | None, *, query: str, instructions: str = _WEB_SEARCH_INSTRUCTIONS
) -> dict[str, Any]:
    """The exact request-kwargs shape sent to the Responses API for a worker
    search call.

    Hoisted out of ``WebSearchWorker.search``'s ``execute()`` closure (round
    8 gauntlet, Architecture finding 8) so ``scripts/verify_eval_candidates.py``'s
    probe builder can call this SAME function instead of hand-mirroring the
    shape independently -- see ``build_router_request_kwargs`` in
    ``server/router.py`` for the matching router-side rationale.
    """
    kwargs: dict[str, Any] = {
        "model": model,
        "tools": [{"type": "web_search"}],
        "tool_choice": "required",
        "include": ["web_search_call.action.sources"],
        "instructions": instructions,
        "input": query,
        "text": structured_text_format(WebSearchAnswer, "web_search_answer"),
        "store": False,
    }
    if effort is not None:
        kwargs["reasoning"] = {"effort": effort}
    return kwargs


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


def _response_citations(
    response: Any,
    *,
    max_candidates: int,
    max_depth: int = 64,
    max_nodes: int = 10_000,
) -> list[dict[str, str]]:
    citations: list[dict[str, str]] = []
    pending: deque[tuple[Any, int]] = deque([(response, 0)])
    seen: set[int] = set()
    visited_nodes = 0

    while pending and visited_nodes < max_nodes and len(citations) < max_candidates:
        value, depth = pending.popleft()
        visited_nodes += 1
        if depth > max_depth:
            continue
        if not isinstance(value, (str, bytes, int, float, bool, type(None))):
            identity = id(value)
            if identity in seen:
                continue
            seen.add(identity)
        if isinstance(value, Mapping):
            url = value.get("url")
            if isinstance(url, str) and (
                "citation" in str(value.get("type", "")).lower() or "title" in value
            ):
                citations.append({"title": str(value.get("title") or ""), "url": url})
            if depth < max_depth:
                for child in value.values():
                    if visited_nodes + len(pending) >= max_nodes:
                        break
                    pending.append((child, depth + 1))
        elif isinstance(value, (list, tuple)):
            for child in value:
                if visited_nodes + len(pending) >= max_nodes:
                    break
                pending.append((child, depth + 1))
        else:
            url = _value(value, "url")
            if isinstance(url, str):
                citations.append({"title": str(_value(value, "title", "") or ""), "url": url})
            if depth < max_depth:
                for child_name in (
                    "output",
                    "content",
                    "annotations",
                    "results",
                    "action",
                    "sources",
                ):
                    if visited_nodes + len(pending) >= max_nodes:
                        break
                    child = _value(value, child_name)
                    if child is not None:
                        pending.append((child, depth + 1))
        if len(citations) >= max_candidates:
            break
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
        reasoning_effort: str | None = None,
        decline: Callable[[str], bool] | None = None,
        can_satisfy: Callable[[str], bool] | None = None,
        needs_clarification: Callable[[str], str | None] | None = None,
        provider_timeout_seconds: float = 75.0,
        max_citations: int = 12,
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
        self.reasoning_effort = reasoning_effort
        self.responses = responses
        self.decline = decline or (
            lambda query: can_satisfy(query) is False if can_satisfy else False
        )
        self.needs_clarification = needs_clarification or default_web_clarification
        self.provider_timeout_seconds = provider_timeout_seconds
        self.max_citations = max_citations

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
        self,
        query: str,
        *,
        turn_id: str,
        origin_epoch: int | None = None,
        clarification_context: ClarificationContext | None = None,
    ) -> GroundedResult:
        refined = (
            clarification_context.provider_query()
            if clarification_context is not None
            else self.refine_query(query)
        )
        if self.decline(refined):
            raise WorkerDeclined("hosted web search cannot satisfy this request")
        if clarification_context is None:
            question = self.needs_clarification(refined)
            if question:
                raise WorkerClarify(question)

        async def execute() -> Any:
            # Build the contextual request inside the mailbox. This keeps a
            # same-worker turn from observing a history snapshot captured
            # before an earlier queued turn has committed its result.
            kwargs = build_worker_request_kwargs(
                self.model, self.reasoning_effort, query=self._contextual_input(refined)
            )
            create = self.responses.create
            async with asyncio.timeout(self.provider_timeout_seconds):
                if inspect.iscoroutinefunction(create):
                    response = create(**kwargs)
                else:
                    response = await asyncio.to_thread(create, **kwargs)
                if inspect.isawaitable(response):
                    response = await response
            answer = _response_answer(response)
            citations = normalize_citations(
                _response_citations(
                    response,
                    max_candidates=max(self.max_citations * 4, self.max_citations),
                )
            )[: self.max_citations]
            result = canonical_result(
                worker_id=self.metadata.worker_id,
                turn_id=turn_id,
                text=answer.display_text,
                spoken_text=answer.spoken_text,
                citations=(citation.model_dump() for citation in citations),
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
