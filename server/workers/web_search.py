"""Persistent OpenAI hosted-web-search context worker."""

from __future__ import annotations

import inspect
import asyncio
from typing import Any, Callable, Mapping

from ..contracts import GroundedResult
from ..results import canonical_result
from .base import ContextWorker, WorkerMetadata


class WorkerDeclined(Exception):
    """The worker determined that hosted search cannot satisfy the request."""


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
            for child_name in ("output", "content", "annotations", "results"):
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
        self.requests: list[dict[str, Any]] = []

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

        kwargs = {
            "model": self.model,
            "tools": [{"type": "web_search"}],
            "input": self._contextual_input(refined),
            "store": False,
        }
        self.requests.append(kwargs.copy())

        async def execute() -> Any:
            create = self.responses.create
            if inspect.iscoroutinefunction(create):
                response = create(**kwargs)
            else:
                response = await asyncio.to_thread(create, **kwargs)
            if inspect.isawaitable(response):
                response = await response
            return response

        response = await self.submit(execute)
        text = _response_text(response)
        if not text:
            raise WorkerDeclined("hosted web search returned no answer")
        result = canonical_result(
            worker_id=self.metadata.worker_id,
            turn_id=turn_id,
            text=text,
            citations=_response_citations(response),
            origin_epoch=origin_epoch,
        )
        self.append_context(
            {"turn_id": turn_id, "query": refined, "text": text, "result_id": result.result_id}
        )
        return result

    async def run(self, query: Any) -> Any:
        # WorkerRunner invokes BaseWorker.run(params); tests and the application
        # invoke this convenience method with a query string.
        if not isinstance(query, str):
            return await super().run(query)
        if self.decline(self.refine_query(query)):
            return type("WorkerOutcome", (), {"outcome": "decline", "text": "", "citations": []})()
        return await self.search(query, turn_id="turn-worker")
