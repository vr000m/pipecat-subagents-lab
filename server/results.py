"""Canonical grounded results and their speech/UI projections."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any
from uuid import uuid4

from .contracts import Citation, GroundedResult


class ProjectedResult:
    def __init__(
        self, result_id: str, text: str, citations: list[Citation], timestamp: str | None = None
    ) -> None:
        self.result_id, self.text, self.citations, self.timestamp = (
            result_id,
            text,
            citations,
            timestamp,
        )
        self.spoken_text = self.ui_text = text


def normalize_citations(items: Iterable[Mapping[str, Any]] | None) -> list[Citation]:
    result: list[Citation] = []
    seen: set[str] = set()
    for item in items or ():
        url = item.get("url")
        if (
            not isinstance(url, str)
            or not url.startswith(("http://", "https://"))
            or any(ch.isspace() for ch in url)
        ):
            continue
        if url in seen:
            continue
        seen.add(url)
        result.append(Citation(title=str(item.get("title") or ""), url=url))
    return result


def canonical_result(
    *,
    worker_id: str,
    turn_id: str,
    text: str,
    citations: Iterable[Mapping[str, Any]] | None = None,
    result_id: str | None = None,
    origin_epoch: int | None = None,
) -> GroundedResult:
    result = GroundedResult(
        result_id=result_id or f"result-{uuid4().hex}",
        worker_id=worker_id,
        turn_id=turn_id,
        text=text,
        citations=normalize_citations(citations),
        spoken_text=text,
        ui_text=text,
        origin_epoch=origin_epoch,
    )
    return result


class ResultLog:
    """Unbounded process-lifetime finalized result history."""

    def __init__(self) -> None:
        self._results: list[GroundedResult] = []

    def append(self, result: GroundedResult) -> GroundedResult:
        if any(existing.result_id == result.result_id for existing in self._results):
            raise ValueError(f"duplicate result: {result.result_id}")
        self._results.append(result)
        return result

    def for_worker(self, worker_id: str) -> tuple[GroundedResult, ...]:
        return tuple(result for result in self._results if result.worker_id == worker_id)

    @property
    def results(self) -> tuple[GroundedResult, ...]:
        return tuple(self._results)


def normalize_grounded_result(**kwargs: Any) -> GroundedResult:
    return canonical_result(**kwargs)


def project_result(
    result: GroundedResult, *, spoken_text: str | None = None, ui_text: str | None = None
) -> tuple[ProjectedResult, ProjectedResult]:
    if (
        spoken_text is not None
        and spoken_text != result.text
        or ui_text is not None
        and ui_text != result.text
    ):
        raise ValueError("projections cannot add facts to the canonical result")
    return (
        ProjectedResult(result.result_id, result.text, list(result.citations), result.timestamp),
        ProjectedResult(result.result_id, result.text, list(result.citations), result.timestamp),
    )
