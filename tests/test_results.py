"""Canonical grounded results are the only source for speech and UI output."""

import pytest

from server.results import normalize_grounded_result, project_result


def test_normalization_rejects_untrusted_citations_and_deduplicates_urls() -> None:
    result = normalize_grounded_result(
        worker_id="worker-weather",
        turn_id="turn-1",
        text="Rain is likely.",
        citations=[
            {"title": "Good", "url": "https://example.com/a"},
            {"title": "Duplicate", "url": "https://example.com/a"},
            {"title": "Bad", "url": "javascript:alert(1)"},
            {"title": "Relative", "url": "/local"},
        ],
    )

    assert [citation.url for citation in result.citations] == ["https://example.com/a"]


def test_normalization_rejects_malformed_absolute_urls() -> None:
    result = normalize_grounded_result(
        worker_id="worker-weather",
        turn_id="turn-1",
        text="Answer",
        citations=[
            {"url": "https://"},
            {"url": "https:///missing-host"},
            {"url": "http://[bad"},
        ],
    )
    assert result.citations == []


def test_spoken_and_ui_projections_share_one_canonical_id_and_cannot_add_facts() -> None:
    result = normalize_grounded_result(
        worker_id="worker-weather",
        turn_id="turn-1",
        text="Rain is likely.",
        citations=[{"title": "Forecast", "url": "https://example.com/forecast"}],
    )
    spoken, ui = project_result(result)

    assert spoken.result_id == ui.result_id == result.result_id
    assert spoken.text == ui.text == result.text
    assert spoken.citations == ui.citations == result.citations
    assert spoken.timestamp == ui.timestamp == result.timestamp

    with pytest.raises(ValueError):
        project_result(result, spoken_text="It will definitely snow.")
