"""Shared OpenAI Responses structured-output helpers."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


def strict_json_schema(value: Any) -> Any:
    """Normalize Pydantic defaults for strict Responses JSON schemas."""
    if isinstance(value, list):
        return [strict_json_schema(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {key: strict_json_schema(item) for key, item in value.items()}
    properties = result.get("properties")
    if isinstance(properties, dict):
        result["required"] = list(properties)
    result.pop("default", None)
    return result


def structured_text_format(model: type[BaseModel], name: str) -> dict[str, Any]:
    """Build a strict Responses ``text.format`` configuration."""
    return {
        "format": {
            "type": "json_schema",
            "name": name,
            "strict": True,
            "schema": strict_json_schema(model.model_json_schema()),
        }
    }
