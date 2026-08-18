"""Shared query text for the router/worker model-comparison eval scenarios and
the live smoke CLI's equivalent scenarios.

Hoisted to its own module (out of ``scripts/_eval_common.py``) so the
``scripts -> evals`` / ``evals -> scripts`` dependency stays one-directional:
``evals/scenarios.py`` needs this text but must not import from ``scripts/``
to get it, since ``scripts/eval_model_comparison.py`` already imports
``evals.scenarios`` -- a two-way package-level coupling otherwise. Consumed by
both ``scripts/_eval_common.py`` (which re-exports these names so
``scripts/smoke_conversation.py``'s existing imports, including
``tests/test_smoke_conversation.py``, keep working unchanged) and
``evals/scenarios.py`` directly.
"""

from __future__ import annotations

DEFAULT_QUERY = "What is the latest stable Pipecat release?"
ROUTING_REGRESSION_QUERIES = (
    "Hi.",
    "Tell me the weather in Riga. For today.",
    "Could you tell me the weather in Helsinki today?",
)
