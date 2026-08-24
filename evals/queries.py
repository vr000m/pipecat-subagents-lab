"""Shared query text for the router/worker model-comparison eval scenarios and
the live smoke CLI's equivalent scenarios.

Hoisted to its own module (out of ``scripts/eval_common.py``) so the
``scripts -> evals`` / ``evals -> scripts`` dependency stays one-directional:
``evals/scenarios.py`` needs this text but must not import from ``scripts/``
to get it, since ``scripts/eval_model_comparison.py`` already imports
``evals.scenarios`` -- a two-way package-level coupling otherwise. Consumed
directly by its two consumers, ``evals/scenarios.py`` and
``scripts/smoke_conversation.py`` -- round 8 gauntlet (Architecture finding
12) removed the ``scripts/eval_common.py`` re-export hop this docstring used
to describe, since it was a needless middle hop with no consumer that
actually needed it (round 9 gauntlet, Architecture lens finding 20).
"""

from __future__ import annotations

DEFAULT_QUERY = "What is the latest stable Pipecat release?"
ROUTING_REGRESSION_QUERIES = (
    "Hi.",
    "Tell me the weather in Riga. For today.",
    "Could you tell me the weather in Helsinki today?",
)
