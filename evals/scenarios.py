"""Ordered-turn scenarios for the router/worker model-comparison eval matrix.

Each scenario is an ordered list of turns with **per-turn** judge criteria
(rather than one criterion for the whole scenario), because
``pipecat.evals.judge.EvalJudge.evaluate()`` only scores the *most recent*
reply -- a scenario-wide single ``evaluate()`` call at the end would silently
ignore every earlier turn's quality (see the dev plan's Codex-review finding
on this).

The ack-ordering query (``scripts/smoke_conversation.py``'s
``_run_ack_ordering``) is deliberately **out of scope** for this matrix: it
requires TTS recording, scheduler admission, and lifecycle-driving assertions
the generic sequential transcript loop here cannot reproduce, and it tests
structural ack/result ordering -- not model quality.
"""

from __future__ import annotations

from dataclasses import dataclass

from evals.queries import DEFAULT_QUERY, ROUTING_REGRESSION_QUERIES


@dataclass(frozen=True)
class Turn:
    """One turn in a scenario's ordered transcript.

    ``judge_criterion`` is time-robust prose (e.g. "names a specific weather
    condition or temperature"), never "gives the correct current weather" --
    the eval suite has no ground truth for what today's actual weather is.

    ``expect_action`` is a deterministic (non-judge) assertion against
    ``host.state.routing.action`` after this turn, when set.

    ``expect_delegated`` marks a turn the runner expects to route to the
    worker: it drives both the post-first-delegation worker
    model/effort assertion and the deterministic non-empty-citations check
    (never a judge criterion -- ``spoken_text``'s contract forbids citation
    markers/URLs, so asking the judge to check for a citation would be
    self-contradictory).
    """

    query: str
    judge_criterion: str | None = None
    expect_action: str | None = None
    expect_delegated: bool = False


@dataclass(frozen=True)
class Scenario:
    name: str
    turns: tuple[Turn, ...]


SINGLE_TURN_DEFAULT = Scenario(
    name="single-turn-default",
    turns=(
        Turn(
            query=DEFAULT_QUERY,
            judge_criterion=(
                "The reply states a specific Pipecat release version number and does not "
                "claim to be unable to answer or refuse the request."
            ),
            expect_delegated=True,
        ),
    ),
)


# Reuses ROUTING_REGRESSION_QUERIES verbatim from evals/queries.py (which
# scripts/smoke_conversation.py's own routing-regression scenario also draws
# from, via scripts/eval_common.py's re-export) -- 3 turns, not 2 (a prior
# draft of this plan miscounted).
ROUTING_REGRESSION = Scenario(
    name="routing-regression",
    turns=(
        # Turn 1: a bare greeting. Deterministic assertion only -- no judge
        # criterion, since "was this handled directly" is a routing-shape
        # fact, not a semantic-quality judgment.
        Turn(
            query=ROUTING_REGRESSION_QUERIES[0],
            expect_action="direct",
            expect_delegated=False,
        ),
        # Turns 2/3: time-robust criteria -- "names a specific condition or
        # temperature for the named city", not "gives the correct current
        # weather" (this runner has no ground truth for today's actual
        # weather, and the candidate models are evaluated on different days).
        Turn(
            query=ROUTING_REGRESSION_QUERIES[1],
            judge_criterion=(
                "The reply names a specific weather condition or temperature for Riga."
            ),
            expect_delegated=True,
        ),
        Turn(
            query=ROUTING_REGRESSION_QUERIES[2],
            judge_criterion=(
                "The reply names a specific weather condition or temperature for Helsinki."
            ),
            expect_delegated=True,
        ),
    ),
)


SCENARIOS: tuple[Scenario, ...] = (SINGLE_TURN_DEFAULT, ROUTING_REGRESSION)

SCENARIOS_BY_NAME: dict[str, Scenario] = {scenario.name: scenario for scenario in SCENARIOS}
