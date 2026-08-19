"""Deterministic smoke-harness tests: per-turn budget selection must not leak
a stale value from a previous turn into the next (plan Acceptance Criteria:
"Direct and delegated turns cannot reuse or overwrite one another's metric
identity in smoke-budget checks."; Edge Cases: "A direct turn immediately
following a slow delegated turn.").

These tests call the smoke harness's async entry points directly (bypassing
its subprocess/parent-child wrapper) against a fake, in-process SessionHost,
so they stay deterministic and credential-free.
"""

import asyncio

from scripts.eval_common import latest_turn_stage_metrics
from scripts.smoke_conversation import (
    ROUTING_REGRESSION_QUERIES,
    _run_routing_regression,
)
from server.config import Config
from server.contracts import Citation, GroundedResult, RoutingDecision
from server.perf_metrics import CollectingMeasurementSink
from server.pipeline import SessionHost


class _LifecycleRunner:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


class _TimedWorker:
    def __init__(self, delay_seconds: float, worker_id: str) -> None:
        self.delay_seconds = delay_seconds
        self.metadata = type(
            "Metadata", (), {"worker_id": worker_id, "topic": "weather", "model_policy": "deep"}
        )()

    async def search(
        self,
        query: str,
        *,
        turn_id: str,
        origin_epoch: int | None = None,
        clarification_context: object = None,
    ) -> GroundedResult:
        await asyncio.sleep(self.delay_seconds)
        return GroundedResult(
            result_id=f"result-{turn_id}",
            worker_id=self.metadata.worker_id,
            turn_id=turn_id,
            text=f"Answer for {query}",
            spoken_text=f"Answer for {query}",
            citations=[Citation(title="Source", url="https://example.com/source")],
            origin_epoch=origin_epoch,
        )


class _AlternatingCoordinator:
    """Direct greeting, then delegated turns with distinct, known delays."""

    def __init__(self, delays: list[float]) -> None:
        self._delays = iter(delays)
        self._pending_delay = 0.0
        self.config = Config()
        self.registry = type("Registry", (), {"workers": ()})()

    def arbitrate(self, _session_id: str, transcript: str) -> object:
        if transcript == ROUTING_REGRESSION_QUERIES[0]:
            return type(
                "Outcome",
                (),
                {
                    "kind": "routed",
                    "decision": RoutingDecision(action="direct", catalogue_version="catalogue-0"),
                    "prose": "Hello!",
                    "transcript": transcript,
                },
            )()
        self._pending_delay = next(self._delays)
        return type(
            "Outcome",
            (),
            {
                "kind": "routed",
                "decision": RoutingDecision(
                    action="existing_worker",
                    worker_id="worker-weather",
                    worker_type="web_search",
                    topic="weather",
                    model_policy="deep",
                    catalogue_version="catalogue-0",
                ),
                "prose": None,
                "transcript": transcript,
            },
        )()

    def dispatch(self, _decision: object) -> object:
        return _TimedWorker(self._pending_delay, "worker-weather")


async def _connected_host(
    coordinator: object, sink: CollectingMeasurementSink | None = None
) -> tuple[SessionHost, object]:
    host = SessionHost(
        runner_factory=_LifecycleRunner,
        coordinator=coordinator,
        measurement_sink=sink,
    )
    host.stt = None
    host.tts = None
    await host.start()
    connection = await host.connect(
        {
            "session_id": host.state.session_id,
            "resume_token": host.state.resume_token,
            "proposed_epoch": 1,
            "snapshot_sequence": 0,
        }
    )
    return host, connection


def test_routing_regression_smoke_does_not_leak_a_prior_turns_budget_into_the_next() -> None:
    async def run() -> None:
        # Two delegated turns with deliberately distinct search delays so a
        # leaked/stale prior-turn value would be detectable by comparing the
        # reported budget against each turn's own worker delay.
        coordinator = _AlternatingCoordinator([0.02, 0.2])
        sink = CollectingMeasurementSink()
        host, connection = await _connected_host(coordinator, sink)

        metrics = await _run_routing_regression(
            host,
            connection,
            sink,
            max_latency_seconds=5.0,
            max_routing_seconds=5.0,
        )

        assert metrics["scenario"] == "routing-regression"
        assert metrics["turns"] == 3
        assert metrics["actions"][0] == "direct"
        assert metrics["workers"][0] == "main"
        assert metrics["actions"][1:] == ["existing_worker", "existing_worker"]
        # A leaked/reused budget would make the fast (0.02s) and slow (0.2s)
        # delegated turns collapse onto the same reported total; the max
        # across turns must reflect the genuinely slow one.
        assert metrics["max_total_ms"] >= 150
        await host.shutdown()

    asyncio.run(run())


def test_direct_turn_between_delegated_turns_does_not_inherit_delegated_worker_identity() -> None:
    """A direct turn immediately following a slow delegated turn must report
    its own ``main`` worker identity and text, not the preceding turn's."""

    async def run() -> None:
        class DirectThenSlowCoordinator:
            def __init__(self) -> None:
                self.config = Config()
                self.registry = type("Registry", (), {"workers": ()})()

            def arbitrate(self, _session_id: str, transcript: str) -> object:
                if transcript == "search slowly":
                    return type(
                        "Outcome",
                        (),
                        {
                            "kind": "routed",
                            "decision": RoutingDecision(
                                action="existing_worker",
                                worker_id="worker-weather",
                                worker_type="web_search",
                                topic="weather",
                                model_policy="deep",
                                catalogue_version="catalogue-0",
                            ),
                            "prose": None,
                            "transcript": transcript,
                        },
                    )()
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "routed",
                        "decision": RoutingDecision(
                            action="direct", catalogue_version="catalogue-0"
                        ),
                        "prose": "Quick direct answer.",
                        "transcript": transcript,
                    },
                )()

            def dispatch(self, _decision: object) -> object:
                return _TimedWorker(0.2, "worker-weather")

        host, connection = await _connected_host(DirectThenSlowCoordinator())

        slow = await host._handle_transcript("search slowly", origin=connection)
        fast = await host._handle_transcript("what is your name", origin=connection)

        assert slow.worker_id == "worker-weather"
        assert fast.worker_id == "main"
        assert fast.text == "Quick direct answer."
        await host.shutdown()

    asyncio.run(run())


def test_direct_turn_stage_metrics_do_not_inherit_a_preceding_delegated_turns_search_ms() -> None:
    """``latest_turn_stage_metrics`` must select records by the caller's own
    ``turn_id``, not the newest record of each kind, so a direct turn (which
    emits no ``work_item_foreground`` record) never inherits the preceding
    delegated turn's ``search_ms``/``total_ms``."""

    async def run() -> None:
        class DirectThenSlowCoordinator:
            def __init__(self) -> None:
                self.config = Config()
                self.registry = type("Registry", (), {"workers": ()})()

            def arbitrate(self, _session_id: str, transcript: str) -> object:
                if transcript == "search slowly":
                    return type(
                        "Outcome",
                        (),
                        {
                            "kind": "routed",
                            "decision": RoutingDecision(
                                action="existing_worker",
                                worker_id="worker-weather",
                                worker_type="web_search",
                                topic="weather",
                                model_policy="deep",
                                catalogue_version="catalogue-0",
                            ),
                            "prose": None,
                            "transcript": transcript,
                        },
                    )()
                return type(
                    "Outcome",
                    (),
                    {
                        "kind": "routed",
                        "decision": RoutingDecision(
                            action="direct", catalogue_version="catalogue-0"
                        ),
                        "prose": "Quick direct answer.",
                        "transcript": transcript,
                    },
                )()

            def dispatch(self, _decision: object) -> object:
                return _TimedWorker(0.2, "worker-weather")

        sink = CollectingMeasurementSink()
        host, connection = await _connected_host(DirectThenSlowCoordinator(), sink)

        slow = await host._handle_transcript("search slowly", origin=connection)
        fast = await host._handle_transcript("what is your name", origin=connection)

        slow_metrics = latest_turn_stage_metrics(sink, 0.0, slow.turn_id)
        fast_metrics = latest_turn_stage_metrics(sink, 0.0, fast.turn_id)

        # The slow delegated turn actually searched, so it must report a
        # nonzero search_ms; a leaking helper would make the direct turn
        # report this same nonzero value instead of its own zero.
        assert slow_metrics["search_ms"] > 0.0
        assert fast_metrics["search_ms"] == 0.0
        await host.shutdown()

    asyncio.run(run())


class TestTurnCorrelatedRoutingAction:
    """Regression for round 8 gauntlet, Logic lens finding 1:
    ``_run_routing_regression`` previously read ``host.state.routing.action``
    with no correlation against the current turn's ``turn_id`` -- a stale
    prior-turn decision could be misread as this turn's own outcome. Mirrors
    ``scripts/eval_model_comparison.py``'s ``run_cell()`` staleness guard.
    """

    def test_returns_the_action_when_turn_ids_match(self) -> None:
        from scripts.smoke_conversation import _turn_correlated_routing_action

        routing = type("Routing", (), {"action": "existing_worker", "turn_id": "turn-1"})()
        assert _turn_correlated_routing_action(routing, "turn-1") == "existing_worker"

    def test_returns_none_for_a_stale_prior_turns_decision(self) -> None:
        from scripts.smoke_conversation import _turn_correlated_routing_action

        routing = type("Routing", (), {"action": "existing_worker", "turn_id": "turn-1"})()
        assert _turn_correlated_routing_action(routing, "turn-2") is None

    def test_returns_none_when_routing_is_none(self) -> None:
        from scripts.smoke_conversation import _turn_correlated_routing_action

        assert _turn_correlated_routing_action(None, "turn-1") is None
