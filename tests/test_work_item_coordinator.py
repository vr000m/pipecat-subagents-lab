"""Bounded work items preserve accepted order and isolate worker contexts."""

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from server.config import Config
from server.work_item_coordinator import (
    Coordinator,
    CoordinatorDefaults,
    OptionalCoordinator,
    PendingDialogue,
    WorkItemCoordinator,
    WorkItemFailure,
    coordinator_view,
)


class FakeSpeechScheduler:
    def __init__(self) -> None:
        self.spoken: list[str] = []

    async def speak(self, text: str, **_: object) -> None:
        self.spoken.append(text)


async def completed_worker(text: str) -> dict:
    return {"text": text, "citations": []}


def test_add_worker_clarification_records_a_pending_candidate_owned_by_the_worker() -> None:
    coordinator = WorkItemCoordinator(max_work_items_per_turn=2, clock=lambda: 100.0)

    coordinator.add_worker_clarification(
        session_id="session",
        worker_id="worker-1",
        turn_id="turn-1",
        result_id="result-1",
        original_query="What's the weather like?",
        question="Which location should I use?",
    )

    pending = coordinator.pending("session")
    assert pending is not None
    assert pending.owner_kind == "worker"
    assert pending.owner_id == "worker-1"
    assert pending.turn_id == "turn-1"
    assert pending.result_id == "result-1"
    assert pending.expires_at == 100.0 + coordinator.config.pending_dialogue_timeout_seconds
    assert pending.original_query == "What's the weather like?"
    assert pending.question == "Which location should I use?"


def test_natural_clarification_answer_continues_with_the_pending_worker() -> None:
    coordinator = WorkItemCoordinator(max_work_items_per_turn=2, clock=lambda: 0)
    pending = PendingDialogue(
        "session",
        "worker",
        "worker-1",
        "turn",
        "result",
        10,
        "What's the weather like?",
        "Which location should I use?",
    )
    coordinator.add_pending(pending)

    outcome = coordinator.arbitrate("session", "Riga")

    assert outcome.kind == "continue_pending"
    assert outcome.pending_dialogue == pending
    assert outcome.work_items == ("worker-1",)
    assert coordinator.pending("session") is None


def test_concurrent_turns_cannot_both_consume_one_pending_clarification() -> None:
    class Registry:
        config = Config()

        @staticmethod
        def catalogue() -> tuple[()]:
            return ()

    class Router:
        @staticmethod
        def route_envelope(_transcript: str, _catalogue: object) -> object:
            return type("Envelope", (), {"decision": object(), "prose": None})()

    coordinator = WorkItemCoordinator(registry=Registry(), router=Router(), clock=lambda: 0)
    coordinator.add_pending(PendingDialogue("session", "worker", "worker-1", "turn", "result", 10))
    classifier_started = threading.Event()
    release_classifier = threading.Event()
    classify = coordinator.pending_intent

    def slow_classify(transcript: str) -> str:
        classifier_started.set()
        release_classifier.wait(timeout=1)
        return classify(transcript)

    coordinator.pending_intent = slow_classify  # type: ignore[method-assign]
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(coordinator.arbitrate, "session", "Riga")
        assert classifier_started.wait(timeout=1)
        second = executor.submit(coordinator.arbitrate, "session", "Latvia")
        release_classifier.set()
        outcomes = (first.result(timeout=1), second.result(timeout=1))

    assert sorted(outcome.kind for outcome in outcomes) == ["continue_pending", "routed"]
    assert coordinator.pending("session") is None


def test_pending_new_topic_routes_normally_without_consuming_candidate() -> None:
    class Registry:
        config = Config()

        @staticmethod
        def catalogue() -> tuple[()]:
            return ()

    class Router:
        @staticmethod
        def route_envelope(_transcript: str, _catalogue: object) -> object:
            decision = object()
            return type("Envelope", (), {"decision": decision, "prose": None})()

    coordinator = WorkItemCoordinator(
        registry=Registry(),
        router=Router(),
        clock=lambda: 0,
    )
    pending = PendingDialogue("session", "worker", "worker-1", "turn", "result", 10)
    coordinator.add_pending(pending)

    outcome = coordinator.arbitrate("session", "What is the capital of India?")

    assert outcome.kind == "routed"
    assert coordinator.pending("session") == pending


def test_pending_multi_intent_carries_candidate_to_dispatch() -> None:
    coordinator = WorkItemCoordinator(max_work_items_per_turn=2, clock=lambda: 0)
    pending = PendingDialogue("session", "worker", "worker-1", "turn", "result", 10)
    coordinator.add_pending(pending)

    outcome = coordinator.arbitrate("session", "Riga and also search the news")

    assert outcome.kind == "multi_intent"
    assert outcome.pending_dialogue == pending
    assert outcome.work_items == ("Riga", "search the news")
    assert coordinator.pending("session") is None


def test_compound_pending_reply_is_classified_as_multi_intent() -> None:
    coordinator = WorkItemCoordinator(max_work_items_per_turn=2, clock=lambda: 0)
    coordinator.add_pending(PendingDialogue("session", "worker", "worker-1", "turn", "result", 10))
    outcome = coordinator.arbitrate("session", "yes, and also search for news")
    assert outcome.kind == "multi_intent"


@pytest.mark.parametrize(
    "answer",
    [
        "Riga and Latvia",
        "Tuesday and Wednesday",
        "Also include Celsius",
        "Yes, and use Reuters",
    ],
)
def test_conjunction_clarification_answers_continue_pending(answer: str) -> None:
    coordinator = WorkItemCoordinator(max_work_items_per_turn=2, clock=lambda: 0)
    coordinator.add_pending(PendingDialogue("session", "worker", "worker-1", "turn", "result", 10))

    outcome = coordinator.arbitrate("session", answer)

    assert outcome.kind == "continue_pending"
    assert outcome.work_items == ("worker-1",)


def test_multi_intent_split_preserves_conjunctions_in_the_clarification_answer() -> None:
    coordinator = WorkItemCoordinator(max_work_items_per_turn=2, clock=lambda: 0)
    coordinator.add_pending(PendingDialogue("session", "worker", "worker-1", "turn", "result", 10))

    outcome = coordinator.arbitrate(
        "session",
        "Riga and Latvia, and also search the latest stock news",
    )

    assert outcome.kind == "multi_intent"
    assert outcome.work_items == ("Riga and Latvia", "search the latest stock news")


def test_pending_imperative_new_topic_routes_normally_without_consuming_candidate() -> None:
    class Registry:
        config = Config()

        @staticmethod
        def catalogue() -> tuple[()]:
            return ()

    class Router:
        @staticmethod
        def route_envelope(_transcript: str, _catalogue: object) -> object:
            decision = object()
            return type("Envelope", (), {"decision": decision, "prose": None})()

    coordinator = WorkItemCoordinator(registry=Registry(), router=Router(), clock=lambda: 0)
    pending = PendingDialogue("session", "worker", "worker-1", "turn", "result", 10)
    coordinator.add_pending(pending)

    outcome = coordinator.arbitrate("session", "Search the latest stock news")

    assert outcome.kind == "routed"
    assert coordinator.pending("session") == pending


@pytest.mark.parametrize(
    "transcript",
    [
        "Can you search the latest stock news instead?",
        "Could you tell me today's headlines?",
        "Would you please look up the exchange rate?",
    ],
)
def test_pending_polite_new_topic_routes_normally_without_consuming_candidate(
    transcript: str,
) -> None:
    class Registry:
        config = Config()

        @staticmethod
        def catalogue() -> tuple[()]:
            return ()

    class Router:
        @staticmethod
        def route_envelope(_transcript: str, _catalogue: object) -> object:
            return type("Envelope", (), {"decision": object(), "prose": None})()

    coordinator = WorkItemCoordinator(registry=Registry(), router=Router(), clock=lambda: 0)
    pending = PendingDialogue("session", "worker", "worker-1", "turn", "result", 10)
    coordinator.add_pending(pending)

    outcome = coordinator.arbitrate("session", transcript)

    assert outcome.kind == "routed"
    assert coordinator.pending("session") == pending


def test_control_and_consent_outcomes_preserve_the_requested_action() -> None:
    coordinator = WorkItemCoordinator(max_work_items_per_turn=2, clock=lambda: 0)
    coordinator.add_pending(PendingDialogue("session", "worker", "worker-1", "turn", "result", 10))

    assert coordinator.arbitrate("session", "cancel").control_action == "cancel"
    consent = coordinator.arbitrate("session", "consent")
    assert consent.kind == "continue_pending"
    assert consent.pending_dialogue is not None


def test_constructor_overrides_preserve_pending_dialogue_timeout() -> None:
    coordinator = WorkItemCoordinator(
        config=Config(pending_dialogue_timeout_seconds=45),
        max_work_items_per_turn=3,
    )

    assert coordinator.config.pending_dialogue_timeout_seconds == 45
    assert coordinator.config.max_work_items_per_turn == 3


def test_control_targets_match_scheduler_work_item_ids() -> None:
    assert WorkItemCoordinator.control_intent("pause work-turn-1") == (
        "pause",
        "work-turn-1",
    )
    assert WorkItemCoordinator.control_intent("cancel work-item-2") == (
        "cancel",
        "work-item-2",
    )


@pytest.mark.parametrize(
    "transcript",
    [
        "stop signs near me",
        "cancel culture history",
        "resume writing tips",
    ],
)
def test_control_parser_does_not_consume_ordinary_queries(transcript: str) -> None:
    assert WorkItemCoordinator.control_intent(transcript) is None


def test_cancel_targets_only_the_selected_submitted_work_item() -> None:
    async def run() -> None:
        started = [asyncio.Event(), asyncio.Event()]
        cancelled: list[str] = []

        async def worker(_worker_id: str, text: str) -> dict:
            started[int(text)].set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.append(text)
                raise

        coordinator = WorkItemCoordinator(wait_timeout_ms=1)
        outcome = await coordinator.submit(
            "work-turn",
            [("worker-a", "0"), ("worker-b", "1")],
            worker,
        )
        await asyncio.gather(*(event.wait() for event in started))

        assert coordinator.cancel("work-turn-0") == ("work-turn-0",)
        await asyncio.sleep(0)
        assert cancelled == ["0"]
        assert outcome.pending_work_item_ids == ("work-turn-0", "work-turn-1")
        await coordinator.shutdown()

    asyncio.run(run())


def test_targeted_cancel_during_submit_preserves_completed_sibling() -> None:
    async def run() -> None:
        started = [asyncio.Event(), asyncio.Event()]
        release_sibling = asyncio.Event()

        async def worker(_worker_id: str, text: str) -> dict:
            started[int(text)].set()
            if text == "0":
                await asyncio.Future()
            await release_sibling.wait()
            return {"text": text, "citations": []}

        coordinator = WorkItemCoordinator(wait_timeout_ms=1_000)
        submission = asyncio.create_task(
            coordinator.submit(
                "work-turn",
                [("worker-a", "0"), ("worker-b", "1")],
                worker,
            )
        )
        await asyncio.gather(*(event.wait() for event in started))

        assert coordinator.cancel("work-turn-0") == ("work-turn-0",)
        release_sibling.set()
        outcome = await submission

        assert [result.text for result in outcome.results] == ["1"]
        assert outcome.pending_work_item_ids == ()
        assert outcome.failures == (
            WorkItemFailure(
                work_item_id="work-turn-0",
                worker_id="worker-a",
                error_type="CancelledError",
                error_message="worker execution was cancelled",
                failure_kind="cancelled",
            ),
        )
        await coordinator.shutdown()

    asyncio.run(run())


def test_same_worker_turns_are_causal_but_different_workers_can_run_concurrently() -> None:
    observed: list[str] = []

    async def worker(worker_id: str, text: str) -> dict:
        observed.append(f"start:{worker_id}:{text}")
        await asyncio.sleep(0)
        observed.append(f"end:{worker_id}:{text}")
        return await completed_worker(text)

    async def run() -> None:
        coordinator = WorkItemCoordinator(
            max_work_items_per_turn=2,
            speech_scheduler=FakeSpeechScheduler(),
        )
        await coordinator.submit("turn-a", [("worker-weather", "A")], worker)
        await coordinator.submit("turn-b", [("worker-weather", "B")], worker)
        await coordinator.submit("turn-c", [("worker-news", "C")], worker)

    asyncio.run(run())
    assert observed.index("end:worker-weather:A") < observed.index("start:worker-weather:B")


def test_multi_intent_is_bounded_and_timeout_keeps_completed_results() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(
            max_work_items_per_turn=2,
            wait_timeout_ms=1,
            speech_scheduler=FakeSpeechScheduler(),
        )

        async def worker(_worker_id: str, text: str) -> dict:
            if text == "slow":
                await asyncio.sleep(0.02)
            return await completed_worker(text)

        outcome = await coordinator.submit(
            "turn-1",
            [("worker-weather", "fast"), ("worker-news", "slow"), ("worker-extra", "ignored")],
            worker,
        )
        assert len(outcome.work_items) == 2
        assert outcome.results[0].text == "fast"
        assert outcome.pending_work_item_ids

    asyncio.run(run())


def test_cancelling_submission_does_not_launch_waiting_same_worker_item() -> None:
    async def run() -> None:
        release = asyncio.Event()
        first_started = asyncio.Event()
        calls: list[str] = []

        async def worker(_worker_id: str, text: str) -> dict:
            calls.append(text)
            if text == "first":
                first_started.set()
                await release.wait()
            return {"text": text, "citations": []}

        coordinator = WorkItemCoordinator(
            config=Config(multi_intent_wait_timeout_ms=1),
        )
        await coordinator.submit("turn-1", [("worker-1", "first")], worker)
        await first_started.wait()
        second = asyncio.create_task(coordinator.submit("turn-2", [("worker-1", "second")], worker))
        await asyncio.sleep(0)
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
        release.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert calls == ["first"]

    asyncio.run(run())


def test_timeout_boundary_completion_is_retained_after_wait_returns_stale_done_set(
    monkeypatch,
) -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(max_work_items_per_turn=2, wait_timeout_ms=1)
        real_wait = asyncio.wait

        async def stale_wait(tasks, *, timeout):
            await real_wait(tasks, timeout=timeout)
            # Model the event-loop boundary where the task has completed but
            # asyncio.wait returned before its completion was observed.
            await asyncio.gather(*tasks)
            return set(), set(tasks)

        monkeypatch.setattr(asyncio, "wait", stale_wait)

        async def worker(_worker_id: str, text: str) -> dict:
            return await completed_worker(text)

        outcome = await coordinator.submit("turn-boundary", [("worker", "done")], worker)
        assert [result.text for result in outcome.results] == ["done"]
        assert outcome.pending_work_item_ids == ()

    asyncio.run(run())


def test_immediate_worker_failure_is_retained_without_reordering_successes() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(max_work_items_per_turn=3, wait_timeout_ms=100)

        async def worker(worker_id: str, text: str) -> dict:
            if text == "fail":
                raise ValueError("provider detail should not be exposed")
            return {"text": text, "citations": []}

        outcome = await coordinator.submit(
            "turn-failure",
            [("worker-a", "first"), ("worker-b", "fail"), ("worker-c", "last")],
            worker,
        )

        assert [result.text for result in outcome.results] == ["first", "last"]
        assert len(outcome.failures) == 1
        assert outcome.failures[0].work_item_id == "turn-failure-1"
        assert outcome.failures[0].worker_id == "worker-b"
        assert outcome.failures[0].error_type == "ValueError"
        assert outcome.failures[0].error_message == "worker execution failed"
        assert outcome.failures[0].failure_kind == "failed"

    asyncio.run(run())


def test_worker_failure_classifies_via_failure_kind_regardless_of_exception_class_name() -> None:
    """A worker exception whose class name matches no known sentinel string
    must still classify as ``failed`` through the structured ``failure_kind``
    field, not through pattern-matching ``error_type`` against a hardcoded
    name list."""

    class SomeRenamedExceptionClassNotInAnySentinelList(Exception):
        pass

    async def run() -> None:
        coordinator = WorkItemCoordinator(max_work_items_per_turn=2, wait_timeout_ms=100)

        async def worker(worker_id: str, text: str) -> dict:
            if text == "fail":
                raise SomeRenamedExceptionClassNotInAnySentinelList("boom")
            return {"text": text, "citations": []}

        outcome = await coordinator.submit(
            "turn-rename", [("worker-a", "fail"), ("worker-b", "ok")], worker
        )

        assert len(outcome.failures) == 1
        assert outcome.failures[0].error_type == "SomeRenamedExceptionClassNotInAnySentinelList"
        assert outcome.failures[0].failure_kind == "failed"

    asyncio.run(run())


def test_timed_out_result_is_drained_once_after_pending_worker_finishes() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(max_work_items_per_turn=2, wait_timeout_ms=1)
        release = asyncio.Event()

        async def worker(worker_id: str, text: str) -> dict:
            assert worker_id == "worker-late"
            await release.wait()
            return {"text": text, "citations": []}

        outcome = await coordinator.submit("turn-late", [("worker-late", "answer")], worker)
        assert outcome.pending_work_item_ids == ("turn-late-0",)
        assert coordinator.drain_late_results() == ()

        release.set()
        for _ in range(4):
            await asyncio.sleep(0)

        late = coordinator.drain_late_results()
        assert len(late) == 1
        assert late[0].work_item_id == "turn-late-0"
        assert late[0].worker_id == "worker-late"
        assert late[0].result.text == "answer"
        assert late[0].error is None
        assert coordinator.drain_late_results() == ()

    asyncio.run(run())


def test_callback_delivered_late_result_does_not_accumulate_in_polling_queue() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(max_work_items_per_turn=2)
        release = asyncio.Event()
        delivered = asyncio.Event()
        observed = []

        async def provider() -> dict:
            await release.wait()
            return {"text": "answer", "citations": []}

        def on_complete(late: object) -> None:
            observed.append(late)
            delivered.set()

        task = asyncio.create_task(provider())
        assert coordinator.retain_late_task(
            task,
            work_item_id="work-1",
            worker_id="worker-1",
            on_complete=on_complete,
        )
        release.set()
        await delivered.wait()

        assert len(observed) == 1
        assert coordinator.drain_late_results() == ()
        await coordinator.shutdown()

    asyncio.run(run())


def test_background_provider_tasks_are_capped_per_coordinator() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(
            max_work_items_per_turn=2,
            max_background_tasks=2,
        )
        release = asyncio.Event()
        cancelled: list[str] = []

        async def provider(label: str) -> dict:
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.append(label)
                raise
            return {"text": label, "citations": []}

        tasks = [asyncio.create_task(provider(label)) for label in ("first", "second", "third")]
        await asyncio.sleep(0)
        for index, task in enumerate(tasks):
            coordinator.retain_late_task(
                task,
                work_item_id=f"work-{index}",
                worker_id=f"worker-{index}",
            )
        await asyncio.sleep(0)

        assert cancelled == ["third"]
        assert len(coordinator._late_tasks) == 2
        await coordinator.shutdown()

    asyncio.run(run())


def test_start_task_reserves_capacity_before_provider_coroutine_runs() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(max_background_tasks=1)
        release = asyncio.Event()
        second_started = False

        async def first() -> None:
            await release.wait()

        async def second() -> None:
            nonlocal second_started
            second_started = True

        first_task = coordinator.start_task(first())
        second_task = coordinator.start_task(second())

        assert first_task is not None
        assert second_task is None
        await asyncio.sleep(0)
        assert second_started is False

        release.set()
        await first_task
        await asyncio.sleep(0)
        await coordinator.shutdown()

    asyncio.run(run())


def test_rejected_cancellation_resistant_task_remains_owned_until_completion() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(max_background_tasks=1)
        release = asyncio.Event()
        ignored_cancellation = asyncio.Event()

        async def provider(*, resist: bool = False) -> dict:
            try:
                await release.wait()
            except asyncio.CancelledError:
                if not resist:
                    raise
                ignored_cancellation.set()
                await release.wait()
            return {"text": "done", "citations": []}

        retained = asyncio.create_task(provider())
        rejected = asyncio.create_task(provider(resist=True))
        await asyncio.sleep(0)
        assert coordinator.retain_late_task(
            retained,
            work_item_id="work-retained",
            worker_id="worker-retained",
        )
        assert not coordinator.retain_late_task(
            rejected,
            work_item_id="work-rejected",
            worker_id="worker-rejected",
        )
        await ignored_cancellation.wait()

        assert rejected in coordinator._cancelling_tasks
        release.set()
        await rejected
        await asyncio.sleep(0)
        assert rejected not in coordinator._cancelling_tasks
        await coordinator.shutdown()

    asyncio.run(run())


def test_shutdown_grace_is_bounded_for_cancellation_resistant_task() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(
            config=Config(shutdown_grace_seconds=0.01),
            max_background_tasks=1,
        )
        started = asyncio.Event()
        ignored_cancellation = asyncio.Event()
        release = asyncio.Event()

        async def provider() -> None:
            started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                ignored_cancellation.set()
                await release.wait()

        task = coordinator.start_task(provider())
        assert task is not None
        await started.wait()

        before = asyncio.get_running_loop().time()
        await coordinator.shutdown()
        elapsed = asyncio.get_running_loop().time() - before

        assert ignored_cancellation.is_set()
        assert elapsed < 0.2
        assert not task.done()
        release.set()
        await task

    asyncio.run(run())


def test_submit_releases_caller_ownership_when_call_returns() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator()

        async def provider(_worker_id: str, text: str) -> dict:
            return {"text": text, "citations": []}

        await coordinator.submit("turn-1", [("worker-1", "answer")], provider)

        assert asyncio.current_task() not in coordinator._submission_tasks
        assert coordinator._submission_tasks == set()
        await coordinator.shutdown()

    asyncio.run(run())


def test_late_result_polling_queue_is_bounded_per_coordinator() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(
            max_work_items_per_turn=2,
            max_background_tasks=2,
        )

        async def provider(label: str) -> dict:
            return {"text": label, "citations": []}

        for index in range(3):
            task = asyncio.create_task(provider(str(index)))
            coordinator.retain_late_task(
                task,
                work_item_id=f"work-{index}",
                worker_id=f"worker-{index}",
            )
            await task
            await asyncio.sleep(0)

        assert [late.work_item_id for late in coordinator.drain_late_results()] == [
            "work-1",
            "work-2",
        ]
        await coordinator.shutdown()

    asyncio.run(run())


def test_shutdown_cancels_retained_providers_and_callbacks_and_fences_mutation() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(max_work_items_per_turn=2)
        provider_started = asyncio.Event()
        provider_cancelled = asyncio.Event()
        callback_started = asyncio.Event()
        callback_cancelled = asyncio.Event()
        provider_release = asyncio.Event()
        callbacks = []

        async def blocking_provider() -> dict:
            provider_started.set()
            try:
                await provider_release.wait()
            except asyncio.CancelledError:
                provider_cancelled.set()
                raise
            return {"text": "late", "citations": []}

        async def completed_provider() -> dict:
            return {"text": "callback", "citations": []}

        async def blocking_callback(late: object) -> None:
            callbacks.append(late)
            callback_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                callback_cancelled.set()
                raise

        provider_task = asyncio.create_task(blocking_provider())
        coordinator.retain_late_task(
            provider_task,
            work_item_id="work-provider",
            worker_id="worker-provider",
            on_complete=lambda late: callbacks.append(late),
        )
        coordinator.retain_late_task(
            asyncio.create_task(completed_provider()),
            work_item_id="work-callback",
            worker_id="worker-callback",
            on_complete=blocking_callback,
        )
        await provider_started.wait()
        await callback_started.wait()

        await coordinator.shutdown()

        assert provider_cancelled.is_set()
        assert callback_cancelled.is_set()
        assert len(callbacks) == 1
        assert coordinator.drain_late_results() == ()

        post_shutdown_task = asyncio.create_task(asyncio.sleep(60))
        assert not coordinator.retain_late_task(
            post_shutdown_task,
            work_item_id="work-after-shutdown",
            worker_id="worker-after-shutdown",
        )
        await asyncio.sleep(0)
        assert post_shutdown_task.cancelled()
        with pytest.raises(RuntimeError, match="coordinator is shut down"):
            coordinator.add_pending(
                PendingDialogue("session", "worker", "worker", "turn", "result", 10)
            )

    asyncio.run(run())


def test_shutdown_cancels_active_submit_and_provider_tasks() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(max_work_items_per_turn=2)
        provider_started = asyncio.Event()
        provider_cancelled = asyncio.Event()

        async def provider(_worker_id: str, _text: str) -> dict:
            provider_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                provider_cancelled.set()
                raise

        submission = asyncio.create_task(
            coordinator.submit("turn-1", [("worker-1", "query")], provider)
        )
        await provider_started.wait()

        await coordinator.shutdown()

        assert provider_cancelled.is_set()
        assert submission.done()
        with pytest.raises(asyncio.CancelledError):
            await submission
        assert coordinator._submission_tasks == set()
        assert coordinator._submit_tasks == set()
        assert coordinator._provider_tasks == set()

    asyncio.run(run())


# --------------------------------------------------------------------------
# Phase 2: structured LateResult.terminal_kind and the synchronous
# on_late_terminal(work_item_id, terminal_kind) hook (plan's Retained
# Branch and Race Matrix / "shared multi-intent callback identity").
# --------------------------------------------------------------------------


def test_late_result_terminal_kind_is_completed_for_a_normal_result() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(max_work_items_per_turn=2)
        observed: list[object] = []

        async def provider() -> dict:
            return {"text": "answer", "citations": []}

        task = asyncio.create_task(provider())
        coordinator.retain_late_task(
            task,
            work_item_id="work-completed",
            worker_id="worker-1",
            on_complete=observed.append,
        )
        await task
        await asyncio.sleep(0)

        assert len(observed) == 1
        assert observed[0].terminal_kind == "completed"
        assert observed[0].error is None
        await coordinator.shutdown()

    asyncio.run(run())


def test_late_result_terminal_kind_is_failed_for_a_raised_exception() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(max_work_items_per_turn=2)
        observed: list[object] = []

        async def provider() -> dict:
            raise RuntimeError("provider exploded")

        task = asyncio.create_task(provider())
        coordinator.retain_late_task(
            task,
            work_item_id="work-failed",
            worker_id="worker-1",
            on_complete=observed.append,
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert len(observed) == 1
        assert observed[0].terminal_kind == "failed"
        assert observed[0].error is not None

    asyncio.run(run())


def test_late_result_terminal_kind_is_cancelled_for_a_cancelled_task() -> None:
    async def run() -> None:
        coordinator = WorkItemCoordinator(max_work_items_per_turn=2)
        observed: list[object] = []
        started = asyncio.Event()

        async def provider() -> dict:
            started.set()
            await asyncio.Future()

        task = asyncio.create_task(provider())
        coordinator.retain_late_task(
            task,
            work_item_id="work-cancelled",
            worker_id="worker-1",
            on_complete=observed.append,
        )
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert len(observed) == 1
        assert observed[0].terminal_kind == "cancelled"

    asyncio.run(run())


def test_on_late_terminal_hook_fires_synchronously_before_shutdown_suppresses_on_complete() -> None:
    """The plan requires ``on_late_terminal`` to be invoked as the first
    statement inside the completion callback, before the ``self._shutdown``
    guard can suppress ``on_complete`` — so it must fire even when the
    coordinator is shut down before the retained task's callback runs.
    """

    async def run() -> None:
        coordinator = WorkItemCoordinator(max_work_items_per_turn=2)
        claimed: list[tuple[str, str]] = []
        completed_calls: list[object] = []
        started = asyncio.Event()
        release = asyncio.Event()

        async def provider() -> dict:
            started.set()
            await release.wait()
            return {"text": "late", "citations": []}

        task = asyncio.create_task(provider())
        coordinator.retain_late_task(
            task,
            work_item_id="work-terminal-hook",
            worker_id="worker-1",
            on_complete=completed_calls.append,
            on_late_terminal=lambda work_item_id, terminal_kind: claimed.append(
                (work_item_id, terminal_kind)
            ),
        )
        await started.wait()

        # Flip the shutdown flag directly (rather than calling coordinator.shutdown(),
        # which would cancel the still-running task before it can complete) so the
        # completed() callback's `if self._shutdown: return` guard is exercised while
        # the task still finishes normally.
        coordinator._shutdown = True
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)

        assert claimed == [("work-terminal-hook", "completed")]
        assert completed_calls == []

    asyncio.run(run())


def test_raising_on_late_terminal_hook_does_not_block_cleanup_or_on_complete() -> None:
    """``on_late_terminal`` is telemetry-only: if the caller-supplied hook
    raises, the completion callback must still discard the task from
    bookkeeping and still deliver ``on_complete``/``LateResult`` — the hook
    failing must never drop the worker's completed result or leak task state.
    """

    async def run() -> None:
        coordinator = WorkItemCoordinator(max_work_items_per_turn=2)
        completed_calls: list[object] = []

        async def provider() -> dict:
            return {"text": "late", "citations": []}

        def raising_hook(_work_item_id: str, _terminal_kind: str) -> None:
            raise RuntimeError("hook exploded")

        task = asyncio.create_task(provider())
        coordinator.retain_late_task(
            task,
            work_item_id="work-terminal-hook-raises",
            worker_id="worker-1",
            on_complete=completed_calls.append,
            on_late_terminal=raising_hook,
        )
        await task
        await asyncio.sleep(0)

        assert task not in coordinator._late_tasks
        assert task not in coordinator._background_task_order
        assert len(completed_calls) == 1
        assert completed_calls[0].work_item_id == "work-terminal-hook-raises"
        assert completed_calls[0].worker_id == "worker-1"
        assert completed_calls[0].terminal_kind == "completed"
        assert completed_calls[0].result.text == "late"
        await coordinator.shutdown()

    asyncio.run(run())


def test_shared_multi_intent_callback_receives_distinct_late_results_per_item() -> None:
    """Multi-intent submission shares one ``on_late_complete`` callback across
    every decomposed work item; each invocation must carry that item's own
    identity and terminal kind, never a conflated/reused record.
    """

    async def run() -> None:
        coordinator = WorkItemCoordinator(max_work_items_per_turn=2, wait_timeout_ms=1)
        observed: list[object] = []

        async def slow_worker(worker_id: str, text: str) -> dict:
            if worker_id == "worker-b":
                raise RuntimeError("worker-b failed")
            await asyncio.sleep(0.05)
            return {"text": text, "citations": []}

        outcome = await coordinator.submit(
            "turn-shared",
            [("worker-a", "first"), ("worker-b", "second")],
            slow_worker,
            on_late_complete=observed.append,
            work_item_ids=["work-a", "work-b"],
        )

        assert set(outcome.pending_work_item_ids) | {f.work_item_id for f in outcome.failures} == {
            "work-a",
            "work-b",
        }

        for _ in range(20):
            await asyncio.sleep(0.01)
            if len(observed) >= 1:
                break

        assert observed
        for late in observed:
            assert late.work_item_id in {"work-a", "work-b"}
            if late.work_item_id == "work-b":
                assert late.terminal_kind == "failed"
            else:
                assert late.terminal_kind == "completed"
        assert len({id(late) for late in observed}) == len(observed)
        await coordinator.shutdown()

    asyncio.run(run())


# -- Phase 2: coordinator ownership boundary --------------------------------
#
# Plan bullet 213 / Integration Seams: "Keep commit ownership in
# SessionHost/SessionState; the coordinator only owns task retention/
# cancellation and callback delivery." WorkItemCoordinator must not gain a
# commit-marking or disposition-computing API of its own -- that stays on
# SessionHost.commit_late_result_once.


def test_work_item_coordinator_has_no_commit_or_disposition_api() -> None:
    """The coordinator remains delivery-only: it retains/cancels tasks and
    invokes on_complete, but never itself decides autoplay vs. display-only
    or performs a canonical commit."""
    forbidden_names = {
        "commit_late_result_once",
        "commit_result",
        "compute_disposition",
        "commit_and_autoplay",
    }
    coordinator_api = set(dir(WorkItemCoordinator))
    assert not (forbidden_names & coordinator_api)


def test_retain_late_task_on_complete_callback_receives_delivery_only_late_result() -> None:
    """The coordinator's on_complete callback is handed a plain LateResult
    (work_item_id/worker_id/result/error/terminal_kind) -- no disposition or
    commit-outcome field lives on that type; the host computes disposition
    separately from the delivery payload the coordinator hands it."""
    from server.work_item_coordinator import LateResult

    late = LateResult(work_item_id="work-1", worker_id="worker-1", result=None)
    disposition_like_fields = {"disposition", "commit_outcome", "autoplay"}
    assert not (disposition_like_fields & set(late.__dataclass_fields__))


def test_work_item_coordinator_satisfies_the_full_coordinator_surface() -> None:
    """``WorkItemCoordinator`` implements both the required ``Coordinator``
    members and the optional ``OptionalCoordinator`` members -- the combined
    surface ``server/pipeline.py`` relies on, whether accessed directly or
    through a ``getattr`` fallback."""
    coordinator = WorkItemCoordinator()
    for protocol in (Coordinator, OptionalCoordinator):
        for member_name in protocol.__protocol_attrs__:
            assert hasattr(coordinator, member_name), (
                f"WorkItemCoordinator is missing {protocol.__name__}.{member_name}"
            )


def test_coordinator_defaults_matches_pipeline_getattr_fallbacks() -> None:
    """``CoordinatorDefaults`` reproduces exactly the fallback values
    ``server/pipeline.py`` uses today when a coordinator omits an optional
    member, so a coordinator can opt into these defaults by subclassing
    instead of relying on scattered ``getattr`` fallbacks."""
    defaults = CoordinatorDefaults()
    assert defaults.registry is None
    assert defaults.config is None
    # Single-sourced with WorkItemCoordinator's own declaration and with
    # coordinator_view's fallback: an empty frozenset here would silently
    # widen SessionHost's config-conflict check for any coordinator that
    # followed this class's documented advice and subclassed it.
    assert defaults.OWNED_CONFIG_FIELDS == WorkItemCoordinator.OWNED_CONFIG_FIELDS
    assert coordinator_view(object()).OWNED_CONFIG_FIELDS == defaults.OWNED_CONFIG_FIELDS
    assert defaults.live_work_item_ids() == frozenset()
    assert defaults.cancel() == ()
    assert defaults.cancel("work-1") == ()

    async def run() -> None:
        task = defaults.start_task(completed_worker("hi"))
        assert isinstance(task, asyncio.Task)
        assert await task == {"text": "hi", "citations": []}
        await defaults.shutdown()

    asyncio.run(run())


def test_coordinator_view_resolves_bare_double_to_the_same_fallbacks() -> None:
    """``coordinator_view`` on a coordinator double with none of the 7
    optional members declared resolves to the same values the ``getattr``
    fallbacks in ``server/pipeline.py`` produce today."""

    class BareCoordinator:
        pass

    view = coordinator_view(BareCoordinator())
    assert view.registry is None
    assert view.config is None
    assert view.OWNED_CONFIG_FIELDS == WorkItemCoordinator.OWNED_CONFIG_FIELDS
    assert view.live_work_item_ids() == frozenset()
    assert view.cancel() == ()

    async def run() -> None:
        task = view.start_task(completed_worker("hi"))
        assert isinstance(task, asyncio.Task)
        await task
        await view.shutdown()

    asyncio.run(run())


def test_coordinator_view_prefers_a_conforming_coordinators_own_members() -> None:
    """A coordinator that declares the optional members wins over
    ``coordinator_view``'s fallbacks -- the view must not shadow a real
    implementation with a default."""
    coordinator = WorkItemCoordinator()
    view = coordinator_view(coordinator)
    assert view.registry is coordinator.registry
    assert view.OWNED_CONFIG_FIELDS == coordinator.OWNED_CONFIG_FIELDS
    assert view.cancel() == coordinator.cancel()


def test_wait_timeout_ms_override_applies_independently_of_max_work_items() -> None:
    """``wait_timeout_ms`` must take effect on its own, and must never
    clobber a caller-supplied ``Config.multi_intent_wait_timeout_ms`` just
    because a *different* constructor override (``max_work_items_per_turn``)
    was also passed."""
    only_wait_timeout = WorkItemCoordinator(wait_timeout_ms=5)
    assert only_wait_timeout.config.multi_intent_wait_timeout_ms == 5

    preserves_caller_config = WorkItemCoordinator(
        config=Config(multi_intent_wait_timeout_ms=7_000),
        max_work_items_per_turn=3,
    )
    assert preserves_caller_config.config.multi_intent_wait_timeout_ms == 7_000


def test_retain_late_task_force_schedules_callback_when_background_capacity_is_exhausted() -> None:
    """The completion callback for a retained task must still run when
    background capacity is exhausted, not be dropped into the
    ``_late_results`` polling queue -- nothing in production drains that
    queue (see ``drain_late_results``), so parking the callback there would
    silently break the at-least-once late-delivery guarantee."""

    async def run() -> None:
        coordinator = WorkItemCoordinator(max_background_tasks=1)

        async def worker() -> dict:
            return {"text": "hi", "citations": []}

        callback_ran = asyncio.Event()

        async def on_complete(_late: object) -> None:
            callback_ran.set()

        task = asyncio.create_task(worker())
        assert coordinator.retain_late_task(
            task,
            work_item_id="work-1",
            worker_id="worker-1",
            on_complete=on_complete,
        )

        # Saturate background capacity right before the retained task
        # completes, so the completion callback below has to schedule its
        # on_complete coroutine with `_has_background_capacity()` false.
        sentinel = asyncio.create_task(asyncio.sleep(60))
        coordinator._owned_tasks.add(sentinel)
        assert not coordinator._has_background_capacity()

        await task
        await asyncio.wait_for(callback_ran.wait(), timeout=1.0)

        # The callback ran directly; nothing was left in the polling queue.
        assert coordinator.drain_late_results() == ()

        coordinator._owned_tasks.discard(sentinel)
        sentinel.cancel()
        await asyncio.gather(sentinel, return_exceptions=True)
        await coordinator.shutdown()

    asyncio.run(run())


def test_retain_late_task_suppresses_callback_once_shutdown_has_begun() -> None:
    """Once shutdown has begun, a retained task's completion must not call
    ``on_complete`` at all (the force-adopt fix must not resurrect delivery
    after shutdown) -- there is no one left to observe it, so the result is
    neither run through the callback nor queued."""

    async def run() -> None:
        coordinator = WorkItemCoordinator()

        async def worker() -> dict:
            return {"text": "hi", "citations": []}

        callback_ran = asyncio.Event()

        async def on_complete(_late: object) -> None:
            callback_ran.set()

        task = asyncio.create_task(worker())
        assert coordinator.retain_late_task(
            task,
            work_item_id="work-1",
            worker_id="worker-1",
            on_complete=on_complete,
        )

        coordinator._shutdown = True
        await task
        await asyncio.sleep(0)

        assert not callback_ran.is_set()
        assert coordinator.drain_late_results() == ()

        coordinator._shutdown = False
        await coordinator.shutdown()

    asyncio.run(run())


def test_late_callback_task_does_not_consume_the_capacity_budget() -> None:
    """Round-2 confirm pass: the late-callback fix force-adopted the
    ``on_complete`` coroutine straight into ``_owned_tasks``, bypassing
    ``_has_background_capacity``'s check *and* landing inside the set it
    counts. At capacity, the owned set therefore grew past the cap and every
    subsequent legitimate ``start_task``/retain admission was refused until
    those callbacks finished.

    The callback is now admitted through ``start_task(..., mandatory=True)``,
    which keeps it outside the budget as well as past the gate.
    """

    async def run() -> None:
        coordinator = WorkItemCoordinator(max_background_tasks=1)
        callback_release = asyncio.Event()
        callback_started = asyncio.Event()

        async def slow_callback(_late: object) -> None:
            callback_started.set()
            await callback_release.wait()

        async def provider() -> dict:
            return {"text": "done", "citations": []}

        retained = asyncio.create_task(provider())
        assert coordinator.retain_late_task(
            retained,
            work_item_id="work-1",
            worker_id="worker-1",
            on_complete=slow_callback,
        )
        await retained
        await asyncio.sleep(0)
        await callback_started.wait()

        # The callback is owned (so it cannot be garbage-collected mid-flight)
        # but is not charged against the one-task budget.
        assert coordinator._mandatory_tasks, "the callback task must be tracked as mandatory"
        assert coordinator._has_background_capacity(), (
            "an in-flight mandatory callback must not exhaust the capacity budget"
        )

        follow_up = coordinator.start_task(provider())
        assert follow_up is not None, "a later legitimate admission must not be starved"
        await follow_up

        callback_release.set()
        await coordinator.shutdown()

    asyncio.run(run())


def test_late_callback_is_admitted_through_the_start_task_seam() -> None:
    """The inline ``asyncio.create_task`` + ``_adopt_task(force=True)`` at the
    call site duplicated ``start_task``'s body minus the capacity check, so a
    coordinator subclass overriding ``start_task`` was silently not honoured
    on this path (and the inline call lost ``start_task``'s ``operation.close()``
    handling and its tolerance for non-coroutine awaitables)."""

    admitted: list[bool] = []

    class RecordingCoordinator(WorkItemCoordinator):
        def start_task(self, operation, *, mandatory: bool = False):  # type: ignore[no-untyped-def]
            admitted.append(mandatory)
            return super().start_task(operation, mandatory=mandatory)

    async def run() -> None:
        coordinator = RecordingCoordinator(max_background_tasks=1)
        seen: list[object] = []

        async def callback(late: object) -> None:
            seen.append(late)

        async def provider() -> dict:
            return {"text": "done", "citations": []}

        retained = asyncio.create_task(provider())
        coordinator.retain_late_task(
            retained,
            work_item_id="work-1",
            worker_id="worker-1",
            on_complete=callback,
        )
        await retained
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert admitted == [True], "the late callback must route through the start_task seam"
        assert len(seen) == 1
        await coordinator.shutdown()

    asyncio.run(run())
