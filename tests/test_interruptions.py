"""Interruption races have explicit stage, policy, and terminal precedence."""

import pytest

from server.contracts import DeliveryState, InterruptionEvent
from server.interruptions import InterruptionController


@pytest.mark.parametrize(
    ("stage", "template"),
    [("result_pending", False), ("voice_llm_accepted", False), ("speech_active", True)],
)
def test_stage_aware_soft_interrupt_only_uses_bridge_after_speech_started(
    stage: str, template: bool
) -> None:
    controller = InterruptionController(policy="resume_after_soft_interrupt")
    event = controller.interrupt(
        stage=stage,
        work_item_id="work-1",
        run_id="run-1",
        utterance_id="utt-1",
    )

    assert isinstance(event, InterruptionEvent)
    assert event.template_used is template
    assert event.resume_outcome == ("resumed" if template else "declined")


def test_result_pending_is_retained_without_bridge_template_and_barge_in_preserves_result() -> None:
    controller = InterruptionController(policy="pause_current_work_item")
    controller.result_ready("result-1", work_item_id="work-1")
    event = controller.interrupt(
        stage="result_pending",
        work_item_id="work-1",
        run_id="run-1",
        utterance_id="utt-1",
    )

    assert event.template_used is False
    assert controller.pending_result("result-1") is not None
    assert controller.delivery_state("utt-1") == DeliveryState.INTERRUPTED


def test_duplicate_and_late_terminal_events_cannot_override_first_terminal_outcome() -> None:
    controller = InterruptionController(policy="pause_current_work_item")
    controller.mark_delivery("utt-1", DeliveryState.STARTED)
    controller.mark_delivery("utt-1", DeliveryState.INTERRUPTED)
    controller.mark_delivery("utt-1", DeliveryState.DELIVERY_COMPLETED)
    controller.mark_delivery("utt-1", DeliveryState.INTERRUPTED)

    assert controller.delivery_state("utt-1") == DeliveryState.INTERRUPTED


def test_task_local_interruption_preserves_pending_results_for_unrelated_work_items() -> None:
    controller = InterruptionController(policy="pause_current_work_item")
    controller.result_ready("result-2", work_item_id="work-2")
    controller.result_ready("result-3", work_item_id="work-3")

    controller.mark_delivery("utt-1", DeliveryState.STARTED)
    event = controller.interrupt(
        stage="speech_active",
        work_item_id="work-1",
        run_id="run-1",
        utterance_id="utt-1",
    )

    assert event.work_item_id == "work-1"
    assert controller.delivery_state("utt-1") == DeliveryState.INTERRUPTED
    assert controller.pending_result("result-2") is not None
    assert controller.pending_result("result-3") is not None
