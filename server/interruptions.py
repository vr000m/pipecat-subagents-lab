"""Stage-aware, task-local interruption policy."""

from __future__ import annotations

from .contracts import DeliveryState, InterruptionEvent


class InterruptionController:
    def __init__(self, policy: str = "pause_current_work_item") -> None:
        self.policy = policy
        self._pending: dict[str, object] = {}
        self._delivery: dict[str, DeliveryState] = {}

    def result_ready(self, result_id: str, *, work_item_id: str) -> None:
        self._pending[result_id] = (result_id, work_item_id)

    def pending_result(self, result_id: str) -> object | None:
        return self._pending.get(result_id)

    def interrupt(
        self, *, stage: str, work_item_id: str, run_id: str, utterance_id: str
    ) -> InterruptionEvent:
        template = self.policy == "resume_after_soft_interrupt" and stage == "speech_active"
        outcome = "resumed" if template else "declined"
        event = InterruptionEvent(
            interruption_id=f"interrupt-{utterance_id}",
            stage=stage,
            policy=self.policy,
            template_used=template,
            resume_outcome=outcome,
            work_item_id=work_item_id,
            run_id=run_id,
            utterance_id=utterance_id,
        )
        self._delivery[utterance_id] = DeliveryState.INTERRUPTED
        return event

    def mark_delivery(self, utterance_id: str, state: DeliveryState) -> DeliveryState:
        if self._delivery.get(utterance_id) in {
            DeliveryState.INTERRUPTED,
            DeliveryState.INTERRUPTED_BY_RECONNECT,
            DeliveryState.DELIVERY_COMPLETED,
            DeliveryState.DELIVERY_UNKNOWN,
        }:
            return self._delivery[utterance_id]
        self._delivery[utterance_id] = state
        return state

    def delivery_state(self, utterance_id: str) -> DeliveryState | None:
        return self._delivery.get(utterance_id)
