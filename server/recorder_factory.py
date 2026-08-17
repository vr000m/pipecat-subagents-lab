"""Telemetry recorder creation/registration owned by a SessionHost.

Extracted from SessionHost (deep-review Architecture finding #1: SessionHost
god class) as the "recorder factory" slice: constructing this session's
``AppTurnRecorder``/``RetainedRecorder`` instances against the host's fixed
``MeasurementSink``, registering a still-open retained recorder for later
late-result claiming, and the shutdown-time sweep that finalizes whatever is
still open.

``sink`` (the host's ``MeasurementSink``, resolved once for the host's whole
lifetime -- see ``SessionHost.measurement_sink``) and ``state`` (for
``state.session_id``, read fresh on every recorder construction rather than
copied) stay owned by SessionHost -- this class holds *references* to the
same objects, not copies. This mirrors ``WorkStatusPublisher``: a focused API
surface over state SessionHost still keeps for itself.

This module is a pure extraction of SessionHost's existing recorder-creation
and retained-recorder-registration logic -- semantics, including the
race-safe "register only if not already finalized" guard and the shutdown
sweep's finalize-then-drop ordering, are unchanged from the code that
previously lived directly on SessionHost.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

from .perf_metrics import AppTurnRecorder, MeasurementSink, RetainedRecorder, WorkOutcome

if TYPE_CHECKING:
    from .session_state import SessionState


class RecorderFactory:
    """Creates and tracks a SessionHost's telemetry recorders.

    Holds a reference to the owning SessionHost's ``sink`` (its fixed
    ``MeasurementSink``) and ``state`` (read only for ``state.session_id``) --
    not copies -- so this class stays in lockstep with the same objects
    SessionHost and its other slices read.
    """

    def __init__(self, *, sink: MeasurementSink, state: SessionState) -> None:
        self.sink = sink
        self.state = state
        self._retained_recorders: dict[str, RetainedRecorder] = {}

    def new_app_turn_recorder(self, *, origin_epoch: int, turn_id: str) -> AppTurnRecorder:
        return AppTurnRecorder(
            self.sink,
            session_id=self.state.session_id,
            origin_epoch=origin_epoch,
            turn_id=turn_id,
        )

    def new_retained_recorder(
        self, *, origin_epoch: int, turn_id: str, work_item_id: str, app_worker_id: str
    ) -> RetainedRecorder:
        return RetainedRecorder(
            self.sink,
            session_id=self.state.session_id,
            origin_epoch=origin_epoch,
            turn_id=turn_id,
            work_item_id=work_item_id,
            app_worker_id=app_worker_id,
        )

    def register_if_open(self, work_item_id: str, recorder: RetainedRecorder) -> None:
        """Register a provisional retained recorder only if it has not already
        raced to completion between dispatch and accepted retention."""
        if not recorder.finalized:
            self._retained_recorders[work_item_id] = recorder

    def pop(self, work_item_id: str) -> RetainedRecorder | None:
        """Claim and drop a registered retained recorder, if any.

        Popping (rather than peeking) makes a callback arriving after a
        recorder's finalization a structural no-op for that recorder -- see
        ``SessionHost.commit_late_result_once``'s use of this call.
        """
        return self._retained_recorders.pop(work_item_id, None)

    @staticmethod
    def make_late_terminal_handler(
        recorders: Mapping[str, RetainedRecorder],
    ) -> Callable[[str, WorkOutcome], None]:
        """Build a coordinator ``on_late_terminal`` callback that claims the
        matching retained recorder, if any, for a late-completing work item.

        Typed against the wider ``WorkOutcome`` rather than the coordinator's
        narrower ``TerminalKind``: by contravariance this still satisfies the
        coordinator's hook type, while remaining assignable wherever a full
        work outcome (including ``invalid_result``) is claimed.
        """

        def on_late_terminal(item_id: str, terminal_kind: WorkOutcome) -> None:
            recorder = recorders.get(item_id)
            if recorder is not None:
                recorder.claim(terminal_kind)

        return on_late_terminal

    def finalize_all(self) -> None:
        """Finalize every retained recorder still open and drop it.

        Called from ``SessionHost.shutdown`` once coordinator work/callback
        tasks have settled: unclaimed work is shutdown-cancelled, while
        claimed work uses its recorded terminal kind and whatever
        commit/speech stage it had already reached.
        """
        for work_item_id, recorder in tuple(self._retained_recorders.items()):
            recorder.finalize()
            self._retained_recorders.pop(work_item_id, None)
