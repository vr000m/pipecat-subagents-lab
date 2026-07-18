"""SessionHost keeps durable workers separate from connection workers."""

import asyncio

from server.contracts import DeliveryState, GroundedResult, WorkerState
from server.pipeline import SessionHost


class FakeRunner:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.workers: list[object] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


def test_start_replacement_and_shutdown_preserve_process_lifetime_worker_registry() -> None:
    async def run() -> None:
        runner = FakeRunner()
        host = SessionHost(runner_factory=lambda: runner)
        await host.start()
        first = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 1,
                "snapshot_sequence": 0,
            }
        )
        second = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 2,
                "snapshot_sequence": 0,
            }
        )
        await host.shutdown()

        assert runner.started and runner.stopped
        assert runner.workers == []
        assert first.epoch < second.epoch
        assert first.active is False
        assert second.active is False
        assert host.connection is None

    asyncio.run(run())


def test_replacement_interrupts_only_old_connection_speech_and_keeps_result_history() -> None:
    async def run() -> None:
        host = SessionHost()
        host.state.set_worker(
            WorkerState(
                worker_id="worker-weather", topic="weather", model_policy="deep", status="idle"
            )
        )
        first = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 1,
                "snapshot_sequence": 0,
            }
        )
        item = first.scheduler.enqueue(
            result_id="result-1",
            work_item_id="work-1",
            run_id="run-1",
            text="Answer",
            origin_epoch=1,
        )
        await first.scheduler.start_next()
        host.state.append_result(
            GroundedResult(
                result_id="result-1",
                worker_id="worker-weather",
                turn_id="turn-1",
                text="Answer",
                spoken_text="Answer",
                ui_text="Answer",
            ),
            origin_epoch=1,
        )

        second = await host.connect(
            {
                "session_id": host.state.session_id,
                "resume_token": host.state.resume_token,
                "proposed_epoch": 2,
                "snapshot_sequence": 0,
            }
        )

        assert first.active is False
        assert host.accepts(2)
        assert not host.accepts(1)
        assert host.state.speech[item.utterance_id].state == DeliveryState.INTERRUPTED_BY_RECONNECT
        assert [result.result_id for result in host.state.result_history("worker-weather")] == [
            "result-1"
        ]
        assert second.scheduler.active is None

    asyncio.run(run())
