"""Coordinator-owned speech generation lifecycle.

Synthesis completion is not transport completion (`shared/protocol.md`).
This module gives every admitted utterance a private, token-fenced
``SpeechGeneration`` and puts one ``SpeechLifecycleCoordinator`` in sole
ownership of the TTS-lane lease, the single global transport slot, timers,
tombstones, and exactly-once terminalization. ``SpeechScheduler`` keeps
owning per-work queues and selection only; lifecycle processors and provider
callbacks are stateless adapters into the coordinator's token-bearing
methods below.

A private ``SpeechGenerationMarkerFrame`` is inserted immediately before
each generation's ``TTSSpeakFrame`` and is consumed by
``TransportSpeechLifecycleProcessor`` after TTS -- it never reaches
``transport.output()``. Fieldless ``BotStartedSpeakingFrame`` /
``BotStoppedSpeakingFrame`` events are applied only to the sole occupied
transport slot; they never establish identity by sampling whichever
scheduler lease happens to be active when they arrive.
"""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol
from uuid import uuid4

from pipecat.frames.frames import SystemFrame


class GenerationPhase(str, Enum):
    ADMITTED = "admitted"
    HANDED_TO_TTS = "handed_to_tts"
    SYNTHESIZING = "synthesizing"
    SYNTHESIS_ENDED = "synthesis_ended"
    TRANSPORT_STARTED = "transport_started"
    TRANSPORT_STOPPED = "transport_stopped"


class DeliveryDisposition(str, Enum):
    """Coordinator-private dispositions.

    The public `DeliveryState` wire/state contract in `server.contracts` is
    unchanged; `SessionHost`/`SpeechScheduler` map these onto it at the
    scheduler boundary.
    """

    PAUSED = "paused"
    INTERRUPTED = "interrupted"
    CANCELLED = "cancelled"
    DELIVERY_UNKNOWN = "delivery_unknown"


@dataclass(frozen=True)
class GenerationIdentity:
    """The minimal identity a coordinator generation is admitted for."""

    utterance_id: str
    work_item_id: str
    origin_epoch: int | None = None


@dataclass
class SpeechGeneration:
    """One admitted utterance's private lifecycle state.

    Every mutation is token-fenced by the coordinator methods below; nothing
    outside this module mutates a generation directly.
    """

    token: str
    identity: GenerationIdentity
    phase: GenerationPhase = GenerationPhase.ADMITTED
    disposition: DeliveryDisposition | None = None
    context_id: str | None = None
    audio_duration_seconds: float = 0.0
    audio_submitted: bool = False
    synthesis_ended_at: float | None = None
    cleanup_pending: bool = False
    tombstoned: bool = False
    terminalized: bool = False


@dataclass
class SpeechGenerationMarkerFrame(SystemFrame):
    """Private marker inserted immediately before a generation's TTSSpeakFrame.

    Consumed by `TransportSpeechLifecycleProcessor` after TTS; never
    forwarded to `transport.output()`.
    """

    token: str = ""
    utterance_id: str = ""
    work_item_id: str = ""
    origin_epoch: int | None = None


class Clock(Protocol):
    def now(self) -> float: ...


class TimerHandle(Protocol):
    def cancel(self) -> None: ...


class TimerScheduler(Protocol):
    def call_at(self, when: float, callback: Callable[[], Any]) -> TimerHandle: ...


class MonotonicClock:
    """Production clock backed by `time.monotonic`."""

    def now(self) -> float:
        return time.monotonic()


class EventLoopTimerScheduler:
    """Production timer scheduler backed by the running asyncio event loop."""

    def call_at(self, when: float, callback: Callable[[], Any]) -> TimerHandle:
        loop = asyncio.get_event_loop()
        delay = max(0.0, when - time.monotonic())
        return loop.call_later(delay, callback)


@dataclass
class _ManualTimerHandle:
    _scheduler: ManualTimerScheduler
    _seq: int

    def cancel(self) -> None:
        self._scheduler._cancel(self._seq)


class ManualTimerScheduler:
    """Deterministic clock plus timer scheduler for tests.

    Both monotonic reads and timer wakeups advance only when `advance()` is
    called; nothing here depends on wall-clock sleeps.
    """

    def __init__(self, start: float = 0.0) -> None:
        self._now = start
        self._pending: list[tuple[float, int, Callable[[], Any]]] = []
        self._seq = 0

    def now(self) -> float:
        return self._now

    def call_at(self, when: float, callback: Callable[[], Any]) -> TimerHandle:
        self._seq += 1
        self._pending.append((when, self._seq, callback))
        return _ManualTimerHandle(self, self._seq)

    def _cancel(self, seq: int) -> None:
        self._pending = [entry for entry in self._pending if entry[1] != seq]

    def advance(self, seconds: float) -> None:
        self._now += seconds
        self._fire_due()

    def _fire_due(self) -> None:
        due = sorted(
            (entry for entry in self._pending if entry[0] <= self._now),
            key=lambda entry: (entry[0], entry[1]),
        )
        for when, seq, callback in due:
            self._cancel(seq)
            callback()


TerminalCallback = Callable[[str, GenerationIdentity, DeliveryDisposition], Any]
CleanupCallback = Callable[[str, GenerationIdentity], Any]
TeardownCallback = Callable[[str, GenerationIdentity], Any]


class SpeechLifecycleCoordinator:
    """Sole owner of the admitted generation, TTS-lane lease, global
    transport slot, timers, tombstones, and exactly-once terminalization.
    """

    def __init__(
        self,
        *,
        clock: Clock | None = None,
        timers: TimerScheduler | None = None,
        speech_start_timeout_seconds: float = 10.0,
        speech_transport_grace_seconds: float = 1.0,
        on_terminal: TerminalCallback | None = None,
        dispatch_cleanup: CleanupCallback | None = None,
        dispatch_teardown: TeardownCallback | None = None,
        context_tombstone_limit: int = 256,
    ) -> None:
        self._clock = clock or MonotonicClock()
        self._timers = timers or EventLoopTimerScheduler()
        self._start_timeout = speech_start_timeout_seconds
        self._grace = speech_transport_grace_seconds
        self._on_terminal = on_terminal
        self._dispatch_cleanup = dispatch_cleanup
        self._dispatch_teardown = dispatch_teardown
        self._context_tombstone_limit = max(context_tombstone_limit, 0)

        self._generations: dict[str, SpeechGeneration] = {}
        self._slot_token: str | None = None
        self._context_tokens: dict[str, str] = {}
        self._context_tombstones: OrderedDict[str, None] = OrderedDict()
        self._timer_handles: dict[str, TimerHandle] = {}
        self._connection_closed = False
        self.connection_epoch = 0

    @property
    def occupied(self) -> bool:
        return self._slot_token is not None

    @property
    def slot_token(self) -> str | None:
        return self._slot_token

    def generation_for_token(self, token: str) -> SpeechGeneration | None:
        return self._generations.get(token)

    def token_for_context(self, context_id: str) -> str | None:
        return self._context_tokens.get(context_id)

    # -- admission and TTS handoff --

    def try_admit(self, identity: GenerationIdentity) -> SpeechGeneration | None:
        """Admit one generation to the sole global transport slot, or
        refuse when it is already occupied."""
        if self._connection_closed or self._slot_token is not None:
            return None
        token = uuid4().hex
        generation = SpeechGeneration(token=token, identity=identity)
        self._generations[token] = generation
        self._slot_token = token
        return generation

    def mark_handed_to_tts(self, token: str) -> None:
        generation = self._live(token)
        if generation is None:
            return
        generation.phase = GenerationPhase.HANDED_TO_TTS
        self._arm(token, self._clock.now() + self._start_timeout, self._on_start_timeout)

    def bind_context(self, token: str, context_id: str) -> bool:
        """Bind a generated TTS context only to its expected marker token.

        The connection-local TTS adapters use the scheduler utterance ID as
        their Pipecat context ID. A stale start therefore cannot consume a
        replacement marker, and a context already owned (or tombstoned) by
        another generation cannot be rebound.
        """
        generation = self._live(token)
        if generation is None or generation.tombstoned:
            return False
        if context_id != generation.identity.utterance_id:
            return False
        if generation.context_id is not None:
            return (
                generation.context_id == context_id
                and self._context_tokens.get(context_id) == token
            )
        owner = self._context_tokens.get(context_id)
        if owner is not None and owner != token:
            return False
        if context_id in self._context_tombstones:
            return False
        generation.context_id = context_id
        self._context_tokens[context_id] = token
        return True

    # -- provider synthesis lifecycle --

    def on_tts_started(self, context_id: str) -> bool:
        token = self._context_tokens.get(context_id)
        if token is None:
            return False
        generation = self._live(token)
        if generation is None or generation.tombstoned:
            return False
        self._cancel_timer(token)
        generation.phase = GenerationPhase.SYNTHESIZING
        return True

    def on_tts_audio(
        self, context_id: str, *, audio: bytes, sample_rate: int, num_channels: int
    ) -> bool:
        """Accumulate PCM duration for the drain deadline.

        Returns False when the context belongs to a stale or tombstoned
        generation, so the caller can drop the frame before output.
        """
        token = self._context_tokens.get(context_id)
        generation = self._generations.get(token) if token else None
        if generation is None or generation.terminalized or generation.tombstoned:
            return False
        sample_width = 2  # pipecat AudioRawFrame is fixed 16-bit PCM
        frames = len(audio) / (max(num_channels, 1) * sample_width)
        generation.audio_duration_seconds += frames / max(sample_rate, 1)
        generation.audio_submitted = True
        return True

    def on_tts_stopped(self, context_id: str) -> bool:
        """Record synthesis end. Non-terminal: does not clear the slot or
        admit the next generation. Returns False for a stale context."""
        token = self._context_tokens.get(context_id)
        if token is None:
            return False
        generation = self._generations.get(token)
        if generation is None or generation.terminalized or generation.tombstoned:
            return False
        generation.phase = GenerationPhase.SYNTHESIS_ENDED
        generation.synthesis_ended_at = self._clock.now()
        deadline = generation.synthesis_ended_at + generation.audio_duration_seconds + self._grace
        self._arm(token, deadline, self._on_drain_timeout)
        return True

    # -- transport lifecycle (fieldless bot frames) --

    def on_transport_bot_started(self) -> None:
        """A fieldless upstream start applies only to the sole occupied
        slot; it never samples which generation the scheduler considers
        active."""
        generation = self._occupied_generation()
        if generation is None:
            return
        generation.phase = GenerationPhase.TRANSPORT_STARTED

    def on_transport_bot_stopped(self) -> asyncio.Future[None] | None:
        """A fieldless upstream stop clears only the still-occupied same
        lane; a torn-down lane can no longer emit it.

        Synchronous like `record_interruption`: the phase mutation below
        runs immediately on the call, not only once awaited, so a caller
        that cannot await (a real frame processor mid-`process_frame`, or
        a test driving the coordinator without a frame pipeline) still
        observes it right away. The terminal transition is dispatched via
        the same fire-and-forget path as the timer-driven transitions; the
        returned future lets a caller that *can* await still observe
        completion (e.g. `await coordinator.on_transport_bot_stopped()`).
        """
        generation = self._occupied_generation()
        if generation is None:
            return self._schedule(self._noop())
        token = generation.token
        self._cancel_timer(token)
        generation.phase = GenerationPhase.TRANSPORT_STOPPED
        return self._schedule(self._terminalize(token, DeliveryDisposition.DELIVERY_UNKNOWN))

    @staticmethod
    async def _noop() -> None:
        return None

    # -- interruption / pause / errors --

    def record_interruption(self, token: str, *, pause: bool = False) -> None:
        """Barge-in or explicit pause: record the disposition and tombstone
        before the interruption is forwarded, cancel normal timers, and arm
        the interruption-cleanup deadline. Never clears the slot itself and
        never auto-advances the scheduler."""
        generation = self._live(token)
        if generation is None:
            return
        self._cancel_timer(token)
        generation.disposition = (
            DeliveryDisposition.PAUSED if pause else DeliveryDisposition.INTERRUPTED
        )
        generation.tombstoned = True
        forwarded_at = self._clock.now()
        self._arm(token, forwarded_at + self._grace, self._on_interruption_timeout)

    def release_flushed_lane(self, token: str) -> bool:
        """Synchronous fast-path for a caller that has itself already
        dispatched an immediate flush of the TTS lane (`SpeechScheduler`'s
        own `pause()`/`cancel()`, which are synchronous and cannot await
        `acknowledge_tts_lane_flush`) and knows no audio reached output.
        Frees the slot immediately rather than waiting out the
        interruption-cleanup grace deadline; the terminal callback still
        fires, just dispatched fire-and-forget rather than awaited here.
        A no-op once audio has been submitted -- that case must still wait
        for the real drain/teardown path."""
        generation = self._generations.get(token)
        if generation is None or generation.audio_submitted:
            return False
        result = self._terminalize_state(
            token, generation.disposition or DeliveryDisposition.DELIVERY_UNKNOWN
        )
        if result is None:
            return False
        self._schedule(self._call(self._on_terminal, token, result.identity, result.disposition))
        return True

    async def provider_error(self, token: str) -> None:
        """Both the generic upstream-`ErrorFrame` and local context-bearing
        error adapters converge on this transition."""
        await self._begin_delivery_unknown(token)

    async def acknowledge_tts_lane_flush(self, token: str) -> None:
        """The old TTS lane confirms cancel/flush completed with no audio
        submitted to output; the slot may now clear."""
        await self._acknowledge_tts_lane_flush(token)

    async def teardown_complete(self, token: str) -> None:
        """The connection-scoped output lane confirms it has been fully
        cancelled/torn down and can emit no later bot event."""
        await self._teardown_complete(token)

    def drop_stale_frame(self, context_id: str) -> bool:
        """True when a context-correlated frame belongs to a tombstoned or
        already-terminalized generation and must be dropped before output.

        Unbound contexts are not classified as stale here because this
        method is also a coordinator query. The post-TTS processor still
        fails closed by forwarding audio/stopped frames only when their
        token-bound transition method positively accepts them.
        """
        if context_id in self._context_tombstones:
            return True
        token = self._context_tokens.get(context_id)
        if token is None:
            return False
        generation = self._generations.get(token)
        return generation is None or generation.terminalized or generation.tombstoned

    def connection_closed(self) -> None:
        """Discard connection-scoped state after its worker/lane is closed."""
        for handle in self._timer_handles.values():
            handle.cancel()
        self._timer_handles.clear()
        self._context_tokens.clear()
        self._context_tombstones.clear()
        self._generations.clear()
        self._slot_token = None
        self._connection_closed = True

    # -- internal --

    def _live(self, token: str | None) -> SpeechGeneration | None:
        if token is None:
            return None
        generation = self._generations.get(token)
        if generation is None or generation.terminalized:
            return None
        return generation

    def _occupied_generation(self) -> SpeechGeneration | None:
        if self._slot_token is None:
            return None
        return self._generations.get(self._slot_token)

    def _arm(self, token: str, when: float, callback: Callable[[str], Any]) -> None:
        self._cancel_timer(token)
        self._timer_handles[token] = self._timers.call_at(
            when, lambda: self._schedule(callback(token))
        )

    def _cancel_timer(self, token: str) -> None:
        handle = self._timer_handles.pop(token, None)
        if handle is not None:
            handle.cancel()

    @staticmethod
    def _schedule(coroutine: Any) -> asyncio.Future[Any] | None:
        """Fire-and-forget one internal transition, returning the scheduled
        future so a caller that can await still observes completion.
        Callers that cannot await (no event loop, or a synchronous call
        site) yield to the loop once instead, e.g. `await
        asyncio.sleep(0)`."""
        try:
            return asyncio.ensure_future(coroutine)
        except RuntimeError:
            coroutine.close()
            return None

    async def _on_start_timeout(self, token: str) -> None:
        await self._begin_delivery_unknown(token)

    async def _on_drain_timeout(self, token: str) -> None:
        await self._begin_delivery_unknown(token)

    async def _on_interruption_timeout(self, token: str) -> None:
        generation = self._generations.get(token)
        if generation is None or generation.terminalized:
            return
        await self._begin_cleanup(generation)

    async def _begin_delivery_unknown(self, token: str) -> None:
        generation = self._generations.get(token)
        if generation is None or generation.terminalized:
            return
        generation.disposition = generation.disposition or DeliveryDisposition.DELIVERY_UNKNOWN
        # Tombstone before the first await in cleanup dispatch, mirroring
        # record_interruption: otherwise a straggling TTS start/audio frame
        # for this context can race the cleanup dispatch and reach output.
        generation.tombstoned = True
        await self._begin_cleanup(generation)

    async def _begin_cleanup(self, generation: SpeechGeneration) -> None:
        """Deadline expiry atomically marks the token cleanup_pending,
        dispatches cancellation/flush, then records the disposition once
        cleanup resolves.

        With no audio submitted to output, an acknowledged TTS-lane flush
        may release the slot. Once audio has crossed into output, expiry
        invalidates the connection and awaits `teardown_complete()`; queued
        speech is not admitted on the old lane.
        """
        if generation.cleanup_pending:
            return
        generation.cleanup_pending = True
        token = generation.token
        await self._call(self._dispatch_cleanup, token, generation.identity)
        if generation.audio_submitted:
            await self._call(self._dispatch_teardown, token, generation.identity)
        else:
            await self._acknowledge_tts_lane_flush(token)

    async def _acknowledge_tts_lane_flush(self, token: str) -> None:
        generation = self._generations.get(token)
        if generation is None or generation.audio_submitted:
            return
        await self._terminalize(
            token, generation.disposition or DeliveryDisposition.DELIVERY_UNKNOWN
        )

    async def _teardown_complete(self, token: str) -> None:
        generation = self._generations.get(token)
        if generation is None:
            return
        await self._terminalize(
            token, generation.disposition or DeliveryDisposition.DELIVERY_UNKNOWN
        )
        self.connection_epoch += 1

    async def _terminalize(self, token: str, disposition: DeliveryDisposition) -> None:
        generation = self._terminalize_state(token, disposition)
        if generation is None:
            return
        await self._call(self._on_terminal, token, generation.identity, generation.disposition)

    def _terminalize_state(
        self, token: str, disposition: DeliveryDisposition
    ) -> SpeechGeneration | None:
        """The synchronous half of terminalization: marks the generation
        terminal and frees the slot if it is the occupant. Split out so a
        caller that cannot await (`release_flushed_lane` below) can still
        free the slot immediately and defer only the terminal callback."""
        generation = self._generations.get(token)
        if generation is None or generation.terminalized:
            return None
        generation.terminalized = True
        generation.disposition = generation.disposition or disposition
        self._cancel_timer(token)
        if self._slot_token == token:
            self._slot_token = None
        # Reap the terminal generation itself so a long-lived connection's
        # per-utterance state does not grow without bound. Every other
        # accessor (`_live`, `generation_for_token`, `drop_stale_frame`)
        # already reads via `.get()` and treats a missing token as "not
        # found" -- popping here cannot turn one of those lookups into a
        # KeyError.
        #
        if generation.context_id is not None:
            if self._context_tokens.get(generation.context_id) == token:
                self._context_tokens.pop(generation.context_id, None)
            if self._context_tombstone_limit:
                self._context_tombstones[generation.context_id] = None
                self._context_tombstones.move_to_end(generation.context_id)
                while len(self._context_tombstones) > self._context_tombstone_limit:
                    self._context_tombstones.popitem(last=False)
        self._generations.pop(token, None)
        return generation

    @staticmethod
    async def _call(callback: Callable[..., Any] | None, *args: Any) -> None:
        if callback is None:
            return
        result = callback(*args)
        if asyncio.iscoroutine(result):
            await result


try:
    from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
except ImportError:  # pragma: no cover - dependency-free contract fallback
    FrameProcessor = object  # type: ignore[assignment,misc]
    FrameDirection = Any  # type: ignore[misc,assignment]


class TransportSpeechLifecycleProcessor(FrameProcessor):
    """Stateless adapter between TTS/transport frames and the coordinator.

    Installed after either TTS integration path and immediately before
    `transport.output()`. Observes the serialized marker, generated
    `TTSStartedFrame`/`TTSAudioRawFrame`/`TTSStoppedFrame`, and upstream bot
    start/stop, then calls token-bearing coordinator methods.
    """

    def __init__(self, coordinator: SpeechLifecycleCoordinator) -> None:
        super().__init__()
        self._coordinator = coordinator
        self._pending_token: str | None = None

    async def process_frame(self, frame: Any, direction: Any) -> None:
        from pipecat.frames.frames import (
            BotStartedSpeakingFrame,
            BotStoppedSpeakingFrame,
            TTSAudioRawFrame,
            TTSStartedFrame,
            TTSStoppedFrame,
        )

        await super().process_frame(frame, direction)

        if isinstance(frame, SpeechGenerationMarkerFrame):
            self._pending_token = frame.token
            return  # the marker never reaches transport.output()

        if direction == FrameDirection.DOWNSTREAM:
            if isinstance(frame, TTSStartedFrame):
                if not frame.context_id:
                    return
                if self._pending_token is not None:
                    if self._coordinator.bind_context(self._pending_token, frame.context_id):
                        self._pending_token = None
                if not self._coordinator.on_tts_started(frame.context_id):
                    return
            elif isinstance(frame, TTSAudioRawFrame):
                if not frame.context_id or not self._coordinator.on_tts_audio(
                    frame.context_id,
                    audio=frame.audio,
                    sample_rate=frame.sample_rate,
                    num_channels=frame.num_channels,
                ):
                    return
            elif isinstance(frame, TTSStoppedFrame):
                if not frame.context_id or not self._coordinator.on_tts_stopped(frame.context_id):
                    return

        if isinstance(frame, BotStartedSpeakingFrame):
            self._coordinator.on_transport_bot_started()
        elif isinstance(frame, BotStoppedSpeakingFrame):
            stopped_future = self._coordinator.on_transport_bot_stopped()
            if stopped_future is not None:
                await stopped_future

        await self.push_frame(frame, direction)


class GenericProviderErrorObserver(FrameProcessor):
    """Pre-TTS adapter for a generic, context-free Pipecat `ErrorFrame`.

    Captures the TTS-lane token when a generation's marker crosses this
    processor and attributes the next matching upstream error to that
    captured token plus processor identity; never infers identity from
    `ErrorFrame.context_id` because none exists on this ingress path.
    """

    def __init__(self, coordinator: SpeechLifecycleCoordinator, tts_processor: Any) -> None:
        super().__init__()
        self._coordinator = coordinator
        self._tts_processor = tts_processor
        self._captured_token: str | None = None

    async def process_frame(self, frame: Any, direction: Any) -> None:
        from pipecat.frames.frames import ErrorFrame

        await super().process_frame(frame, direction)
        if isinstance(frame, SpeechGenerationMarkerFrame):
            self._captured_token = frame.token
        elif (
            isinstance(frame, ErrorFrame)
            and direction == FrameDirection.UPSTREAM
            and frame.processor is self._tts_processor
            and self._captured_token is not None
        ):
            await self._coordinator.provider_error(self._captured_token)
            self._captured_token = None
        await self.push_frame(frame, direction)
