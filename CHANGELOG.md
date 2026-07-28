# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.2] - 2026-07-28

### Added

- Grep-friendly `PERF_METRIC` console telemetry contract: one single-line
  `key=value` record per event, with a stable event registry, closed outcome
  enums, and unknown-field/unknown-event rejection.
- Pipecat 1.6.0 `StartupTimingObserver` and `UserBotLatencyObserver` wired into
  each browser connection's `PipelineWorker`, alongside the framework's default
  `TurnTrackingObserver`, reporting pipeline startup, transport readiness,
  Pipecat turn start/end, interruption, first-bot-speech latency, user-to-bot
  latency, and available per-service metric breakdowns.
- Application-owned foreground timing: one `app_turn_foreground` event per
  accepted semantic turn and one `work_item_foreground` event per dispatched
  child, covering direct, unsupported, control, clarification, decline,
  completed, mixed, retained, and failed/cancelled outcomes across
  `_handle_transcript_impl`, `_handle_pending`, and `_handle_multi_intent`.
- Application-owned retained-work timing: one correlated `work_item_background`
  terminal event per registered retained item, reporting independent
  `work_outcome`, `commit_outcome`, and `speech_outcome` axes so a successful
  search whose result is suppressed or cannot be spoken is not mislabeled as a
  failed search.
- Injectable measurement sink (`ConsoleMeasurementSink` production default,
  `CollectingMeasurementSink` for tests and the paid smoke harness) owned once
  by `SessionHost` for its process lifetime.

### Changed

- Migrate the paid conversation smoke harness off `SessionHost.last_turn_metrics`
  onto an injected `CollectingMeasurementSink`, so a direct turn can no longer
  inherit a preceding delegated turn's stale latency budget.
- Adopt `mypy` in CI, gated on `server/perf_metrics.py`, `server/pipeline.py`,
  and `server/work_item_coordinator.py`. The `PERF_METRIC` outcome vocabularies
  are now `Literal`-alias-derived rather than hand-duplicated frozensets, so a
  typo or future rename in an outcome literal fails CI instead of silently
  dropping a metric in production.
- Invert the `mypy` gate from an allowlist (`server.*` ignored by default,
  three modules un-ignored) to a denylist (`server.*` checked by default, an
  explicit legacy-debt list of the 12 modules with pre-existing errors
  exempted). A newly added `server/*.py` file is now gated automatically
  instead of silently unchecked. Behavior-neutral: `uv run mypy` output is
  unchanged.

### Fixed

- Suppress RTVI metrics forwarding to the browser client
  (`RTVIObserverParams(metrics_enabled=False)`); `enable_metrics=True` no
  longer leaks `MetricsFrame`s over the wire protocol, closing a gap between
  this feature's console-only scope and its actual behavior.
- Revert `SessionHost`'s default `WorkItemCoordinator` construction; a bare
  `SessionHost(...)` again leaves `coordinator=None` instead of silently
  acquiring a router-less coordinator that would fail on non-control turns.
- Classify work-item failures from the structured `failure_kind` field
  instead of pattern-matching the free-text `error_type` (exception class
  name), which could mislabel a worker exception merely named after a known
  sentinel string.
- Make `AppTurnRecorder.finalize` total: no argument combination (an empty
  multi-intent fan-out, a control turn with no resolvable action) can latch
  the recorder as finalized while silently skipping emission.
- Give `AppTurnRecorder` ownership of its child `WorkItemRecorder`s: a parent
  finalize now sweeps any child left open by a cancelled turn or a raising
  commit/speak step, instead of under-reporting `child_count`.
- Harden the multi-intent fan-in against a worker returning an unexpected
  `turn_id` or `work_item_id`: the turn now degrades gracefully (partial
  results still commit and speak; only the unattributable item drops, with a
  warning) instead of the entire turn silently failing.
- Validate `AppTurnRecorder.finalize`'s `outcome` and `WorkItemRecorder
  .finalize`'s `outcome` against their closed vocabularies before emitting,
  degrading an out-of-vocabulary value to `"failed"` instead of letting the
  record drop silently inside `_safe_emit` after the recorder was already
  latched finalized (no retry possible).
- Classify `_failure_child_outcome` directly from `failure.failure_kind`
  instead of a defensive `getattr` default, restoring the static type
  guarantee `mypy` proves in `server.pipeline`; the runtime membership check
  is kept, since the coordinator seam is untyped and a duck-typed caller can
  still supply an off-domain kind.
- Attribute the multi-intent fan-in's `work_item_foreground` record to the
  actually-committed result on a duplicate `turn_id`, instead of the first
  (discarded) one, so telemetry and committed content never diverge.
- Decouple `AppTurnRecorder`'s child-outcome counting from the finalize
  sweep's open-child tracking, so a caller-bug duplicate `work_item_id` no
  longer leaves an emitted `work_item_foreground` record uncounted.
- Split the control-turn acknowledgement text into two vocabulary-correct
  lookup tables (`ControlAction`, `ControlOutcome`) instead of one table
  keyed by their union with a mid-function variable reassignment.

## [0.1.1] - 2026-07-26

### Added

- Required paid 0.1.1 routing-regression release gate covering a greeting
  followed by two weather requests against one live session.

### Fixed

- Route greetings, thanks, acknowledgements, goodbyes, and casual conversation
  directly instead of creating persistent web-search workers.
- Preserve immutable existing-worker catalogue metadata during routing so a new
  transcript cannot mutate a worker's topic and trigger a false routing outage.
- Queue same-epoch late results exactly once for deferred TTS without interrupting
  active speech; stale-epoch and TTS-less results remain display-only and are
  labelled `background result`.

## [0.1.0] - 2026-07-26

### Added

- Browser-first Pipecat voice-assistant lab using Small WebRTC and RTVI.
- Persistent, context-owning web-search workers backed by OpenAI hosted search.
- Structured routing across direct, unsupported, clarification, existing-worker,
  and new-worker outcomes.
- Canonical grounded results with separate complete display text and concise TTS
  projections.
- Server-authored transcript, routing, worker, result-history, delivery-state,
  timestamp, and reconnect projections.
- Local Nemotron STT and Pocket TTS integrations, with independently selectable
  Deepgram and Cartesia hosted alternatives.
- Smart Turn aggregation with configurable incomplete-turn timeout and
  complete-turn grace period.
- Repeatable local-versus-hosted speech latency benchmark.
- Credential-safe Python and browser contract, integration, and regression tests.
- Credential-free real-process server smoke covering health, built browser
  assets, origin enforcement, session discovery, and clean shutdown.
- Local-only TTS-to-STT media smoke mode for the existing speech benchmark.
- Bounded paid conversation smoke covering the real router, worker, structured
  display/speech projections, normalized citations, and explicit routing,
  search, and total latency budgets without printing content.
- Stable architecture and browser-protocol references covering runtime
  ownership, provider boundaries, reconnect fencing, and deferred contracts.

### Fixed

- Wire worker clarification into pending-dialogue continuation: a web-search
  worker's clarifying question (`WorkerClarify`) is now caught at every
  pipeline dispatch site and recorded with its original request. Natural
  answers resume the same worker through typed continuation context, compound
  replies retain the pending owner, and explicit unrelated requests route
  normally. Weather time and temperature modifiers no longer masquerade as
  locations. Adds the TOML/environment
  `pending_dialogue_timeout_seconds` setting, defaulting to 30 seconds.
- Keep a bounded set of timed-out worker searches alive for late UI delivery
  without autoplay, give each late result one delivery owner, cancel retained
  work during session shutdown, propagate caller cancellation, and redact
  provider exception details from operational logs.
- Stop active TTS before speaking pause or cancellation confirmation.
- Keep router prompts, decisions, and prose request-local so concurrent turns
  cannot observe another turn's prompt or response text.
- Complete only unambiguous nullable fields in a public-web `new_worker`
  structured route, while continuing to reject explicit unsupported values.
- Bound direct, pending, and multi-intent searches with truthful
  completed/retained/rejected ownership outcomes; capacity rejection no longer
  claims that work will continue in the background.
- Reserve coordinator capacity before provider work starts, release submission
  callers when calls return, and bound shutdown when cancellation-resistant
  work outlives its grace period.
- Use cancellable asynchronous hosted-search clients with provider deadlines,
  cap normalized citations, preserve long clarification answers, and request
  minimal router reasoning under a separate routing deadline.
- Fence connection callbacks before coordinator shutdown and log routing,
  search, and total turn latency.
- Align Python, JSON Schema, and browser validation for non-negative connection
  epochs and strict RFC 3339 transcript, result, and work-event timestamps.
- Own each accepted connection's Pipecat pipeline lifecycle directly and
  document the implemented `BaseWorker` mailbox boundary.
- Scan the complete pull-request commit range for committed credentials during
  the pre-push release gate.

[Unreleased]: https://github.com/vr000m/pipecat-subagents-lab/commits/main
[0.1.2]: https://github.com/vr000m/pipecat-subagents-lab/releases/tag/v0.1.2
[0.1.1]: https://github.com/vr000m/pipecat-subagents-lab/releases/tag/v0.1.1
[0.1.0]: https://github.com/vr000m/pipecat-subagents-lab/releases/tag/v0.1.0
