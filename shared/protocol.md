# Browser protocol v1

This document defines the versioned Python-to-browser boundary for the first
slice. The authoritative diagnostic record remains the Pipecat server log;
browser state is a deliberately limited product/debug projection.

## Version and envelope rules

The current contract version is `v1.0`. Every state-bearing message includes
stable identifiers and an origin epoch. Internal records may use a nullable
epoch before a browser connection is accepted; active-connection events require
the accepted epoch. JSON objects reject unknown fields.
The browser rejects unknown message kinds even when their contract version is
known, and rejects unknown contract versions. `snapshot_sequence` and the RTVI
envelope `sequence` are monotonic. A reconnect replaces runtime projections
with a fresh snapshot; local connection diagnostics are retained only with an
explicit local label.

The schemas in `shared/schemas/` are the wire artifacts. Python models in
`server/contracts.py` are the validation authority for server-produced data;
the browser must treat provider text, titles, and URLs as untrusted.

The only server-to-browser RTVI state message kinds in `v1.0` are
`runtime_snapshot`, `result`, `speech_progress`, `worker`, `routing`,
`user_transcript`, and `bot_transcript`. Their envelope is defined by
`shared/schemas/rtvi-message.json`. Each message carries its payload directly
in `data`; aliases and wrapper objects are not part of the contract. In
particular, `runtime_result` and `speech` are not message kinds.

| Kind | `data` contract |
| --- | --- |
| `runtime_snapshot` | `runtime-snapshot.json` |
| `result` | `grounded-result.json` |
| `speech_progress` | `speech-progress.json` |
| `worker` | `worker-state.json` |
| `routing` | `routing-state.json` |
| `user_transcript` | `transcript-entry.json` with `role: "user"` |
| `bot_transcript` | `transcript-entry.json` with `role: "assistant"` |

The envelope and payload carry the same non-negative `origin_epoch`. Snapshot
envelope and payload session identifiers match, as do `sequence` and
`snapshot_sequence`.

## Identity, ordering, and nullable fields

`session_id` identifies durable process state. `turn_id` identifies a user
turn, `work_item_id` identifies one bounded item within that turn, `run_id`
identifies one execution/retry, `event_id` identifies one event, `result_id`
identifies one canonical result, and `utterance_id` identifies one speech
delivery attempt. `worker_id` is a stable context-owner identity. Timestamps
are producer-generated RFC 3339 date-time strings with an explicit `Z` or
numeric UTC offset. Every populated `origin_epoch` is a non-negative integer;
it is nullable only for pre-arbiter internal records and must be populated for
accepted callbacks.

## Routing and capability

Routing actions are `direct`, `unsupported`, `clarify`, `existing_worker`, and
`new_worker`. The internal Python `RoutingDecision` carries catalogue identity,
capability label/availability, worker selection, topic, and configured
model-policy label. Model IDs emitted by a model are never trusted. Dispatch
validates that decision against the identical immutable catalogue snapshot;
unavailable private capabilities cannot be converted into topical web search.

The browser receives only the reduced `RoutingState`: turn, action, selected
worker/type, topic, model-policy label, and origin epoch. Catalogue contents and
capability classification stay server-side and are not RTVI payload fields.

## Work and speech states

The active browser protocol exposes delivery states `displayed`,
`queued`, `started`, `synthesis_ended`, `delivery_completed`,
`delivery_unknown`, `interrupted`, `interrupted_by_reconnect`, `paused`, and
`resumed`. Work completion and speech delivery are separate state machines.
`synthesis_ended` is not transport completion; neither transport completion
nor any current state proves browser decode, playout, or audibility. Terminal
delivery precedence is reconnect interruption, interruption, confirmed
completion, unknown delivery, then cancellation; duplicate or late events
cannot replace an already terminal outcome. Word-level progress is reserved
for a future, verified Phase-3 extension.

### Transport-aware lease boundary

Scheduler ownership of an admitted speech generation is not released on
provider synthesis end. The server-internal `SpeechLifecycleCoordinator`
(`server/speech_lifecycle.py`) holds the single global transport slot for a
generation until either the output transport reports its normal fieldless
stop for that still-occupied lane, or the connection-scoped output lane has
completed cancellation/teardown. `TTSStoppedFrame` and provider
`synthesis_ended` record synthesis state only and do not by themselves
admit another generation to TTS or output; a bare timer expiry cannot clear
a slot that has already submitted audio to output either.

This is a strictly stronger liveness proof than synthesis completion: it
rules out the case where a later result's timeout notice has already left
scheduler ownership by the time synthesis reports done, which previously let
a stale notice queue-jump into the transport ahead of the real answer. It is
not a stronger *audibility* proof. Transport stop confirms the server handed
the utterance off cleanly (or tore the lane down) but proves nothing about
browser-side decode, playout, or whether the listener actually heard it.
Browser playout acknowledgement remains out of scope and is deferred to a
later plan, per the Objective of
`docs/dev_plans/20260728-bug-transport-aware-speech-supersession.md`.

A timeout notice is supersedable only while it remains in its own per-work
queue; once its generation occupies the global transport slot, a
later-arriving final result for the same work item does not interrupt it.

`WorkItemEvent` reserves work states `started`, `progress`,
`cancellation_requested`, `cancelled`, `completed`, and `failed`.
`InterruptionEvent` similarly reserves normalized interruption telemetry. These
models and schemas are internal/future seams in v1.0: the production runtime
does not emit them as RTVI message kinds. Their `event_sequence` is monotonic
within a future work-item event stream, independently of the active RTVI
envelope `sequence`.

## Canonical results and snapshots

One `GroundedResult` wire record owns one `result_id`, one complete `text`
field, and one normalized absolute HTTP(S) citation set; projection IDs,
duplicate citation arrays, and duplicate UI text are not serialized. The
provider is instructed to keep `spoken_text` factually consistent with the
complete canonical text; that semantic equivalence is an evaluation target,
not a wire-schema invariant enforced by this runtime. The browser may present
`spoken_text` as the primary assistant turn and place the complete text,
delivery state, worker/turn metadata, and citations under a structured-output
disclosure; this is not hidden model reasoning. Worker cards expose only
identity, topic, policy label, status, and latest-result pointer. A runtime
snapshot contains the latest validated routing decision, semantic transcript
turns, worker state, canonical results, and speech progress but never prompts,
private context, raw STT fragments, or raw logs.
Transcript entries are created by the server only after the application turn
boundary closes; browser SDK transcript callbacks are not authoritative state.

## Contract inventory

- `rtvi-message.json` and its seven payload schemas define the active
  server-to-browser boundary listed above.
- `snapshot-handshake.json` defines same-origin HTTP session discovery and
  reconnect negotiation; it is not an RTVI state message.
- `routing-decision.json` defines the internal router-to-dispatch decision.
- `work-item-event.json` and `interruption-event.json` reserve deferred
  lifecycle contracts and are not emitted by v1.0.

Reconnect uses `session_id`, a resume token/known-process identity, proposed
epoch, and the last snapshot sequence. The new epoch is fenced before the
snapshot is sent; old callbacks may append an immutable canonical result tied
to their originating turn but cannot mutate active state or autoplay.

A retained result completing on its still-active originating epoch is committed
before its concise `spoken_text` is enqueued exactly once. It waits behind any
active utterance and then follows the normal speech-progress state machine.
Cancelled work, old epochs, disconnected sessions, and connections without TTS
remain display-only and never create a speech attempt.

## Verified Pipecat 1.6.0 seam notes

The pinned package does not expose the planned `LLMContextWorker` module.
`PipelineWorker(..., bridged=...)`, `BusBridgeProcessor(bus, worker_name,
target_task, bridge)`, and `RTVIServerMessageFrame(data)` remain available.
`RTVIServerMessageFrame` carries its arbitrary payload in `data`. The pinned
`WorkerRunner` constructor has no `auto_end` parameter. The durable session host
owns the process-lifetime runner, while every accepted browser connection owns
and awaits its real `PipelineWorker` lifecycle task. Replacement cancels the
prior connection worker, and epoch-aware publishers, observers, and session
state reject stale callbacks before they can mutate or emit active state.
