# Browser protocol v1

This document defines the versioned Python-to-browser boundary for the first
slice. The authoritative diagnostic record remains the Pipecat server log;
browser state is a deliberately limited product/debug projection.

## Version and envelope rules

The current contract version is `v1.0`. Every state-bearing message includes
stable identifiers and an origin epoch. Internal records may use a nullable
epoch before a browser connection is accepted; active-connection events require
the accepted epoch. JSON objects reject unknown fields.
Unknown future message types may be ignored only when their version is known;
an unknown contract version is rejected. `snapshot_sequence` and incremental
`event_sequence` values are monotonic. A reconnect replaces runtime projections
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
are producer-generated ISO-8601 strings. `origin_epoch` is nullable only for
pre-arbiter internal records and must be populated for accepted callbacks.

## Routing and capability

Routing actions are `direct`, `unsupported`, `clarify`, `existing_worker`, and
`new_worker`. A worker route carries a catalogue version, worker identity,
topic, capability label/availability, and configured model-policy label. Model
IDs emitted by a model are never trusted. Dispatch validates the decision
against the identical immutable catalogue snapshot; unavailable private
capabilities cannot be converted into topical web search.

## Work and speech states

Work-item/result states are `started`, `progress`, `cancellation_requested`,
`cancelled`, `completed`, and `failed`. Delivery states are `displayed`,
`queued`, `started`, `synthesis_ended`, `delivery_completed`,
`delivery_unknown`, `interrupted`, `interrupted_by_reconnect`, `paused`, and
`resumed`. Work completion and speech delivery are separate state machines.
`synthesis_ended` is not transport completion; neither transport completion
nor any current state proves browser decode, playout, or audibility. Terminal
delivery precedence is reconnect interruption, interruption, confirmed
completion, unknown delivery, then cancellation; duplicate or late events
cannot replace an already terminal outcome. Word-level progress is reserved
for a future, verified Phase-3 extension.

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

Reconnect uses `session_id`, a resume token/known-process identity, proposed
epoch, and the last snapshot sequence. The new epoch is fenced before the
snapshot is sent; old callbacks may append an immutable canonical result tied
to their originating turn but cannot mutate active state or autoplay.

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
