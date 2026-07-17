# Browser protocol v1

This document defines the versioned Python-to-browser boundary for the first
slice. The authoritative diagnostic record remains the Pipecat server log;
browser state is a deliberately limited product/debug projection.

## Version and envelope rules

The current contract version is `v1.0`. Every state-bearing message includes
stable identifiers and an origin epoch (nullable before Phase 3's connection
arbiter, but required after acceptance). JSON objects reject unknown fields.
Unknown future message types may be ignored only when their version is known;
an unknown contract version is rejected. `snapshot_sequence` and incremental
`event_sequence` values are monotonic. A reconnect replaces runtime projections
with a fresh snapshot; local connection diagnostics are retained only with an
explicit local label.

The schemas in `shared/schemas/` are the wire artifacts. Python models in
`server/contracts.py` are the validation authority for server-produced data;
the browser must treat provider text, titles, and URLs as untrusted.

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

One `GroundedResult` owns the text and normalized absolute HTTP(S) citations.
The spoken and UI projections carry the same `result_id`, text facts, and
citations. Worker cards expose only identity, topic, policy label, status, and
latest-result pointer. A runtime snapshot contains worker state, canonical
results, and speech progress but never prompts, private context, or raw logs.

Reconnect uses `session_id`, a resume token/known-process identity, proposed
epoch, and the last snapshot sequence. The new epoch is fenced before the
snapshot is sent; old callbacks may append an immutable canonical result tied
to their originating turn but cannot mutate active state or autoplay.

## Verified Pipecat 1.4.0 seam notes

The pinned package exposes `LLMContextWorker(name, llm, active=False,
bridged=...)`, `PipelineWorker(..., bridged=...)`, `BusBridgeProcessor(bus,
worker_name, target_task, bridge)`, and `RTVIServerMessageFrame(data)`.
`RTVIServerMessageFrame` carries its arbitrary payload in `data`. The pinned
`WorkerRunner` constructor has no `auto_end` parameter; its runtime default is
`_auto_end=True`, so the durable-host implementation must set/verify the
long-lived runner policy at the Phase-3 seam or use an equivalent lifecycle
adapter. No phase-1 contract treats durable-host or per-connection filtering
as accepted before that probe.
