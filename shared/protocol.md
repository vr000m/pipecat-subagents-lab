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

The server-to-browser RTVI state message kinds in `v1.0` are
`runtime_snapshot`, `result`, `speech_progress`, `worker`, `routing`,
`user_transcript`, `bot_transcript`, and the capability-conditional
`work_status`. Their envelope is defined by `shared/schemas/rtvi-message.json`.
Each message carries its payload directly in `data`; aliases and wrapper
objects are not part of the contract. In particular, `runtime_result` and
`speech` are not message kinds.

| Kind | `data` contract |
| --- | --- |
| `runtime_snapshot` | `runtime-snapshot.json` |
| `result` | `grounded-result.json` |
| `speech_progress` | `speech-progress.json` |
| `worker` | `worker-state.json` |
| `routing` | `routing-state.json` |
| `user_transcript` | `transcript-entry.json` with `role: "user"` |
| `bot_transcript` | `transcript-entry.json` with `role: "assistant"` |
| `work_status` | `work-status.json` (capability-gated; see below) |

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
cannot replace an already terminal outcome. `v0.1.3` also emits, at most
once per delegating semantic turn, an ephemeral, wire-invisible
acknowledgement utterance: it is user-audible but carries no `result_id`,
is never queryable through any of the states above, and never enters
canonical result state. Browser-visible speech state therefore does not
account for all audio the connection may have played. Word-level progress remains
reserved for a future, verified extension beyond this release: the `v0.1.3`
Phase 3 `work_status` kind below is a coarse, truthful progress contract only
(`routing`/`searching`/`background`/`result_ready`/`failed`/`cancelled`) and
does not satisfy or close that reservation.

## Progressive work status (capability-gated, v0.1.3 Phase 3)

The optional snapshot-handshake capability `work_status_v1` gates a coarse,
truthful `work_status` RTVI kind. Absent or unknown capability names mean
unsupported; no capability state is ever inferred from browser rendering or
message-kind fallback. Only an `existing_worker`/`new_worker` routing
*decision* allocates a client-visible parent work-status record; a
`direct`/`unsupported`/`clarify` routing action never does. That allocation
gate is about how the parent record comes to exist, not about which states
it may later reach: a worker that raises `clarify` or `declined` on a child
that was already delegated this way is a terminal outcome for that child,
settling that child terminal at `result_ready` (the canonical result is
still committed and display-ready) once its canonical result commits --
rather than leaving it non-terminal -- and settling it at `failed` instead
if that commit fails or is stale; the parent re-aggregates as usual.

Each payload carries `turn_id`, a nullable `work_item_id` (the parent key for
a mixed multi-intent turn), a nullable `worker_id`, the coarse `state` enum,
a per-`(origin_epoch, turn_id, parent work item)` `event_sequence`, a
nullable `terminal_reason` (only `missing_worker` or `retention_rejected`,
and only alongside `failed`), and `origin_epoch`. Legal transitions are
`routing -> searching|failed|cancelled`; `searching ->
background|result_ready|failed|cancelled`; `background ->
result_ready|failed|cancelled`; `result_ready|failed|cancelled` are terminal.
A work item with no prior record may start cold at `routing`, `searching`,
`background`, `result_ready`, or `failed` -- `failed` because a child can fail
before it is ever routed (missing worker or missing search capability) and its
parent must still terminalize -- but never at `cancelled`, so a blind
whole-turn cancel sweep leaves a child that never started untouched.

This transition table governs per-child bookkeeping only. The wire-visible
parent record is a pure recomputation over its current child set on every
change, not a step through this table in its own right, so the table does
not constrain it directly; the parent's only wire-level invariants are that
a terminal parent state never regresses and that its `event_sequence`
strictly increases.

Parent aggregation over delegated children is exhaustive: `routing` while any
child is routing; `searching` while any child is searching and none is
routing; `background` while no child is active and at least one remains
retained; once every delegated child is terminal, `failed` wins if any child
failed, otherwise `cancelled` applies only when every child is cancelled,
otherwise `result_ready`. `result_ready` means the canonical result is
committed and display-ready -- it does not mean speech was queued, delivered,
or heard. Terminal records preserve their original `origin_epoch` and remain
in a capable client's reconnect snapshot until removed, whichever comes
first of: a five-minute session-clock TTL, or bounded-capacity eviction
once the record set exceeds 256 keys (terminal-first, oldest-first). Both
numbers are defined once in `shared/work-status-retention.json`, which
`server/session_state.py` and `web/src/state.js` both load rather than
hardcoding independently -- so this section's "five minutes" and "256" are
descriptive of that file's current values, not a second place to edit when
they change. Only
terminal records are eviction candidates -- evicting a live (non-terminal)
record would erase the sole record of its children and strand the parent
aggregate non-terminal forever, so eviction is refused while every retained
record is live, and the 256-key bound is a soft cap that a live-record-heavy
ledger may deliberately exceed. Removal is pruned lazily at projection time.

Capability negotiation carries one URL-encoded JSON array of capability names
in a single `capabilities` query parameter on both the `POST /api/rtc` offer
and the `PATCH /api/rtc` ICE-candidate request. The server normalizes,
deduplicates, and lexically sorts the declared set and binds it immutably to
the promoted connection epoch. A `PATCH` request either omits the field
(inheriting the `POST`-bound set) or repeats the identical normalized set; a
present mismatch is rejected and cannot mutate observer entitlement after
promotion. `enable_background_status` (default on) gates emission
server-side regardless of client capability; when disabled, no `work_status`
frame is ever produced and the legacy foreground-timeout notice applies
universally, reproducing pre-Phase-3 behavior.

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

- `rtvi-message.json` and the seven payload schemas covering its eight
  `kind` values (including `work-status.json`; `user_transcript` and
  `bot_transcript` share `transcript-entry.json`) define the active
  server-to-browser boundary listed above.
- `snapshot-handshake.json` defines same-origin HTTP session discovery,
  reconnect negotiation, and the optional `capabilities` carrier; it is not
  an RTVI state message. Capability negotiation is bounded: at most
  `_MAX_CAPABILITY_ENTRIES` (16) capability names, each at most
  `_MAX_CAPABILITY_NAME_LENGTH` (64) characters, and the whole encoded
  `capabilities` field at most `_MAX_CAPABILITY_FIELD_LENGTH` bytes
  (`server/app.py`); on the request side, decoding the `capabilities` query
  parameter also produces a `capabilities_present` boolean distinguishing
  "no `capabilities` query parameter was sent" from "an empty capability set
  was sent," which the server consumes to decide handshake behavior. This
  flag is a request-parse artifact only -- the handshake response itself
  (`SessionHost.session_handshake()`) never carries it back to the client.
- `routing-decision.json` defines the internal router-to-dispatch decision.
- `work-item-event.json` and `interruption-event.json` reserve deferred
  lifecycle contracts and are not emitted by v1.0.
- `v013-evidence.json`, `v013-query-context-raw.json`,
  `v013-query-context-post-change-analysis.json`, and
  `v013-transport-browser-contract.json` are Phase 4 evidence-gate
  artifacts, validated by `scripts/validate_v013_evidence.py` and
  siblings; they are internal analysis inputs, not a wire boundary.

Reconnect uses `session_id`, a resume token/known-process identity, proposed
epoch, and the last snapshot sequence. The new epoch is fenced before the
snapshot is sent; old callbacks may append an immutable canonical result tied
to their originating turn but cannot mutate active state or autoplay.

A retained result is committed exactly once regardless of its delivery
disposition. When `enable_autoplay_policy` is disabled, every valid,
still-active-epoch, non-cancelled result on a speakable connection is
enqueued and spoken (the pre-v0.1.3 behavior, unconditionally). When
`enable_autoplay_policy` is enabled (the default), `spoken_text` is enqueued
and follows the normal speech-progress state machine only when *all* of the
following hold: the loaded promotion manifest is schema-valid *and*
`promotion_eligible` (schema validity alone never proves promotion-eligible
-- see the v0.1.3 dev plan's Phase 2 evidence-gate predicate), the
originating epoch is still active, no newer semantic turn has been accepted
since the result's work was dispatched, and no explicit pause is in effect.
Any predicate failing -- including cancelled work, an old epoch, a
disconnected session, a connection without TTS, or evidence that is
missing/blocked/unavailable-only/malformed/not promotion-eligible -- commits
the result display-only and never creates a speech attempt. As of v0.1.3's
initial release, autoplay promotion is expected ineligible (no shipped
manifest carries complete real provider/model evidence plus verified browser
audibility), so the policy defaults to display-only in practice even though
the mechanism itself is on by default.

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
