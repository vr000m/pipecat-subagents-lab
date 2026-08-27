# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Removed

- Query-context narrowing experiment and the runtime promotion-manifest
  chain it fed: the four experiment scripts (`run_query_context_experiment.py`,
  `collect_query_context_latency.py`, `analyze_query_context_latency.py`,
  `query_context_common.py`), both query-context schemas, the phase4c
  validator path in `scripts/validate_v013_evidence.py`, and
  `load_promotion_manifest`/`PromotionManifest` (`server/config.py`) plus all
  of its consumers (`server/app.py`, `server/composition.py`,
  `scripts/eval_common.py`, `scripts/smoke_conversation.py`), and the
  `config.toml` manifest key. Operator decision: retire — promote was never
  demonstrated to have value (`promotion_eligible=false` shipped in v0.1.3)
  and required the full Appendix A effort, including paid live-provider
  collection, to even attempt. Late-result disposition is now unconditionally
  `display_only` (autoplay is structurally unreachable), pinned by a
  regression test covering both `enable_autoplay_policy` values.
  `scripts/validate_v013_evidence.py --verify-manifest`, the `justfile`
  `verify-manifest` recipe, CI's `promotion-manifest-drift` job, and
  `scripts/check_release_metadata.py`'s fixed historical-record path check
  survive as read-only validators of the frozen v0.1.3 `docs/benchmarks/`
  records. The unused CI release-metadata job and its environment export were
  removed because no current consumer reads them; `record_phase3_completion.py`
  remains only as a standalone historical artifact helper, not an active
  release/evidence gate. The frozen records themselves are untouched.
  `enable_autoplay_policy` is retained for `feature_policy_fingerprint`
  stability only — its behavioural consumer is retired.

## [0.1.3] - 2026-08-24

### Added

- Early acknowledgement: a deterministic, non-progress-claiming spoken ack
  ("One moment while I look into that.") is scheduled the instant routing
  confirms delegation to a worker, replacing timeout-gated silence for
  direct, pending-dialogue, and multi-intent delegated turns. Exactly one
  ack per semantic turn, enforced by a turn-scoped acknowledgement latch;
  never claims a result exists, is discarded atomically if the real result
  is ready before admission, and never enters canonical result/transcript
  state. Gated by `enable_early_ack` (`Config`/`FeaturePolicy`), on by
  default; disabling it reproduces the exact pre-0.1.3 legacy-timeout
  behavior.
- Background-delivery autoplay policy: a late (retained) search result now
  commits exactly once and is either autoplayed or delivered display-only
  based on a machine-checked promotion gate — schema-valid evidence alone
  never enables autoplay; complete real (non-credential-free) provider/model
  evidence plus a named-browser/device `audibility_verified` transport
  contract are both required. Gated by `enable_autoplay_policy`; v0.1.3
  ships with promotion expected ineligible (no paid-provider evidence
  collected), so the shipped behavior is commit-and-display-only for late
  results.
- Progressive RTVI status: coarse, truthful `work_status` states (`routing`,
  `searching`, `background`, `result_ready`, `failed`, `cancelled`) for
  delegated search work, delivered only to browsers that negotiate the new
  `work_status_v1` capability via a versioned `capabilities` handshake field
  on connect/reconnect. Gated by `enable_background_status`; legacy and
  non-advertising clients keep the pre-0.1.3 timeout-notice behavior
  unchanged. Terminal statuses are preserved across reconnects for a bounded
  5-minute TTL.
- Deployment promotion-manifest system: `scripts/validate_v013_evidence.py
  --write-manifest`, an atomic fsync/rename-durable writer, and
  `server/config.py`'s fail-closed `load_promotion_manifest()` loader bind
  the three feature flags' autoplay-eligibility evidence to a specific
  release/source identity. A CI `release-metadata` job derives deployment
  identity on `main` pushes; `scripts/check_release_metadata.py` gates
  future releases on `pyproject.toml`/`CHANGELOG.md` agreement.
- Query-context narrowing experiment scaffolding (`scripts/
  run_query_context_experiment.py`, `collect_query_context_latency.py`,
  `analyze_query_context_latency.py`): a bounded, credential-free-safe
  statistical framework (paired bootstrap resampling, versioned quality
  scorer, Spearman correlation) to evaluate whether narrowing
  `_contextual_input`'s `history[-4:]` window reduces search latency. No
  paid-provider evidence was collected for this release, so the experiment
  resolves to "no change": production `_contextual_input` behavior is
  unmodified.
- Microphone and speaker device selectors in the browser client, letting
  input/output devices be switched mid-session; the speaker selector is
  hidden in browsers without `HTMLMediaElement.setSinkId()` support.
- Icon toolbar with a merged connect/disconnect toggle button, replacing the
  previous separate Connect/Disconnect buttons.
- Structured browser-console diagnostics (`[HH:MM:SS.mmm][component]
  message`) for connection, track, mic, and speaker events.
- Router/worker reasoning-effort config knob (`router_reasoning_effort_policy`
  / `worker_reasoning_effort_policy` on `Config`, `WEBSEARCH_ROUTER_REASONING_EFFORT`
  / `WEBSEARCH_WORKER_REASONING_EFFORT` env overrides) with zero behavior
  change when unset, and a bespoke `evals/` + `scripts/eval_model_comparison.py`
  runner that drives `SessionHost._handle_transcript()` directly to compare
  candidate router/worker models and effort levels against the live paid
  provider, scored via `pipecat.evals.judge.EvalJudge`. Candidate
  verification (`scripts/verify_eval_candidates.py`) gates the runner behind
  a versioned manifest of live-confirmed (model, effort, tools) tuples.
- `scripts/eval_model_comparison.py --repeat N` (default 1): runs each
  (router, worker, scenario) cell N times live and majority-votes the
  results into one summary, since live calls run at `temperature=1.0` and a
  single sample is noisy. Raw per-repetition results are preserved on the
  report for audit; a coin-flip tie always resolves toward the failing
  outcome. `--repeat 1` reproduces the prior single-run report shape
  exactly.
- `luna-medium` (`gpt-5.6-luna` at `effort=medium`) as a router candidate in
  the eval suite's `ROUTER_CANDIDATES` (`scripts/eval_common.py`).
- `[models].router_reasoning_effort` / `[models].worker_reasoning_effort`
  TOML keys (`config.toml`), mapped to the existing
  `WEBSEARCH_ROUTER_REASONING_EFFORT`/`WEBSEARCH_WORKER_REASONING_EFFORT`
  env-var path so reasoning effort can be set the same way model IDs
  already are, with env vars still taking precedence over TOML.

### Changed

- `CONNECTION_LOCAL_FRAMES` (`server/speech_lifecycle.py`) is now the single
  declaration site for frame types that must stay off `WorkerBus`, replacing
  a hand-maintained tuple that previously lived separately in
  `server/pipeline.py`'s `framework_bridge()`. A new test asserts every
  private speech frame defined in `speech_lifecycle.py` is covered by the
  tuple, so a future frame added there without updating the exclusion list
  now fails at test time instead of silently breaking TTS audio.
- The browser toolbar (mic/speaker selects, connect/disconnect toggle, mic
  button) now renders from `state` through a new `web/src/controls.js`
  module instead of being mutated imperatively from connection callbacks,
  matching the existing `state` → `render` pattern already used for the
  runtime panel. `web/src/state.js` gained device-list/selection/mic-enabled
  reducers to support this.
- `scripts/eval_model_comparison.py --full-matrix`'s help text now documents
  that the router cannot influence worker output in this codebase, so its
  non-baseline×non-baseline cells add no comparison signal beyond `--repeat`'s
  resampling — it remains available as an opt-in "confirm two already-good
  candidates pair well together" check, not the default comparison method.
- `config.toml`'s `[models]` defaults changed to the shortlist from the
  `--repeat 3` live comparison: router `gpt-5.6-luna`@medium (was
  `gpt-5-mini`, no reasoning param), worker `gpt-5.6-terra`@medium (was
  `gpt-5`, no reasoning param). See
  `docs/dev_plans/artifacts/router-worker-eval-shortlist-2026-08-20.md` for
  the comparison data. Override via `WEBSEARCH_ROUTER_MODEL`/
  `WEBSEARCH_WORKER_MODEL`/`WEBSEARCH_ROUTER_REASONING_EFFORT`/
  `WEBSEARCH_WORKER_REASONING_EFFORT` to keep the prior baseline.

### Fixed

- `SpeechGenerationMarkerFrame` and `SpeechGenerationFlushAckFrame` were
  never added to the bus-bridge's frame exclusion list after being
  introduced, so they were silently diverted to `WorkerBus` instead of
  reaching `TransportSpeechLifecycleProcessor`. With no bound context, every
  TTS audio frame was treated as belonging to an unbound generation and
  dropped: synthesis and the WebRTC track worked correctly throughout, so
  nothing in transport state, logs, or the browser signalled a problem — the
  assistant was simply never audible. See `docs/architecture.md`'s "Bus
  bridge frame exclusions" section for the full incident writeup.
- An unexpected transport disconnect left the toolbar's connect/disconnect
  toggle and device selectors claiming an active connection against a dead
  client, and a rejected `HTMLMediaElement.setSinkId()` call left the
  speaker selector displaying a device that was never actually routed to.
  Connection teardown is now a single idempotent path invoked from both a
  transport-driven disconnect and an explicit one, and `setSinkId()` success
  is now the commit boundary for what the speaker selector displays.
- A latent XSS sink in the toolbar's button-label rendering (unescaped
  `innerHTML` interpolation, not reachable by any current caller) was closed
  by routing all button labels through `textContent`.

- Discard a still-queued "taking longer than expected" timeout notice before
  speaking a late search result that arrives for the same work item, so the
  stale notice no longer plays after the real answer is ready. A notice that
  has already started speaking is left to finish rather than interrupted
  mid-utterance.
- Keep scheduler ownership of an admitted speech generation until the output
  transport reports its normal stop or the connection-scoped output lane
  completes teardown, instead of releasing it on provider synthesis end.
  Previously a still-playing utterance's synthesis could finish before an
  unrelated later result was ready, letting that result's timeout notice
  cross into the transport ahead of the real answer where queue-only
  supersession could no longer remove it. Adds a private, token-fenced
  `SpeechGeneration`/`SpeechLifecycleCoordinator` lifecycle
  (`server/speech_lifecycle.py`), a work-scoped notice-specific replacement
  in place of broad queue discard, and start/drain/interruption watchdogs
  backed by an injectable clock and timer scheduler. No package version
  change; ships as a release-neutral precursor to
  `docs/dev_plans/20260728-feature-early-ack-background-delivery-v0.1.3.md`.
- A pause control command no longer tombstones an unrelated active utterance
  when its target work item is only queued, not yet playing.
- `shutdown()` now always forces scheduler lease cleanup, even after a failed
  output teardown already set the connection inactive directly.
- A browser reconnect mid-speech now routes through the lifecycle coordinator
  (matching pause/cancel), so the old connection's peer connection and audio
  tracks are torn down instead of leaking.
- `TTSStartedFrame` and the start/drain-timeout cleanup path each close a race
  that could let a stale generation's audio reach the output transport.
- A `0.0` TOML/env override for `speech_start_timeout_seconds` or
  `speech_transport_grace_seconds` now raises a config error instead of being
  silently dropped in favor of the default.
- The `delivery_completed` TTS callback event no longer bypasses the lifecycle
  coordinator's transport-slot ownership when one is installed.
- A multi-intent turn's "taking longer than expected" timeout notice is now
  correctly tagged as a timeout notice rather than a regular result, so it is
  still discardable if the real result for that item arrives late.
- Local TTS audio frames (`LocalTTS.run_tts()`) now carry the same
  `context_id` as their sibling start/stop frames, so they participate in the
  lifecycle coordinator's stale-frame guard: audio from an
  interrupted/tombstoned generation can no longer reach the output transport
  after barge-in.
- The speech lifecycle coordinator now reaps a generation's internal state
  once it terminalizes, and bounds its reaped-context tombstone cache with an
  LRU eviction limit, so a long-lived connection's memory no longer grows
  without bound.
- A TTS-provider frame is now rejected unless it belongs to the generation
  its own marker most recently admitted; a stale start from a superseded
  generation can no longer mis-bind a replacement generation's context or
  have its late audio/stop frames forwarded to the output transport.
- The transport slot backing an interrupted, paused, or cancelled generation
  is now released only once a token-bearing acknowledgement confirms TTS
  actually processed the interruption (or the connection lane tears down),
  closing a window where a still-live old generation could be admitted
  alongside its replacement. A cleanup-dispatch failure now also escalates
  to connection teardown immediately, instead of permanently blocking all
  further speech admission on that connection.
- A reconnect snapshot could still be raced by an incremental status update
  dispatched moments earlier, delivering events to the client out of order;
  and once a snapshot barrier install began, an unresponsive connection
  worker could leave the runtime observer paused forever with an unbounded
  event buffer and a lock that never released. The barrier now drains and
  acknowledges through a bounded wait with a cancellation path, writes the
  snapshot frame before replaying buffered events (so a replay could never
  arrive ahead of the snapshot it applies against), and always resets
  pause/buffer state on unsubscribe so a torn-down connection can't stay
  permanently mute after reconnecting.
- A late (retained) search result could still autoplay after the user had
  already started a newer utterance: both the single-intent and
  multi-intent turn handlers captured the turn's sequence number only after
  earlier awaits, so a concurrent newer turn could bump the counter first
  and make the late result compare as still-current. Both paths now
  snapshot the sequence before their first await. Separately, a real TTS
  submission failure during commit was silently swallowed instead of
  failing the turn, and a mid-loop commit failure in the multi-intent path
  could abort before already-computed sibling results — already reported to
  the client as complete — were ever committed or spoken; each sibling is
  now committed independently and the turn still records as failed.
- SessionHost's search-timeout and shutdown-grace budgets moved onto
  SessionHost.config, but the paid-conversation smoke harness was still
  mutating the coordinator/registry config objects to raise the budget for
  long-running paid searches, so its override silently no-op'd against the
  real 15-second default and every paid run timed out into the generic
  fallback with no citations.
- A reconnect dropped the entire work-status ledger and its per-key
  sequence counters instead of restoring them from the snapshot, and the
  browser's reducer keyed status records without the epoch the server
  includes, so two records differing only by epoch could collapse into one
  and be discarded as stale; both are fixed, and a work item could also
  cold-start directly into a "cancelled" state because the validation that
  should have rejected it only ran once a prior record already existed. The
  browser's own work-status store was also insert-only and unbounded with
  no terminal-record expiry, so a long session would keep rendering
  finished statuses indefinitely — it now mirrors the server's bounded,
  TTL-evicting ledger.
- A capacity-rejected turn could still speak its early acknowledgement
  immediately before the "search service is busy" reply; a turn whose
  exception handler fired while a sibling was still legitimately running in
  the background could sweep that live child to a permanent "failed" state
  (later contradicted when its real result committed successfully); and a
  multi-intent turn where every dispatched child failed to land in any
  results/pending/failure bucket could leave the parent's status stuck at
  "searching" forever. All three are closed, along with a failed
  ack-admission retry that no longer blindly re-enqueues into a stale
  cancellation/reconnect window — it now re-checks the turn's liveness
  first, and is capped so a stuck turn can't retry forever.
- The v0.1.3 promotion-manifest evidence gate checked artifact shape but
  not binding, letting several forged-but-well-shaped artifacts pass
  validation: digest fields were checked by length rather than actual hex
  content, an unreadable schema file was treated as "verified," an empty
  Phase 4C hash string skipped its check entirely, and Phase 4B analysis
  trusted a self-declared fixture version instead of the fixture's own
  bytes. All are now checked against real content, and a manifest missing
  required phase bindings is rejected rather than silently promoted —
  closing paths that could have silently mis-gated autoplay eligibility for
  a release.
- `resume()` re-enqueued a paused utterance under a new id without ever
  closing out the old id's record, so a stale "paused" entry rode along in
  every snapshot for the rest of the process's life; a coalesced snapshot
  request could be dropped with no retry if the in-flight request holding
  the lock failed without delivering a snapshot, leaving the client stuck
  discarding every incremental until a manual reconnect; and an
  already-cancelled advance task could resurrect itself via its own
  cancellation callback during a full stop. All three are fixed.
- Review-gauntlet rounds 8–10 (`cafb89e`..`9b6fbca`) closed a further batch
  of hardening and correctness gaps found by adversarial/deep/security
  review: the evidence writers (`analyze_query_context_latency.py`,
  `run_query_context_experiment.py`, `collect_query_context_latency.py`,
  `record_phase3_completion.py`) previously wrote through plain
  `write_text()`/`.open("w")` at predictable paths, letting a planted
  symlink redirect the write — now routed through the same
  `O_NOFOLLOW`-hardened helpers the promotion-manifest writer already used;
  `server/config.py`'s runtime evidence reads had a stat-then-read TOCTOU
  window, closed by reading through one held fd; a restored (non-terminal)
  work-status record surviving a TTL prune lost the marker distinguishing
  it from a complete record, letting a single still-running child's result
  terminalize a multi-intent parent early — the client (`web/src/state.js`)
  had the same gap, with no retention clock for a restored record at all;
  `audibility_verified` transport-contract metadata was accepted on key
  presence alone, letting an empty browser name, a stringly-typed gesture
  flag, or an unparseable timestamp through into a promotion-eligible
  manifest; the source-anchor package check matched a bare unscoped leaf
  name, letting a lookalike URL pass as the real scoped package; the
  promotion-manifest writer stamped the package-default release version
  instead of the configured one, so any non-default configured release
  always failed closed; a timer scheduler read its delay off the wall
  clock instead of the injectable `Clock` it was handed, silently
  defeating every timeout under a fake/test clock; and a TTS hand-off no
  longer proceeds against an already-tombstoned generation, closing a race
  that could hold the global transport slot roughly tenfold longer than
  intended.
- Review-gauntlet round 11 (`390b764`, `9b1155b`) closed a further batch of
  logic and security findings, each with its own regression test:
  - `server/turn_ack_ledger.py`: a per-turn admission-chain generation
    counter stops a superseded ack chain's belated retry from re-entering
    `start_next` after a later eligible sibling re-latched the same turn
    under the identical `ack_work_item_id`. The counter is bounded
    (oldest-turn-first) rather than retained for the process's lifetime.
  - `server/session_state.py`: the diagnostic event log is a
    `deque(maxlen=2000)` instead of an unbounded list, and a missing
    `shared/work-status-retention.json` (a packaged install has no `shared/`
    tree) falls back to that file's documented defaults instead of crashing
    at import.
  - `scripts/evidence_common.py`: evidence *reads* now go through the same
    `O_NOFOLLOW`/`O_NONBLOCK` plus regular-file guard the writers already
    used, so a symlink, FIFO, or device node planted at a predictable
    evidence path can neither redirect nor hang a read; the write path
    enforces its documented size cap and loops `os.write` to completion so a
    short write cannot silently truncate an artifact.
  - `server/config.py`: `_is_hex_hash` rejects uppercase hex, matching
    `hexdigest()`'s own output alphabet.
  - `server/speech_scheduler.py`: `start_next("")` no longer falls through a
    truthiness check and scans every queue instead of the requested one;
    resuming a paused ack settles the turn's ack latch through
    `on_ack_terminal` instead of re-enqueueing it outside `TurnAckLedger`'s
    bounded admission chain.
  - `server/pipeline.py`: a search completing with a `None` result is
    reported as "no reliable result" instead of being misclassified as a
    busy service.
  - `server/work_item_coordinator.py`: a late-result `on_complete` callback
    is admitted past (and outside) the background-capacity budget, so
    nothing in the at-least-once late-delivery path can be silently refused.
  - `server/app.py`: the Loguru `diagnose`/`backtrace` hardening now also
    runs from `create_app()`, so `uvicorn server.app:app` direct-ASGI
    serving gets it; it strips only Loguru's own default handler rather
    than every handler, and warns when a host-installed sink survives with
    traceback-locals rendering still enabled.
  - The five evidence-writing scripts confine `--output` to the repo tree
    before writing, and write through the *confined* path rather than the
    raw one.
  - `server/handshake_gate.py`: a `CapabilityCarrier` Protocol replaces the
    `Connection | ConnectionPipeline | None` union in handshake validation.
- Restart-gauntlet round 4 (`dd2ee9f`..) closed a batch of bypasses a 4-lens
  confirm pass reproduced *in round 3's own hardening additions*, each with
  its own regression test:
  - `scripts/validate_v013_evidence.py`: the new `--verify-manifest` CI drift
    gate failed open three ways. Its verdict re-derivation was guarded on all
    of phase0/1/2 having resolved, so a manifest that simply *omitted* an
    `inputs` entry skipped the check entirely and a forged
    `promotion_eligible=true` exited 0; its path confinement was a private
    copy of the runtime loader's rule that accepted absolute in-repo paths
    the loader rejects; and three fields it advertised as verified
    (`reason`, `manifest_phase`, `phase3_command_digest`) were never compared
    to anything, while the completeness guard counted them as covered. The
    required input set is now derived from the manifest's own
    `manifest_phase`, every "cannot check this" path reports drift instead of
    falling through, `promotion_eligible`/`reason` are both re-derived through
    one function the writer also stamps from, `phase3_command_digest` is
    compared against the resolved Phase 3 artifact, and `release_version` /
    `feature_policy_fingerprint` are verified rather than excluded as
    volatile — so CI now pre-catches the two identity mismatches the runtime
    loader is guaranteed to reject.
  - `scripts/evidence_common.py`: the manifest-declared-path confinement rule
    now lives once, as `confined_evidence_input_path`, pinned by test against
    `server/config._resolve_confined_evidence_path` so the CI gate and the
    runtime loader cannot disagree about which declared paths are legal.
  - `.github/workflows/ci.yml`: the manifest drift check runs as its own
    `promotion-manifest-drift` job on pull requests as well as pushes. As a
    step inside the main-only `release-metadata` job it only ever ran
    post-merge, so a PR editing the manifest or any evidence artifact merged
    before the gate protecting that file had executed.
  - `scripts/validate_phase2_transport_browser_contract.py`: round 3's new
    semver range support discarded prerelease/build suffixes, so the declared
    `1.10.6` "admitted" lockfile pins `1.10.6-evil.0`, `1.10.6+evil` and
    `v1.10.6` — three different package builds that npm semver satisfies
    none of, and that the pre-round-3 exact string comparison had caught.
    Both sides now fail closed on any suffix.
  - `web/src/state.js`: round 3's work-status tombstone mechanism had two
    resurrection holes of its own. A terminal record updated after its TTL
    had elapsed was written and immediately pruned in the same pass, leaving
    neither record nor watermark; and a snapshot that no longer carries a key
    dropped it with no watermark at all. Both paths now leave a tombstone, so
    a late lower-sequence increment can no longer re-insert an aged-out
    terminal record.
  - `server/config.py`: `_read_regular_file_no_follow` is now the public
    `read_regular_file_no_follow`, since `server/session_state.py` reads
    through it too and the underscore name misstated it as config-private.
- Restart-gauntlet round 5 (`b6ce9d2`..) fixed all 28 findings a fresh
  full-corpus 4-lens review raised (documentation returned zero, the first
  clean lens of the restart). Four were real production bugs:
  - `server/config.py` treated an operator's explicit numeric `0` as absent.
    `[turn] max_citations = 0`, `smart_turn_timeout_seconds = 0`, and their
    siblings were gated on a truthiness walrus, so TOML's real integers/floats
    evaluated falsy, the key was dropped, and the packaged default was
    substituted — bypassing the `__post_init__` range checks that exist to
    reject those values by name. Now membership-gated, matching the round-7 fix
    for the string fields. Two unwrapped `int()` conversions that escaped as a
    bare `ValueError` now raise the field-named `ConfigError` every sibling
    raises.
  - `server/config.py` resolved the STT/TTS endpoint families by hardcoded key
    priority, ignoring the layered precedence system the rest of the loader
    uses: `[tts] tts_ws_uri` in `config.toml` (layer 0) silently beat a
    `WEBSEARCH_TTS_WS_SOCKET` exported in the process environment (layer 2), so
    the service connected to the endpoint the operator's highest-precedence
    override had replaced. The family now resolves by layer first, with key
    priority breaking only a same-layer tie. Closes the gap
    `_clear_inherited_reasoning_effort`'s docstring had signposted since round 4.
    A half-specified `WEBSEARCH_TTS_WS_HOST`/`_PORT` pair is now a startup error
    rather than a silent fall-through to the dataclass default.
  - `SpeechScheduler.cancel()` swept queued and paused ack items without
    calling `_notify_ack_swept`, asymmetric with `interrupt()`. Since
    `_emit_progress` is a deliberate no-op for acks, the turn-ack ledger learned
    nothing about a cancelled ack: its latch stayed set and an in-flight
    admission-retry chain could re-enqueue an ack for a turn the user had just
    cancelled. `cancel()` now notifies with a new `PreAdmissionTerminalReason.
    CANCELLED` (the connection is still live, so `CONNECTION_CLOSED` would have
    misreported the cause).
  - `SessionHost.commit_late_result_once`'s `finally` settled the turn ack
    against `self.connection` — the *live* connection, re-pointed on every
    reconnect — rather than the epoch the committing turn belongs to. A late
    result surviving a reconnect discarded the *new* epoch's queued ack and
    released a latch that scheduler never set. Now fenced on the turn's own
    origin epoch; on a mismatch only the connection-independent latch half is
    cleared.
  - Also: `scripts/validate_v013_evidence.py --verify-manifest`'s completeness
    guard was one-directional (it rejected unknown manifest fields but never
    checked that required ones were present and correctly typed), so a manifest
    missing `deployed_at_utc`, or carrying `source_commit: 123`, passed CI and
    then failed closed to display-only at boot; the ack-generation LRU bound
    added in round 2 made an evicted-but-still-live chain read itself as
    superseded and abandon *without settling*, orphaning its queued ack;
    `RuntimeSnapshot._highest_by_session` was the one unbounded
    process-lifetime cache left on the branch; and the justfile/CI parity guard
    was hardcoded to CI's `test` job, leaving the new pull-request-blocking
    `promotion-manifest-drift` job with no local `just` equivalent at all
    (now `just verify-manifest`, reachable from `just check`).
  - The remaining findings were consolidation follow-through: one shared
    `evidence_common.now_utc()` replacing seven hand-written UTC stamps in two
    incompatible formats, one `validate_scored_record` replacing a gate
    hand-rolled (and already drifting) in two scripts, one
    `confine_output_arg` replacing a five-times-copy-pasted `--output`
    confinement idiom, one `effective_feature_policy_fingerprint`,
    `ClarificationContext` moved to `server/workers/base.py` so a session-level
    projection no longer imports from a concrete worker, the work-status
    "replaces the legacy result" predicate named once as
    `WorkStatusPublisher.replaces_legacy_result_for`, `CanonicalResultAdapter`
    given its connection's entitlement instead of sniffing it back out of the
    serialized payload, two more unhardened `read_text()` reads routed through
    `read_bytes_no_follow`, the Phase 2 `source_anchor` provenance check
    tightened from two unanchored substring tests to an exact npm-form match,
    and `web/src/protocol.js`'s bespoke runtime-snapshot key check folded into
    the shared `hasExactKeys` helper. The `terminal_reason` enum gained the
    Python-to-schema parity assertion its two siblings already had.
- Restart-gauntlet round 6 (confirm pass 3) fixed all 14 findings a 4-lens
  confirm pass over round 5's own fix diff raised (5 logic, 2 security,
  7 architecture). Most were round 5's new rules left unswept in a sibling
  location:
  - `server/config.py`'s new TTS host/port pairing check fired before endpoint
    resolution and regardless of which spelling won, so a leftover
    `[tts] tts_ws_host` in `config.toml` -- never consulted once
    `WEBSEARCH_TTS_ENDPOINT` is exported -- hard-failed startup for a config
    that had booted fine. It now fires only when the half-pair sits at a
    strictly higher precedence layer than the winning member.
  - The vendor-credential aliases (`CARTESIA_API_KEY`, `DEEPGRAM_API_KEY`,
    `CARTESIA_VOICE_ID`, and the variable named by
    `WEBSEARCH_OPENAI_API_KEY_ENV`) kept the hardcoded `scoped or bare` chain
    the endpoint families had just been migrated off, so a scoped spelling in
    the env file silently beat a bare spelling exported in the process
    environment. They resolve by configuration layer first, key priority
    second, like every other family; the `WEBSEARCH_`-prefixed name still wins
    a tie within one layer.
  - An explicit `[tts] tts_ws_port = 0` was still read as an unset port and
    reported as "must be set together" instead of reaching the range validator
    that names the field -- the truthiness-vs-membership class round 5 fixed
    for `max_citations`, unswept in the endpoint block.
  - The Phase 2 `source_anchor` URL branch matched the locked version as an
    unanchored substring, accepting `.../tree/v1.10.60` and
    `.../compare/1.10.6...attacker-branch`; it now compares whole path
    segments, and the `registry.npmjs.org` spelling the host allowlist
    advertises actually works.
  - `scripts/check_release_metadata.py` now parses `ci.yml` and looks for the
    promotion-manifest path in the `promotion-manifest-drift` job's own step
    commands, rather than anywhere in the file (a stale comment satisfied the
    old check); its `ci.yml`, `CHANGELOG.md` and `scripts/*.py` reads are
    hardened like every other evidence read on this branch.
  - Ack-admission generations are drawn from one ledger-wide sequence: a
    per-turn counter restarted at 1 for an evicted turn, colliding with the
    generation its own still-live chain held.
  - Drift surfaces closed: the endpoint member tuples are module constants
    every key roster derives from (the hand-written `_ENDPOINT_FAMILY_KEYS`
    mirror is gone), `load_promotion_manifest` adopted
    `effective_feature_policy_fingerprint`, the provisional manifest input
    roster is a named constant rather than an inline subtraction, the three
    manifest rosters `scripts/` imports became public names, and the slice
    import-boundary test discovers its roster from `server/*.py` instead of
    listing 6 of ~19 modules.
- Restart-gauntlet round 7 (confirm pass 4) fixed all 9 findings a 4-lens
  confirm pass over round 6's fix diff raised (2 logic, 2 security,
  5 architecture) — the smallest round of the loop:
  - **Behaviour change:** the vendor-credential aliases no longer resolve by
    configuration layer. Round 6 gave them the endpoint families' layer-first
    rule on the strength of the shape match, which let an ambient
    `OPENAI_API_KEY`/`DEEPGRAM_API_KEY` in a developer's shell — set for
    something else entirely — silently outrank the `WEBSEARCH_`-scoped
    credential in the project's env file and ship the wrong key to a vendor
    API. Every endpoint spelling is `WEBSEARCH_`-prefixed, which is what makes
    layer-first safe there; the credential aliases' bare spellings are
    deliberately not, so the scoped spelling now wins at any layer. Unset the
    scoped spelling to use a bare one.
  - The manifest producer/consumer split round 6 closed on the verifier side
    was still open on the writer side: `write_manifest` built its `inputs`
    from three literal phase keys while `verify_manifest` derived its roster
    from `server.config`. The writer now checks its own output against the
    same two constants. `MANIFEST_REQUIRED_PROVISIONAL_INPUTS` is a real
    independent literal rather than `FINAL - {"phase3"}`, with a parity
    assertion — round 6 moved that subtraction to a different file instead of
    removing the coupling it was meant to break.
  - The alias families adopted the endpoint families' `(name, members)` shape,
    and both kinds are now enumerated by one `_families()` registry that
    `load_config` and the provenance parity test share, so a family added
    later cannot land in only one of them.
  - Round 6's parsed-`ci.yml` drift check still passed on a job or step
    disabled with `if: false` and on a manifest path appearing only inside a
    quoted `echo` or a run-script comment — two of the three bypasses its own
    docstring claimed to have closed. It now rejects a conditional job, rejects
    a roster whose every referencing step is conditional, and matches the path
    as a whole shell word of a comment-stripped command. `check_release_metadata.py`'s
    `pyproject.toml` read is hardened like its siblings, and all three decode
    sites report `FAIL` rather than a traceback on non-UTF-8 input.
  - The Phase 2 `source_anchor` URL check anchored the version to a whole path
    segment in round 6 but left its *position* free, so
    `.../blob/refs/pull/9999/head/1.10.6/index.js` — a tree any outside
    contributor can populate — still validated. The version must now be the
    segment a recognised ref-introducing keyword introduces, in one of four
    named layouts.
  - The slice import-boundary test's roster was widened in round 6 while its
    detection stayed narrow: `from .workers import web_search` names the
    concrete worker as an imported *name*, not in the module path, and was
    missed. Detection is now a named, directly tested helper.
- Restart-gauntlet round 8 (confirm pass 5) fixed all 11 findings a 4-lens
  confirm pass over round 7's fix diff raised (4 logic, 2 security,
  4 architecture, 1 documentation):
  - **Behaviour change:** the `ci.yml` drift gate now requires a step that
    actually *runs* `scripts/validate_v013_evidence.py --verify-manifest`
    against the expected path, not one that merely names the path as a shell
    word — round 7's whole-word matching defeated a *quoted* `echo` reminder
    but not an unquoted one. It also rejects a truthy `continue-on-error` on
    the job, and treats it as disqualifying on a step, exactly as it does
    `if:`: `continue-on-error` disables the gate more completely than
    `if: false`, since the check runs, reports drift, and leaves CI green.
    Line continuations are joined before tokenising, so a correct workflow
    written in the standard `\`-continued style no longer fails the gate.
  - **Behaviour change:** `source_anchor` rejects a `;`-suffixed ref.
    `urlparse` splits a `;`-suffixed final path segment into `.params`, so
    `.../tree/v1.10.6;attacker-branch` was validated against the truncated
    `v1.10.6` while naming a different ref.
  - The endpoint families are declared once in `_ENDPOINT_FAMILIES`, which
    `load_config` iterates to resolve them and `_families()` splats — round 7
    derived the alias axis but left `load_config` hand-naming the endpoint
    member constants, so a third endpoint family could have been resolved
    while staying outside the registry. A scanning test now pins that every
    `_*_ENDPOINT_MEMBERS` constant is registered.
  - `_ALIAS_FAMILY_SPELLINGS` carries "this row's bare spelling is named at
    runtime" as data (a `None` bare key) instead of a
    `field_name == "openai_api_key"` comparison inside the generic builder,
    and `_winning_alias_key`'s parameter type is narrowed to the single-key
    member shape it actually supports rather than the endpoint families'
    shape, which it would have crashed on.
  - The `MANIFEST_REQUIRED_PROVISIONAL_INPUTS ⊆ FINAL` guard raises instead of
    asserting: `python -O` strips an assert, taking the only mechanism
    covering the "narrow `final`" direction with it.
  - `scripts/run_query_context_experiment.py`'s live credential gate resolves
    the OpenAI key through `load_config()` like its sibling scripts, instead
    of reading the bare `OPENAI_API_KEY` env var and reporting
    `provider_unavailable` to an operator who configured the scoped spelling.
- Review-gauntlet round 9 (confirm pass 6) — the consolidation and gate fixes
  above, closed for real:
  - **Behaviour change:** the `promotion-manifest-drift` gate now requires the
    verifier, `--verify-manifest`, and the expected path to appear in ONE
    command's argv. Testing them against the whole step's union of shell words
    let CI verify the *previous* release's manifest in one command while a
    second command merely named the current path (`ls`, `test -f`,
    `/bin/echo`, `env echo`, a `cat <<EOF` body) — the exact mention-vs-use
    bypass rounds 7 and 8 each set out to close. Reporting commands are now
    identified through wrappers and path spellings (`/bin/echo`, `env echo`,
    `LC_ALL=C echo`), heredoc bodies are not treated as commands, and
    redirection targets are not treated as arguments.
  - **Behaviour change:** the same gate rejects a drift job whose transitive
    `needs:` ancestor carries an `if:`. GitHub skips a job when any job it
    needs is skipped, so an ancestor's `if: false` switched the drift check off
    while the job itself stayed unconditional and the gate stayed green.
    `needs:` itself is still accepted — the real workflow's `needs: test` is
    legitimate.
  - **Behaviour change:** `source_anchor` rejects a ref ending in a *bare* `;`.
    The round-8 fold-back was guarded by `if parsed.params:`, and an empty
    `params` is falsy, so `.../tree/v1.10.6;` still validated against the
    truncated `v1.10.6`. The validator uses `urlsplit`, which performs no
    `params` splitting at all, so there is no reconstruction step to get wrong.
  - `_ENDPOINT_FAMILIES` rows carry a builder, so `load_config` both resolves
    and *consumes* the registry. Round 8 consolidated resolution only: a third
    registered family resolved correctly and was then silently dropped, its
    configured endpoint replaced by the dataclass default, with the
    registration scan still green. The half-pair guard reads the members
    stored by the resolution loop instead of naming `_TTS_ENDPOINT_MEMBERS`,
    making the registry docstring's claim true.
  - The OpenAI credential gate lives once, in `scripts/eval_common.py`
    (`resolve_openai_api_key`), instead of as two private copies; a config
    defect that makes `load_config()` raise is now diagnosed as
    `BLOCKED: provider_unavailable` with exit 1 rather than tracebacking out of
    `run_query_context_experiment.py`'s `run_live`.
- `scripts/eval_model_comparison.py`'s citations assertion no longer fails
  weather-query turns that were genuinely delegated and answered correctly:
  the hosted `web_search` tool answers weather via an internal `oai-weather`
  sub-tool whose sources never carry a citable URL, by design. Decoupled via
  a new `Turn.expect_citations` field (`evals/scenarios.py`), independent of
  `expect_delegated`.

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
