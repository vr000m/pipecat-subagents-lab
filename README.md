# Pipecat Subagents Lab

Experimental Pipecat-native voice assistants with persistent specialist subagents and a lightweight browser RTVI client.

The first experiment routes requests through a tool-free main model to
persistent context-owning subagents, speaks concise answers through a configured
TTS provider (local by default), and renders structured web-search results plus
interruption state in a plain browser client. Electron packaging is deferred
until the browser protocol and interaction model are proven.

Each hosted-search response is one strict structured envelope with a complete
`display_text` answer and a separate speech-friendly `spoken_text` summary.
Both projections retain the same canonical result ID and normalized citations.
The browser renders the complete answer while the connection-local speech
scheduler sends only the concise projection to TTS.

## Architecture and protocol

The Python process owns durable session state, persistent workers, bounded work,
and result history. Each accepted browser connection owns its Small WebRTC
transport, speech adapters, RTVI publisher, and awaited Pipecat pipeline
lifecycle; a server-issued epoch fences replaced connections and stale
callbacks.

See [Architecture](docs/architecture.md) for ownership, call flow, reconnect
semantics, provider boundaries, and design decisions. The versioned
Python-to-browser contract is documented in
[Browser protocol v1](shared/protocol.md), with machine-readable schemas in
`shared/schemas/`.

## Local runbook

This repository is the browser-first verification slice. It keeps the Python
session, worker contexts, result history, and connection epoch alive in one
process; the browser is a plain RTVI client. Electron packaging is deliberately
out of scope until these contracts have evidence behind them.

### Environment and local services

Load credentials in the shell from `~/.secrets/ai.env` (or provide an
equivalent environment file) without copying secret values into this
repository. Hosted search uses the variable named by
`WEBSEARCH_OPENAI_API_KEY_ENV`, defaulting to `OPENAI_API_KEY`. Optional hosted
speech uses `DEEPGRAM_API_KEY`, `CARTESIA_API_KEY`, and
`CARTESIA_VOICE_ID`. Authenticated smoke tests and benchmarks are opt-in and
must be kept separate from the credential-free test suite.

Hosted web search sends OpenAI the current worker request and, when available,
up to four prior entries from that same worker: each prior query plus at most
1,200 characters of its answer. Requests set `store=False`. This repository
does not independently verify or promise the provider's account-level or
abuse-monitoring retention behaviour, so operators should treat those fields
as external data egress and configure the provider account accordingly.

The checked-in `config.toml` contains the local macOS socket defaults for the
Nemotron STT service and the sibling TTS service:

```toml
[stt]
provider = "local"
stt_service = "websocket"
stt_ws_socket = "~/Library/Caches/pipecat-stt/nemotron.sock"
stt_language = "en"
model = "nova-3-general"

[turn]
smart_turn_timeout_seconds = 5.0
smart_turn_complete_grace_seconds = 1.5
speech_start_timeout_seconds = 10.0
speech_transport_grace_seconds = 1.0
pending_dialogue_timeout_seconds = 30.0
foreground_search_timeout_seconds = 15.0
router_timeout_seconds = 12.0
provider_timeout_seconds = 75.0
shutdown_grace_seconds = 2.0
max_citations = 12

[models]
router_model = "gpt-5.6-luna"
router_reasoning_effort = "medium"
worker_model = "gpt-5.6-terra"
worker_reasoning_effort = "medium"

[tts]
provider = "local"
tts_ws_host = "127.0.0.1"
tts_ws_port = 8965
voice_id = "azelma"
model = "sonic-3.5"
```

Set `stt.provider = "deepgram"` and/or `tts.provider = "cartesia"` to use
hosted speech. Provider model IDs remain in TOML; credentials and the Cartesia
voice UUID remain environment-only. The equivalent provider/model overrides
are `WEBSEARCH_STT_PROVIDER`, `WEBSEARCH_STT_MODEL`,
`WEBSEARCH_TTS_PROVIDER`, and `WEBSEARCH_TTS_MODEL`. A selected hosted provider
fails closed at startup when its required environment values are absent.

Environment variables override the checked-in defaults. Use transport-qualified
endpoints when overriding them; discovery must not assume UDS or TCP:

```sh
export WEBSEARCH_STT_ENDPOINT=uds:///path/to/stt.sock
export WEBSEARCH_TTS_ENDPOINT=uds:///path/to/tts.sock
```

The equivalent overrides are `WEBSEARCH_STT_WS_SOCKET`,
`WEBSEARCH_TTS_WS_SOCKET`, and `WEBSEARCH_TTS_WS_URI`; for TTS, URI takes
precedence over socket, followed by `WEBSEARCH_TTS_WS_HOST` plus
`WEBSEARCH_TTS_WS_PORT`. `WEBSEARCH_SMART_TURN_TIMEOUT_SECONDS` overrides the
semantic-turn fallback. After Smart Turn reports an incomplete turn, each new
speech boundary resets this timer; five silent seconds finalize the accumulated
transcript by default. `WEBSEARCH_SMART_TURN_COMPLETE_GRACE_SECONDS` controls a
separate application debounce after Smart Turn reports a complete turn. Speech
that resumes within the default 1.5-second grace remains part of the same turn.
`WEBSEARCH_SPEECH_START_TIMEOUT_SECONDS` bounds how long a generation handed
to TTS may wait for a correlated start/audio/provider-error event before the
speech lifecycle coordinator releases it as `delivery_unknown`; the default
is 10 seconds. `WEBSEARCH_SPEECH_TRANSPORT_GRACE_SECONDS` is the grace added
after synthesis end (plus accumulated audio duration) or after an
interruption/pause is forwarded, before the coordinator tears down the
transport slot; the default is 1 second. Their TOML equivalents are
`[turn].speech_start_timeout_seconds` and
`[turn].speech_transport_grace_seconds`.
`WEBSEARCH_PENDING_DIALOGUE_TIMEOUT_SECONDS` controls how long a worker's
clarifying question remains available for continuation; the default is 30
seconds. Its TOML equivalent is `[turn].pending_dialogue_timeout_seconds`.
`WEBSEARCH_FOREGROUND_SEARCH_TIMEOUT_SECONDS` bounds how long a voice turn
waits for one search before acknowledging safe background continuation.
`WEBSEARCH_ROUTER_TIMEOUT_SECONDS` and `WEBSEARCH_PROVIDER_TIMEOUT_SECONDS`
bound hosted routing and worker requests, while
`WEBSEARCH_SHUTDOWN_GRACE_SECONDS` prevents cancellation-resistant work from
blocking server shutdown. `WEBSEARCH_MAX_CITATIONS` caps the normalized source
list. Their TOML equivalents use the same lowercase names under `[turn]`.
`WEBSEARCH_MAX_WORK_ITEMS_PER_TURN` limits one routed turn to 2, 3, or 4 work
items and defaults to 2. `WEBSEARCH_MULTI_INTENT_WAIT_TIMEOUT_MS` controls how
long a multi-intent turn waits for all routed work before returning its
available results; it must be a positive integer and defaults to 10000
milliseconds. These two settings are environment-only and have no TOML
equivalents.
`WEBSEARCH_ROUTER_MODEL` and `WEBSEARCH_WORKER_MODEL` override the configured
OpenAI model IDs without allowing model output to select an arbitrary model.
Their TOML equivalents are `[models].router_model`/`[models].worker_model`
(checked-in defaults above reflect the
`docs/dev_plans/artifacts/router-worker-eval-shortlist-2026-08-20.md`
comparison, machine-checked against
`docs/dev_plans/artifacts/eval-candidates-manifest.json` — the shortlist is
the human-facing decision record, the manifest is the machine-checked gate
the paid runner refuses to run without). `WEBSEARCH_ROUTER_REASONING_EFFORT` and
`WEBSEARCH_WORKER_REASONING_EFFORT` override the `reasoning.effort` value
sent with router/worker requests (validated against the OpenAI SDK's
`ReasoningEffort` literal: `none`, `minimal`, `low`, `medium`, `high`,
`xhigh`, `max`); unset preserves current behavior (router keeps its
`gpt-5*`-conditional `minimal` default, worker omits `reasoning` entirely),
while an explicit override applies unconditionally regardless of model name.
Overriding only a role's model at a higher-precedence layer than the one that
set its effort clears that inherited effort, so an env-only model override
does not silently keep a TOML effort default.
Their TOML equivalents are `[models].router_reasoning_effort`/
`[models].worker_reasoning_effort`.

The v0.1.3 delivery behaviours are gated by feature flags whose TOML
equivalents live under `[features]`. `WEBSEARCH_ENABLE_EARLY_ACK`
(`[features].enable_early_ack`, default `true`) controls whether a routed turn
speaks a short acknowledgement before its first result is ready.
`WEBSEARCH_EARLY_ACK_TEXT` (`[features].early_ack_text`, default
`One moment while I look into that.`) is the spoken text used for that
acknowledgement and must not be empty.
`WEBSEARCH_ENABLE_BACKGROUND_STATUS`
(`[features].enable_background_status`, default `true`) gates server-side
emission of the capability-gated `work_status` updates described below;
when disabled, no `work_status` frame is produced and the legacy
foreground-timeout notice applies instead.
`WEBSEARCH_ENABLE_AUTOPLAY_POLICY` (`[features].enable_autoplay_policy`,
default `true`) enables the browser autoplay-recovery policy that surfaces the
page's audio action when playback is blocked.
`WEBSEARCH_PROMOTION_MANIFEST_PATH`
(`[features].promotion_manifest_path`, default
`docs/benchmarks/v0.1.3-promotion-manifest.json`) points at the release
promotion manifest that the server bind-checks at startup; it must not be
empty. `WEBSEARCH_PHASE4C_ARTIFACT_PATH`
(`[features].phase4c_artifact_path`, default unset) points at the Phase 4C
completion artifact whose SHA-256 the manifest's `phase4c_artifact_sha256`
field is checked against, when present; leaving it unset makes any manifest
declaring that field resolve to a `phase4c_unresolvable` (display-only)
verdict. `WEBSEARCH_RELEASE_VERSION` (`[features].release_version`, default
the installed package version) overrides the release version the promotion
manifest's own `release_version` field is bind-checked against; it must not be
empty, and overriding it to a version the manifest was not cut for resolves to
a display-only verdict. The three boolean flags are parsed strictly: only `true` or `false`
(case-insensitive) is accepted, and any other value fails startup.

The accepted endpoint forms are `uds://`, `tcp://`, `ws://`, and `wss://`.
The default host opens these websocket endpoints with the versioned local STT
and TTS wire clients; adapter-level client factories remain injectable for
alternate verified clients.
Run the values-redacted provider preflight before connecting a browser:

```sh
uv run python -m server.preflight
```

The same concrete probe backs `/api/readyz`; it verifies the local websocket
hello/session handshake or the presence of the selected hosted provider's
required credentials. `/api/healthz` remains process liveness and does not
claim dependency readiness.

### Verification commands

From the repository root:

```sh
uv sync
uv run pytest
uv run ruff format --check .
uv run ruff check .
uv run mypy
gitleaks git --no-banner --redact
cd web
bun install
bun run build
bun test
bun run lint
```

A `justfile` at the repo root wraps the Python half of these into `just
sync` / `just py-check` / `just build` / `just web-test` / `just check` (the
full compile+test pipeline) / `just run` (starts the real server, loading
`~/.secrets/ai.env` or `$AI_ENV_FILE`) / `just all` (install, check, run).
`just check` is the recommended local entrypoint before opening or updating
a PR — it runs everything `just py-check`/`build`/`web-test` do in one command;
the raw `uv`/`bun` commands above remain the reference for scripting or
running a single check in isolation. Its `sync`/`build` recipes intentionally
use the same `--frozen` / `--frozen-lockfile` flags as
`.github/workflows/ci.yml` so a lockfile drift fails the same way locally and
in CI. `tests/test_justfile_ci_parity.py` asserts every `uv run`/`bun`
command CI's `test` job executes is reachable from `just check`'s recipe
closure, so the two files drifting out of sync now fails a test rather than
depending on a developer noticing by hand.

`uv run mypy` checks every module under `server/` by default, so a newly added
file is type-gated without anyone remembering to opt it in. The explicit
override list in `pyproject.toml` exempts the modules that still carry
pre-existing errors; it is legacy debt to shrink, not a policy, and new code
should never be added to it. `server/perf_metrics.py` is among the checked
modules because it owns the closed `PERF_METRIC` vocabularies, where an
unchecked string literal becomes a dropped telemetry record at runtime.

Run `bun run build` before opening or serving `web/index.html`: `dist/` is
intentionally ignored, while lint writes its bundle only to `/tmp`.

After the browser bundle is built, run the credential-free process smoke:

```sh
uv run python scripts/smoke_server.py
```

It starts the real FastAPI/Uvicorn process on an ephemeral loopback port,
captures its logs, verifies liveness, browser assets, same-origin session
discovery, cross-origin rejection, the versioned session handshake, and clean
shutdown. It does not claim WebRTC media, STT, TTS, or hosted-model acceptance.

With the configured local speech services running, verify the local
TTS-to-PCM-to-STT round trip separately:

```sh
uv run python scripts/benchmark_speech.py --local-only --runs 1
```

This local-media smoke needs no hosted credentials and fails if STT does not
recover the normalized fixture text. It deliberately remains outside the
credential-free CI job because the local socket services are machine-owned.

Finally, with `OPENAI_API_KEY` loaded, run one bounded paid router-to-worker
smoke:

```sh
uv run python scripts/smoke_conversation.py
```

The command prints only stage timings, projection lengths, worker identity, and
citation count. It fails on routing fallbacks, main-responder fallback, missing
citations, an invalid spoken projection, routing over 15 seconds, total turn
latency over 60 seconds, or a default 120-second process deadline. Override the
budgets with `--max-routing-seconds` and `--max-latency-seconds`. The outer
deadline prevents a provider that ignores in-process cancellation from hanging
the merge check indefinitely.

For the 0.1.1 routing regression, the following paid live smoke is a required
release gate. With the same credential loaded, run the three-turn sequence that
starts with `Hi.` and then asks for weather in Riga and Helsinki:

```sh
uv run python scripts/smoke_conversation.py --routing-regression --timeout 180
```

It verifies that the greeting takes the direct path without creating a worker,
then verifies that both weather turns route to workers rather than the main
routing fallback. Citation and projection validation remains in the single-turn
paid smoke above. The conversational-direct policy is provider prompt behavior,
so the credential-safe pytest suite cannot deterministically prove it; do not
tag or publish 0.1.1 without a passing routing-regression run. This exercises the
live OpenAI router and worker without requiring browser media or local STT/TTS
services.

For the 0.1.3 early-ack/background-delivery policy, the following paid live
smoke proves the ack-ordering contract itself rather than only the final
result:

```sh
uv run python scripts/smoke_conversation.py --ack-ordering
```

Prerequisites are the same as the paid smokes above and no more: an OpenAI
credential in the environment (`set -a; source ~/.secrets/ai.env; set +a`) for
the live router and search worker. It needs **no** browser media, no WebRTC
transport, and no local or hosted STT/TTS service — the "recording TTS/worker"
mentioned below are in-process test doubles the script installs itself
(`scripts/smoke_conversation.py::_run_ack_ordering`), which record the frames
handed to them and synthesize no audio, so the scenario adds no TTS cost beyond
the router/worker calls.

Unlike the other scenarios above, this one wires that recording TTS/worker into
the connection so the scheduler actually admits speech, then drives a real
delegated search and asserts the early ack is admitted before the delegated
result — proving the externally observable early-ack behavior against a live
provider, not just the fake-coordinator coverage in
`tests/test_pipeline.py::test_early_ack_is_enqueued_immediately_after_delegated_search_dispatch`.
Override the ack budget with `--max-ack-seconds` (default 15s) if routing
latency is consistently tighter or looser than that.

To compare the configured local services with Deepgram and Cartesia using the
same text and PCM fixture:

```sh
set -a
source ~/.secrets/ai.env
set +a
uv run python scripts/benchmark_speech.py --runs 5
```

The script reports warm time-to-first-audio, synthesis completion,
speech-end-to-final-transcript latency, and connection setup separately. It
prints aggregate timings and transcript equality only; it does not print
credentials or provider payloads. See
`docs/benchmarks/20260724-speech-latency.md` for the current machine snapshot
and interpretation.

### Start the local browser server

Build the browser bundle, then serve the bundled page and the Small WebRTC
signaling endpoint from the same FastAPI process:

```sh
cd web && bun run build && cd ..
uv run python -m server.app
```

Open <http://127.0.0.1:7860/> and use the toolbar's connect/disconnect toggle
button. The browser requests microphone permission as part of that explicit
user action; the Mic button then mutes or unmutes the active track. Once
connected, the microphone and speaker dropdowns populate with the available
devices and can be used to switch input/output routing mid-session; the
speaker dropdown is hidden entirely in browsers that don't support
`HTMLMediaElement.setSinkId()`. The browser obtains its session identity from
`/api/session` and posts Small WebRTC offers to `/api/rtc`. Daily transport is
not part of this
local path; if the lab is deployed to a cloud runtime later, add Daily as a
deployment adapter behind the same session and RTVI contracts.

The integration test is `tests/integration/test_browser_session.py`; it is a
credential-free contract test, not proof that live browser media is available.
It uses fake model/provider boundaries and proves the routing matrix, same-topic
context persistence, unrelated-worker isolation, citation normalization,
canonical speech/UI identity, result-before-speech ordering, interruption on
connection replacement, stale-epoch fencing, and snapshot reconstruction. It
does not call paid APIs or require live STT/TTS services.

### Browser and media acceptance

Start the application host and the browser client using the local
Pipecat/Small WebRTC server configuration for the checkout. Connect only after
the page is open; the connect toggle is the microphone permission boundary.
The mic/speaker selectors and Mic button stay disabled until connected, and
disconnecting (explicitly, or via a transport-driven drop) must disable and
clear them again. When separately configured
STT/TTS services are running, confirm a final transcript, local TTS synthesis,
and audible browser output. Disconnecting must disable capture. If autoplay is
blocked, use the page's audio action and record that diagnostic as local browser
state. To verify device switching, pick a different entry from the mic or
speaker dropdown mid-session and confirm the browser's console log shows a
`[speaker] setSinkId succeeded` or `[mic] switching mic device` diagnostic; a
rejected speaker switch (e.g. the device was removed) logs `Failed to switch
speaker` and the dropdown reverts to the last device that was actually
routed.

A browser declares optional protocol capabilities with a single `capabilities`
query parameter — one URL-encoded JSON array of capability names — on both the
`POST /api/rtc` offer and the `PATCH /api/rtc` ICE-candidate request. The
server normalizes, deduplicates, and sorts the declared set and binds it
immutably to the promoted connection epoch; a `PATCH` either omits the field or
repeats the identical set. A client that declares `work_status_v1` receives
coarse `work_status` updates (`routing`, `searching`, `background`,
`result_ready`, `failed`, `cancelled`) for delegated work, including in its
reconnect snapshot; a client that declares nothing keeps the legacy behaviour
with no `work_status` frames. `result_ready` means the canonical result is
committed and display-ready, not that speech was queued, delivered, or heard.
Emission is additionally gated server-side by
`WEBSEARCH_ENABLE_BACKGROUND_STATUS`. See `shared/protocol.md` for the wire
contract, transition rules, and parent aggregation semantics.

For live STT, `Track audio received` proves only WebRTC negotiation. A complete
utterance should also produce `VADProcessor: User started speaking`,
`VADProcessor: User stopped speaking`, `committed <N> PCM bytes to local STT`,
and `local STT transcription completed` in the server log. VAD may commit several
short acoustic segments for one utterance; `UserTurnProcessor: User stopped
speaking (strategy: TurnAnalyzerUserTurnStopStrategy...)` is the semantic turn
boundary that routes their combined transcript once. The local STT adapter uses
Pipecat's VAD-delimited segments and sends the Nemotron service raw 16 kHz mono
PCM16 rather than a WAV container.

For live TTS, a completed worker result should produce
`Generating TTS [<spoken projection>]` followed by local synthesis events and
outbound audio. The Pipecat worker bus bridge explicitly keeps
`TTSSpeakFrame` on the browser connection pipeline; sending that frame to the
worker bus would remove it before `LocalTTS`.

On reconnect, the server-issued `session_id`, resume token, and proposed epoch
fence the replacement transport. The replacement receives a fresh snapshot of
workers, the complete per-worker Result Log, and delivery outcomes. Older
transport callbacks are rejected. Result text may appear before speech; the UI
marks server transport completion separately from incomplete or unconfirmed
speech and never claims verified browser audibility. Transcript turns show their
server-authored local-time timestamp. Assistant turns show the exact concise TTS
projection as the primary message once speech is queued. A result that arrives
after a foreground timeout is queued once behind active speech when its original
TTS connection is still active; stale-epoch or TTS-less results remain
display-only and are labelled `background result`. A `Subagent output`
disclosure caret contains
the complete structured answer, worker/turn metadata, delivery state, and
sources; the complete history remains available in the Result Log.

### Boundaries and diagnostics

Pipecat logs are the authoritative diagnostic record. Browser state is a
versioned product/debug projection and intentionally excludes raw logs, full
prompts, and private worker context. Source links are normalized to absolute
HTTP(S) URLs and open with `target="_blank" rel="noopener noreferrer"`.

The browser client also writes structured diagnostics to the browser console,
one line per connection/track/device event:
`[HH:MM:SS.mmm][component] message [data]`, e.g.
`[14:02:11.483][speaker] setSinkId succeeded { deviceId: "..." }`. This is a
local, ephemeral debug aid only — Pipecat server logs remain the record of
truth for anything that needs to be reported.

The paid OpenAI web-search smoke test and the local STT/TTS/Small WebRTC media
acceptance are environment-dependent. Report them separately from the
credential-safe unit and integration tests. Any semantic protocol change found
during verification must return to the earlier contract/runtime/browser phases;
Phase 5 documentation does not silently change those contracts.

## Performance telemetry (`PERF_METRIC`)

Every performance record is one console line beginning `PERF_METRIC
event=<name> schema=1`, defined in `server/perf_metrics.py`. Select all
records with:

```sh
rg 'PERF_METRIC'
```

Narrow to one event, and further to one correlation ID, with:

```sh
rg 'PERF_METRIC event=user_bot_latency'
rg 'PERF_METRIC event=work_item_foreground' | rg 'turn_id="turn-7"'
```

Framework-owned events come from Pipecat 1.6.0 observers attached to each
browser connection's `PipelineWorker` — one `StartupTimingObserver`, one
`UserBotLatencyObserver`, and handlers on the worker's own default
`turn_tracking_observer` (no duplicate turn tracker is created):

- `pipeline_startup`, `transport_ready` — processor and Small WebRTC
  connection timing. `bot_connected_ms` is SFU-only and is omitted (never
  zero-filled) under the Small WebRTC transport this app uses.
- `pipecat_turn_start`, `pipecat_turn_end` — Pipecat's own conversation-turn
  boundaries, identified only by `pipecat_turn`; this is not the application
  `turn_id`.
- `first_bot_speech_latency`, `user_bot_latency`, `service_latency` — speech
  latency and, when `PipelineParams(enable_metrics=True)` and a processor
  actually reports a `MetricsFrame`, its per-service breakdown
  (`ttfb`/`text_aggregation`/`function_calls`/`user_turn_secs`). Missing
  values are omitted, never invented as zero.

Application-owned events measure routing, worker dispatch, and retained
background completion outside the Pipecat frame graph:

- `app_turn_foreground` — exactly one per accepted semantic turn, identified
  by the application `turn_id` (never the Pipecat `pipecat_turn`). Reports a
  closed `outcome` (`direct`/`unsupported`/`control`/`clarify`/`completed`/
  `mixed`/`retained`/`declined`/`failed`/`cancelled`) plus exhaustive
  `*_count` fan-out counters. A single-intent direct/unsupported/clarify
  response resolved by the router attributes its own category counter without
  dispatching (and so without counting toward) a child work item.
- `work_item_foreground` — one per dispatched single- or multi-intent child,
  carrying the parent `turn_id` and its own `work_item_id`.
- `work_item_background` — one per registered retained work item, reporting
  independent `work_outcome`/`commit_outcome`/`speech_outcome` axes so a
  successful search whose result is suppressed or cannot be spoken is never
  mislabeled as a failed search.

Every producer emits through one `MeasurementSink` owned by `SessionHost` for
its process lifetime: `ConsoleMeasurementSink` in production, and
`CollectingMeasurementSink` (indexed by event, `turn_id`, and `work_item_id`)
in tests and the smoke harness, injected via
`_default_session_host(measurement_sink=...)`. `scripts/smoke_conversation.py`
reads its correlated turn/child records straight off the collecting sink
instead of a latest-value cache, so a preceding turn can never supply the
current turn's budget values. No record ever contains transcript, prompt,
response, citation, or credential content — this stays console-only and does
not project into RTVI or browser state.

## Scripts

Every module in `scripts/` except `_evidence_common.py` (a shared helper) is a
command-line entry point. Only the paid harnesses need hosted credentials;
every gate and validator is credential-free.

Smoke and benchmark:

- `smoke_server.py` — starts the real server and verifies its credential-free
  HTTP boundary.
- `smoke_conversation.py` — bounded, paid router-to-web-search conversation
  smoke. Useful when changing turn handling or delegation.
- `benchmark_speech.py` — local speech smoke (credential-free) or paid
  provider comparison over identical text and audio.

Router/worker model evaluation (run in this order):

- `verify_eval_candidates.py` — live-verifies every candidate (model, effort,
  tools) tuple with production-equivalent request shapes and writes a
  versioned manifest that gates the runner below. The manifest is written to
  the tracked `docs/dev_plans/artifacts/eval-candidates-manifest.json`
  (`DEFAULT_MANIFEST_RELATIVE_PATH` in `scripts/eval_common.py`) — unlike the
  run reports below, this path IS git-tracked, since both the verifier and
  the runner need to read it from a fresh checkout; overridable via `--out`
  on this writer and `--manifest-path` on the runner. Its judge probe pins
  `reasoning_effort="minimal"` (`scripts/eval_common.py`'s
  `build_judge_request_kwargs`/`judge_extra_kwargs`) because the judge runs
  the Chat Completions API on a `gpt-5*` reasoning model: without an explicit
  low effort, hidden reasoning tokens can consume the entire
  `max_completion_tokens` budget before any verdict text is emitted, unlike
  the router/worker (Responses API), which already configures effort
  explicitly per candidate.
- `eval_model_comparison.py` — paid runner that drives `SessionHost` through
  its real connect/turn lifecycle for each router/worker model+effort
  candidate and scenario, scoring replies via `pipecat.evals.judge.EvalJudge`
  and producing one aggregate pass/fail + latency report. Supports
  `--dry-run` (zero live calls), `--router`/`--worker`/`--scenario` for a
  single cheap smoke pair (valid `--router` labels: `baseline`, `luna-high`,
  `luna-medium`, `terra-low`; valid `--worker` labels: `baseline`,
  `terra-medium`, `sol-low` — these are `Candidate.label` values from
  `ROUTER_CANDIDATES`/`WORKER_CANDIDATES` in `scripts/eval_common.py`, the
  source of truth if this list drifts; `shipped` is NOT a selectable label,
  see the `--router <shipped-label>` note below), `--max-calls`/`--max-cost`
  spend gates, and `--i-know-the-manifest-is-stale` to proceed against a
  manifest whose
  recorded source commit no longer matches `HEAD` — the manifest's
  `source_commit` is checked for exact equality, so any commit made after the
  manifest was written trips staleness even when the commit is unrelated to
  the OpenAI request shapes the manifest attests to; regenerate a fresh
  manifest with `verify_eval_candidates.py` rather than reaching for the
  override unless you've confirmed the intervening commits didn't touch
  model IDs, effort values, or request-kwargs building. Every run's aggregate
  report is always persisted (default: a timestamped file under
  `.review-plan/eval-reports/`; override with `--out`), not only when `--out`
  is passed.
  - `--repeat N` (default 1) — run each (pair, scenario) cell N times live
    and majority-vote the results into one summary, with every raw
    repetition preserved on the report for audit. Live calls run at
    `temperature=1.0`, so a single sample is noisy; this is the recommended
    way to get a stable comparison signal for the default sweep. A
    coin-flip tie always resolves toward the failing outcome, never a
    silent pass.
  - `--full-matrix` — opts into the full router x worker cross product
    instead of the default one-at-a-time sweep (baseline x baseline, each
    router candidate x baseline worker, baseline router x each worker
    candidate). The router cannot influence worker output in this codebase
    (no query/context flows from `RoutingDecision` into the worker
    dispatch), so the cross product's non-baseline x non-baseline cells add
    no comparison signal beyond `--repeat`'s resampling of the same
    one-at-a-time cells — use it only to confirm two already-good
    candidates work well together, not as the default comparison method. The
    default sweep does not run a live shipped x shipped cell (config.toml's
    current router/worker paired together) — the one-at-a-time sweep already
    contains a cell for each shipped candidate against the historical
    baseline, and the aggregate report's `shipped_config_cells` key names
    which cells those are; run `--router <shipped-label> --worker
    <shipped-label>` explicitly to measure the joint pairing.

Phase 4 query-context narrowing experiment (run in this order):

- `run_query_context_experiment.py` — bounded experiment runner producing raw
  samples (Phase 4A).
- `collect_query_context_latency.py` — normalizes raw samples into the
  artifact the analyzer consumes (Phase 4A).
- `analyze_query_context_latency.py` — deterministic, credential-free
  promotion decision over the collected artifact (Phase 4B).

Release and evidence gates:

- `emit_v013_deployment_metadata.py` — derives the deployment identity
  (`PIPECAT_SOURCE_COMMIT`, `PIPECAT_SOURCE_TREE_HASH`,
  `PIPECAT_DEPLOYED_AT_UTC`, `PIPECAT_FEATURE_POLICY_FINGERPRINT`) from a
  clean release checkout. Refuses a dirty or untracked tree.
- `validate_v013_evidence.py` — validates evidence artifacts against their
  schemas and phase minimums, and with `--write-manifest` writes the
  promotion manifest.
- `validate_phase2_transport_browser_contract.py` — validates the Phase 2
  transport/browser contract artifact.
- `record_phase3_completion.py` — records the exact Phase 3 command digest,
  source commit, and tree hash after that phase's test command passes.
- `check_release_metadata.py` — checks that `pyproject.toml` and
  `CHANGELOG.md` agree on the release version and date. Run at release
  finalization.

## Promotion manifest

`docs/benchmarks/v0.1.3-promotion-manifest.json` is the single artifact that
decides whether the runtime may enable autoplay. It binds the accumulated
evidence artifacts (their SHA-256 hashes plus the schema hash) to one
deployment identity: release version, source commit, filtered source tree
hash, `FeaturePolicy` fingerprint, and deployment timestamp. At startup
`server.config.load_promotion_manifest()` re-verifies every binding against
the running `Config` and **fails closed** — a missing, malformed, foreign, or
stale manifest silently degrades to display-only rather than blocking boot.

A `manifest_phase=provisional` manifest is permanently
`promotion_eligible=false` and exists only for diagnostics; only a
`manifest_phase=final` manifest bound to a Phase 3 completion record can make
the runtime promotion-eligible.

CI's `release-metadata` job (`.github/workflows/ci.yml`, `main` pushes only)
regenerates the provisional manifest: it runs
`emit_v013_deployment_metadata.py --shell-export` into `$GITHUB_ENV`, then
`validate_v013_evidence.py --write-manifest --manifest-phase provisional`
with the Phase 0-2 inputs and the exported identity values.

To regenerate locally, export the same identity from a clean checkout and run
the writer by hand:

```sh
eval "$(uv run python scripts/emit_v013_deployment_metadata.py --shell-export)"
uv run python scripts/validate_v013_evidence.py --write-manifest --manifest-phase provisional \
  --phase0-input docs/benchmarks/v0.1.3-phase0-transport-baseline.jsonl \
  --phase1-input docs/benchmarks/v0.1.3-phase1-ack-evidence.jsonl \
  --phase2-input docs/benchmarks/v0.1.3-phase2-transport-browser-contract.json \
  --source-commit "$PIPECAT_SOURCE_COMMIT" \
  --source-tree-hash "$PIPECAT_SOURCE_TREE_HASH" \
  --deployed-at-utc "$PIPECAT_DEPLOYED_AT_UTC" \
  --feature-policy-fingerprint "$PIPECAT_FEATURE_POLICY_FINGERPRINT" \
  --output docs/benchmarks/v0.1.3-promotion-manifest.json
```

Add `--manifest-phase final --phase3-input
docs/benchmarks/v0.1.3-phase3-completion.json` (and optionally
`--phase4c-input …`) to produce the final, activation-eligible manifest. A
dirty tree makes the emitter refuse, so a locally-modified checkout cannot
produce an eligible manifest.

## Repository layout

```text
server/          Python and Pipecat runtime
web/             Bun-managed plain HTML, JavaScript, and CSS RTVI client
shared/          Shared message schemas and protocol documentation
evals/           Router/worker model-comparison eval scenarios
docs/architecture.md
                 Stable current-state architecture and design decisions
docs/benchmarks/ Provider latency evidence
docs/dev_plans/  Reviewed implementation plans
```

## License

Licensed under the [BSD 2-Clause License](LICENSE), matching Pipecat's license.
