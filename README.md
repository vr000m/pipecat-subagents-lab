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
pending_dialogue_timeout_seconds = 30.0
foreground_search_timeout_seconds = 15.0
router_timeout_seconds = 12.0
provider_timeout_seconds = 75.0
shutdown_grace_seconds = 2.0
max_citations = 12

[models]
router_model = "gpt-5-mini"
worker_model = "gpt-5"

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

`uv run mypy` enforces type checking per-module rather than repo-wide: the
modules listed clean in `[tool.mypy]` are gated, the rest are reported but
ignored until brought clean. `server/perf_metrics.py` is gated because it owns
the closed `PERF_METRIC` vocabularies, where an unchecked string literal
becomes a dropped telemetry record at runtime.

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

Open <http://127.0.0.1:7860/> and use Connect. The browser requests microphone
permission as part of that explicit user action; the Mic button then mutes or
unmutes the active track. The browser obtains its session identity from
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
the page is open; the Connect action is the microphone permission boundary. When
separately configured
STT/TTS services are running, confirm a final transcript, local TTS synthesis,
and audible browser output. Disconnecting must disable capture. If autoplay is
blocked, use the page's audio action and record that diagnostic as local browser
state.

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

## Repository layout

```text
server/          Python and Pipecat runtime
web/             Bun-managed plain HTML, JavaScript, and CSS RTVI client
shared/          Shared message schemas and protocol documentation
docs/architecture.md
                 Stable current-state architecture and design decisions
docs/benchmarks/ Provider latency evidence
docs/dev_plans/  Reviewed implementation plans
```

## License

Licensed under the [BSD 2-Clause License](LICENSE), matching Pipecat's license.
