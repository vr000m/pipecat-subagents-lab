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
`WEBSEARCH_ROUTER_MODEL` and `WEBSEARCH_WORKER_MODEL` override the configured
OpenAI model IDs without allowing model output to select an arbitrary model.

The accepted endpoint forms are `uds://`, `tcp://`, `ws://`, and `wss://`.
The default host opens these websocket endpoints with the versioned local STT
and TTS wire clients; adapter-level client factories remain injectable for
alternate verified clients.
The values-redacted preflight is implemented by `server.preflight.run_preflight`.
Call it from the host integration that owns a service probe before connecting a
browser. This repository does not expose a standalone preflight CLI.

### Verification commands

From the repository root:

```sh
uv sync
uv run pytest
uv run ruff format --check .
uv run ruff check .
cd web
bun install
bun run build
bun test
bun run lint
```

Run `bun run build` before opening or serving `web/index.html`: `dist/` is
intentionally ignored, while lint writes its bundle only to `/tmp`.

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
projection as the primary message. A `Subagent output` disclosure caret contains
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

## Planned layout

```text
server/          Python and Pipecat runtime
web/             Bun-managed plain HTML, JavaScript, and CSS RTVI client
shared/          Shared message schemas and protocol documentation
docs/dev_plans/  Reviewed implementation plans
```

## License

Licensed under the [BSD 2-Clause License](LICENSE), matching Pipecat's license.
