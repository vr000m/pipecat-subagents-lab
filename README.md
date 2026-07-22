# Pipecat Subagents Lab

Experimental Pipecat-native voice assistants with persistent specialist subagents and a lightweight browser RTVI client.

The first experiment will route requests through a tool-free main model to persistent context-owning subagents, speak concise answers through a local TTS server, and render structured web-search results plus interruption state in a plain browser client. Electron packaging is deferred until the browser protocol and interaction model are proven.

## Local runbook

This repository is the browser-first verification slice. It keeps the Python
session, worker contexts, result history, and connection epoch alive in one
process; the browser is a plain RTVI client. Electron packaging is deliberately
out of scope until these contracts have evidence behind them.

### Environment and local services

Load credentials in the shell from `~/.secrets/ai.env` (or provide an
equivalent environment file) without copying secret values into this
repository. The only credential used by the hosted-search worker is the
variable named by `WEBSEARCH_OPENAI_API_KEY_ENV`, defaulting to
`OPENAI_API_KEY`. The authenticated hosted-search smoke test is opt-in and
must be kept separate from the credential-free test suite.

The checked-in `config.toml` contains the local macOS socket defaults for the
Nemotron STT service and the sibling TTS service:

```toml
[stt]
stt_service = "websocket"
stt_ws_socket = "~/Library/Caches/pipecat-stt/nemotron.sock"
stt_language = "en"

[tts]
tts_ws_host = "127.0.0.1"
tts_ws_port = 8965
voice_id = "azelma"
```

Environment variables override the checked-in defaults. Use transport-qualified
endpoints when overriding them; discovery must not assume UDS or TCP:

```sh
export WEBSEARCH_STT_ENDPOINT=uds:///path/to/stt.sock
export WEBSEARCH_TTS_ENDPOINT=uds:///path/to/tts.sock
```

The equivalent overrides are `WEBSEARCH_STT_WS_SOCKET`,
`WEBSEARCH_TTS_WS_SOCKET`, and `WEBSEARCH_TTS_WS_URI`; for TTS, URI takes
precedence over socket, followed by `WEBSEARCH_TTS_WS_HOST` plus
`WEBSEARCH_TTS_WS_PORT`.

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
and `local STT transcription completed` in the server log. The local STT adapter
uses Pipecat's VAD-delimited segments and sends the Nemotron service raw 16 kHz
mono PCM16 rather than a WAV container.

On reconnect, the server-issued `session_id`, resume token, and proposed epoch
fence the replacement transport. The replacement receives a fresh snapshot of
workers, the complete per-worker Result Log, and delivery outcomes. Older
transport callbacks are rejected. Result text may appear before speech; the UI
marks server transport completion separately from incomplete or unconfirmed
speech and never claims verified browser audibility.

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
