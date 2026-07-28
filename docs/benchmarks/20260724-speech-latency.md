# Speech latency benchmark — 2026-07-24

## Scope

This is a five-run development-machine comparison, not a production SLA. The
local providers were Nemotron STT over UDS and Pocket TTS over loopback TCP.
Hosted providers were Deepgram `nova-3-general` and Cartesia `sonic-3.5`.

Both TTS providers synthesized “What is the capital of India?”. The last local
TTS result was resampled to 16 kHz mono PCM16 and used as the identical 2.32
second STT fixture for both recognizers. Deepgram received 20 ms audio frames,
matching the browser pipeline cadence. STT timing starts after the final audio
frame; provider connection setup is reported separately.

Run:

```sh
set -a
source ~/.secrets/ai.env
set +a
uv run python scripts/benchmark_speech.py --runs 5
```

## Results

| Metric | Local median / p95 | Hosted median / p95 |
| --- | ---: | ---: |
| TTS time to first audio | 25.0 / 81.7 ms | 92.2 / 114.0 ms |
| TTS synthesis complete | 132.0 / 236.5 ms | 336.9 / 409.7 ms |
| STT final after speech end | 71.5 / 138.4 ms | 240.5 / 277.3 ms |
| Connection setup | TTS 7.0 ms | Cartesia 177.4 ms; Deepgram 564.5 / 629.4 ms |

Both STT providers matched the expected transcript in all five runs.

## Pre-merge baseline — 2026-07-26

The following smoke results were captured from commit `f062d84` immediately
before the v0.1.0 merge. They are retained as a regression baseline, not as an
SLA or a statistically robust provider comparison.

### Live service and media smoke

Values-redacted preflight found the local Nemotron UDS, Pocket TTS loopback
websocket, and OpenAI credentials ready. The real-process server smoke also
passed health, browser assets, origin policy, session handshake, and clean
shutdown.

One cold local-only run used a 2.64-second fixture:

| Metric | Result |
| --- | ---: |
| Local TTS first audio | 202.6 ms |
| Local TTS synthesis complete | 331.3 ms |
| Local TTS connection setup | 6.1 ms |
| Local STT final after speech end | 2125.2 ms |

The local transcript matched exactly. The unusually slow cold STT sample did
not repeat in the immediately following comparison, so both cold and warm
observations are recorded rather than silently discarding the outlier.

The minimum valid two-run local-versus-hosted comparison then produced:

| Metric | Local median / p95 | Hosted median / p95 |
| --- | ---: | ---: |
| TTS time to first audio | 60.7 / 81.2 ms | 166.6 / 210.6 ms |
| TTS synthesis complete | 231.5 / 291.0 ms | 396.3 / 444.8 ms |
| STT final after speech end | 101.9 / 124.5 ms | 166.6 / 192.5 ms |
| Connection setup | TTS 6.4 ms | Cartesia 1395.8 ms; Deepgram 546.5 / 628.0 ms |

Local and Deepgram transcripts both matched exactly in both runs. With only two
samples, the reported p95 is effectively the slower observation and should not
be compared to a production percentile.

### Router and hosted-search smoke

The paid conversation smoke exercised the real router, a persistent hosted
web-search worker, citation normalization, and separate display/TTS
projections:

| Stage | Result |
| --- | ---: |
| Routing | 3069.5 ms |
| Hosted search | 50295.9 ms |
| Total | 53376.4 ms |
| Structured output | 508 display characters; 252 spoken characters; 12 citations |

The smoke passed its 15-second routing and 60-second total budgets. Hosted
search remained the dominant latency and consumed most of the total budget.

Reproduce the pre-merge checks with:

```sh
set -a
source ~/.secrets/ai.env
set +a
uv run python -m server.preflight
uv run python scripts/benchmark_speech.py --local-only --runs 1
uv run python scripts/benchmark_speech.py --runs 2
uv run python scripts/smoke_conversation.py
uv run python scripts/smoke_server.py
```

## Interpretation

Local speech is the lower-latency default on this machine. Cartesia and
Deepgram remain useful cloud deployment options, where local model processes
and Unix sockets are unavailable, but this sample does not justify selecting
them as a latency optimization.

The speech providers are not the dominant response delay. Earlier single
samples measured 10.1 seconds for one direct router response and 26.3 seconds
for one routed web-search response; the 2026-07-26 smoke instead routed in 3.1
seconds but spent 50.3 seconds in hosted search. These are observations rather
than benchmark distributions, but model/provider work remains two orders of
magnitude larger than warm TTS time-to-first-audio. The configured 1.5-second
Smart Turn completion grace is another deliberate fixed delay after a natural
pause.

The next latency work should therefore instrument and reduce router/worker
model time before changing the local speech defaults. Keep provider connection
setup outside the per-turn path by preserving connection-local service
instances for the lifetime of a browser epoch.

## Recording future live runs — v0.1.2 `PERF_METRIC` telemetry

The benchmark and smoke results above are the fixed pre-`PERF_METRIC` baseline
and are not rewritten by later runs. v0.1.2 adds grep-friendly `PERF_METRIC`
console telemetry (see `README.md`'s "Performance telemetry" section) that
gives every accepted turn and retained background item a correlated duration
without changing the router, search, speech, or benchmark harness behavior
exercised above.

To capture a live comparison sample against this baseline:

1. Run the existing benchmark and smoke commands unchanged (see "Reproduce the
   pre-merge checks" above); they still measure the same TTS/STT/routing/search
   stages.
2. Capture the server's stdout/stderr for the same run and extract the
   relevant `PERF_METRIC` lines, for example:

   ```sh
   rg 'PERF_METRIC event=app_turn_foreground' server.log
   rg 'PERF_METRIC event=work_item_foreground' server.log
   rg 'PERF_METRIC event=work_item_background' server.log
   ```

3. Append a new, dated `## Live PERF_METRIC sample — <date>` section below this
   one with the extracted `total_ms`, `routing_ms`, `search_ms`, and
   `background_ms` values and the exact `rg` filters used. Do not edit the
   `## Results` or `## Pre-merge baseline` sections above — they document what
   was measured before this telemetry existed and remain the historical
   comparison point.
