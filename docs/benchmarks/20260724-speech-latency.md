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

## Interpretation

Local speech is the lower-latency default on this machine. Cartesia and
Deepgram remain useful cloud deployment options, where local model processes
and Unix sockets are unavailable, but this sample does not justify selecting
them as a latency optimization.

The speech providers are not the dominant response delay. A separate live
diagnostic measured 10.1 seconds for one direct router response and 26.3
seconds for one routed web-search response. Those are single samples rather
than benchmark distributions, but they are two orders of magnitude larger than
warm TTS time-to-first-audio. The configured 1.5 second Smart Turn completion
grace is another deliberate fixed delay after a natural pause.

The next latency work should therefore instrument and reduce router/worker
model time before changing the local speech defaults. Keep provider connection
setup outside the per-turn path by preserving connection-local service
instances for the lifetime of a browser epoch.
