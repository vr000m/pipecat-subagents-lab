# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-07-25

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

### Fixed

- Wire worker clarification into pending-dialogue continuation: a web-search
  worker's clarifying question (`WorkerClarify`) is now caught at every
  pipeline dispatch site and recorded as a pending-dialogue candidate, so the
  existing `continue_pending`/steer/multi-intent arbitration — previously
  unreachable in production — actually resumes the paused work item on the
  next turn. Adds the `pending_dialogue_timeout_seconds` config
  (`WEBSEARCH_PENDING_DIALOGUE_TIMEOUT_SECONDS`, default 30s).

[Unreleased]: https://github.com/vr000m/pipecat-subagents-lab/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/vr000m/pipecat-subagents-lab/releases/tag/v0.1.0
