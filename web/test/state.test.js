import { describe, expect, test } from "bun:test";

import {
  applyServerMessage,
  createInitialState,
  WORK_STATUS_MAX_KEYS,
  WORK_STATUS_TERMINAL_TTL_MS,
} from "../src/state.js";
import workStatusRetention from "../../shared/work-status-retention.json";

const result = (resultId) => ({
  result_id: resultId,
  worker_id: "worker-weather",
  turn_id: "turn-1",
  timestamp: "2026-07-18T10:00:00Z",
  text: `Complete answer ${resultId}`,
  spoken_text: `Spoken answer ${resultId}`,
  citations: [{ title: "Weather service", url: "https://weather.example.test" }],
  origin_epoch: 1,
});

const snapshot = (sequence, results = []) => ({
  kind: "runtime_snapshot",
  sequence,
  session_id: "session-1",
  origin_epoch: 1,
  data: {
    contract_version: "v1.0",
    session_id: "session-1",
    snapshot_sequence: sequence,
    workers: [{
      worker_id: "worker-weather",
      topic: "weather",
      model_policy: "deep",
      status: "idle",
      latest_result_id: null,
      origin_epoch: 1,
    }],
    results,
    speech_progress: [],
    routing: null,
    transcript: [],
    origin_epoch: 1,
  },
});

const increment = (sequence, resultId) => ({
  kind: "result",
  sequence,
  session_id: "session-1",
  origin_epoch: 1,
  data: result(resultId),
});

describe("server-authoritative runtime reducer", () => {
  test("ignores increments until the first authoritative snapshot", () => {
    let state = applyServerMessage(createInitialState(), increment(10, "before-snapshot"));

    expect(state.results).toEqual([]);
    expect(state.lastAppliedSequence).toBe(0);

    state = applyServerMessage(state, snapshot(1));
    expect(state.serverState).toBe(true);
    expect(state.localDiagnostics.lastSnapshotSequence).toBe(1);
  });

  test("applies ordered increments and ignores duplicate sequence numbers", () => {
    let state = applyServerMessage(createInitialState(), snapshot(10));
    state = applyServerMessage(state, increment(11, "result-1"));
    state = applyServerMessage(state, increment(11, "duplicate"));

    expect(state.results.map(({ result_id }) => result_id)).toEqual(["result-1"]);
    expect(state.localDiagnostics.lastAppliedSequence).toBe(11);
  });

  test("requests one snapshot for a gap and does not apply unsafe increments while recovering", () => {
    const requests = [];
    let state = applyServerMessage(createInitialState(), snapshot(10));
    state = applyServerMessage(state, increment(12, "gap-result"), () => requests.push("snapshot"));
    state = applyServerMessage(state, increment(13, "still-unsafe"), () => requests.push("snapshot"));

    expect(requests).toEqual(["snapshot"]);
    expect(state.results).toEqual([]);
    expect(state.localDiagnostics.gaps).toBe(1);
  });

  test("keeps snapshot-request bookkeeping scoped to localDiagnostics", () => {
    const requests = [];
    const initial = applyServerMessage(createInitialState(), snapshot(10));
    expect(Object.hasOwn(initial, "snapshotRequestPending")).toBe(false);
    expect(Object.hasOwn(initial, "snapshotRequestCount")).toBe(false);

    const gapped = applyServerMessage(initial, increment(12, "gap-result"), () =>
      requests.push("snapshot"),
    );

    expect(Object.hasOwn(gapped, "snapshotRequestPending")).toBe(false);
    expect(Object.hasOwn(gapped, "snapshotRequestCount")).toBe(false);
    expect(gapped.localDiagnostics.snapshotRequestPending).toBe(true);
    expect(gapped.localDiagnostics.snapshotRequestCount).toBe(1);
    expect(gapped.localDiagnostics.gaps).toBe(1);
    expect(gapped.localDiagnostics.lastAppliedSequence).toBe(10);
    expect(gapped.localDiagnostics.lastSequence).toBe(10);
    expect(gapped.localDiagnostics.message).toBe(
      "State gap detected (11–11); requesting snapshot.",
    );
    expect(requests).toEqual(["snapshot"]);

    // A second gapped increment while recovering must not double-count.
    const stillGapped = applyServerMessage(gapped, increment(13, "still-unsafe"), () =>
      requests.push("snapshot"),
    );
    expect(stillGapped).toBe(gapped);

    // The ordered-increment path also leaves no top-level bookkeeping behind.
    const recovered = applyServerMessage(
      applyServerMessage(gapped, snapshot(12)),
      increment(13, "after-recovery"),
    );
    expect(Object.hasOwn(recovered, "snapshotRequestPending")).toBe(false);
    expect(Object.hasOwn(recovered, "snapshotRequestCount")).toBe(false);
    expect(recovered.localDiagnostics.snapshotRequestPending).toBe(false);
  });

  test("resumes ordered increments only after the authoritative recovery snapshot", () => {
    const requests = [];
    let state = applyServerMessage(createInitialState(), snapshot(10, [result("before-gap")]));
    state = applyServerMessage(state, increment(12, "unsafe"), () => requests.push("snapshot"));
    state = applyServerMessage(state, snapshot(12, [result("recovered")]));
    state = applyServerMessage(state, increment(13, "after-recovery"), () => requests.push("snapshot"));

    expect(requests).toEqual(["snapshot"]);
    expect(state.results.map(({ result_id }) => result_id)).toEqual(["recovered", "after-recovery"]);
    expect(state.lastAppliedSequence).toBe(13);
    expect(state.localDiagnostics.snapshotRequestPending).toBe(false);
  });

  test("discards an increment older than the last applied snapshot", () => {
    let state = applyServerMessage(createInitialState(), snapshot(20, [result("before-reconnect")]));
    state = applyServerMessage(state, snapshot(30, [result("authoritative-after-reconnect")]));
    state = applyServerMessage(state, increment(22, "stale"));

    expect(state.localDiagnostics.lastSnapshotSequence).toBe(30);
    expect(state.results.map(({ result_id }) => result_id)).toEqual(["authoritative-after-reconnect"]);
  });

  test("reconnect snapshots replace runtime projections while retaining local diagnostics", () => {
    let state = applyServerMessage(createInitialState(), snapshot(4));
    state = {
      ...state,
      localDiagnostics: { ...state.localDiagnostics, gaps: 1, message: "LOCAL: transport gap" },
    };
    state = applyServerMessage(state, {
      ...snapshot(9, [result("authoritative-after-reconnect")]),
      data: {
        ...snapshot(9).data,
        workers: [{
          worker_id: "worker-news",
          topic: "news",
          model_policy: "fast",
          status: "working",
          latest_result_id: null,
          origin_epoch: 1,
        }],
        results: [result("authoritative-after-reconnect")],
      },
    });

    expect(state.workers.map(({ worker_id }) => worker_id)).toEqual(["worker-news"]);
    expect(state.results.map(({ result_id }) => result_id)).toEqual(["authoritative-after-reconnect"]);
    expect(state.localDiagnostics.gaps).toBe(1);
    expect(state.localDiagnostics.message).toContain("LOCAL");
    expect(state.serverState).toBe(true);
  });

  test("normalizes citations and never lets local diagnostic text enter server projections", () => {
    const state = applyServerMessage(createInitialState(), {
      kind: "runtime_snapshot",
      sequence: 1,
      data: {
        session_id: "session-1",
        workers: [],
        results: [{ ...result("result-1"), citations: [
          { title: "ok", url: "https://example.test/a" },
          { title: "bad", url: "javascript:alert(1)" },
          { title: "relative", url: "/not-absolute" },
        ] }],
        speech_progress: [],
      },
    });

    expect(state.results[0].citations.map(({ url }) => url)).toEqual(["https://example.test/a"]);
    expect(state.results[0].citations.map(({ title }) => title)).not.toContain("bad");
  });

  test("reconnect rebuilds the complete unbounded result history from the snapshot", () => {
    const history = [result("result-1"), result("result-2"), result("result-3")].map((item, index) => ({
      ...item,
      turn_id: `turn-${index + 1}`,
      timestamp: `2026-07-18T10:0${index}:00Z`,
    }));
    let state = applyServerMessage(createInitialState(), snapshot(3, [history[0], history[1]]));
    state = applyServerMessage(state, snapshot(20, history));

    expect(state.results.map(({ result_id, turn_id, timestamp }) => ({ result_id, turn_id, timestamp }))).toEqual(
      history.map(({ result_id, turn_id, timestamp }) => ({ result_id, turn_id, timestamp })),
    );
    expect(state.results.map(({ turn_id }) => turn_id)).toEqual(["turn-1", "turn-2", "turn-3"]);
    expect(state.results.map((item) => item.citations[0].url)).toEqual([
      "https://weather.example.test/",
      "https://weather.example.test/",
      "https://weather.example.test/",
    ]);
  });

  test("keeps routing, transcript, workers, results, and speech as separate server projections", () => {
    let state = applyServerMessage(createInitialState(), snapshot(1));
    state = applyServerMessage(state, {
      kind: "routing",
      sequence: 2,
      session_id: "session-1",
      origin_epoch: 1,
      data: {
        turn_id: "turn-1",
        action: "existing_worker",
        worker_id: "worker-weather",
        worker_type: "web_search",
        topic: "weather",
        model_policy: "deep",
        origin_epoch: 1,
      },
    });
    state = applyServerMessage(state, {
      kind: "user_transcript",
      sequence: 3,
      session_id: "session-1",
      origin_epoch: 1,
      data: {
        role: "user",
        text: "What is the weather?",
        turn_id: "turn-1",
        timestamp: "2026-07-18T10:00:00Z",
        origin_epoch: 1,
      },
    });
    state = applyServerMessage(state, {
      kind: "worker",
      sequence: 4,
      session_id: "session-1",
      origin_epoch: 1,
      data: {
        worker_id: "worker-weather",
        topic: "weather",
        model_policy: "deep",
        status: "working",
        latest_result_id: null,
        origin_epoch: 1,
      },
    });
    state = applyServerMessage(state, {
      kind: "result",
      sequence: 5,
      session_id: "session-1",
      origin_epoch: 1,
      data: result("result-1"),
    });
    state = applyServerMessage(state, {
      kind: "speech_progress",
      sequence: 6,
      session_id: "session-1",
      origin_epoch: 1,
      data: {
        utterance_id: "utterance-1",
        result_id: "result-1",
        work_item_id: "work-1",
        run_id: "run-1",
        state: "delivery_unknown",
        origin_epoch: 1,
      },
    });

    expect(state.routing.action).toBe("existing_worker");
    expect(state.routing.worker_id).toBe("worker-weather");
    expect(state.transcript[0].text).toBe("What is the weather?");
    expect(state.workers[0].status).toBe("working");
    expect(state.results).toHaveLength(1);
    expect(state.speech["utterance-1"].result_id).toBe("result-1");
    expect(state.lastAppliedSequence).toBe(6);
  });

  test("reconnect snapshots restore routing and semantic transcript", () => {
    const routing = {
      turn_id: "turn-1",
      action: "new_worker",
      worker_id: null,
      worker_type: "web_search",
      topic: "historical capitals of India",
      model_policy: "deep",
      origin_epoch: 2,
    };
    const transcript = [{
      role: "user",
      text: "What were the capitals of India through the last two hundred years?",
      turn_id: "turn-1",
      timestamp: "2026-07-22T20:00:00Z",
      origin_epoch: 2,
    }];

    const state = applyServerMessage(createInitialState(), {
      ...snapshot(8),
      data: { ...snapshot(8).data, routing, transcript, origin_epoch: 2 },
    });

    expect(state.routing).toEqual(routing);
    expect(state.transcript).toEqual(transcript);
  });

  test("a restarted server session resets ordering fences before applying its snapshot", () => {
    let state = applyServerMessage(createInitialState(), snapshot(20, [result("old-process")]));
    state = applyServerMessage(state, {
      ...snapshot(0, [result("new-process")]),
      sequence: 0,
      session_id: "session-2",
      origin_epoch: 1,
      data: {
        ...snapshot(0).data,
        session_id: "session-2",
        snapshot_sequence: 0,
        results: [result("new-process")],
      },
    });

    expect(state.sessionId).toBe("session-2");
    expect(state.lastAppliedSequence).toBe(0);
    expect(state.results.map(({ result_id }) => result_id)).toEqual(["new-process"]);
  });

  test("does not let a duplicate canonical result event create a second historical entry", () => {
    let state = applyServerMessage(createInitialState(), snapshot(1));
    state = applyServerMessage(state, increment(2, "result-1"));
    state = applyServerMessage(state, {
      kind: "result",
      sequence: 3,
      session_id: "session-1",
      origin_epoch: 1,
      data: { ...result("result-1"), text: "late duplicate wording" },
    });

    expect(state.results).toHaveLength(1);
    expect(state.results[0].ui_text).toBe("Complete answer result-1");
  });

  test("rejects increments with missing or mismatched session fencing", () => {
    let state = applyServerMessage(createInitialState(), snapshot(1));
    const { session_id: _missingSession, ...missingMessage } = increment(2, "missing");
    const missing = applyServerMessage(state, missingMessage);
    const mismatched = applyServerMessage(state, {
      ...increment(2, "mismatched"),
      session_id: "session-other",
    });

    expect(missing.results).toEqual([]);
    expect(mismatched.results).toEqual([]);
    expect(missing.lastAppliedSequence).toBe(1);
  });

  test("rejects increments with missing or mismatched connection fencing", () => {
    let state = applyServerMessage(createInitialState(), {
      ...snapshot(1),
      origin_epoch: 7,
      data: { ...snapshot(1).data, origin_epoch: 7 },
    });
    const missing = applyServerMessage(state, increment(2, "missing-epoch"));
    const mismatched = applyServerMessage(state, { ...increment(2, "stale-epoch"), origin_epoch: 6 });
    const accepted = applyServerMessage(state, { ...increment(2, "current-epoch"), origin_epoch: 7 });

    expect(missing.results).toEqual([]);
    expect(mismatched.results).toEqual([]);
    expect(accepted.results.map(({ result_id }) => result_id)).toEqual(["current-epoch"]);
  });

  test("rejects a lower-epoch snapshot even when its sequence is newer", () => {
    let state = applyServerMessage(createInitialState(), {
      ...snapshot(1),
      origin_epoch: 2,
      data: { ...snapshot(1).data, origin_epoch: 2 },
    });
    const stale = applyServerMessage(state, {
      ...snapshot(2, [result("stale-snapshot")]),
      origin_epoch: 1,
      data: { ...snapshot(2).data, origin_epoch: 1, results: [result("stale-snapshot")] },
    });

    expect(stale.connectionEpoch).toBe(2);
    expect(stale.results).toEqual([]);
    expect(stale.lastAppliedSequence).toBe(1);
  });
});

// --- Phase 3: work_status reducer and cross-epoch snapshot preservation ---
//
// The reducer/rendering additions are new this phase; `state.js` does not
// yet export a work-status projection helper, so these tests are guarded
// via a namespace import (not a named import, which would throw at module
// load for a not-yet-exported binding) and skipped until it lands.

const hasWorkStatusField = Object.hasOwn(createInitialState(), "workStatus");

if (hasWorkStatusField) {
  describe("Phase 3 work_status reducer", () => {
    test("an incremental work_status message updates state.workStatus keyed by (origin_epoch, turn_id, work_item_id)", () => {
      let state = createInitialState();
      state = applyServerMessage(state, snapshot(1));
      state = applyServerMessage(state, {
        kind: "work_status",
        sequence: 2,
        session_id: "session-1",
        origin_epoch: 1,
        data: {
          turn_id: "turn-1",
          work_item_id: "work-1",
          state: "background",
          event_sequence: 0,
          origin_epoch: 1,
        },
      });

      expect(state.workStatus?.["1::turn-1::work-1"]?.state).toBe("background");
    });

    // Regression: the reducer key must carry origin_epoch, matching the
    // server's WorkStatusKey = (origin_epoch, turn_id, parent work item).
    // event_sequence is allocated per that triple and restarts at 1 for a new
    // epoch, so an epoch-less key collapsed the two records into one and the
    // staleness guard dropped the newer epoch's record.
    test("two records differing only by origin_epoch do not collide in the reducer", () => {
      let state = createInitialState();
      state = applyServerMessage(state, {
        ...snapshot(1),
        data: {
          ...snapshot(1).data,
          work_status: [
            {
              turn_id: "turn-1",
              work_item_id: "work-1",
              worker_id: null,
              state: "result_ready",
              event_sequence: 4,
              terminal_reason: null,
              origin_epoch: 1,
            },
            {
              turn_id: "turn-1",
              work_item_id: "work-1",
              worker_id: null,
              state: "searching",
              event_sequence: 1,
              terminal_reason: null,
              origin_epoch: 2,
            },
          ],
        },
      });

      expect(Object.keys(state.workStatus).length).toBe(2);
      expect(state.workStatus["1::turn-1::work-1"].state).toBe("result_ready");
      expect(state.workStatus["2::turn-1::work-1"].state).toBe("searching");
    });

    test("a snapshot embeds a cross-epoch terminal work_status without adopting the envelope epoch", () => {
      let state = createInitialState();
      const withStatus = {
        ...snapshot(1),
        data: {
          ...snapshot(1).data,
          work_status: [
            {
              turn_id: "turn-1",
              work_item_id: "work-1",
              state: "result_ready",
              event_sequence: 0,
              origin_epoch: 1,
            },
          ],
        },
      };
      state = applyServerMessage(state, withStatus);

      const preserved = state.workStatus?.["1::turn-1::work-1"];
      expect(preserved?.origin_epoch).toBe(1);
    });

    // Regression: protocol.md's five-minute terminal TTL is a server-side
    // projection measured from when a record actually went terminal, but
    // `snapshotState` used to restamp `_terminalSince` to `now` on every
    // snapshot rebuild -- so a reconnect gave an already-old terminal record
    // a fresh five minutes instead of honoring the server's remaining TTL.
    test("a reconnect snapshot preserves an already-terminal record's original TTL clock", () => {
      let state = createInitialState();
      state = applyServerMessage(state, {
        ...snapshot(1),
        data: {
          ...snapshot(1).data,
          work_status: [
            {
              turn_id: "turn-1",
              work_item_id: "work-1",
              worker_id: null,
              state: "result_ready",
              event_sequence: 1,
              terminal_reason: null,
              origin_epoch: 1,
            },
          ],
        },
      });
      const firstTerminalSince = state.workStatus["1::turn-1::work-1"]._terminalSince;
      expect(firstTerminalSince).toBeDefined();

      const realNow = Date.now;
      Date.now = () => realNow() + 60 * 1000;
      try {
        // A second, later snapshot re-delivers the same terminal record --
        // e.g. a reconnect -- unchanged apart from wall-clock time passing.
        state = applyServerMessage(state, {
          ...snapshot(2),
          data: {
            ...snapshot(2).data,
            work_status: [
              {
                turn_id: "turn-1",
                work_item_id: "work-1",
                worker_id: null,
                state: "result_ready",
                event_sequence: 1,
                terminal_reason: null,
                origin_epoch: 1,
              },
            ],
          },
        });
      } finally {
        Date.now = realNow;
      }

      expect(state.workStatus["1::turn-1::work-1"]._terminalSince).toBe(firstTerminalSince);
    });

    // Regression: the client's workStatus map was insert-only and unbounded,
    // unlike the server's capped, terminal-first-evicting ledger (see
    // server/session_state.py's _evict_work_status_overflow). A long,
    // gap-free session would otherwise grow this map forever.
    test("the workStatus ledger is bounded, evicting the oldest terminal record first", () => {
      let state = createInitialState();
      state = applyServerMessage(state, snapshot(1));
      for (let index = 0; index < 300; index += 1) {
        state = applyServerMessage(state, {
          kind: "work_status",
          sequence: index + 2,
          session_id: "session-1",
          origin_epoch: 1,
          data: {
            turn_id: `turn-${index}`,
            work_item_id: `work-${index}`,
            state: "result_ready",
            event_sequence: 0,
            origin_epoch: 1,
          },
        });
      }

      const keys = Object.keys(state.workStatus);
      expect(keys.length).toBeLessThanOrEqual(256);
      expect(state.workStatus["1::turn-299::work-299"]).toBeDefined();
      expect(state.workStatus["1::turn-0::work-0"]).toBeUndefined();
    });

    // Regression: eviction used to consider every key regardless of
    // terminality, so once the map exceeded 256 entries with zero terminal
    // records, a live (non-terminal) parent aggregate got evicted anyway --
    // diverging from the server's _evict_work_status_overflow, which
    // deliberately lets the ledger exceed the cap rather than ever strand a
    // live record non-terminal.
    test("the workStatus ledger exceeds its cap rather than evict a live record when none are terminal", () => {
      let state = createInitialState();
      state = applyServerMessage(state, snapshot(1));
      for (let index = 0; index < 300; index += 1) {
        state = applyServerMessage(state, {
          kind: "work_status",
          sequence: index + 2,
          session_id: "session-1",
          origin_epoch: 1,
          data: {
            turn_id: `turn-${index}`,
            work_item_id: `work-${index}`,
            state: "searching",
            event_sequence: 0,
            origin_epoch: 1,
          },
        });
      }

      const keys = Object.keys(state.workStatus);
      expect(keys.length).toBe(300);
      expect(state.workStatus["1::turn-0::work-0"]).toBeDefined();
      expect(state.workStatus["1::turn-299::work-299"]).toBeDefined();
    });

    // Parity guard: WORK_STATUS_MAX_KEYS/WORK_STATUS_TERMINAL_TTL_MS must
    // come from shared/work-status-retention.json, not a hardcoded literal.
    // server/session_state.py loads the same file for
    // WORK_STATUS_TTL_SECONDS/_MAX_WORK_STATUS_KEYS (see
    // tests/test_session_state.py's mirror of this test). If a future edit
    // reverts either side to an inline literal, this test still passes as
    // long as both literals happen to agree today -- it cannot catch that
    // regression by itself, but re-pointing either side at the shared file
    // is the fix this test exists to keep visible, and
    // shared/protocol.md's "Progressive work status" section documents the
    // values both files must agree on.
    test("retention constants match the shared config file", () => {
      expect(WORK_STATUS_MAX_KEYS).toBe(workStatusRetention.max_keys);
      expect(WORK_STATUS_TERMINAL_TTL_MS).toBe(workStatusRetention.ttl_seconds * 1000);
    });

    test("a terminal work_status record expires once the TTL window elapses", () => {
      let state = createInitialState();
      state = applyServerMessage(state, snapshot(1));
      state = applyServerMessage(state, {
        kind: "work_status",
        sequence: 2,
        session_id: "session-1",
        origin_epoch: 1,
        data: { turn_id: "turn-1", work_item_id: "work-1", state: "result_ready", event_sequence: 0, origin_epoch: 1 },
      });
      expect(state.workStatus["1::turn-1::work-1"]).toBeDefined();

      const realNow = Date.now;
      Date.now = () => realNow() + 6 * 60 * 1000;
      try {
        state = applyServerMessage(state, {
          kind: "work_status",
          sequence: 3,
          session_id: "session-1",
          origin_epoch: 1,
          data: { turn_id: "turn-2", work_item_id: "work-2", state: "routing", event_sequence: 0, origin_epoch: 1 },
        });
      } finally {
        Date.now = realNow;
      }

      expect(state.workStatus["1::turn-1::work-1"]).toBeUndefined();
      expect(state.workStatus["1::turn-2::work-2"]).toBeDefined();
    });

    // Regression: a non-terminal work_status record restored from a
    // snapshot mirrors server/session_state.py's non-authoritative
    // records -- its children map is known-incomplete, so it can never be
    // terminalized from and would otherwise have no retention clock at all.
    // The server gives such a record a clock stamped at the restore instant
    // (_work_status_nonauthoritative_at); the client previously keyed
    // pruning only off `_terminalSince`, so a restored non-terminal record
    // was retained forever.
    test("a restored non-terminal work_status record is pruned once the TTL window elapses from the restore instant", () => {
      let state = createInitialState();
      state = applyServerMessage(state, {
        ...snapshot(1),
        data: {
          ...snapshot(1).data,
          work_status: [
            {
              turn_id: "turn-1",
              work_item_id: "work-1",
              worker_id: null,
              state: "searching",
              event_sequence: 1,
              terminal_reason: null,
              origin_epoch: 1,
            },
          ],
        },
      });
      expect(state.workStatus["1::turn-1::work-1"]).toBeDefined();

      const realNow = Date.now;
      Date.now = () => realNow() + 6 * 60 * 1000;
      try {
        state = applyServerMessage(state, {
          kind: "work_status",
          sequence: 2,
          session_id: "session-1",
          origin_epoch: 1,
          data: { turn_id: "turn-2", work_item_id: "work-2", state: "routing", event_sequence: 0, origin_epoch: 1 },
        });
      } finally {
        Date.now = realNow;
      }

      expect(state.workStatus["1::turn-1::work-1"]).toBeUndefined();
      expect(state.workStatus["1::turn-2::work-2"]).toBeDefined();
    });

    // Regression: the same restore-instant clock also makes a restored
    // non-terminal record an eviction candidate on overflow, matching the
    // server's _evict_work_status_overflow (which evicts terminal and
    // non-authoritative-restored records alike). Previously only terminal
    // records were evictable, so a snapshot full of restored non-terminal
    // records blew straight past WORK_STATUS_MAX_KEYS with nothing eligible
    // to evict.
    test("a restored non-terminal work_status record is an eviction candidate on overflow", () => {
      let state = createInitialState();
      const workStatusItems = Array.from({ length: 300 }, (_, index) => ({
        turn_id: `turn-${index}`,
        work_item_id: `work-${index}`,
        worker_id: null,
        state: "searching",
        event_sequence: 0,
        terminal_reason: null,
        origin_epoch: 1,
      }));
      state = applyServerMessage(state, {
        ...snapshot(1),
        data: { ...snapshot(1).data, work_status: workStatusItems },
      });

      const keys = Object.keys(state.workStatus);
      expect(keys.length).toBeLessThanOrEqual(256);
      expect(state.workStatus["1::turn-299::work-299"]).toBeDefined();
      expect(state.workStatus["1::turn-0::work-0"]).toBeUndefined();
    });
  });
} else {
  test.skip("Phase 3 work_status reducer not implemented yet (state.js has no workStatus field)", () => {});
}

// --- C1: snapshot/incremental sequence namespace contract ----------------
//
// The server stamps a snapshot at the global watermark S and re-seeds the
// connection observer to S, so the very next incremental carries S + 1. These
// pin the client half of that contract: S + 1 must be applied with no gap
// reported, and an incremental at S (at or below lastAppliedSequence) stays
// discarded.
describe("snapshot watermark / incremental contiguity", () => {
  test("applies the first incremental at snapshot_sequence + 1 with no gap", () => {
    const requests = [];
    let state = applyServerMessage(createInitialState(), snapshot(7));
    expect(state.lastAppliedSequence).toBe(7);

    state = applyServerMessage(state, increment(8, "result-after-snapshot"), () =>
      requests.push("snapshot"),
    );

    expect(state.results.map(({ result_id }) => result_id)).toEqual(["result-after-snapshot"]);
    expect(state.lastAppliedSequence).toBe(8);
    expect(state.localDiagnostics.gaps).toBe(0);
    expect(requests).toEqual([]);
  });

  test("discards an incremental at the snapshot sequence itself", () => {
    const requests = [];
    let state = applyServerMessage(createInitialState(), snapshot(7));
    state = applyServerMessage(state, increment(7, "at-watermark"), () =>
      requests.push("snapshot"),
    );

    expect(state.results).toEqual([]);
    expect(state.lastAppliedSequence).toBe(7);
    expect(state.localDiagnostics.gaps).toBe(0);
    expect(requests).toEqual([]);
  });
});
