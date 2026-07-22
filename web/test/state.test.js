import { describe, expect, test } from "bun:test";

import { applyServerMessage, createInitialState } from "../src/state.js";

const result = (resultId) => ({
  result_id: resultId,
  worker_id: "worker-weather",
  turn_id: "turn-1",
  timestamp: "2026-07-18T10:00:00Z",
  text: `Complete answer ${resultId}`,
  ui_text: `Complete answer ${resultId}`,
  spoken_text: `Spoken answer ${resultId}`,
  citations: [{ title: "Weather service", url: "https://weather.example.test" }],
});

const snapshot = (sequence, results = []) => ({
  kind: "runtime_snapshot",
  sequence,
  session_id: "session-1",
  data: {
    session_id: "session-1",
    snapshot_sequence: sequence,
    workers: [{ worker_id: "worker-weather", topic: "weather", model_policy: "deep", status: "idle" }],
    results,
    speech_progress: [],
    routing: null,
    transcript: [],
  },
});

const increment = (sequence, resultId) => ({
  kind: "result",
  sequence,
  session_id: "session-1",
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
        workers: [{ worker_id: "worker-news", topic: "news", model_policy: "fast", status: "working" }],
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
      data: { action: "existing_worker", worker_id: "worker-weather" },
    });
    state = applyServerMessage(state, {
      kind: "user_transcript",
      sequence: 3,
      session_id: "session-1",
      data: { text: "What is the weather?", turn_id: "turn-1" },
    });
    state = applyServerMessage(state, {
      kind: "worker",
      sequence: 4,
      session_id: "session-1",
      data: { worker: { worker_id: "worker-weather", topic: "weather", status: "working" } },
    });
    state = applyServerMessage(state, {
      kind: "result",
      sequence: 5,
      session_id: "session-1",
      data: { result: result("result-1") },
    });
    state = applyServerMessage(state, {
      kind: "speech",
      sequence: 6,
      session_id: "session-1",
      data: { progress: { utterance_id: "utterance-1", result_id: "result-1", state: "delivery_unknown" } },
    });

    expect(state.routing).toEqual({ action: "existing_worker", worker_id: "worker-weather" });
    expect(state.transcript).toEqual([{ role: "user", text: "What is the weather?", turn_id: "turn-1" }]);
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

  test("does not let a duplicate result event create a second historical entry", () => {
    let state = applyServerMessage(createInitialState(), snapshot(1));
    state = applyServerMessage(state, increment(2, "result-1"));
    state = applyServerMessage(state, {
      kind: "runtime_result",
      sequence: 3,
      session_id: "session-1",
      data: { result: { ...result("result-1"), ui_text: "late duplicate wording" } },
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
