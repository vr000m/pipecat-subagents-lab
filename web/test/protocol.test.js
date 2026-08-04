import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import {
  createClientProtocol,
  RTVI_MESSAGE_KINDS,
  validateServerMessage,
} from "../src/protocol.js";

const appSource = readFileSync(new URL("../src/app.js", import.meta.url), "utf8");
const indexSource = readFileSync(new URL("../index.html", import.meta.url), "utf8");
const packageSource = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
const messageSchema = JSON.parse(
  readFileSync(new URL("../../shared/schemas/rtvi-message.json", import.meta.url), "utf8"),
);
const result = (resultId, originEpoch = 1) => {
  const citations = [{ title: "Source", url: "https://example.com/source" }];
  return {
    result_id: resultId,
    worker_id: "worker-1",
    turn_id: "turn-1",
    timestamp: "2026-07-23T10:00:00Z",
    text: "The complete sourced answer for the browser.",
    citations,
    spoken_text: "Here is the short spoken answer.",
    origin_epoch: originEpoch,
  };
};

test("browser entrypoint requests microphone capture on explicit connect", () => {
  expect(appSource).toContain("enableMic: true");
  expect(appSource).not.toContain("createMicrophoneManager");
  expect(appSource).toContain("if (typeof client.initDevices === \"function\") await client.initDevices();");
  expect(appSource).toContain("client?.enableMic(false)");
  expect(appSource).toContain("await target.disconnect()");
});

test("browser entrypoint owns remote audio and handles track lifecycle", () => {
  expect(appSource).toContain('createElement("audio")');
  expect(appSource).toContain("audio.srcObject = new MediaStream([track])");
  expect(appSource).toContain("audio.srcObject = null");
  expect(appSource).toContain("onTrackStarted: attachTrack");
  expect(appSource).toContain("onTrackStopped: detachTrack");
  expect(appSource).toContain("playback was blocked");
});

test("entrypoint requests a snapshot only after Pipecat reports bot readiness", () => {
  expect(appSource).toContain('onConnected: () => { logDiag("connection", "onConnected"); if (current()) update');
  expect(appSource).toContain('onBotReady: () => { logDiag("connection", "onBotReady"); requestSnapshot(); }');
  expect(appSource).toContain('state.connection === "connected" ? disconnect() : connect()');
});

test("browser transcript is projected only from server-authored runtime messages", () => {
  expect(appSource).not.toContain("onUserTranscript");
  expect(appSource).not.toContain("onBotTranscript");
});

test("page has no credential-bearing or server-diagnostic browser contract", () => {
  expect(indexSource).toContain('src="./dist/app.js"');
  expect(indexSource).not.toMatch(/api[_-]?key|secret|prompt|raw[_-]?logs/i);
});

test("browser entrypoint has a Bun build that bundles bare package imports", () => {
  expect(packageSource.scripts.build).toBe("bun build --target browser --outfile dist/app.js src/app.js");
});

test("browser message kinds match the shared v1 envelope schema", () => {
  expect(RTVI_MESSAGE_KINDS).toEqual(messageSchema.properties.kind.enum);
  expect(RTVI_MESSAGE_KINDS).not.toContain("runtime_result");
  expect(RTVI_MESSAGE_KINDS).not.toContain("speech");
});

test("validates versioned RTVI messages and rejects aliases, wrappers, and private fields", () => {
  const message = {
    contract_version: "v1.0",
    session_id: "session-1",
    kind: "result",
    sequence: 1,
    origin_epoch: 1,
    data: result("r1"),
  };
  expect(validateServerMessage(message)).toBe(true);
  expect(validateServerMessage({ ...message, kind: "runtime_result" })).toBe(false);
  expect(validateServerMessage({ ...message, kind: "future_kind" })).toBe(false);
  expect(validateServerMessage({ ...message, kind: "speech" })).toBe(false);
  expect(validateServerMessage({ ...message, data: { result: result("r1") } })).toBe(false);
  expect(validateServerMessage({ contract_version: "v1.0", kind: "result", sequence: 1, data: { prompt: "private" } })).toBe(false);
  expect(validateServerMessage({ contract_version: "v1.0", session_id: "session-1", kind: "result", sequence: -1, data: {} })).toBe(false);
  expect(validateServerMessage({ contract_version: "v2.0", session_id: "session-1", kind: "result", sequence: 99, data: {} })).toBe(false);
  expect(validateServerMessage({ contract_version: "v1.0", kind: "result", sequence: 1, data: {} })).toBe(false);
});

test("requires complete runtime snapshots and preserves state on malformed snapshots", async () => {
  const states = [];
  const protocol = createClientProtocol({ requestSnapshot: () => {}, onState: (state) => states.push(state) });

  await protocol.receive({ kind: "client-ready" });
  const validSnapshot = {
    contract_version: "v1.0",
    kind: "runtime_snapshot",
    sequence: 1,
    session_id: "session-1",
    origin_epoch: 1,
    data: { contract_version: "v1.0", session_id: "session-1", snapshot_sequence: 1, workers: [], results: [], speech_progress: [], routing: null, transcript: [], origin_epoch: 1 },
  };
  await protocol.receive(validSnapshot);
  const beforeMalformed = protocol.getState();

  expect(validateServerMessage({ ...validSnapshot, data: {} })).toBe(false);
  expect(validateServerMessage({ ...validSnapshot, data: { ...validSnapshot.data, results: {} } })).toBe(false);
  expect(validateServerMessage({
    ...validSnapshot,
    data: { ...validSnapshot.data, unexpected_private_field: true },
  })).toBe(false);
  expect(validateServerMessage({
    ...validSnapshot,
    data: {
      ...validSnapshot.data,
      workers: [{
        worker_id: "worker-1",
        topic: "weather",
        model_policy: "deep",
        status: "idle",
        latest_result_id: null,
        origin_epoch: 1,
        unexpected_private_field: true,
      }],
    },
  })).toBe(false);
  expect(validateServerMessage({ ...validSnapshot, session_id: "other" })).toBe(false);
  await protocol.receive({ ...validSnapshot, sequence: 2, data: {} });

  expect(protocol.getState()).toBe(beforeMalformed);
  expect(protocol.getState().lastAppliedSequence).toBe(1);
  expect(states).toHaveLength(1);
});

test("accepts a complete runtime snapshot with an envelope session id", () => {
  expect(validateServerMessage({
    contract_version: "v1.0",
    kind: "runtime_snapshot",
    sequence: 3,
    session_id: "session-1",
    origin_epoch: 1,
    data: { contract_version: "v1.0", session_id: "session-1", snapshot_sequence: 3, workers: [], results: [], speech_progress: [], routing: null, transcript: [], origin_epoch: 1 },
})).toBe(true);
});

test("accepts a full display result with a separate concise spoken projection", () => {
  const payload = result("result-1");
  const message = {
    contract_version: "v1.0",
    kind: "runtime_snapshot",
    sequence: 4,
    session_id: "session-1",
    origin_epoch: 1,
    data: {
      contract_version: "v1.0",
      session_id: "session-1",
      snapshot_sequence: 4,
      workers: [],
      results: [payload],
      speech_progress: [],
      routing: null,
      transcript: [],
      origin_epoch: 1,
    },
  };

  expect(validateServerMessage(message)).toBe(true);
  expect(validateServerMessage({
    ...message,
    data: { ...message.data, results: [{ ...payload, spoken_text: "" }] },
  })).toBe(false);
  expect(validateServerMessage({
    ...message,
    data: { ...message.data, results: [{ ...payload, ui_text: "Redundant UI copy." }] },
  })).toBe(false);
});

for (const field of ["ui_text", "spoken_result_id", "ui_result_id", "spoken_citations", "ui_citations"]) {
  test(`rejects redundant grounded-result field ${field}`, () => {
    const payload = result("result-1");
    payload[field] = field.endsWith("_citations") ? [] : `redundant-${field}`;
    const directMessage = {
      contract_version: "v1.0",
      session_id: "session-1",
      kind: "result",
      sequence: 4,
      origin_epoch: 1,
      data: payload,
    };
    const snapshotMessage = {
      ...directMessage,
      kind: "runtime_snapshot",
      data: {
        contract_version: "v1.0",
        session_id: "session-1",
        snapshot_sequence: 4,
        workers: [],
        results: [payload],
        speech_progress: [],
        routing: null,
        transcript: [],
        origin_epoch: 1,
      },
    };

    expect(validateServerMessage(directMessage)).toBe(false);
    expect(validateServerMessage(snapshotMessage)).toBe(false);
  });
}

for (const [field, value] of [
  ["result_id", ""],
  ["worker_id", ""],
  ["turn_id", ""],
  ["timestamp", "not-a-date"],
  ["timestamp", "2026-07-25T12:00:00"],
  ["timestamp", "2026-07-25 12:00:00+00:00"],
  ["timestamp", "2026-02-30T12:00:00Z"],
  ["origin_epoch", -1],
]) {
  test(`rejects invalid grounded-result ${field}`, () => {
    const message = {
      contract_version: "v1.0",
      session_id: "session-1",
      kind: "result",
      sequence: 4,
      origin_epoch: 1,
      data: { ...result("result-1"), [field]: value },
    };
    expect(validateServerMessage(message)).toBe(false);
    expect(validateServerMessage({
      ...message,
      kind: "runtime_snapshot",
      data: {
        contract_version: "v1.0",
        session_id: "session-1",
        snapshot_sequence: 4,
        workers: [],
        results: [message.data],
        speech_progress: [],
        routing: null,
        transcript: [],
        origin_epoch: 1,
      },
    })).toBe(false);
  });
}

test("validates server-authored routing and semantic transcript messages", () => {
  const routing = {
    contract_version: "v1.0",
    session_id: "session-1",
    kind: "routing",
    sequence: 2,
    origin_epoch: 1,
    data: {
      turn_id: "turn-1",
      action: "new_worker",
      worker_id: null,
      worker_type: "web_search",
      topic: "historical capitals of India",
      model_policy: "deep",
      origin_epoch: 1,
    },
  };
  const transcript = {
    ...routing,
    kind: "user_transcript",
    sequence: 3,
    data: {
      role: "user",
      text: "What were the capitals of India through the last two hundred years?",
      turn_id: "turn-1",
      timestamp: "2026-07-22T20:00:00Z",
      origin_epoch: 1,
    },
  };

  expect(validateServerMessage(routing)).toBe(true);
  expect(validateServerMessage(transcript)).toBe(true);
  expect(validateServerMessage({ ...transcript, data: { ...transcript.data, role: "assistant" } })).toBe(false);
  for (const timestamp of [
    "not-a-date",
    "2026-07-25T12:00:00",
    "2026-07-25 12:00:00+00:00",
    "2026-02-30T12:00:00Z",
  ]) {
    expect(validateServerMessage({
      ...transcript,
      data: { ...transcript.data, timestamp },
    })).toBe(false);
  }
});

test("rejects negative epochs throughout nested runtime snapshots", () => {
  const snapshot = {
    contract_version: "v1.0",
    kind: "runtime_snapshot",
    sequence: 4,
    session_id: "session-1",
    origin_epoch: 1,
    data: {
      contract_version: "v1.0",
      session_id: "session-1",
      snapshot_sequence: 4,
      workers: [],
      results: [],
      speech_progress: [],
      routing: null,
      transcript: [],
      origin_epoch: 1,
    },
  };
  const nestedValues = {
    workers: [{
      worker_id: "worker-1",
      topic: "weather",
      model_policy: "fast",
      status: "idle",
      latest_result_id: null,
      origin_epoch: -1,
    }],
    speech_progress: [{
      result_id: "result-1",
      work_item_id: "work-1",
      run_id: "run-1",
      utterance_id: "utterance-1",
      state: "queued",
      origin_epoch: -1,
    }],
    routing: {
      turn_id: "turn-1",
      action: "direct",
      worker_id: null,
      worker_type: null,
      topic: null,
      model_policy: null,
      origin_epoch: -1,
    },
    transcript: [{
      role: "user",
      text: "Hello",
      turn_id: "turn-1",
      timestamp: "2026-07-25T12:00:00Z",
      origin_epoch: -1,
    }],
  };

  for (const [field, value] of Object.entries(nestedValues)) {
    expect(validateServerMessage({
      ...snapshot,
      data: { ...snapshot.data, [field]: value },
    })).toBe(false);
  }
});

test("validates every canonical increment payload and matching epoch fence", () => {
  const base = {
    contract_version: "v1.0",
    session_id: "session-1",
    sequence: 2,
    origin_epoch: 1,
  };
  const messages = [
    { ...base, kind: "result", data: result("result-1") },
    {
      ...base,
      kind: "speech_progress",
      data: {
        result_id: "result-1",
        work_item_id: "work-1",
        run_id: "run-1",
        utterance_id: "utterance-1",
        state: "started",
        origin_epoch: 1,
      },
    },
    {
      ...base,
      kind: "worker",
      data: {
        worker_id: "worker-1",
        topic: "weather",
        model_policy: "deep",
        status: "idle",
        latest_result_id: null,
        origin_epoch: 1,
      },
    },
    {
      ...base,
      kind: "bot_transcript",
      data: {
        role: "assistant",
        text: "Answer",
        turn_id: "turn-1",
        timestamp: "2026-07-23T10:00:00Z",
        origin_epoch: 1,
      },
    },
  ];

  expect(messages.every(validateServerMessage)).toBe(true);
  expect(validateServerMessage({ ...messages[0], origin_epoch: 2 })).toBe(false);
  expect(validateServerMessage({ ...messages[1], data: { ...messages[1].data, extra: true } })).toBe(false);
  expect(validateServerMessage({ ...messages[2], data: { ...messages[2].data, status: null } })).toBe(false);
});

test("rejects an unsupported contract before it advances sequence state", async () => {
  const protocol = createClientProtocol({ requestSnapshot: () => {} });

  await protocol.receive({ kind: "client-ready" });
  await protocol.receive({ contract_version: "v2.0", session_id: "session-1", kind: "result", sequence: 99, data: {} });

  expect(protocol.getState().lastAppliedSequence).toBe(0);
  await protocol.receive({
    contract_version: "v1.0",
    session_id: "session-1",
    kind: "runtime_snapshot",
    sequence: 1,
    origin_epoch: 1,
    data: { contract_version: "v1.0", session_id: "session-1", snapshot_sequence: 1, workers: [], results: [], speech_progress: [], routing: null, transcript: [], origin_epoch: 1 },
  });
  await protocol.receive({ contract_version: "v1.0", session_id: "session-1", kind: "result", sequence: 2, origin_epoch: 1, data: result("accepted") });
  expect(protocol.getState().lastAppliedSequence).toBe(2);
});

test("gates the first snapshot request on client readiness and ignores stale increments", async () => {
  const snapshots = [];
  const states = [];
  const protocol = createClientProtocol({
    requestSnapshot: () => snapshots.push("snapshot"),
    onState: (state) => states.push(state),
  });

  await protocol.receive({ kind: "result", sequence: 1, data: { result_id: "before-ready" } });
  expect(snapshots).toEqual([]);

  await protocol.receive({ kind: "client-ready" });
  expect(snapshots).toEqual(["snapshot"]);
  await protocol.receive({
    contract_version: "v1.0",
    kind: "runtime_snapshot",
    sequence: 10,
    session_id: "session-1",
    origin_epoch: 1,
    data: { contract_version: "v1.0", session_id: "session-1", snapshot_sequence: 10, workers: [], results: [], speech_progress: [], routing: null, transcript: [], origin_epoch: 1 },
  });
  await protocol.receive({ contract_version: "v1.0", session_id: "session-1", kind: "result", sequence: 9, data: { result_id: "stale" } });
  expect(protocol.getState().lastAppliedSequence).toBe(10);
  expect(protocol.getState().results).toEqual([]);
  expect(states.at(-1)).toEqual(protocol.getState());
});

test("keeps microphone capture user-controlled across connect, enable, and disconnect", async () => {
  const calls = [];
  const protocol = createClientProtocol({
    transport: {
      connect: () => calls.push("connect"),
      enableMic: (enabled) => calls.push(`enable:${enabled}`),
      disableMic: () => calls.push("disable"),
      disconnect: () => calls.push("disconnect"),
    },
    requestSnapshot: () => {},
  });

  await protocol.setMicrophoneEnabled(true);
  expect(calls).toEqual([]);
  await protocol.connect();
  expect(calls).toEqual(["connect"]);
  await protocol.setMicrophoneEnabled(true);
  await protocol.setMicrophoneEnabled(false);
  await protocol.disconnect();

  expect(calls).toEqual(["connect", "enable:true", "disable", "disable", "disconnect"]);
});

test("requests a snapshot at each explicit readiness boundary, never during initialization", async () => {
  const snapshots = [];
  const protocol = createClientProtocol({ requestSnapshot: () => snapshots.push("snapshot") });

  await protocol.initialize();
  expect(snapshots).toEqual([]);
  await protocol.receive({ kind: "client-ready" });
  await protocol.receive({ kind: "reconnected" });

  expect(snapshots).toEqual(["snapshot", "snapshot"]);
});

// --- Phase 3: work_status kind -------------------------------------------
//
// shared/schemas/rtvi-message.json extends its closed v1.0 kind enum with
// "work_status"; RTVI_MESSAGE_KINDS must track the schema exactly (existing
// test above already asserts RTVI_MESSAGE_KINDS === schema kind enum, so
// once the schema lands "work_status", this constant must too).

const workStatus = (overrides = {}) => ({
  turn_id: "turn-1",
  work_item_id: "work-1",
  worker_id: null,
  state: "routing",
  event_sequence: 0,
  terminal_reason: null,
  origin_epoch: 1,
  ...overrides,
});

test("rtvi-message.json's closed v1.0 kind enum includes work_status", () => {
  expect(messageSchema.properties.kind.enum).toContain("work_status");
});

test("validateServerMessage accepts a well-formed work_status envelope for each coarse state", () => {
  for (const state of ["routing", "searching", "background", "result_ready", "failed", "cancelled"]) {
    const message = {
      contract_version: "v1.0",
      session_id: "session-1",
      sequence: 1,
      kind: "work_status",
      data: workStatus({ state }),
      origin_epoch: 1,
    };
    expect(validateServerMessage(message)).toBe(true);
  }
});

test("validateServerMessage rejects a work_status payload carrying word-level progress fields", () => {
  const message = {
    contract_version: "v1.0",
    session_id: "session-1",
    sequence: 1,
    kind: "work_status",
    data: workStatus({ word_progress: "thinking about the weather" }),
    origin_epoch: 1,
  };
  expect(validateServerMessage(message)).toBe(false);
});

test("validateServerMessage rejects work_status using the WorkItemEvent-reserved started/progress states", () => {
  for (const reserved of ["started", "progress"]) {
    const message = {
      contract_version: "v1.0",
      session_id: "session-1",
      sequence: 1,
      kind: "work_status",
      data: workStatus({ state: reserved }),
      origin_epoch: 1,
    };
    expect(validateServerMessage(message)).toBe(false);
  }
});
