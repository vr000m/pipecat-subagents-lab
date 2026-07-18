import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { createClientProtocol, validateServerMessage } from "../src/protocol.js";

const appSource = readFileSync(new URL("../src/app.js", import.meta.url), "utf8");
const indexSource = readFileSync(new URL("../index.html", import.meta.url), "utf8");
const packageSource = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));

test("browser entrypoint starts with microphone capture disabled", () => {
  expect(appSource).toContain("enableMic: false");
  expect(appSource).toContain("client?.enableMic(false)");
  expect(appSource).toContain("await client?.disconnect()");
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
  expect(appSource).toContain('onConnected: () => update({ ...state, connection: "connected" })');
  expect(appSource).toContain('onBotReady: () => requestSnapshot()');
  expect(appSource).toContain("if (!await connect()) return;");
});

test("page has no credential-bearing or server-diagnostic browser contract", () => {
  expect(indexSource).toContain('src="./dist/app.js"');
  expect(indexSource).not.toMatch(/api[_-]?key|secret|prompt|raw[_-]?logs/i);
});

test("browser entrypoint has a Bun build that bundles bare package imports", () => {
  expect(packageSource.scripts.build).toBe("bun build --target browser --outfile dist/app.js src/app.js");
});

test("validates versioned RTVI messages and rejects private server fields", () => {
  expect(validateServerMessage({ contract_version: "v1.0", kind: "result", sequence: 1, data: { result: { result_id: "r1" } } })).toBe(true);
  expect(validateServerMessage({ contract_version: "v1.0", kind: "result", sequence: 1, data: { prompt: "private" } })).toBe(false);
  expect(validateServerMessage({ contract_version: "v1.0", kind: "result", sequence: -1, data: {} })).toBe(false);
  expect(validateServerMessage({ contract_version: "v2.0", kind: "result", sequence: 99, data: {} })).toBe(false);
});

test("rejects an unsupported contract before it advances sequence state", async () => {
  const protocol = createClientProtocol({ requestSnapshot: () => {} });

  await protocol.receive({ kind: "client-ready" });
  await protocol.receive({ contract_version: "v2.0", kind: "result", sequence: 99, data: {} });

  expect(protocol.getState().lastAppliedSequence).toBe(0);
  await protocol.receive({ contract_version: "v1.0", kind: "result", sequence: 1, data: {} });
  expect(protocol.getState().lastAppliedSequence).toBe(1);
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
    data: { session_id: "session-1", snapshot_sequence: 10, workers: [], results: [], speech_progress: [] },
  });
  await protocol.receive({ contract_version: "v1.0", kind: "result", sequence: 9, data: { result_id: "stale" } });
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
