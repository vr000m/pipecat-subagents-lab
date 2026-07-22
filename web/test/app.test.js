import { expect, test } from "bun:test";
import { createApp } from "../src/app.js";

class Element {
  constructor() { this.children = []; this.innerHTML = ""; this.disabled = false; this.hidden = false; }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; this.innerHTML = ""; }
  setAttribute() {}
  play() { return Promise.resolve(); }
}

function fakeDocument() {
  const body = new Element();
  return { body, createElement: () => new Element(), querySelector: () => new Element() };
}

test("connection readiness sends RTVI ready before requesting a snapshot", async () => {
  const documentRef = fakeDocument();
  const root = new Element();
  const messages = [];
  let transportOptions;
  let shouldFail = true;
  const app = createApp({ root, documentRef, transportFactory: (options) => {
    transportOptions = options;
    return { sendMessage: (message) => messages.push(message) };
  }, clientFactory: (transport, callbacks) => ({
    connect: async () => {
      if (shouldFail) throw new Error("offline");
      // PipecatClient.connect() performs this transport call internally
      // before invoking the documented onBotReady callback.
      transport.sendMessage({ type: "client-ready" });
      callbacks.onConnected();
      callbacks.onBotReady();
    },
    sendClientMessage: (message) => messages.push(message),
    disconnect: async () => {},
    enableMic: () => {},
  }) });

  const connectButton = root.children[0].children[1];
  await connectButton.onclick();
  expect(connectButton.disabled).toBe(false);
  expect(messages).toEqual([]);

  shouldFail = false;
  await connectButton.onclick();
  expect(connectButton.disabled).toBe(true);
  expect(messages).toEqual([{ type: "client-ready" }, "snapshot-request"]);
  expect(transportOptions).toEqual({ webrtcRequestParams: { endpoint: "/api/rtc" } });
});

test("onServerMessage rejects unsupported contracts before applying state", () => {
  const documentRef = fakeDocument();
  const root = new Element();
  let callbacks;
  const app = createApp({
    root,
    documentRef,
    transportFactory: () => ({}),
    clientFactory: (_transport, nextCallbacks) => {
      callbacks = nextCallbacks;
      return { enableMic: () => {}, disconnect: async () => {} };
    },
  });

  callbacks = undefined;
  void app;
  // Creating the client is intentionally deferred until connect.
  return app.connect().then(() => {
    callbacks.onServerMessage({ contract_version: "v2.0", session_id: "session-1", kind: "result", sequence: 1, data: {} });
    expect(app.getState().lastAppliedSequence).toBe(0);
    callbacks.onServerMessage({
      contract_version: "v1.0",
      session_id: "session-1",
      kind: "runtime_snapshot",
      sequence: 1,
      data: { contract_version: "v1.0", session_id: "session-1", snapshot_sequence: 1, workers: [], results: [], speech_progress: [], origin_epoch: 1 },
    });
    callbacks.onServerMessage({ contract_version: "v1.0", session_id: "session-1", kind: "result", sequence: 2, origin_epoch: 1, data: { result_id: "result-1" } });
    expect(app.getState().lastAppliedSequence).toBe(2);
  });
});
