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

test("connection readiness requests a snapshot and failed connect leaves Connect enabled", async () => {
  const documentRef = fakeDocument();
  const root = new Element();
  const snapshots = [];
  let shouldFail = true;
  const app = createApp({ root, documentRef, transportFactory: () => ({}), clientFactory: (_transport, callbacks) => ({
    connect: async () => {
      if (shouldFail) throw new Error("offline");
      callbacks.onConnected();
    },
    sendClientMessage: (message) => snapshots.push(message),
    disconnect: async () => {},
    enableMic: () => {},
  }) });

  const connectButton = root.children[0].children[1];
  await connectButton.onclick();
  expect(connectButton.disabled).toBe(false);

  shouldFail = false;
  await connectButton.onclick();
  expect(connectButton.disabled).toBe(true);
  expect(snapshots).toEqual(["snapshot-request"]);
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
    callbacks.onServerMessage({ contract_version: "v2.0", kind: "result", sequence: 1, data: {} });
    expect(app.getState().lastAppliedSequence).toBe(0);
    callbacks.onServerMessage({ contract_version: "v1.0", kind: "result", sequence: 1, data: {} });
    expect(app.getState().lastAppliedSequence).toBe(1);
  });
});
