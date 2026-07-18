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
