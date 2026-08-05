import { afterAll, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { createApp } from "../src/app.js";
import { WORK_STATUS_V1 } from "../src/protocol.js";

const appEntrySource = readFileSync(new URL("../src/app.js", import.meta.url), "utf8");

class Element {
  constructor() { this.children = []; this.innerHTML = ""; this.disabled = false; this.hidden = false; }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; this.innerHTML = ""; }
  setAttribute() {}
  play() { return Promise.resolve(); }
}

function fakeDocument({ audioSupportsSinkId = false } = {}) {
  const body = new Element();
  return {
    body,
    createElement: (tag) => {
      const node = new Element();
      if (tag === "audio" && audioSupportsSinkId) node.setSinkId = async (id) => { node.sinkId = id; };
      return node;
    },
    querySelector: () => new Element(),
  };
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

  const connectToggleButton = root.children[0].children[4];
  await connectToggleButton.onclick();
  expect(connectToggleButton.children[1].textContent).toBe("Connect");
  expect(messages).toEqual([]);

  shouldFail = false;
  await connectToggleButton.onclick();
  expect(connectToggleButton.children[1].textContent).toBe("Disconnect");
  expect(messages).toEqual([{ type: "client-ready" }, "snapshot-request"]);
  expect(transportOptions).toEqual({ webrtcRequestParams: { endpoint: "/api/rtc" } });
});

test("device selects populate from onAvailableMicsUpdated/onAvailableSpeakersUpdated and route selection through the client and audio sink", async () => {
  const documentRef = fakeDocument({ audioSupportsSinkId: true });
  const root = new Element();
  const mics = [{ deviceId: "mic-1", label: "Built-in Mic" }, { deviceId: "mic-2", label: "USB Mic" }];
  const speakers = [{ deviceId: "spk-1", label: "Speakers" }, { deviceId: "spk-2", label: "Headphones" }];
  const updateMicCalls = [];
  const updateSpeakerCalls = [];
  let callbacks;
  const app = createApp({
    root,
    documentRef,
    transportFactory: () => ({}),
    clientFactory: (_transport, nextCallbacks) => {
      callbacks = nextCallbacks;
      return {
        connect: async () => {},
        disconnect: async () => {},
        enableMic: () => {},
        isMicEnabled: true,
        selectedMic: mics[0],
        selectedSpeaker: speakers[0],
        updateMic: (id) => updateMicCalls.push(id),
        updateSpeaker: (id) => updateSpeakerCalls.push(id),
      };
    },
  });

  const header = root.children[0];
  const connectToggleButton = header.children[4];
  const micSelect = header.children[1];
  const speakerSelect = header.children[2];

  expect(micSelect.disabled).toBe(true);
  await connectToggleButton.onclick();

  // Simulate what client.initDevices()/connect() trigger internally.
  callbacks.onAvailableMicsUpdated(mics);
  callbacks.onAvailableSpeakersUpdated(speakers);

  expect(micSelect.children.map((option) => option.value)).toEqual(["mic-1", "mic-2"]);
  expect(speakerSelect.children.map((option) => option.value)).toEqual(["spk-1", "spk-2"]);
  expect(micSelect.value).toBe("mic-1");
  expect(micSelect.disabled).toBe(false);
  expect(speakerSelect.disabled).toBe(false);

  micSelect.value = "mic-2";
  micSelect.onchange();
  expect(updateMicCalls).toEqual(["mic-2"]);

  speakerSelect.value = "spk-2";
  await speakerSelect.onchange();
  expect(updateSpeakerCalls).toEqual(["spk-2"]);
  expect(app.audio.sinkId).toBe("spk-2");
});

test("failed speaker sink switch restores the last confirmed speaker selection", async () => {
  const documentRef = fakeDocument({ audioSupportsSinkId: true });
  const root = new Element();
  const speakers = [{ deviceId: "spk-1", label: "Speakers" }, { deviceId: "spk-2", label: "Headphones" }];
  let activeSpeaker = "spk-1";
  let callbacks;
  const app = createApp({
    root,
    documentRef,
    transportFactory: () => ({}),
    clientFactory: (_transport, nextCallbacks) => {
      callbacks = nextCallbacks;
      return {
        connect: async () => {},
        disconnect: async () => {},
        enableMic: () => {},
        selectedSpeaker: speakers[0],
        updateSpeaker: (id) => { activeSpeaker = id; },
      };
    },
  });

  const speakerSelect = root.children[0].children[2];
  await app.connect();
  callbacks.onAvailableSpeakersUpdated(speakers);
  app.audio.setSinkId = async () => { throw new Error("device removed"); };

  speakerSelect.value = "spk-2";
  await speakerSelect.onchange();

  expect(speakerSelect.value).toBe("spk-1");
  expect(activeSpeaker).toBe("spk-1");
});

test("speaker select is hidden and never populated when the browser cannot switch audio output devices", async () => {
  const documentRef = fakeDocument({ audioSupportsSinkId: false });
  const root = new Element();
  let callbacks;
  const app = createApp({
    root,
    documentRef,
    transportFactory: () => ({}),
    clientFactory: (_transport, nextCallbacks) => {
      callbacks = nextCallbacks;
      return { connect: async () => {}, disconnect: async () => {}, enableMic: () => {} };
    },
  });

  const speakerSelect = root.children[0].children[2];
  expect(speakerSelect.hidden).toBe(true);

  await app.connect();
  callbacks.onAvailableSpeakersUpdated([{ deviceId: "spk-1", label: "Speakers" }]);
  expect(speakerSelect.children).toEqual([]);
});

test("populateSelect does not force-select a device id that is not in the provided device list", async () => {
  const documentRef = fakeDocument({ audioSupportsSinkId: true });
  const root = new Element();
  const mics = [{ deviceId: "mic-1", label: "Built-in Mic" }, { deviceId: "mic-2", label: "USB Mic" }];
  let callbacks;
  const app = createApp({
    root,
    documentRef,
    transportFactory: () => ({}),
    clientFactory: (_transport, nextCallbacks) => {
      callbacks = nextCallbacks;
      return { connect: async () => {}, disconnect: async () => {}, enableMic: () => {}, selectedMic: { deviceId: "stale-mic" } };
    },
  });
  void app;

  const micSelect = root.children[0].children[1];
  await app.connect();
  callbacks.onAvailableMicsUpdated(mics);

  expect(micSelect.value).not.toBe("stale-mic");
});

test("disconnect() itself disables and clears the mic/speaker selects", async () => {
  const documentRef = fakeDocument({ audioSupportsSinkId: true });
  const root = new Element();
  const mics = [{ deviceId: "mic-1", label: "Built-in Mic" }];
  let callbacks;
  const app = createApp({
    root,
    documentRef,
    transportFactory: () => ({}),
    clientFactory: (_transport, nextCallbacks) => {
      callbacks = nextCallbacks;
      return { connect: async () => {}, disconnect: async () => {}, enableMic: () => {}, selectedMic: mics[0] };
    },
  });

  const micSelect = root.children[0].children[1];
  const speakerSelect = root.children[0].children[2];
  await app.connect();
  callbacks.onAvailableMicsUpdated(mics);
  micSelect.disabled = false;
  speakerSelect.disabled = false;

  await app.disconnect();

  expect(micSelect.disabled).toBe(true);
  expect(speakerSelect.disabled).toBe(true);
  expect(micSelect.children).toEqual([]);
  expect(speakerSelect.children).toEqual([]);
});

test("transport onDisconnected performs full cleanup and invalidates its callback generation", async () => {
  const documentRef = fakeDocument({ audioSupportsSinkId: true });
  const root = new Element();
  const mics = [{ deviceId: "mic-1", label: "Built-in Mic" }];
  const speakers = [{ deviceId: "spk-1", label: "Speakers" }];
  let callbacks;
  const app = createApp({
    root,
    documentRef,
    transportFactory: () => ({}),
    clientFactory: (_transport, nextCallbacks) => {
      callbacks = nextCallbacks;
      return {
        connect: async () => {},
        disconnect: async () => {},
        enableMic: () => {},
        isMicEnabled: true,
        selectedMic: mics[0],
        selectedSpeaker: speakers[0],
      };
    },
  });

  const header = root.children[0];
  const micSelect = header.children[1];
  const speakerSelect = header.children[2];
  const connectToggleButton = header.children[4];
  await connectToggleButton.onclick();
  callbacks.onAvailableMicsUpdated(mics);
  callbacks.onAvailableSpeakersUpdated(speakers);
  app.audio.srcObject = { getTracks: () => [] };

  callbacks.onDisconnected();

  expect(connectToggleButton.children[1].textContent).toBe("Connect");
  expect(micSelect.disabled).toBe(true);
  expect(speakerSelect.disabled).toBe(true);
  expect(micSelect.children).toEqual([]);
  expect(speakerSelect.children).toEqual([]);
  expect(app.audio.srcObject).toBe(null);
  expect(app.getState().connection).toBe("disconnected");

  callbacks.onConnected();
  expect(app.getState().connection).toBe("disconnected");
});

test("once disconnect() has run, the selects are disabled so a real <select> cannot fire onchange again", async () => {
  const documentRef = fakeDocument({ audioSupportsSinkId: true });
  const root = new Element();
  const updateMicCalls = [];
  const app = createApp({
    root,
    documentRef,
    transportFactory: () => ({}),
    clientFactory: () => ({
      connect: async () => {},
      disconnect: async () => {},
      enableMic: () => {},
      updateMic: (id) => updateMicCalls.push(id),
    }),
  });

  const micSelect = root.children[0].children[1];
  await app.connect();
  await app.disconnect();

  expect(micSelect.disabled).toBe(true);
  // A disabled real <select> cannot dispatch a change event; this fake
  // Element doesn't enforce that, so we assert on the disabled state
  // itself rather than invoking onchange() post-disconnect.
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
      origin_epoch: 1,
      data: { contract_version: "v1.0", session_id: "session-1", snapshot_sequence: 1, workers: [], results: [], speech_progress: [], routing: null, transcript: [], origin_epoch: 1 },
    });
    callbacks.onServerMessage({
      contract_version: "v1.0",
      session_id: "session-1",
      kind: "result",
      sequence: 2,
      origin_epoch: 1,
      data: {
        result_id: "result-1",
        worker_id: "worker-1",
        turn_id: "turn-1",
        timestamp: "2026-07-25T00:00:00Z",
        text: "Answer",
        citations: [],
        spoken_text: "Answer",
        origin_epoch: 1,
      },
    });
    expect(app.getState().lastAppliedSequence).toBe(2);
  });
});

// --- Phase 3: capability advertisement on the connection URL --------------
//
// Plan bullet 223 step (4): "advertise work_status_v1 from the browser only
// after the server carrier/storage/observer path is live." The canonical
// encoding (bullet 224) is one URL-encoded JSON array of strings in a
// single `capabilities` query parameter, emitted by URLSearchParams as
// JSON.stringify([...]) -- i.e. url.searchParams.set("capabilities",
// JSON.stringify([...])), matching the existing per-field loop at
// web/src/app.js:179 that already builds this same connection URL.

test("browser entrypoint advertises the work_status_v1 capability on the connection URL", () => {
  // The literal now lives in exactly one place (protocol.js); the entrypoint
  // advertises it by importing that shared constant.
  expect(WORK_STATUS_V1).toBe("work_status_v1");
  expect(appEntrySource).toContain("WORK_STATUS_V1");
  expect(appEntrySource).not.toContain('"work_status_v1"');
  expect(appEntrySource).toMatch(/searchParams\.set\(\s*["']capabilities["']/);
});

// --- Real DOM-behavioral coverage for remote track playback ---------------
//
// Bun's test runtime has no built-in MediaStream, unlike a browser; the
// fakeDocument()/Element fixtures above already substitute for the DOM the
// same way, so this stub follows that existing pattern rather than pulling
// in jsdom.
class FakeMediaStream {
  constructor(tracks) { this.tracks = tracks; }
  getTracks() { return this.tracks; }
}
const previousMediaStream = globalThis.MediaStream;
globalThis.MediaStream ??= FakeMediaStream;
afterAll(() => {
  if (previousMediaStream === undefined) delete globalThis.MediaStream;
  else globalThis.MediaStream = previousMediaStream;
});

function fakeAudioTrack({ id = "track-1", kind = "audio" } = {}) {
  return { id, kind, readyState: "live", muted: false, enabled: true, addEventListener: () => {} };
}

test("a remote audio track delivered via onTrackStarted actually plays through the real audio element", async () => {
  const documentRef = fakeDocument();
  const root = new Element();
  const app = createApp({
    root,
    documentRef,
    transportFactory: () => ({}),
    clientFactory: (_transport, nextCallbacks) => ({
      connect: async () => nextCallbacks.onTrackStarted(fakeAudioTrack(), { local: false }),
      disconnect: async () => {},
      enableMic: () => {},
    }),
  });

  const playCalls = [];
  const realPlay = app.audio.play.bind(app.audio);
  app.audio.play = (...args) => {
    playCalls.push(args);
    return realPlay(...args);
  };

  await app.connect();

  // connect() itself also calls audio.play() once as the user-gesture
  // autoplay unlock (see the comment at web/src/app.js:216); the assertion
  // here is that onTrackStarted's own delivery-triggered play() happened
  // too, i.e. more than the single connect()-only call.
  expect(playCalls.length).toBeGreaterThanOrEqual(2);
  expect(app.audio.srcObject).toBeInstanceOf(FakeMediaStream);
  expect(app.audio.srcObject.getTracks()).toHaveLength(1);
});
