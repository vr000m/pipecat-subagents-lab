import { PipecatClient } from "@pipecat-ai/client-js";
import { SmallWebRTCTransport } from "@pipecat-ai/small-webrtc-transport";
import { validateServerMessage, WORK_STATUS_V1 } from "./protocol.js";
import {
  applyServerMessage,
  createInitialState,
  disconnectedState,
  withConnection,
  withDeviceList,
  withMicEnabled,
  withSelectedDevice,
} from "./state.js";
import { render } from "./render.js";
import { createControls } from "./controls.js";

function defaultLogger(component, message, data) {
  const timestamp = new Date().toISOString().slice(11, 23);
  if (data === undefined) console.info(`[${timestamp}][${component}] ${message}`);
  else console.info(`[${timestamp}][${component}] ${message}`, data);
}

// Sole owner of the actual audio-output routing for the assistant's
// remote audio element. `client.updateSpeaker(...)` (in app.js) is only an
// advisory notification to the RTVI client after a switch here succeeds;
// this adapter is what actually moves audio to the selected device.
export function createAudioSink(audioElement) {
  let confirmedSinkId = null;
  return {
    element: audioElement,
    get supportsSinkId() {
      return typeof audioElement.setSinkId === "function";
    },
    getSinkId: () => confirmedSinkId,
    setSink: async (deviceId) => {
      // Property lookup at call time, never a captured reference: callers
      // (including tests) may replace audioElement.setSinkId after this
      // adapter is constructed.
      if (typeof audioElement.setSinkId !== "function") return false;
      try {
        await audioElement.setSinkId(deviceId);
        confirmedSinkId = deviceId;
        return true;
      } catch {
        return false;
      }
    },
    reset: async () => {
      const hadConfirmedSink = confirmedSinkId !== null;
      confirmedSinkId = null;
      if (hadConfirmedSink && typeof audioElement.setSinkId === "function") {
        try { await audioElement.setSinkId(""); } catch { /* best effort */ }
      }
    },
  };
}

export function createApp({ root, documentRef = globalThis.document, webrtcUrl = "/api/rtc", sessionUrl = "/api/session", fetchImpl = globalThis.fetch, clientFactory, transportFactory, logger = defaultLogger } = {}) {
  root ??= documentRef?.querySelector?.("#app");
  let state = createInitialState();
  let client;
  let connectPromise = null;
  let generation = 0;
  let activeGeneration = 0;
  let connectionActive = false;
  const tornDownClients = new WeakSet();
  const logDiag = logger;

  const audio = documentRef.createElement("audio");
  audio.autoplay = true;
  audio.playsInline = true;
  audio.setAttribute("aria-label", "Assistant audio");
  documentRef.body.append(audio);

  const audioSink = createAudioSink(audio);
  // Capability, probed once at startup; controls.render reads only state.
  state = { ...state, devices: { ...state.devices, speakerSupported: audioSink.supportsSinkId } };

  const content = documentRef.createElement("div");
  const update = (next) => { state = next; render(state, content); controls.render(state); };
  const report = (message) => {
    logDiag("diagnostics", message);
    update({ ...state, localDiagnostics: { ...state.localDiagnostics, message } });
  };

  const controls = createControls({
    documentRef,
    handlers: {
      onToggleConnection: () => (state.connection === "connected" ? disconnect() : connect()),
      onToggleMic: () => toggleMic(),
      onPlayAudio: () => audio.play().catch(() => report("Playback is still blocked; check browser site permissions.")),
      onSelectMic: (deviceId) => selectMic(deviceId),
      onSelectSpeaker: (deviceId) => applySpeaker(deviceId, { notifyClient: true }),
    },
  });

  const callbacksFor = (callbackGeneration) => {
    const current = () => callbackGeneration === activeGeneration;
    const requestSnapshot = () => {
      if (current()) client?.sendClientMessage("snapshot-request");
    };
    const attachTrack = (track, participant) => {
      if (!current() || participant?.local || track?.kind !== "audio") return;
      logDiag("track", "attaching remote audio track", { id: track.id, readyState: track.readyState, muted: track.muted, enabled: track.enabled });
      track.addEventListener("mute", () => logDiag("track", "remote audio track muted (no data arriving)", { id: track.id }));
      track.addEventListener("unmute", () => logDiag("track", "remote audio track unmuted (data arriving)", { id: track.id }));
      audio.srcObject = new MediaStream([track]);
      audio.play()
        .then(() => logDiag("track", "audio.play() resolved"))
        .catch(() => report("Assistant audio is ready, but playback was blocked. Click the page or Play audio to allow it."));
    };
    const detachTrack = (track) => {
      if (!current() || !audio.srcObject) return;
      const tracks = audio.srcObject.getTracks();
      if (!track || tracks.includes(track)) {
        logDiag("track", "detaching remote audio track", { id: track?.id });
        audio.srcObject = null;
      }
    };
    return {
      onConnected: () => { logDiag("connection", "onConnected"); if (current()) update(withConnection(state, "connected")); },
      // PipecatClient.connect() sends the standard RTVI client-ready message
      // before resolving onBotReady. Request the server-authoritative snapshot
      // only after that media/RTVI readiness boundary.
      onBotReady: () => { logDiag("connection", "onBotReady"); requestSnapshot(); },
      onDisconnected: () => { logDiag("connection", "onDisconnected"); cleanupConnection(callbackGeneration); },
      onError: (message) => { if (current()) report(message?.data?.message || "The RTVI connection reported an error."); },
      onServerMessage: (message) => {
        if (!current()) return;
        const candidate = message?.data?.kind ? message.data : message;
        if (!validateServerMessage(candidate)) {
          report("Ignored malformed or unsupported server message.");
          return;
        }
        update(applyServerMessage(state, candidate, requestSnapshot));
      },
      onTrackStarted: attachTrack,
      onTrackStopped: detachTrack,
      onAvailableMicsUpdated: (mics) => {
        if (!current()) return;
        update(withDeviceList(state, "mic", mics, client?.selectedMic?.deviceId));
      },
      onAvailableSpeakersUpdated: (speakers) => {
        if (!current()) return;
        const preferred = audioSink.getSinkId() ?? client?.selectedSpeaker?.deviceId ?? null;
        const next = withDeviceList(state, "speaker", speakers, preferred);
        update(next);
        const resolved = next.devices.selectedSpeakerId;
        if (resolved && resolved !== audioSink.getSinkId()) void applySpeaker(resolved, { notifyClient: false });
        else if (!resolved && audioSink.getSinkId()) void audioSink.reset();
      },
    };
  };

  const teardownClient = async (target) => {
    if (!target || tornDownClients.has(target)) return;
    tornDownClients.add(target);
    try { target.enableMic(false); } catch (error) { logDiag("connection", "enableMic(false) failed during teardown", { error: error?.message || String(error) }); }
    try { await target.disconnect(); } catch (error) { logDiag("connection", "disconnect() failed during teardown", { error: error?.message || String(error) }); }
  };

  const connect = async () => {
    if (connectPromise) return connectPromise;
    logDiag("connection", "connect() starting");
    connectPromise = (async () => {
      // A second connect() while connected tears the old client down
      // instead of orphaning its RTCPeerConnection and mic capture.
      if (connectionActive) await disconnect();
      const callbackGeneration = ++generation;
      activeGeneration = callbackGeneration;
      connectionActive = true;
      let connectionUrl = webrtcUrl;
      if (!transportFactory && fetchImpl) {
        try {
          const response = await fetchImpl(sessionUrl);
          if (!response.ok) throw new Error(`session discovery returned HTTP ${response.status}`);
          const handshake = await response.json();
          const base = documentRef?.baseURI || globalThis.location?.href || "http://localhost/";
          const url = new URL(webrtcUrl, base);
          for (const key of ["contract_version", "session_id", "resume_token", "proposed_epoch", "snapshot_sequence"]) {
            if (handshake[key] !== undefined) url.searchParams.set(key, String(handshake[key]));
          }
          // Canonical capability encoding: one URL-encoded JSON array of
          // strings in a single `capabilities` query parameter, bound
          // immutably to the promoted connection epoch server-side.
          url.searchParams.set("capabilities", JSON.stringify([WORK_STATUS_V1]));
          connectionUrl = url.toString();
        } catch (error) {
          report(`Session discovery failed: ${error?.message || error}`);
          connectionActive = false;
          update(disconnectedState(state));
          return false;
        }
      }
      const transportOptions = { webrtcRequestParams: { endpoint: connectionUrl } };
      const transport = transportFactory
        ? transportFactory(transportOptions)
        : new SmallWebRTCTransport(transportOptions);
      const callbacks = callbacksFor(callbackGeneration);
      const nextClient = clientFactory ? clientFactory(transport, callbacks) : new PipecatClient({ transport, callbacks, enableMic: true });
      client = nextClient;
      try {
        // Initialize devices inside this catch boundary so microphone permission
        // failures are surfaced in the local diagnostics instead of leaving the
        // client connection promise pending.
        if (typeof client.initDevices === "function") await client.initDevices();
        await client.connect();
        if (callbackGeneration !== activeGeneration) {
          // disconnect() landed mid-connect; the WeakSet makes this safe
          // even when disconnect() already tore this same client down.
          await teardownClient(nextClient);
          if (client === nextClient) client = undefined;
          return false;
        }
        logDiag("connection", "connect() succeeded");
        update(withMicEnabled(withConnection(state, "connected"), Boolean(client.isMicEnabled)));
        // Connect is a user gesture, so a previously-created audio element may play here.
        audio.play().catch(() => report("Connected, but browser audio is blocked. Click Play audio to continue."));
        return true;
      } catch (error) {
        report(`Connection failed: ${error?.message || error}`);
        await teardownClient(nextClient);
        if (client === nextClient) client = undefined;
        connectionActive = false;
        update(disconnectedState(state));
        return false;
      }
    })();
    try { return await connectPromise; } finally { connectPromise = null; }
  };

  const cleanupConnection = (callbackGeneration = activeGeneration) => {
    if (!connectionActive || callbackGeneration !== activeGeneration) return;
    activeGeneration = ++generation;
    connectionActive = false;
    try { client?.enableMic(false); }
    catch (error) { logDiag("connection", "enableMic(false) failed during cleanup", { error: error?.message || String(error) }); }
    audio.srcObject = null;
    audioSink.reset().catch(() => {});
    update(disconnectedState(state));
  };

  const disconnect = async () => {
    logDiag("connection", "disconnect() starting");
    cleanupConnection(activeGeneration);
    await teardownClient(client);
  };

  const applySpeaker = async (deviceId, { notifyClient }) => {
    if (!deviceId) return;
    logDiag("speaker", "switching speaker device", { deviceId });
    if (!audioSink.supportsSinkId) return;
    const ok = await audioSink.setSink(deviceId);
    if (ok) {
      if (notifyClient) {
        try { client?.updateSpeaker(deviceId); }
        catch (error) { logDiag("speaker", "client.updateSpeaker threw", { error: error?.message || String(error) }); }
      }
      update(withSelectedDevice(state, "speaker", deviceId));
      logDiag("speaker", "setSinkId succeeded", { deviceId });
    } else {
      report(`Failed to switch speaker: device removed`);
    }
  };

  const selectMic = (deviceId) => {
    if (!deviceId) return;
    logDiag("mic", "switching mic device", { deviceId });
    client?.updateMic(deviceId);
    update(withSelectedDevice(state, "mic", deviceId));
  };

  const toggleMic = () => {
    if (!client) return;
    const enabled = !client.isMicEnabled;
    logDiag("mic", `toggling mic ${enabled ? "on" : "off"}`);
    client.enableMic(enabled);
    const next = withMicEnabled(state, enabled);
    update({ ...next, localDiagnostics: { ...next.localDiagnostics, message: enabled ? "Microphone enabled." : "Microphone disabled." } });
  };

  root.replaceChildren();
  root.append(controls.element, content);
  update(state);
  return { connect, disconnect, toggleMic, getState: () => state, getClient: () => client, audio };
}

if (typeof document !== "undefined") createApp();
