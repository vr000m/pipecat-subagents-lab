import { PipecatClient } from "@pipecat-ai/client-js";
import { SmallWebRTCTransport } from "@pipecat-ai/small-webrtc-transport";
import { validateServerMessage } from "./protocol.js";
import { applyServerMessage, createInitialState } from "./state.js";
import { render } from "./render.js";

const ICONS = {
  connect: `<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M9 2v4"/><path d="M15 2v4"/><path d="M8 8h8l-1 6a3 3 0 0 1-3 3h0a3 3 0 0 1-3-3z"/><path d="M12 17v5"/></svg>`,
  disconnect: `<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M9 2v4"/><path d="M15 2v4"/><path d="M8 8h8l-1 6a3 3 0 0 1-3 3h0a3 3 0 0 1-3-3z"/><path d="M12 17v5"/><path d="M3 3l18 18"/></svg>`,
  micOn: `<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="9" y="2" width="6" height="11" rx="3"/><path d="M5 10a7 7 0 0 0 14 0"/><path d="M12 17v5"/><path d="M8 22h8"/></svg>`,
  micOff: `<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="9" y="2" width="6" height="11" rx="3"/><path d="M5 10a7 7 0 0 0 14 0"/><path d="M12 17v5"/><path d="M8 22h8"/><path d="M3 3l18 18"/></svg>`,
  play: `<svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor" aria-hidden="true"><path d="M6 4l14 8-14 8z"/></svg>`,
};

function iconButton(documentRef, iconSvg, text) {
  const node = documentRef.createElement("button");
  node.innerHTML = `<span class="btn-icon">${iconSvg}</span><span class="btn-text">${text}</span>`;
  return node;
}

function logDiag(component, message, data) {
  const timestamp = new Date().toISOString().slice(11, 23);
  if (data === undefined) console.info(`[${timestamp}][${component}] ${message}`);
  else console.info(`[${timestamp}][${component}] ${message}`, data);
}

export function createApp({ root, documentRef = globalThis.document, webrtcUrl = "/api/rtc", sessionUrl = "/api/session", fetchImpl = globalThis.fetch, clientFactory, transportFactory } = {}) {
  root ??= documentRef?.querySelector?.("#app");
  let state = createInitialState();
  let client;
  let connectPromise = null;
  let generation = 0;
  let activeGeneration = 0;
  let connectionActive = false;
  let lastConfirmedSinkId = null;
  let audio = documentRef.createElement("audio");
  audio.autoplay = true;
  audio.playsInline = true;
  audio.setAttribute("aria-label", "Assistant audio");
  documentRef.body.append(audio);
  const supportsSinkId = typeof audio.setSinkId === "function";

  const content = documentRef.createElement("div");
  const update = (next) => { state = next; render(state, content); };
  const report = (message) => {
    logDiag("diagnostics", message);
    update({ ...state, localDiagnostics: { ...state.localDiagnostics, message } });
  };

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
      onConnected: () => { logDiag("connection", "onConnected"); if (current()) update({ ...state, connection: "connected" }); },
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
      onAvailableMicsUpdated: (mics) => { if (current()) populateSelect(documentRef, micSelect, mics, client?.selectedMic?.deviceId); },
      onAvailableSpeakersUpdated: (speakers) => {
        if (!current() || !supportsSinkId) return;
        lastConfirmedSinkId ??= client?.selectedSpeaker?.deviceId || null;
        populateSelect(documentRef, speakerSelect, speakers, lastConfirmedSinkId);
      },
    };
  };

  const connect = async () => {
    if (connectPromise) return connectPromise;
    logDiag("connection", "connect() starting");
    connectPromise = (async () => {
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
        connectionUrl = url.toString();
      } catch (error) {
        report(`Session discovery failed: ${error?.message || error}`);
        connectionActive = false;
        update({ ...state, connection: "disconnected" });
        return false;
      }
    }
    const transportOptions = { webrtcRequestParams: { endpoint: connectionUrl } };
    const transport = transportFactory
      ? transportFactory(transportOptions)
      : new SmallWebRTCTransport(transportOptions);
    const callbacks = callbacksFor(callbackGeneration);
    client = clientFactory ? clientFactory(transport, callbacks) : new PipecatClient({ transport, callbacks, enableMic: true });
    try {
      // Initialize devices inside this catch boundary so microphone permission
      // failures are surfaced in the local diagnostics instead of leaving the
      // client connection promise pending.
      if (typeof client.initDevices === "function") await client.initDevices();
      await client.connect();
      if (callbackGeneration !== activeGeneration) return false;
      logDiag("connection", "connect() succeeded");
      update({ ...state, connection: "connected" });
      setMicButton(Boolean(client.isMicEnabled));
      // Connect is a user gesture, so a previously-created audio element may play here.
      audio.play().catch(() => report("Connected, but browser audio is blocked. Click Play audio to continue."));
      return true;
    } catch (error) {
      report(`Connection failed: ${error?.message || error}`);
      connectionActive = false;
      update({ ...state, connection: "disconnected" });
      return false;
    }
    })();
    try { return await connectPromise; } finally { connectPromise = null; }
  };
  const cleanupConnection = (callbackGeneration = activeGeneration) => {
    if (!connectionActive || callbackGeneration !== activeGeneration) return;
    activeGeneration = ++generation;
    connectionActive = false;
    isConnected = false;
    micSelect.disabled = true;
    speakerSelect.disabled = true;
    populateSelect(documentRef, micSelect, [], null);
    populateSelect(documentRef, speakerSelect, [], null);
    lastConfirmedSinkId = null;
    client?.enableMic(false);
    audio.srcObject = null;
    setMicButton(false);
    micButton.disabled = true;
    playButton.hidden = true;
    setConnectToggleButton(false);
    update({ ...state, connection: "disconnected" });
  };
  const disconnect = async () => {
    logDiag("connection", "disconnect() starting");
    cleanupConnection(activeGeneration);
    await client?.disconnect();
  };
  const selectMic = (deviceId) => { if (deviceId) { logDiag("mic", "switching mic device", { deviceId }); client?.updateMic(deviceId); } };
  const selectSpeaker = async (deviceId) => {
    if (!deviceId) return;
    logDiag("speaker", "switching speaker device", { deviceId });
    // The client's own speaker routing does not know about the <audio>
    // element this app manages by hand (see attachTrack); the sink must be
    // set explicitly for switching to actually change what plays.
    if (supportsSinkId) {
      try {
        await audio.setSinkId(deviceId);
        lastConfirmedSinkId = deviceId;
        speakerSelect.value = deviceId;
        client?.updateSpeaker(deviceId);
        logDiag("speaker", "setSinkId succeeded", { deviceId });
      } catch (error) {
        speakerSelect.value = lastConfirmedSinkId || "";
        report(`Failed to switch speaker: ${error?.message || error}`);
      }
    }
  };
  const toggleMic = () => {
    if (!client) return;
    const enabled = !client.isMicEnabled;
    logDiag("mic", `toggling mic ${enabled ? "on" : "off"}`);
    client.enableMic(enabled);
    setMicButton(enabled);
    update({ ...state, localDiagnostics: { ...state.localDiagnostics, message: enabled ? "Microphone enabled." : "Microphone disabled." } });
  };

  root.replaceChildren();
  const header = documentRef.createElement("header");
  header.className = "topbar";
  header.append(el(documentRef, "h1", "Pipecat Subagents Lab"));
  const connectToggleButton = iconButton(documentRef, ICONS.connect, "Connect");
  const micButton = iconButton(documentRef, ICONS.micOff, "Mic: off");
  const playButton = iconButton(documentRef, ICONS.play, "Play audio");
  const micSelect = documentRef.createElement("select");
  micSelect.setAttribute("aria-label", "Microphone");
  micSelect.className = "icon-select icon-select-mic";
  const speakerSelect = documentRef.createElement("select");
  speakerSelect.setAttribute("aria-label", "Speaker");
  speakerSelect.className = "icon-select icon-select-speaker";
  const setMicButton = (enabled) => {
    micButton.innerHTML = `<span class="btn-icon">${enabled ? ICONS.micOn : ICONS.micOff}</span><span class="btn-text">Mic: ${enabled ? "on" : "off"}</span>`;
  };
  let isConnected = false;
  const setConnectToggleButton = (connectedState) => {
    connectToggleButton.innerHTML = `<span class="btn-icon">${connectedState ? ICONS.disconnect : ICONS.connect}</span><span class="btn-text">${connectedState ? "Disconnect" : "Connect"}</span>`;
  };
  micButton.disabled = true; playButton.hidden = true;
  micSelect.disabled = true; speakerSelect.disabled = true;
  if (!supportsSinkId) {
    speakerSelect.hidden = true;
    speakerSelect.title = "This browser does not support switching audio output devices.";
  }
  connectToggleButton.onclick = async () => {
    if (isConnected) {
      await disconnect();
      return;
    }
    if (!await connect()) return;
    isConnected = true;
    micButton.disabled = false; playButton.hidden = false;
    micSelect.disabled = false; speakerSelect.disabled = false;
    setConnectToggleButton(true);
  };
  micButton.onclick = toggleMic;
  playButton.onclick = () => audio.play().catch(() => report("Playback is still blocked; check browser site permissions."));
  micSelect.onchange = () => selectMic(micSelect.value);
  speakerSelect.onchange = () => selectSpeaker(speakerSelect.value);
  header.append(micSelect, speakerSelect, micButton, connectToggleButton, playButton);
  root.append(header, content);
  render(state, content);
  return { connect, disconnect, toggleMic, getState: () => state, getClient: () => client, audio };
}

function el(documentRef, tag, text, className) {
  const node = documentRef.createElement(tag);
  node.textContent = text;
  if (className) node.className = className;
  return node;
}

function populateSelect(documentRef, select, devices, selectedDeviceId) {
  if (!select) return;
  const deviceList = devices || [];
  select.replaceChildren(
    ...deviceList.map((device, index) => {
      const option = documentRef.createElement("option");
      option.value = device.deviceId;
      option.textContent = device.label || `Device ${index + 1}`;
      return option;
    }),
  );
  if (selectedDeviceId && deviceList.some((device) => device.deviceId === selectedDeviceId)) {
    select.value = selectedDeviceId;
  }
}

if (typeof document !== "undefined") createApp();
