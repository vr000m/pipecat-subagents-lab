import { PipecatClient } from "@pipecat-ai/client-js";
import { SmallWebRTCTransport } from "@pipecat-ai/small-webrtc-transport";
import { validateServerMessage } from "./protocol.js";
import { applyServerMessage, createInitialState } from "./state.js";
import { render } from "./render.js";

export function createApp({ root, documentRef = globalThis.document, webrtcUrl = "/api/rtc", sessionUrl = "/api/session", fetchImpl = globalThis.fetch, clientFactory, transportFactory } = {}) {
  root ??= documentRef?.querySelector?.("#app");
  let state = createInitialState();
  let client;
  let connectPromise = null;
  let generation = 0;
  let activeGeneration = 0;
  let audio = documentRef.createElement("audio");
  audio.autoplay = true;
  audio.playsInline = true;
  audio.setAttribute("aria-label", "Assistant audio");
  documentRef.body.append(audio);
  const supportsSinkId = typeof audio.setSinkId === "function";

  const content = documentRef.createElement("div");
  const update = (next) => { state = next; render(state, content); };
  const report = (message) => update({ ...state, localDiagnostics: { ...state.localDiagnostics, message } });

  const callbacksFor = (callbackGeneration) => {
    const current = () => callbackGeneration === activeGeneration;
    const requestSnapshot = () => {
      if (current()) client?.sendClientMessage("snapshot-request");
    };
    const attachTrack = (track, participant) => {
      if (!current() || participant?.local || track?.kind !== "audio") return;
      audio.srcObject = new MediaStream([track]);
      audio.play().catch(() => report("Assistant audio is ready, but playback was blocked. Click the page or Play audio to allow it."));
    };
    const detachTrack = (track) => {
      if (!current() || !audio.srcObject) return;
      const tracks = audio.srcObject.getTracks();
      if (!track || tracks.includes(track)) audio.srcObject = null;
    };
    return {
      onConnected: () => { if (current()) update({ ...state, connection: "connected" }); },
      // PipecatClient.connect() sends the standard RTVI client-ready message
      // before resolving onBotReady. Request the server-authoritative snapshot
      // only after that media/RTVI readiness boundary.
      onBotReady: () => requestSnapshot(),
      onDisconnected: () => { if (current()) update({ ...state, connection: "disconnected" }); },
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
      onAvailableSpeakersUpdated: (speakers) => { if (current() && supportsSinkId) populateSelect(documentRef, speakerSelect, speakers, client?.selectedSpeaker?.deviceId); },
    };
  };

  const connect = async () => {
    if (connectPromise) return connectPromise;
    connectPromise = (async () => {
    const callbackGeneration = ++generation;
    activeGeneration = callbackGeneration;
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
      update({ ...state, connection: "connected" });
      setMicButton(Boolean(client.isMicEnabled));
      // Connect is a user gesture, so a previously-created audio element may play here.
      audio.play().catch(() => report("Connected, but browser audio is blocked. Click Play audio to continue."));
      return true;
    } catch (error) {
      report(`Connection failed: ${error?.message || error}`);
      update({ ...state, connection: "disconnected" });
      return false;
    }
    })();
    try { return await connectPromise; } finally { connectPromise = null; }
  };
  const disconnect = async () => {
    micSelect.disabled = true;
    speakerSelect.disabled = true;
    populateSelect(documentRef, micSelect, [], null);
    populateSelect(documentRef, speakerSelect, [], null);
    activeGeneration = ++generation;
    client?.enableMic(false);
    await client?.disconnect();
    audio.srcObject = null;
    setMicButton(false);
    update({ ...state, connection: "disconnected" });
  };
  const selectMic = (deviceId) => { if (deviceId) client?.updateMic(deviceId); };
  const selectSpeaker = async (deviceId) => {
    if (!deviceId) return;
    client?.updateSpeaker(deviceId);
    // The client's own speaker routing does not know about the <audio>
    // element this app manages by hand (see attachTrack); the sink must be
    // set explicitly for switching to actually change what plays.
    if (supportsSinkId) {
      try {
        await audio.setSinkId(deviceId);
      } catch (error) {
        report(`Failed to switch speaker: ${error?.message || error}`);
      }
    }
  };
  const toggleMic = () => {
    if (!client) return;
    const enabled = !client.isMicEnabled;
    client.enableMic(enabled);
    setMicButton(enabled);
    update({ ...state, localDiagnostics: { ...state.localDiagnostics, message: enabled ? "Microphone enabled." : "Microphone disabled." } });
  };

  root.replaceChildren();
  const header = documentRef.createElement("header");
  header.className = "topbar";
  header.append(el(documentRef, "h1", "Pipecat Subagents Lab"));
  const connectButton = el(documentRef, "button", "Connect");
  const disconnectButton = el(documentRef, "button", "Disconnect");
  const micButton = el(documentRef, "button", "Mic: off");
  const playButton = el(documentRef, "button", "Play audio");
  const micSelect = documentRef.createElement("select");
  micSelect.setAttribute("aria-label", "Microphone");
  const speakerSelect = documentRef.createElement("select");
  speakerSelect.setAttribute("aria-label", "Speaker");
  const setMicButton = (enabled) => { micButton.textContent = `Mic: ${enabled ? "on" : "off"}`; };
  disconnectButton.disabled = true; micButton.disabled = true; playButton.hidden = true;
  micSelect.disabled = true; speakerSelect.disabled = true;
  if (!supportsSinkId) {
    speakerSelect.hidden = true;
    speakerSelect.title = "This browser does not support switching audio output devices.";
  }
  connectButton.onclick = async () => {
    if (!await connect()) return;
    connectButton.disabled = true; disconnectButton.disabled = false; micButton.disabled = false; playButton.hidden = false;
    micSelect.disabled = false; speakerSelect.disabled = false;
  };
  disconnectButton.onclick = async () => {
    await disconnect();
    connectButton.disabled = false; disconnectButton.disabled = true; micButton.disabled = true;
  };
  micButton.onclick = toggleMic;
  playButton.onclick = () => audio.play().catch(() => report("Playback is still blocked; check browser site permissions."));
  micSelect.onchange = () => selectMic(micSelect.value);
  speakerSelect.onchange = () => selectSpeaker(speakerSelect.value);
  header.append(connectButton, disconnectButton, micButton, micSelect, speakerSelect, playButton);
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
