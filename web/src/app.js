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
  let audio = documentRef.createElement("audio");
  audio.autoplay = true;
  audio.playsInline = true;
  audio.setAttribute("aria-label", "Assistant audio");
  documentRef.body.append(audio);

  const content = documentRef.createElement("div");
  const update = (next) => { state = next; render(state, content); };
  const requestSnapshot = () => client?.sendClientMessage("snapshot-request");
  const report = (message) => update({ ...state, localDiagnostics: { ...state.localDiagnostics, message } });
  const attachTrack = (track, participant) => {
    if (participant?.local || track?.kind !== "audio") return;
    audio.srcObject = new MediaStream([track]);
    audio.play().catch(() => report("Assistant audio is ready, but playback was blocked. Click the page or Play audio to allow it."));
  };
  const detachTrack = (track) => {
    if (!audio.srcObject) return;
    const tracks = audio.srcObject.getTracks();
    if (!track || tracks.includes(track)) audio.srcObject = null;
  };

  const callbacks = {
    onConnected: () => update({ ...state, connection: "connected" }),
    // PipecatClient.connect() sends the standard RTVI client-ready message
    // before resolving onBotReady. Request the server-authoritative snapshot
    // only after that media/RTVI readiness boundary.
    onBotReady: () => requestSnapshot(),
    onDisconnected: () => update({ ...state, connection: "disconnected" }),
    onError: (message) => report(message?.data?.message || "The RTVI connection reported an error."),
    onServerMessage: (message) => {
      const candidate = message?.data?.kind ? message.data : message;
      if (!validateServerMessage(candidate)) {
        report("Ignored malformed or unsupported server message.");
        return;
      }
      update(applyServerMessage(state, candidate, requestSnapshot));
    },
    onTrackStarted: attachTrack,
    onTrackStopped: detachTrack,
    onUserTranscript: (data) => update({ ...state, transcript: [...state.transcript, { role: "user", ...data }] }),
    onBotTranscript: (data) => update({ ...state, transcript: [...state.transcript, { role: "assistant", ...data }] }),
  };

  const connect = async () => {
    if (connectPromise) return connectPromise;
    connectPromise = (async () => {
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
    const transport = transportFactory
      ? transportFactory({ webrtcUrl: connectionUrl })
      : new SmallWebRTCTransport({ webrtcUrl: connectionUrl });
    client = clientFactory ? clientFactory(transport, callbacks) : new PipecatClient({ transport, callbacks, enableMic: false });
    try {
      await client.connect();
      update({ ...state, connection: "connected" });
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
    client?.enableMic(false);
    await client?.disconnect();
    audio.srcObject = null;
    update({ ...state, connection: "disconnected" });
  };
  const toggleMic = () => {
    if (!client) return;
    const enabled = !client.isMicEnabled;
    client.enableMic(enabled);
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
  disconnectButton.disabled = true; micButton.disabled = true; playButton.hidden = true;
  connectButton.onclick = async () => {
    if (!await connect()) return;
    connectButton.disabled = true; disconnectButton.disabled = false; micButton.disabled = false; playButton.hidden = false;
  };
  disconnectButton.onclick = async () => { await disconnect(); connectButton.disabled = false; disconnectButton.disabled = true; micButton.disabled = true; };
  micButton.onclick = toggleMic;
  playButton.onclick = () => audio.play().catch(() => report("Playback is still blocked; check browser site permissions."));
  header.append(connectButton, disconnectButton, micButton, playButton);
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

if (typeof document !== "undefined") createApp();
