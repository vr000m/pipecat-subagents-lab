import { PipecatClient } from "@pipecat-ai/client-js";
import { SmallWebRTCTransport } from "@pipecat-ai/small-webrtc-transport";
import { applyServerMessage, createInitialState } from "./state.js";
import { render } from "./render.js";

export function createApp({ root, documentRef = globalThis.document, webrtcUrl = "/api/rtc", clientFactory, transportFactory } = {}) {
  root ??= documentRef?.querySelector?.("#app");
  let state = createInitialState();
  let client;
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
    onConnected: () => { update({ ...state, connection: "connected" }); requestSnapshot(); },
    onDisconnected: () => update({ ...state, connection: "disconnected" }),
    onError: (message) => report(message?.data?.message || "The RTVI connection reported an error."),
    onServerMessage: (message) => update(applyServerMessage(state, message, requestSnapshot)),
    onTrackStarted: attachTrack,
    onTrackStopped: detachTrack,
    onUserTranscript: (data) => update({ ...state, transcript: [...state.transcript, { role: "user", ...data }] }),
    onBotTranscript: (data) => update({ ...state, transcript: [...state.transcript, { role: "assistant", ...data }] }),
  };

  const connect = async () => {
    const transport = transportFactory
      ? transportFactory({ webrtcUrl })
      : new SmallWebRTCTransport({ webrtcUrl });
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
