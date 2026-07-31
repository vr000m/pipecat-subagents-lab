// Owns the toolbar DOM: construction plus a pure, dirty-checked render(state).
// No lifecycle/connection logic lives here — this module only ever reads
// `state` and reports user gestures back through `handlers`.

export const ICONS = {
  connect: `<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M9 2v4"/><path d="M15 2v4"/><path d="M8 8h8l-1 6a3 3 0 0 1-3 3h0a3 3 0 0 1-3-3z"/><path d="M12 17v5"/></svg>`,
  disconnect: `<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M9 2v4"/><path d="M15 2v4"/><path d="M8 8h8l-1 6a3 3 0 0 1-3 3h0a3 3 0 0 1-3-3z"/><path d="M12 17v5"/><path d="M3 3l18 18"/></svg>`,
  micOn: `<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="9" y="2" width="6" height="11" rx="3"/><path d="M5 10a7 7 0 0 0 14 0"/><path d="M12 17v5"/><path d="M8 22h8"/></svg>`,
  micOff: `<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="9" y="2" width="6" height="11" rx="3"/><path d="M5 10a7 7 0 0 0 14 0"/><path d="M12 17v5"/><path d="M8 22h8"/><path d="M3 3l18 18"/></svg>`,
  play: `<svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor" aria-hidden="true"><path d="M6 4l14 8-14 8z"/></svg>`,
};

function el(documentRef, tag, text, className) {
  const node = documentRef.createElement(tag);
  node.textContent = text;
  if (className) node.className = className;
  return node;
}

// The single template site for button contents. Only module-local `ICONS`
// constants may ever be passed as `iconSvg` here — never a device label,
// server string, or error message — since it is assigned via innerHTML.
function setButton(documentRef, node, iconSvg, text) {
  const iconSpan = documentRef.createElement("span");
  iconSpan.className = "btn-icon";
  iconSpan.innerHTML = iconSvg;
  const textSpan = documentRef.createElement("span");
  textSpan.className = "btn-text";
  textSpan.textContent = text;
  node.replaceChildren(iconSpan, textSpan);
}

function iconButton(documentRef, iconSvg, text) {
  const node = documentRef.createElement("button");
  setButton(documentRef, node, iconSvg, text);
  return node;
}

function applySelectValue(select, deviceList, selectedDeviceId) {
  if (selectedDeviceId && deviceList.some((device) => device.deviceId === selectedDeviceId)) {
    select.value = selectedDeviceId;
    return;
  }
  // Never leave the browser's implicit first-option selection standing.
  select.selectedIndex = -1;
  select.value = "";
}

export function populateSelect(documentRef, select, devices, selectedDeviceId) {
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
  applySelectValue(select, deviceList, selectedDeviceId);
}

function deviceSignature(devices) {
  return devices.map((device) => `${device.deviceId} ${device.label}`).join("|");
}

export function createControls({ documentRef, handlers }) {
  const header = documentRef.createElement("header");
  header.className = "topbar";
  header.append(el(documentRef, "h1", "Pipecat Subagents Lab"));

  const micSelect = documentRef.createElement("select");
  micSelect.setAttribute("aria-label", "Microphone");
  micSelect.className = "icon-select icon-select-mic";

  const speakerSelect = documentRef.createElement("select");
  speakerSelect.setAttribute("aria-label", "Speaker");
  speakerSelect.className = "icon-select icon-select-speaker";

  const micButton = iconButton(documentRef, ICONS.micOff, "Mic: off");
  const connectToggleButton = iconButton(documentRef, ICONS.connect, "Connect");
  const playButton = iconButton(documentRef, ICONS.play, "Play audio");

  micSelect.onchange = () => handlers.onSelectMic(micSelect.value);
  speakerSelect.onchange = () => handlers.onSelectSpeaker(speakerSelect.value);
  micButton.onclick = () => handlers.onToggleMic();
  connectToggleButton.onclick = () => handlers.onToggleConnection();
  playButton.onclick = () => handlers.onPlayAudio();

  header.append(micSelect, speakerSelect, micButton, connectToggleButton, playButton);

  // Dirty-checking is mandatory, not an optimization: `render` fires on
  // every server message. Blindly calling replaceChildren on a <select>
  // would close an open dropdown mid-interaction, and blindly rewriting
  // button children would drop focus.
  const lastRendered = {
    micSelectSignature: undefined,
    speakerSelectSignature: undefined,
    connectButtonText: undefined,
    micButtonText: undefined,
  };

  const render = (state) => {
    const connected = state.connection === "connected";

    const connectText = connected ? "Disconnect" : "Connect";
    if (lastRendered.connectButtonText !== connectText) {
      setButton(documentRef, connectToggleButton, connected ? ICONS.disconnect : ICONS.connect, connectText);
      lastRendered.connectButtonText = connectText;
    }

    const micText = state.micEnabled ? "Mic: on" : "Mic: off";
    if (lastRendered.micButtonText !== micText) {
      setButton(documentRef, micButton, state.micEnabled ? ICONS.micOn : ICONS.micOff, micText);
      lastRendered.micButtonText = micText;
    }
    micButton.disabled = !connected;

    playButton.hidden = !connected;

    const micDevices = state.devices.mics;
    const micSig = deviceSignature(micDevices);
    if (lastRendered.micSelectSignature !== micSig) {
      populateSelect(documentRef, micSelect, micDevices, state.devices.selectedMicId);
      lastRendered.micSelectSignature = micSig;
    } else {
      applySelectValue(micSelect, micDevices, state.devices.selectedMicId);
    }
    micSelect.disabled = !connected;

    const speakerSupported = state.devices.speakerSupported;
    speakerSelect.hidden = !speakerSupported;
    const speakerDevices = speakerSupported ? state.devices.speakers : [];
    const speakerSig = deviceSignature(speakerDevices);
    if (lastRendered.speakerSelectSignature !== speakerSig) {
      populateSelect(documentRef, speakerSelect, speakerDevices, state.devices.selectedSpeakerId);
      lastRendered.speakerSelectSignature = speakerSig;
    } else {
      applySelectValue(speakerSelect, speakerDevices, state.devices.selectedSpeakerId);
    }
    speakerSelect.disabled = !connected || !speakerSupported;
  };

  return {
    element: header,
    render,
    nodes: { micSelect, speakerSelect, micButton, connectToggleButton, playButton },
  };
}
