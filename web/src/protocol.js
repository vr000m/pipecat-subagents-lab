import { applyMessage, createInitialState } from "./state.js";

export const CONTRACT_VERSION = "v1.0";
const forbidden = ["raw_logs", "prompt", "context"];

export function validateServerMessage(message) {
  if (!message || typeof message !== "object" || typeof message.kind !== "string") return false;
  if (message.contract_version !== CONTRACT_VERSION) return false;
  if (!Number.isInteger(message.sequence) || message.sequence < 0) return false;
  const data = message.data;
  if (!data || typeof data !== "object") return false;
  return !forbidden.some((field) => Object.prototype.hasOwnProperty.call(data, field));
}

export function createClientProtocol({ transport, onState = () => {}, requestSnapshot } = {}) {
  let state = createInitialState();
  let ready = false;
  let connected = false;
  const snapshot = requestSnapshot || (() => transport?.requestSnapshot?.());
  const publish = (next) => { state = next; onState(state); return state; };
  return {
    async initialize() {},
    async connect() { connected = true; await transport?.connect?.(); },
    async disconnect() { connected = false; await transport?.disableMic?.(); await transport?.disconnect?.(); },
    async setMicrophoneEnabled(enabled) { if (!connected) return; if (enabled) await transport?.enableMic?.(true); else await transport?.disableMic?.(); },
    async receive(message) {
      if (message?.kind === "client-ready" || message?.kind === "reconnected") {
        ready = true;
        await snapshot();
        return;
      }
      // The server's runtime projection is only authoritative after the
      // media/RTVI readiness boundary and its initial snapshot request.
      if (!ready) return;
      if (validateServerMessage(message)) {
        const next = applyMessage(state, message, snapshot);
        if (next !== state) publish(next);
      }
    },
    getState: () => state,
    get ready() { return ready; },
  };
}
