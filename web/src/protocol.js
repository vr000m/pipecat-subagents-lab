import { applyMessage, createInitialState } from "./state.js";

export const CONTRACT_VERSION = "v1.0";
const forbidden = ["raw_logs", "prompt", "context"];
const runtimeKinds = new Set([
  "result",
  "runtime_result",
  "speech",
  "speech_progress",
  "worker",
  "routing",
  "user_transcript",
  "bot_transcript",
  "runtime_snapshot",
]);

export function validateServerMessage(message) {
  if (!message || typeof message !== "object" || typeof message.kind !== "string") return false;
  if (!runtimeKinds.has(message.kind)) return false;
  if (typeof message.session_id !== "string" || message.session_id.length === 0) return false;
  if (message.contract_version !== CONTRACT_VERSION) return false;
  if (!Number.isInteger(message.sequence) || message.sequence < 0) return false;
  if (message.origin_epoch !== undefined && message.origin_epoch !== null &&
      (!Number.isInteger(message.origin_epoch) || message.origin_epoch < 0)) return false;
  const data = message.data;
  if (!data || typeof data !== "object") return false;
  const payload = data.result ?? data.progress ?? data.worker ?? data;
  if (["result", "runtime_result"].includes(message.kind) &&
      (typeof payload?.result_id !== "string" || payload.result_id.length === 0)) return false;
  if (["speech", "speech_progress"].includes(message.kind) &&
      (typeof payload?.utterance_id !== "string" || typeof payload?.state !== "string")) return false;
  if (message.kind === "worker" && typeof payload?.worker_id !== "string") return false;
  if (["user_transcript", "bot_transcript"].includes(message.kind) && typeof data.text !== "string") return false;
  if (message.kind === "runtime_snapshot") {
    if (typeof data.session_id !== "string" || data.session_id.length === 0) return false;
    if (message.session_id !== undefined && message.session_id !== data.session_id) return false;
    if (!Number.isInteger(data.snapshot_sequence) || data.snapshot_sequence < 0) return false;
    if (!Array.isArray(data.workers) || !Array.isArray(data.results) || !Array.isArray(data.speech_progress)) {
      return false;
    }
  }
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
