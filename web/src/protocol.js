import { applyMessage, createInitialState } from "./state.js";

export const CONTRACT_VERSION = "v1.0";
const forbidden = ["raw_logs", "prompt", "context"];
export const RTVI_MESSAGE_KINDS = Object.freeze([
  "runtime_snapshot",
  "result",
  "speech_progress",
  "worker",
  "routing",
  "user_transcript",
  "bot_transcript",
]);
const runtimeKinds = new Set(RTVI_MESSAGE_KINDS);
const deliveryStates = new Set([
  "displayed",
  "queued",
  "started",
  "synthesis_ended",
  "delivery_completed",
  "delivery_unknown",
  "interrupted",
  "interrupted_by_reconnect",
  "paused",
  "resumed",
]);

function validOrigin(value) {
  return value === undefined || value === null || (Number.isInteger(value) && value >= 0);
}

function validCitation(value) {
  if (!value || typeof value !== "object" || typeof value.url !== "string") return false;
  if (Object.keys(value).some((key) => !["title", "url"].includes(key))) return false;
  try {
    const url = new URL(value.url);
    return (url.protocol === "http:" || url.protocol === "https:") && typeof value.title === "string";
  } catch {
    return false;
  }
}

function validWorker(value) {
  if (!value || Object.keys(value).some((key) => !["worker_id", "topic", "model_policy", "status", "latest_result_id", "origin_epoch"].includes(key))) return false;
  return value && typeof value === "object" &&
    typeof value.worker_id === "string" && typeof value.topic === "string" &&
    typeof value.model_policy === "string" && typeof value.status === "string" &&
    (value.latest_result_id === null || typeof value.latest_result_id === "string") &&
    Object.hasOwn(value, "origin_epoch") && validOrigin(value.origin_epoch);
}

function validResult(value) {
  const keys = ["result_id", "worker_id", "turn_id", "timestamp", "text", "citations", "spoken_text", "ui_text", "spoken_result_id", "ui_result_id", "spoken_citations", "ui_citations", "origin_epoch"];
  if (!value || Object.keys(value).some((key) => !keys.includes(key))) return false;
  const validOptionalId = (item) => item === null || typeof item === "string";
  const validOptionalCitations = (items) => items === null || (Array.isArray(items) && items.every(validCitation));
  return value && typeof value === "object" &&
    typeof value.result_id === "string" && typeof value.worker_id === "string" &&
    typeof value.turn_id === "string" && typeof value.timestamp === "string" &&
    typeof value.text === "string" && value.text.trim().length > 0 &&
    typeof value.spoken_text === "string" && value.spoken_text.trim().length > 0 &&
    typeof value.ui_text === "string" &&
    value.ui_text === value.text && Array.isArray(value.citations) &&
    value.citations.every(validCitation) && validOptionalId(value.spoken_result_id) &&
    validOptionalId(value.ui_result_id) && value.spoken_result_id === value.result_id &&
    value.ui_result_id === value.result_id && validOptionalCitations(value.spoken_citations) &&
    validOptionalCitations(value.ui_citations) && Object.hasOwn(value, "origin_epoch") &&
    validOrigin(value.origin_epoch);
}

function validSpeech(value) {
  if (!value || Object.keys(value).some((key) => !["result_id", "work_item_id", "run_id", "utterance_id", "state", "origin_epoch"].includes(key))) return false;
  return value && typeof value === "object" &&
    typeof value.result_id === "string" && typeof value.work_item_id === "string" &&
    typeof value.run_id === "string" && typeof value.utterance_id === "string" &&
    deliveryStates.has(value.state) && Object.hasOwn(value, "origin_epoch") && validOrigin(value.origin_epoch);
}

function validRouting(value) {
  const keys = ["turn_id", "action", "worker_id", "worker_type", "topic", "model_policy", "origin_epoch"];
  const actions = new Set(["direct", "unsupported", "clarify", "existing_worker", "new_worker"]);
  const optionalString = (item) => item === null || typeof item === "string";
  if (!value || typeof value !== "object" || Object.keys(value).some((key) => !keys.includes(key))) return false;
  return typeof value.turn_id === "string" && actions.has(value.action) &&
    optionalString(value.worker_id) && optionalString(value.worker_type) &&
    optionalString(value.topic) && optionalString(value.model_policy) &&
    Object.hasOwn(value, "origin_epoch") && validOrigin(value.origin_epoch);
}

function validTranscript(value) {
  const keys = ["role", "text", "turn_id", "timestamp", "origin_epoch"];
  if (!value || typeof value !== "object" || Object.keys(value).some((key) => !keys.includes(key))) return false;
  return ["user", "assistant"].includes(value.role) && typeof value.text === "string" &&
    typeof value.turn_id === "string" && typeof value.timestamp === "string" &&
    Object.hasOwn(value, "origin_epoch") && validOrigin(value.origin_epoch);
}

export function validateServerMessage(message) {
  if (!message || typeof message !== "object" || typeof message.kind !== "string") return false;
  const envelopeKeys = ["contract_version", "session_id", "sequence", "kind", "data", "origin_epoch"];
  if (Object.keys(message).some((key) => !envelopeKeys.includes(key))) return false;
  if (!runtimeKinds.has(message.kind)) return false;
  if (typeof message.session_id !== "string" || message.session_id.length === 0) return false;
  if (message.contract_version !== CONTRACT_VERSION) return false;
  if (!Number.isInteger(message.sequence) || message.sequence < 0) return false;
  if (!Number.isInteger(message.origin_epoch) || message.origin_epoch < 0) return false;
  const data = message.data;
  if (!data || typeof data !== "object") return false;
  if (data.origin_epoch !== message.origin_epoch) return false;
  if (message.kind === "result" && !validResult(data)) return false;
  if (message.kind === "speech_progress" && !validSpeech(data)) return false;
  if (message.kind === "worker" && !validWorker(data)) return false;
  if (message.kind === "routing" && !validRouting(data)) return false;
  if (["user_transcript", "bot_transcript"].includes(message.kind)) {
    if (!validTranscript(data)) return false;
    if (message.kind === "user_transcript" && data.role !== "user") return false;
    if (message.kind === "bot_transcript" && data.role !== "assistant") return false;
  }
  if (message.kind === "runtime_snapshot") {
    if (typeof data.session_id !== "string" || data.session_id.length === 0) return false;
    if (message.session_id !== data.session_id) return false;
    if (data.contract_version !== CONTRACT_VERSION || !validOrigin(data.origin_epoch)) return false;
    if (!Number.isInteger(data.snapshot_sequence) || data.snapshot_sequence < 0) return false;
    if (message.sequence !== data.snapshot_sequence) return false;
    if (!Array.isArray(data.workers) || !Array.isArray(data.results) ||
        !Array.isArray(data.speech_progress) || !Array.isArray(data.transcript)) {
      return false;
    }
    if (!data.workers.every(validWorker) || !data.results.every(validResult) || !data.speech_progress.every(validSpeech)) {
      return false;
    }
    if (!(data.routing === null || validRouting(data.routing)) || !data.transcript.every(validTranscript)) return false;
  }
  return !forbidden.some((field) => Object.hasOwn(data, field));
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
