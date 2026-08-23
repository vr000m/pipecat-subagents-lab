import { applyMessage, createInitialState } from "./state.js";
import groundedResultSchema from "../../shared/schemas/grounded-result.json";
import runtimeSnapshotSchema from "../../shared/schemas/runtime-snapshot.json";
import workStatusSchema from "../../shared/schemas/work-status.json";

export const CONTRACT_VERSION = "v1.0";
// Capability name gating the `work_status` wire kind. Must match
// server/contracts.py's WORK_STATUS_V1 exactly (asserted in both suites).
export const WORK_STATUS_V1 = "work_status_v1";
const forbidden = ["raw_logs", "prompt", "context"];
export const RTVI_MESSAGE_KINDS = Object.freeze([
  "runtime_snapshot",
  "result",
  "speech_progress",
  "worker",
  "routing",
  "user_transcript",
  "bot_transcript",
  "work_status",
]);
const runtimeKinds = new Set(RTVI_MESSAGE_KINDS);
const groundedResultKeys = Object.freeze(Object.keys(groundedResultSchema.properties));
// "work_status" is the one optional runtime-snapshot key: non-capable
// projections omit it entirely (field absent, not an empty array), so the
// exact-key check below treats it separately rather than requiring it.
const runtimeSnapshotKeys = Object.freeze(
  Object.keys(runtimeSnapshotSchema.properties).filter((key) => key !== "work_status"),
);
// shared/schemas/work-status.json lists every one of these as `required`, so
// the browser check is symmetric (no extra keys AND no missing keys) rather
// than extra-keys-only. `terminal_reason` and `origin_epoch` are nullable but
// still mandatory on the wire -- the Python side always emits them as literal
// null -- so treating an absent key as acceptable would have quietly diverged
// from the schema.
const workStatusKeys = Object.freeze(Object.keys(workStatusSchema.properties));
// Derived from the schema's own `enum` arrays, not hand-copied from them
// (round-3 restart gauntlet, Architecture finding). This file already imported
// work-status.json to derive `workStatusKeys` from `properties`, then
// hand-wrote the two enums out of that same file -- so a state added to the
// schema was picked up by the key check and silently rejected by the value
// check. The Python side derives `WORK_STATUS_STATES`/`WORK_STATUS_TERMINAL`
// from a single transitions table for the same reason.
const workStatusStates = new Set(workStatusSchema.properties.state.enum);
// `null` is a legal `terminal_reason` and is in the schema enum, but it is
// handled by the explicit `!== null` check at the call site, so it is filtered
// out here rather than being a member of the "named reason" set.
const workStatusTerminalReasons = new Set(
  workStatusSchema.properties.terminal_reason.enum.filter((reason) => reason !== null),
);
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

function hasExactKeys(value, expectedKeys) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const actualKeys = Object.keys(value);
  return actualKeys.length === expectedKeys.length &&
    expectedKeys.every((key) => Object.hasOwn(value, key));
}

function validTimestamp(value) {
  if (typeof value !== "string") return false;
  const match = value.match(
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|([+-])(\d{2}):(\d{2}))$/,
  );
  if (!match) return false;
  const [, yearText, monthText, dayText, hourText, minuteText, secondText, , offsetHourText, offsetMinuteText] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText);
  const offsetHour = offsetHourText === undefined ? 0 : Number(offsetHourText);
  const offsetMinute = offsetMinuteText === undefined ? 0 : Number(offsetMinuteText);
  const daysInMonth = month >= 1 && month <= 12
    ? new Date(Date.UTC(year, month, 0)).getUTCDate()
    : 0;
  return year >= 1 && day >= 1 && day <= daysInMonth &&
    hour <= 23 && minute <= 59 && second <= 59 &&
    offsetHour <= 23 && offsetMinute <= 59 &&
    !Number.isNaN(Date.parse(value));
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
  if (!hasExactKeys(value, groundedResultKeys)) return false;
  return value && typeof value === "object" &&
    typeof value.result_id === "string" && value.result_id.length > 0 &&
    typeof value.worker_id === "string" && value.worker_id.length > 0 &&
    typeof value.turn_id === "string" && value.turn_id.length > 0 &&
    validTimestamp(value.timestamp) &&
    typeof value.text === "string" && value.text.trim().length > 0 &&
    typeof value.spoken_text === "string" && value.spoken_text.trim().length > 0 &&
    Array.isArray(value.citations) && value.citations.every(validCitation) &&
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
    typeof value.turn_id === "string" && validTimestamp(value.timestamp) &&
    Object.hasOwn(value, "origin_epoch") && validOrigin(value.origin_epoch);
}

function validWorkStatus(value) {
  if (!hasExactKeys(value, workStatusKeys)) return false;
  const optionalString = (item) => item === null || typeof item === "string";
  if (typeof value.turn_id !== "string" || value.turn_id.length === 0) return false;
  if (!optionalString(value.work_item_id) || !optionalString(value.worker_id)) return false;
  if (!workStatusStates.has(value.state)) return false;
  if (!Number.isInteger(value.event_sequence) || value.event_sequence < 0) return false;
  if (value.terminal_reason !== null && !workStatusTerminalReasons.has(value.terminal_reason)) return false;
  if (value.terminal_reason !== null && value.state !== "failed") return false;
  return validOrigin(value.origin_epoch);
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
  if (message.kind === "work_status" && !validWorkStatus(data)) return false;
  if (["user_transcript", "bot_transcript"].includes(message.kind)) {
    if (!validTranscript(data)) return false;
    if (message.kind === "user_transcript" && data.role !== "user") return false;
    if (message.kind === "bot_transcript" && data.role !== "assistant") return false;
  }
  if (message.kind === "runtime_snapshot") {
    const extraKeys = Object.keys(data).filter(
      (key) => !runtimeSnapshotKeys.includes(key) && key !== "work_status",
    );
    if (extraKeys.length > 0 || !runtimeSnapshotKeys.every((key) => Object.hasOwn(data, key))) {
      return false;
    }
    if (Object.hasOwn(data, "work_status")) {
      if (!Array.isArray(data.work_status) || !data.work_status.every(validWorkStatus)) return false;
    }
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
