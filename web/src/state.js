export const DELIVERY_COMPLETE = "delivery_completed";

const EMPTY_DIAGNOSTICS = {
  lastSequence: 0,
  lastAppliedSequence: 0,
  lastSnapshotSequence: 0,
  gaps: 0,
  snapshotRequestPending: false,
  snapshotRequestCount: 0,
  message: "",
};

export function createInitialState() {
  return {
    connection: "disconnected",
    sessionId: null,
    routing: null,
    transcript: [],
    workers: [],
    results: [],
    speech: {},
    workStatus: {},
    localDiagnostics: { ...EMPTY_DIAGNOSTICS },
    lastAppliedSequence: 0,
    serverState: false,
    connectionEpoch: null,
    // Device/toolbar UI state. `devices.speakerSupported` is a capability
    // flag the app seeds once at startup from the audio sink; everything
    // else here is driven by the withDeviceList/withSelectedDevice reducers.
    devices: {
      mics: [],
      speakers: [],
      selectedMicId: null,
      selectedSpeakerId: null,
      speakerSupported: true,
    },
    micEnabled: false,
  };
}

// `connectionActive` and `generation`/`activeGeneration` (owned by app.js)
// are deliberately NOT part of this state shape: they are internal,
// synchronous session-lifecycle guards that decide which client callbacks
// are still live, not render-facing truth. `state.connection` is the only
// connection value the UI renders from.
const DEVICE_SLOTS = {
  mic: { list: "mics", selected: "selectedMicId" },
  speaker: { list: "speakers", selected: "selectedSpeakerId" },
};

export function withConnection(state, connection) {
  return { ...state, connection };
}

export function withMicEnabled(state, enabled) {
  return { ...state, micEnabled: Boolean(enabled) };
}

export function withDeviceList(state, slot, devices, preferredId = null) {
  const { list, selected } = DEVICE_SLOTS[slot];
  const normalized = (Array.isArray(devices) ? devices : [])
    .filter((device) => device && device.deviceId)
    .map((device) => ({ deviceId: device.deviceId, label: device.label || "" }));
  const currentSelected = state.devices[selected];
  let resolvedId = null;
  if (preferredId && normalized.some((device) => device.deviceId === preferredId)) {
    resolvedId = preferredId;
  } else if (currentSelected && normalized.some((device) => device.deviceId === currentSelected)) {
    resolvedId = currentSelected;
  }
  return {
    ...state,
    devices: { ...state.devices, [list]: normalized, [selected]: resolvedId },
  };
}

export function withSelectedDevice(state, slot, deviceId) {
  const { selected } = DEVICE_SLOTS[slot];
  return { ...state, devices: { ...state.devices, [selected]: deviceId ?? null } };
}

export function disconnectedState(state) {
  return {
    ...state,
    connection: "disconnected",
    micEnabled: false,
    devices: {
      ...state.devices,
      mics: [],
      speakers: [],
      selectedMicId: null,
      selectedSpeakerId: null,
    },
  };
}

export function normalizeUrl(value) {
  if (typeof value !== "string") return null;
  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:" ? url.href : null;
  } catch {
    return null;
  }
}

function normalizedResult(result) {
  if (!result || typeof result !== "object") return null;
  const citations = Array.isArray(result.citations)
    ? result.citations
    : Array.isArray(result.sources)
      ? result.sources
      : [];
  const seen = new Set();
  return {
    result_id: result.result_id,
    worker_id: result.worker_id,
    turn_id: result.turn_id,
    timestamp: result.timestamp,
    text: result.text,
    spoken_text: result.spoken_text,
    ui_text: result.text,
    origin_epoch: result.origin_epoch,
    citations: citations
      .map((citation) => ({ ...citation, url: normalizeUrl(citation?.url) }))
      .filter((citation) => citation.url && !seen.has(citation.url) && seen.add(citation.url)),
  };
}

function projectedWorker(worker) {
  if (!worker || typeof worker !== "object" || !worker.worker_id) return null;
  return {
    worker_id: worker.worker_id,
    topic: worker.topic,
    model_policy: worker.model_policy,
    status: worker.status,
    latest_result_id: worker.latest_result_id,
    origin_epoch: worker.origin_epoch,
  };
}

function projectedWorkStatus(status) {
  if (!status || typeof status !== "object" || !status.turn_id) return null;
  return {
    turn_id: status.turn_id,
    work_item_id: status.work_item_id,
    worker_id: status.worker_id,
    state: status.state,
    event_sequence: status.event_sequence,
    terminal_reason: status.terminal_reason,
    origin_epoch: status.origin_epoch,
  };
}

// Mirrors the server's WorkStatusKey = (origin_epoch, turn_id, parent
// work item) exactly. `origin_epoch` is part of the identity, not decoration:
// `event_sequence` is allocated per that whole triple, so two server records
// differing only by epoch each restart their sequence at 1. Keying without
// the epoch collapses them into one entry, where the reducer's
// `previous.event_sequence >= projected.event_sequence` staleness guard would
// silently drop the newer-epoch record. Terminal records deliberately survive
// reconnect carrying their historical origin_epoch, so cross-epoch collisions
// on one turn_id are a supported case, not a corner case.
function workStatusKey(status) {
  return `${status.origin_epoch ?? ""}::${status.turn_id}::${status.work_item_id ?? ""}`;
}

// Client-side mirror of server/session_state.py's ledger bound
// (_MAX_WORK_STATUS_KEYS / WORK_STATUS_TTL_SECONDS): a long, gap-free session
// never receives a snapshot rebuild, so an insert-only map would otherwise
// keep every terminal record rendered indefinitely. This is a simplified
// version of the server's eviction, not a byte-for-byte port.
const WORK_STATUS_MAX_KEYS = 256;
const WORK_STATUS_TERMINAL_TTL_MS = 5 * 60 * 1000;
const WORK_STATUS_TERMINAL_STATES = new Set(["result_ready", "failed", "cancelled"]);

function pruneExpiredWorkStatus(workStatus, now) {
  let changed = false;
  const next = {};
  for (const [key, record] of Object.entries(workStatus)) {
    if (record._terminalSince !== undefined && now - record._terminalSince >= WORK_STATUS_TERMINAL_TTL_MS) {
      changed = true;
      continue;
    }
    next[key] = record;
  }
  return changed ? next : workStatus;
}

// Terminal-first eviction, oldest terminal first, mirroring
// _evict_work_status_overflow's ordering; `protectKey` is the record just
// written and is never the eviction victim.
function evictOldestWorkStatus(workStatus, protectKey) {
  if (Object.keys(workStatus).length <= WORK_STATUS_MAX_KEYS) return workStatus;
  const next = { ...workStatus };
  while (Object.keys(next).length > WORK_STATUS_MAX_KEYS) {
    const candidates = Object.keys(next).filter((key) => key !== protectKey);
    if (candidates.length === 0) break;
    let oldestKey = candidates[0];
    for (const key of candidates) {
      const a = next[key];
      const b = next[oldestKey];
      const aTerminal = a._terminalSince !== undefined;
      const bTerminal = b._terminalSince !== undefined;
      const aIsOlder = aTerminal !== bTerminal ? aTerminal : (a._terminalSince ?? 0) < (b._terminalSince ?? 0);
      if (aIsOlder) oldestKey = key;
    }
    delete next[oldestKey];
  }
  return next;
}

function upsertWorkStatus(workStatus, key, projected, now = Date.now(), previousRecord = workStatus[key]) {
  // Preserve an already-terminal key's original `_terminalSince` (e.g. a
  // reconnect snapshot re-delivering the same terminal record) instead of
  // restamping it to `now`: within this serving process, the server's
  // five-minute TTL is measured from when the record actually went
  // terminal, not from whenever the client last happened to receive it
  // (protocol.md, "Progressive work status") -- though
  // `SessionState.from_snapshot` does restamp `terminal_at` on restore (no
  // production caller today, only tests), so this is not a cross-process
  // guarantee.
  //
  // If a terminal record was pruned client-side and a later snapshot
  // re-adds the same key, `previousRecord` is undefined here and this
  // grants a fresh five-minute window -- bounded today only because the
  // server also prunes at projection and stops sending it, not because
  // this function independently enforces the TTL.
  const record = WORK_STATUS_TERMINAL_STATES.has(projected.state)
    ? { ...projected, _terminalSince: previousRecord?._terminalSince ?? now }
    : projected;
  const withEntry = { ...workStatus, [key]: record };
  return evictOldestWorkStatus(pruneExpiredWorkStatus(withEntry, now), key);
}

function projectedSpeech(progress) {
  if (!progress || typeof progress !== "object" || !progress.utterance_id) return null;
  return {
    result_id: progress.result_id,
    work_item_id: progress.work_item_id,
    run_id: progress.run_id,
    utterance_id: progress.utterance_id,
    state: progress.state,
    origin_epoch: progress.origin_epoch,
  };
}

function snapshotState(state, snapshot, sequence) {
  const snapshotSequence = Number(snapshot.snapshot_sequence ?? sequence);
  const results = Array.isArray(snapshot.results)
    ? snapshot.results.map(normalizedResult).filter(Boolean)
    : [];
  const speech = Object.fromEntries(
    (Array.isArray(snapshot.speech_progress) ? snapshot.speech_progress : [])
      .map(projectedSpeech)
      .filter(Boolean)
      .map((item) => [item.utterance_id, item]),
  );
  const now = Date.now();
  // The snapshot is still a full replacement (a key the server no longer
  // reports must disappear, e.g. server-side eviction) -- but a key that
  // does reappear keeps its pre-snapshot `_terminalSince` looked up from
  // `state.workStatus` explicitly, rather than restamping a fresh TTL
  // window on every reconnect.
  const workStatus = (Array.isArray(snapshot.work_status) ? snapshot.work_status : [])
    .map(projectedWorkStatus)
    .filter(Boolean)
    .reduce((acc, item) => {
      const key = workStatusKey(item);
      // `previousRecord` is seeded from the pre-snapshot `state.workStatus`,
      // not from `acc` (the in-progress reduce accumulator). Harmless today
      // since a snapshot carries at most one parent record per key, but
      // would make this non-idempotent under a hypothetical future
      // duplicate-key-within-one-snapshot case.
      return upsertWorkStatus(acc, key, item, now, state.workStatus[key]);
    }, {});
  const diagnostics = {
    ...state.localDiagnostics,
    lastSequence: snapshotSequence,
    lastAppliedSequence: snapshotSequence,
    lastSnapshotSequence: Math.max(state.localDiagnostics.lastSnapshotSequence, snapshotSequence),
    snapshotRequestPending: false,
  };
  return {
    ...state,
    sessionId: snapshot.session_id ?? state.sessionId,
    connectionEpoch: snapshot.origin_epoch ?? state.connectionEpoch,
    workers: Array.isArray(snapshot.workers) ? snapshot.workers.map(projectedWorker).filter(Boolean) : [],
    results,
    speech,
    workStatus,
    routing: snapshot.routing ?? null,
    transcript: Array.isArray(snapshot.transcript) ? snapshot.transcript.map((item) => ({ ...item })) : [],
    serverState: true,
    lastAppliedSequence: snapshotSequence,
    localDiagnostics: diagnostics,
  };
}

function addResult(state, result) {
  const normalized = normalizedResult(result);
  if (!normalized || !normalized.result_id) return state;
  if (state.results.some((item) => item.result_id === normalized.result_id)) return state;
  return { ...state, results: [...state.results, normalized] };
}

function applyIncrement(state, payload) {
  switch (payload.kind) {
    case "result":
      return addResult(state, payload.data);
    case "speech_progress": {
      const progress = payload.data;
      const projected = projectedSpeech(progress);
      if (!projected) return state;
      return { ...state, speech: { ...state.speech, [projected.utterance_id]: projected } };
    }
    case "worker": {
      const worker = payload.data;
      const projected = projectedWorker(worker);
      if (!projected) return state;
      return { ...state, workers: [...state.workers.filter((item) => item.worker_id !== projected.worker_id), projected] };
    }
    case "routing":
      return { ...state, routing: payload.data };
    case "work_status": {
      const projected = projectedWorkStatus(payload.data);
      if (!projected) return state;
      const key = workStatusKey(projected);
      const previous = state.workStatus[key];
      if (previous && previous.event_sequence >= projected.event_sequence) return state;
      return { ...state, workStatus: upsertWorkStatus(state.workStatus, key, projected) };
    }
    case "user_transcript":
    case "bot_transcript":
      return {
        ...state,
        transcript: [...state.transcript, { ...payload.data, role: payload.kind === "user_transcript" ? "user" : "assistant" }],
      };
    default:
      return state;
  }
}

export function applyServerMessage(state, rawMessage, requestSnapshot = () => {}) {
  const message = rawMessage?.data?.kind ? rawMessage.data : rawMessage;
  if (!message || typeof message !== "object") return state;
  const sequence = Number(message.sequence ?? 0);
  const kind = message.kind;
  const snapshot = kind === "runtime_snapshot" ? message.data : null;
  if (!snapshot && state.sessionId && message.session_id !== state.sessionId) return state;
  if (!snapshot && state.connectionEpoch !== null && message.origin_epoch !== state.connectionEpoch) return state;
  if (snapshot) {
    if (state.sessionId && snapshot.session_id && snapshot.session_id !== state.sessionId) {
      state = {
        ...createInitialState(),
        connection: state.connection,
        devices: state.devices,
        micEnabled: state.micEnabled,
        localDiagnostics: {
          ...EMPTY_DIAGNOSTICS,
          gaps: state.localDiagnostics.gaps,
          message: state.localDiagnostics.message,
        },
      };
    }
    const snapshotSequence = Number(snapshot.snapshot_sequence ?? sequence);
    if (snapshotSequence < Math.max(state.lastAppliedSequence, state.localDiagnostics.lastSnapshotSequence)) return state;
    const snapshotEpoch = snapshot.origin_epoch;
    if (
      state.connectionEpoch !== null &&
      snapshotEpoch !== undefined &&
      snapshotEpoch !== null &&
      snapshotEpoch < state.connectionEpoch
    ) return state;
    return snapshotState(state, snapshot, sequence);
  }
  // Increments are meaningful only after the first authoritative snapshot.
  // A ready event can race the snapshot response; accepting that increment
  // would make a later, lower-sequence snapshot look stale and strand state.
  if (!state.serverState) return state;
  if (!sequence || sequence <= state.lastAppliedSequence) return state;
  if (state.localDiagnostics.snapshotRequestPending) return state;
  if (sequence < state.localDiagnostics.lastSnapshotSequence) return state;
  const expected = state.lastAppliedSequence + 1;
  if (sequence !== expected) {
    if (!state.localDiagnostics.snapshotRequestPending) requestSnapshot();
    return {
      ...state,
      localDiagnostics: {
        ...state.localDiagnostics,
        gaps: state.localDiagnostics.gaps + 1,
        lastAppliedSequence: state.lastAppliedSequence,
        lastSequence: state.lastAppliedSequence,
        snapshotRequestPending: true,
        snapshotRequestCount: state.localDiagnostics.snapshotRequestCount + (state.localDiagnostics.snapshotRequestPending ? 0 : 1),
        message: `State gap detected (${expected}–${sequence - 1}); requesting snapshot.`,
      },
    };
  }
  const next = applyIncrement({ ...state, serverState: true }, message);
  return {
    ...next,
    lastAppliedSequence: sequence,
    localDiagnostics: { ...next.localDiagnostics, lastSequence: sequence, lastAppliedSequence: sequence, snapshotRequestPending: false },
  };
}

export const applyMessage = applyServerMessage;
export const replaceFromSnapshot = (state, snapshotMessage) => applyServerMessage(state, snapshotMessage);
