import workStatusRetention from "../../shared/work-status-retention.json";
import workStatusSchema from "../../shared/schemas/work-status.json";

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
    // key -> the last `event_sequence` seen for a work_status key that has
    // since been TTL-pruned or evicted out of `workStatus`. See
    // `rememberDroppedWorkStatus` for why the client needs these at all.
    workStatusTombstones: {},
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
// version of the server's eviction, not a byte-for-byte port -- the numeric
// bounds below are loaded from the same shared/work-status-retention.json
// server/session_state.py loads for WORK_STATUS_TTL_SECONDS /
// _MAX_WORK_STATUS_KEYS, so the two numbers cannot silently drift apart. The
// eligibility/ordering rules (terminal-only eviction, terminal-first/
// oldest-first tiebreak) are hand-implemented per language in
// evictOldestWorkStatus below and pinned by parity tests in
// web/test/state.test.js and tests/test_session_state.py against
// shared/protocol.md's "Progressive work status" retention section.
export const WORK_STATUS_MAX_KEYS = workStatusRetention.max_keys;
export const WORK_STATUS_TERMINAL_TTL_MS = workStatusRetention.ttl_seconds * 1000;
// Read from shared/schemas/work-status.json's `x-terminal-states`, not
// hand-written here (round-3 restart gauntlet, Architecture finding). JSON
// Schema cannot express terminality -- it is a property of the state machine,
// not of any single document -- so it is declared beside the `state` enum it
// is a subset of. Python does not read it: server/contracts.py derives
// WORK_STATUS_TERMINAL from _WORK_STATUS_TRANSITIONS (no successors =>
// terminal), which stays the authority, and tests/test_contracts.py asserts
// the two agree so the array cannot drift from the transitions table.
const WORK_STATUS_TERMINAL_STATES = new Set(workStatusSchema["x-terminal-states"]);

// `_restoredAt` mirrors the server's `_work_status_nonauthoritative_at`: a
// non-terminal record restored from a snapshot can never be terminalized
// client-side either (the client, like the server post-restore, has no
// children map to re-aggregate from), so without a clock of its own it
// would never satisfy the `_terminalSince` check below and would be
// retained forever. Stamping it at the restore instant gives it the same
// five-minute TTL a terminal record gets, measured from the same instant.
function pruneExpiredWorkStatus(workStatus, now, dropped) {
  let changed = false;
  const next = {};
  for (const [key, record] of Object.entries(workStatus)) {
    if (record._terminalSince !== undefined && now - record._terminalSince >= WORK_STATUS_TERMINAL_TTL_MS) {
      changed = true;
      dropped?.push([key, record]);
      continue;
    }
    if (
      record._terminalSince === undefined &&
      record._restoredAt !== undefined &&
      now - record._restoredAt >= WORK_STATUS_TERMINAL_TTL_MS
    ) {
      changed = true;
      dropped?.push([key, record]);
      continue;
    }
    next[key] = record;
  }
  return changed ? next : workStatus;
}

// Bound on the tombstone map. Deliberately the same bound as the ledger it
// shadows: a tombstone is strictly smaller than the record it replaces, so
// this cannot grow the client's worst-case footprint by more than a constant
// factor, and an unbounded tombstone map would just be the unbounded-ledger
// problem `WORK_STATUS_MAX_KEYS` exists to prevent, wearing a different hat.
export const WORK_STATUS_MAX_TOMBSTONES = WORK_STATUS_MAX_KEYS;

// Records the staleness watermark for keys leaving `workStatus`.
//
// Round-3 restart gauntlet, Logic finding: the increment reducer's guard is
// `if (previous && previous.event_sequence >= projected.event_sequence)`.
// `pruneExpiredWorkStatus`/`evictOldestWorkStatus` removed terminal entries
// outright rather than leaving anything behind, so once a key was dropped
// `previous` was `undefined`, the guard short-circuited on the falsy check,
// and any late-arriving *lower*-sequence increment for that same
// (origin_epoch, turn_id, work_item_id) triple was re-inserted as if fresh --
// resurrecting a record that had already gone terminal and aged out. The
// server keeps `_work_status_terminal_keys` precisely to stop this on its
// side; this is the client's mirror of it.
//
// Only the sequence number is retained, not the record: the point is to
// answer "have we already seen this key at or past this sequence?", not to
// keep rendering anything.
function rememberDroppedWorkStatus(tombstones, dropped) {
  if (dropped.length === 0) return tombstones;
  const next = { ...tombstones };
  for (const [key, record] of dropped) {
    const previous = next[key];
    // A key can be dropped more than once (re-added by a snapshot, aged out
    // again); keep the highest sequence ever seen for it, never regress.
    next[key] = previous === undefined ? record.event_sequence : Math.max(previous, record.event_sequence);
  }
  // Insertion-ordered oldest-first trim. Losing the oldest tombstone only
  // degrades back to the pre-fix behaviour for that one key, which is the
  // correct thing to give up first under pressure.
  const keys = Object.keys(next);
  for (let index = 0; index < keys.length - WORK_STATUS_MAX_TOMBSTONES; index += 1) {
    delete next[keys[index]];
  }
  return next;
}

// Oldest-eligible-first eviction, mirroring _evict_work_status_overflow's
// ordering (which picks the single oldest expiry stamp across both terminal
// and restored-non-authoritative candidates, not terminal-before-restored);
// `protectKey` is the record just written and is never the eviction victim.
function evictOldestWorkStatus(workStatus, protectKey, dropped) {
  if (Object.keys(workStatus).length <= WORK_STATUS_MAX_KEYS) return workStatus;
  const next = { ...workStatus };
  const expiryStamp = (key) => next[key]._terminalSince ?? next[key]._restoredAt;
  while (Object.keys(next).length > WORK_STATUS_MAX_KEYS) {
    // Terminal and restored-non-authoritative records are evictable,
    // matching the server's _evict_work_status_overflow. A live authoritative
    // non-terminal record is not: dropping it would strand it non-terminal
    // forever. When every retained record is live, the ledger deliberately
    // exceeds the cap instead of evicting one.
    const candidates = Object.keys(next).filter((key) => key !== protectKey && expiryStamp(key) !== undefined);
    if (candidates.length === 0) break;
    let oldestKey = candidates[0];
    for (const key of candidates) {
      if (expiryStamp(key) < expiryStamp(oldestKey)) oldestKey = key;
    }
    dropped?.push([oldestKey, next[oldestKey]]);
    delete next[oldestKey];
  }
  return next;
}

// Returns `{ workStatus, tombstones }`: the prune/evict passes below can drop
// keys, and each dropped key must leave a staleness watermark behind (see
// `rememberDroppedWorkStatus`), so the two maps can only be updated together.
function upsertWorkStatus(workStatus, key, projected, now = Date.now(), previousRecord = workStatus[key], isRestore = false, tombstones = {}) {
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
  //
  // A non-terminal record only gets `_restoredAt` when `isRestore` is set
  // (i.e. this upsert comes from `snapshotState`, not a live increment),
  // preserving a prior restore's stamp the same way `_terminalSince` is
  // preserved above. A live increment for a key already carrying
  // `_restoredAt` keeps that stamp -- it still traces back to a snapshot
  // restore -- so the clock survives every subsequent non-terminal rewrite
  // of the record, same as the server's key-scoped tombstone outliving each
  // record it clocks.
  const record = WORK_STATUS_TERMINAL_STATES.has(projected.state)
    ? { ...projected, _terminalSince: previousRecord?._terminalSince ?? now }
    : isRestore
      ? { ...projected, _restoredAt: previousRecord?._restoredAt ?? now }
      : previousRecord?._restoredAt !== undefined
        ? { ...projected, _restoredAt: previousRecord._restoredAt }
        : projected;
  const withEntry = { ...workStatus, [key]: record };
  const dropped = [];
  const retained = evictOldestWorkStatus(pruneExpiredWorkStatus(withEntry, now, dropped), key, dropped);
  // A *retained* key is never its own tombstone: it is present and
  // authoritative, so `previous` in the increment guard covers it. The filter
  // must test survival, not identity -- `evictOldestWorkStatus` protects
  // `key`, but `pruneExpiredWorkStatus` does not, and it runs first. When the
  // record just written preserves an already-expired `_terminalSince` (a
  // higher-sequence terminal update arriving after the TTL elapsed), that
  // prune pass drops the key it just wrote. Filtering it out on identity then
  // left neither a record nor a tombstone, so the very next lower-sequence
  // increment resurrected it -- the exact hole this mechanism exists to close
  // (round-4 confirm pass, Logic finding).
  const droppedOthers = dropped.filter(
    ([droppedKey]) => droppedKey !== key || retained[key] === undefined,
  );
  return { workStatus: retained, tombstones: rememberDroppedWorkStatus(tombstones, droppedOthers) };
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
  const restored = (Array.isArray(snapshot.work_status) ? snapshot.work_status : [])
    .map(projectedWorkStatus)
    .filter(Boolean)
    .reduce(
      (acc, item) => {
        const key = workStatusKey(item);
        // `previousRecord` is seeded from the pre-snapshot `state.workStatus`,
        // not from `acc` (the in-progress reduce accumulator). Harmless today
        // since a snapshot carries at most one parent record per key, but
        // would make this non-idempotent under a hypothetical future
        // duplicate-key-within-one-snapshot case.
        const next = upsertWorkStatus(acc.workStatus, key, item, now, (state.workStatus || {})[key], true, acc.tombstones);
        // The snapshot is authoritative for every key it carries, so a
        // tombstone for one of them is superseded: drop it rather than let a
        // pre-snapshot watermark reject a later legitimate increment on a key
        // the server has just re-established.
        const { [key]: _superseded, ...tombstones } = next.tombstones;
        return { workStatus: next.workStatus, tombstones };
      },
      { workStatus: {}, tombstones: state.workStatusTombstones || {} },
    );
  const workStatus = restored.workStatus;
  // The reduce seeds `workStatus` from `{}`, so a pre-snapshot key the
  // snapshot does not carry never passes through `upsertWorkStatus` and so
  // never reaches `rememberDroppedWorkStatus` -- it was dropped with no
  // watermark, and a later lower-sequence increment re-inserted it, which is
  // the same resurrection the prune/evict paths tombstone against (round-4
  // confirm pass, Logic finding). The snapshot stays authoritative about what
  // is *rendered*; the watermark only says "this key has already been seen at
  // or past this sequence", which remains true whatever the server reports.
  // Keys the snapshot *does* carry are excluded here for the same reason the
  // reduce supersedes their tombstones above.
  const preSnapshotDrops = Object.entries(state.workStatus || {}).filter(
    ([key]) => workStatus[key] === undefined,
  );
  const workStatusTombstones = rememberDroppedWorkStatus(restored.tombstones, preSnapshotDrops);
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
    workStatusTombstones,
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
      const workStatus = state.workStatus || {};
      const tombstones = state.workStatusTombstones || {};
      const previous = workStatus[key];
      // The staleness watermark is the live record's sequence when the key is
      // still retained, and the tombstoned sequence once it has been pruned or
      // evicted. Without the second half, a dropped key made `previous`
      // `undefined`, the guard short-circuited, and a late lower-sequence
      // increment resurrected an already-terminal record.
      const lastSequence = previous ? previous.event_sequence : tombstones[key];
      if (lastSequence !== undefined && lastSequence >= projected.event_sequence) return state;
      const upserted = upsertWorkStatus(workStatus, key, projected, undefined, undefined, false, tombstones);
      return { ...state, workStatus: upserted.workStatus, workStatusTombstones: upserted.tombstones };
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
