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
    localDiagnostics: { ...EMPTY_DIAGNOSTICS },
    lastAppliedSequence: 0,
    serverState: false,
    connectionEpoch: null,
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
    ui_text: result.ui_text,
    spoken_result_id: result.spoken_result_id,
    ui_result_id: result.ui_result_id,
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
  const diagnostics = Array.isArray(state.localDiagnostics)
    ? state.localDiagnostics
    : {
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
    routing: snapshot.routing ?? null,
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
    case "runtime_result":
    case "result":
      return addResult(state, payload.data?.result ?? payload.data ?? payload.result ?? payload);
    case "speech":
    case "speech_progress": {
      const progress = payload.data?.progress ?? payload.data ?? payload.progress ?? payload;
      const projected = projectedSpeech(progress);
      if (!projected) return state;
      return { ...state, speech: { ...state.speech, [projected.utterance_id]: projected } };
    }
    case "worker": {
      const worker = payload.data?.worker ?? payload.data ?? payload.worker ?? payload;
      const projected = projectedWorker(worker);
      if (!projected) return state;
      return { ...state, workers: [...state.workers.filter((item) => item.worker_id !== projected.worker_id), projected] };
    }
    case "routing":
      return { ...state, routing: payload.data?.routing ?? payload.data ?? payload.routing ?? payload };
    case "user_transcript":
    case "bot_transcript":
      return {
        ...state,
        transcript: [...state.transcript, { ...(payload.data ?? payload), role: payload.kind === "user_transcript" ? "user" : "assistant" }],
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
  const snapshot = kind === "runtime_snapshot" ? message.data ?? message.snapshot : null;
  if (!snapshot && state.sessionId && message.session_id !== state.sessionId) return state;
  if (!snapshot && state.connectionEpoch !== null && message.origin_epoch !== state.connectionEpoch) return state;
  if (snapshot) {
    const snapshotSequence = Number(snapshot.snapshot_sequence ?? sequence);
    if (snapshotSequence < Math.max(state.lastAppliedSequence, state.localDiagnostics.lastSnapshotSequence)) return state;
    return snapshotState(state, snapshot, sequence);
  }
  if (!sequence || sequence <= state.lastAppliedSequence) return state;
  if (state.localDiagnostics.snapshotRequestPending) return state;
  if (sequence < state.localDiagnostics.lastSnapshotSequence) return state;
  const expected = state.lastAppliedSequence + 1;
  if (state.lastAppliedSequence && sequence !== expected) {
    if (!state.localDiagnostics.snapshotRequestPending) requestSnapshot();
    return {
      ...state,
      snapshotRequestPending: true,
      snapshotRequestCount: (state.snapshotRequestCount ?? 0) + (state.localDiagnostics.snapshotRequestPending ? 0 : 1),
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
    snapshotRequestPending: false,
    localDiagnostics: { ...next.localDiagnostics, lastSequence: sequence, lastAppliedSequence: sequence, snapshotRequestPending: false },
  };
}

export const applyMessage = applyServerMessage;
export const replaceFromSnapshot = (state, snapshotMessage) => applyServerMessage(state, snapshotMessage);
