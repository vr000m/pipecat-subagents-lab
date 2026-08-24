import { normalizeUrl } from "./state.js";

const escape = (value) => String(value ?? "").replace(/[&<>\"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" })[char]);

function links(citations = []) {
  return citations
    .map((citation) => {
      const url = normalizeUrl(citation?.url);
      return url ? `<li><a href="${escape(url)}" target="_blank" rel="noopener noreferrer">${escape(citation.title || url)}</a></li>` : "";
    })
    .join("");
}

export function formatTimestamp(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "time unknown";
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(date);
}

function deliveryFor(result, state) {
  const speech = Object.values(state.speech || {}).find((item) => item.result_id === result.result_id);
  const deliveryState = speech?.state || "displayed";
  return { deliveryState, incomplete: deliveryState !== "delivery_completed" };
}

function resultBody(result, state, { styleAnswerForDelivery = true } = {}) {
  const { deliveryState, incomplete } = deliveryFor(result, state);
  const answerIncomplete = styleAnswerForDelivery && incomplete;
  const answerClass = answerIncomplete ? "answer unspoken" : "answer";
  const text = result.ui_text || result.text;
  return `<p class="${answerClass}">${escape(text)}</p><p class="delivery ${incomplete ? "unspoken incomplete" : ""}">${escape(deliveryState)}${incomplete ? " — speech delivery incomplete or unconfirmed" : " — server transport completed; not proof of browser audibility"}</p><ul class="sources">${links(result.citations || result.sources)}</ul>`;
}

function resultCard(result, state) {
  const meta = `result from ${escape(result.worker_id)} · turn ${escape(result.turn_id)}`;
  return `<article class="result-card"><div class="result-meta">${meta}</div>${resultBody(result, state)}</article>`;
}

function matchingResult(item, results = []) {
  if (item.role !== "assistant") return null;
  const text = item.text || item.data?.text;
  for (let index = results.length - 1; index >= 0; index -= 1) {
    const result = results[index];
    if (result.turn_id === item.turn_id && (result.ui_text || result.text) === text) return result;
  }
  return null;
}

function projectionLabel(result, state) {
  const { deliveryState } = deliveryFor(result, state);
  const displayOnly = deliveryState === "displayed";
  const label = displayOnly ? "background result" : "TTS";
  const className = displayOnly ? "projection-label background" : "projection-label";
  return ` <span class="${className}">${label}</span>`;
}

function transcriptTurn(item, state) {
  const timestamp = item.timestamp;
  const result = matchingResult(item, state.results);
  const { incomplete = false } = result ? deliveryFor(result, state) : {};
  const text = result?.spoken_text || item.text || item.data?.text;
  const projection = result ? projectionLabel(result, state) : "";
  const hasStructuredDetails = result && (
    result.worker_id !== "main" ||
    result.spoken_text !== (result.ui_text || result.text) ||
    (result.citations || result.sources || []).length > 0
  );
  const details = hasStructuredDetails
    ? `<details class="turn-details"><summary>Subagent output · ${escape(result.worker_id)} · ${escape(result.turn_id)}</summary><div class="turn-details-content">${resultBody(result, state, { styleAnswerForDelivery: false })}</div></details>`
    : "";
  return `<div class="turn ${escape(item.role)}"><div class="turn-header"><strong>${escape(item.role)}${projection}</strong><time datetime="${escape(timestamp)}" title="${escape(timestamp)}">${escape(formatTimestamp(timestamp))}</time></div><p class="${result && incomplete ? "spoken-text unspoken" : "spoken-text"}">${escape(text)}</p>${details}</div>`;
}

function workStatusLog(state) {
  const entries = Object.entries(state.workStatus || {});
  if (entries.length === 0) return "";
  // Epoch is part of the record's identity (see state.js's workStatusKey):
  // after a reconnect, one turn_id can have two records at different
  // epochs. Rendering the epoch keeps those rows visually distinguishable
  // instead of looking like duplicate/contradictory entries for one turn.
  const items = entries
    .map(([key, item]) => `<li class="work-status ${escape(item.state)}" data-work-status-key="${escape(key)}">${escape(item.turn_id)} · epoch ${escape(item.origin_epoch ?? "unknown")} · ${escape(item.state)}${item.terminal_reason ? ` (${escape(item.terminal_reason)})` : ""}</li>`)
    .join("");
  return `<h2>Work status</h2><ul class="work-status-log">${items}</ul>`;
}

export function renderRuntime(state) {
  const transcript = (state.transcript || []).map((item) => transcriptTurn(item, state)).join("");
  const workers = (state.workers || []).map((worker) => {
    const count = (state.results || []).filter((result) => result.worker_id === worker.worker_id).length;
    return `<article class="worker"><strong>${escape(worker.worker_id)}</strong><div class="worker-meta">${escape(worker.topic)} · ${escape(worker.model_policy)} · ${escape(worker.status)}</div>${count ? `<div class="muted">${count} finalized result${count === 1 ? "" : "s"}</div>` : ""}</article>`;
  }).join("");
  const results = (state.results || []).map((result) => `<details><summary>${escape(result.timestamp || "time unknown")} · ${escape(result.worker_id)} · ${escape(result.turn_id)} · ${(result.citations || result.sources || []).filter((item) => normalizeUrl(item?.url)).length} sources · ${escape((result.ui_text || result.text || "").split("\n")[0])}</summary>${resultCard(result, state)}</details>`).join("");
  const diagnostics = state.localDiagnostics?.message || "runtime state below is server-authored";
  return `<main class="app-shell"><div class="local-diagnostics"><strong>LOCAL</strong> ${escape(diagnostics)}</div><div class="live-grid"><section class="panel"><h2>Transcript</h2>${transcript || `<p class="muted">${state.connection === "connected" ? "Listening for server state…" : "Press Connect to start. Microphone permission is requested when you connect."}</p>`}</section><section class="inspector"><h2>Router / runtime</h2><p class="route">${escape(state.routing ? `${state.routing.action}${state.routing.worker_id ? ` → ${state.routing.worker_id}` : ""}` : "No active routing decision.")}</p><h2>Workers (persistent)</h2>${workers || `<p class="muted">No server workers yet.</p>`}${workStatusLog(state)}</section></div><section class="result-log"><h2>Result Log</h2>${results || `<p class="muted">Finalized worker results appear here.</p>`}</section></main>`;
}

export function render(state, root) {
  const html = renderRuntime(state);
  if ("innerHTML" in root) root.innerHTML = html;
  else root.replaceChildren({ textContent: html, serialize: () => html });
  return root;
}
