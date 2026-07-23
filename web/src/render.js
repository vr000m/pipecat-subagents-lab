import { normalizeUrl } from "./state.js";

const escape = (value) => String(value ?? "").replace(/[&<>\"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" })[char]);
const RESULT_PREVIEW_LINE_LIMIT = 5;
const resultResizeObservers = new WeakMap();

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

export function resultContentExceedsLineLimit(scrollHeight, lineHeight) {
  return Number.isFinite(scrollHeight) && Number.isFinite(lineHeight) &&
    scrollHeight > (lineHeight * RESULT_PREVIEW_LINE_LIMIT) + 1;
}

function setResultExpanded(card, toggle, expanded) {
  card.classList.toggle("is-expanded", expanded);
  toggle.setAttribute("aria-expanded", String(expanded));
  toggle.setAttribute("aria-label", expanded ? "Collapse full worker result" : "Expand full worker result");
  toggle.textContent = expanded ? "▾" : "▸";
}

function configureResultCard(card, getComputedStyle) {
  const content = card.querySelector(".result-card-content");
  const toggle = card.querySelector(".result-toggle");
  if (!content || !toggle) return null;
  const update = () => {
    const lineHeight = Number.parseFloat(getComputedStyle(content).lineHeight);
    const collapsible = resultContentExceedsLineLimit(content.scrollHeight, lineHeight);
    if (Number.isFinite(lineHeight)) {
      content.style.setProperty("--result-preview-height", `${lineHeight * RESULT_PREVIEW_LINE_LIMIT}px`);
    }
    card.classList.toggle("is-collapsible", collapsible);
    toggle.hidden = !collapsible;
    if (!collapsible) setResultExpanded(card, toggle, false);
  };
  toggle.addEventListener("click", () => {
    setResultExpanded(card, toggle, !card.classList.contains("is-expanded"));
  });
  update();
  return { content, update };
}

export function enhanceResultCards(root) {
  resultResizeObservers.get(root)?.disconnect();
  resultResizeObservers.delete(root);
  if (typeof root.querySelectorAll !== "function") return;
  const view = root.ownerDocument?.defaultView ?? globalThis;
  if (typeof view.getComputedStyle !== "function") return;
  const configured = [...root.querySelectorAll(".result-card-measured")]
    .map((card) => configureResultCard(card, view.getComputedStyle.bind(view)))
    .filter(Boolean);
  if (configured.length === 0 || typeof view.ResizeObserver !== "function") return;
  const observer = new view.ResizeObserver(() => {
    for (const { update } of configured) update();
  });
  for (const { content } of configured) observer.observe(content);
  resultResizeObservers.set(root, observer);
}

function resultCard(result, state, { collapsible = false } = {}) {
  const speech = Object.values(state.speech || {}).find((item) => item.result_id === result.result_id);
  const deliveryState = speech?.state || "displayed";
  const incomplete = deliveryState !== "delivery_completed";
  const text = result.ui_text || result.text;
  const meta = `result from ${escape(result.worker_id)} · turn ${escape(result.turn_id)}`;
  const body = `<p class="answer ${incomplete ? "unspoken" : ""}"${incomplete ? ' style="font-style: italic"' : ""}>${escape(text)}</p><p class="delivery ${incomplete ? "unspoken incomplete" : ""}">${escape(deliveryState)}${incomplete ? " — speech delivery incomplete or unconfirmed" : " — server transport completed; not proof of browser audibility"}</p><ul class="sources">${links(result.citations || result.sources)}</ul>`;
  if (collapsible) {
    return `<article class="result-card result-card-measured"><div class="result-card-heading"><div class="result-meta">${meta}</div><button class="result-toggle" type="button" aria-expanded="false" aria-label="Expand full worker result" hidden>▸</button></div><div class="result-card-content">${body}</div></article>`;
  }
  return `<article class="result-card"><div class="result-meta">${meta}</div>${body}</article>`;
}

function transcriptTurn(item) {
  const timestamp = item.timestamp;
  return `<div class="turn ${escape(item.role)}"><div class="turn-header"><strong>${escape(item.role)}</strong><time datetime="${escape(timestamp)}" title="${escape(timestamp)}">${escape(formatTimestamp(timestamp))}</time></div><p>${escape(item.text || item.data?.text)}</p></div>`;
}

export function renderRuntime(state) {
  const transcript = (state.transcript || []).map(transcriptTurn).join("");
  const workers = (state.workers || []).map((worker) => {
    const count = (state.results || []).filter((result) => result.worker_id === worker.worker_id).length;
    return `<article class="worker"><strong>${escape(worker.worker_id)}</strong><div class="worker-meta">${escape(worker.topic)} · ${escape(worker.model_policy)} · ${escape(worker.status)}</div>${count ? `<div class="muted">${count} finalized result${count === 1 ? "" : "s"}</div>` : ""}</article>`;
  }).join("");
  const results = (state.results || []).map((result) => `<details><summary>${escape(result.timestamp || "time unknown")} · ${escape(result.worker_id)} · ${escape(result.turn_id)} · ${(result.citations || result.sources || []).filter((item) => normalizeUrl(item?.url)).length} sources · ${escape((result.ui_text || result.text || "").split("\n")[0])}</summary>${resultCard(result, state)}</details>`).join("");
  const diagnostics = state.localDiagnostics?.message || "runtime state below is server-authored";
  return `<main class="app-shell"><div class="local-diagnostics"><strong>LOCAL</strong> ${escape(diagnostics)}</div><div class="live-grid"><section class="panel"><h2>Transcript</h2>${transcript || `<p class="muted">${state.connection === "connected" ? "Listening for server state…" : "Press Connect to start. Microphone permission is requested when you connect."}</p>`}${(state.results || []).slice(-3).map((result) => resultCard(result, state, { collapsible: true })).join("")}</section><section class="inspector"><h2>Router / runtime</h2><p class="route">${escape(state.routing ? `${state.routing.action}${state.routing.worker_id ? ` → ${state.routing.worker_id}` : ""}` : "No active routing decision.")}</p><h2>Workers (persistent)</h2>${workers || `<p class="muted">No server workers yet.</p>`}</section></div><section class="result-log"><h2>Result Log</h2>${results || `<p class="muted">Finalized worker results appear here.</p>`}</section></main>`;
}

export function render(state, root) {
  const html = renderRuntime(state);
  if ("innerHTML" in root) {
    root.innerHTML = html;
    enhanceResultCards(root);
  }
  else root.replaceChildren({ textContent: html, serialize: () => html });
  return root;
}
