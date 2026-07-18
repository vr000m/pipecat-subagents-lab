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

function resultCard(result, state) {
  const speech = Object.values(state.speech || {}).find((item) => item.result_id === result.result_id);
  const deliveryState = speech?.state || "displayed";
  const incomplete = deliveryState !== "delivery_completed";
  return `<article class="result-card"><div class="result-meta">result from ${escape(result.worker_id)} · turn ${escape(result.turn_id)}</div><p class="answer ${incomplete ? "unspoken" : ""}"${incomplete ? ' style="font-style: italic"' : ""}>${escape(result.ui_text || result.text)}</p><p class="delivery ${incomplete ? "unspoken incomplete" : ""}">${escape(deliveryState)}${incomplete ? " — speech delivery incomplete or unconfirmed" : " — server transport completed; not proof of browser audibility"}</p><ul class="sources">${links(result.citations || result.sources)}</ul></article>`;
}

export function renderRuntime(state) {
  const transcript = (state.transcript || []).map((item) => `<div class="turn ${escape(item.role)}"><strong>${escape(item.role)}</strong><p>${escape(item.text || item.data?.text)}</p></div>`).join("");
  const workers = (state.workers || []).map((worker) => {
    const count = (state.results || []).filter((result) => result.worker_id === worker.worker_id).length;
    return `<article class="worker"><strong>${escape(worker.worker_id)}</strong><div class="worker-meta">${escape(worker.topic)} · ${escape(worker.model_policy)} · ${escape(worker.status)}</div>${count ? `<div class="muted">${count} finalized result${count === 1 ? "" : "s"}</div>` : ""}</article>`;
  }).join("");
  const results = (state.results || []).map((result) => `<details><summary>${escape(result.timestamp || "time unknown")} · ${escape(result.worker_id)} · ${escape(result.turn_id)} · ${(result.citations || result.sources || []).filter((item) => normalizeUrl(item?.url)).length} sources · ${escape((result.ui_text || result.text || "").split("\n")[0])}</summary>${resultCard(result, state)}</details>`).join("");
  const diagnostics = state.localDiagnostics?.message || "runtime state below is server-authored";
  return `<main class="app-shell"><div class="local-diagnostics"><strong>LOCAL</strong> ${escape(diagnostics)}</div><div class="live-grid"><section class="panel"><h2>Transcript</h2>${transcript || `<p class="muted">${state.connection === "connected" ? "Listening for server state…" : "Press Connect to start. Microphone capture begins only after you enable it."}</p>`}${(state.results || []).slice(-3).map((result) => resultCard(result, state)).join("")}</section><section class="inspector"><h2>Router / runtime</h2><p class="route">${escape(state.routing ? `${state.routing.action}${state.routing.worker_id ? ` → ${state.routing.worker_id}` : ""}` : "No active routing decision.")}</p><h2>Workers (persistent)</h2>${workers || `<p class="muted">No server workers yet.</p>`}</section></div><section class="result-log"><h2>Result Log</h2>${results || `<p class="muted">Finalized worker results appear here.</p>`}</section></main>`;
}

export function render(state, root) {
  const html = renderRuntime(state);
  if ("innerHTML" in root) root.innerHTML = html;
  else root.replaceChildren({ textContent: html, serialize: () => html });
  return root;
}
