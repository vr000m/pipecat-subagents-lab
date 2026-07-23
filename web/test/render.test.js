import { expect, test } from "bun:test";

import { formatTimestamp, renderRuntime } from "../src/render.js";

const state = {
  connection: "connected",
  routing: { action: "existing_worker", worker_id: "worker-weather" },
  workers: [{ worker_id: "worker-weather", topic: "weather", model_policy: "deep", status: "idle" }],
  transcript: [],
  results: [{
    result_id: "result-1",
    worker_id: "worker-weather",
    turn_id: "turn-1",
    timestamp: "2026-07-18T10:00:00Z",
    ui_text: "Complete answer text, available before speech finishes.",
    spoken_text: "Concise spoken projection.",
    citations: [{ title: "Valid source", url: "https://example.com/source" }],
  }],
  speech: { "utterance-1": {
    utterance_id: "utterance-1",
    result_id: "result-1",
    state: "synthesis_ended",
  } },
  localDiagnostics: { message: "LOCAL: transport SmallWebRTC" },
};

test("renders server state and makes complete text versus incomplete speech visible", () => {
  const html = renderRuntime(state);

  expect(html).toContain("worker-weather");
  expect(html).toContain("existing_worker");
  expect(html).toContain("Complete answer text, available before speech finishes.");
  expect(html).toContain("synthesis_ended");
  expect(html).toMatch(/unspoken|incomplete|italic/);
  expect(html).toContain("LOCAL");
  expect(html).not.toContain("verified audible");
});

test("renders a separate persistent result log with worker, turn, timestamp, and source count", () => {
  const html = renderRuntime(state);

  expect(html).toContain("result-log");
  expect(html).toContain("2026-07-18T10:00:00Z");
  expect(html).toContain("turn-1");
  expect(html).toContain("1 source");
  expect(html).toContain("<details>");
});

test("renders server-authored timestamps beside user and assistant transcript messages", () => {
  const userTimestamp = "2026-07-23T11:31:23Z";
  const assistantTimestamp = "2026-07-23T11:31:47Z";
  const html = renderRuntime({
    ...state,
    transcript: [
      { role: "user", text: "What is the forecast?", turn_id: "turn-1", timestamp: userTimestamp },
      { role: "assistant", text: "Mostly cloudy.", turn_id: "turn-1", timestamp: assistantTimestamp },
    ],
  });

  expect(html).toContain(`<time datetime="${userTimestamp}" title="${userTimestamp}">${formatTimestamp(userTimestamp)}</time>`);
  expect(html).toContain(`<time datetime="${assistantTimestamp}" title="${assistantTimestamp}">${formatTimestamp(assistantTimestamp)}</time>`);
  expect(html).toContain('<div class="turn user">');
  expect(html).toContain('<div class="turn assistant">');
});

test("shows the spoken projection and nests the full structured result under a caret", () => {
  const html = renderRuntime({
    ...state,
    transcript: [{
      role: "assistant",
      text: state.results[0].ui_text,
      turn_id: "turn-1",
      timestamp: "2026-07-18T10:00:01Z",
    }],
  });

  expect(html).toContain('<span class="projection-label">TTS</span>');
  expect(html).toContain('<p class="spoken-text unspoken">Concise spoken projection.</p>');
  expect(html).toContain('<details class="turn-details">');
  expect(html).toContain("<summary>Subagent output · worker-weather · turn-1</summary>");
  expect(html).toContain('<div class="turn-details-content"><p class="answer">');
  expect(html).toContain("Complete answer text, available before speech finishes.");
  expect(html).toContain("synthesis_ended — speech delivery incomplete or unconfirmed");
  expect(html).not.toContain("result-card-measured");
});

test("leaves unmatched transcript messages unchanged without claiming a TTS projection", () => {
  const html = renderRuntime({
    ...state,
    transcript: [{
      role: "assistant",
      text: "A transient response without a committed result.",
      turn_id: "turn-pending",
      timestamp: "2026-07-18T10:00:01Z",
    }],
  });

  expect(html).toContain("A transient response without a committed result.");
  expect(html).not.toContain("projection-label");
  expect(html).not.toContain("turn-details");
});

test("does not invent subagent details for a main response with no separate projection", () => {
  const mainResult = {
    ...state.results[0],
    result_id: "result-main",
    worker_id: "main",
    ui_text: "Please clarify your request.",
    spoken_text: "Please clarify your request.",
    citations: [],
  };
  const html = renderRuntime({
    ...state,
    results: [mainResult],
    transcript: [{
      role: "assistant",
      text: mainResult.ui_text,
      turn_id: "turn-1",
      timestamp: "2026-07-18T10:00:01Z",
    }],
  });

  expect(html).toContain('<span class="projection-label">TTS</span>');
  expect(html).toContain("Please clarify your request.");
  expect(html).not.toContain("Subagent output");
});

test("does not style server-transport-completed text as incomplete or claim browser audibility", () => {
  const html = renderRuntime({
    ...state,
    speech: { "utterance-1": { result_id: "result-1", state: "delivery_completed" } },
  });

  expect(html).toContain("delivery_completed");
  expect(html).toContain("server transport completed; not proof of browser audibility");
  expect(html).not.toContain("speech delivery incomplete or unconfirmed");
});

test("renders every external source link with safe new-tab attributes", () => {
  const html = renderRuntime(state);

  expect(html).toContain('href="https://example.com/source"');
  expect(html).toContain('target="_blank"');
  expect(html).toContain('rel="noopener noreferrer"');
  const links = [...html.matchAll(/<a href="([^"]+)"[^>]*>/g)];
  expect(links).toHaveLength(1);
  for (const match of links) {
    const [, href] = match;
    const start = html.lastIndexOf("<a ", match.index);
    const tag = html.slice(start, html.indexOf(">", start) + 1);
    expect(href).toMatch(/^https?:\/\//);
    expect(tag).toContain('target="_blank"');
    expect(tag).toContain("noopener");
    expect(tag).toContain("noreferrer");
  }
});

test("does not expose raw logs, prompts, or full worker context", () => {
  const html = renderRuntime({ ...state, raw_logs: "server diagnostic log", prompt: "private prompt", context: "private context" });

  expect(html).not.toContain("server diagnostic log");
  expect(html).not.toContain("private prompt");
  expect(html).not.toContain("private context");
});

test("renders the full cross-worker history separately from the live result projection", () => {
  const history = [
    { ...state.results[0], result_id: "result-1", worker_id: "worker-weather", turn_id: "turn-1", timestamp: "2026-07-18T10:00:00Z" },
    { ...state.results[0], result_id: "result-2", worker_id: "worker-news", turn_id: "turn-2", timestamp: "2026-07-18T10:01:00Z", ui_text: "Second finalized answer", citations: [] },
    { ...state.results[0], result_id: "result-3", worker_id: "worker-weather", turn_id: "turn-3", timestamp: "2026-07-18T10:02:00Z", ui_text: "Third finalized answer", citations: [] },
    { ...state.results[0], result_id: "result-4", worker_id: "worker-news", turn_id: "turn-4", timestamp: "2026-07-18T10:03:00Z", ui_text: "Fourth finalized answer", citations: [] },
  ];
  const html = renderRuntime({
    ...state,
    workers: [
      ...state.workers,
      { worker_id: "worker-news", topic: "news", model_policy: "fast", status: "idle", prompt: "private" },
    ],
    results: history,
  });

  expect((html.match(/<details>/g) || []).length).toBe(4);
  expect(html).toContain("worker-news");
  expect(html).toContain("2026-07-18T10:03:00Z");
  expect(html).toContain("Fourth finalized answer");
  expect(html).not.toContain("private");
});

test("worker cards expose only the approved inspection fields", () => {
  const html = renderRuntime({
    ...state,
    workers: [{
      worker_id: "worker-private",
      topic: "private topic",
      model_policy: "deep",
      status: "idle",
      prompt: "do not render this prompt",
      context: "do not render this context",
      raw_logs: "do not render these logs",
    }],
    results: [],
  });

  expect(html).toContain("worker-private");
  expect(html).toContain("private topic");
  expect(html).toContain("deep");
  expect(html).toContain("idle");
  expect(html).not.toContain("do not render this prompt");
  expect(html).not.toContain("do not render this context");
  expect(html).not.toContain("do not render these logs");
});

test("uses conservative incomplete styling for every non-transport-complete delivery state", () => {
  for (const deliveryState of ["synthesis_ended", "delivery_unknown", "interrupted", "interrupted_by_reconnect", "paused", "resumed"]) {
    const html = renderRuntime({
      ...state,
      speech: { "utterance-1": { result_id: "result-1", state: deliveryState } },
    });

    expect(html).toContain(`>${deliveryState} — speech delivery incomplete or unconfirmed</p>`);
    expect(html).toContain('class="answer unspoken"');
    expect(html).not.toContain("verified browser audibility");
  }
});
