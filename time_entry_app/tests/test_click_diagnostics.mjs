import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, readFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import vm from "node:vm";
import express from "express";
import { createClickDiagnostics, requireIT } from "../src/clickDiagnostics.js";

test("diagnostics requires IT even when an approver knows the direct URL", () => {
  for (const roles of [null, [], ["operator"], ["approver"], ["operator", "exporter", "approver"], ["it"]]) {
    let allowed = false;
    const res = { set() {}, redirect(target) { this.redirected = target; },
      status(code) { this.statusCode = code; return this; }, send() {} };
    requireIT({ session: roles === null ? {} : { cncUser: { roles } } }, res, () => { allowed = true; });
    assert.equal(allowed, roles?.includes("it") || false);
    if (roles === null) assert.equal(res.redirected, "/");
    else if (!roles.includes("it")) assert.equal(res.statusCode, 403);
  }
});

test("client events correlate with server requests; warnings survive redirects; values are excluded", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "cnc-click-test-"));
  const diagnostics = createClickDiagnostics(directory);
  const app = express();
  app.use(express.json());
  app.use((req, res, next) => { req.session = { cncUser: { employee: { emp_id: "TEST" }, machine_no: "10" } }; next(); });
  app.post("/diagnostics/events", diagnostics.ingest);
  app.use(diagnostics.middleware);
  app.post("/time/start", (req, res) => {
    req.session.notice = { kind: "warning", message: "Private failure detail" };
    res.redirect("/time");
  });
  const server = app.listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  try {
    const event = { id: "event-1", kind: "click", interaction: "press-1", control: "Start Entry",
      page: "/time?secret=hidden", action: "/time/start?token=hidden", value: "private" };
    const send = (events, headers = {}) => fetch(`${base}/diagnostics/events`, {
      method: "POST", headers: { "Content-Type": "application/json", ...headers }, body: JSON.stringify({ events }) });
    assert.equal((await send([event])).status, 204);
    assert.equal((await send([event])).status, 204);
    assert.equal((await send([event], { "sec-fetch-site": "cross-site" })).status, 403);
    assert.equal((await send(Array(51).fill(event))).status, 400);
    const response = await fetch(`${base}/time/start`, { method: "POST", redirect: "manual",
      headers: { "Content-Type": "application/json" }, body: JSON.stringify({ _click_interaction: "press-1", comments: "private" }) });
    assert.equal(response.status, 302);
    const rows = await diagnostics.read();
    assert.equal(rows.filter((row) => row.kind === "click").length, 1);
    assert.equal(rows[0].page, "/time");
    assert.equal(rows[0].action, "/time/start");
    const received = rows.find((row) => row.kind === "server_received");
    const finished = rows.find((row) => row.kind === "server_response");
    assert.equal(received.interaction, "press-1");
    assert.equal(received.request, finished.request);
    assert.equal(finished.outcome, "warning");
    assert.equal(finished.status, 302);
    assert.ok(finished.duration_ms >= 0);
    assert.ok(!JSON.stringify(rows).includes("private"));
    assert.ok(!JSON.stringify(rows).includes("hidden"));
  } finally {
    await new Promise((resolve) => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
});

test("browser capture records disabled presses, validation, cancellation, keyboard clicks, and submission IDs", async () => {
  const documentHandlers = {};
  const windowHandlers = {};
  const store = new Map();
  const batches = [];
  class Element {
    constructor(tagName = "BUTTON") { this.tagName = tagName; this.id = "start"; this.textContent = "Start Entry"; }
    closest(selector) { return selector.startsWith(":disabled") ? (this.disabled ? this : null) : this; }
  }
  class Input extends Element {}
  const form = { action: "http://local/time/start", querySelector: () => form.field,
    appendChild: (field) => { form.field = field; } };
  const button = new Element();
  button.form = form;
  const field = new Input("INPUT");
  field.id = "quantity";
  field.value = "PRIVATE VALUE";
  field.form = form;
  let sequence = 0;
  const context = {
    Element, HTMLInputElement: Input, URL, Date, Math, Set, JSON, Boolean,
    crypto: { randomUUID: () => `id-${++sequence}` }, location: { pathname: "/time" },
    sessionStorage: { getItem: (key) => store.get(key), setItem: (key, value) => store.set(key, value) },
    document: { addEventListener: (kind, handler) => { documentHandlers[kind] = handler; }, createElement: () => new Input(), body: new Element("BODY") },
    window: { addEventListener: (kind, handler) => { windowHandlers[kind] = handler; }, setInterval: () => {} },
    queueMicrotask,
    fetch: async (url, options) => { batches.push(...JSON.parse(options.body).events); return { ok: true }; },
  };
  vm.runInNewContext(await readFile(new URL("../public/click-diagnostics.js", import.meta.url), "utf8"), context);
  button.disabled = true;
  documentHandlers.pointerdown({ target: button });
  await new Promise(setImmediate);
  button.disabled = false;
  documentHandlers.pointerdown({ target: button });
  documentHandlers.click({ target: button, detail: 1 });
  documentHandlers.invalid({ target: field });
  const submission = { target: form, submitter: button, defaultPrevented: true };
  documentHandlers.submit(submission);
  await new Promise(setImmediate);
  await windowHandlers.online();
  const click = batches.find((row) => row.kind === "click");
  assert.ok(batches.some((row) => row.kind === "pointerdown" && row.disabled));
  assert.equal(batches.find((row) => row.kind === "invalid").interaction, click.interaction);
  assert.equal(batches.find((row) => row.kind === "submit").interaction, form.field.value);
  assert.ok(batches.some((row) => row.kind === "submit_cancelled"));
  assert.ok(!JSON.stringify(batches).includes("PRIVATE VALUE"));
  documentHandlers.click({ target: button, detail: 0 });
  await new Promise(setImmediate);
  assert.notEqual(batches.at(-1).interaction, click.interaction);
});
