import test from "node:test";
import assert from "node:assert/strict";
import { registerLunchWorkflow } from "../src/lunchWorkflow.js";

function fixture() {
  const routes = {};
  const calls = [];
  const session = { inspector: { name: "Original", branch: "Ennis" }, sessionRecord: { id: 7 }, selection: { productionNumber: "WO-1", pipeNumber: "3", inspectionScope: "standard" }, activeInspection: { attempt_id: 30 }, breakCsrf: "token" };
  registerLunchWorkflow({ post: (path, fn) => { routes[`POST ${path}`] = fn; }, get: (path, fn) => { routes[`GET ${path}`] = fn; } }, async (action, args) => {
    if (action === "inspection_break_state") return {mode:null};
    calls.push({ action, args });
    if (action === "begin_lunch_inspection") return { draft: { id: "draft", snapshot: { pipe_number: "2" } }, records: [] };
    return { id: "draft", result: "pass" };
  }, () => "2.875 7.90 BTS-6 PIN");
  return { session, calls, async request(method, path, body = {}) {
    const response = { code: 200, status(code) { this.code = code; return this; }, json(data) { this.data = data; } };
    await routes[`${method} ${path}`]({ session, body, params: { id: "draft" } }, response);
    return response;
  } };
}

test("opening lunch uses session/WO/connection and never changes current pipe", async () => {
  const f = fixture();
  const before = JSON.stringify([f.session.selection, f.session.activeInspection, f.session.inspector]);
  const result = await f.request("POST", "/workflow/lunch/open", { csrf: "token" });
  assert.equal(result.data.draft.snapshot.pipe_number, "2");
  assert.equal(f.calls[0].args.params.session_id, 7);
  assert.equal(f.calls[0].args.params.production_number, "WO-1");
  assert.equal(before, JSON.stringify([f.session.selection, f.session.activeInspection, f.session.inspector]));
});

test("saving uses a separate lunch action and cannot replace original inspector or production measurements", async () => {
  const f = fixture();
  await f.request("POST", "/workflow/lunch/open", { csrf: "token" });
  const result = await f.request("POST", "/workflow/lunch/:id", { csrf: f.session.lunchCsrf, adp: "002", values: { "1": ".050" } });
  assert.equal(result.code, 200);
  assert.equal(f.calls.at(-1).action, "save_lunch_inspection");
  assert.equal(f.session.inspector.name, "Original");
  assert.equal(f.session.selection.pipeNumber, "3");
  assert.equal(f.session.activeInspection.attempt_id, 30);
});

test("CSRF, login, and paused breaks gate lunch writes", async () => {
  const f = fixture();
  assert.equal((await f.request("POST", "/workflow/lunch/open", { csrf: "bad" })).code, 403);
  f.session.inspectionBreak = { relief: null };
  assert.equal((await f.request("POST", "/workflow/lunch/open", { csrf: "token" })).code, 409);
  f.session.inspector = null;
  assert.equal((await f.request("GET", "/workflow/lunch/:id")).code, 401);
  assert.equal(f.calls.length, 0);
});
