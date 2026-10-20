import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import vm from "node:vm";
import { canEditInspectionHistory, createHistoryEditGuard } from "../src/inspectionPermissions.js";

test("only the four complete ADPJobTitleDesc values grant history access", () => {
  for (const title of ["Director of Quality", "Quality Control Specialist", "Quality Manager", "Quality Supervisor", " quality manager "]) {
    assert.equal(canEditInspectionHistory({ adp_job_title_desc: title }), true, title);
  }
  for (const title of ["", "IT", "Administrator", "Manager", "Supervisor", "CNC Supervisor", "Quality", "Assistant Quality Manager", "Quality Manager Trainee", "Quality Control Inspector"]) {
    assert.equal(canEditInspectionHistory({ adp_job_title_desc: title, role_title: "Quality Manager", canAccessAdmin: true }), false, title);
  }
  assert.equal(canEditInspectionHistory({ role_title: "Quality Manager" }), false);
  assert.equal(canEditInspectionHistory(null), false);
});

function response() {
  return { set() {}, status(code) { this.statusCode = code; return this; }, send(body) { this.body = body; }, redirect(url) { this.url = url; } };
}

test("history authorization uses a fresh employee lookup and ignores cached admin roles and posted titles", async () => {
  const guard = createHistoryEditGuard(async (action, payload) => {
    assert.equal(action, "get_employee_by_adp");
    assert.equal(payload.adp_number, "TEST");
    return { adp_job_title_desc: "IT" };
  });
  const res = response();
  await guard({ session: { inspector: { adp_number: "TEST", adp_job_title_desc: "Quality Manager" }, sessionRecord: { id: 1 }, canAccessAdmin: true },
    body: { adp_job_title_desc: "Quality Manager" } }, res, () => { throw new Error("Access must be denied"); });
  assert.equal(res.statusCode, 403);
});

test("authorized active employees proceed; missing employees and lookup failures deny access", async () => {
  const req = { session: { inspector: { adp_number: "TEST" }, sessionRecord: { id: 1 } } };
  let allowed = false;
  await createHistoryEditGuard(async () => ({ adp_job_title_desc: "Quality Supervisor" }))(req, response(), () => { allowed = true; });
  assert.ok(allowed);
  const res = response();
  await createHistoryEditGuard(async () => null)(req, res, () => { throw new Error("Access must be denied"); });
  assert.equal(res.statusCode, 403);
  let error;
  await createHistoryEditGuard(async () => { throw new Error("Lookup unavailable"); })(req, response(), (err) => { error = err; });
  assert.equal(error.message, "Lookup unavailable");
  const unauthenticated = response();
  await createHistoryEditGuard(async () => { throw new Error("Must not look up an anonymous user"); })({ session: {} }, unauthenticated, () => {});
  assert.equal(unauthenticated.url, "/");
});

test("direct GET, forged edit POST, and delete POST routes reject unauthorized users before touching records", async () => {
  const source = await readFile(new URL("../src/server.js", import.meta.url), "utf8");
  const req = { session: { inspector: { adp_number: "TEST" }, sessionRecord: { id: 1 } }, body: {}, params: { pipeUnitId: "1" } };
  for (const [method, url] of [["get", "/workflow/history/edit/:pipeUnitId"], ["post", "/workflow/history/edit/:pipeUnitId"], ["post", "/workflow/history/delete"]]) {
    const start = source.indexOf(`app.${method}("${url}",`);
    const end = source.indexOf("\napp.", start + 1);
    let handlers;
    vm.runInNewContext(source.slice(start, end), {
      app: { [method]: (_, ...args) => { handlers = args; } },
      requireHistoryEditor: createHistoryEditGuard(async () => ({ adp_job_title_desc: "Inspector" })),
      callBridge: () => { throw new Error("Must not touch saved records"); },
    });
    assert.equal(handlers.length, 2);
    const res = response();
    await handlers[0](req, res, () => handlers[1](req, res, (err) => { throw err; }));
    assert.equal(res.statusCode, 403, `${method} ${url}`);
  }
});
