import test from "node:test";
import assert from "node:assert/strict";
import { registerAdminInspections } from "../src/adminInspections.js";

function fixture() {
  const routes = {};
  const calls = [];
  const session = { inspector: { adp_number: "ADMIN" }, sessionRecord: { id: 7 }, activeInspection: { attempt_id: 3 }, breakCsrf: "csrf" };
  let admin = true;
  registerAdminInspections({ get:(path,fn)=>routes[`GET ${path}`]=fn, post:(path,fn)=>routes[`POST ${path}`]=fn }, async (action, args) => {
    calls.push({ action, args });
    if (action === "get_employee_by_adp") return {};
    if (action === "is_admin_user") return admin;
    if (!admin && action.startsWith("get_admin")) throw Error("Admin access required.");
    return { live: [], lunches: [] };
  }, (_req, html) => html);
  return { session, calls, revoke() { admin = false; }, async request(method, path, body = {}) {
    const response = { code:200, status(code){this.code=code;return this;}, send(value){this.body=value;}, json(value){this.body=value;}, redirect(url){this.url=url;}, setHeader(){} };
    await routes[`${method} ${path}`]({ session, body, query: {}, params: { id:"record" } }, response, error=>{throw error;});
    return response;
  } };
}

test("admins can open monitoring without a floor session", async () => {
  const f=fixture();f.session.sessionRecord=null;
  const response=await f.request("GET","/admin/inspections");
  assert.equal(response.code,200);
  assert.match(response.body,/Ongoing Local Inspections/);
  assert.match(response.body,/Lunch Inspections/);
});

test("monitor and lunch details use fresh backend admin authorization", async () => {
  const f=fixture();f.revoke();
  assert.equal((await f.request("GET","/admin/inspections/data")).code,403);
  assert.equal((await f.request("GET","/admin/inspections/lunch/:id")).code,403);
});

test("draft writes use server-owned attempt and session IDs", async () => {
  const f=fixture();
  assert.equal((await f.request("POST","/workflow/live-draft",{csrf:"csrf",attempt_id:999,readings:{"1":".05"}})).code,409);
  assert.equal((await f.request("POST","/workflow/live-draft",{csrf:"bad",attempt_id:3,readings:{"1":".05"}})).code,403);
  assert.equal(f.calls.length,0);
  await f.request("POST","/workflow/live-draft",{csrf:"csrf",attempt_id:3,session_id:999,readings:{"1":".05"}});
  assert.equal(f.calls[0].action,"save_live_draft");
  assert.equal(f.calls[0].args.params.session_id,7);
});
