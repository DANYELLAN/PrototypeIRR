import test from "node:test";
import assert from "node:assert/strict";
import express from "express";
import { registerPipeReportRoutes, renderPipeReport, buildPipeReportGrids } from "../src/pipeReport.js";
import { canViewPipeReport } from "../src/inspectionPermissions.js";

test("report access covers Quality admins and IT without widening saved-history editing", () => {
  for (const title of ["Director of Quality", "Quality Control Specialist", "Quality Manager", "Quality Supervisor", "IT", "IT Manager", "Integration Technologist"]) assert.ok(canViewPipeReport({ adp_job_title_desc: title }));
  for (const employee of [null, {}, { adp_job_title_desc: "Quality Inspector" }, { role_title: "Manager" }, { adp_job_title_desc: "IT", employee_status: "Inactive" }]) assert.equal(canViewPipeReport(employee), false);
});

const sample = (status = "passed") => ({ stations: [], pipes: [{ attempt_id: 1, pipe_unit_id: 1, pipe_number: "001", status, attempt_no: 1,
  inspector_name: '<script>alert("x")</script>', measurements: [{ element_description: "Diameter", measured_value: "1.234", pass_fail: "Pass" }], snapshot_hash: "hash" }], page: 1, pages: 2, total: 25 });

test("report renders navigation, escapes source values, and only offers verification for submitted inspections", () => {
  const html = renderPipeReport(sample(), "2");
  assert.match(html, /Next 20 pipes →/);
  assert.match(html, /page=2&amp;station=2|page=2&station=2/);
  assert.match(html, /Verify Inspection/);
  assert.doesNotMatch(html, /<script>alert/);
  assert.match(html, /&lt;script&gt;/);
  assert.doesNotMatch(renderPipeReport(sample("in_progress")), /data-verify-inspection/);
  const verified = sample();
  verified.pipes[0].verification = { verifier_name: "Reviewer", verifier_title: "Quality Manager", verified_at: "2026-10-19T12:00:00Z" };
  assert.match(renderPipeReport(verified), /Verified by Reviewer/);
  assert.doesNotMatch(renderPipeReport(verified), /data-verify-inspection/);
  assert.match(html, /inspection-sheet-table pipe-history-sheet-table pipe-report-grid/);
  assert.match(html, /scope="col">DWG DIM/);
  assert.doesNotMatch(html, /<details/);
});

test("worksheet grids align pipe values by element and keep different stations, recipes, and specifications separate", () => {
  const element = { element_sequence: 1, element_description: "Diameter", dwg_dim: "1.230–1.240", gauge: "Micrometer" };
  const first = { ...sample().pipes[0], location_id: 1, production_number: "WO1", recipe_name: "IRR", operation_description: "BOX", measurements: [{ ...element, measured_value: "1.234", pass_fail: "Pass" }] };
  const second = { ...first, attempt_id: 2, pipe_number: "002", measurements: [{ ...element, measured_value: "1.239", pass_fail: "Pass" }] };
  const grids = buildPipeReportGrids([first, second, { ...first, attempt_id: 3, location_id: 2 }, { ...first, attempt_id: 4, recipe_name: "Other IRR" }]);
  assert.equal(grids.length, 3);
  assert.equal(grids[0].columns.length, 2);
  assert.equal(grids[0].rows.length, 1);
  const rowKey = grids[0].rows[0][0];
  assert.equal(grids[0].columns[0].measurements.get(rowKey).measured_value, "1.234");
  assert.equal(grids[0].columns[1].measurements.get(rowKey).measured_value, "1.239");
  const changedSpec = buildPipeReportGrids([first, { ...second, measurements: [{ ...element, dwg_dim: "Different specification" }] }]);
  assert.equal(changedSpec[0].rows.length, 2);
});

test("unentered recipe elements and not-due cells are visible before the first live value", () => {
  const data = sample("in_progress");
  data.pipes[0].measurements = [];
  data.pipes[0].recipe_elements = [{ element_sequence: 1, element_description: "Diameter", dwg_dim: "1.230–1.240" }, { element_sequence: 2, element_description: "Rotating check" }];
  data.pipes[0].planned_sequences = [1];
  const html = renderPipeReport(data);
  assert.match(html, /Diameter/);
  assert.match(html, /Not due/);
  assert.match(html, /Awaiting submission/);
});

test("all report routes require current authorized identity; verification ignores a forged reviewer; live drafts require the active attempt", async () => {
  const calls = [];
  const app = express();
  app.use(express.json());
  app.use(express.urlencoded({ extended: false }));
  app.use((req, res, next) => {
    const actor = req.get("x-test-actor");
    req.session = actor ? { inspector: { adp_number: actor }, sessionRecord: { id: 5 },
      activeInspection: { attempt_id: 10, inspection_plan: [{ element_sequence: 1, capture_type: "numeric", min: 1, max: 2 }] } } : {};
    next();
  });
  registerPipeReportRoutes(app, async (action, payload) => {
    calls.push({ action, payload });
    if (action === "get_employee_by_adp") return { adp_number: payload.adp_number, adp_job_title_desc: payload.adp_number === "IT" ? "IT" : payload.adp_number === "QUALITY" ? "Quality Manager" : "Inspector" };
    if (action === "get_live_pipe_report") return sample();
    if (action === "get_locations") return [];
    if (["verify_pipe_inspection", "save_live_inspection_draft"].includes(action)) return { saved: true };
    throw new Error(action);
  }, (_, content) => content);
  const server = app.listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  try {
    for (const route of ["/workflow/pipe-report", "/workflow/pipe-report/live.json", "/workflow/pipe-report/verify"]) {
      for (const actor of ["", "INSPECTOR"]) {
        const response = await fetch(base + route, { method: route.endsWith("verify") ? "POST" : "GET", headers: actor ? { "x-test-actor": actor } : {} });
        assert.equal(response.status, actor ? 403 : 401);
      }
    }
    assert.ok(!calls.some((call) => call.action === "verify_pipe_inspection"));
    for (const actor of ["QUALITY", "IT"]) assert.equal((await fetch(base + "/workflow/pipe-report", { headers: { "x-test-actor": actor } })).status, 200);
    const verified = await fetch(base + "/workflow/pipe-report/verify", { method: "POST", headers: { "Content-Type": "application/json", "x-test-actor": "QUALITY" }, body: JSON.stringify({ attempt_id: 1, snapshot_hash: "hash", verifier_adp: "FORGED" }) });
    assert.equal(verified.status, 200);
    assert.equal(calls.find((call) => call.action === "verify_pipe_inspection").payload.verifier_adp, "QUALITY");
    const draft = (attempt) => fetch(base + "/workflow/live-draft", { method: "POST", headers: { "Content-Type": "application/json", "x-test-actor": "INSPECTOR" }, body: JSON.stringify({ attempt_id: attempt, values: { element_1: "1.234" }, client_sequence: 10 }) });
    assert.equal((await draft(999)).status, 403);
    assert.equal((await draft(10)).status, 204);
    const saved = calls.find((call) => call.action === "save_live_inspection_draft").payload;
    assert.equal(saved.session_id, 5);
    assert.equal(saved.measurements[0].min, 1);
    assert.equal(saved.measurements[0].measured_value, "1.234");
  } finally { await new Promise((resolve) => server.close(resolve)); }
});
