import test from "node:test";
import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import vm from "node:vm";
import {reliefColumnClass,reliefColumnLabel} from "../src/inspectionRelief.js";
import { submittedMeasurementValue, loadNextInspection } from "../src/inspectionFlow.js";

const source = await readFile(new URL("../src/server.js", import.meta.url), "utf8");
function functionSource(name) {
  const start = source.indexOf(`function ${name}(`);
  const end = source.indexOf("\nfunction ", start + 1);
  return source.slice(start, end);
}
const context = vm.createContext({reliefColumnClass,reliefColumnLabel});
vm.runInContext(["escapeHtml", "renderPopupNoticeModal", "incrementPipeNumber", "buildInspectionConnectionLabel", "renderScopeBadge", "formatDateValue", "formatDigitalIrrName", "formatPipeStatusResult", "renderInspectionInputControl", "renderCurrentAttemptWorksheet", "layout"].map(functionSource).join("\n"), context);

const numeric = { element_sequence: 1, element_description: "Diameter", capture_type: "numeric", nominal: 1.234, min: 1.23, max: 1.24, dwg_dim: "1.234", frequency: "every_pipe" };
const visual = { element_sequence: 2, element_description: "Visual", capture_type: "boolean", frequency: "every_pipe" };
const recipe = { elements: [numeric, visual], recipe_name: "Test IRR", drawing: "TEST", approval_rules: [] };
function session() {
  return { selection: { productionNumber: "TEST-WO", recipeName: "Test IRR", pipeNumber: "P009", sizeLabel: "5", weightLabel: "20", connectionLabel: "Test", endType: "PIN", inspectionScope: "full" },
    inspector: { branch: "Ennis", full_name: "Test Inspector" }, sessionRecord: { id: 99, cnc_operator_name: "Test Operator" },
    activeInspection: { attempt_id: 1, pipe_unit_id: 1, pipe_number: "P009", inspection_plan: [numeric, visual] } };
}

test("new worksheet is empty even when previous pipe values passed; inline browser script compiles", async () => {
  const current = session();
  const sheet = context.renderCurrentAttemptWorksheet({ activeInspection: current.activeInspection, recipeDefinition: recipe,
    selection: current.selection, inspectorName: "Test Inspector", sessionRecord: { location_name: "TEST" },
    historyColumns: [{ pipeUnitId: 3, pipeNumber: "P008", measurementsBySequence: new Map([[1, { measured_value: 1.235 }], [2, { measured_value: "Yes" }]]) }] });
  assert.match(sheet, /placeholder="Full measurement" value="" name="element_1"/);
  assert.doesNotMatch(sheet, /data-prefix=|data-tail-length=|maxlength=|\schecked[\s/>]/);
  assert.match(sheet, /name="element_2" value="No"/);
  assert.match(sheet, /name="pipeNumber" value="P009"/);
  assert.doesNotMatch(sheet, /data-edit-url=|data-delete-pipe-id=|class="[^"]*history-column-action/);
  const authorizedSheet = context.renderCurrentAttemptWorksheet({ activeInspection: current.activeInspection, recipeDefinition: recipe,
    selection: current.selection, canEditHistory: true,
    historyColumns: [{ pipeUnitId: 3, pipeNumber: "P008", measurementsBySequence: new Map() }] });
  assert.match(authorizedSheet, /data-edit-url="\/workflow\/history\/edit\/3"/);
  const html = context.layout({ title: "Inspection fixture", sidebar: "", content: `<div id="inspection-workspace">${sheet}</div>` });
  for (const match of html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)) new vm.Script(match[1]);
  if (process.env.INSPECTION_FIXTURE_PATH) await writeFile(process.env.INSPECTION_FIXTURE_PATH, html);
});

test("blank/omitted visual is No; missing numeric remains missing", () => {
  assert.equal(submittedMeasurementValue(visual, {}), "No");
  assert.equal(submittedMeasurementValue(visual, { element_2: " " }), "No");
  assert.equal(submittedMeasurementValue(visual, { element_2: "Yes" }), "Yes");
  assert.equal(submittedMeasurementValue(numeric, {}), undefined);
});

function completeHandler(bridge) {
  let handler;
  const route = source.slice(source.indexOf('app.post("/workflow/complete",'), source.indexOf('\napp.get("/admin",', source.indexOf('app.post("/workflow/complete",')));
  vm.runInNewContext(route, { app: { post: (_, fn) => { handler = fn; } }, callBridge: bridge,
    submittedMeasurementValue, loadNextInspection, incrementPipeNumber: context.incrementPipeNumber,
    buildInspectionConnectionLabel: context.buildInspectionConnectionLabel });
  return handler;
}

for (const disposition of ["pass", "rework", "manager_approved"]) {
  test(`${disposition} completion saves first and loads the next pipe in the same request`, async () => {
    const current = session();
    const calls = [];
    const handler = completeHandler(async (action, payload) => {
      calls.push({ action, payload });
      if (action === "evaluate_measurements") return { has_failures: disposition !== "pass", measurements: payload.measurements };
      if (action === "complete_inspection_attempt") return { pipe_status: disposition === "rework" ? "rework" : "completed" };
      if (action === "get_recipe_elements") return recipe;
      if (action === "create_inspection_attempt") return { attempt_id: 2, inspection_plan: recipe.elements };
      throw new Error(action);
    });
    let redirect;
    await handler({ session: current, body: { attempt_id: "1", element_1: "1.234", element_2: disposition === "pass" ? "Yes" : "",
      failure_action: disposition, manager_name: "Manager", manager_reason: "Reviewed" } }, { redirect: (url) => { redirect = url; } }, (err) => { throw err; });
    assert.deepEqual(calls.map((call) => call.action), ["evaluate_measurements", "complete_inspection_attempt", "get_recipe_elements", "create_inspection_attempt"]);
    assert.equal(current.selection.pipeNumber, "P010");
    assert.equal(current.activeInspection.attempt_id, 2);
    assert.equal(calls.at(-1).payload.params.inspection_scope, "full");
    assert.equal(calls.at(-1).payload.params.pipe_number, "P010");
    assert.equal(redirect, "/workflow/inspection");
    assert.match(current.notice.message, /loaded and ready/);
  });
}

test("a next-pipe failure preserves the saved result and allows changing the number", async () => {
  const current = session();
  let saved = 0;
  const handler = completeHandler(async (action, payload) => {
    if (action === "evaluate_measurements") return { has_failures: false, measurements: payload.measurements };
    if (action === "complete_inspection_attempt") { saved++; return { pipe_status: "completed" }; }
    if (action === "get_recipe_elements") return recipe;
    throw new Error("Next pipe number is already completed");
  });
  await handler({ session: current, body: { attempt_id: "1", element_1: "1.234", element_2: "Yes" } }, { redirect() {} }, (err) => { throw err; });
  assert.equal(saved, 1);
  assert.equal(current.activeInspection, null);
  assert.equal(current.selection.pipeNumber, "P010");
  assert.match(current.notice.message, /Inspection completed.*next pipe could not be loaded/i);
});

test("missing numeric values do not save or advance", async () => {
  const current = session();
  await completeHandler(async () => { throw new Error("Must not call backend"); })(
    { session: current, body: { attempt_id: "1" } }, { redirect() {} }, (err) => { throw err; });
  assert.equal(current.selection.pipeNumber, "P009");
  assert.equal(current.activeInspection.attempt_id, 1);
});

test("a stale or retried previous worksheet cannot submit measurements onto the next pipe", async () => {
  const current = session();
  current.activeInspection.attempt_id = 2;
  await completeHandler(async () => { throw new Error("Must not save a stale worksheet"); })(
    { session: current, body: { attempt_id: "1", element_1: "1.234", element_2: "Yes" } },
    { redirect() {} }, (err) => { throw err; });
  assert.equal(current.activeInspection.attempt_id, 2);
  assert.match(current.notice.message, /different inspection attempt/);
});
