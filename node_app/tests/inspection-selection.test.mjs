import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import vm from "node:vm";
import { resolveWorkOrderSelection, chooseInspectionRecipe } from "../src/inspectionSelection.js";

const box = { production_number: "WO1", operation_description: "BOX description", inspection_entry: { size: '2 7/8"', weight: "7.90#", connection: "BTS-6", end_type: "BOX" } };
const pin = { ...box, operation_description: "PIN description", inspection_entry: { ...box.inspection_entry, end_type: "PIN" } };
const other = { ...pin, production_number: "WO2", operation_description: "Other WO description" };

test("only the selected WO's descriptions are available, and all four values come from WO data", () => {
  const { descriptions, selection } = resolveWorkOrderSelection([box, pin, other], { productionNumber: "WO1", operationDescription: "BOX description", sizeLabel: "999", endType: "PIN" });
  assert.equal(descriptions.length, 2);
  assert.equal(selection.sizeLabel, '2 7/8"');
  assert.equal(selection.weightLabel, "7.90#");
  assert.equal(selection.connectionLabel, "BTS-6");
  assert.equal(selection.endType, "BOX");
});

test("changing WO or description clears the old recipe and pipe and does not reuse stale specs", () => {
  const previous = { productionNumber: "WO1", operationDescription: box.operation_description, pipeNumber: "009", recipeName: "Box IRR", endType: "BOX" };
  const { selection, changed } = resolveWorkOrderSelection([box, pin, other], { productionNumber: "WO2" }, previous);
  assert.ok(changed);
  assert.equal(selection.operationDescription, other.operation_description);
  assert.equal(selection.endType, "PIN");
  assert.equal(selection.pipeNumber, "");
  assert.equal(selection.recipeName, "");
  const changedEnd = resolveWorkOrderSelection([box, pin], { operationDescription: pin.operation_description }, previous).selection;
  assert.equal(changedEnd.endType, "PIN");
  assert.equal(changedEnd.recipeName, "");
});

test("reloading the same context retains the pipe number for manual loading and automatic progression", () => {
  const previous = { productionNumber: "WO1", operationDescription: box.operation_description, recipeName: "Box IRR", pipeNumber: "010" };
  const result = resolveWorkOrderSelection([box, pin], { pipeNumber: "022" }, previous);
  assert.equal(result.changed, false);
  assert.equal(result.selection.pipeNumber, "022");
  assert.equal(result.selection.recipeName, "Box IRR");
});

test("multiple descriptions require selection; an unrelated WO description cannot supply manual specs", () => {
  assert.equal(resolveWorkOrderSelection([box, pin], { productionNumber: "WO1" }).selection.operationDescription, "");
  const result = resolveWorkOrderSelection([box, pin, other], { productionNumber: "WO1", operationDescription: other.operation_description, sizeLabel: "2.875" });
  assert.equal(result.selection.operationDescription, "");
  assert.equal(result.selection.sizeLabel, "");
});

test("unique or uniquely best specification/alias matches auto-select, while ambiguous and absent matches stay blank", () => {
  assert.equal(chooseInspectionRecipe([{ recipe_name: "Box", match_type: "specification", score: 100 }]), "Box");
  assert.equal(chooseInspectionRecipe([{ recipe_name: "Mapped", match_type: "alias" }]), "Mapped");
  const candidates = [{ recipe_name: "Drawing match", match_type: "specification", score: 110 }, { recipe_name: "Other drawing", match_type: "specification", score: 100 }];
  assert.equal(chooseInspectionRecipe(candidates), "Drawing match");
  assert.equal(chooseInspectionRecipe(candidates, "Other drawing"), "Other drawing");
  assert.equal(chooseInspectionRecipe(candidates.map((row) => ({ ...row, score: 100 }))), "");
  assert.equal(chooseInspectionRecipe([]), "");
  assert.equal(chooseInspectionRecipe([{ recipe_name: "Word overlap", match_type: "token", score: 5 }]), "");
});

test("starting an inspection revalidates the WO description and ignores posted manual specs or an unrelated IRR", async () => {
  const source = await readFile(new URL("../src/server.js", import.meta.url), "utf8");
  const start = source.indexOf('app.post("/workflow/start",');
  const end = source.indexOf('\napp.post("/workflow/scope",', start);
  let handler;
  const calls = [];
  const session = { inspector: { branch: "Ennis" }, sessionRecord: { id: 1 },
    selection: { productionNumber: "WO1", operationDescription: box.operation_description, pipeNumber: "010", recipeName: "Box IRR" } };
  vm.runInNewContext(source.slice(start, end), {
    app: { post: (_, fn) => { handler = fn; } }, resolveWorkOrderSelection, chooseInspectionRecipe,
    buildInspectionConnectionLabel: (selection) => [selection.sizeLabel, selection.weightLabel, selection.connectionLabel, selection.endType].join(" "),
    callBridge: async (action, payload) => {
      calls.push({ action, payload });
      if (action === "get_open_work_orders") return [box, pin, other];
      if (action === "get_pipe_unit") return null;
      if (action === "find_recipe_candidates") {
        assert.equal(payload.operation_description, box.operation_description);
        return [{ recipe_name: "Box IRR", match_type: "specification", score: 110 }];
      }
      if (action === "get_recipe_elements") return { elements: [{ element_sequence: 1 }] };
      if (action === "create_inspection_attempt") return { attempt_id: 1 };
      throw new Error(action);
    },
  });
  await handler({ session, body: { productionNumber: "WO1", operationDescription: box.operation_description,
    sizeLabel: "999", weightLabel: "999", connectionLabel: "BAD", endType: "PIN", recipeName: "Unrelated IRR", pipeNumber: "022" } },
    { redirect() {} }, (err) => { throw err; });
  const created = calls.find((call) => call.action === "create_inspection_attempt").payload.params;
  assert.equal(created.recipe_name, "Box IRR");
  assert.equal(created.operation_description, '2 7/8" 7.90# BTS-6 BOX');
  assert.equal(created.pipe_number, "022");
});
