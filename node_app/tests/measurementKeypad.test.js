import test from "node:test";
import assert from "node:assert/strict";
import { editMeasurement, editPipeNumber } from "../public/measurementKeypadCore.js";

test("starts empty and supports explicit signed decimal readings", () => {
  let state = { value: "", caret: 0 };
  for (const key of ["-", ".", "0", "0", "3"]) state = editMeasurement(state.value, state.caret, state.caret, key);
  assert.deepEqual(state, { value: "-.003", caret: 5 });
});
test("sign keys replace each other and toggle off", () => {
  assert.equal(editMeasurement(".003", 4, 4, "+").value, "+.003");
  assert.equal(editMeasurement("+.003", 5, 5, "-").value, "-.003");
  assert.equal(editMeasurement("-.003", 5, 5, "-").value, ".003");
});
test("typing replaces the selected reading without prefilling digits", () => {
  assert.deepEqual(editMeasurement("3.0294", 0, 6, "2"), { value: "2", caret: 1 });
});
test("backspace respects selection and caret", () => {
  assert.deepEqual(editMeasurement("3.0294", 6, 6, "backspace"), { value: "3.029", caret: 5 });
  assert.deepEqual(editMeasurement("3.0294", 2, 4, "backspace"), { value: "3.94", caret: 2 });
  assert.deepEqual(editMeasurement("", 0, 0, "backspace"), { value: "", caret: 0 });
});
test("decimal can be inserted once or replace a selected decimal", () => {
  assert.equal(editMeasurement(".050", 4, 4, ".").value, ".050");
  assert.deepEqual(editMeasurement(".050", 0, 1, "."), { value: ".050", caret: 1 });
});
test("clear removes the entire value", () => {
  assert.deepEqual(editMeasurement("-.003", 2, 2, "clear"), { value: "", caret: 0 });
});
test("unsupported characters never enter the measurement", () => {
  assert.equal(editMeasurement(".050", 4, 4, "abc").value, ".050");
});

test("pipe numbers preserve leading zeroes", () => {
  let state = { value: "", caret: 0 };
  for (const key of ["0", "0", "4", "2"]) state = editPipeNumber(state.value, state.caret, state.caret, key);
  assert.deepEqual(state, { value: "0042", caret: 4 });
});
test("pipe identifiers preserve prefixes and insert hyphens literally", () => {
  assert.deepEqual(editPipeNumber("PIN-009", 6, 7, "4"), { value: "PIN-004", caret: 7 });
  assert.equal(editPipeNumber("12", 2, 2, "-").value, "12-");
  assert.equal(editPipeNumber("12-", 3, 3, "3").value, "12-3");
  assert.deepEqual(editPipeNumber("0042", 4, 4, "backspace"), { value: "004", caret: 3 });
});
