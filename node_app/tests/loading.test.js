import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";

const source = fs.readFileSync(new URL("../public/loading.js", import.meta.url), "utf8");

function fixture(readyState = "complete") {
  const documentEvents = {};
  const windowEvents = {};
  const label = { textContent: "" };
  const main = { setAttribute(name, value) { this[name] = value; } };
  const dialog = { open: true, querySelector: () => label, addEventListener() {}, close() { this.open = false; }, showModal() { this.open = true; } };
  const window = { addEventListener(name, fn) { windowEvents[name] = fn; } };
  const document = { readyState, getElementById: () => dialog, querySelector: () => main, addEventListener(name, fn) { documentEvents[name] = fn; } };
  const location = new URL("http://localhost/workflow/inspection");
  vm.runInNewContext(source, { window, document, location, URL, queueMicrotask });
  return { window, dialog, label, main, documentEvents, windowEvents };
}

test("initial loading clears when the page finishes loading", () => {
  const f = fixture("interactive");
  assert.equal(f.dialog.open, true);
  f.windowEvents.load();
  assert.equal(f.dialog.open, false);
  assert.equal(f.main["aria-busy"], "false");
});

test("overlapping requests remain visible until all finish", () => {
  const f = fixture();
  const first = f.window.inspectionLoading.start("Loading pipe...");
  const second = f.window.inspectionLoading.start("Saving inspection...");
  first();
  first();
  assert.equal(f.dialog.open, true);
  assert.equal(f.label.textContent, "Saving inspection...");
  second();
  assert.equal(f.dialog.open, false);
});

test("back navigation clears stale loading state", () => {
  const f = fixture();
  f.window.inspectionLoading.start();
  f.windowEvents.pageshow();
  assert.equal(f.dialog.open, false);
});

test("prevented form submissions never leave an overlay", async () => {
  const f = fixture();
  const event = { defaultPrevented: false, target: { method: "post", target: "" } };
  f.documentEvents.submit(event);
  event.defaultPrevented = true;
  await new Promise(resolve => queueMicrotask(resolve));
  assert.equal(f.dialog.open, false);
});

test("native form submissions show loading", async () => {
  const f = fixture();
  f.documentEvents.submit({ defaultPrevented: false, target: { method: "post", target: "" } });
  await new Promise(resolve => queueMicrotask(resolve));
  assert.equal(f.dialog.open, true);
});

test("same-page anchors and downloads do not show loading", async () => {
  for (const download of [true, false]) {
    const f = fixture();
    const link = { href: "http://localhost/workflow/inspection#pipe", target: "", hasAttribute: () => download };
    f.documentEvents.click({ target: { closest: () => link }, button: 0 });
    await new Promise(resolve => queueMicrotask(resolve));
    assert.equal(f.dialog.open, false);
  }
});
