import { editMeasurement, editPipeNumber } from "./measurementKeypadCore.js";

const selector = '.inspection-table-input[data-capture-type="numeric"], input[data-lunch-reading], input[data-pipe-number-entry], input[data-measurement-offset]';
const coarse = () => window.matchMedia("(any-pointer: coarse)").matches;
let active = null;
let ownerDialog = null;
let touchEntry = false;
let outsidePointer = false;
const keypadInputs = new WeakSet();
const prepareInput = input => {
  if (input.readOnly && !keypadInputs.has(input)) return;
  keypadInputs.add(input);
  input.inputMode = "none";
  // Android keyboards can ignore inputmode; readonly blocks the native editor
  // while still allowing the app pad to set and submit the field value.
  input.readOnly = true;
};
const pad = document.createElement("div");
pad.className = "measurement-keypad";
pad.setAttribute("popover", "manual");
pad.setAttribute("role", "group");
pad.setAttribute("aria-label", "Measurement number pad");
pad.hidden = true;
pad.innerHTML = `<div class="measurement-keypad-inner">
  <div class="measurement-keypad-heading"><span data-keypad-label></span><output data-keypad-value aria-label="Current measurement"></output><button type="button" data-keypad-key="hide" aria-label="Hide number pad" title="Hide number pad">&#8595;</button></div>
  <div class="measurement-keypad-grid">${[
    ["7", "7"], ["8", "8"], ["9", "9"], ["backspace", "&#9003;"],
    ["4", "4"], ["5", "5"], ["6", "6"], ["+", "+"],
    ["1", "1"], ["2", "2"], ["3", "3"], ["-", "-"],
    [".", "."], ["0", "0"], ["clear", "Clear"], ["next", "Next &#8594;"],
  ].map(([key, label]) => `<button type="button" data-keypad-key="${key}" ${key === "backspace" ? 'aria-label="Backspace" title="Backspace"' : key === "+" ? 'aria-label="Positive sign" title="Positive sign"' : key === "-" ? 'aria-label="Negative sign" title="Negative sign"' : ""}>${label}</button>`).join("")}</div>
</div>`;
document.body.append(pad);

const update = () => {
  if (active) pad.querySelector("[data-keypad-value]").textContent = active.value;
};
const hide = () => {
  if (pad.hidePopover && pad.matches(":popover-open")) pad.hidePopover();
  pad.hidden = true;
  ownerDialog?.classList.remove("measurement-keypad-host");
  document.body.classList.remove("measurement-keypad-visible");
  document.documentElement.style.removeProperty("--measurement-keypad-height");
  active = null;
  ownerDialog = null;
};
const frameInput = () => {
  if (!active?.isConnected || pad.hidden) return;
  const height = pad.getBoundingClientRect().height;
  document.documentElement.style.setProperty("--measurement-keypad-height", `${height}px`);
  active.scrollIntoView({ block: "nearest", inline: "nearest" });
  const rect = active.getBoundingClientRect();
  const top = pad.getBoundingClientRect().top;
  if (rect.bottom > top - 12) {
    (ownerDialog || window).scrollBy({ top: rect.bottom - top + 12, behavior: "instant" });
  }
};
const show = input => {
  if (!input.matches(selector) || input.disabled || (input.readOnly && !keypadInputs.has(input)) || input.closest("fieldset:disabled") || document.getElementById("inspection-loading")?.open) return;
  const dialog = input.closest("dialog");
  if (dialog && !dialog.open) return;
  ownerDialog?.classList.remove("measurement-keypad-host");
  active = input;
  prepareInput(input);
  ownerDialog = dialog;
  const parent = dialog || document.body;
  if (pad.parentElement !== parent) {
    if (pad.hidePopover && pad.matches(":popover-open")) pad.hidePopover();
    parent.append(pad);
  }
  const pipeNumber = input.matches("[data-pipe-number-entry]");
  pad.querySelector("[data-keypad-label]").textContent = pipeNumber ? "Pipe #" : input.closest("tr")?.cells[1]?.textContent.trim() || "Measurement";
  pad.querySelector("[data-keypad-value]").setAttribute("aria-label", pipeNumber ? "Current pipe number" : "Current measurement");
  pad.querySelector('[data-keypad-key="next"]').textContent = pipeNumber ? "Load Pipe" : "Next \u2192";
  pad.hidden = false;
  if (pad.showPopover && !pad.matches(":popover-open")) pad.showPopover();
  document.body.classList.add("measurement-keypad-visible");
  ownerDialog?.classList.add("measurement-keypad-host");
  update();
  requestAnimationFrame(frameInput);
};
window.initializeMeasurementKeypad = (root = document) => {
  if (!coarse() && !touchEntry) return;
  document.body.classList.add("measurement-touch-entry");
  root.querySelectorAll(selector).forEach(prepareInput);
};
window.initializeMeasurementKeypad(document);
document.addEventListener("DOMContentLoaded", () => window.initializeMeasurementKeypad(document));
document.addEventListener("pointerdown", event => {
  const input = event.target.closest(selector);
  outsidePointer = Boolean(active && !input && !pad.contains(event.target));
  if (input && (event.pointerType === "touch" || coarse() || touchEntry)) {
    touchEntry = true;
    document.body.classList.add("measurement-touch-entry");
    prepareInput(input);
  }
}, true);
document.addEventListener("focusin", event => {
  if (event.target.matches(selector) && (coarse() || touchEntry)) {
    prepareInput(event.target);
    show(event.target);
  }
  else if (!pad.contains(event.target) && !outsidePointer) hide();
});
document.addEventListener("click", event => {
  if (event.target.matches(selector) && (coarse() || touchEntry)) show(event.target);
  else if (!pad.contains(event.target) && outsidePointer) hide();
  outsidePointer = false;
});
document.addEventListener("pointercancel", () => { outsidePointer = false; });
document.addEventListener("input", event => { if (event.target === active) update(); });
document.addEventListener("keydown", event => {
  if (event.key === "Escape" && active) { event.preventDefault(); event.stopPropagation(); hide(); }
}, true);
pad.addEventListener("pointerdown", event => {
  if (event.target.closest("button")) event.preventDefault();
});
pad.addEventListener("click", event => {
  const button = event.target.closest("[data-keypad-key]");
  if (!button || !active?.isConnected || active.disabled) return;
  const input = active;
  const key = button.dataset.keypadKey;
  if (key === "hide") { hide(); return; }
  if (key === "next") {
    input.dispatchEvent(new Event("change", { bubbles: true }));
    if (input.closest("#measurements")) {
      const fields = Array.from(input.form.querySelectorAll("input[data-measurement-offset], select[data-sequence]"));
      const next = fields[fields.indexOf(input) + 1];
      if(next) next.focus(); else {hide();input.form.querySelector('button[type="submit"]')?.focus();}
    } else if(input.closest("#load-pipe")) {
      hide();input.form.requestSubmit();
    } else if (input.matches(".inspection-table-input, [data-pipe-number-entry]")) {
      input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true }));
    } else {
      const form = input.closest("form");
      const fields = Array.from(form.querySelectorAll("[data-lunch-reading]:enabled"));
      const next = fields[fields.indexOf(input) + 1];
      if (next) next.focus();
      else { hide(); form.querySelector('button[type="submit"]')?.focus(); }
    }
    return;
  }
  const edit = input.matches("[data-pipe-number-entry]") ? editPipeNumber : editMeasurement;
  const result = edit(input.value, input.selectionStart, input.selectionEnd, key);
  input.value = result.value;
  input.focus({ preventScroll: true });
  input.setSelectionRange(result.caret, result.caret);
  input.dispatchEvent(new Event("input", { bubbles: true }));
  update();
});
new MutationObserver(() => {
  window.initializeMeasurementKeypad(document);
  if (active && (!active.isConnected || active.disabled || active.closest("fieldset:disabled") || (ownerDialog && !ownerDialog.open) || document.getElementById("inspection-loading")?.open)) hide();
}).observe(document.body, { childList: true, subtree: true, attributes: true, attributeFilter: ["open", "disabled"] });
window.addEventListener("resize", frameInput);
window.visualViewport?.addEventListener("resize", frameInput);
