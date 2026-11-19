import fs from "node:fs";

export const ncrCatalog = JSON.parse(fs.readFileSync(new URL("../config/ncr_catalog.json", import.meta.url), "utf8").replace(/^\uFEFF/, ""));
const escape = (value) => String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#39;");
const areas = ["Bucking", "CNC Operation", "End Finishing", "Pre-Processing", "Shipping/Receiving"];

export function renderNcrFields(values = {}, { capture = false, context = {}, number = "", status = "open" } = {}) {
  context = { ...(values.context || {}), ...context };
  const input = (key, label, type = "text", fallback = "", required = true) => `<div class="field"><label for="ncr-${key}">${label}</label><input id="ncr-${key}" name="ncr_${key}" type="${type}" value="${escape(values[key] ?? fallback)}" ${type === "number" ? `min="${key === "quantity_affected" ? "1" : "0.001"}" step="${key === "quantity_affected" ? "1" : "any"}"` : ""} ${required ? "required" : ""} /></div>`;
  const select = (key, label, choices, fallback = "") => `<div class="field"><label for="ncr-${key}">${label}</label><select id="ncr-${key}" name="ncr_${key}" required><option value=""></option>${choices.map((item) => {
    const value = typeof item === "object" ? item.id : item;
    const title = typeof item === "object" ? item.label : item;
    return `<option value="${escape(value)}" ${String(values[key] ?? fallback) === String(value) ? "selected" : ""}>${escape(title)}</option>`;
  }).join("")}</select></div>`;
  const readonly = (key, label, value) => `<div class="field"><label for="ncr-${key}">${label}</label><input id="ncr-${key}" value="${escape(value)}" readonly /></div>`;
  const tier2 = ncrCatalog.tier2.filter((row) => String(row.tier1_id) === String(values.tier1_id));
  const details = values.ncr_details ?? [values.nonconformance, values.immediate_containment ? `Immediate containment: ${values.immediate_containment}` : ""].filter(Boolean).join("\n\n");
  const engineeringReport = [true, "true", "yes"].includes(values.engineering_report);
  return `<fieldset data-ncr-fields ${capture ? "disabled" : ""}>
    <legend>NCR Info</legend>
    <div class="form-grid two">
      ${readonly("number", "NCR Number", number || "Assigned when sent to SharePoint")}
      ${input("ncr_date", "NCR Date", "date", new Date().toLocaleDateString("en-CA", { timeZone: "America/Chicago" }))}
      ${readonly("status", "Status", status === "closed" ? "Closed" : "Open")}
      ${readonly("work-order", "W/O Number", context.work_order)}
      ${select("ncr_type", "NCR Type", ncrCatalog.choices["NCR Type"].choices, "Internal")}
      ${readonly("shift", "Work Shift", context.shift)}
      ${readonly("inspector", "Inspector", context.inspector)}
      ${readonly("operator", "Operator", context.operator)}
      ${select("area", "Area", areas, "CNC Operation")}
      ${select("tier1_id", "Tier 1 Defect Code", ncrCatalog.tier1)}
      ${select("tier2_id", "Tier 2 Defect Code", tier2)}
      ${select("end_type", "End Type", ["Box", "Pin"])}
      <div class="field" style="grid-column:1/-1"><label for="ncr-details">Nonconformance and Immediate Containment</label><textarea id="ncr-details" name="ncr_ncr_details" required>${escape(details)}</textarea></div>
      ${readonly("pipe", "Pipe ID", context.pipe_number)}
      ${readonly("submitted-by", "Submitted By", context.submitted_by || context.inspector)}
      <div class="field"><label for="ncr-engineering">Engineering Report</label><input type="checkbox" id="ncr-engineering" data-ncr-engineering ${engineeringReport ? "checked" : ""} /><input type="hidden" name="ncr_engineering_report" value="${engineeringReport ? "true" : "false"}" /></div>
    </div>
  </fieldset>`;
}

export function submittedNcr(body) {
  const data = {};
  for (const [key, value] of Object.entries(body)) {
    if (key.startsWith("ncr_")) data[key.slice(4)] = value;
  }
  if (Object.hasOwn(body, "ncr_attachments")) data.attachments = JSON.parse(String(body.ncr_attachments || "[]"));
  return data;
}
