import express from "express";
import session from "express-session";
import path from "path";
import { fileURLToPath } from "url";
import { callBridge } from "./pythonBridge.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const WORKORDER_SYNC_ENABLED = process.env.WORKORDER_SYNC_ENABLED !== "false";
const WORKORDER_SYNC_INTERVAL_HOURS = Number(process.env.WORKORDER_SYNC_INTERVAL_HOURS || 6);
const WORKORDER_SYNC_RUN_ON_START = process.env.WORKORDER_SYNC_RUN_ON_START !== "false";
let workOrderSyncInProgress = false;

const app = express();
app.use(express.urlencoded({ extended: true, limit: "12mb" }));
app.use(express.json({ limit: "12mb" }));
app.use(
  session({
    secret: process.env.SESSION_SECRET || "autoirr-node-dev",
    resave: false,
    saveUninitialized: false,
  }),
);
app.use("/public", express.static(path.resolve(__dirname, "..", "public")));

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function renderOptions(items, selectedValue, getValue, getLabel) {
  return items
    .map((item) => {
      const value = getValue(item);
      const selected = String(selectedValue ?? "") === String(value) ? "selected" : "";
      return `<option value="${escapeHtml(value)}" ${selected}>${escapeHtml(getLabel(item))}</option>`;
    })
    .join("");
}

function renderLocationOptions(locations, selectedValue = null, inspector = null) {
  const inspectorAdp = String(inspector?.adp_number || "").trim();
  return locations
    .map((item) => {
      const value = item.id;
      const selected = String(selectedValue ?? "") === String(value) ? "selected" : "";
      const isOwnSession = item.is_locked && inspectorAdp && String(item.active_inspector_adp || "").trim() === inspectorAdp;
      const disabled = item.is_locked && inspectorAdp && !isOwnSession ? "disabled" : "";
      const lockedSuffix = item.is_locked
        ? isOwnSession
          ? " (Resume your session)"
          : ` (In Use${item.active_inspector_name ? ` - ${item.active_inspector_name}` : ""})`
        : "";
      const isFloatingTablet = String(item.location_name || "").trim().toLowerCase() === "floating tablet";
      return `<option value="${escapeHtml(value)}" data-floating-tablet="${isFloatingTablet ? "1" : "0"}" ${selected} ${disabled}>${escapeHtml(`${item.location_name}${lockedSuffix}`)}</option>`;
    })
    .join("");
}

function renderFloatingTabletNoteField(value = "") {
  return `
    <div class="field hidden" data-floating-tablet-note-field>
      <label>Floating Tablet Note</label>
      <textarea name="floating_tablet_note" rows="3" placeholder="Optional: reason for using the floating tablet">${escapeHtml(value)}</textarea>
    </div>
  `;
}

function isFloatingTabletLocation(location) {
  return String(location?.location_name || "").trim().toLowerCase() === "floating tablet";
}

function renderNotice(notice) {
  if (!notice) return "";
  return `<div class="notice ${escapeHtml(notice.kind || "info")}"${notice.popup ? ' data-popup="true"' : ""}>${escapeHtml(notice.message)}</div>`;
}

function renderPopupNoticeModal() {
  return `
    <div id="popup-notice-modal" class="approval-modal hidden" aria-hidden="true">
      <div class="approval-modal-backdrop" data-popup-notice-close></div>
      <div class="approval-modal-panel popup-notice-panel">
        <div class="approval-modal-header">
          <h4 id="popup-notice-title">Duplicate Pipe Number</h4>
          <button type="button" class="approval-modal-close" data-popup-notice-close aria-label="Close message dialog">×</button>
        </div>
        <div class="popup-notice-body">
          <p id="popup-notice-message"></p>
        </div>
        <div class="actions approval-modal-actions">
          <button type="button" class="button" id="popup-notice-ok-button">Okay</button>
        </div>
      </div>
    </div>
  `;
}

function renderLockedLocationRows(locations) {
  if (!locations.length) {
    return `<tr><td colspan="4">No machines are currently locked.</td></tr>`;
  }
  return locations
    .map(
      (location) => `<tr>
        <td>${escapeHtml(location.location_name)}</td>
        <td>${escapeHtml(location.active_inspector_name || "Unknown")}</td>
        <td>${escapeHtml(formatDateValue(location.active_logged_in_at))}</td>
        <td>
          <form method="post" action="/admin/unlock-location" onsubmit="return confirm('Unlock ${escapeHtml(location.location_name)} and release its current session?');">
            <input type="hidden" name="location_id" value="${escapeHtml(location.id)}" />
            <button class="button compact-button" type="submit">Unlock Machine</button>
          </form>
        </td>
      </tr>`,
    )
    .join("");
}

function summarizeRefreshError(error) {
  const message = String(error?.message || "").trim();
  if (!message) return "Unable to refresh work orders right now.";
  if (message.includes("ProxyError") || message.includes("login.microsoftonline.com")) {
    return "Unable to refresh work orders right now because the SharePoint sign-in service could not be reached.";
  }
  if (message.includes("Authentication failed")) {
    return "Unable to refresh work orders right now because SharePoint authentication failed.";
  }
  if (message.includes("Graph request failed")) {
    return "Unable to refresh work orders right now because the SharePoint request failed.";
  }
  return "Unable to refresh work orders right now. Please try again later.";
}

function formatValue(value) {
  if (value === null || value === undefined || value === "") return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function formatNumberForInput(value) {
  if (value === null || value === undefined || value === "" || Number.isNaN(Number(value))) return "";
  return Number(value).toFixed(6).replace(/0+$/, "").replace(/\.$/, "");
}

function formatDecimalTailForInput(value) {
  if (value === null || value === undefined || value === "") return "";
  const text = String(value).trim();
  if (text.startsWith("0.")) return text.slice(2);
  if (text.startsWith(".")) return text.slice(1);
  return text;
}

function formatDateValue(value) {
  if (value === null || value === undefined || value === "") return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const year = String(date.getFullYear());
  return `${month}/${day}/${year}`;
}

function formatDigitalIrrName(name, drawing = "") {
  const irrName = String(name || "").trim();
  const drawingValue = String(drawing || "").trim();
  if (irrName && drawingValue) return `${irrName} (${drawingValue})`;
  return irrName || drawingValue;
}

function formatCellValue(header, value) {
  const key = String(header || "").toLowerCase();
  if (key.endsWith("_at") || key.includes("date")) {
    return formatDateValue(value);
  }
  return formatValue(value);
}

function buildInspectionConnectionLabel(selection = {}) {
  const sizeLabel = String(selection.sizeLabel || "").trim();
  const weightLabel = String(selection.weightLabel || "").trim();
  const connectionLabel = String(selection.connectionLabel || "").trim();
  const endType = String(selection.endType || "").trim().toUpperCase();
  return [sizeLabel, weightLabel, connectionLabel, endType].filter(Boolean).join(" ").trim();
}

function incrementPipeNumber(pipeNumber) {
  const text = String(pipeNumber ?? "").trim();
  const match = text.match(/^(.*?)(\d+)$/);
  if (!match) return text;
  const [, prefix, digits] = match;
  const nextValue = String(Number(digits) + 1);
  const nextDigits = digits.length > nextValue.length ? nextValue.padStart(digits.length, "0") : nextValue;
  return `${prefix}${nextDigits}`;
}

function formatPipeStatusResult(status, attemptStatus = "", requiresManagerApproval = false) {
  const normalizedAttempt = String(attemptStatus || "").trim().toLowerCase();
  if (normalizedAttempt === "approved" || requiresManagerApproval) return "Pass With Approval";
  if (normalizedAttempt === "passed") return "Pass";
  if (normalizedAttempt === "rework") return "Re-work";
  if (normalizedAttempt === "scrapped") return "Scrapped";

  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "completed") return "Pass";
  if (normalized === "rework") return "Re-work";
  if (normalized === "scrapped") return "Scrapped";
  if (normalized === "in_progress") return "In Progress";
  return status ? String(status) : "";
}

function countDecimalPlaces(value) {
  if (value === null || value === undefined || value === "") return 0;
  const text = normalizeNumericText(value);
  if (!text.includes(".")) return 0;
  return text.split(".")[1].length;
}

function normalizeNumericText(value) {
  const text = String(value ?? "").trim();
  if (!text) return "";
  if (!/^-?\d+(?:\.\d+)?$/.test(text)) return text;
  return text.replace(/(\.\d*?)0+$/, "$1").replace(/\.$/, "");
}

function formatNumericReference(value, decimals, { trim = true } = {}) {
  const numericValue = Number(value);
  if (Number.isNaN(numericValue)) return "";
  const text = numericValue.toFixed(decimals);
  return trim ? normalizeNumericText(text) : text;
}

function buildNumericEntryConfig(element) {
  if (element.nominal !== null && element.nominal !== undefined && element.nominal !== "") {
    return null;
  }

  const candidates = [element.nominal, element.min, element.max]
    .map((value) => Number(value))
    .filter((value) => !Number.isNaN(value));

  if (!candidates.length || candidates.some((value) => value < 0)) {
    return null;
  }

  const decimals = Math.max(
    countDecimalPlaces(element.nominal),
    countDecimalPlaces(element.min),
    countDecimalPlaces(element.max),
  );

  if (decimals < 2) {
    return null;
  }

  const referenceSource =
    element.nominal !== null && element.nominal !== undefined && element.nominal !== "" && Number(element.nominal) !== 0
      ? element.nominal
      : element.max ?? element.min ?? element.nominal;
  const reference = formatNumericReference(referenceSource, decimals, { trim: false });
  if (!reference || reference.length <= 2 || reference.length > 8) {
    return null;
  }

  return {
    prefix: reference.slice(0, -2),
    tailLength: 2,
    tailPlaceholder: reference.slice(-2),
    fullPlaceholder: reference,
    decimals,
  };
}

function renderScopeBadge(scope) {
  const normalized = String(scope || "standard").toLowerCase();
  const label = normalized === "full" ? "Full Inspection" : "Standard Inspection";
  const className = normalized === "full" ? "scope-badge full" : "scope-badge standard";
  return `<span class="${className}">${escapeHtml(label)}</span>`;
}

function renderTable(rows) {
  if (!rows?.length) return "<p>No records found.</p>";
  const headers = Object.keys(rows[0]);
  return `
    <div class="table-wrap">
      <table>
        <thead><tr>${headers.map((h) => `<th>${escapeHtml(h)}</th>`).join("")}</tr></thead>
        <tbody>
          ${rows
            .map(
              (row) =>
                `<tr>${headers.map((h) => `<td>${escapeHtml(formatCellValue(h, row[h]))}</td>`).join("")}</tr>`,
            )
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderPipeHistorySheets(workorderGroups, { canManage = false } = {}) {
  if (!workorderGroups?.length) return "<p>No records found.</p>";
  return `
    <form id="pipe-history-sheet-delete-form" method="post" action="/workflow/history/delete">
      <input type="hidden" id="pipe-history-sheet-delete-pipe-unit-id" name="pipeUnitId" value="" />
      <input type="hidden" name="redirectTo" value="" data-current-history-url />
    </form>
    <div class="pipe-history-sheet-list">
      ${workorderGroups
        .map((workorder) => {
          return `
            <details class="table-card pipe-history-entry">
              <summary class="pipe-history-summary">
                <span class="pipe-history-summary-label"><strong>Workorder #:</strong> ${escapeHtml(workorder.productionNumber)}</span>
                <span class="pipe-history-summary-label"><strong>Latest Activity:</strong> ${escapeHtml(formatDateValue(workorder.latestUpdatedAt || new Date()))}</span>
                <span class="pipe-history-summary-label"><strong>Digital IRRs:</strong> ${escapeHtml(String(workorder.connectionGroups.length))}</span>
              </summary>
              <div class="pipe-history-workorder-body">
                ${workorder.connectionGroups
                  .map((group) => {
                    const drawing = group.recipeDefinition?.drawing || "";
                    const digitalIrrName = group.recipeDefinition?.display_name || formatDigitalIrrName(group.recipeDefinition?.recipe_name || "", drawing);
                    const connectionType = group.recipeDefinition?.connection_type || group.operationDescription || "";
                    const reportTitle = group.recipeDefinition?.source_report || digitalIrrName || "";
                    return `
                      <section class="card nested-card pipe-history-connection-card">
                        <div class="inspection-sheet-meta pipe-history-sheet-meta">
                          <div class="inspection-sheet-meta-row">
                            <div><strong>Date:</strong> ${escapeHtml(formatDateValue(group.latestUpdatedAt || new Date()))}</div>
                            <div><strong>Drawing #:</strong> ${escapeHtml(drawing)}</div>
                            <div><strong>Machine #:</strong> ${escapeHtml(group.latestLocationName || "")}</div>
                          </div>
                          <div class="inspection-sheet-meta-row">
                            <div><strong>Inspector:</strong> ${escapeHtml(group.latestInspectorName || "")}</div>
                            <div><strong>Workorder #:</strong> ${escapeHtml(workorder.productionNumber)}</div>
                            <div><strong>Connection Type:</strong> ${escapeHtml(connectionType)}</div>
                          </div>
                        </div>
                        ${reportTitle ? `<p class="pipe-history-report-title">${escapeHtml(reportTitle)}</p>` : ""}
                        <div class="table-wrap inspection-sheet-wrap">
                          <table class="inspection-sheet-table pipe-history-sheet-table">
                            <thead>
                              <tr>
                                <th class="inspection-col-num">#</th>
                                <th>Element</th>
                                <th>DWG DIM</th>
                                <th>Gauge</th>
                                ${group.columns
                        .map(
                          (column) => `<th
                            class="inspection-col-history ${canManage ? "history-column-action" : ""}"
                            ${canManage ? `data-history-pipe-id="${escapeHtml(column.pipeUnitId)}" data-edit-url="/workflow/history/edit/${encodeURIComponent(column.pipeUnitId)}" title="Click to edit this saved pipe. Right-click to delete it."` : ""}
                          >
                            <div class="pipe-history-connection-header centered">
                              <span class="pipe-history-connection-label">Pipe # ${escapeHtml(column.pipeNumber || "")}</span>
                              ${
                                canManage
                                  ? `<div class="pipe-history-header-actions">
                                      <a class="pipe-history-inline-action" href="/workflow/history/edit/${encodeURIComponent(column.pipeUnitId)}">Edit</a>
                                      <button class="pipe-history-inline-action danger" type="button" data-delete-pipe-id="${escapeHtml(column.pipeUnitId)}">Delete</button>
                                    </div>`
                                  : ""
                              }
                            </div>
                          </th>`,
                        )
                        .join("")}
                              </tr>
                            </thead>
                            <tbody>
                              ${group.rows
                      .map((row) => {
                        const rowClass =
                          row.frequency === "rotating"
                            ? "inspection-sheet-row-rotating"
                            : row.frequency === "every_pipe"
                              ? "inspection-sheet-row-active"
                              : "";
                        return `<tr class="${rowClass}">
                          <td class="inspection-col-num">${escapeHtml(row.element_sequence)}</td>
                          <td>${escapeHtml(row.element_description)}</td>
                          <td>${escapeHtml(row.dwg_dim || "")}</td>
                          <td>${escapeHtml(row.gauge || "")}</td>
                          ${group.columns
                            .map((column) => {
                              const measurement = column.measurementsBySequence?.get(Number(row.element_sequence));
                              return `<td
                                class="inspection-history-cell ${canManage ? "history-column-action " : ""}${measurement ? "inspection-history-cell-filled" : "inspection-history-cell-empty"}"
                                ${canManage ? `data-history-pipe-id="${escapeHtml(column.pipeUnitId)}" data-edit-url="/workflow/history/edit/${encodeURIComponent(column.pipeUnitId)}" title="Click to edit this saved pipe. Right-click to delete it."` : ""}
                              >${escapeHtml(measurement?.measured_value ?? "")}</td>`;
                            })
                            .join("")}
                        </tr>`;
                      })
                      .join("")}
                            </tbody>
                            <tfoot>
                              <tr class="inspection-result-row">
                                <td class="inspection-col-num inspection-result-label-piece inspection-result-label-num"></td>
                                <td class="inspection-result-label-piece inspection-result-label-main"><strong>Inspection Result</strong></td>
                                <td class="inspection-result-label-piece inspection-result-label-dwg"></td>
                                <td class="inspection-result-label-piece inspection-result-label-gauge"></td>
                                ${group.columns
                        .map((column) => {
                          const resultLabel = formatPipeStatusResult(
                            column.status,
                            column.attemptStatus,
                            column.requiresManagerApproval,
                          );
                          return `<td
                            class="inspection-history-result-cell ${canManage ? "history-column-action" : ""}"
                            ${canManage ? `data-history-pipe-id="${escapeHtml(column.pipeUnitId)}" data-edit-url="/workflow/history/edit/${encodeURIComponent(column.pipeUnitId)}" title="Click to edit this saved pipe. Right-click to delete it."` : ""}
                          >${escapeHtml(resultLabel)}</td>`;
                        })
                        .join("")}
                              </tr>
                            </tfoot>
                          </table>
                        </div>
                      </section>
                    `;
                  })
                  .join("")}
              </div>
            </details>
          `;
        })
        .join("")}
    </div>
  `;
}

function renderWorkflowNav(activePath, req = null) {
  const hasInspectionSession = Boolean(req?.session?.sessionRecord?.id);
  const canAccessAdmin = Boolean(req?.session?.canAccessAdmin);
  const items = [
    ...(hasInspectionSession ? [{ href: "/workflow/inspection", label: "Inspection Entry" }] : []),
    { href: "/workflow/history", label: "Pipe History" },
    { href: "/workflow/ncr", label: "NCR Queue" },
    ...(canAccessAdmin ? [{ href: "/admin", label: "Admin Tools" }] : []),
  ];
  return `
    <nav class="workflow-nav card">
      <div class="workflow-nav-links">
        ${items
          .map(
            (item) =>
              `<a class="workflow-tab ${activePath === item.href ? "active" : ""}" href="${item.href}">${escapeHtml(item.label)}</a>`,
          )
          .join("")}
      </div>
      <div class="workflow-nav-actions">
        ${
          hasInspectionSession
            ? `<form method="post" action="/workflow/refresh-workorders" class="workflow-nav-refresh">
                 <input type="hidden" name="redirectTo" value="${escapeHtml(activePath)}" />
                 <button class="button secondary workflow-action-button" type="submit">Refresh Work Orders</button>
               </form>
               <form method="post" action="/workflow/end-shift" class="workflow-nav-end-shift" onsubmit="return confirm('End shift, publish today\\'s saved pipe history, and log out?');">
                 <button class="button secondary workflow-action-button" type="submit">End Shift</button>
               </form>`
            : ""
        }
        <form method="post" action="/logout" class="workflow-nav-logout" onsubmit="return confirm('Log out and save today\\'s completed pipe history? Any unfinished pipe stays unpublished.');">
          <button class="button workflow-action-button" type="submit">Log Out</button>
        </form>
      </div>
    </nav>
  `;
}

function renderMeasurementMeta(element) {
  const meta = [];
  if (element.dwg_dim) meta.push(`DWG DIM: ${element.dwg_dim}`);
  if (element.gauge) meta.push(`Gauge: ${element.gauge}`);
  if (element.nominal !== null && element.nominal !== undefined && element.nominal !== "") {
    meta.push(`Nominal: ${element.nominal}`);
  }
  if (element.min !== null && element.min !== undefined && element.min !== "") {
    meta.push(`Min: ${element.min}`);
  }
  if (element.max !== null && element.max !== undefined && element.max !== "") {
    meta.push(`Max: ${element.max}`);
  }
  if (element.frequency || element.inspection_frequency) {
    meta.push(`Frequency: ${element.inspection_frequency || element.frequency}`);
  }
  return meta.map((item) => `<span class="pill">${escapeHtml(item)}</span>`).join("");
}

function buildCarriedValueConfig(element, priorMeasuredValue, numericEntryConfig = null) {
  if (priorMeasuredValue === null || priorMeasuredValue === undefined || priorMeasuredValue === "") return null;
  if (element.capture_type === "boolean") {
    const normalized = String(priorMeasuredValue).trim().toLowerCase();
    return {
      checked: ["yes", "y", "pass", "true", "1"].includes(normalized),
      hiddenValue: ["yes", "y", "pass", "true", "1"].includes(normalized) ? "Yes" : "No",
    };
  }

  const text = String(priorMeasuredValue).trim();
  if (numericEntryConfig) {
    const numericValue = Number(text);
    if (!Number.isNaN(numericValue)) {
      const formattedValue = formatNumericReference(numericValue, numericEntryConfig.decimals);
      return {
        fullValue: formattedValue,
        shortTail: formattedValue.slice(-numericEntryConfig.tailLength),
      };
    }
  }
  return {
    fullValue: text,
    shortTail: text.slice(-2),
  };
}

function renderInspectionInputControl(element, priorMeasuredValue = null) {
  const numericEntryConfig = element.capture_type === "boolean" ? null : buildNumericEntryConfig(element);
  const carriedValue = buildCarriedValueConfig(element, priorMeasuredValue, numericEntryConfig);
  if (element.capture_type === "boolean") {
    return `<label class="measurement-checkbox-row inspection-table-checkbox">
      <input
        type="checkbox"
        class="measurement-input measurement-checkbox"
        data-capture-type="boolean"
        data-full-target="element_full_${element.element_sequence}"
        ${carriedValue?.checked ? "checked" : ""}
      />
      <span>Pass</span>
      <input type="hidden" id="element_full_${element.element_sequence}" name="element_${element.element_sequence}" value="${escapeHtml(carriedValue?.hiddenValue || "No")}" />
    </label>`;
  }

  if (numericEntryConfig) {
    return `<div class="measurement-short-entry inspection-table-short-entry">
      <span class="measurement-prefix">${escapeHtml(numericEntryConfig.prefix)}</span>
      <input
        class="measurement-input measurement-short-input"
        data-capture-type="numeric"
        data-nominal="${escapeHtml(element.nominal ?? "")}"
        data-min="${escapeHtml(element.min ?? "")}"
        data-max="${escapeHtml(element.max ?? "")}"
        data-prefix="${escapeHtml(numericEntryConfig.prefix)}"
        data-tail-length="${escapeHtml(numericEntryConfig.tailLength)}"
        data-full-target="element_full_${element.element_sequence}"
        placeholder="${escapeHtml(numericEntryConfig.tailPlaceholder)}"
        value="${escapeHtml(carriedValue?.shortTail || "")}"
        inputmode="numeric"
        maxlength="${escapeHtml(numericEntryConfig.tailLength)}"
        required
      />
      <input type="hidden" id="element_full_${element.element_sequence}" name="element_${element.element_sequence}" value="${escapeHtml(carriedValue?.fullValue || "")}" />
    </div>`;
  }

  return `<input class="measurement-input inspection-table-input" data-capture-type="numeric" data-nominal="${escapeHtml(element.nominal ?? "")}" data-min="${escapeHtml(element.min ?? "")}" data-max="${escapeHtml(element.max ?? "")}" placeholder="${escapeHtml(element.nominal ?? "")}" value="${escapeHtml(carriedValue?.fullValue || "")}" name="element_${element.element_sequence}" required />`;
}

function renderCurrentAttemptWorksheet({ activeInspection, recipeDefinition, selection, sessionRecord, inspectorName, historyColumns = [] }) {
  const plan = activeInspection?.inspection_plan || [];
  const planMap = new Map(plan.map((element) => [Number(element.element_sequence), element]));
  const rows = recipeDefinition?.elements?.length ? recipeDefinition.elements : plan;
  const drawing = recipeDefinition?.drawing || "";
  const digitalIrrName = recipeDefinition?.display_name || formatDigitalIrrName(recipeDefinition?.recipe_name || "", drawing);
  const locationName = sessionRecord?.location_name || "";
  const currentPipeNumber = activeInspection?.pipe_number || selection.pipeNumber || "";
  const hasPendingNextPipe = !activeInspection && Boolean(currentPipeNumber);
  const previousMeasurementsBySequence = historyColumns.length
    ? historyColumns[historyColumns.length - 1].measurementsBySequence
    : new Map();
  const connectionCellForm = `
    <div class="connection-header-entry">
      <span class="connection-header-label">Pipe #</span>
      <div class="connection-header-form">
        <div class="pipe-number-stepper">
          <button type="button" class="pipe-number-step-button" data-pipe-step="-1" aria-label="Previous pipe number">-</button>
          <input type="text" name="pipeNumber" value="${escapeHtml(currentPipeNumber)}" placeholder="Enter pipe #" required data-pipe-number-entry />
          <button type="button" class="pipe-number-step-button" data-pipe-step="1" aria-label="Next pipe number">+</button>
        </div>
        <button type="button" class="button compact-button" data-start-pipe-button>${hasPendingNextPipe ? "Start Pipe" : "Load Pipe"}</button>
      </div>
    </div>
  `;

  return `
    <section class="card nested-card inspection-sheet-card">
      <h3 class="section-title">Current Attempt</h3>
      <p>${
        activeInspection
          ? `Attempt #${escapeHtml(activeInspection.attempt_no)} | ${activeInspection.is_rework ? "Re-work" : "First inspection"} | ${renderScopeBadge(activeInspection.inspection_scope)}`
          : hasPendingNextPipe
            ? "Next pipe is ready. Edit the pipe number if needed, then start it when inspection begins."
            : "Enter the pipe number in the table header to start or resume that pipe."
      }</p>
      ${
        activeInspection
          ? `<form method="post" action="/workflow/scope" class="inspection-scope-toggle-form">
              <input type="hidden" name="inspectionScope" value="${activeInspection.inspection_scope === "full" ? "standard" : "full"}" />
              <button class="button secondary compact-button" type="submit">
                ${activeInspection.inspection_scope === "full" ? "Switch to Standard Inspection" : "Switch to Full Inspection"}
              </button>
            </form>`
          : ""
      }
      ${
        activeInspection && !plan.length
          ? renderNotice({ kind: "warning", message: "This attempt has no measurement plan yet. Go back to the selection above and make sure a Digital IRR is selected before preparing the inspection." })
          : ""
      }
      <form id="worksheet-history-delete-form" method="post" action="/workflow/history/delete">
        <input type="hidden" id="worksheet-history-delete-pipe-unit-id" name="pipeUnitId" value="" />
      </form>
      <form id="inspection-complete-form" method="post" action="/workflow/complete" class="form-grid">
        <input type="hidden" name="productionNumber" value="${escapeHtml(selection.productionNumber || "")}" />
        <input type="hidden" name="sizeLabel" value="${escapeHtml(selection.sizeLabel || "")}" />
        <input type="hidden" name="weightLabel" value="${escapeHtml(selection.weightLabel || "")}" />
        <input type="hidden" name="connectionLabel" value="${escapeHtml(selection.connectionLabel || "")}" />
        <input type="hidden" name="endType" value="${escapeHtml(selection.endType || "")}" />
        <input type="hidden" name="recipeName" value="${escapeHtml(selection.recipeName || "")}" />
        <input type="hidden" name="inspectionScope" value="${escapeHtml(selection.inspectionScope || "standard")}" />
        <div class="inspection-sheet-meta">
          <div class="inspection-sheet-meta-row">
            <div><strong>Date:</strong> ${escapeHtml(formatDateValue(new Date()))}</div>
            <div><strong>Drawing #:</strong> ${escapeHtml(drawing)}</div>
            <div><strong>Machine #:</strong> ${escapeHtml(locationName)}</div>
          </div>
          <div class="inspection-sheet-meta-row">
            <div><strong>Inspector:</strong> ${escapeHtml(inspectorName || "")}</div>
            <div><strong>Workorder #:</strong> ${escapeHtml(selection.productionNumber || "")}</div>
            <div><strong>Connection Type:</strong> ${escapeHtml(buildInspectionConnectionLabel(selection))}</div>
          </div>
        </div>
        <div class="table-wrap inspection-sheet-wrap">
          <table class="inspection-sheet-table">
            <thead>
              <tr>
                <th class="inspection-col-num">#</th>
                <th>Element</th>
                <th>DWG DIM</th>
                <th>Gauge</th>
                ${historyColumns
                  .map(
                    (column) => `<th
                      class="inspection-col-history history-column-action"
                      data-history-pipe-id="${escapeHtml(column.pipeUnitId)}"
                      data-edit-url="/workflow/history/edit/${encodeURIComponent(column.pipeUnitId)}"
                      title="Click to edit this saved pipe. Right-click to delete it."
                    >
                      <div class="pipe-history-connection-header centered">
                        <span class="pipe-history-connection-label">Pipe # ${escapeHtml(column.pipe_number || "")}</span>
                        <div class="pipe-history-header-actions">
                          <a class="pipe-history-inline-action" href="/workflow/history/edit/${encodeURIComponent(column.pipeUnitId)}">Edit</a>
                          <button class="pipe-history-inline-action danger" type="button" data-delete-pipe-id="${escapeHtml(column.pipeUnitId)}">Delete</button>
                        </div>
                      </div>
                    </th>`,
                  )
                  .join("")}
                <th class="inspection-col-measure inspection-col-current">${connectionCellForm}</th>
              </tr>
            </thead>
            <tbody>
              ${rows
                .map((row) => {
                  const planned = planMap.get(Number(row.element_sequence));
                  const rowClass = planned
                    ? planned.inspection_frequency === "rotating"
                      ? "inspection-sheet-row-rotating"
                      : "inspection-sheet-row-active"
                    : hasPendingNextPipe
                      ? "inspection-sheet-row-next"
                    : "inspection-sheet-row-muted";
                  return `<tr class="${rowClass}">
                    <td class="inspection-col-num">${escapeHtml(row.element_sequence)}</td>
                    <td>${escapeHtml(row.element_description)}</td>
                    <td>${escapeHtml(row.dwg_dim || "")}</td>
                    <td>${escapeHtml(row.gauge || "")}</td>
                    ${historyColumns
                      .map((column) => {
                        const historicalMeasurement = column.measurementsBySequence?.get(Number(row.element_sequence));
                        return `<td
                          class="inspection-history-cell history-column-action ${historicalMeasurement ? "inspection-history-cell-filled" : "inspection-history-cell-empty"}"
                          data-history-pipe-id="${escapeHtml(column.pipeUnitId)}"
                          data-edit-url="/workflow/history/edit/${encodeURIComponent(column.pipeUnitId)}"
                          title="Click to edit this saved pipe. Right-click to delete it."
                        >${escapeHtml(historicalMeasurement?.measured_value ?? "")}</td>`;
                      })
                      .join("")}
                    <td class="inspection-measure-cell">
                      ${
                        planned
                          ? renderInspectionInputControl(planned, previousMeasurementsBySequence.get(Number(row.element_sequence))?.measured_value ?? null)
                          : activeInspection
                            ? `<span class="inspection-sheet-not-due">${escapeHtml(row.frequency === "rotating" ? "Rotating" : "")}</span>`
                            : hasPendingNextPipe
                              ? `<span class="inspection-sheet-not-due">Ready</span>`
                            : `<span class="inspection-sheet-not-due"></span>`
                      }
                    </td>
                  </tr>`;
                })
                .join("")}
            </tbody>
            <tfoot>
              <tr class="inspection-result-row">
                <td class="inspection-col-num inspection-result-label-piece inspection-result-label-num"></td>
                <td class="inspection-result-label-piece inspection-result-label-main"><strong>Inspection Result</strong></td>
                <td class="inspection-result-label-piece inspection-result-label-dwg"></td>
                <td class="inspection-result-label-piece inspection-result-label-gauge"></td>
                ${historyColumns
                  .map((column) => {
                    const resultLabel = formatPipeStatusResult(
                      column.status,
                      column.attemptStatus,
                      column.requiresManagerApproval,
                    );
                    return `<td
                      class="inspection-history-result-cell history-column-action"
                      data-history-pipe-id="${escapeHtml(column.pipeUnitId)}"
                      data-edit-url="/workflow/history/edit/${encodeURIComponent(column.pipeUnitId)}"
                      title="Click to edit this saved pipe. Right-click to delete it."
                    >${escapeHtml(resultLabel)}</td>`;
                  })
                  .join("")}
                <td class="inspection-result-cell">
                  <div id="inspection-table-result" class="inspection-table-result pending">
                    <span id="inspection-table-result-text"></span>
                    <small id="inspection-table-result-detail"></small>
                    <div id="inspection-result-fail-controls" class="inspection-result-fail-controls hidden">
                      <label for="failure-action-select"><strong>Fail Disposition</strong></label>
                      <select id="failure-action-select" name="failure_action">
                        <option value=""></option>
                        <option value="rework">Re-work</option>
                        <option value="manager_approved">Pass with Approval</option>
                      </select>
                    </div>
                  </div>
                </td>
              </tr>
            </tfoot>
          </table>
        </div>
        <div id="manager-approval-modal" class="approval-modal hidden" aria-hidden="true">
          <div class="approval-modal-backdrop" data-approval-close></div>
          <div class="approval-modal-panel">
            <div class="approval-modal-header">
              <h4>Pass with Approval</h4>
              <button type="button" class="approval-modal-close" data-approval-close aria-label="Close approval dialog">×</button>
            </div>
            <div class="form-grid two">
              <div class="field"><label>Manager Name</label><input id="manager-name-input" name="manager_name" /></div>
              <div class="field"><label>Reason</label><input id="manager-reason-input" name="manager_reason" /></div>
            </div>
            <div class="actions approval-modal-actions">
              <button type="submit" class="button">Submit Approval</button>
            </div>
          </div>
        </div>
        <div id="failure-fields" class="failure-fields hidden">
          <div class="field"><label>Tier Code</label><input name="tier_code" /></div>
          <div class="field"><label>Nonconformance</label><textarea name="nonconformance"></textarea></div>
          <div class="field"><label>Immediate Containment</label><textarea name="immediate_containment"></textarea></div>
        </div>
        <div class="field">
          <label>Relief / Coverage Notes</label>
          <textarea name="relief_note" placeholder="Example: Relieved inspector at 10:15. I inspected pipes 42-47."></textarea>
        </div>
        <div class="field"><label>Attempt Notes</label><textarea name="notes"></textarea></div>
        <div class="actions">${activeInspection ? `<button id="complete-inspection-button" class="button" type="submit">Complete Inspection</button>` : ""}</div>
      </form>
    </section>
  `;
}

function renderRecipeBuilderRows(builderOptions, rowCount = 25, visibleRowCount = 1) {
  const elementOptions = builderOptions?.element_options || [];
  const gaugeOptions = builderOptions?.gauge_options || [];
  const measurementModes = builderOptions?.measurement_modes || [];
  const frequencyOptions = builderOptions?.frequency_options || [];
  const visibleRows = Math.max(1, Math.min(Number(visibleRowCount) || 1, rowCount));

  return Array.from({ length: rowCount }, (_, index) => {
    const rowNumber = index + 1;
    return `
      <tr class="recipe-builder-row ${rowNumber > visibleRows ? "hidden" : ""}" data-recipe-row="${rowNumber}">
        <td>${rowNumber}</td>
        <td>
          <div class="recipe-builder-cell">
            <select name="row_${rowNumber}_element">
              <option value=""></option>
              ${renderOptions(elementOptions, "", (item) => item, (item) => item)}
            </select>
            <input name="row_${rowNumber}_element_custom" placeholder="Or enter new element" />
          </div>
        </td>
        <td>
          <select name="row_${rowNumber}_mode" class="recipe-mode-select" data-row="${rowNumber}">
            <option value=""></option>
            ${measurementModes.map((mode) => `<option value="${escapeHtml(mode.value)}">${escapeHtml(mode.label)}</option>`).join("")}
          </select>
        </td>
        <td>
          <div class="recipe-mode-fields" data-row="${rowNumber}">
            <div class="recipe-mode-panel hidden" data-mode-panel="nominal_tolerance">
              <input name="row_${rowNumber}_nominal" placeholder="Nominal" />
              <div class="recipe-mini-grid">
                <select name="row_${rowNumber}_tol_places">
                  <option value="3">3 dp</option>
                  <option value="4">4 dp</option>
                </select>
                <label class="recipe-decimal-input">
                  <span>+/- .</span>
                  <input name="row_${rowNumber}_tol_digits" placeholder="002" />
                </label>
              </div>
            </div>
            <div class="recipe-mode-panel hidden" data-mode-panel="asymmetric_tolerance">
              <input name="row_${rowNumber}_asym_nominal" placeholder="Nominal" />
              <div class="recipe-mini-grid">
                <label class="recipe-decimal-input">
                  <span>+ .</span>
                  <input name="row_${rowNumber}_plus_tolerance" placeholder="002" />
                </label>
                <label class="recipe-decimal-input">
                  <span>- .</span>
                  <input name="row_${rowNumber}_minus_tolerance" placeholder="002" />
                </label>
              </div>
            </div>
            <div class="recipe-mode-panel hidden" data-mode-panel="range">
              <div class="recipe-mini-grid">
                <input name="row_${rowNumber}_range_min" placeholder="Low" />
                <input name="row_${rowNumber}_range_max" placeholder="High" />
              </div>
            </div>
            <div class="recipe-mode-panel hidden" data-mode-panel="max_limit">
              <label class="recipe-decimal-input">
                <span>&lt;=</span>
                <input name="row_${rowNumber}_limit_max" placeholder="Max" />
              </label>
            </div>
            <div class="recipe-mode-panel hidden" data-mode-panel="min_limit">
              <label class="recipe-decimal-input">
                <span>&gt;=</span>
                <input name="row_${rowNumber}_limit_min" placeholder="Min" />
              </label>
            </div>
            <div class="recipe-mode-panel hidden" data-mode-panel="deviation">
              <div class="recipe-mini-grid">
                <select name="row_${rowNumber}_dev_places">
                  <option value="3">3 dp</option>
                  <option value="4">4 dp</option>
                </select>
                <label class="recipe-decimal-input">
                  <span>+/- .</span>
                  <input name="row_${rowNumber}_dev_digits" placeholder="002" />
                </label>
              </div>
            </div>
            <div class="recipe-mode-panel hidden" data-mode-panel="visual">
              <input name="row_${rowNumber}_visual_spec" placeholder="Visual spec / SOP" />
            </div>
          </div>
        </td>
        <td>
          <div class="recipe-builder-cell">
            <select name="row_${rowNumber}_gauge">
              <option value=""></option>
              ${renderOptions(gaugeOptions, "", (item) => item, (item) => item)}
            </select>
            <input name="row_${rowNumber}_gauge_custom" placeholder="Or enter new gauge" />
          </div>
        </td>
        <td>
          <div class="recipe-frequency-radio">
            ${frequencyOptions
              .map(
                (option) => `<label><input type="radio" name="row_${rowNumber}_frequency" value="${escapeHtml(option.value)}" ${option.value === "every_pipe" ? "checked" : ""} /> ${escapeHtml(option.label)}</label>`,
              )
              .join("")}
          </div>
        </td>
      </tr>
    `;
  }).join("");
}

function buildSubmittedRecipePayload(req, rowCount = 25) {
  const rows = Array.from({ length: rowCount }, (_, index) => {
    const rowNumber = index + 1;
    const measurementMode = req.body[`row_${rowNumber}_mode`] || "";
    const selectedElement = req.body[`row_${rowNumber}_element`] || "";
    const customElement = req.body[`row_${rowNumber}_element_custom`] || "";
    const selectedGauge = req.body[`row_${rowNumber}_gauge`] || "";
    const customGauge = req.body[`row_${rowNumber}_gauge_custom`] || "";
    return {
      element_sequence: rowNumber,
      element_description: String(customElement || selectedElement).trim(),
      measurement_mode: measurementMode,
      gauge: String(customGauge || selectedGauge).trim(),
      frequency: req.body[`row_${rowNumber}_frequency`] || "every_pipe",
      nominal_value:
        measurementMode === "asymmetric_tolerance"
          ? req.body[`row_${rowNumber}_asym_nominal`] || ""
          : req.body[`row_${rowNumber}_nominal`] || "",
      tolerance_decimal_places:
        measurementMode === "deviation"
          ? req.body[`row_${rowNumber}_dev_places`] || "3"
          : req.body[`row_${rowNumber}_tol_places`] || "3",
      tolerance_digits:
        measurementMode === "deviation"
          ? req.body[`row_${rowNumber}_dev_digits`] || ""
          : req.body[`row_${rowNumber}_tol_digits`] || "",
      plus_tolerance: req.body[`row_${rowNumber}_plus_tolerance`] || "",
      minus_tolerance: req.body[`row_${rowNumber}_minus_tolerance`] || "",
      range_min: req.body[`row_${rowNumber}_range_min`] || "",
      range_max: req.body[`row_${rowNumber}_range_max`] || "",
      limit_max: req.body[`row_${rowNumber}_limit_max`] || "",
      limit_min: req.body[`row_${rowNumber}_limit_min`] || "",
      visual_spec: req.body[`row_${rowNumber}_visual_spec`] || "",
    };
  });

  return {
    branch: "",
    size_label: req.body.size_label,
    weight_label: req.body.weight_label,
    first_article_label: req.body.first_article_label,
    grade_label: req.body.grade_label,
    connector_type: req.body.connector_type,
    drawing: req.body.drawing,
    source_report: req.body.source_report,
    edit_comment: req.body.edit_comment,
    created_by: req.session.inspector.name,
    rows,
  };
}

function renderRecipeBuilderRowsWithValues(builderOptions, existingRows = [], rowCount = 25, visibleRowCount = null) {
  const rowsBySequence = new Map(existingRows.map((row) => [Number(row.element_sequence), row]));
  const elementOptions = builderOptions?.element_options || [];
  const gaugeOptions = builderOptions?.gauge_options || [];
  const measurementModes = builderOptions?.measurement_modes || [];
  const frequencyOptions = builderOptions?.frequency_options || [];
  const rowHasValue = (row) =>
    Boolean(
      row.element_description ||
        row.measurement_mode ||
        row.gauge ||
        row.nominal_value ||
        row.nominal ||
        row.tolerance_digits ||
        row.plus_tolerance ||
        row.minus_tolerance ||
        row.range_min ||
        row.range_max ||
        row.min_value ||
        row.max_value ||
        row.limit_max ||
        row.limit_min ||
        row.visual_spec ||
        row.dwg_dim,
    );
  const highestFilledRow = existingRows.reduce(
    (highest, row) => (rowHasValue(row) ? Math.max(highest, Number(row.element_sequence) || 0) : highest),
    0,
  );
  const visibleRows = Math.max(
    1,
    Math.min(
      Number(visibleRowCount) || highestFilledRow || 1,
      rowCount,
    ),
  );

  return Array.from({ length: rowCount }, (_, index) => {
    const rowNumber = index + 1;
    const row = rowsBySequence.get(rowNumber) || {};
    const selectedElement = elementOptions.includes(row.element_description) ? row.element_description : "";
    const customElement = selectedElement ? "" : (row.element_description || "");
    const selectedGauge = gaugeOptions.includes(row.gauge) ? row.gauge : "";
    const customGauge = selectedGauge ? "" : (row.gauge || "");
    const nominalValue = row.nominal_value ?? row.nominal ?? "";
    const toleranceDecimalPlaces = String(row.tolerance_decimal_places || "3");
    const toleranceDigits = row.tolerance_digits || "";
    const rangeMin = row.range_min ?? row.min_value ?? "";
    const rangeMax = row.range_max ?? row.max_value ?? "";
    const limitMax = row.limit_max ?? row.max_value ?? "";
    const limitMin = row.limit_min ?? row.min_value ?? "";
    const visualSpec = row.visual_spec ?? row.dwg_dim ?? "";
    const nominalToleranceDigits = formatDecimalTailForInput(toleranceDigits);
    const plusTolerance =
      row.plus_tolerance !== undefined
        ? formatDecimalTailForInput(row.plus_tolerance)
        : row.measurement_mode === "asymmetric_tolerance" && row.nominal !== null && row.nominal !== undefined && row.max_value !== null && row.max_value !== undefined
        ? formatDecimalTailForInput(formatNumberForInput(Number(row.max_value) - Number(row.nominal)))
        : "";
    const minusTolerance =
      row.minus_tolerance !== undefined
        ? formatDecimalTailForInput(row.minus_tolerance)
        : row.measurement_mode === "asymmetric_tolerance" && row.nominal !== null && row.nominal !== undefined && row.min_value !== null && row.min_value !== undefined
        ? formatDecimalTailForInput(formatNumberForInput(Number(row.nominal) - Number(row.min_value)))
        : "";

    return `
      <tr class="recipe-builder-row ${rowNumber > visibleRows ? "hidden" : ""}" data-recipe-row="${rowNumber}">
        <td>${rowNumber}</td>
        <td>
          <div class="recipe-builder-cell">
            <select name="row_${rowNumber}_element">
              <option value=""></option>
              ${renderOptions(elementOptions, selectedElement, (item) => item, (item) => item)}
            </select>
            <input name="row_${rowNumber}_element_custom" placeholder="Or enter new element" value="${escapeHtml(customElement)}" />
          </div>
        </td>
        <td>
          <select name="row_${rowNumber}_mode" class="recipe-mode-select" data-row="${rowNumber}">
            <option value=""></option>
            ${measurementModes.map((mode) => `<option value="${escapeHtml(mode.value)}" ${row.measurement_mode === mode.value ? "selected" : ""}>${escapeHtml(mode.label)}</option>`).join("")}
          </select>
        </td>
        <td>
          <div class="recipe-mode-fields" data-row="${rowNumber}">
            <div class="recipe-mode-panel ${row.measurement_mode === "nominal_tolerance" ? "" : "hidden"}" data-mode-panel="nominal_tolerance">
              <input name="row_${rowNumber}_nominal" placeholder="Nominal" value="${escapeHtml(nominalValue)}" />
              <div class="recipe-mini-grid">
                <select name="row_${rowNumber}_tol_places">
                  <option value="3" ${toleranceDecimalPlaces === "3" ? "selected" : ""}>3 dp</option>
                  <option value="4" ${toleranceDecimalPlaces === "4" ? "selected" : ""}>4 dp</option>
                </select>
                <label class="recipe-decimal-input">
                  <span>+/- .</span>
                  <input name="row_${rowNumber}_tol_digits" placeholder="002" value="${escapeHtml(nominalToleranceDigits)}" />
                </label>
              </div>
            </div>
            <div class="recipe-mode-panel ${row.measurement_mode === "asymmetric_tolerance" ? "" : "hidden"}" data-mode-panel="asymmetric_tolerance">
              <input name="row_${rowNumber}_asym_nominal" placeholder="Nominal" value="${escapeHtml(nominalValue)}" />
              <div class="recipe-mini-grid">
                <label class="recipe-decimal-input">
                  <span>+ .</span>
                  <input name="row_${rowNumber}_plus_tolerance" placeholder="002" value="${escapeHtml(plusTolerance)}" />
                </label>
                <label class="recipe-decimal-input">
                  <span>- .</span>
                  <input name="row_${rowNumber}_minus_tolerance" placeholder="002" value="${escapeHtml(minusTolerance)}" />
                </label>
              </div>
            </div>
            <div class="recipe-mode-panel ${row.measurement_mode === "range" ? "" : "hidden"}" data-mode-panel="range">
              <div class="recipe-mini-grid">
                <input name="row_${rowNumber}_range_min" placeholder="Low" value="${escapeHtml(rangeMin)}" />
                <input name="row_${rowNumber}_range_max" placeholder="High" value="${escapeHtml(rangeMax)}" />
              </div>
            </div>
            <div class="recipe-mode-panel ${row.measurement_mode === "max_limit" ? "" : "hidden"}" data-mode-panel="max_limit">
              <label class="recipe-decimal-input">
                <span>&lt;=</span>
                <input name="row_${rowNumber}_limit_max" placeholder="Max" value="${escapeHtml(limitMax)}" />
              </label>
            </div>
            <div class="recipe-mode-panel ${row.measurement_mode === "min_limit" ? "" : "hidden"}" data-mode-panel="min_limit">
              <label class="recipe-decimal-input">
                <span>&gt;=</span>
                <input name="row_${rowNumber}_limit_min" placeholder="Min" value="${escapeHtml(limitMin)}" />
              </label>
            </div>
            <div class="recipe-mode-panel ${row.measurement_mode === "deviation" ? "" : "hidden"}" data-mode-panel="deviation">
              <div class="recipe-mini-grid">
                <select name="row_${rowNumber}_dev_places">
                  <option value="3" ${toleranceDecimalPlaces === "3" ? "selected" : ""}>3 dp</option>
                  <option value="4" ${toleranceDecimalPlaces === "4" ? "selected" : ""}>4 dp</option>
                </select>
                <label class="recipe-decimal-input">
                  <span>+/- .</span>
                  <input name="row_${rowNumber}_dev_digits" placeholder="002" value="${escapeHtml(toleranceDigits)}" />
                </label>
              </div>
            </div>
            <div class="recipe-mode-panel ${row.measurement_mode === "visual" ? "" : "hidden"}" data-mode-panel="visual">
              <input name="row_${rowNumber}_visual_spec" placeholder="Visual spec / SOP" value="${escapeHtml(visualSpec)}" />
            </div>
          </div>
        </td>
        <td>
          <div class="recipe-builder-cell">
            <select name="row_${rowNumber}_gauge">
              <option value=""></option>
              ${renderOptions(gaugeOptions, selectedGauge, (item) => item, (item) => item)}
            </select>
            <input name="row_${rowNumber}_gauge_custom" placeholder="Or enter new gauge" value="${escapeHtml(customGauge)}" />
          </div>
        </td>
        <td>
          <div class="recipe-frequency-radio">
            ${frequencyOptions
              .map(
                (option) => `<label><input type="radio" name="row_${rowNumber}_frequency" value="${escapeHtml(option.value)}" ${(row.frequency || "every_pipe") === option.value ? "checked" : ""} /> ${escapeHtml(option.label)}</label>`,
              )
              .join("")}
          </div>
        </td>
      </tr>
    `;
  }).join("");
}

function renderWorkflowHeader(req) {
  const inspector = req.session.inspector;
  return `
    <div class="topbar">
      <div class="hero workflow-hero" style="flex:1">
        <h1>Inspection Run Report Workflow</h1>
        <p>Run inspections, review pipe history, and manage NCR follow-up from one workspace.</p>
        <div class="badges">
          <span class="badge">Inspector: ${escapeHtml(inspector.name)}</span>
          <span class="badge">Role: ${escapeHtml(req.session.roleLabel || "Inspector")}</span>
          <span class="badge">Branch: ${escapeHtml(inspector.branch || "Unknown")}</span>
          <span class="badge">Department: ${escapeHtml(inspector.department || "Unknown")}</span>
          <span class="badge">Shift: ${escapeHtml(req.session.sessionShift || "Day")}</span>
        </div>
      </div>
    </div>
  `;
}

function layout({ title, sidebar, content, theme = "Light" }) {
  return `<!doctype html>
  <html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(title)}</title>
    <link rel="stylesheet" href="/public/styles.css" />
    </head>
    <body data-theme="${theme === "Dark" ? "dark" : "light"}">
      <div class="benoit-app-frame">
        <header class="benoit-topbar">
          <div class="brand-block">
            <div class="brand-logo-panel"><img src="/public/BenoitLogoRegistered-Red.png" alt="Benoit" /></div>
            <div class="brand-copy">
              <span>Inspection Run Report</span>
              <small>Benoit Connect · Ennis</small>
            </div>
          </div>
          <div class="topbar-product">Quality Operations</div>
        </header>
        <div class="app-shell">
        <aside class="sidebar">
          <button type="button" class="sidebar-toggle" id="sidebar-toggle" aria-label="Toggle sidebar">«</button>
          <div class="sidebar-inner">${sidebar}</div>
        </aside>
          <main class="content">${content}</main>
        </div>
      </div>
      ${renderPopupNoticeModal()}
      <script>
        (function () {
          const popupNotice = document.querySelector(".notice[data-popup='true']");
          const popupNoticeModal = document.getElementById("popup-notice-modal");
          const popupNoticeMessage = document.getElementById("popup-notice-message");
          const popupNoticeOkButton = document.getElementById("popup-notice-ok-button");
          const popupNoticeCloseButtons = document.querySelectorAll("[data-popup-notice-close]");
          const adminLoginToggle = document.querySelector("[data-admin-login-toggle]");
          const adminLoginLocationField = document.querySelector("[data-admin-login-field='location']");
          const adminLoginLocationSelect = adminLoginLocationField ? adminLoginLocationField.querySelector("select") : null;
          const floatingTabletLocationSelects = document.querySelectorAll("select[name='location_id']");
          const setPopupNoticeVisible = (visible) => {
            if (!popupNoticeModal) return;
            popupNoticeModal.classList.toggle("hidden", !visible);
            popupNoticeModal.setAttribute("aria-hidden", visible ? "false" : "true");
          };
          const updateAdminLoginFields = () => {
            if (!adminLoginToggle || !adminLoginLocationField || !adminLoginLocationSelect) return;
            const isAdminLogin = adminLoginToggle.checked;
            adminLoginLocationField.classList.toggle("hidden", isAdminLogin);
            adminLoginLocationSelect.disabled = isAdminLogin;
            adminLoginLocationSelect.dispatchEvent(new Event("change"));
          };
          if (adminLoginToggle) {
            adminLoginToggle.addEventListener("change", updateAdminLoginFields);
            updateAdminLoginFields();
          }
          floatingTabletLocationSelects.forEach((select) => {
            const form = select.closest("form");
            const noteField = form ? form.querySelector("[data-floating-tablet-note-field]") : null;
            const noteInput = noteField ? noteField.querySelector("textarea") : null;
            const updateFloatingTabletNote = () => {
              const selectedOption = select.selectedOptions && select.selectedOptions.length ? select.selectedOptions[0] : null;
              const shouldShow = Boolean(!select.disabled && selectedOption && selectedOption.dataset.floatingTablet === "1");
              if (noteField) noteField.classList.toggle("hidden", !shouldShow);
              if (noteInput) {
                noteInput.disabled = !shouldShow;
                if (!shouldShow) noteInput.value = "";
              }
            };
            select.addEventListener("change", updateFloatingTabletNote);
            updateFloatingTabletNote();
          });
          if (popupNotice && popupNoticeModal && popupNoticeMessage) {
            popupNoticeMessage.textContent = popupNotice.textContent.trim();
            window.setTimeout(() => {
              setPopupNoticeVisible(true);
              if (popupNoticeOkButton) popupNoticeOkButton.focus();
            }, 0);
          }
          popupNoticeCloseButtons.forEach((button) =>
            button.addEventListener("click", () => setPopupNoticeVisible(false)),
          );
          if (popupNoticeOkButton) {
            popupNoticeOkButton.addEventListener("click", () => setPopupNoticeVisible(false));
          }
          document.addEventListener("keydown", (event) => {
            if (event.key === "Escape" && popupNoticeModal && !popupNoticeModal.classList.contains("hidden")) {
              setPopupNoticeVisible(false);
            }
          });
          const body = document.body;
          const key = "autoirr-node-sidebar-collapsed";
          const button = document.getElementById("sidebar-toggle");
          const apply = (collapsed) => {
            body.classList.toggle("sidebar-collapsed", collapsed);
            if (button) {
              button.textContent = collapsed ? "»" : "«";
            }
          };
          const saved = window.localStorage.getItem(key) === "true";
          apply(saved);
          if (button) {
            button.addEventListener("click", function () {
              const next = !body.classList.contains("sidebar-collapsed");
              window.localStorage.setItem(key, String(next));
              apply(next);
            });
          }
          document.querySelectorAll("[data-irr-import-form]").forEach((form) => {
            form.addEventListener("submit", (event) => {
              const fileInput = form.querySelector("[data-irr-import-file]");
              const nameInput = form.querySelector("input[name='file_name']");
              const contentInput = form.querySelector("input[name='file_content']");
              const submitButton = form.querySelector("button[type='submit']");
              const file = fileInput?.files?.[0];
              if (!file || !nameInput || !contentInput || contentInput.value) return;
              event.preventDefault();
              if (file.size > 8 * 1024 * 1024) {
                window.alert("Choose a report smaller than 8 MB.");
                return;
              }
              if (submitButton) {
                submitButton.disabled = true;
                submitButton.textContent = "Reading Report...";
              }
              const reader = new FileReader();
              reader.addEventListener("load", () => {
                nameInput.value = file.name;
                contentInput.value = String(reader.result || "").split(",", 2)[1] || "";
                form.submit();
              });
              reader.addEventListener("error", () => {
                if (submitButton) {
                  submitButton.disabled = false;
                  submitButton.textContent = "Create Draft";
                }
                window.alert("That report could not be read.");
              });
              reader.readAsDataURL(file);
            });
          });
        })();

        (function () {
          const initializeInspectionWorkspace = (root = document) => {
          const inspectionSheetWrap = root.querySelector(".inspection-sheet-wrap");
          if (inspectionSheetWrap) {
            requestAnimationFrame(() => {
              requestAnimationFrame(() => {
                inspectionSheetWrap.scrollLeft = inspectionSheetWrap.scrollWidth;
              });
            });
          }

          const historyActionCells = root.querySelectorAll(".history-column-action");
          const historyDeleteForm =
            root.querySelector("#worksheet-history-delete-form") ||
            root.querySelector("#pipe-history-sheet-delete-form");
          const historyDeletePipeInput =
            root.querySelector("#worksheet-history-delete-pipe-unit-id") ||
            root.querySelector("#pipe-history-sheet-delete-pipe-unit-id");
          const historyRedirectInput = historyDeleteForm ? historyDeleteForm.querySelector("[data-current-history-url]") : null;
          if (historyRedirectInput) {
            historyRedirectInput.value = window.location.pathname + window.location.search;
          }
          const submitHistoryDelete = async (pipeUnitId) => {
            if (!pipeUnitId || !historyDeleteForm || !historyDeletePipeInput) return;
            historyDeletePipeInput.value = pipeUnitId;
            if (historyRedirectInput) {
              historyRedirectInput.value = window.location.pathname + window.location.search;
            }
            if (!window.fetch || !window.DOMParser || historyDeleteForm.id !== "pipe-history-sheet-delete-form") {
              historyDeleteForm.submit();
              return;
            }
            const currentScrollY = window.scrollY;
            try {
              const response = await fetch(historyDeleteForm.action, {
                method: "POST",
                body: new URLSearchParams(new FormData(historyDeleteForm)),
                credentials: "same-origin",
              });
              const html = await response.text();
              if (!response.ok) throw new Error("Delete failed.");
              const nextDocument = new DOMParser().parseFromString(html, "text/html");
              const nextList = nextDocument.querySelector(".pipe-history-sheet-list");
              const currentList = document.querySelector(".pipe-history-sheet-list");
              const nextNotice = nextDocument.querySelector(".notice");
              const currentNotice = document.querySelector(".notice");
              if (!nextList || !currentList) throw new Error("History response was missing the table.");
              currentList.replaceWith(nextList);
              if (nextNotice && currentNotice) currentNotice.replaceWith(nextNotice);
              initializeInspectionWorkspace(document);
              window.scrollTo({ top: currentScrollY, left: window.scrollX });
            } catch (error) {
              historyDeleteForm.submit();
            }
          };
          const historyActionMenus = root.querySelectorAll(".pipe-history-action-menu, .pipe-history-action-menu-toggle, .pipe-history-action-menu-panel");

          historyActionMenus.forEach((element) => {
            element.addEventListener("click", (event) => {
              event.stopPropagation();
            });
            element.addEventListener("contextmenu", (event) => {
              event.stopPropagation();
            });
          });

          historyActionCells.forEach((cell) => {
            cell.addEventListener("click", (event) => {
              if (event.target.closest(".pipe-history-action-menu")) {
                return;
              }
              if (event.target.closest(".pipe-history-inline-action")) {
                return;
              }
              const editUrl = cell.dataset.editUrl;
              if (!editUrl) return;
              window.location.href = editUrl;
            });

            cell.addEventListener("contextmenu", (event) => {
              if (event.target.closest(".pipe-history-action-menu")) {
                return;
              }
              const pipeUnitId = cell.dataset.historyPipeId;
              if (!pipeUnitId || !historyDeleteForm || !historyDeletePipeInput) return;
              event.preventDefault();
              if (!window.confirm("Delete this saved pipe inspection and all related attempts, measurements, and NCR records?")) {
                return;
              }
              submitHistoryDelete(pipeUnitId);
            });
          });
          root.querySelectorAll("[data-delete-pipe-id]").forEach((button) => {
            button.addEventListener("click", (event) => {
              event.preventDefault();
              event.stopPropagation();
              const pipeUnitId = button.dataset.deletePipeId;
              if (!pipeUnitId || !historyDeleteForm || !historyDeletePipeInput) return;
              if (!window.confirm("Delete this saved pipe inspection and all related attempts, measurements, and NCR records?")) {
                return;
              }
              submitHistoryDelete(pipeUnitId);
            });
          });

          const inputs = root.querySelectorAll(".measurement-input");
          const modeSelects = root.querySelectorAll(".recipe-mode-select");
          const addRecipeRowButtons = root.querySelectorAll("[data-add-recipe-row]");
          const pipeNumberStepButtons = root.querySelectorAll("[data-pipe-step]");
          const pipeNumberEntries = root.querySelectorAll("[data-pipe-number-entry]");
          const startPipeButtons = root.querySelectorAll("[data-start-pipe-button]");
          const scopeToggleForms = root.querySelectorAll(".inspection-scope-toggle-form");
          const replaceInspectionWorkspace = (html, currentScrollY, focusSelector) => {
            const nextDocument = new DOMParser().parseFromString(html, "text/html");
            const nextWorkspace = nextDocument.getElementById("inspection-workspace");
            const popupNotice = nextDocument.querySelector(".notice[data-popup='true']");
            if (!nextWorkspace) throw new Error("Pipe load response was missing the inspection workspace.");
            const workspace = document.getElementById("inspection-workspace");
            if (!workspace) throw new Error("Inspection workspace is missing.");
            workspace.replaceWith(nextWorkspace);
            initializeInspectionWorkspace(nextWorkspace);
            if (popupNotice) {
              window.alert(popupNotice.textContent.trim());
            }
            window.scrollTo({ top: currentScrollY, left: window.scrollX });
            const focusTarget = focusSelector ? nextWorkspace.querySelector(focusSelector) : null;
            if (focusTarget) {
              focusTarget.focus();
              if (typeof focusTarget.select === "function" && !focusTarget.classList.contains("measurement-checkbox")) {
                focusTarget.select();
              }
            }
          };
          const snapshotInspectionValues = () => {
            const values = new Map();
            document.querySelectorAll("#inspection-complete-form [name^='element_']").forEach((input) => {
              values.set(input.name, input.type === "checkbox" ? input.checked : input.value);
            });
            return values;
          };
          const restoreInspectionValues = (workspace, values) => {
            values.forEach((value, name) => {
              const input = workspace.querySelector("#inspection-complete-form [name='" + CSS.escape(name) + "']");
              if (!input) return;
              if (input.type === "checkbox") {
                input.checked = Boolean(value);
              } else {
                input.value = value;
              }
              input.dispatchEvent(new Event("input", { bubbles: true }));
              input.dispatchEvent(new Event("change", { bubbles: true }));
            });
          };
          const stepPipeNumber = (value, amount) => {
            const text = String(value || "").trim();
            const match = text.match(/^(.*?)(\\d+)$/);
            if (!match) return text;
            const prefix = match[1];
            const digits = match[2];
            const nextNumber = Math.max(0, Number(digits) + amount);
            const nextText = String(nextNumber);
            const nextDigits = digits.length > nextText.length ? nextText.padStart(digits.length, "0") : nextText;
            return prefix + nextDigits;
          };
          const updateRecipeAddButtons = () => {
            addRecipeRowButtons.forEach((button) => {
              const form = button.closest("form");
              if (!form) return;
              const hiddenRows = form.querySelectorAll(".recipe-builder-row.hidden");
              button.disabled = hiddenRows.length === 0;
              button.textContent = hiddenRows.length === 0 ? "All Elements Added" : "Add Element";
            });
          };
          if (modeSelects.length) {
            const syncRecipeModeRow = (select) => {
              const row = select.dataset.row;
              const wrapper = root.querySelector('.recipe-mode-fields[data-row="' + row + '"]');
              if (!wrapper) return;
              wrapper.querySelectorAll(".recipe-mode-panel").forEach((panel) => {
                panel.classList.toggle("hidden", panel.dataset.modePanel !== select.value);
              });
            };
            modeSelects.forEach((select) => {
              select.addEventListener("change", () => syncRecipeModeRow(select));
              syncRecipeModeRow(select);
            });
          }
          if (addRecipeRowButtons.length) {
            addRecipeRowButtons.forEach((button) => {
              button.addEventListener("click", () => {
                const form = button.closest("form");
                if (!form) return;
                const nextRow = form.querySelector(".recipe-builder-row.hidden");
                if (!nextRow) {
                  updateRecipeAddButtons();
                  return;
                }
                nextRow.classList.remove("hidden");
                const firstInput = nextRow.querySelector("select, input");
                if (firstInput) firstInput.focus();
                updateRecipeAddButtons();
              });
            });
            updateRecipeAddButtons();
          }
          pipeNumberStepButtons.forEach((button) => {
            button.addEventListener("click", () => {
              const wrapper = button.closest(".pipe-number-stepper");
              const input = wrapper ? wrapper.querySelector("[data-pipe-number-entry]") : null;
              if (!input) return;
              const nextValue = stepPipeNumber(input.value, Number(button.dataset.pipeStep || 0));
              input.value = nextValue;
              input.focus();
              if (typeof input.select === "function") input.select();
              input.dispatchEvent(new Event("input", { bubbles: true }));
            });
          });
          pipeNumberEntries.forEach((entry) => {
            entry.addEventListener("keydown", (event) => {
              if (event.key !== "Enter") return;
              event.preventDefault();
              const button = root.querySelector("[data-start-pipe-button]");
              if (button) button.click();
            });
          });
          startPipeButtons.forEach((button) => {
            button.addEventListener("click", async () => {
              const form = button.closest("form");
              const entry = form ? form.querySelector("[data-pipe-number-entry]") : root.querySelector("[data-pipe-number-entry]");
              if (!form || !entry) return;
              if (!entry.reportValidity()) return;
              const currentScrollY = window.scrollY;
              const previousButtonText = button.textContent;
              button.disabled = true;
              button.textContent = "Loading...";
              try {
                const formData = new FormData(form);
                formData.set("pipeNumber", entry.value);
                const response = await fetch("/workflow/start", {
                  method: "POST",
                  body: new URLSearchParams(formData),
                  credentials: "same-origin",
                });
                const html = await response.text();
                if (!response.ok) throw new Error("Pipe load failed.");
                replaceInspectionWorkspace(
                  html,
                  currentScrollY,
                  ".measurement-short-input, .inspection-table-input, .measurement-checkbox, [data-pipe-number-entry]",
                );
              } catch (error) {
                form.action = "/workflow/start";
                form.method = "post";
                form.noValidate = true;
                form.submit();
              } finally {
                button.disabled = false;
                button.textContent = previousButtonText;
              }
            });
          });
          scopeToggleForms.forEach((form) => {
            form.addEventListener("submit", async (event) => {
              if (!window.fetch || !window.DOMParser) return;
              event.preventDefault();
              const button = event.submitter || form.querySelector('button[type="submit"]');
              const previousButtonText = button ? button.textContent : "";
              const currentScrollY = window.scrollY;
              const measurementValues = snapshotInspectionValues();
              if (button) {
                button.disabled = true;
                button.textContent = "Switching...";
              }
              try {
                const response = await fetch(form.action, {
                  method: "POST",
                  body: new URLSearchParams(new FormData(form)),
                  credentials: "same-origin",
                });
                const html = await response.text();
                if (!response.ok) throw new Error("Scope switch failed.");
                replaceInspectionWorkspace(html, currentScrollY, null);
                const nextWorkspace = document.getElementById("inspection-workspace");
                if (nextWorkspace) restoreInspectionValues(nextWorkspace, measurementValues);
              } catch (error) {
                form.submit();
              } finally {
                if (button) {
                  button.disabled = false;
                  button.textContent = previousButtonText;
                }
              }
            });
          });

          if (!inputs.length) return;
          const resultPanel = root.querySelector("#inspection-table-result");
          const resultText = root.querySelector("#inspection-table-result-text");
          const resultDetail = root.querySelector("#inspection-table-result-detail");
          const failureActionSelect = root.querySelector("#failure-action-select");
          const failureControls = root.querySelector("#inspection-result-fail-controls");
          const failureFields = root.querySelector("#failure-fields");
          const managerApprovalModal = root.querySelector("#manager-approval-modal");
          const managerNameInput = root.querySelector("#manager-name-input");
          const managerReasonInput = root.querySelector("#manager-reason-input");
          const completeInspectionButton = root.querySelector("#complete-inspection-button");
          const approvalCloseButtons = root.querySelectorAll("[data-approval-close]");
          const focusableMeasurementInputs = Array.from(
            root.querySelectorAll(".measurement-short-input, .inspection-table-input, .measurement-checkbox"),
          );

          const setApprovalModalVisible = (visible) => {
            if (!managerApprovalModal) return;
            managerApprovalModal.classList.toggle("hidden", !visible);
            managerApprovalModal.setAttribute("aria-hidden", visible ? "false" : "true");
          };

          const getEffectiveValue = (element) => {
            const normalizeNumericTextForEntry = (value) => {
              const text = String(value ?? "").trim();
              if (!text) return "";
              if (!/^-?\\d+(?:\\.\\d+)?$/.test(text)) return text;
              return text.replace(/(\\.\\d*?)0+$/, "$1").replace(/\\.$/, "");
            };
            const countDecimalPlacesForEntry = (value) => {
              const text = normalizeNumericTextForEntry(value);
              if (!text.includes(".")) return 0;
              return text.split(".")[1].length;
            };
            const normalizeNumericMeasurementValue = (rawValue, targetElement) => {
              const text = String(rawValue ?? "").trim();
              if (!text) return "";
              const nominalText = String(targetElement.dataset.nominal || "").trim();
              const minText = String(targetElement.dataset.min || "").trim();
              const maxText = String(targetElement.dataset.max || "").trim();
              const unsignedOffsetText = /^\.?\d+$/.test(text) && !text.includes(".");
              const isOffsetEntry =
                nominalText !== "" &&
                !Number.isNaN(Number(nominalText)) &&
                (/^[+-]/.test(text) ||
                  text.startsWith(".") ||
                  unsignedOffsetText);
              if (isOffsetEntry) {
                const sign = text.startsWith("-") ? -1 : 1;
                const offsetText = /^[+-]/.test(text) ? text.slice(1).trim() : text;
                if (!offsetText) return "";
                let offset;
                if (offsetText.startsWith(".")) {
                  offset = Number(offsetText);
                } else if (/^\d+$/.test(offsetText)) {
                  const decimalPlaces = Math.max(
                    countDecimalPlacesForEntry(nominalText),
                    countDecimalPlacesForEntry(minText),
                    countDecimalPlacesForEntry(maxText),
                    3,
                  );
                  offset = Number(offsetText) / Math.pow(10, decimalPlaces);
                } else {
                  offset = Number(offsetText);
                }
                if (Number.isNaN(offset)) return "";
                const decimals = Math.max(
                  countDecimalPlacesForEntry(nominalText),
                  countDecimalPlacesForEntry(minText),
                  countDecimalPlacesForEntry(maxText),
                  3,
                );
                return normalizeNumericTextForEntry((Number(nominalText) + sign * offset).toFixed(decimals));
              }
              return text;
            };
            if (element.classList.contains("measurement-short-input")) {
              const hiddenTarget = document.getElementById(element.dataset.fullTarget || "");
              const rawDigits = String(element.value ?? "").replace(/\D/g, "");
              const tailLength = Number(element.dataset.tailLength || 0);
              if (tailLength && rawDigits.length !== tailLength) {
                if (hiddenTarget) hiddenTarget.value = "";
                return "";
              }
              const prefix = String(element.dataset.prefix || "");
              const fullValue = prefix + rawDigits;
              if (hiddenTarget) hiddenTarget.value = fullValue;
              return fullValue;
            }
            if (element.classList.contains("measurement-checkbox")) {
              const hiddenTarget = document.getElementById(element.dataset.fullTarget || "");
              const checkboxValue = element.checked ? "Yes" : "No";
              if (hiddenTarget) hiddenTarget.value = checkboxValue;
              return checkboxValue;
            }
            return normalizeNumericMeasurementValue(element.value, element);
          };

          const isFilled = (element) => {
            if (element.classList.contains("measurement-checkbox")) {
              getEffectiveValue(element);
              return true;
            }
            return getEffectiveValue(element) !== "";
          };

          const applyState = (element, state) => {
            element.classList.remove("measurement-pass", "measurement-fail");
            if (state === "pass") element.classList.add("measurement-pass");
            if (state === "fail") element.classList.add("measurement-fail");
          };

          const updateOverallResult = () => {
            const states = Array.from(inputs).map((element) => {
              if (element.classList.contains("measurement-fail")) return "fail";
              if (element.classList.contains("measurement-pass")) return "pass";
              return "pending";
            });
            const allFilled = Array.from(inputs).every((element) => isFilled(element));
            const anyFail = states.includes("fail");

            if (resultPanel && resultText && resultDetail) {
              resultPanel.classList.remove("pending", "pass", "fail");
              if (!allFilled) {
                resultPanel.classList.add("pending");
                resultText.textContent = "";
                resultDetail.textContent = "";
              } else if (anyFail) {
                resultPanel.classList.add("fail");
                if (failureActionSelect && failureActionSelect.value === "manager_approved") {
                  resultText.textContent = "Pass with Approval";
                  resultDetail.textContent = "Enter the manager name and reason, then submit the approval.";
                } else if (failureActionSelect && failureActionSelect.value === "rework") {
                  resultText.textContent = "Re-work";
                  resultDetail.textContent = "This failed inspection will be sent to re-work when submitted.";
                } else {
                  resultText.textContent = "Fail";
                  resultDetail.textContent = "Choose re-work or pass with approval.";
                }
              } else {
                resultPanel.classList.add("pass");
                resultText.textContent = "Pass";
                resultDetail.textContent = "All entered measurements are within spec.";
              }
            }

            if (failureFields) {
              failureFields.classList.toggle("hidden", !allFilled || !anyFail);
            }

            if (failureControls) {
              failureControls.classList.toggle("hidden", !allFilled || !anyFail);
            }

            if (failureActionSelect) {
              if (!allFilled || !anyFail) {
                failureActionSelect.value = "";
                setApprovalModalVisible(false);
              }
            }

            if (managerApprovalModal && failureActionSelect) {
              const shouldShowApproval = allFilled && anyFail && failureActionSelect.value === "manager_approved";
              setApprovalModalVisible(shouldShowApproval);
              if (!shouldShowApproval) {
                if (managerNameInput) managerNameInput.value = "";
                if (managerReasonInput) managerReasonInput.value = "";
              }
            }

            if (completeInspectionButton) {
              completeInspectionButton.classList.toggle(
                "hidden",
                Boolean(failureActionSelect && failureActionSelect.value === "manager_approved"),
              );
              if (failureActionSelect && failureActionSelect.value === "rework" && allFilled && anyFail) {
                completeInspectionButton.textContent = "Submit Re-work";
              } else {
                completeInspectionButton.textContent = "Complete Inspection";
              }
            }
          };

          const evaluate = (element) => {
            if (element.classList.contains("measurement-short-input")) {
              const sanitized = String(element.value ?? "").replace(/\D/g, "");
              const tailLength = Number(element.dataset.tailLength || 0);
              if (tailLength > 0) {
                element.value = sanitized.slice(0, tailLength);
              } else {
                element.value = sanitized;
              }
            }

            const rawValue = getEffectiveValue(element);
            if (!rawValue) {
              applyState(element, null);
              updateOverallResult();
              return;
            }

            const captureType = String(element.dataset.captureType || "").toLowerCase();
            if (captureType === "boolean") {
              const normalized = rawValue.toLowerCase();
              applyState(element, ["yes", "y", "pass", "true", "1"].includes(normalized) ? "pass" : "fail");
              updateOverallResult();
              return;
            }

            const numericValue = Number(rawValue);
            if (Number.isNaN(numericValue)) {
              applyState(element, "fail");
              updateOverallResult();
              return;
            }

            const minText = String(element.dataset.min || "").trim();
            const maxText = String(element.dataset.max || "").trim();
            const hasMin = minText !== "" && !Number.isNaN(Number(minText));
            const hasMax = maxText !== "" && !Number.isNaN(Number(maxText));
            const min = hasMin ? Number(minText) : null;
            const max = hasMax ? Number(maxText) : null;

            let pass = true;
            if (hasMin && numericValue < min) pass = false;
            if (hasMax && numericValue > max) pass = false;
            applyState(element, pass ? "pass" : "fail");
            updateOverallResult();
          };

          inputs.forEach((element) => {
            if (element.classList.contains("measurement-short-input") || element.classList.contains("inspection-table-input")) {
              element.addEventListener("focus", () => {
                if (typeof element.select === "function") {
                  element.select();
                }
              });
            }

            ["input", "change", "blur"].forEach((eventName) => {
              element.addEventListener(eventName, () => evaluate(element));
            });

            element.addEventListener("keydown", (event) => {
              if (event.key !== "Enter") return;
              event.preventDefault();
              const currentIndex = focusableMeasurementInputs.indexOf(element);
              const nextInput = currentIndex >= 0 ? focusableMeasurementInputs[currentIndex + 1] : null;
              if (nextInput) {
                nextInput.focus();
                if (typeof nextInput.select === "function" && !nextInput.classList.contains("measurement-checkbox")) {
                  nextInput.select();
                }
              }
            });
            evaluate(element);
          });
          if (failureActionSelect) {
            failureActionSelect.addEventListener("change", updateOverallResult);
          }

          if (approvalCloseButtons.length && failureActionSelect) {
            approvalCloseButtons.forEach((button) => {
              button.addEventListener("click", () => {
                failureActionSelect.value = "";
                updateOverallResult();
              });
            });
          }
          updateOverallResult();
          };

          document.addEventListener("submit", async (event) => {
            const form = event.target;
            if (!form || form.id !== "inspection-complete-form") return;
            if (form.dataset.nativeSubmit === "1") {
              delete form.dataset.nativeSubmit;
              return;
            }
            const workspace = document.getElementById("inspection-workspace");
            if (!workspace || !window.fetch || !window.DOMParser) return;
            event.preventDefault();
            const submitButton = event.submitter || form.querySelector('button[type="submit"]');
            const requestAction = submitButton?.getAttribute("formaction")
              ? new URL(submitButton.getAttribute("formaction"), window.location.href).href
              : form.action;
            const requestMethod = submitButton?.getAttribute("formmethod") || form.getAttribute("method") || "post";
            const isCompletingInspection = requestAction.includes("/workflow/complete");
            const previousButtonText = submitButton ? submitButton.textContent : "";
            const currentScrollY = window.scrollY;
            if (submitButton) {
              submitButton.disabled = true;
              submitButton.textContent = isCompletingInspection ? "Saving..." : "Loading...";
            }
            try {
              const formData = new FormData(form);
              const response = await fetch(requestAction, {
                method: requestMethod.toUpperCase(),
                body: new URLSearchParams(formData),
                credentials: "same-origin",
              });
              const html = await response.text();
              if (!response.ok) throw new Error("Pipe load failed.");
              const nextDocument = new DOMParser().parseFromString(html, "text/html");
              const nextWorkspace = nextDocument.getElementById("inspection-workspace");
              const popupNotice = nextDocument.querySelector(".notice[data-popup='true']");
              if (!nextWorkspace) throw new Error("Pipe load response was missing the inspection workspace.");
              workspace.replaceWith(nextWorkspace);
              initializeInspectionWorkspace(nextWorkspace);
              if (popupNotice) {
                window.alert(popupNotice.textContent.trim());
              }
              window.scrollTo({ top: currentScrollY, left: window.scrollX });
              const focusTarget =
                isCompletingInspection
                  ? nextWorkspace.querySelector("[data-pipe-number-entry]")
                  : nextWorkspace.querySelector(".measurement-short-input, .inspection-table-input, .measurement-checkbox");
              if (focusTarget) {
                focusTarget.focus();
                if (typeof focusTarget.select === "function" && !focusTarget.classList.contains("measurement-checkbox")) {
                  focusTarget.select();
                }
              }
            } catch (error) {
              form.action = requestAction;
              form.method = requestMethod;
              form.noValidate = !isCompletingInspection;
              form.dataset.nativeSubmit = "1";
              form.submit();
            } finally {
              if (submitButton) {
                submitButton.disabled = false;
                submitButton.textContent = previousButtonText;
              }
            }
          });

          initializeInspectionWorkspace(document);
        })();
      </script>
    </body>
    </html>`;
}

function baseSidebar(req) {
  const inspector = req.session.inspector;
  const hasInspectionSession = Boolean(req.session.sessionRecord?.id);
  return `
    <h2>Display</h2>
    <form method="post" action="/theme">
      <label>App Theme</label>
      <select name="theme_mode">
        <option value="Light" ${req.session.themeMode === "Dark" ? "" : "selected"}>Light</option>
        <option value="Dark" ${req.session.themeMode === "Dark" ? "selected" : ""}>Dark</option>
      </select>
      <button type="submit">Apply Theme</button>
    </form>
    ${
      inspector
        ? `<hr />
           <p><strong>User:</strong> ${escapeHtml(inspector.name)}</p>
           <p><strong>Role:</strong> ${escapeHtml(req.session.roleLabel || "Inspector")}</p>
           <p><strong>Branch:</strong> ${escapeHtml(inspector.branch || "Unknown")}</p>
           ${
             hasInspectionSession
               ? `<a class="button secondary sidebar-link-button" href="/workflow/inspection">Inspection Workflow</a>`
               : ""
           }
           ${
             req.session.canAccessAdmin
               ? `<a class="button sidebar-link-button" href="/admin">Admin Tools</a>
                  <a class="button secondary sidebar-link-button" href="/workflow/history?historyView=all">All Pipe History</a>
                  <a class="button secondary sidebar-link-button" href="/workflow/ncr">All NCRs</a>`
               : ""
           }
           <form method="post" action="/logout" onsubmit="return confirm('Log out and save today\\'s completed pipe history? Any unfinished pipe stays unpublished.');"><button type="submit">Log Out</button></form>`
        : ""
    }
  `;
}

async function ensureInitialized() {
  await callBridge("initialize");
}

async function runScheduledWorkOrderSync(trigger = "scheduled") {
  if (!WORKORDER_SYNC_ENABLED || workOrderSyncInProgress) return;
  workOrderSyncInProgress = true;
  try {
    const result = await callBridge("sync_work_orders");
    const syncCounts = result?.sync_counts || {};
    const summary = Object.values(syncCounts)
      .flatMap((siteCounts) => Object.entries(siteCounts))
      .map(([listName, itemCount]) => `${listName}: ${itemCount}`)
      .join(", ");
    console.log(`[workorder-sync] ${trigger} refresh complete${summary ? ` (${summary})` : ""}`);
  } catch (error) {
    console.error(`[workorder-sync] ${trigger} refresh failed: ${error.message}`);
  } finally {
    workOrderSyncInProgress = false;
  }
}

app.post("/theme", (req, res) => {
  req.session.themeMode = req.body.theme_mode === "Dark" ? "Dark" : "Light";
  res.redirect("back");
});

async function publishAndCloseSession(sessionRecord) {
  if (!sessionRecord?.id) {
    return { publishedCount: 0 };
  }
  const publishResult = await callBridge("publish_session_history", { session_id: sessionRecord.id });
  await callBridge("close_inspector_session", { session_id: sessionRecord.id });
  return { publishedCount: Number(publishResult?.published_count || 0) };
}

app.post("/logout", async (req, res, next) => {
  try {
    const { publishedCount } = await publishAndCloseSession(req.session.sessionRecord);
    const message =
      publishedCount > 0
        ? `Logout complete. ${publishedCount} pipe record${publishedCount === 1 ? "" : "s"} published to Pipe History.`
        : "Logout complete. No completed or re-work pipe records were ready to publish to Pipe History.";
    req.session.destroy(() =>
      res.redirect(`/?shiftEnded=1&notice=${encodeURIComponent(message)}&kind=success`),
    );
  } catch (error) {
    next(error);
  }
});

app.post("/workflow/end-shift", async (req, res, next) => {
  try {
    if (!req.session.inspector || !req.session.sessionRecord?.id) return res.redirect("/");
    const { publishedCount } = await publishAndCloseSession(req.session.sessionRecord);
    const message =
      publishedCount > 0
        ? `End shift complete. ${publishedCount} pipe record${publishedCount === 1 ? "" : "s"} published to Pipe History.`
        : "End shift complete. No completed or re-work pipe records were ready to publish to Pipe History.";
    req.session.destroy(() =>
      res.redirect(`/?shiftEnded=1&notice=${encodeURIComponent(message)}&kind=success`),
    );
  } catch (error) {
    next(error);
  }
});

app.get("/", async (req, res, next) => {
  try {
    await ensureInitialized();
    if (req.session.sessionRecord && req.session.inspector) {
      return res.redirect("/workflow");
    }
    if (req.session.inspector && req.session.canAccessAdmin) {
      return res.redirect("/admin");
    }

    const locations = await callBridge("get_locations");
    const pending = req.session.pendingLoginContext || null;
    const operators = pending ? await callBridge("get_cnc_operators", { branch: pending.inspector.branch }) : [];
    const pageNotice =
      req.query.shiftEnded && req.query.notice
        ? { kind: String(req.query.kind || "success"), message: String(req.query.notice || "") }
        : req.session.notice;

    const content = `
      ${renderNotice(req.session.notice)}
      <section class="card">
        <details class="login-panel"${pending ? "" : " open"}>
          <summary>
            <span class="section-title">Inspector Login</span>
            ${pending ? '<span class="summary-hint">Change inspector or session setup</span>' : ""}
          </summary>
          <form method="post" action="/login/find" class="form-grid">
            <label class="toggle-row">
              <input type="checkbox" name="admin_login" value="1" data-admin-login-toggle />
              <span>Admin login</span>
            </label>
            <div class="field">
              <label>Inspector ADP Number</label>
              <input type="text" name="adp_number" />
            </div>
            <div class="field">
              <label>Shift</label>
              <select name="shift">
                <option value="Day">Day</option>
                <option value="Night">Night</option>
              </select>
            </div>
            <div class="field" data-admin-login-field="location">
              <label>Location / Machine</label>
              <select name="location_id">${renderLocationOptions(locations, null, pending?.inspector)}</select>
            </div>
            ${renderFloatingTabletNoteField()}
            <div class="actions"><button class="button" type="submit">Find Inspector</button></div>
          </form>
        </details>
      </section>
      ${
        pending
          ? `<section class="card">
               <h2 class="section-title">Inspector Ready</h2>
               <p>${escapeHtml(pending.inspector.name)} | ${escapeHtml(req.session.roleLabel || "Inspector")} | ${escapeHtml(pending.shift)} | ${escapeHtml(pending.location.location_name)}</p>
               ${pending.floatingTabletNote ? `<p><strong>Floating Tablet Note:</strong> ${escapeHtml(pending.floatingTabletNote)}</p>` : ""}
               <form method="post" action="/login/start" class="form-grid">
                 <div class="field">
                   <label>CNC Operator</label>
                   <select name="operator_item_id">${renderOptions(operators, null, (item) => item.item_id, (item) => item.name)}</select>
                 </div>
                 <div class="actions"><button class="button" type="submit">Start Session</button></div>
               </form>
             </section>`
          : ""
      }
    `;

    const notice = req.session.notice;
    req.session.notice = null;
    res.send(layout({ title: "Inspection Run Report", sidebar: baseSidebar(req), content, theme: req.session.themeMode }));
  } catch (error) {
    next(error);
  }
});

app.post("/login/find", async (req, res, next) => {
  try {
    const inspector = await callBridge("get_employee_by_adp", { adp_number: req.body.adp_number });
    if (!inspector) {
      req.session.notice = { kind: "warning", message: "No employee found for that ADP number." };
      return res.redirect("/");
    }
    const isAdmin = await callBridge("is_admin_user", { employee: inspector });
    const isManager = await callBridge("is_manager_or_supervisor", { employee: inspector });
    req.session.roleLabel = isManager ? "Manager/Supervisor" : isAdmin ? "Admin Access Only" : "Inspector";
    req.session.canAccessAdmin = Boolean(isAdmin);
    if (req.body.admin_login === "1") {
      if (!isAdmin) {
        req.session.notice = { kind: "warning", message: "Admin login is available only to managers, supervisors, and IT." };
        return res.redirect("/");
      }
      req.session.inspector = inspector;
      req.session.sessionRecord = null;
      req.session.sessionShift = req.body.shift || (await callBridge("determine_shift"));
      req.session.pendingLoginContext = null;
      req.session.activeInspection = null;
      req.session.selection = null;
      req.session.notice = { kind: "success", message: `Admin login complete: ${inspector.name}` };
      return res.redirect("/admin");
    }
    const locations = await callBridge("get_locations");
    const location = locations.find((item) => String(item.id) === String(req.body.location_id));
    if (!location) {
      req.session.notice = { kind: "warning", message: "Please select a valid machine or location." };
      return res.redirect("/");
    }
    const isOwnLockedSession =
      location.is_locked &&
      String(location.active_inspector_adp || "").trim() === String(inspector.adp_number || "").trim();
    if (location.is_locked && !isOwnLockedSession) {
      req.session.notice = {
        kind: "warning",
        message: `${location.location_name} is currently unavailable${location.active_inspector_name ? ` because it is in use by ${location.active_inspector_name}` : ""}.`,
      };
      return res.redirect("/");
    }
    req.session.pendingLoginContext = {
      inspector,
      shift: req.body.shift || (await callBridge("determine_shift")),
      location,
      floatingTabletNote: String(req.body.floating_tablet_note || "").trim(),
    };
    req.session.notice = {
      kind: "success",
      message: isOwnLockedSession
        ? `Inspector found: ${inspector.name}. Resume your open session at ${location.location_name}.`
        : `Inspector found: ${inspector.name}`,
    };
    res.redirect("/");
  } catch (error) {
    next(error);
  }
});

app.post("/login/start", async (req, res, next) => {
  try {
    const pending = req.session.pendingLoginContext;
    if (!pending) return res.redirect("/");
    const operators = await callBridge("get_cnc_operators", { branch: pending.inspector.branch });
    const operator = operators.find((item) => String(item.item_id) === String(req.body.operator_item_id));
    const sessionRecord = await callBridge("create_inspector_session", {
      params: {
        inspector: pending.inspector,
        shift: pending.shift,
        location: pending.location,
        cnc_operator: operator,
        floating_tablet_note: pending.floatingTabletNote || "",
      },
    });
    req.session.inspector = pending.inspector;
    req.session.sessionRecord = sessionRecord;
    req.session.sessionShift = pending.shift;
    req.session.pendingLoginContext = null;
    req.session.activeInspection = null;
    req.session.selection = null;
    res.redirect("/workflow/inspection");
  } catch (error) {
    next(error);
  }
});

app.post("/admin/session/setup", async (req, res, next) => {
  try {
    if (!req.session.inspector) return res.redirect("/");
    const isAdmin = req.session.canAccessAdmin ?? (await callBridge("is_admin_user", { employee: req.session.inspector }));
    if (!isAdmin) {
      req.session.notice = { kind: "warning", message: "Admin tools are available only to managers, supervisors, and IT." };
      return res.redirect("/");
    }
    req.session.canAccessAdmin = true;
    if (req.session.sessionRecord?.id) {
      req.session.notice = { kind: "info", message: "You already have an active inspection session." };
      return res.redirect("/workflow/inspection");
    }
    const locations = await callBridge("get_locations");
    const location = locations.find((item) => String(item.id) === String(req.body.location_id));
    if (!location) {
      req.session.notice = { kind: "warning", message: "Please select a valid machine or location." };
      return res.redirect("/admin");
    }
    const isOwnLockedSession =
      location.is_locked &&
      String(location.active_inspector_adp || "").trim() === String(req.session.inspector.adp_number || "").trim();
    if (location.is_locked && !isOwnLockedSession) {
      req.session.notice = {
        kind: "warning",
        message: `${location.location_name} is currently unavailable${location.active_inspector_name ? ` because it is in use by ${location.active_inspector_name}` : ""}.`,
      };
      return res.redirect("/admin");
    }
    req.session.pendingLoginContext = {
      inspector: req.session.inspector,
      shift: req.body.shift || (await callBridge("determine_shift")),
      location,
      floatingTabletNote: String(req.body.floating_tablet_note || "").trim(),
    };
    req.session.notice = { kind: "success", message: `Floor session ready for ${req.session.inspector.name}.` };
    res.redirect("/admin");
  } catch (error) {
    next(error);
  }
});

app.get("/workflow", async (req, res) => {
  res.redirect("/workflow/inspection");
});

app.post("/workflow/refresh-workorders", async (req, res, next) => {
  try {
    if (!req.session.inspector || !req.session.sessionRecord) return res.redirect("/");
    const redirectTo = String(req.body.redirectTo || "/workflow/inspection");
    const result = await callBridge("sync_work_orders");
    const syncCounts = result?.sync_counts || {};
    const summary = Object.values(syncCounts)
      .flatMap((siteCounts) => Object.entries(siteCounts))
      .map(([listName, itemCount]) => `${listName}: ${itemCount}`)
      .join(", ");
    req.session.notice = {
      kind: "success",
      message: `Work orders refreshed successfully${summary ? ` (${summary})` : ""}.`,
    };
    res.redirect(redirectTo.startsWith("/workflow") ? redirectTo : "/workflow/inspection");
  } catch (error) {
    req.session.notice = {
      kind: "warning",
      message: summarizeRefreshError(error),
    };
    res.redirect("/workflow/inspection");
  }
});

app.get("/workflow/inspection", async (req, res, next) => {
  try {
    if (!req.session.inspector || !req.session.sessionRecord) return res.redirect("/");
    const inspector = req.session.inspector;
    const selection = {
      productionNumber: req.query.productionNumber || req.session.selection?.productionNumber || "",
      sizeLabel: req.query.sizeLabel || req.session.selection?.sizeLabel || "",
      weightLabel: req.query.weightLabel || req.session.selection?.weightLabel || "",
      connectionLabel: req.query.connectionLabel || req.session.selection?.connectionLabel || "",
      endType: req.query.endType || req.session.selection?.endType || "",
      recipeName: req.query.recipeName || req.session.selection?.recipeName || "",
      pipeNumber: req.session.selection?.pipeNumber || "",
      inspectionScope: req.query.inspectionScope || req.session.selection?.inspectionScope || "standard",
    };
    req.session.selection = selection;
    if (selection.sizeLabel || selection.weightLabel || selection.connectionLabel) {
      await callBridge("remember_inspection_entry_values", {
        branch: inspector.branch,
        size_label: selection.sizeLabel,
        weight_label: selection.weightLabel,
        connection_label: selection.connectionLabel,
      });
    }

    const workOrders = await callBridge("get_open_work_orders", { branch: inspector.branch });
    const entryOptions = await callBridge("get_inspection_entry_options", { branch: inspector.branch });
    const productionNumbers = [...new Set(workOrders.map((item) => item.production_number))].sort();
    if (!selection.productionNumber && productionNumbers.length) {
      selection.productionNumber = productionNumbers[0];
    }
    const lookupDescription = buildInspectionConnectionLabel(selection);
    const recipeCandidates = lookupDescription
      ? await callBridge("find_recipe_candidates", { operation_description: lookupDescription, branch: inspector.branch })
      : [];
    if (selection.recipeName && !recipeCandidates.some((item) => item.recipe_name === selection.recipeName)) {
      selection.recipeName = "";
    }
    if (!selection.recipeName && recipeCandidates.length) {
      selection.recipeName = recipeCandidates[0].recipe_name;
    }
    req.session.selection = selection;
    const activeInspection = req.session.activeInspection || null;
    const recipeDefinition = selection.recipeName
      ? await callBridge("get_recipe_elements", { recipe_name: selection.recipeName, branch: inspector.branch })
      : null;
    const digitalIrrName = recipeDefinition
      ? recipeDefinition.display_name || formatDigitalIrrName(recipeDefinition.recipe_name || "", recipeDefinition.drawing || "")
      : "";
    const existingPipe = selection.pipeNumber && lookupDescription
      ? await callBridge("get_pipe_unit", {
          production_number: selection.productionNumber,
          operation_description: lookupDescription,
          pipe_number: selection.pipeNumber,
        })
      : null;
    const history = existingPipe ? await callBridge("get_pipe_attempt_history", { pipe_unit_id: existingPipe.id }) : [];
    const allPipeRows = lookupDescription && selection.productionNumber
      ? await callBridge("search_pipe_units", {
          branch: inspector.branch,
          production_number: selection.productionNumber,
        })
      : [];
    const relevantPipeRows = allPipeRows
      .filter((item) => item.operation_description === lookupDescription)
      .sort((a, b) => {
        const aNum = Number(a.pipe_number);
        const bNum = Number(b.pipe_number);
        if (!Number.isNaN(aNum) && !Number.isNaN(bNum)) return aNum - bNum;
        return String(a.pipe_number).localeCompare(String(b.pipe_number), undefined, { numeric: true, sensitivity: "base" });
      });
    const historySourceRows = relevantPipeRows.filter((item) => {
      if (activeInspection && item.id === activeInspection.pipe_unit_id) return false;
      if (selection.pipeNumber && String(item.pipe_number) === String(selection.pipeNumber)) return false;
      return true;
    });
    const historyColumns = [];
    for (const pipeRow of historySourceRows) {
      const attempts = await callBridge("get_pipe_attempt_history", { pipe_unit_id: pipeRow.id });
      const latestAttempt = attempts[0];
      if (!latestAttempt) continue;
      const measurements = await callBridge("get_attempt_measurements", { attempt_id: latestAttempt.id });
      historyColumns.push({
        pipeUnitId: pipeRow.id,
        pipe_number: pipeRow.pipe_number,
        status: pipeRow.current_status,
        attemptStatus: latestAttempt.status,
        requiresManagerApproval: Boolean(latestAttempt.requires_manager_approval),
        measurementsBySequence: new Map(measurements.map((item) => [Number(item.element_sequence), item])),
      });
    }
    const showWorksheet = Boolean(recipeDefinition && (req.session.showInspectionSheet || req.session.activeInspection));
    const content = `
      ${renderWorkflowHeader(req)}
      ${renderNotice(req.session.notice)}
      ${renderWorkflowNav("/workflow/inspection", req)}
      <details class="card inspection-entry-panel"${showWorksheet ? "" : " open"}>
        <summary class="section-title">Inspection Entry</summary>
        <form method="get" action="/workflow/inspection" class="form-grid inspection-entry-form" id="inspection-selection-form">
          <div class="field"><label>Production Number / WO</label><select name="productionNumber" onchange="this.form.recipeName.value=''; this.form.submit();">${renderOptions(productionNumbers, selection.productionNumber, (item) => item, (item) => item)}</select></div>
          <div class="inspection-entry-inline-row">
            <div class="field"><label>Size <span class="required-marker" aria-hidden="true">*</span></label><input name="sizeLabel" list="size-options" value="${escapeHtml(selection.sizeLabel)}" oninput="this.form.recipeName.value='';" required /></div>
            <div class="field"><label>Weight <span class="required-marker" aria-hidden="true">*</span></label><input name="weightLabel" list="weight-options" value="${escapeHtml(selection.weightLabel)}" oninput="this.form.recipeName.value='';" required /></div>
            <div class="field"><label>Connection <span class="required-marker" aria-hidden="true">*</span></label><input name="connectionLabel" list="connection-options" value="${escapeHtml(selection.connectionLabel)}" oninput="this.form.recipeName.value='';" required /></div>
            <div class="field"><label>Box / Pin <span class="required-marker" aria-hidden="true">*</span></label><select name="endType" onchange="this.form.recipeName.value='';" required><option value=""></option><option value="BOX" ${selection.endType === "BOX" ? "selected" : ""}>Box</option><option value="PIN" ${selection.endType === "PIN" ? "selected" : ""}>Pin</option></select></div>
          </div>
          <datalist id="size-options">${(entryOptions?.size_options || []).map((item) => `<option value="${escapeHtml(item)}"></option>`).join("")}</datalist>
          <datalist id="weight-options">${(entryOptions?.weight_options || []).map((item) => `<option value="${escapeHtml(item)}"></option>`).join("")}</datalist>
          <datalist id="connection-options">${(entryOptions?.connection_options || []).map((item) => `<option value="${escapeHtml(item)}"></option>`).join("")}</datalist>
          <div class="field"><label>Digital IRR</label><select name="recipeName"><option value=""></option>${renderOptions(recipeCandidates, selection.recipeName, (item) => item.recipe_name, (item) => item.display_name || item.recipe_name)}</select></div>
          <div class="field"><label>Inspection Scope</label><select name="inspectionScope"><option value="standard" ${selection.inspectionScope === "full" ? "" : "selected"}>Standard Inspection</option><option value="full" ${selection.inspectionScope === "full" ? "selected" : ""}>Full Inspection</option></select></div>
          <div class="actions">
            <button class="button secondary" type="submit">Load Digital IRR Options</button>
            <button class="button workflow-action-button" type="submit" formaction="/workflow/start" formmethod="post">Start Inspection</button>
          </div>
        </form>
      </details>
      <div id="inspection-workspace">
        ${
          existingPipe
            ? existingPipe.current_status === "in_progress"
              ? renderNotice({
                  kind: "warning",
                  message: `Pipe ${selection.pipeNumber} already has an in-progress inspection for this WO/connection. Starting inspection will resume attempt #${existingPipe.latest_attempt_no}.`,
                })
              : existingPipe.current_status === "rework"
                ? renderNotice({
                    kind: "warning",
                    message: `Pipe ${selection.pipeNumber} is currently in re-work for this WO/connection. Starting inspection will continue with re-work attempt #${existingPipe.latest_attempt_no + 1}.`,
                  })
                : ["completed", "scrapped"].includes(existingPipe.current_status)
                  ? renderNotice({
                      kind: "error",
                      message: `Pipe ${selection.pipeNumber} is already resolved for this WO/connection. This pipe number is not a re-work and can not be re-entered. Please check the entered pipe number.`,
                    })
                : ""
            : ""
        }
        ${lookupDescription ? renderNotice({ kind: "info", message: `Inspection selection: ${lookupDescription}` }) : ""}
        ${
          recipeDefinition
            ? `<section class="card nested-card">
                 <h3 class="section-title">Digital IRR Summary</h3>
                 <div class="badges">
                   ${digitalIrrName ? `<span class="pill">${escapeHtml(digitalIrrName)}</span>` : ""}
                   ${recipeDefinition.connection_type ? `<span class="pill">Connection: ${escapeHtml(recipeDefinition.connection_type)}</span>` : ""}
                   ${recipeDefinition.drawing ? `<span class="pill">Drawing: ${escapeHtml(recipeDefinition.drawing)}</span>` : ""}
                   ${recipeDefinition.source_report ? `<span class="pill">Report: ${escapeHtml(recipeDefinition.source_report)}</span>` : ""}
                 </div>
               </section>
               <details>
                 <summary>Digital IRR Elements Preview</summary>
                 ${renderTable(
                   recipeDefinition.elements.map(({ item_id, capture_type, ...element }) => element),
                 )}
               </details>`
            : renderNotice({ kind: "warning", message: "Select a connection and Digital IRR before preparing the inspection." })
        }
        ${showWorksheet ? renderCurrentAttemptWorksheet({
          activeInspection,
          recipeDefinition,
          selection,
          sessionRecord: req.session.sessionRecord,
          inspectorName: req.session.inspector?.name,
          historyColumns,
        }) : ""}
        ${history.length ? `<h3 class="section-title">Pipe History</h3>${renderTable(history)}` : ""}
      </div>
    `;

    req.session.notice = null;
    res.send(layout({ title: "Inspection Run Report Workflow", sidebar: baseSidebar(req), content, theme: req.session.themeMode }));
  } catch (error) {
    next(error);
  }
});

app.get("/workflow/history", async (req, res, next) => {
  try {
    if (!req.session.inspector) return res.redirect("/");
    const inspector = req.session.inspector;
    const isAdmin = req.session.canAccessAdmin ?? (await callBridge("is_admin_user", { employee: inspector }));
    const hasInspectionSession = Boolean(req.session.sessionRecord?.id);
    if (!hasInspectionSession && !isAdmin) return res.redirect("/");
    req.session.canAccessAdmin = Boolean(isAdmin);
    const canManage = Boolean(hasInspectionSession);
    const canViewAllHistory = Boolean(isAdmin);
    const requestedHistoryView = String(req.query.historyView || "").trim().toLowerCase();
    const historyView =
      canViewAllHistory && (requestedHistoryView === "all" || (!requestedHistoryView && !hasInspectionSession))
        ? "all"
        : requestedHistoryView === "mine"
          ? "mine"
          : hasInspectionSession
            ? "machine"
            : "all";
    const historyBranchFilter = historyView === "machine" ? inspector.branch : null;
    const historyPublishedOnly = !(isAdmin && historyView === "all");
    const currentLocationName = String(req.session.sessionRecord?.location_name || "").trim();
    const currentInspectorName = String(req.session.inspector?.name || "").trim();
    const allPipeRows = await callBridge("search_pipe_units", {
      branch: historyBranchFilter,
      production_number: null,
      pipe_number: null,
      status: null,
      inspection_scope: null,
      published_only: historyPublishedOnly,
    });
    const productionOptions = [...new Set(allPipeRows.map((item) => item.production_number).filter(Boolean))].sort();
    const pipeNumberOptions = [...new Set(allPipeRows.map((item) => item.pipe_number).filter(Boolean))].sort((a, b) =>
      String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: "base" }),
    );
    const pipeRows = await callBridge("search_pipe_units", {
      branch: historyBranchFilter,
      production_number: req.query.historyProduction || null,
      pipe_number: req.query.historyPipe || null,
      status: req.query.historyStatus || null,
      inspection_scope: req.query.historyScope || null,
      published_only: historyPublishedOnly,
    });
    const historyDetails = await callBridge("get_pipe_history_details", {
      pipe_unit_ids: pipeRows.map((pipeRow) => pipeRow.id),
    });
    const attemptsByPipe = historyDetails?.attempts_by_pipe || {};
    const measurementsByAttempt = historyDetails?.measurements_by_attempt || {};
    const workorderGroupsMap = new Map();
    const recipeDefinitionCache = new Map();
    for (const pipeRow of pipeRows) {
      const attempts = attemptsByPipe[String(pipeRow.id)] || [];
      const latestAttempt = attempts[0];
      if (!latestAttempt) continue;

      const matchesCurrentMachine = currentLocationName
        ? attempts.some((attempt) => String(attempt.location_name || "").trim() === currentLocationName)
        : true;
      const matchesCurrentInspector = currentInspectorName
        ? attempts.some((attempt) => String(attempt.inspector_name || "").trim() === currentInspectorName)
        : true;
      if (historyView === "machine" && !matchesCurrentMachine) continue;
      if (historyView === "mine" && !matchesCurrentInspector) continue;

      const matchingAttempt =
        historyView === "machine"
          ? attempts.find((attempt) => String(attempt.location_name || "").trim() === currentLocationName) || latestAttempt
          : historyView === "mine"
            ? attempts.find((attempt) => String(attempt.inspector_name || "").trim() === currentInspectorName) || latestAttempt
            : latestAttempt;

      const measurements = measurementsByAttempt[String(latestAttempt.id)] || [];
      const productionNumber = pipeRow.production_number || "";
      const operationDescription = pipeRow.operation_description || "";
      const workorderKey = productionNumber;
      const connectionKey = `${productionNumber}||${operationDescription}`;
      const updatedAt =
        matchingAttempt.completed_at || matchingAttempt.started_at || latestAttempt.completed_at || latestAttempt.started_at || pipeRow.updated_at || pipeRow.created_at || null;
      const latestLocationName = String(latestAttempt.location_name || "").trim();
      const latestInspectorName = String(latestAttempt.inspector_name || "").trim();
      const matchingLocationName = String(matchingAttempt.location_name || "").trim();
      const matchingInspectorName = String(matchingAttempt.inspector_name || "").trim();

      if (!workorderGroupsMap.has(workorderKey)) {
        workorderGroupsMap.set(workorderKey, {
          productionNumber,
          latestUpdatedAt: updatedAt,
          connectionGroupsMap: new Map(),
        });
      }
      const workorderGroup = workorderGroupsMap.get(workorderKey);
      if (updatedAt && (!workorderGroup.latestUpdatedAt || new Date(updatedAt) > new Date(workorderGroup.latestUpdatedAt))) {
        workorderGroup.latestUpdatedAt = updatedAt;
      }

      if (!workorderGroup.connectionGroupsMap.has(connectionKey)) {
        const recipeCacheKey = `${historyBranchFilter || ""}||${operationDescription}`;
        let recipeDefinition = recipeDefinitionCache.get(recipeCacheKey);
        if (!recipeDefinitionCache.has(recipeCacheKey)) {
          const recipeCandidates = await callBridge("find_recipe_candidates", {
            operation_description: operationDescription,
            branch: historyBranchFilter,
          });
          const recipeName =
            recipeCandidates?.length
              ? (typeof recipeCandidates[0] === "string" ? recipeCandidates[0] : recipeCandidates[0].recipe_name)
              : null;
          recipeDefinition = recipeName
            ? await callBridge("get_recipe_elements", { recipe_name: recipeName, branch: historyBranchFilter })
            : null;
          recipeDefinitionCache.set(recipeCacheKey, recipeDefinition);
        }

        workorderGroup.connectionGroupsMap.set(connectionKey, {
          productionNumber,
          operationDescription,
          latestUpdatedAt: updatedAt,
          latestInspectorName: historyView === "all" ? latestInspectorName : matchingInspectorName,
          latestLocationName: historyView === "all" ? latestLocationName : matchingLocationName,
          recipeDefinition,
          measurementRowsBySequence: new Map(),
          columns: [],
        });
      }

      const connectionGroup = workorderGroup.connectionGroupsMap.get(connectionKey);
      if (updatedAt && (!connectionGroup.latestUpdatedAt || new Date(updatedAt) > new Date(connectionGroup.latestUpdatedAt))) {
        connectionGroup.latestUpdatedAt = updatedAt;
        connectionGroup.latestInspectorName = historyView === "all" ? latestInspectorName : matchingInspectorName;
        connectionGroup.latestLocationName = historyView === "all" ? latestLocationName : matchingLocationName;
      }

      measurements.forEach((measurement) => {
        const sequence = Number(measurement.element_sequence);
        if (!connectionGroup.measurementRowsBySequence.has(sequence)) {
          connectionGroup.measurementRowsBySequence.set(sequence, {
            element_sequence: measurement.element_sequence,
            element_description: measurement.element_description,
            dwg_dim: measurement.dwg_dim,
            gauge: measurement.gauge,
            frequency: "",
          });
        }
      });

      connectionGroup.columns.push({
        pipeUnitId: pipeRow.id,
        pipeNumber: pipeRow.pipe_number,
        status: pipeRow.current_status,
        attemptStatus: latestAttempt.status,
        requiresManagerApproval: Boolean(latestAttempt.requires_manager_approval),
        measurementsBySequence: new Map(measurements.map((item) => [Number(item.element_sequence), item])),
      });
    }

    const pipeHistoryGroups = [...workorderGroupsMap.values()]
      .map((workorderGroup) => ({
        productionNumber: workorderGroup.productionNumber,
        latestUpdatedAt: workorderGroup.latestUpdatedAt,
        connectionGroups: [...workorderGroup.connectionGroupsMap.values()]
          .map((connectionGroup) => ({
            productionNumber: connectionGroup.productionNumber,
            operationDescription: connectionGroup.operationDescription,
            latestUpdatedAt: connectionGroup.latestUpdatedAt,
            latestInspectorName: connectionGroup.latestInspectorName,
            latestLocationName: connectionGroup.latestLocationName,
            recipeDefinition: connectionGroup.recipeDefinition,
            rows:
              connectionGroup.recipeDefinition?.elements?.length
                ? connectionGroup.recipeDefinition.elements
                : [...connectionGroup.measurementRowsBySequence.values()].sort(
                    (a, b) => Number(a.element_sequence) - Number(b.element_sequence),
                  ),
            columns: [...connectionGroup.columns].sort((a, b) =>
              String(a.pipeNumber).localeCompare(String(b.pipeNumber), undefined, { numeric: true, sensitivity: "base" }),
            ),
          }))
          .sort((a, b) => new Date(b.latestUpdatedAt || 0) - new Date(a.latestUpdatedAt || 0)),
      }))
      .sort((a, b) => new Date(b.latestUpdatedAt || 0) - new Date(a.latestUpdatedAt || 0));

    const historyViewLabel =
      historyView === "all" ? "All History" : historyView === "mine" ? "My History" : "This Machine";
    const content = `
      ${renderWorkflowHeader(req)}
      ${renderNotice(req.session.notice)}
      ${renderWorkflowNav("/workflow/history", req)}
      <section class="table-card">
        <h2 class="section-title">Pipe History</h2>
        <p>Review saved inspection history by machine, inspector, or plant-wide scope. Entries are sorted by most recent activity first.</p>
      </section>
      <section class="table-card">
        <form method="get" action="/workflow/history" class="form-grid two">
          <div class="field"><label>History View</label><select name="historyView">
            ${hasInspectionSession ? `<option value="machine" ${historyView === "machine" ? "selected" : ""}>This Machine</option>` : ""}
            <option value="mine" ${historyView === "mine" ? "selected" : ""}>My History</option>
            ${canViewAllHistory ? `<option value="all" ${historyView === "all" ? "selected" : ""}>All History</option>` : ""}
          </select></div>
          <div class="field"><label>Filter Production Number</label><select name="historyProduction"><option value=""></option>${renderOptions(productionOptions, req.query.historyProduction || "", (item) => item, (item) => item)}</select></div>
          <div class="field"><label>Filter Pipe Number</label><select name="historyPipe"><option value=""></option>${renderOptions(pipeNumberOptions, req.query.historyPipe || "", (item) => item, (item) => item)}</select></div>
          <div class="field"><label>Filter Status</label><select name="historyStatus"><option value=""></option><option value="in_progress" ${req.query.historyStatus === "in_progress" ? "selected" : ""}>in_progress</option><option value="completed" ${req.query.historyStatus === "completed" ? "selected" : ""}>completed</option><option value="rework" ${req.query.historyStatus === "rework" ? "selected" : ""}>rework</option><option value="scrapped" ${req.query.historyStatus === "scrapped" ? "selected" : ""}>scrapped</option></select></div>
          <div class="field"><label>Filter Scope</label><select name="historyScope"><option value=""></option><option value="standard" ${req.query.historyScope === "standard" ? "selected" : ""}>Standard Inspection</option><option value="full" ${req.query.historyScope === "full" ? "selected" : ""}>Full Inspection</option></select></div>
          <div class="actions"><button class="button" type="submit">Search Pipe History</button></div>
        </form>
        <p><strong>Viewing:</strong> ${escapeHtml(historyViewLabel)}${historyView === "machine" && currentLocationName ? ` for ${escapeHtml(currentLocationName)}` : ""}${historyView === "mine" && currentInspectorName ? ` for ${escapeHtml(currentInspectorName)}` : ""}</p>
        ${renderPipeHistorySheets(pipeHistoryGroups, {
          canManage,
        })}
      </section>
    `;
    req.session.notice = null;
    res.send(layout({ title: "Pipe History", sidebar: baseSidebar(req), content, theme: req.session.themeMode }));
  } catch (error) {
    next(error);
  }
});

app.get("/workflow/history/edit/:pipeUnitId", async (req, res, next) => {
  try {
    if (!req.session.inspector || !req.session.sessionRecord) return res.redirect("/");
    const pipeRow = await callBridge("get_pipe_unit_by_id", { pipe_unit_id: req.params.pipeUnitId });
    if (!pipeRow) {
      req.session.notice = { kind: "warning", message: "That pipe inspection could not be found." };
      return res.redirect("/workflow/history");
    }
    const attempts = await callBridge("get_pipe_attempt_history", { pipe_unit_id: pipeRow.id });
    const latestAttempt = attempts?.[0] || null;
    const measurements = latestAttempt
      ? await callBridge("get_attempt_measurements", { attempt_id: latestAttempt.id })
      : [];

    const content = `
      ${renderWorkflowHeader(req)}
      ${renderNotice(req.session.notice)}
      ${renderWorkflowNav("/workflow/history", req)}
      <section class="table-card">
        <h2 class="section-title">Edit Pipe Record</h2>
        <p>Update the pipe identifiers and latest saved measurement values without removing the inspection history tied to this record.</p>
      </section>
      <section class="table-card">
        <form method="post" action="/workflow/history/edit/${encodeURIComponent(pipeRow.id)}" class="form-grid">
          <div class="field"><label>Production Number / WO</label><input name="production_number" value="${escapeHtml(pipeRow.production_number)}" required /></div>
          <div class="field"><label>Connection Type / Operation Description</label><input name="operation_description" value="${escapeHtml(pipeRow.operation_description)}" required /></div>
          <div class="field"><label>Pipe Number</label><input name="pipe_number" value="${escapeHtml(pipeRow.pipe_number)}" required /></div>
          <div class="field"><label>Current Status</label><input value="${escapeHtml(pipeRow.current_status)}" disabled /></div>
          ${
            latestAttempt && measurements.length
              ? `<input type="hidden" name="attempt_id" value="${escapeHtml(latestAttempt.id)}" />
                 <div class="table-wrap">
                   <table>
                     <thead>
                       <tr>
                         <th>#</th>
                         <th>Element</th>
                         <th>DWG DIM</th>
                         <th>Gauge</th>
                         <th>Measured Value</th>
                         <th>Result</th>
                       </tr>
                     </thead>
                     <tbody>
                       ${measurements
                         .map(
                           (measurement, index) => `<tr>
                             <td>${escapeHtml(measurement.element_sequence)}</td>
                             <td>
                               ${escapeHtml(measurement.element_description)}
                               <input type="hidden" name="measurement_${index}_sequence" value="${escapeHtml(measurement.element_sequence)}" />
                               <input type="hidden" name="measurement_${index}_element" value="${escapeHtml(measurement.element_description)}" />
                               <input type="hidden" name="measurement_${index}_dwg_dim" value="${escapeHtml(measurement.dwg_dim || "")}" />
                               <input type="hidden" name="measurement_${index}_gauge" value="${escapeHtml(measurement.gauge || "")}" />
                             </td>
                             <td>${escapeHtml(measurement.dwg_dim || "")}</td>
                             <td>${escapeHtml(measurement.gauge || "")}</td>
                             <td><input name="measurement_${index}_value" value="${escapeHtml(formatValue(measurement.measured_value))}" required /></td>
                             <td>
                               <select name="measurement_${index}_pass_fail">
                                 <option value=""></option>
                                 <option value="Pass" ${measurement.pass_fail === "Pass" ? "selected" : ""}>Pass</option>
                                 <option value="Fail" ${measurement.pass_fail === "Fail" ? "selected" : ""}>Fail</option>
                               </select>
                             </td>
                           </tr>`,
                         )
                         .join("")}
                     </tbody>
                   </table>
                 </div>
                 <input type="hidden" name="measurement_count" value="${escapeHtml(measurements.length)}" />`
              : `<p>No saved measurements were found for the latest attempt.</p>`
          }
          <div class="actions">
            <button class="button" type="submit">Save Changes</button>
            <a class="button secondary" href="/workflow/history">Cancel</a>
          </div>
        </form>
      </section>
    `;
    req.session.notice = null;
    res.send(layout({ title: "Edit Pipe Record", sidebar: baseSidebar(req), content, theme: req.session.themeMode }));
  } catch (error) {
    next(error);
  }
});

app.post("/workflow/history/edit/:pipeUnitId", async (req, res, next) => {
  try {
    if (!req.session.inspector || !req.session.sessionRecord) return res.redirect("/");
    await callBridge("update_pipe_unit", {
      pipe_unit_id: req.params.pipeUnitId,
      production_number: req.body.production_number,
      operation_description: req.body.operation_description,
      pipe_number: req.body.pipe_number,
    });
    const measurementCount = Number(req.body.measurement_count || 0);
    const attemptId = req.body.attempt_id;
    if (attemptId && measurementCount > 0) {
      const measurements = Array.from({ length: measurementCount }, (_, index) => ({
        element_sequence: req.body[`measurement_${index}_sequence`],
        element_description: req.body[`measurement_${index}_element`],
        dwg_dim: req.body[`measurement_${index}_dwg_dim`],
        gauge: req.body[`measurement_${index}_gauge`],
        measured_value: req.body[`measurement_${index}_value`],
        pass_fail: req.body[`measurement_${index}_pass_fail`],
        inspected_this_pipe: true,
      }));
      await callBridge("update_attempt_measurements", {
        attempt_id: attemptId,
        measurements,
      });
    }

    if (req.session.activeInspection && String(req.session.activeInspection.pipe_unit_id) === String(req.params.pipeUnitId)) {
      req.session.selection = {
        ...(req.session.selection || {}),
        productionNumber: req.body.production_number || "",
        operationDescription: req.body.operation_description || "",
        pipeNumber: req.body.pipe_number || "",
      };
    }

    req.session.notice = { kind: "success", message: "The pipe record was updated successfully." };
    res.redirect("/workflow/history");
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message || "Unable to update that pipe record." };
    res.redirect(`/workflow/history/edit/${encodeURIComponent(req.params.pipeUnitId)}`);
  }
});

app.post("/workflow/history/delete", async (req, res, next) => {
  try {
    if (!req.session.inspector || !req.session.sessionRecord) return res.redirect("/");
    const redirectTo = String(req.body.redirectTo || "/workflow/history");
    const safeRedirect = redirectTo.startsWith("/workflow/history") ? redirectTo : "/workflow/history";
    const pipeUnitId = req.body.pipeUnitId;
    if (!pipeUnitId) {
      req.session.notice = { kind: "warning", message: "No pipe inspection was selected for deletion." };
      return res.redirect(safeRedirect);
    }

    await callBridge("delete_pipe_unit", { pipe_unit_id: pipeUnitId });
    if (req.session.activeInspection && String(req.session.activeInspection.pipe_unit_id) === String(pipeUnitId)) {
      req.session.activeInspection = null;
    }
    req.session.notice = { kind: "success", message: "The selected pipe inspection and its related records were deleted." };
    res.redirect(safeRedirect);
  } catch (error) {
    next(error);
  }
});

app.post("/workflow/history/reset", async (req, res, next) => {
  try {
    if (!req.session.inspector || !req.session.sessionRecord) return res.redirect("/");
    const pipeUnitId = req.body.pipeUnitId;
    if (!pipeUnitId) {
      req.session.notice = { kind: "warning", message: "No in-progress pipe inspection was selected for reset." };
      return res.redirect("/workflow/history");
    }

    const result = await callBridge("reset_in_progress_pipe_unit", { pipe_unit_id: pipeUnitId });
    if (req.session.activeInspection && String(req.session.activeInspection.pipe_unit_id) === String(pipeUnitId)) {
      req.session.activeInspection = null;
    }
    req.session.notice = {
      kind: "success",
      message: result.deleted_pipe_unit
        ? "The unfinished attempt was cleared and the pipe was removed because it had no completed history yet."
        : `The unfinished attempt was cleared and the pipe was restored to its last resolved status: ${result.restored_status}.`,
    };
    res.redirect("/workflow/history");
  } catch (error) {
    next(error);
  }
});

app.get("/report/pipe/:pipeUnitId", async (req, res, next) => {
  try {
    if (!req.session.inspector) return res.redirect("/");
    const inspector = req.session.inspector;
    const isAdmin = req.session.canAccessAdmin ?? (await callBridge("is_admin_user", { employee: inspector }));
    const hasInspectionSession = Boolean(req.session.sessionRecord?.id);
    if (!hasInspectionSession && !isAdmin) return res.redirect("/");
    req.session.canAccessAdmin = Boolean(isAdmin);
    const reportBranch = isAdmin ? null : inspector.branch;
    const pipeRows = await callBridge("search_pipe_units", { branch: reportBranch, published_only: !isAdmin });
    const pipeRow = pipeRows.find((item) => String(item.id) === String(req.params.pipeUnitId));
    if (!pipeRow) {
      req.session.notice = { kind: "warning", message: "That pipe report could not be found." };
      return res.redirect("/workflow/history");
    }

    const attempts = await callBridge("get_pipe_attempt_history", { pipe_unit_id: pipeRow.id });
    const attemptsWithMeasurements = [];
    for (const attempt of attempts) {
      const measurements = await callBridge("get_attempt_measurements", { attempt_id: attempt.id });
      const recipeDefinition = attempt.recipe_name
        ? await callBridge("get_recipe_elements", { recipe_name: attempt.recipe_name, branch: reportBranch })
        : null;
      attemptsWithMeasurements.push({ ...attempt, measurements, recipeDefinition });
    }

    const branchNcrs = await callBridge("get_ncr_reports", { branch: reportBranch, status: null });
    const ncrs = branchNcrs.filter((item) => String(item.pipe_unit_id) === String(pipeRow.id));

    const content = `
      <section class="hero">
        <h1>Full Inspection Report</h1>
        <p>Review the complete measurement and disposition history for this pipe.</p>
        <div class="badges">
          <a class="badge" href="/workflow/history">Back to Pipe History</a>
          <button class="badge badge-button" type="button" onclick="window.print()">Print / Save PDF</button>
        </div>
      </section>
      <section class="card report-card report-sheet">
        <div class="report-sheet-header">
          <div class="report-brand">
            <img class="report-logo" src="/public/BenoitLogoRegistered-Red.png" alt="Benoit logo" />
          </div>
          <div class="report-heading">
            <p class="report-kicker">Inspection Run Report</p>
            <h2 class="section-title">Pipe Summary</h2>
            <p class="report-subtitle">Formal inspection history for one production pipe and all recorded attempts.</p>
          </div>
          <div class="report-doc-meta">
            <div><strong>Document No:</strong> IRR-${escapeHtml(pipeRow.production_number)}-${escapeHtml(pipeRow.pipe_number)}</div>
            <div><strong>Revision:</strong> Rev A</div>
            <div><strong>Report Date:</strong> ${escapeHtml(formatDateValue(new Date()))}</div>
            <div><strong>Current Status:</strong> ${escapeHtml(pipeRow.current_status)}</div>
            <div><strong>Latest Attempt:</strong> ${escapeHtml(pipeRow.latest_attempt_no)}</div>
          </div>
        </div>
        <div class="report-grid">
          <div><strong>Production Number:</strong> ${escapeHtml(pipeRow.production_number)}</div>
          <div><strong>Pipe Number:</strong> ${escapeHtml(pipeRow.pipe_number)}</div>
          <div><strong>Branch:</strong> ${escapeHtml(pipeRow.branch)}</div>
          <div><strong>Updated:</strong> ${escapeHtml(formatDateValue(pipeRow.updated_at))}</div>
        </div>
        <div class="report-connection"><strong>Connection Type / Operation Description:</strong> ${escapeHtml(pipeRow.operation_description)}</div>
      </section>
      ${
        ncrs.length
          ? `<section class="card report-card report-sheet">
               <h2 class="section-title">NCR Summary</h2>
               ${renderTable(
                 ncrs.map((ncr) => ({
                   id: ncr.id,
                   status: ncr.status,
                   disposition: ncr.disposition,
                   tier_code: ncr.tier_code,
                   nonconformance: ncr.nonconformance,
                   immediate_containment: ncr.immediate_containment,
                   opened_at: ncr.opened_at,
                   closed_at: ncr.closed_at,
                 })),
               )}
             </section>`
          : ""
      }
      ${
        attemptsWithMeasurements.length
          ? attemptsWithMeasurements
              .map(
                (attempt) => `
                  <section class="card report-card report-sheet">
                    <div class="worksheet-report-header">
                      <div class="worksheet-title-block">
                        <p class="worksheet-title">${
                          escapeHtml(
                            attempt.recipeDefinition?.source_report ||
                            attempt.recipeDefinition?.display_name ||
                            attempt.recipeDefinition?.connection_type ||
                            attempt.recipe_name ||
                            "INSPECTION REPORT",
                          )
                        }</p>
                        <p class="worksheet-subtitle">
                          Attempt #${escapeHtml(attempt.attempt_no)} |
                          ${escapeHtml(attempt.status)} |
                          ${escapeHtml(attempt.inspection_scope === "full" ? "Full Inspection" : "Standard Inspection")}
                        </p>
                      </div>
                      <div class="report-attempt-badges">
                        <span class="pill">Status: ${escapeHtml(attempt.status)}</span>
                        ${renderScopeBadge(attempt.inspection_scope)}
                      </div>
                    </div>
                    <div class="worksheet-meta-grid">
                      <div class="worksheet-meta-row">
                        <span class="worksheet-meta-label">Date:</span>
                        <span class="worksheet-meta-value">${escapeHtml(formatDateValue(attempt.completed_at || attempt.started_at || ""))}</span>
                      </div>
                      <div class="worksheet-meta-row">
                        <span class="worksheet-meta-label">Drawing #:</span>
                        <span class="worksheet-meta-value">${escapeHtml(attempt.recipeDefinition?.drawing || "")}</span>
                      </div>
                      <div class="worksheet-meta-row">
                        <span class="worksheet-meta-label">Inspector:</span>
                        <span class="worksheet-meta-value">${escapeHtml(attempt.inspector_name || "")}</span>
                      </div>
                      <div class="worksheet-meta-row">
                        <span class="worksheet-meta-label">Workorder #:</span>
                        <span class="worksheet-meta-value">${escapeHtml(pipeRow.production_number)}</span>
                      </div>
                      <div class="worksheet-meta-row">
                        <span class="worksheet-meta-label">Pipe #:</span>
                        <span class="worksheet-meta-value">${escapeHtml(pipeRow.pipe_number)}</span>
                      </div>
                      <div class="worksheet-meta-row">
                        <span class="worksheet-meta-label">CNC Operator:</span>
                        <span class="worksheet-meta-value">${escapeHtml(attempt.cnc_operator_name || "")}</span>
                      </div>
                    </div>
                    <div class="worksheet-connection-row">
                      <span class="worksheet-meta-label">Connection Type / Operation Description:</span>
                      <span class="worksheet-meta-value">${escapeHtml(pipeRow.operation_description)}</span>
                    </div>
                    ${attempt.notes ? `<div class="report-notes"><strong>Attempt Notes:</strong> ${escapeHtml(attempt.notes)}</div>` : ""}
                    ${
                      attempt.measurements?.length
                        ? `<div class="table-wrap worksheet-table-wrap">
                             <table class="worksheet-table">
                               <thead>
                                 <tr>
                                   <th>#</th>
                                   <th>Element</th>
                                   <th>DWG DIM</th>
                                   <th>Gauge</th>
                                   <th>Measured Value</th>
                                   <th>Pass / Fail</th>
                                 </tr>
                               </thead>
                               <tbody>
                                 ${attempt.measurements
                                   .map(
                                     (measurement) => `
                                       <tr>
                                         <td>${escapeHtml(measurement.element_sequence)}</td>
                                         <td>${escapeHtml(measurement.element_description)}</td>
                                         <td>${escapeHtml(measurement.dwg_dim)}</td>
                                         <td>${escapeHtml(measurement.gauge)}</td>
                                         <td>${escapeHtml(formatValue(measurement.measured_value))}</td>
                                         <td class="${measurement.pass_fail === "Pass" ? "worksheet-pass" : measurement.pass_fail === "Fail" ? "worksheet-fail" : ""}">${escapeHtml(measurement.pass_fail)}</td>
                                       </tr>`,
                                   )
                                   .join("")}
                               </tbody>
                             </table>
                           </div>`
                        : "<p>No measurements were saved for this attempt.</p>"
                    }
                    <div class="worksheet-footer-note">${
                      escapeHtml(
                        attempt.recipeDefinition?.sampling_plan?.rule ||
                        "Review the recorded measurements above against the Digital IRR and drawing requirements for this connection.",
                      )
                    }</div>
                    <div class="signature-grid">
                      <div class="signature-card">
                        <div class="signature-line"></div>
                        <div class="signature-meta">
                          <strong>Inspector Sign-Off</strong>
                          <span>${escapeHtml(attempt.inspector_name || "")}</span>
                        </div>
                      </div>
                      <div class="signature-card">
                        <div class="signature-line"></div>
                        <div class="signature-meta">
                          <strong>Manager / Supervisor Approval</strong>
                          <span>${escapeHtml(attempt.manager_name || "Not recorded")}</span>
                        </div>
                      </div>
                    </div>
                  </section>`,
              )
              .join("")
          : `<section class="card report-card report-sheet"><p>No attempt history is available for this pipe yet.</p></section>`
      }
    `;

    res.send(layout({ title: "Full Inspection Report", sidebar: baseSidebar(req), content, theme: req.session.themeMode }));
  } catch (error) {
    next(error);
  }
});

app.get("/workflow/ncr", async (req, res, next) => {
  try {
    if (!req.session.inspector) return res.redirect("/");
    const inspector = req.session.inspector;
    const isAdmin = req.session.canAccessAdmin ?? (await callBridge("is_admin_user", { employee: inspector }));
    const hasInspectionSession = Boolean(req.session.sessionRecord?.id);
    if (!hasInspectionSession && !isAdmin) return res.redirect("/");
    req.session.canAccessAdmin = Boolean(isAdmin);
    const ncrBranch = isAdmin ? null : inspector.branch;
    const ncrRows = await callBridge("get_ncr_reports", {
      branch: ncrBranch,
      status: req.query.ncrStatus || null,
    });
    const content = `
      ${renderWorkflowHeader(req)}
      ${renderNotice(req.session.notice)}
      ${renderWorkflowNav("/workflow/ncr", req)}
      <section class="table-card">
        <h2 class="section-title">NCR Queue</h2>
        <p>Review NCRs${isAdmin ? " across all machines" : " for this branch"}, including disposition and containment details.</p>
      </section>
      <section class="table-card">
        <form method="get" action="/workflow/ncr" class="form-grid">
          <div class="field"><label>NCR Status</label><select name="ncrStatus"><option value=""></option><option value="open" ${req.query.ncrStatus === "open" ? "selected" : ""}>open</option><option value="closed" ${req.query.ncrStatus === "closed" ? "selected" : ""}>closed</option></select></div>
          <div class="actions"><button class="button" type="submit">Filter NCRs</button></div>
        </form>
        ${renderTable(ncrRows)}
      </section>
    `;
    req.session.notice = null;
    res.send(layout({ title: "NCR Queue", sidebar: baseSidebar(req), content, theme: req.session.themeMode }));
  } catch (error) {
    next(error);
  }
});

app.post("/workflow/start", async (req, res, next) => {
  try {
    const previousSelection = req.session.selection || {};
    const bodyValue = (name, fallback = "") =>
      Object.prototype.hasOwnProperty.call(req.body, name)
        ? req.body[name] || ""
        : previousSelection[name] || fallback;
    const selection = {
      productionNumber: bodyValue("productionNumber"),
      sizeLabel: bodyValue("sizeLabel"),
      weightLabel: bodyValue("weightLabel"),
      connectionLabel: bodyValue("connectionLabel"),
      endType: bodyValue("endType"),
      recipeName: bodyValue("recipeName"),
      pipeNumber: bodyValue("pipeNumber"),
      inspectionScope: bodyValue("inspectionScope", "standard") || "standard",
    };
    req.session.selection = selection;
    req.session.showInspectionSheet = true;
    const inspector = req.session.inspector;
    const sessionRecord = req.session.sessionRecord;
    await callBridge("remember_inspection_entry_values", {
      branch: inspector?.branch,
      size_label: selection.sizeLabel,
      weight_label: selection.weightLabel,
      connection_label: selection.connectionLabel,
    });
    const lookupDescription = buildInspectionConnectionLabel(selection);
    const hasRequiredConnectionDetails = [
      selection.sizeLabel,
      selection.weightLabel,
      selection.connectionLabel,
      selection.endType,
    ].every((value) => String(value || "").trim());
    if (!hasRequiredConnectionDetails) {
      req.session.notice = { kind: "warning", message: "Enter size, weight, connection, and Box/Pin before starting an inspection." };
      return res.redirect("/workflow/inspection");
    }
    if (!selection?.pipeNumber) {
      req.session.activeInspection = null;
      req.session.notice = {
        kind: "info",
        message: "Inspection sheet opened. Enter the pipe number in the worksheet header to start or resume that pipe.",
      };
      return res.redirect("/workflow/inspection");
    }
    const existingPipe = await callBridge("get_pipe_unit", {
      production_number: selection.productionNumber,
      operation_description: lookupDescription,
      pipe_number: selection.pipeNumber,
    });
    if (existingPipe && ["completed", "scrapped"].includes(String(existingPipe.current_status || "").toLowerCase())) {
      req.session.activeInspection = null;
      req.session.notice = {
        kind: "error",
        popup: true,
        message: "This pipe number is not a re-work and can not be re-entered. Please check the entered pipe number.",
      };
      return res.redirect("/workflow/inspection");
    }
    let recipeName = selection.recipeName;
    const recipeCandidates = await callBridge("find_recipe_candidates", {
      operation_description: lookupDescription,
      branch: inspector.branch,
    });
    if (recipeName && !recipeCandidates.some((item) => item.recipe_name === recipeName)) {
      recipeName = "";
    }
    if (!recipeName && recipeCandidates.length === 1) {
      recipeName = recipeCandidates[0].recipe_name;
    }
    if (!recipeName) {
      req.session.notice = { kind: "warning", message: "Load and choose a Digital IRR before starting the inspection." };
      return res.redirect("/workflow/inspection");
    }
    selection.recipeName = recipeName;
    req.session.selection = selection;
    const recipeDefinition = await callBridge("get_recipe_elements", { recipe_name: recipeName, branch: inspector.branch });
    if (!recipeDefinition?.elements?.length) {
      req.session.notice = { kind: "warning", message: "The selected Digital IRR does not have any elements to inspect." };
      return res.redirect("/workflow/inspection");
    }
    const activeInspection = await callBridge("create_inspection_attempt", {
      params: {
        production_number: selection.productionNumber,
        operation_description: lookupDescription,
        pipe_number: selection.pipeNumber,
        branch: inspector.branch,
        session_id: sessionRecord.id,
        inspector,
        cnc_operator: {
          item_id: sessionRecord.cnc_operator_item_id,
          name: sessionRecord.cnc_operator_name,
        },
        recipe_name: recipeName,
        recipe_elements: recipeDefinition,
        inspection_scope: selection.inspectionScope || "standard",
      },
    });
    req.session.activeInspection = activeInspection;
    req.session.showInspectionSheet = true;
    req.session.notice = {
      kind: "info",
      message: activeInspection.resumed_attempt
        ? `Resumed in-progress inspection for pipe ${selection.pipeNumber}.`
        : activeInspection.is_rework
          ? `Started re-work inspection for pipe ${selection.pipeNumber}.`
          : `Inspection started for pipe ${selection.pipeNumber}.`,
    };
    res.redirect("/workflow/inspection");
  } catch (error) {
    next(error);
  }
});

app.post("/workflow/scope", async (req, res, next) => {
  try {
    const active = req.session.activeInspection;
    const selection = req.session.selection || {};
    const inspector = req.session.inspector;
    if (!active || !selection.recipeName || !inspector) {
      req.session.notice = { kind: "warning", message: "Start or resume an inspection before changing inspection scope." };
      return res.redirect("/workflow/inspection");
    }

    const nextScope = String(req.body.inspectionScope || "").trim().toLowerCase() === "full" ? "full" : "standard";
    const recipeDefinition = await callBridge("get_recipe_elements", {
      recipe_name: selection.recipeName,
      branch: inspector.branch,
    });
    const updated = await callBridge("update_inspection_attempt_scope", {
      params: {
        attempt_id: active.attempt_id,
        recipe_elements: recipeDefinition,
        inspection_scope: nextScope,
      },
    });

    req.session.activeInspection = { ...active, ...updated };
    req.session.selection = { ...selection, inspectionScope: nextScope };
    req.session.showInspectionSheet = true;
    req.session.notice = {
      kind: "info",
      message: nextScope === "full" ? "Switched this pipe to full inspection." : "Switched this pipe to standard inspection.",
    };
    res.redirect("/workflow/inspection");
  } catch (error) {
    next(error);
  }
});

app.post("/workflow/complete", async (req, res, next) => {
  try {
    const active = req.session.activeInspection;
    if (!active) return res.redirect("/workflow");
    const measurements = active.inspection_plan.map((element) => ({
      element_sequence: element.element_sequence,
      element_description: element.element_description,
      dwg_dim: element.dwg_dim,
      gauge: element.gauge,
      capture_type: element.capture_type,
      value_format: element.value_format,
      nominal: element.nominal,
      min: element.min,
      max: element.max,
      measured_value: req.body[`element_${element.element_sequence}`],
      inspected_this_pipe: true,
    }));
    const missingMeasurements = measurements.filter((measurement) => {
      const value = measurement.measured_value;
      return value === undefined || value === null || String(value).trim() === "";
    });
    if (missingMeasurements.length) {
      req.session.notice = {
        kind: "warning",
        message: "Enter a value for every required inspection measurement before submitting the form.",
      };
      return res.redirect("/workflow/inspection");
    }
    const evaluation = await callBridge("evaluate_measurements", {
      measurements,
      approval_rules: active.approval_rules || [],
    });
    let disposition = "pass";
    if (evaluation.has_failures) {
      const failureAction = String(req.body.failure_action || "").trim().toLowerCase();
      if (!["rework", "manager_approved"].includes(failureAction)) {
        req.session.notice = {
          kind: "warning",
          message: "One or more measurements failed. Choose whether to send the pipe to re-work or pass it with approval.",
        };
        return res.redirect("/workflow/inspection");
      }
      if (failureAction === "manager_approved") {
        const managerName = String(req.body.manager_name || "").trim();
        const managerReason = String(req.body.manager_reason || "").trim();
        if (!managerName || !managerReason) {
          req.session.notice = {
            kind: "warning",
            message: "Enter the manager name and approval reason before saving a failed inspection as pass with approval.",
          };
          return res.redirect("/workflow/inspection");
        }
      }
      disposition = failureAction;
    }
    const attemptNotes = String(req.body.notes || "").trim();
    const reliefNote = String(req.body.relief_note || "").trim();
    const combinedNotes = [reliefNote ? `Relief / Coverage Notes: ${reliefNote}` : "", attemptNotes]
      .filter(Boolean)
      .join("\n\n");
    const result = await callBridge("complete_inspection_attempt", {
      params: {
        attempt_id: active.attempt_id,
        pipe_unit_id: active.pipe_unit_id,
        measurements: evaluation.measurements,
        disposition,
        notes: combinedNotes,
        manager_name: req.body.manager_name || "",
        manager_reason: req.body.manager_reason || "",
        ncr_data: {
          tier_code: req.body.tier_code || "",
          nonconformance: req.body.nonconformance || "",
          immediate_containment: req.body.immediate_containment || "",
        },
      },
    });
    req.session.activeInspection = null;
    req.session.showInspectionSheet = true;
    if (req.session.selection) {
      req.session.selection.pipeNumber = incrementPipeNumber(active.pipe_number || req.session.selection.pipeNumber || "");
    }
    req.session.notice = {
      kind: result.pipe_status === "rework" ? "warning" : "success",
      message:
        result.pipe_status === "completed"
          ? result.attempt_status === "approved"
            ? "Inspection saved as Pass With Approval."
            : "Inspection completed and moved to Completed."
          : result.pipe_status === "scrapped"
            ? "Inspection completed and moved to Scrapped."
            : "Inspection submitted and moved to Re-work.",
    };
    res.redirect("/workflow/inspection");
  } catch (error) {
    next(error);
  }
});

app.get("/admin", async (req, res, next) => {
  try {
    if (!req.session.inspector) return res.redirect("/");
    const inspector = req.session.inspector;
    const isAdmin = req.session.canAccessAdmin ?? (await callBridge("is_admin_user", { employee: inspector }));
    if (!isAdmin) {
      req.session.notice = { kind: "warning", message: "Admin tools are available only to managers, supervisors, and IT." };
      return res.redirect("/workflow");
    }
    const builderOptions = await callBridge("get_recipe_builder_options", { branch: inspector.branch });
    const recipeCatalog = await callBridge("list_recipe_catalog", { branch: inspector.branch });
    const locations = await callBridge("get_locations");
    const lockedLocations = locations.filter((item) => item.is_locked);
    const pending = req.session.pendingLoginContext || null;
    const createRecipeDraft = req.session.createRecipeDraft || {};
    const createRecipeRows = createRecipeDraft.rows || [];
    const operators =
      pending && !req.session.sessionRecord?.id
        ? await callBridge("get_cnc_operators", { branch: pending.inspector.branch })
        : [];
    const content = `
      <section class="hero">
        <div class="admin-hero-heading">
          <div>
            <h1>Inspection Run Report Admin</h1>
            <p>Administrative tools for setup and maintenance.</p>
            <div class="badges">
              ${req.session.sessionRecord?.id ? `<a class="badge" href="/workflow/inspection">Back to Inspection Workflow</a>` : ""}
            </div>
          </div>
          <form method="post" action="/logout" class="admin-logout-form" onsubmit="return confirm('Log out and save today\'s completed pipe history? Any unfinished pipe stays unpublished.');">
            <button class="button danger compact-button" type="submit">Log Out</button>
          </form>
        </div>
      </section>
      ${renderNotice(req.session.notice)}
      <section class="card">
        <h2 class="section-title">Manager Review</h2>
        <p>Open plant-wide inspection history and NCR records without starting a machine session.</p>
        <div class="actions">
          <a class="button" href="/workflow/history?historyView=all">View All Pipe History</a>
          <a class="button secondary" href="/workflow/ncr">View All NCRs</a>
        </div>
      </section>
      ${
        !req.session.sessionRecord?.id
          ? `<section class="card">
               <h2 class="section-title">Start Floor Session</h2>
               <p>Begin an inspection session as ${escapeHtml(inspector.name)} without signing out of admin tools.</p>
               <form method="post" action="/admin/session/setup" class="form-grid">
                 <div class="field">
                   <label>Shift</label>
                   <select name="shift">
                     <option value="Day" ${req.session.sessionShift === "Night" ? "" : "selected"}>Day</option>
                     <option value="Night" ${req.session.sessionShift === "Night" ? "selected" : ""}>Night</option>
                   </select>
                 </div>
                 <div class="field">
                   <label>Location / Machine</label>
                   <select name="location_id">${renderLocationOptions(locations, pending?.location?.id, inspector)}</select>
                 </div>
                 ${renderFloatingTabletNoteField(pending?.floatingTabletNote || "")}
                 <div class="actions"><button class="button" type="submit">Prepare Floor Session</button></div>
               </form>
             </section>`
          : ""
      }
      ${
        pending && !req.session.sessionRecord?.id
          ? `<section class="card">
               <h2 class="section-title">Floor Session Ready</h2>
               <p>${escapeHtml(pending.inspector.name)} | ${escapeHtml(req.session.roleLabel || "Admin Access Only")} | ${escapeHtml(pending.shift)} | ${escapeHtml(pending.location.location_name)}</p>
               ${pending.floatingTabletNote ? `<p><strong>Floating Tablet Note:</strong> ${escapeHtml(pending.floatingTabletNote)}</p>` : ""}
               <form method="post" action="/login/start" class="form-grid">
                 <div class="field">
                   <label>CNC Operator</label>
                   <select name="operator_item_id">${renderOptions(operators, null, (item) => item.item_id, (item) => item.name)}</select>
                 </div>
                 <div class="actions"><button class="button" type="submit">Start Inspection Session</button></div>
               </form>
             </section>`
          : ""
      }
      <section class="card">
        <h2 class="section-title">Machine Locks</h2>
        <p>Release a machine if a session was left open unexpectedly.</p>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Machine</th>
                <th>In Use By</th>
                <th>Locked Since</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              ${renderLockedLocationRows(lockedLocations)}
            </tbody>
          </table>
        </div>
      </section>
      <section class="card">
        <div class="catalog-heading-row">
          <div>
            <h2 class="section-title">Import Digital IRR</h2>
            <p>Load an existing PDF, Excel, or CSV inspection report into the builder for review before saving.</p>
          </div>
          <form method="post" action="/admin/recipes/import" class="irr-import-form" data-irr-import-form>
            <label class="file-picker">
              <span>Inspection report</span>
              <input type="file" accept=".pdf,.xlsx,.csv" data-irr-import-file required />
            </label>
            <input type="hidden" name="file_name" />
            <input type="hidden" name="file_content" />
            <button class="button compact-button" type="submit">Create Draft</button>
          </form>
        </div>
      </section>
      <section class="card" id="build-digital-irr">
        <h2 class="section-title">Build Digital IRR</h2>
        <p>${createRecipeDraft.imported_file_name ? `Review the imported values from <strong>${escapeHtml(createRecipeDraft.imported_file_name)}</strong>, then save when they are correct.` : "Create an app-managed Digital IRR with up to 25 inspection elements. Saved Digital IRRs are immediately available to the inspection workflow."}</p>
        <form method="post" action="/admin/recipes" class="form-grid">
          <div class="form-grid two">
            <div class="field"><label>Size</label><input name="size_label" placeholder='2.875' value="${escapeHtml(createRecipeDraft.size_label || "")}" required /></div>
            <div class="field"><label>Weight</label><input name="weight_label" placeholder='7.90#' value="${escapeHtml(createRecipeDraft.weight_label || "")}" required /></div>
            <div class="field"><label>Grade</label><input name="grade_label" placeholder='BTS-6' value="${escapeHtml(createRecipeDraft.grade_label || "")}" required /></div>
            <div class="field"><label>Connector Type</label><input name="connector_type" placeholder='PIN' value="${escapeHtml(createRecipeDraft.connector_type || "")}" required /></div>
            <div class="field"><label>Drawing Number</label><input name="drawing" placeholder='013 Rev 2' value="${escapeHtml(createRecipeDraft.drawing || "")}" /></div>
            <div class="field"><label>First Article</label><input name="first_article_label" placeholder='First Article' value="${escapeHtml(createRecipeDraft.first_article_label || "")}" /></div>
            <div class="field"><label>Source Report Title</label><input name="source_report" placeholder='2.875 7.90# BTS-6 (PIN) INSPECTION REPORT' value="${escapeHtml(createRecipeDraft.source_report || "")}" /></div>
          </div>
          <div class="table-wrap recipe-builder-table-wrap">
            <table class="recipe-builder-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Element</th>
                  <th>Measurement Type</th>
                  <th>Measurement Inputs</th>
                  <th>Gauge</th>
                  <th>Frequency</th>
                </tr>
              </thead>
              <tbody>
                ${
                  createRecipeRows.length
                    ? renderRecipeBuilderRowsWithValues(builderOptions, createRecipeRows, 25)
                    : renderRecipeBuilderRows(builderOptions, 25, 1)
                }
              </tbody>
            </table>
          </div>
          <div class="actions recipe-builder-actions">
            <button class="button secondary" type="button" data-add-recipe-row>Add Element</button>
          </div>
          <div class="actions"><button class="button" type="submit">Save Digital IRR</button></div>
        </form>
      </section>
      <section class="card">
        <div class="catalog-heading-row">
          <div>
            <h2 class="section-title">All Digital IRRs</h2>
            <p>Local and SharePoint Digital IRRs are combined by name, with the latest revision shown once.</p>
          </div>
          <form method="post" action="/admin/recipes/refresh">
            <button class="button secondary compact-button" type="submit">Refresh SharePoint</button>
          </form>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Digital IRR</th>
                <th>Source</th>
                <th>Branch</th>
                <th>Connection</th>
                <th>Drawing</th>
                <th>Revision</th>
                <th>Source Report</th>
                <th>Last Edit Comment</th>
                <th>Updated</th>
                <th>Admin</th>
                <th>SharePoint</th>
              </tr>
            </thead>
            <tbody>
              ${
                recipeCatalog.length
                  ? recipeCatalog
                      .map(
                        (recipe) => `<tr>
                    <td>${escapeHtml(formatDigitalIrrName(recipe.recipe_name, recipe.drawing))}</td>
                    <td><span class="pill source-badge">${escapeHtml(recipe.source)}</span></td>
                    <td>${escapeHtml(recipe.branch || "All")}</td>
                    <td>${escapeHtml(recipe.connection_type)}</td>
                    <td>${escapeHtml(recipe.drawing)}</td>
                    <td>${escapeHtml(recipe.recipe_version || 1)}</td>
                    <td>${escapeHtml(recipe.source_report)}</td>
                    <td>${escapeHtml(recipe.last_edit_comment || "")}</td>
                    <td>${escapeHtml(formatDateValue(recipe.updated_at))}</td>
                    <td>${
                      recipe.local_recipe_id
                        ? `<a class="button secondary compact-button" href="/admin/recipes/${encodeURIComponent(recipe.local_recipe_id)}/edit">Edit</a>`
                        : '<span class="catalog-muted">SharePoint managed</span>'
                    }</td>
                    <td>
                      ${
                        recipe.local_recipe_id
                          ? `<form method="post" action="/admin/recipes/${encodeURIComponent(recipe.local_recipe_id)}/publish">
                               <button class="button compact-button" type="submit">Publish</button>
                             </form>`
                          : '<span class="catalog-muted">Published</span>'
                      }
                    </td>
                  </tr>`,
                      )
                      .join("")
                  : '<tr><td colspan="11" class="catalog-empty">No Digital IRRs are available for this branch.</td></tr>'
              }
            </tbody>
          </table>
        </div>
      </section>
    `;
    req.session.notice = null;
    res.send(layout({ title: "Inspection Run Report Admin", sidebar: baseSidebar(req), content, theme: req.session.themeMode }));
  } catch (error) {
    next(error);
  }
});

app.post("/admin/recipes/refresh", async (req, res, next) => {
  try {
    if (!req.session.inspector) return res.redirect("/");
    if (!req.session.canAccessAdmin) {
      req.session.notice = { kind: "warning", message: "Only admin users can refresh Digital IRRs." };
      return res.redirect("/workflow/inspection");
    }
    const result = await callBridge("sync_inspection_recipes");
    const refreshedCount = Object.values(result?.sync_counts || {}).reduce(
      (total, siteCounts) => total + Number(siteCounts?.InspectionRecipes || 0),
      0,
    );
    req.session.notice = {
      kind: "success",
      message: `SharePoint Digital IRRs refreshed. ${refreshedCount} item${refreshedCount === 1 ? "" : "s"} synchronized.`,
    };
    res.redirect("/admin");
  } catch (error) {
    next(error);
  }
});

app.post("/admin/recipes/import", async (req, res) => {
  try {
    if (!req.session.inspector || !req.session.canAccessAdmin) return res.redirect("/");
    const draft = await callBridge("parse_irr_upload", {
      file_name: req.body.file_name,
      file_content: req.body.file_content,
    });
    draft.branch = req.session.inspector.branch || "";
    draft.created_by = req.session.inspector.name;
    req.session.createRecipeDraft = draft;
    req.session.notice = {
      kind: "success",
      message: `${draft.rows.length} elements imported from ${draft.imported_file_name}. Review the draft below before saving.`,
    };
    res.redirect("/admin#build-digital-irr");
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message || "Unable to import that inspection report." };
    res.redirect("/admin");
  }
});

app.post("/admin/unlock-location", async (req, res, next) => {
  try {
    if (!req.session.inspector) return res.redirect("/");
    if (!req.session.canAccessAdmin) {
      req.session.notice = { kind: "warning", message: "Only admin users can unlock machines." };
      return res.redirect("/workflow/inspection");
    }
    const result = await callBridge("unlock_location", { location_id: req.body.location_id });
    req.session.notice = {
      kind: "success",
      message:
        result?.unlocked_count > 0
          ? `${result.location_name} was unlocked and is available again.`
          : `${result.location_name} was already available.`,
    };
    res.redirect("/admin");
  } catch (error) {
    next(error);
  }
});

app.use((error, req, res, _next) => {
  res.status(500).send(
    layout({
      title: "Inspection Run Report Error",
      sidebar: baseSidebar(req),
      theme: req.session?.themeMode,
      content: `<section class="card"><h1>Error</h1><p>${escapeHtml(error.message)}</p></section>`,
    }),
  );
});

const port = Number(process.env.NODE_APP_PORT || 3000);
app.listen(port, () => {
  console.log(`Inspection Run Report app listening on http://localhost:${port}`);
  if (WORKORDER_SYNC_ENABLED) {
    const intervalHours = Math.max(WORKORDER_SYNC_INTERVAL_HOURS, 1);
    const intervalMs = intervalHours * 60 * 60 * 1000;
    if (WORKORDER_SYNC_RUN_ON_START) {
      setTimeout(() => {
        runScheduledWorkOrderSync("startup");
      }, 1000);
    }
    setInterval(() => {
      runScheduledWorkOrderSync("interval");
    }, intervalMs);
    console.log(`[workorder-sync] enabled every ${intervalHours} hour(s)`);
  } else {
    console.log("[workorder-sync] disabled");
  }
});

app.get("/admin/recipes/:recipeHeaderId/edit", async (req, res, next) => {
  try {
    if (!req.session.inspector) return res.redirect("/");
    if (!req.session.canAccessAdmin) {
      req.session.notice = { kind: "warning", message: "Only admin users can edit local Digital IRRs." };
      return res.redirect("/admin");
    }

    const recipe = await callBridge("get_local_recipe_by_id", { recipe_header_id: req.params.recipeHeaderId });
    if (!recipe) {
      req.session.notice = { kind: "warning", message: "That local Digital IRR could not be found." };
      return res.redirect("/admin");
    }
    const builderOptions = await callBridge("get_recipe_builder_options", { branch: req.session.inspector.branch });
    const editRecipeDraft = req.session.editRecipeDrafts?.[req.params.recipeHeaderId] || null;
    const recipeForm = editRecipeDraft || recipe;
    const recipeRows = editRecipeDraft?.rows || recipe.rows || [];

    const content = `
      <section class="hero">
        <h1>Edit Local Digital IRR</h1>
        <p>Adjust Digital IRR details, drawing numbers, and inspected elements for this app-managed record.</p>
        <div class="badges">
          <a class="badge" href="/admin">Back to Admin Tools</a>
        </div>
      </section>
      ${renderNotice(req.session.notice)}
      <section class="card">
        <h2 class="section-title">Edit Digital IRR</h2>
        <p>Current revision: ${escapeHtml(recipe.recipe_version || 1)}. Saving changes will increment the revision number.</p>
        <form method="post" action="/admin/recipes/${encodeURIComponent(recipe.id)}/edit" class="form-grid">
          <div class="form-grid two">
            <div class="field"><label>Size</label><input name="size_label" value="${escapeHtml(recipeForm.size_label || "")}" required /></div>
            <div class="field"><label>Weight</label><input name="weight_label" value="${escapeHtml(recipeForm.weight_label || "")}" required /></div>
            <div class="field"><label>Grade</label><input name="grade_label" value="${escapeHtml(recipeForm.grade_label || "")}" required /></div>
            <div class="field"><label>Connector Type</label><input name="connector_type" value="${escapeHtml(recipeForm.connector_type || "")}" required /></div>
            <div class="field"><label>Drawing Number</label><input name="drawing" value="${escapeHtml(recipeForm.drawing || "")}" /></div>
            <div class="field"><label>First Article</label><input name="first_article_label" value="${escapeHtml(recipeForm.first_article_label || "")}" /></div>
            <div class="field"><label>Source Report Title</label><input name="source_report" value="${escapeHtml(recipeForm.source_report || "")}" /></div>
          </div>
          <div class="field">
            <label>Edit Comment</label>
            <textarea name="edit_comment" rows="3" placeholder="State why this Digital IRR is being changed" required>${escapeHtml(editRecipeDraft?.edit_comment || "")}</textarea>
          </div>
          <div class="table-wrap recipe-builder-table-wrap">
            <table class="recipe-builder-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Element</th>
                  <th>Measurement Type</th>
                  <th>Measurement Inputs</th>
                  <th>Gauge</th>
                  <th>Frequency</th>
                </tr>
              </thead>
              <tbody>
                ${renderRecipeBuilderRowsWithValues(builderOptions, recipeRows, 25)}
              </tbody>
            </table>
          </div>
          <div class="actions recipe-builder-actions">
            <button class="button secondary" type="button" data-add-recipe-row>Add Element</button>
          </div>
          <div class="actions">
            <button class="button" type="submit">Save Digital IRR Changes</button>
            <a class="button secondary" href="/admin">Cancel</a>
          </div>
        </form>
      </section>
    `;
    req.session.notice = null;
    res.send(layout({ title: "Edit Local Digital IRR", sidebar: baseSidebar(req), content, theme: req.session.themeMode }));
  } catch (error) {
    next(error);
  }
});

app.post("/admin/recipes", async (req, res, next) => {
  let recipePayload = null;
  try {
    if (!req.session.inspector || !req.session.canAccessAdmin) return res.redirect("/");
    recipePayload = buildSubmittedRecipePayload(req);

    const recipe = await callBridge("create_local_recipe", {
      recipe_payload: recipePayload,
    });

    req.session.createRecipeDraft = null;
    req.session.notice = { kind: "success", message: `Local Digital IRR saved: ${formatDigitalIrrName(recipe.display_name || recipe.recipe_name, recipe.drawing)}.` };
    res.redirect("/admin");
  } catch (error) {
    if (recipePayload) req.session.createRecipeDraft = recipePayload;
    req.session.notice = { kind: "warning", message: error.message || "Unable to save that Digital IRR." };
    res.redirect("/admin");
  }
});

app.post("/admin/recipes/:recipeHeaderId/edit", async (req, res, next) => {
  let recipePayload = null;
  try {
    if (!req.session.inspector || !req.session.canAccessAdmin) return res.redirect("/");
    recipePayload = buildSubmittedRecipePayload(req);

    const recipe = await callBridge("update_local_recipe", {
      recipe_header_id: req.params.recipeHeaderId,
      recipe_payload: recipePayload,
    });

    if (req.session.editRecipeDrafts) delete req.session.editRecipeDrafts[req.params.recipeHeaderId];
    req.session.notice = { kind: "success", message: `Local Digital IRR updated: ${formatDigitalIrrName(recipe.display_name || recipe.recipe_name, recipe.drawing)}.` };
    res.redirect("/admin");
  } catch (error) {
    if (recipePayload) {
      req.session.editRecipeDrafts = req.session.editRecipeDrafts || {};
      req.session.editRecipeDrafts[req.params.recipeHeaderId] = recipePayload;
    }
    req.session.notice = { kind: "warning", message: error.message || "Unable to update that local Digital IRR." };
    res.redirect(`/admin/recipes/${encodeURIComponent(req.params.recipeHeaderId)}/edit`);
  }
});

app.post("/admin/recipes/:recipeHeaderId/publish", async (req, res, next) => {
  try {
    if (!req.session.inspector || !req.session.canAccessAdmin) return res.redirect("/");
    const result = await callBridge("publish_local_recipe_to_sharepoint", {
      recipe_header_id: req.params.recipeHeaderId,
    });
    req.session.notice = {
      kind: "success",
      message: `SharePoint item ${result.action}: ${formatDigitalIrrName(result.recipe_name, result.drawing)} Rev ${result.recipe_version || 1}.`,
    };
  } catch (error) {
    req.session.notice = {
      kind: "warning",
      message: error.message || "Unable to publish that Digital IRR to SharePoint.",
    };
  }
  res.redirect("/admin");
});
