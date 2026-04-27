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
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
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

function renderNotice(notice) {
  if (!notice) return "";
  return `<div class="notice ${escapeHtml(notice.kind || "info")}">${escapeHtml(notice.message)}</div>`;
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

function formatDateValue(value) {
  if (value === null || value === undefined || value === "") return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const year = String(date.getFullYear());
  return `${month}/${day}/${year}`;
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
  if (!/^\d+$/.test(text)) return text;
  const nextValue = String(Number(text) + 1);
  return text.length > nextValue.length ? nextValue.padStart(text.length, "0") : nextValue;
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
  const text = String(value);
  if (!text.includes(".")) return 0;
  return text.split(".")[1].length;
}

function formatNumericReference(value, decimals) {
  const numericValue = Number(value);
  if (Number.isNaN(numericValue)) return "";
  return numericValue.toFixed(decimals);
}

function buildNumericEntryConfig(element) {
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

  const referenceSource = element.nominal ?? element.max ?? element.min;
  const reference = formatNumericReference(referenceSource, decimals);
  if (!reference || reference.length <= 2) {
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

function renderPipeHistorySheets(groups, { inspectorName = "", locationName = "", canManage = false } = {}) {
  if (!groups?.length) return "<p>No records found.</p>";
  return `
    <form id="pipe-history-sheet-delete-form" method="post" action="/workflow/history/delete">
      <input type="hidden" id="pipe-history-sheet-delete-pipe-unit-id" name="pipeUnitId" value="" />
    </form>
    <div class="pipe-history-sheet-list">
      ${groups
        .map((group) => {
          const drawing = group.recipeDefinition?.drawing || "";
          const connectionType = group.recipeDefinition?.connection_type || group.operationDescription || "";
          const reportTitle = group.recipeDefinition?.source_report || group.recipeDefinition?.recipe_name || "";
          return `
            <details class="table-card pipe-history-entry">
              <summary class="pipe-history-summary">
                <span class="pipe-history-summary-label"><strong>Workorder #:</strong> ${escapeHtml(group.productionNumber)}</span>
                <span class="pipe-history-summary-label"><strong>Connection Type:</strong> ${escapeHtml(connectionType)}</span>
                <span class="pipe-history-summary-label"><strong>Connections:</strong> ${escapeHtml(String(group.columns.length))}</span>
              </summary>
              <div class="inspection-sheet-meta pipe-history-sheet-meta">
                <div class="inspection-sheet-meta-row">
                  <div><strong>Date:</strong> ${escapeHtml(formatDateValue(group.latestUpdatedAt || new Date()))}</div>
                  <div><strong>Drawing #:</strong> ${escapeHtml(drawing)}</div>
                  <div><strong>Machine #:</strong> ${escapeHtml(locationName || "")}</div>
                </div>
                <div class="inspection-sheet-meta-row">
                  <div><strong>Inspector:</strong> ${escapeHtml(inspectorName || "")}</div>
                  <div><strong>Workorder #:</strong> ${escapeHtml(group.productionNumber)}</div>
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
                          >Connection # ${escapeHtml(column.pipeNumber || "")}</th>`,
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
              <div class="pipe-history-entry-actions">
                ${group.columns
                  .map(
                    (column) => `
                      <div class="pipe-history-entry-action-row">
                        <span class="pipe-history-entry-action-label">Connection # ${escapeHtml(column.pipeNumber || "")}</span>
                        <a class="button secondary compact-button" href="/report/pipe/${encodeURIComponent(column.pipeUnitId)}">View Full Report</a>
                        ${canManage ? `<a class="button secondary compact-button" href="/workflow/history/edit/${encodeURIComponent(column.pipeUnitId)}">Edit</a>` : ""}
                        ${canManage && column.status === "in_progress"
                          ? `<form method="post" action="/workflow/history/reset" onsubmit="return confirm('Reset this in-progress pipe inspection? The unfinished attempt will be removed and the pipe will roll back to its last resolved state.');">
                               <input type="hidden" name="pipeUnitId" value="${escapeHtml(column.pipeUnitId)}" />
                               <button class="button warning compact-button" type="submit">Reset In-Progress</button>
                             </form>`
                          : ""}
                        ${canManage
                          ? `<form method="post" action="/workflow/history/delete" onsubmit="return confirm('Delete this pipe inspection and all related attempts, measurements, and NCR records?');">
                               <input type="hidden" name="pipeUnitId" value="${escapeHtml(column.pipeUnitId)}" />
                               <button class="button danger compact-button" type="submit">Delete</button>
                             </form>`
                          : ""}
                      </div>`,
                  )
                  .join("")}
              </div>
            </details>
          `;
        })
        .join("")}
    </div>
  `;
}

function renderWorkflowNav(activePath) {
  const items = [
    { href: "/workflow/inspection", label: "Inspection Entry" },
    { href: "/workflow/history", label: "Pipe History" },
    { href: "/workflow/ncr", label: "NCR Queue" },
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
        <form method="post" action="/workflow/refresh-workorders" class="workflow-nav-refresh">
          <input type="hidden" name="redirectTo" value="${escapeHtml(activePath)}" />
          <button class="button secondary workflow-action-button" type="submit">Refresh Work Orders</button>
        </form>
        <form method="post" action="/logout" class="workflow-nav-logout">
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

  return `<input class="measurement-input inspection-table-input" data-capture-type="numeric" data-min="${escapeHtml(element.min ?? "")}" data-max="${escapeHtml(element.max ?? "")}" placeholder="${escapeHtml(element.nominal ?? "")}" value="${escapeHtml(carriedValue?.fullValue || "")}" name="element_${element.element_sequence}" required />`;
}

function renderCurrentAttemptWorksheet({ activeInspection, recipeDefinition, selection, sessionRecord, inspectorName, historyColumns = [] }) {
  const plan = activeInspection?.inspection_plan || [];
  const planMap = new Map(plan.map((element) => [Number(element.element_sequence), element]));
  const rows = recipeDefinition?.elements?.length ? recipeDefinition.elements : plan;
  const drawing = recipeDefinition?.drawing || "";
  const locationName = sessionRecord?.location_name || "";
  const currentPipeNumber = activeInspection?.pipe_number || selection.pipeNumber || "";
  const previousMeasurementsBySequence = historyColumns.length
    ? historyColumns[historyColumns.length - 1].measurementsBySequence
    : new Map();
  const loadPipeFormId = "load-pipe-form";
  const connectionCellForm = `
    <div class="connection-header-entry">
      <span class="connection-header-label">Connection #</span>
      <div class="connection-header-form">
        <input form="${loadPipeFormId}" type="text" name="pipeNumber" value="${escapeHtml(currentPipeNumber)}" placeholder="Enter pipe #" required />
        <button form="${loadPipeFormId}" type="submit" class="button compact-button">Load Pipe</button>
      </div>
    </div>
  `;

  return `
    <section class="card nested-card inspection-sheet-card">
      <h3 class="section-title">Current Attempt</h3>
      <p>${
        activeInspection
          ? `Attempt #${escapeHtml(activeInspection.attempt_no)} | ${activeInspection.is_rework ? "Re-work" : "First inspection"} | ${renderScopeBadge(activeInspection.inspection_scope)}`
          : "Enter the pipe number in the table header to start or resume that pipe."
      }</p>
      ${
        activeInspection && !plan.length
          ? renderNotice({ kind: "warning", message: "This attempt has no measurement plan yet. Go back to the selection above and make sure a recipe is selected before preparing the inspection." })
          : ""
      }
      <form id="${loadPipeFormId}" method="post" action="/workflow/start">
        <input type="hidden" name="productionNumber" value="${escapeHtml(selection.productionNumber || "")}" />
        <input type="hidden" name="sizeLabel" value="${escapeHtml(selection.sizeLabel || "")}" />
        <input type="hidden" name="weightLabel" value="${escapeHtml(selection.weightLabel || "")}" />
        <input type="hidden" name="connectionLabel" value="${escapeHtml(selection.connectionLabel || "")}" />
        <input type="hidden" name="endType" value="${escapeHtml(selection.endType || "")}" />
        <input type="hidden" name="recipeName" value="${escapeHtml(selection.recipeName || "")}" />
        <input type="hidden" name="inspectionScope" value="${escapeHtml(selection.inspectionScope || "standard")}" />
      </form>
      <form id="worksheet-history-delete-form" method="post" action="/workflow/history/delete">
        <input type="hidden" id="worksheet-history-delete-pipe-unit-id" name="pipeUnitId" value="" />
      </form>
      <form method="post" action="/workflow/complete" class="form-grid">
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
                    >Connection # ${escapeHtml(column.pipe_number || "")}</th>`,
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
        <div class="field"><label>Attempt Notes</label><textarea name="notes"></textarea></div>
        <div class="actions">${activeInspection ? `<button id="complete-inspection-button" class="button" type="submit">Complete Inspection</button>` : ""}</div>
      </form>
    </section>
  `;
}

function renderRecipeBuilderRows(builderOptions, rowCount = 25) {
  const elementOptions = builderOptions?.element_options || [];
  const gaugeOptions = builderOptions?.gauge_options || [];
  const measurementModes = builderOptions?.measurement_modes || [];
  const frequencyOptions = builderOptions?.frequency_options || [];

  return Array.from({ length: rowCount }, (_, index) => {
    const rowNumber = index + 1;
    return `
      <tr>
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
                <input name="row_${rowNumber}_tol_digits" placeholder="Tol digits" />
              </div>
            </div>
            <div class="recipe-mode-panel hidden" data-mode-panel="range">
              <div class="recipe-mini-grid">
                <input name="row_${rowNumber}_range_min" placeholder="Low" />
                <input name="row_${rowNumber}_range_max" placeholder="High" />
              </div>
            </div>
            <div class="recipe-mode-panel hidden" data-mode-panel="deviation">
              <div class="recipe-mini-grid">
                <select name="row_${rowNumber}_dev_places">
                  <option value="3">3 dp</option>
                  <option value="4">4 dp</option>
                </select>
                <input name="row_${rowNumber}_dev_digits" placeholder="Tol digits" />
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

function renderRecipeBuilderRowsWithValues(builderOptions, existingRows = [], rowCount = 25) {
  const rowsBySequence = new Map(existingRows.map((row) => [Number(row.element_sequence), row]));
  const elementOptions = builderOptions?.element_options || [];
  const gaugeOptions = builderOptions?.gauge_options || [];
  const measurementModes = builderOptions?.measurement_modes || [];
  const frequencyOptions = builderOptions?.frequency_options || [];

  return Array.from({ length: rowCount }, (_, index) => {
    const rowNumber = index + 1;
    const row = rowsBySequence.get(rowNumber) || {};
    const selectedElement = elementOptions.includes(row.element_description) ? row.element_description : "";
    const customElement = selectedElement ? "" : (row.element_description || "");
    const selectedGauge = gaugeOptions.includes(row.gauge) ? row.gauge : "";
    const customGauge = selectedGauge ? "" : (row.gauge || "");

    return `
      <tr>
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
              <input name="row_${rowNumber}_nominal" placeholder="Nominal" value="${escapeHtml(row.nominal ?? "")}" />
              <div class="recipe-mini-grid">
                <select name="row_${rowNumber}_tol_places">
                  <option value="3" ${String(row.value_format || "").includes("decimal") ? "selected" : ""}>3 dp</option>
                  <option value="4">4 dp</option>
                </select>
                <input name="row_${rowNumber}_tol_digits" placeholder="Tol digits" />
              </div>
            </div>
            <div class="recipe-mode-panel ${row.measurement_mode === "range" ? "" : "hidden"}" data-mode-panel="range">
              <div class="recipe-mini-grid">
                <input name="row_${rowNumber}_range_min" placeholder="Low" value="${escapeHtml(row.min_value ?? "")}" />
                <input name="row_${rowNumber}_range_max" placeholder="High" value="${escapeHtml(row.max_value ?? "")}" />
              </div>
            </div>
            <div class="recipe-mode-panel ${row.measurement_mode === "deviation" ? "" : "hidden"}" data-mode-panel="deviation">
              <div class="recipe-mini-grid">
                <select name="row_${rowNumber}_dev_places">
                  <option value="3" selected>3 dp</option>
                  <option value="4">4 dp</option>
                </select>
                <input name="row_${rowNumber}_dev_digits" placeholder="Tol digits" />
              </div>
            </div>
            <div class="recipe-mode-panel ${row.measurement_mode === "visual" ? "" : "hidden"}" data-mode-panel="visual">
              <input name="row_${rowNumber}_visual_spec" placeholder="Visual spec / SOP" value="${escapeHtml(row.dwg_dim || "")}" />
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
      <div class="app-shell">
        <aside class="sidebar">
          <button type="button" class="sidebar-toggle" id="sidebar-toggle" aria-label="Toggle sidebar">«</button>
          <div class="sidebar-inner">${sidebar}</div>
        </aside>
        <main class="content">${content}</main>
      </div>
      <script>
        (function () {
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
        })();

        (function () {
          const inspectionSheetWrap = document.querySelector(".inspection-sheet-wrap");
          if (inspectionSheetWrap) {
            requestAnimationFrame(() => {
              requestAnimationFrame(() => {
                inspectionSheetWrap.scrollLeft = inspectionSheetWrap.scrollWidth;
              });
            });
          }

          const historyActionCells = document.querySelectorAll(".history-column-action");
          const historyDeleteForm = document.getElementById("worksheet-history-delete-form");
          const historyDeletePipeInput = document.getElementById("worksheet-history-delete-pipe-unit-id");

          historyActionCells.forEach((cell) => {
            cell.addEventListener("click", (event) => {
              const editUrl = cell.dataset.editUrl;
              if (!editUrl) return;
              if (!window.confirm("Edit this saved pipe record?")) {
                event.preventDefault();
                return;
              }
              window.location.href = editUrl;
            });

            cell.addEventListener("contextmenu", (event) => {
              const pipeUnitId = cell.dataset.historyPipeId;
              if (!pipeUnitId || !historyDeleteForm || !historyDeletePipeInput) return;
              event.preventDefault();
              if (!window.confirm("Delete this saved pipe inspection and all related attempts, measurements, and NCR records?")) {
                return;
              }
              historyDeletePipeInput.value = pipeUnitId;
              historyDeleteForm.submit();
            });
          });

          const inputs = document.querySelectorAll(".measurement-input");
          const modeSelects = document.querySelectorAll(".recipe-mode-select");
          if (modeSelects.length) {
            const syncRecipeModeRow = (select) => {
              const row = select.dataset.row;
              const wrapper = document.querySelector('.recipe-mode-fields[data-row="' + row + '"]');
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

          if (!inputs.length) return;
          const resultPanel = document.getElementById("inspection-table-result");
          const resultText = document.getElementById("inspection-table-result-text");
          const resultDetail = document.getElementById("inspection-table-result-detail");
          const failureActionSelect = document.getElementById("failure-action-select");
          const failureControls = document.getElementById("inspection-result-fail-controls");
          const failureFields = document.getElementById("failure-fields");
          const managerApprovalModal = document.getElementById("manager-approval-modal");
          const managerNameInput = document.getElementById("manager-name-input");
          const managerReasonInput = document.getElementById("manager-reason-input");
          const completeInspectionButton = document.getElementById("complete-inspection-button");
          const approvalCloseButtons = document.querySelectorAll("[data-approval-close]");
          const focusableMeasurementInputs = Array.from(
            document.querySelectorAll(".measurement-short-input, .inspection-table-input, .measurement-checkbox"),
          );

          const setApprovalModalVisible = (visible) => {
            if (!managerApprovalModal) return;
            managerApprovalModal.classList.toggle("hidden", !visible);
            managerApprovalModal.setAttribute("aria-hidden", visible ? "false" : "true");
          };

          const getEffectiveValue = (element) => {
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
            return String(element.value ?? "").trim();
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
        })();
      </script>
    </body>
    </html>`;
}

function baseSidebar(req) {
  const inspector = req.session.inspector;
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
           <a class="button secondary sidebar-link-button" href="/workflow/inspection">Inspection Workflow</a>
           ${
             req.session.canAccessAdmin
               ? `<a class="button sidebar-link-button" href="/admin">Admin Tools</a>`
               : ""
           }
           <form method="post" action="/logout"><button type="submit">Log Out</button></form>`
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

app.post("/logout", async (req, res) => {
  if (req.session.sessionRecord?.id) {
    await callBridge("close_inspector_session", { session_id: req.session.sessionRecord.id });
  }
  req.session.destroy(() => res.redirect("/"));
});

app.get("/", async (req, res, next) => {
  try {
    await ensureInitialized();
    if (req.session.sessionRecord && req.session.inspector) {
      return res.redirect("/workflow");
    }

    const locations = await callBridge("get_locations");
    const pending = req.session.pendingLoginContext || null;
    const operators = pending ? await callBridge("get_cnc_operators", { branch: pending.inspector.branch }) : [];

    const content = `
      <section class="hero">
        <h1>Inspection Run Report</h1>
        <p>Manage inspector login, inspection entry, pipe history, NCR follow-up, and supervisor approvals in one place.</p>
        <div class="badges">
          <span class="badge">Login</span>
          <span class="badge">Workflow</span>
          <span class="badge">Admin</span>
        </div>
      </section>
      ${renderNotice(req.session.notice)}
      <section class="card">
        <details class="login-panel"${pending ? "" : " open"}>
          <summary>
            <span class="section-title">Inspector Login</span>
            ${pending ? '<span class="summary-hint">Change inspector or session setup</span>' : ""}
          </summary>
          <form method="post" action="/login/find" class="form-grid">
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
            <div class="field">
              <label>Location / Machine</label>
              <select name="location_id">${renderOptions(locations, null, (item) => item.id, (item) => item.location_name)}</select>
            </div>
            <div class="actions"><button class="button" type="submit">Find Inspector</button></div>
          </form>
        </details>
      </section>
      ${
        pending
          ? `<section class="card">
               <h2 class="section-title">Inspector Ready</h2>
               <p>${escapeHtml(pending.inspector.name)} | ${escapeHtml(req.session.roleLabel || "Inspector")} | ${escapeHtml(pending.shift)} | ${escapeHtml(pending.location.location_name)}</p>
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
    const locations = await callBridge("get_locations");
    const location = locations.find((item) => String(item.id) === String(req.body.location_id));
    const isAdmin = await callBridge("is_admin_user", { employee: inspector });
    const isManager = await callBridge("is_manager_or_supervisor", { employee: inspector });
    req.session.roleLabel = isManager ? "Manager/Supervisor" : isAdmin ? "Admin Access Only" : "Inspector";
    req.session.canAccessAdmin = Boolean(isAdmin);
    req.session.pendingLoginContext = {
      inspector,
      shift: req.body.shift || (await callBridge("determine_shift")),
      location,
    };
    req.session.notice = { kind: "success", message: `Inspector found: ${inspector.name}` };
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

    const workOrders = await callBridge("get_open_work_orders", { branch: inspector.branch });
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
      ${renderWorkflowNav("/workflow/inspection")}
      <details class="card inspection-entry-panel"${showWorksheet ? "" : " open"}>
        <summary class="section-title">Inspection Entry</summary>
        <form method="get" action="/workflow/inspection" class="form-grid inspection-entry-form" id="inspection-selection-form">
          <div class="field"><label>Production Number / WO</label><select name="productionNumber" onchange="this.form.recipeName.value=''; this.form.submit();">${renderOptions(productionNumbers, selection.productionNumber, (item) => item, (item) => item)}</select></div>
          <div class="inspection-entry-inline-row">
            <div class="field"><label>Size</label><input name="sizeLabel" value="${escapeHtml(selection.sizeLabel)}" oninput="this.form.recipeName.value='';" /></div>
            <div class="field"><label>Weight</label><input name="weightLabel" value="${escapeHtml(selection.weightLabel)}" oninput="this.form.recipeName.value='';" /></div>
            <div class="field"><label>Connection</label><input name="connectionLabel" value="${escapeHtml(selection.connectionLabel)}" oninput="this.form.recipeName.value='';" /></div>
            <div class="field"><label>Box / Pin</label><select name="endType" onchange="this.form.recipeName.value='';"><option value=""></option><option value="BOX" ${selection.endType === "BOX" ? "selected" : ""}>Box</option><option value="PIN" ${selection.endType === "PIN" ? "selected" : ""}>Pin</option></select></div>
          </div>
          <div class="field"><label>Recipe</label><select name="recipeName"><option value=""></option>${renderOptions(recipeCandidates, selection.recipeName, (item) => item.recipe_name, (item) => item.recipe_name)}</select></div>
          <div class="field"><label>Inspection Scope</label><select name="inspectionScope"><option value="standard" ${selection.inspectionScope === "full" ? "" : "selected"}>Standard Inspection</option><option value="full" ${selection.inspectionScope === "full" ? "selected" : ""}>Full Inspection</option></select></div>
          <div class="actions">
            <button class="button secondary" type="submit">Load Recipe Options</button>
            <button class="button workflow-action-button" type="submit" formaction="/workflow/start" formmethod="post">Start Inspection</button>
          </div>
        </form>
      </details>
        ${
          existingPipe
            ? existingPipe.current_status === "in_progress"
              ? renderNotice({
                  kind: "warning",
                  message: `Pipe ${selection.pipeNumber} already has an in-progress inspection for this WO/connection. Starting inspection will resume attempt #${existingPipe.latest_attempt_no}.`,
                })
              : ["completed", "rework"].includes(existingPipe.current_status)
                ? renderNotice({
                    kind: "warning",
                    message: `Pipe ${selection.pipeNumber} already has completed/re-work history for this WO/connection. Starting inspection will create re-work attempt #${existingPipe.latest_attempt_no + 1}.`,
                  })
                : ""
            : ""
        }
        ${lookupDescription ? renderNotice({ kind: "info", message: `Inspection selection: ${lookupDescription}` }) : ""}
        ${
          recipeDefinition
            ? `<section class="card nested-card">
                 <h3 class="section-title">Recipe Summary</h3>
                 <div class="badges">
                   ${recipeDefinition.recipe_name ? `<span class="pill">${escapeHtml(recipeDefinition.recipe_name)}</span>` : ""}
                   ${recipeDefinition.connection_type ? `<span class="pill">Connection: ${escapeHtml(recipeDefinition.connection_type)}</span>` : ""}
                   ${recipeDefinition.drawing ? `<span class="pill">Drawing: ${escapeHtml(recipeDefinition.drawing)}</span>` : ""}
                   ${recipeDefinition.source_report ? `<span class="pill">Report: ${escapeHtml(recipeDefinition.source_report)}</span>` : ""}
                 </div>
               </section>
               <details>
                 <summary>Recipe Elements Preview</summary>
                 ${renderTable(
                   recipeDefinition.elements.map(({ item_id, capture_type, ...element }) => element),
                 )}
               </details>`
            : renderNotice({ kind: "warning", message: "Select a connection and recipe before preparing the inspection." })
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
    `;

    req.session.notice = null;
    res.send(layout({ title: "Inspection Run Report Workflow", sidebar: baseSidebar(req), content, theme: req.session.themeMode }));
  } catch (error) {
    next(error);
  }
});

app.get("/workflow/history", async (req, res, next) => {
  try {
    if (!req.session.inspector || !req.session.sessionRecord) return res.redirect("/");
    const inspector = req.session.inspector;
    const canManage = Boolean(req.session.inspector);
    const allPipeRows = await callBridge("search_pipe_units", {
      branch: inspector.branch,
      production_number: null,
      pipe_number: null,
      status: null,
      inspection_scope: null,
    });
    const productionOptions = [...new Set(allPipeRows.map((item) => item.production_number).filter(Boolean))].sort();
    const pipeNumberOptions = [...new Set(allPipeRows.map((item) => item.pipe_number).filter(Boolean))].sort((a, b) =>
      String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: "base" }),
    );
    const pipeRows = await callBridge("search_pipe_units", {
      branch: inspector.branch,
      production_number: req.query.historyProduction || null,
      pipe_number: req.query.historyPipe || null,
      status: req.query.historyStatus || null,
      inspection_scope: req.query.historyScope || null,
    });
    const content = `
      ${renderWorkflowHeader(req)}
      ${renderNotice(req.session.notice)}
      ${renderWorkflowNav("/workflow/history")}
      <section class="table-card">
        <h2 class="section-title">Pipe History</h2>
        <p>Search by work order, pipe number, or status to review prior attempts and measurements.</p>
      </section>
      <section class="table-card">
        <form method="get" action="/workflow/history" class="form-grid two">
          <div class="field"><label>Filter Production Number</label><select name="historyProduction"><option value=""></option>${renderOptions(productionOptions, req.query.historyProduction || "", (item) => item, (item) => item)}</select></div>
          <div class="field"><label>Filter Pipe Number</label><select name="historyPipe"><option value=""></option>${renderOptions(pipeNumberOptions, req.query.historyPipe || "", (item) => item, (item) => item)}</select></div>
          <div class="field"><label>Filter Status</label><select name="historyStatus"><option value=""></option><option value="in_progress" ${req.query.historyStatus === "in_progress" ? "selected" : ""}>in_progress</option><option value="completed" ${req.query.historyStatus === "completed" ? "selected" : ""}>completed</option><option value="rework" ${req.query.historyStatus === "rework" ? "selected" : ""}>rework</option><option value="scrapped" ${req.query.historyStatus === "scrapped" ? "selected" : ""}>scrapped</option></select></div>
          <div class="field"><label>Filter Scope</label><select name="historyScope"><option value=""></option><option value="standard" ${req.query.historyScope === "standard" ? "selected" : ""}>Standard Inspection</option><option value="full" ${req.query.historyScope === "full" ? "selected" : ""}>Full Inspection</option></select></div>
          <div class="actions"><button class="button" type="submit">Search Pipe History</button></div>
        </form>
        ${renderPipeHistoryResults(pipeRows, canManage)}
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

    const content = `
      ${renderWorkflowHeader(req)}
      ${renderNotice(req.session.notice)}
      ${renderWorkflowNav("/workflow/history")}
      <section class="table-card">
        <h2 class="section-title">Edit Pipe Record</h2>
        <p>Update the pipe identifiers without removing the inspection history tied to this record.</p>
      </section>
      <section class="table-card">
        <form method="post" action="/workflow/history/edit/${encodeURIComponent(pipeRow.id)}" class="form-grid">
          <div class="field"><label>Production Number / WO</label><input name="production_number" value="${escapeHtml(pipeRow.production_number)}" required /></div>
          <div class="field"><label>Connection Type / Operation Description</label><input name="operation_description" value="${escapeHtml(pipeRow.operation_description)}" required /></div>
          <div class="field"><label>Pipe Number</label><input name="pipe_number" value="${escapeHtml(pipeRow.pipe_number)}" required /></div>
          <div class="field"><label>Current Status</label><input value="${escapeHtml(pipeRow.current_status)}" disabled /></div>
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
    const pipeUnitId = req.body.pipeUnitId;
    if (!pipeUnitId) {
      req.session.notice = { kind: "warning", message: "No pipe inspection was selected for deletion." };
      return res.redirect("/workflow/history");
    }

    await callBridge("delete_pipe_unit", { pipe_unit_id: pipeUnitId });
    if (req.session.activeInspection && String(req.session.activeInspection.pipe_unit_id) === String(pipeUnitId)) {
      req.session.activeInspection = null;
    }
    req.session.notice = { kind: "success", message: "The selected pipe inspection and its related records were deleted." };
    res.redirect("/workflow/history");
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
    if (!req.session.inspector || !req.session.sessionRecord) return res.redirect("/");
    const inspector = req.session.inspector;
    const pipeRows = await callBridge("search_pipe_units", { branch: inspector.branch });
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
        ? await callBridge("get_recipe_elements", { recipe_name: attempt.recipe_name, branch: inspector.branch })
        : null;
      attemptsWithMeasurements.push({ ...attempt, measurements, recipeDefinition });
    }

    const branchNcrs = await callBridge("get_ncr_reports", { branch: inspector.branch, status: null });
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
                        <span class="worksheet-meta-label">Connection #:</span>
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
                        "Review the recorded measurements above against the recipe and drawing requirements for this connection.",
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
    if (!req.session.inspector || !req.session.sessionRecord) return res.redirect("/");
    const inspector = req.session.inspector;
    const ncrRows = await callBridge("get_ncr_reports", {
      branch: inspector.branch,
      status: req.query.ncrStatus || null,
    });
    const content = `
      ${renderWorkflowHeader(req)}
      ${renderNotice(req.session.notice)}
      ${renderWorkflowNav("/workflow/ncr")}
      <section class="table-card">
        <h2 class="section-title">NCR Queue</h2>
        <p>Review open NCRs, update disposition details, and close records when the pipe is resolved.</p>
      </section>
      <section class="table-card">
        <form method="get" action="/workflow/ncr" class="form-grid">
          <div class="field"><label>NCR Status</label><select name="ncrStatus"><option value=""></option><option value="open">open</option><option value="closed">closed</option></select></div>
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
    const selection = {
      productionNumber: req.body.productionNumber || "",
      sizeLabel: req.body.sizeLabel || "",
      weightLabel: req.body.weightLabel || "",
      connectionLabel: req.body.connectionLabel || "",
      endType: req.body.endType || "",
      recipeName: req.body.recipeName || "",
      pipeNumber: req.body.pipeNumber || "",
      inspectionScope: req.body.inspectionScope || "standard",
    };
    req.session.selection = selection;
    req.session.showInspectionSheet = true;
    const inspector = req.session.inspector;
    const sessionRecord = req.session.sessionRecord;
    const lookupDescription = buildInspectionConnectionLabel(selection);
    if (!lookupDescription) {
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
      req.session.notice = { kind: "warning", message: "Load and choose a recipe before starting the inspection." };
      return res.redirect("/workflow/inspection");
    }
    selection.recipeName = recipeName;
    req.session.selection = selection;
    const recipeDefinition = await callBridge("get_recipe_elements", { recipe_name: recipeName, branch: inspector.branch });
    if (!recipeDefinition?.elements?.length) {
      req.session.notice = { kind: "warning", message: "The selected recipe does not have any elements to inspect." };
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
    const result = await callBridge("complete_inspection_attempt", {
      params: {
        attempt_id: active.attempt_id,
        pipe_unit_id: active.pipe_unit_id,
        measurements: evaluation.measurements,
        disposition,
        notes: req.body.notes || "",
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
      kind: evaluation.has_failures ? "warning" : "success",
      message: evaluation.has_failures
        ? `Inspection failed automatically based on the entered measurements. Attempt status: ${result.attempt_status}. Pipe status: ${result.pipe_status}.`
        : `Inspection passed automatically. Attempt status: ${result.attempt_status}. Pipe status: ${result.pipe_status}.`,
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
    const managers = await callBridge("get_manager_candidates", { branch: inspector.branch });
    const builderOptions = await callBridge("get_recipe_builder_options", { branch: inspector.branch });
    const localRecipes = await callBridge("list_local_recipes", { branch: inspector.branch });
    const content = `
      <section class="hero">
        <h1>Inspection Run Report Admin</h1>
        <p>Administrative tools for setup and maintenance.</p>
        <div class="badges">
          <a class="badge" href="/workflow/inspection">Back to Inspection Workflow</a>
        </div>
      </section>
      ${renderNotice(req.session.notice)}
      <section class="card">
        <h2 class="section-title">Manager PIN Setup</h2>
        <form method="post" action="/admin/manager-pin" class="form-grid">
          <div class="field"><label>Manager</label><select name="manager_item_id">${renderOptions(managers, null, (item) => item.item_id, (item) => item.name)}</select></div>
          <div class="field"><label>New PIN</label><input type="password" name="pin" /></div>
          <div class="actions"><button class="button" type="submit">Save Manager PIN</button></div>
        </form>
      </section>
      <section class="card">
        <h2 class="section-title">Build Recipe</h2>
        <p>Create an app-managed recipe with up to 25 inspection elements. Saved recipes are immediately available to the inspection workflow.</p>
        <form method="post" action="/admin/recipes" class="form-grid">
          <div class="form-grid two">
            <div class="field"><label>Size</label><input name="size_label" placeholder='2.875' required /></div>
            <div class="field"><label>Weight</label><input name="weight_label" placeholder='7.90#' required /></div>
            <div class="field"><label>Grade</label><input name="grade_label" placeholder='BTS-6' required /></div>
            <div class="field"><label>Connector Type</label><input name="connector_type" placeholder='PIN' required /></div>
            <div class="field"><label>Drawing Number</label><input name="drawing" placeholder='013 Rev 2' /></div>
            <div class="field"><label>Source Report Title</label><input name="source_report" placeholder='2.875 7.90# BTS-6 (PIN) INSPECTION REPORT' /></div>
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
                ${renderRecipeBuilderRows(builderOptions, 25)}
              </tbody>
            </table>
          </div>
          <div class="actions"><button class="button" type="submit">Save Recipe</button></div>
        </form>
      </section>
      <section class="card">
        <h2 class="section-title">Local Recipes</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Recipe</th>
                <th>Connection</th>
                <th>Drawing</th>
                <th>Source Report</th>
                <th>Updated</th>
                <th>Admin</th>
              </tr>
            </thead>
            <tbody>
              ${localRecipes
                .map(
                  (recipe) => `<tr>
                    <td>${escapeHtml(recipe.recipe_name)}</td>
                    <td>${escapeHtml(recipe.connection_type)}</td>
                    <td>${escapeHtml(recipe.drawing)}</td>
                    <td>${escapeHtml(recipe.source_report)}</td>
                    <td>${escapeHtml(formatDateValue(recipe.updated_at))}</td>
                    <td><a class="button secondary compact-button" href="/admin/recipes/${encodeURIComponent(recipe.id)}/edit">Edit</a></td>
                  </tr>`,
                )
                .join("")}
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

app.post("/admin/manager-pin", async (req, res, next) => {
  try {
    const managers = await callBridge("get_manager_candidates", { branch: req.session.inspector.branch });
    const manager = managers.find((item) => String(item.item_id) === String(req.body.manager_item_id));
    await callBridge("set_manager_pin", { manager_employee: manager, pin: req.body.pin });
    req.session.notice = { kind: "success", message: `Manager PIN saved for ${manager.name}.` };
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
      req.session.notice = { kind: "warning", message: "Only admin users can edit local recipes." };
      return res.redirect("/admin");
    }

    const recipe = await callBridge("get_local_recipe_by_id", { recipe_header_id: req.params.recipeHeaderId });
    if (!recipe) {
      req.session.notice = { kind: "warning", message: "That local recipe could not be found." };
      return res.redirect("/admin");
    }
    const builderOptions = await callBridge("get_recipe_builder_options", { branch: req.session.inspector.branch });

    const content = `
      <section class="hero">
        <h1>Edit Local Recipe</h1>
        <p>Adjust recipe details, drawing numbers, and inspected elements for this app-managed recipe.</p>
        <div class="badges">
          <a class="badge" href="/admin">Back to Admin Tools</a>
        </div>
      </section>
      ${renderNotice(req.session.notice)}
      <section class="card">
        <h2 class="section-title">Edit Recipe</h2>
        <form method="post" action="/admin/recipes/${encodeURIComponent(recipe.id)}/edit" class="form-grid">
          <div class="form-grid two">
            <div class="field"><label>Size</label><input name="size_label" value="${escapeHtml(recipe.size_label || "")}" required /></div>
            <div class="field"><label>Weight</label><input name="weight_label" value="${escapeHtml(recipe.weight_label || "")}" required /></div>
            <div class="field"><label>Grade</label><input name="grade_label" value="${escapeHtml(recipe.grade_label || "")}" required /></div>
            <div class="field"><label>Connector Type</label><input name="connector_type" value="${escapeHtml(recipe.connector_type || "")}" required /></div>
            <div class="field"><label>Drawing Number</label><input name="drawing" value="${escapeHtml(recipe.drawing || "")}" /></div>
            <div class="field"><label>Source Report Title</label><input name="source_report" value="${escapeHtml(recipe.source_report || "")}" /></div>
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
                ${renderRecipeBuilderRowsWithValues(builderOptions, recipe.rows || [], 25)}
              </tbody>
            </table>
          </div>
          <div class="actions">
            <button class="button" type="submit">Save Recipe Changes</button>
            <a class="button secondary" href="/admin">Cancel</a>
          </div>
        </form>
      </section>
    `;
    req.session.notice = null;
    res.send(layout({ title: "Edit Local Recipe", sidebar: baseSidebar(req), content, theme: req.session.themeMode }));
  } catch (error) {
    next(error);
  }
});

app.post("/admin/recipes", async (req, res, next) => {
  try {
    if (!req.session.inspector || !req.session.canAccessAdmin) return res.redirect("/");
    const rows = Array.from({ length: 25 }, (_, index) => {
      const rowNumber = index + 1;
      const measurementMode = req.body[`row_${rowNumber}_mode`] || "";
      const selectedElement = req.body[`row_${rowNumber}_element`] || "";
      const customElement = req.body[`row_${rowNumber}_element_custom`] || "";
      const selectedGauge = req.body[`row_${rowNumber}_gauge`] || "";
      const customGauge = req.body[`row_${rowNumber}_gauge_custom`] || "";
      return {
        element_description: String(customElement || selectedElement).trim(),
        measurement_mode: measurementMode,
        gauge: String(customGauge || selectedGauge).trim(),
        frequency: req.body[`row_${rowNumber}_frequency`] || "every_pipe",
        nominal_value: req.body[`row_${rowNumber}_nominal`] || "",
        tolerance_decimal_places:
          measurementMode === "deviation"
            ? req.body[`row_${rowNumber}_dev_places`] || "3"
            : req.body[`row_${rowNumber}_tol_places`] || "3",
        tolerance_digits:
          measurementMode === "deviation"
            ? req.body[`row_${rowNumber}_dev_digits`] || ""
            : req.body[`row_${rowNumber}_tol_digits`] || "",
        range_min: req.body[`row_${rowNumber}_range_min`] || "",
        range_max: req.body[`row_${rowNumber}_range_max`] || "",
        visual_spec: req.body[`row_${rowNumber}_visual_spec`] || "",
      };
    });

    const recipe = await callBridge("create_local_recipe", {
      recipe_payload: {
        branch: req.session.inspector.branch,
        size_label: req.body.size_label,
        weight_label: req.body.weight_label,
        grade_label: req.body.grade_label,
        connector_type: req.body.connector_type,
        drawing: req.body.drawing,
        source_report: req.body.source_report,
        created_by: req.session.inspector.name,
        rows,
      },
    });

    req.session.notice = { kind: "success", message: `Local recipe saved: ${recipe.recipe_name}.` };
    res.redirect("/admin");
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message || "Unable to save that recipe." };
    res.redirect("/admin");
  }
});

app.post("/admin/recipes/:recipeHeaderId/edit", async (req, res, next) => {
  try {
    if (!req.session.inspector || !req.session.canAccessAdmin) return res.redirect("/");
    const rows = Array.from({ length: 25 }, (_, index) => {
      const rowNumber = index + 1;
      const measurementMode = req.body[`row_${rowNumber}_mode`] || "";
      const selectedElement = req.body[`row_${rowNumber}_element`] || "";
      const customElement = req.body[`row_${rowNumber}_element_custom`] || "";
      const selectedGauge = req.body[`row_${rowNumber}_gauge`] || "";
      const customGauge = req.body[`row_${rowNumber}_gauge_custom`] || "";
      return {
        element_description: String(customElement || selectedElement).trim(),
        measurement_mode: measurementMode,
        gauge: String(customGauge || selectedGauge).trim(),
        frequency: req.body[`row_${rowNumber}_frequency`] || "every_pipe",
        nominal_value: req.body[`row_${rowNumber}_nominal`] || "",
        tolerance_decimal_places:
          measurementMode === "deviation"
            ? req.body[`row_${rowNumber}_dev_places`] || "3"
            : req.body[`row_${rowNumber}_tol_places`] || "3",
        tolerance_digits:
          measurementMode === "deviation"
            ? req.body[`row_${rowNumber}_dev_digits`] || ""
            : req.body[`row_${rowNumber}_tol_digits`] || "",
        range_min: req.body[`row_${rowNumber}_range_min`] || "",
        range_max: req.body[`row_${rowNumber}_range_max`] || "",
        visual_spec: req.body[`row_${rowNumber}_visual_spec`] || "",
      };
    });

    const recipe = await callBridge("update_local_recipe", {
      recipe_header_id: req.params.recipeHeaderId,
      recipe_payload: {
        branch: req.session.inspector.branch,
        size_label: req.body.size_label,
        weight_label: req.body.weight_label,
        grade_label: req.body.grade_label,
        connector_type: req.body.connector_type,
        drawing: req.body.drawing,
        source_report: req.body.source_report,
        created_by: req.session.inspector.name,
        rows,
      },
    });

    req.session.notice = { kind: "success", message: `Local recipe updated: ${recipe.recipe_name}.` };
    res.redirect("/admin");
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message || "Unable to update that local recipe." };
    res.redirect(`/admin/recipes/${encodeURIComponent(req.params.recipeHeaderId)}/edit`);
  }
});
