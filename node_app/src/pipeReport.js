import { reliefColumnClass, reliefColumnLabel } from "./inspectionRelief.js";
import { canViewPipeReport } from "./inspectionPermissions.js";

const escape = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
const time = (value) => value ? `<time datetime="${escape(value)}">${escape(value)}</time>` : "—";
const label = (value) => String(value || "").replaceAll("_", " ");
const resultLabel = (status) => ({ passed: "Pass", approved: "Pass with Approval", rework: "Re-work", scrapped: "Scrapped", in_progress: "In Progress" })[status] || label(status);
const measurementKey = (measurement, index) => JSON.stringify([measurement.element_sequence ?? index + 1, measurement.element_description || "", measurement.dwg_dim || "", measurement.gauge || ""]);

export function buildPipeReportGrids(pipes) {
  const groups = new Map();
  for (const pipe of pipes) {
    const key = JSON.stringify([pipe.location_id ?? pipe.location_name ?? "", pipe.branch || "", pipe.production_number || "", pipe.operation_description || "", pipe.recipe_name || ""]);
    if (!groups.has(key)) groups.set(key, { key, first: pipe, columns: [], rows: new Map() });
    const group = groups.get(key);
    const measurements = new Map();
    (pipe.recipe_elements || []).forEach((row, index) => group.rows.set(measurementKey(row, index), row));
    (pipe.measurements || []).forEach((row, index) => {
      const rowKey = measurementKey(row, index);
      measurements.set(rowKey, row);
      group.rows.set(rowKey, row);
    });
    group.columns.push({ pipe, measurements });
  }
  return [...groups.values()].map((group) => ({ ...group, rows: [...group.rows.entries()].sort((a, b) => Number(a[1].element_sequence || 0) - Number(b[1].element_sequence || 0)) }));
}

function verificationCell(pipe) {
  if (pipe.verification) return `<strong class="pipe-report-verified">Verified</strong><small>Verified by ${escape(pipe.verification.verifier_name)} (${escape(pipe.verification.verifier_title)})</small><small>${time(pipe.verification.verified_at)}</small>`;
  if (pipe.status === "in_progress") return '<span class="inspection-sheet-not-due">Awaiting submission</span>';
  return `${pipe.verification_changed ? '<small>Needs re-verification</small>' : ''}<form method="post" action="/workflow/pipe-report/verify" data-verify-inspection><input type="hidden" name="attempt_id" value="${pipe.attempt_id}" /><input type="hidden" name="snapshot_hash" value="${escape(pipe.snapshot_hash)}" /><button type="submit" class="button compact-button">Verify Inspection</button></form>`;
}

function footerRow(title, columns, render) {
  return `<tr class="inspection-result-row"><td class="inspection-col-num inspection-result-label-piece"></td><td class="inspection-result-label-piece"><strong>${title}</strong></td><td class="inspection-result-label-piece"></td><td class="inspection-result-label-piece"></td>${columns.map(({ pipe }) => `<td class="inspection-history-result-cell ${reliefColumnClass(pipe.relief_inspectors)}" data-attempt="${pipe.attempt_id}">${render(pipe)}</td>`).join("")}</tr>`;
}

export function renderPipeReport(data, station = "") {
  const url = (page) => `/workflow/pipe-report?page=${page}${station ? `&station=${encodeURIComponent(station)}` : ""}`;
  const grids = buildPipeReportGrids(data.pipes);
  return `<div id="pipe-report-content" data-page="${data.page}" data-station="${escape(station)}">
    <section class="card"><h2>Ongoing Stations</h2><div class="table-wrap"><table class="pipe-report-station-table"><thead><tr><th>Station</th><th>Inspector</th><th>Branch / Shift</th><th>Current Pipe</th><th>WO</th><th>Last Activity</th></tr></thead><tbody>${data.stations.map((item) => `<tr><td><strong>${escape(item.location_name || "Unassigned station")}</strong></td><td>${escape(item.inspector_name)}${item.break_mode === "with_relief" ? `<small>Relief: ${escape(item.relief_name)} (ADP ${escape(item.relief_adp)})</small>` : item.break_mode === "without_relief" ? "<small>On break / paused</small>" : ""}</td><td>${escape(item.branch)} / ${escape(item.shift)}</td><td>${item.attempt_id ? escape(item.pipe_number) : "Ready / no active pipe"}</td><td>${escape(item.production_number)}</td><td>${time(item.last_activity)}</td></tr>`).join("") || '<tr><td colspan="6">No stations currently signed in.</td></tr>'}</tbody></table></div></section>
    <section class="card"><h2>Pipe Inspections</h2><p>${data.total} pipes · Page ${data.page} of ${data.pages} · 20 pipes per page. Pipes run across the columns, with inspection elements down the rows. Scroll across the grid to review all pipes. Live values are not submitted results.</p>
    ${grids.map(({ key, first, columns, rows }) => `<section class="card nested-card inspection-sheet-card pipe-report-grid-section">
      <div class="inspection-sheet-meta"><div class="inspection-sheet-meta-row"><div><strong>Date:</strong> ${time(first.started_at)}</div><div><strong>Drawing #:</strong> ${escape(first.recipe_drawing || "—")}</div><div><strong>Machine #:</strong> ${escape(first.location_name || "Unassigned station")}</div></div>
      <div class="inspection-sheet-meta-row"><div><strong>Digital IRR:</strong> ${escape(first.recipe_name)}</div><div><strong>Workorder #:</strong> ${escape(first.production_number)}</div><div><strong>Connection Type:</strong> ${escape(first.operation_description)}</div></div></div>
      <div class="table-wrap inspection-sheet-wrap pipe-report-grid-wrap" data-grid-key="${escape(key)}" tabindex="0" role="region" aria-label="Inspection grid for ${escape(first.location_name || 'station')} ${escape(first.production_number)} ${escape(first.recipe_name)}">
      <table class="inspection-sheet-table pipe-history-sheet-table pipe-report-grid"><caption class="pipe-report-grid-caption">${escape(first.location_name)} · ${escape(first.production_number)} · ${escape(first.recipe_name)}</caption>
      <thead><tr><th class="inspection-col-num" scope="col">#</th><th scope="col">Element</th><th scope="col">DWG DIM</th><th scope="col">Gauge</th>${columns.map(({ pipe }) => `<th class="inspection-col-history ${reliefColumnClass(pipe.relief_inspectors)}" scope="col" data-attempt="${pipe.attempt_id}"><strong>Pipe # ${escape(pipe.pipe_number)}</strong><small>${escape(pipe.inspector_name)}${pipe.inspector_adp ? ` (ADP ${escape(pipe.inspector_adp)})` : ""}</small>${reliefColumnLabel(pipe.relief_inspectors)}<small>Attempt ${pipe.attempt_no} · ${escape(pipe.inspection_scope)}</small>${pipe.status === "in_progress" ? '<small class="pipe-report-live-label">Live / not submitted</small>' : ''}</th>`).join("")}</tr></thead>
      <tbody>${rows.map(([rowKey, row]) => `<tr><td class="inspection-col-num">${escape(row.element_sequence)}</td><td>${escape(row.element_description)}</td><td>${escape(row.dwg_dim)}</td><td>${escape(row.gauge)}</td>${columns.map(({ pipe, measurements }) => {
        const measured = measurements.get(rowKey);
        const notDue = !measured && Array.isArray(pipe.planned_sequences) && !pipe.planned_sequences.map(Number).includes(Number(row.element_sequence));
        const state = measured?.pass_fail === "Pass" ? "pass" : measured?.pass_fail === "Fail" ? "fail" : "pending";
        return `<td class="inspection-history-cell pipe-report-value pipe-report-value-${state} ${reliefColumnClass(pipe.relief_inspectors)}" data-attempt="${pipe.attempt_id}" title="${escape(notDue ? 'Not due on this pipe' : measured?.pass_fail || 'Waiting for measurement')}">${measured ? escape(measured.measured_value) : `<span class="inspection-sheet-not-due">${notDue ? 'Not due' : '—'}</span>`}${state === 'fail' ? '<small>Fail</small>' : ''}</td>`;
      }).join("")}</tr>`).join("") || `<tr><td></td><td>Waiting for inspection measurements</td><td></td><td></td>${columns.map(() => '<td>—</td>').join('')}</tr>`}</tbody>
      <tfoot>${footerRow("Inspection Result", columns, (pipe) => `<strong>${escape(resultLabel(pipe.status))}</strong>${pipe.manager_name ? `<small>Approved by ${escape(pipe.manager_name)}</small>` : ''}`)}
      ${footerRow("Verification", columns, verificationCell)}
      ${footerRow("Submitted / Live Updated", columns, (pipe) => time(pipe.status === 'in_progress' ? pipe.live_updated_at : pipe.completed_at))}
      ${footerRow("Operator / Shift", columns, (pipe) => `${escape(pipe.cnc_operator_name)}<small>${escape(pipe.shift)} · ${escape(pipe.branch)}</small>`)}
      ${columns.some(({pipe}) => pipe.notes) ? footerRow("Notes", columns, (pipe) => escape(pipe.notes)) : ''}
      ${footerRow("Full Pipe Report", columns, (pipe) => `<a href="/report/pipe/${pipe.pipe_unit_id}" target="_blank" rel="noopener">Saved report / attempts</a>`)}
      </tfoot></table></div></section>`).join("") || "<p>No pipes found for this station.</p>"}
    <nav class="pipe-report-pagination" aria-label="Pipe report pages">${data.page > 1 ? `<a class="button secondary" href="${url(data.page - 1)}">← Previous 20 pipes</a>` : '<span>← Previous</span>'}<span>Page ${data.page} of ${data.pages}</span>${data.page < data.pages ? `<a class="button" href="${url(data.page + 1)}">Next 20 pipes →</a>` : '<span>Next →</span>'}</nav></section></div>`;
}

export function registerPipeReportRoutes(app, callBridge, pageLayout) {
  const guard = async (req, res, next) => {
    res.set("Cache-Control", "no-store");
    if (!req.session?.inspector) return res.status(401).send("Sign in to view the pipe report.");
    try {
      const employee = await callBridge("get_employee_by_adp", { adp_number: req.session.inspector.adp_number });
      if (!canViewPipeReport(employee)) return res.status(403).send("The live pipe report is available only to Quality admins and IT.");
      req.pipeReportReviewer = employee;
      next();
    } catch (error) { next(error); }
  };
  app.get("/workflow/pipe-report", guard, async (req, res, next) => {
    try {
      const data = await callBridge("get_live_pipe_report", { page: req.query.page || 1, station_id: req.query.station });
      const locations = await callBridge("get_locations");
      const station = String(req.query.station || "");
      const content = `<section class="card"><h1>Live Pipe Report</h1><p>All ongoing stations and the latest attempt for every pipe, including inspections awaiting end-of-shift publication.</p><p>Verification confirms the inspection record was reviewed as correct; it does not change its pass/re-work disposition.</p><form method="get"><label>Station <select name="station"><option value="">All stations</option>${locations.map((item) => `<option value="${item.id}" ${String(item.id) === station ? "selected" : ""}>${escape(item.location_name)}</option>`).join("")}</select></label><button class="button secondary" type="submit">Filter</button></form><p id="pipe-report-freshness" role="status">Live refresh every 5 seconds.</p><p id="pipe-report-error" role="alert"></p></section>${renderPipeReport(data, station)}<script defer src="/public/pipe-report.js?v=2"></script>`;
      res.send(pageLayout(req, content));
    } catch (error) { next(error); }
  });
  app.get("/workflow/pipe-report/live.json", guard, async (req, res, next) => {
    try {
      const data = await callBridge("get_live_pipe_report", { page: req.query.page || 1, station_id: req.query.station });
      res.json({ html: renderPipeReport(data, String(req.query.station || "")), updated_at: new Date().toISOString() });
    } catch (error) { next(error); }
  });
  app.post("/workflow/pipe-report/verify", guard, async (req, res) => {
    try {
      await callBridge("verify_pipe_inspection", { attempt_id: req.body.attempt_id, expected_hash: req.body.snapshot_hash, verifier_adp: req.pipeReportReviewer.adp_number });
      res.json({ verified: true });
    } catch (error) { res.status(409).json({ error: error.message }); }
  });
  app.post("/workflow/live-draft", async (req, res) => {
    res.set("Cache-Control", "no-store");
    const active = req.session?.activeInspection;
    if (!active || String(active.attempt_id) !== String(req.body.attempt_id) || !req.session.sessionRecord || !req.session.inspector) return res.sendStatus(403);
    try {
      const measurements = active.inspection_plan.map((element) => ({ ...element,
        measured_value: String(req.body.values?.[`element_${element.element_sequence}`] ?? (element.capture_type === "boolean" ? "No" : "")).slice(0, 100) }));
      await callBridge("save_live_inspection_draft", { attempt_id: active.attempt_id, session_id: req.session.sessionRecord.id,
        inspector_adp: req.session.inspector.adp_number, measurements, client_sequence: Math.min(Number(req.body.client_sequence) || 0, Number.MAX_SAFE_INTEGER), coverage_version: req.breakState?.version ?? req.body.coverage_version, edited_sequence: req.body.edited_sequence });
      res.sendStatus(204);
    } catch { res.sendStatus(409); }
  });
}
