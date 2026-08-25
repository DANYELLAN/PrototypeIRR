import express from "express";
import session from "express-session";
import path from "path";
import { fileURLToPath } from "url";
import { callCncBridge } from "./cncTimeBridge.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const port = Number(process.env.CNC_TIME_NODE_PORT || 3100);
const syncIntervalMs = Number(process.env.CNC_TIME_SYNC_INTERVAL_MS || 60 * 60 * 1000);

app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(
  session({
    secret: process.env.SESSION_SECRET || "cnc-time-dev",
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

function noticeMarkup(notice) {
  if (!notice) return "";
  return `<div class="cnc-notice ${escapeHtml(notice.kind || "info")}">${escapeHtml(notice.message)}</div>`;
}

function machineBadge(sessionData) {
  if (!sessionData?.machine_no) return "";
  return `<span class="cnc-chip">CNC ${escapeHtml(sessionData.machine_no)}</span>`;
}

function shiftBadge(sessionData) {
  if (!sessionData?.shift_title) return "";
  return `<span class="cnc-chip secondary">${escapeHtml(sessionData.shift_title)}</span>`;
}

function requireAuth(req, res, next) {
  if (!req.session?.cncUser) return res.redirect("/");
  return next();
}

function hasRole(sessionData, role) {
  return Array.isArray(sessionData?.roles) && sessionData.roles.includes(role);
}

function requireExporter(req, res, next) {
  if (!req.session?.cncUser) return res.redirect("/");
  if (!hasRole(req.session.cncUser, "exporter")) {
    req.session.notice = { kind: "warning", message: "You do not have access to exports." };
    return res.redirect("/dashboard");
  }
  return next();
}

function requireOperator(req, res, next) {
  if (!req.session?.cncUser) return res.redirect("/");
  if (!hasRole(req.session.cncUser, "operator")) {
    if (hasRole(req.session.cncUser, "exporter")) return res.redirect("/exports");
    req.session.notice = { kind: "warning", message: "You do not have access to operator time entry." };
    return res.redirect("/");
  }
  return next();
}

function csvEscape(value) {
  const text = String(value ?? "");
  if (/[",\r\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function exportFileName(startDate, endDate) {
  const range = [startDate, endDate].filter(Boolean).join("_to_") || "all";
  return `cnc_time_entries_${range}.csv`;
}

function layout({ title, body, sessionData, notice, active = "" }) {
  const authed = Boolean(sessionData?.employee);
  const now = new Date();
  const navItems = [
    { key: "home", href: "/dashboard", label: "Machine Overview" },
    { key: "time", href: "/time", label: "Time Entry" },
    { key: "checklist", href: "/checklist", label: "Daily Checklist" },
    { key: "maintenance", href: "/maintenance", label: "Maintenance" },
    { key: "it", href: "/contact/it", label: "IT Support" },
  ].filter((item) => {
    if (!authed) return true;
    if (item.key === "home" || item.key === "time" || item.key === "checklist" || item.key === "maintenance" || item.key === "it") {
      return hasRole(sessionData, "operator");
    }
    return true;
  });
  if (authed && hasRole(sessionData, "exporter")) {
    navItems.push({ key: "exports", href: "/exports", label: "Exports" });
  }

  return `<!doctype html>
  <html lang="en">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <meta name="theme-color" content="#8a1f24" />
      <title>${escapeHtml(title)}</title>
      <link rel="manifest" href="/public/cnc-time.webmanifest" />
      <link rel="stylesheet" href="/public/cnc-time.css" />
      <script defer src="/public/cnc-time.js?v=3"></script>
    </head>
    <body>
      <div class="benoit-app-shell">
        <header class="benoit-topbar">
          <div class="brand-block">
            <div class="brand-mark">B</div>
            <div class="brand-copy">
              <span>CNC TIME ENTRY</span>
              <small>BENOIT CONNECT · ENNIS</small>
            </div>
          </div>
          <div class="topbar-status">
            <span class="status-time" id="header_clock">${escapeHtml(now.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }))}</span>
            ${authed ? `<span class="status-meta">CNC ${escapeHtml(sessionData.machine_no || "")}</span>` : ""}
            ${authed ? `<span class="status-pill">${escapeHtml(sessionData.shift_title || "Shift")}</span>` : ""}
            <a class="header-button" href="http://192.168.32.142:8000/ui/liveview/" target="_blank" rel="noopener noreferrer">LiveView</a>
            <a class="header-button" href="http://192.168.32.142:8000/ui/datatool/" target="_blank" rel="noopener noreferrer">DataTool</a>
            ${authed ? `<form method="post" action="/logout" class="logout-form"><button class="header-button danger" type="submit">Log Out</button></form>` : ""}
          </div>
        </header>

        ${authed ? `<nav class="benoit-tabs">
          ${navItems
            .map(
              (item) => `<a class="${active === item.key ? "active" : ""}" href="${item.href}">${item.label}</a>`,
            )
            .join("")}
        </nav>` : ""}

        <main class="benoit-main">
          ${noticeMarkup(notice)}
          ${body}
        </main>
      </div>
    </body>
  </html>`;
}

function optionList(values, selectedValue = "") {
  return (values || [])
    .map((value) => {
      const selected = String(value) === String(selectedValue) ? " selected" : "";
      return `<option value="${escapeHtml(value)}"${selected}>${escapeHtml(value)}</option>`;
    })
    .join("");
}

function detailOptionList(details, selectedValue = "") {
  return (details || [])
    .map((item) => {
      const selected = String(item.title) === String(selectedValue) ? " selected" : "";
      return `<option value="${escapeHtml(item.title)}"${selected}>${escapeHtml(item.title)}</option>`;
    })
    .join("");
}

function operationOptionList(operations, selectedProduction = "", selectedOperation = "") {
  return (operations || [])
    .map((item) => {
      const selected =
        String(item.production_number) === String(selectedProduction) &&
        String(item.operation_id) === String(selectedOperation)
          ? " selected"
          : "";
      return `<option value="${escapeHtml(item.operation_id)}" data-wo="${escapeHtml(item.production_number)}"${selected}>${escapeHtml(
        item.operation_id,
      )} - ${escapeHtml(item.operation_description || item.description)}</option>`;
    })
    .join("");
}

function downtimeReasonOptionList(reasons, selectedValue = "") {
  const blankSelected = !String(selectedValue || "").trim() ? " selected" : "";
  const reasonOptions = (reasons || [])
    .map((item) => {
      const selected = String(item.code) === String(selectedValue) ? " selected" : "";
      return `<option value="${escapeHtml(item.code)}"${selected}>${escapeHtml(item.label)}</option>`;
    })
    .join("");
  return `<option value=""${blankSelected}></option>${reasonOptions}`;
}

function timeTable(rows, editable = false, editContext = {}) {
  if (!rows?.length) return "<p class='cnc-empty'>No recent records yet.</p>";
  return `<div class="cnc-table-wrap"><table class="cnc-table">
    <thead><tr><th>Date</th><th>Status</th><th>WO</th><th>Detail</th><th>Total</th><th>Qty</th>${editable ? "<th>Correction</th>" : ""}</tr></thead>
    <tbody>
      ${rows
        .map(
          (row) => `<tr>
            <td>${escapeHtml(row.labor_date || "")}</td>
            <td>${escapeHtml(row.status || "")}</td>
            <td>${escapeHtml(row.production_number || "")}</td>
            <td>${escapeHtml(row.details_type_ii || row.details_type || "")}</td>
            <td>${escapeHtml(row.total || "")}</td>
            <td>${escapeHtml(row.quantity || "")}</td>
            ${
              editable
                ? `<td>
                    <details class="correction-details">
                      <summary>Edit</summary>
                      <form method="post" action="/time/correct" class="correction-form">
                        <input type="hidden" name="entry_id" value="${escapeHtml(row.sp_id || row.id)}" />
                        <label><span>WO</span><select name="production_number">${optionList(editContext.workOrders, row.production_number)}</select></label>
                        <label class="wide"><span>Operation</span><select name="operation_id" class="operation-select">${operationOptionList(
                          editContext.operations,
                          row.production_number,
                          row.operation_id,
                        )}</select></label>
                        <label><span>Detail</span><select name="detail_type">${detailOptionList(editContext.details, row.details_type === "DT" ? "Downtime" : row.details_type)}</select></label>
                        <label><span>DT Reason</span><select name="downtime_reason">${downtimeReasonOptionList(editContext.downtimeReasons, row.details_type_ii || row.tran_description)}</select></label>
                        <label><span>Qty</span><input name="quantity" type="number" min="0" step="1" value="${escapeHtml(row.quantity || 0)}" /></label>
                        <label><span>Break Min</span><input name="break_minutes" type="number" min="0" step="1" value="${escapeHtml(row.break_minutes || 0)}" /></label>
                        <label><span>Total Hours</span><input name="total_hours" type="number" min="0" max="24" step="1" value="${escapeHtml(Math.floor(Number(row.total_minutes || 0) / 60))}" /></label>
                        <label><span>Total Min</span><input name="total_minutes_remainder" type="number" min="0" max="59" step="1" value="${escapeHtml(Number(row.total_minutes || 0) % 60)}" /></label>
                        <label class="wide"><span>Comments</span><input name="comments" value="${escapeHtml(row.tran_description || "")}" /></label>
                        <button class="cnc-button ghost" type="submit">Save</button>
                      </form>
                    </details>
                    <form method="post" action="/time/delete" class="delete-entry-form" onsubmit="return confirm('Delete this time entry?');">
                      <input type="hidden" name="entry_id" value="${escapeHtml(row.sp_id || row.id)}" />
                      <input type="hidden" name="entry_list" value="${escapeHtml(row.entry_list || "timeentry")}" />
                      <button class="cnc-button danger small" type="submit">Delete</button>
                    </form>
                  </td>`
                : ""
            }
          </tr>`,
        )
        .join("")}
    </tbody>
  </table></div>`;
}

async function dashboardContext(req) {
  const sessionData = req.session.cncUser;
  return callCncBridge("get_dashboard_context", {
    emp_id: sessionData.employee.emp_id,
    user_email: sessionData.user_email,
  });
}

async function runBackgroundSync(reason = "scheduled") {
  try {
    const result = await callCncBridge("sync_background_jobs");
    console.log(`CNC Time Entry ${reason} sync complete`, JSON.stringify(result));
  } catch (error) {
    console.warn(`CNC Time Entry ${reason} sync failed: ${error.message}`);
  }
}

app.get("/", async (req, res, next) => {
  try {
    if (req.session?.cncUser) return res.redirect("/dashboard");
    const signInContext = await callCncBridge("get_sign_in_context");
    const shiftOptions = signInContext.shift_options
      .map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.title)}</option>`)
      .join("");
    const machineOptions = (signInContext.machine_options || [])
      .map(
        (machine) => `<option value="${escapeHtml(machine.machine_no)}">${escapeHtml(`Machine ${machine.machine_no}`)}</option>`,
      )
      .join("");
    res.send(
      layout({
        title: "CNC Time Entry Sign In",
        notice: req.session.notice,
        body: `<section class="login-workspace">
          <div class="login-heading">
            <h2>Ennis Time Entry : Sign In</h2>
            <p>Enter the employee ADP badge number to sign in and verify the employee record.</p>
          </div>
          <div class="login-layout single">
            <section class="cnc-card cnc-login-card">
              <div class="cnc-section-header"><h3>Badge</h3></div>
              <form method="post" action="/login" class="cnc-form">
                <label><span>Badge / ADP Number</span><input id="adp_number_input" name="adp_number" inputmode="numeric" autocomplete="off" required autofocus /></label>
                <div class="employee-match" id="employee_match" aria-live="polite">Employee match will appear here.</div>
                <label><span>Machine Number</span><select name="machine_no" required>${machineOptions || "<option value=''>No machines available</option>"}</select></label>
                <label><span>Shift</span><select name="shift_id">${shiftOptions}</select></label>
                <div class="login-actions">
                  <button class="cnc-button" type="submit">Sign In</button>
                  <button class="cnc-button install ghost" id="install-app-button" type="button" hidden>Install App</button>
                </div>
              </form>
            </section>
          </div>
        </section>`,
      }),
    );
    req.session.notice = null;
  } catch (error) {
    next(error);
  }
});

app.post("/login", async (req, res) => {
  try {
    req.session.cncUser = await callCncBridge("sign_in", {
      adp_number: req.body.adp_number,
      user_email: req.body.user_email,
      machine_no: req.body.machine_no,
      shift_id: req.body.shift_id,
    });
    req.session.notice = { kind: "success", message: "Signed in successfully." };
    if (hasRole(req.session.cncUser, "exporter") && !hasRole(req.session.cncUser, "operator")) {
      return res.redirect("/exports");
    }
    res.redirect("/dashboard");
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
    res.redirect("/");
  }
});

app.get("/lookup-employee", async (req, res) => {
  try {
    const adpNumber = String(req.query.adp_number || "").trim();
    if (!adpNumber) {
      return res.json({ employee: null });
    }

    const employeeList = await callCncBridge("get_employee_lookup", { adp_number: adpNumber });
    return res.json({ employee: employeeList || null });
  } catch (error) {
    return res.json({ employee: null, error: error.message });
  }
});

app.post("/logout", (req, res) => {
  req.session.destroy(() => {
    res.redirect("/");
  });
});

app.get("/exports", requireExporter, (req, res) => {
  const sessionData = req.session.cncUser;
  const startDate = escapeHtml(req.query.start_date || "");
  const endDate = escapeHtml(req.query.end_date || "");
  res.send(
    layout({
      title: "CNC Time Entry Exports",
      sessionData,
      notice: req.session.notice,
      active: "exports",
      body: `<section class="cnc-hero compact">
        <div>
          <p class="eyebrow">Exports</p>
          <h2>Download submitted CNC time entries for shipping and receiving.</h2>
        </div>
      </section>
      <section class="cnc-card">
        <div class="cnc-section-header"><h3>Time Entry Export</h3></div>
        <form method="get" action="/exports.csv" class="cnc-form direct-start-form">
          <label><span>Start Date</span><input name="start_date" type="date" value="${startDate}" /></label>
          <label><span>End Date</span><input name="end_date" type="date" value="${endDate}" /></label>
          <button class="cnc-button" type="submit">Download CSV</button>
        </form>
      </section>`,
    }),
  );
  req.session.notice = null;
});

app.get("/exports.csv", requireExporter, async (req, res) => {
  try {
    const startDate = String(req.query.start_date || "").trim();
    const endDate = String(req.query.end_date || "").trim();
    const rows = await callCncBridge("get_time_export_rows", { start_date: startDate, end_date: endDate });
    const columns = [
      ["labor_date", "Labor Date"],
      ["status", "Status"],
      ["production_number", "WO"],
      ["operation_id", "Operation"],
      ["operation_description", "Operation Description"],
      ["detail", "Detail"],
      ["dt_reason", "DT Reason"],
      ["comments", "Comments"],
      ["employee_id", "Employee ID"],
      ["operator", "Operator"],
      ["machine_no", "Machine"],
      ["shift", "Shift"],
      ["start", "Start"],
      ["end", "End"],
      ["break_minutes", "Break Minutes"],
      ["total", "Total"],
      ["total_minutes", "Total Minutes"],
      ["quantity", "Quantity"],
      ["average", "Average"],
    ];
    const csv = [
      columns.map(([, label]) => csvEscape(label)).join(","),
      ...(rows || []).map((row) => columns.map(([key]) => csvEscape(row[key])).join(",")),
    ].join("\r\n");
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${exportFileName(startDate, endDate)}"`);
    res.send(csv);
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
    res.redirect("/exports");
  }
});

app.get("/dashboard", requireOperator, async (req, res, next) => {
  try {
    const context = await dashboardContext(req);
    const sessionData = req.session.cncUser;
    const cards = [
      { href: "/time", title: "Time Sheet Entry", copy: "Start work, pause lunch, submit active jobs, and add misc or manual time." },
      { href: "/checklist", title: "CNC Daily Checklist", copy: "Run the pre-use checklist and save it to the maintenance site." },
      { href: "/maintenance", title: "Contact Maintenance", copy: "Open a maintenance request tied to a location and asset." },
      { href: "/contact/it", title: "Contact IT", copy: "Submit workstation support requests." },
      { href: "/contact/supervisor", title: "Contact Supervisor", copy: "Send an urgent note with your machine number." },
    ];

    res.send(
      layout({
        title: "CNC Time Entry Dashboard",
        sessionData,
        notice: req.session.notice,
        active: "home",
        body: `<section class="overview-panel">
          <div class="overview-header">
            <div>
              <p class="eyebrow">Overview</p>
              <h2>Shift Overview</h2>
            </div>
            <div class="overview-meta">Shift Up - Updated ${new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}</div>
          </div>

          <div class="metrics-row">
            <div class="metric-box">
              <div class="metric-value">6/6</div>
              <div class="metric-label">Machines Online</div>
            </div>
            <div class="metric-box">
              <div class="metric-value">197</div>
              <div class="metric-label">Shift Parts</div>
            </div>
            <div class="metric-box">
              <div class="metric-value">72</div>
              <div class="metric-label">Last Hour Parts</div>
            </div>
            <div class="metric-box">
              <div class="metric-value">day</div>
              <div class="metric-label">Current Shift</div>
            </div>
          </div>

          <div class="machine-table-wrap">
            <table class="machine-table">
              <thead>
                <tr>
                  <th>Machine</th>
                  <th>Time</th>
                  <th>Program</th>
                  <th>Status</th>
                  <th>Part Count</th>
                  <th>Shift Parts</th>
                  <th>Shift PPH</th>
                  <th>Hourly PPH</th>
                </tr>
              </thead>
              <tbody>
                ${
                  context.recent_entries && context.recent_entries.length
                    ? context.recent_entries
                        .slice(0, 6)
                        .map(
                          (entry) => `<tr>
                            <td>${escapeHtml(entry.machine_no || "CNC 1")}</td>
                            <td>${escapeHtml(entry.labor_date || "--")}</td>
                            <td>${escapeHtml(entry.production_number || entry.operation_description || "-")}</td>
                            <td><span class="status-badge ${escapeHtml((entry.status || "ready").toLowerCase())}">${escapeHtml(entry.status || "Ready")}</span></td>
                            <td>${escapeHtml(entry.quantity || "0")}</td>
                            <td>${escapeHtml(entry.quantity || "0")}</td>
                            <td>${escapeHtml(entry.total || "0.00")}</td>
                            <td>${escapeHtml(entry.total || "0.00")}</td>
                          </tr>`,
                        )
                        .join("")
                    : `<tr><td colspan="8" class="empty-row">No active entries</td></tr>`
                }
              </tbody>
            </table>
          </div>
        </section>

        <section class="utility-grid">
          ${cards
            .map(
              (card) => `<a class="data-card" href="${card.href}">
                <h3>${escapeHtml(card.title)}</h3>
                <p>${escapeHtml(card.copy)}</p>
              </a>`,
            )
            .join("")}
        </section>

        <section class="panel-block">
          <div class="panel-title-row">
            <h3>Current Status</h3>
          </div>
          ${
            context.active_entry
              ? `<div class="status-card">
                  <strong>${escapeHtml(context.active_entry.production_number || "In Progress")}</strong>
                  <p>${escapeHtml(context.active_entry.operation_description || context.active_entry.description || "")}</p>
                  <div class="status-swatch-row"><span class="status-badge ${escapeHtml((context.active_entry.status || "ready").toLowerCase())}">${escapeHtml(context.active_entry.status || "Ready")}</span><span class="status-tab secondary">${escapeHtml(context.active_entry.details_type || "Manual")}</span></div>
                </div>`
              : "<p class='cnc-empty'>No active entry is running right now.</p>"
          }
        </section>

        <section class="panel-block">
          <div class="panel-title-row">
            <h3>Recent Time Records</h3>
          </div>
          ${timeTable(context.recent_entries)}
        </section>`,
      }),
    );
    req.session.notice = null;
  } catch (error) {
    next(error);
  }
});

app.get("/time", requireOperator, async (req, res, next) => {
  try {
    const context = await dashboardContext(req);
    const sessionData = req.session.cncUser;
    const workOrderOptions = context.work_orders
      .map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`)
      .join("");
    const operationOptions = context.operations
      .map(
        (item) =>
          `<option value="${escapeHtml(item.operation_id)}" data-wo="${escapeHtml(item.production_number)}">${escapeHtml(
            item.operation_id,
          )} - ${escapeHtml(item.operation_description || item.description)}</option>`,
      )
      .join("");
    const detailOptions = context.details_step_two
      .map((item) => `<option value="${escapeHtml(item.title)}">${escapeHtml(item.title)}</option>`)
      .join("");
    const downtimeReasonOptions = (context.downtime_reasons || [])
      .map((item) => `<option value="${escapeHtml(item.code)}">${escapeHtml(item.label)}</option>`)
      .join("");

    res.send(
      layout({
        title: "CNC Time Entry",
        sessionData,
        notice: req.session.notice,
        active: "time",
        body: `<section class="cnc-hero compact">
          <div>
            <p class="eyebrow">Time Entry</p>
            <h2>Start jobs, stop jobs, add downtime, and key in manual production time.</h2>
          </div>
        </section>
        <section class="cnc-grid single">
          <article class="cnc-card${context.active_entry ? "" : " is-hidden"}">
            <div class="cnc-section-header"><h3>Active Entry</h3></div>
            ${
              context.active_entry
                ? `<div class="cnc-status-card">
                    <strong>${escapeHtml(context.active_entry.production_number || "")}</strong>
                    <p>${escapeHtml(context.active_entry.operation_description || "")}</p>
                    <div class="cnc-chip-row"><span class="cnc-chip">${escapeHtml(context.active_entry.status)}</span><span class="cnc-chip secondary">${escapeHtml(context.active_entry.details_type || "")}</span></div>
                    ${
                      context.active_entry.status === "Paused" && context.active_break
                        ? `<div class="break-note">
                            <strong>${escapeHtml(context.active_break.break_type || "Break")}</strong>
                            ${context.active_break.comment ? `<span>${escapeHtml(context.active_break.comment)}</span>` : ""}
                          </div>`
                        : ""
                    }
                    <div class="cnc-inline-actions active-action-row">
                      <details class="cnc-active-downtime">
                        <summary>Edit Time</summary>
                        <form method="post" action="/time/active/edit" class="cnc-form two-col">
                          <input type="hidden" name="entry_id" value="${escapeHtml(context.active_entry.sp_id)}" />
                          <label><span>WO</span><select name="production_number">${optionList(context.work_orders, context.active_entry.production_number)}</select></label>
                          <label><span>Operation</span><select name="operation_id" class="operation-select">${operationOptionList(
                            context.operations,
                            context.active_entry.production_number,
                            context.active_entry.operation_id,
                          )}</select></label>
                          <label><span>Detail</span><select name="detail_type">${detailOptionList(
                            context.details_step_two,
                            context.active_entry.details_type === "DT" ? "Downtime" : context.active_entry.details_type,
                          )}</select></label>
                          <label><span>Break Minutes</span><input name="break_minutes" type="number" min="0" step="1" value="${escapeHtml(context.active_entry.break_minutes || 0)}" /></label>
                          <label class="full"><span>Correction Note</span><input name="comments" placeholder="Forgot lunch, wrong quantity, etc." /></label>
                          <button class="cnc-button ghost" type="submit">Save Correction</button>
                        </form>
                      </details>
                      <details class="cnc-active-downtime">
                        <summary>Log Downtime</summary>
                        <form method="post" action="/time/misc" class="cnc-form two-col">
                          <input type="hidden" name="production_number" value="${escapeHtml(context.active_entry.production_number || "")}" />
                          <input type="hidden" name="operation_id" value="${escapeHtml(context.active_entry.operation_id || "")}" />
                          <label class="full"><span>Downtime Reason</span><select name="detail_type_ii" required>${downtimeReasonOptions}</select></label>
                          <label><span>Hours</span><input name="hours" type="number" min="0" max="16" value="0" required /></label>
                          <label><span>Minutes</span><input name="minutes" type="number" min="0" max="55" step="5" value="5" required /></label>
                          <label class="full"><span>Comments</span><textarea name="comments" rows="2"></textarea></label>
                          <button class="cnc-button ghost" type="submit">Submit Downtime</button>
                        </form>
                      </details>
                      ${
                        context.active_entry.status === "In Progress"
                          ? `<details class="cnc-active-downtime">
                              <summary>Take Break</summary>
                              <form method="post" action="/time/pause" class="cnc-form two-col">
                                <input type="hidden" name="entry_id" value="${escapeHtml(context.active_entry.sp_id)}" />
                                <label class="full"><span>Break Type</span><select name="break_type" required>
                                  <option value="No Relief">Break With No Relief</option>
                                  <option value="With Relief">Break With Relief</option>
                                </select></label>
                                <label class="full"><span>Comments</span><textarea name="comments" rows="2" placeholder="Relief operator name or note"></textarea></label>
                                <button class="cnc-button ghost" type="submit">Pause Time</button>
                              </form>
                            </details>`
                          : ""
                      }
                      ${
                        context.active_entry.status === "In Progress"
                          ? `<form method="post" action="/time/pause" class="cnc-active-downtime lunch-action-tile">
                              <input type="hidden" name="entry_id" value="${escapeHtml(context.active_entry.sp_id)}" />
                              <input type="hidden" name="break_type" value="Lunch" />
                              <button class="cnc-button ghost" type="submit">Start Lunch</button>
                            </form>`
                          : ""
                      }
                      ${
                        context.active_entry.status === "Paused"
                          ? `<form method="post" action="/time/resume" class="cnc-active-downtime lunch-action-tile"><input type="hidden" name="entry_id" value="${escapeHtml(context.active_entry.sp_id)}" /><button class="cnc-button ghost" type="submit">Resume Production</button></form>`
                          : ""
                      }
                    </div>
                    <form method="post" action="/time/stop" class="cnc-inline-stop-form stop-submit-row">
                      <input type="hidden" name="entry_id" value="${escapeHtml(context.active_entry.sp_id)}" />
                      <label><span>Quantity</span><input name="quantity" type="number" min="0" step="1" value="${escapeHtml(context.active_entry.quantity || 1)}" required /></label>
                      <button class="cnc-button danger" type="submit">Stop And Submit</button>
                    </form>
                  </div>`
                : "<p class='cnc-empty'>No active entry. Use the start form to begin a direct labor record.</p>"
            }
          </article>
          <article class="cnc-card${context.active_entry ? " is-hidden" : ""}">
            <div class="cnc-section-header"><h3>Start Direct Time</h3></div>
            <form method="post" action="/time/start" class="cnc-form direct-start-form">
              <label><span>Production Order</span><select name="production_number" required>${workOrderOptions}</select></label>
              <label><span>Operation</span><select name="operation_id" required class="operation-select">${operationOptions}</select></label>
              <label><span>Detail Type</span><select name="detail_type" required>${detailOptions}</select></label>
              <button class="cnc-button" type="submit">Start Entry</button>
            </form>
          </article>
        </section>
        <section class="cnc-grid two">
          <article class="cnc-card">
            <div class="cnc-section-header"><h3>Misc Time</h3></div>
            <form method="post" action="/time/misc" class="cnc-form two-col">
              <label><span>Production Order</span><select name="production_number" required>${workOrderOptions}</select></label>
              <label><span>Operation</span><select name="operation_id" required class="operation-select">${operationOptions}</select></label>
              <label class="full"><span>Downtime Reason</span><select name="detail_type_ii" required>${downtimeReasonOptions}</select></label>
              <label><span>Hours</span><input name="hours" type="number" min="0" max="16" value="0" required /></label>
              <label><span>Minutes</span><input name="minutes" type="number" min="0" max="55" step="5" value="0" required /></label>
              <label class="full"><span>Comments</span><textarea name="comments" rows="3"></textarea></label>
              <button class="cnc-button" type="submit">Submit Misc Time</button>
            </form>
          </article>
          <article class="cnc-card">
            <div class="cnc-section-header"><h3>Manual Time</h3></div>
            <form method="post" action="/time/manual" class="cnc-form two-col">
              <label><span>Production Order</span><select name="production_number" required>${workOrderOptions}</select></label>
              <label><span>Operation</span><select name="operation_id" required class="operation-select">${operationOptions}</select></label>
              <label><span>Detail Type</span><select name="detail_type" required>${detailOptions}</select></label>
              <label><span>Quantity</span><input name="quantity" type="number" min="0" step="1" value="1" required /></label>
              <label><span>Hours</span><input name="hours" type="number" min="0" max="16" value="0" required /></label>
              <label><span>Minutes</span><input name="minutes" type="number" min="0" max="55" step="5" value="0" required /></label>
              <label class="full"><span>Comments</span><textarea name="comments" rows="3"></textarea></label>
              <button class="cnc-button" type="submit">Submit Manual Time</button>
            </form>
          </article>
        </section>
        <section class="cnc-card">
          <div class="cnc-section-header"><h3>Recent Entries</h3></div>
          ${timeTable(context.recent_entries, true, {
            workOrders: context.work_orders,
            operations: context.operations,
            details: context.details_step_two,
            downtimeReasons: context.downtime_reasons,
          })}
        </section>`,
      }),
    );
    req.session.notice = null;
  } catch (error) {
    next(error);
  }
});

app.post("/time/start", requireOperator, async (req, res) => {
  try {
    await callCncBridge("start_time_entry", {
      employee: req.session.cncUser.employee,
      shift_id: req.session.cncUser.shift_id,
      machine_no: req.session.cncUser.machine_no,
      production_number: req.body.production_number,
      operation_id: req.body.operation_id,
      detail_type: req.body.detail_type,
    });
    req.session.notice = { kind: "success", message: "Direct time entry started." };
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
  }
  res.redirect("/time");
});

app.post("/time/pause", requireOperator, async (req, res) => {
  try {
    await callCncBridge("pause_for_lunch", {
      entry_id: req.body.entry_id,
      break_type: req.body.break_type,
      comments: req.body.comments,
    });
    const label = req.body.break_type === "Lunch" ? "Lunch started." : "Break started.";
    req.session.notice = { kind: "info", message: `${label} Production time is paused.` };
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
  }
  res.redirect("/time");
});

app.post("/time/resume", requireOperator, async (req, res) => {
  try {
    await callCncBridge("resume_from_lunch", { entry_id: req.body.entry_id });
    req.session.notice = { kind: "success", message: "Break ended. Production time resumed." };
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
  }
  res.redirect("/time");
});

app.post("/time/stop", requireOperator, async (req, res) => {
  try {
    await callCncBridge("stop_time_entry", {
      entry_id: req.body.entry_id,
      quantity: req.body.quantity,
    });
    req.session.notice = { kind: "success", message: "Active time entry submitted." };
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
  }
  res.redirect("/time");
});

app.post("/time/active/edit", requireOperator, async (req, res) => {
  try {
    await callCncBridge("edit_active_time_entry", {
      entry_id: req.body.entry_id,
      production_number: req.body.production_number,
      operation_id: req.body.operation_id,
      detail_type: req.body.detail_type,
      break_minutes: req.body.break_minutes,
      comments: req.body.comments,
    });
    req.session.notice = { kind: "success", message: "Active entry correction saved." };
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
  }
  res.redirect("/time");
});

app.post("/time/correct", requireOperator, async (req, res) => {
  try {
    await callCncBridge("correct_time_entry", {
      entry_id: req.body.entry_id,
      production_number: req.body.production_number,
      operation_id: req.body.operation_id,
      detail_type: req.body.detail_type,
      downtime_reason: req.body.downtime_reason,
      quantity: req.body.quantity,
      break_minutes: req.body.break_minutes,
      total_hours: req.body.total_hours,
      total_minutes_remainder: req.body.total_minutes_remainder,
      comments: req.body.comments,
    });
    req.session.notice = { kind: "success", message: "Time entry correction saved." };
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
  }
  res.redirect("/time");
});

app.post("/time/delete", requireOperator, async (req, res) => {
  try {
    await callCncBridge("delete_time_entry", {
      entry_id: req.body.entry_id,
      entry_list: req.body.entry_list,
    });
    req.session.notice = { kind: "success", message: "Time entry deleted." };
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
  }
  res.redirect("/time");
});

app.post("/time/misc", requireOperator, async (req, res) => {
  try {
    await callCncBridge("submit_misc_time", {
      employee: req.session.cncUser.employee,
      shift_id: req.session.cncUser.shift_id,
      machine_no: req.session.cncUser.machine_no,
      detail_type_ii: req.body.detail_type_ii,
      production_number: req.body.production_number,
      operation_id: req.body.operation_id,
      hours: req.body.hours,
      minutes: req.body.minutes,
      comments: req.body.comments,
    });
    req.session.notice = { kind: "success", message: "Misc time submitted." };
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
  }
  res.redirect("/time");
});

app.post("/time/manual", requireOperator, async (req, res) => {
  try {
    await callCncBridge("submit_manual_time", {
      employee: req.session.cncUser.employee,
      shift_id: req.session.cncUser.shift_id,
      machine_no: req.session.cncUser.machine_no,
      production_number: req.body.production_number,
      operation_id: req.body.operation_id,
      detail_type: req.body.detail_type,
      quantity: req.body.quantity,
      hours: req.body.hours,
      minutes: req.body.minutes,
      comments: req.body.comments,
    });
    req.session.notice = { kind: "success", message: "Manual time submitted." };
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
  }
  res.redirect("/time");
});

app.get("/checklist", requireOperator, async (req, res, next) => {
  try {
    res.send(
      layout({
        title: "Daily Checklist",
        sessionData: req.session.cncUser,
        notice: req.session.notice,
        active: "checklist",
        body: `<section class="cnc-hero compact"><div><p class="eyebrow">Checklist</p><h2>CNC pre-use and daily maintenance checklist</h2></div></section>
        <section class="cnc-card">
          <form method="post" action="/checklist" class="cnc-form">
            <div class="cnc-check-grid">
              <label><input type="checkbox" name="lub_unit" value="1" /> Lubricating Unit - Look/Clean</label>
              <label><input type="checkbox" name="oil_air_lub" value="1" /> Oil Air Lubrication</label>
              <label><input type="checkbox" name="machine_chamber" value="1" /> Machine Chamber - Clean</label>
              <label><input type="checkbox" name="chuck" value="1" /> Chuck - Clean/Warmup/Grease</label>
              <label><input type="checkbox" name="obs_window" value="1" /> Observation Window</label>
              <label><input type="checkbox" name="pneu_device" value="1" /> Pneumatic Device - Look/Check</label>
              <label><input type="checkbox" name="chip_conveyor" value="1" /> Chip Conveyor</label>
              <label><input type="checkbox" name="coolant_unit" value="1" /> Coolant Unit - Look/Clean</label>
              <label><input type="checkbox" name="oil_chiller" value="1" /> Oil Chiller - Look/Clean</label>
              <label><input type="checkbox" name="hydraulic_unit" value="1" /> Hydraulic Unit - Look/Clean</label>
              <label><input type="checkbox" name="oil_skimmer" value="1" /> Oil Skimmer</label>
            </div>
            <label><span>Operator Initials</span><input name="initials" maxlength="10" required /></label>
            <label><span>Notes</span><textarea name="notes" rows="4" placeholder="Damage, leaks, or repairs"></textarea></label>
            <button class="cnc-button" type="submit">Submit Checklist</button>
          </form>
        </section>`,
      }),
    );
    req.session.notice = null;
  } catch (error) {
    next(error);
  }
});

app.post("/checklist", requireOperator, async (req, res) => {
  try {
    await callCncBridge("submit_daily_checklist", {
      employee: req.session.cncUser.employee,
      shift_id: req.session.cncUser.shift_id,
      machine_no: req.session.cncUser.machine_no,
      initials: req.body.initials,
      notes: req.body.notes,
      checks: {
        lub_unit: Boolean(req.body.lub_unit),
        oil_air_lub: Boolean(req.body.oil_air_lub),
        machine_chamber: Boolean(req.body.machine_chamber),
        chuck: Boolean(req.body.chuck),
        obs_window: Boolean(req.body.obs_window),
        pneu_device: Boolean(req.body.pneu_device),
        chip_conveyor: Boolean(req.body.chip_conveyor),
        coolant_unit: Boolean(req.body.coolant_unit),
        oil_chiller: Boolean(req.body.oil_chiller),
        hydraulic_unit: Boolean(req.body.hydraulic_unit),
        oil_skimmer: Boolean(req.body.oil_skimmer),
      },
    });
    req.session.notice = { kind: "success", message: "Checklist submitted." };
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
  }
  res.redirect("/checklist");
});

app.get("/maintenance", requireOperator, async (req, res, next) => {
  try {
    const context = await dashboardContext(req);
    const locationOptions = context.maintenance_locations
      .map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.title)}</option>`)
      .join("");
    const assetOptions = context.maintenance_assets
      .map(
        (item) =>
          `<option value="${escapeHtml(item.asset_id || item.id)}" data-location="${escapeHtml(item.location_id)}">${escapeHtml(
            item.title,
          )}</option>`,
      )
      .join("");
    res.send(
      layout({
        title: "Maintenance Request",
        sessionData: req.session.cncUser,
        notice: req.session.notice,
        active: "maintenance",
        body: `<section class="cnc-hero compact"><div><p class="eyebrow">Maintenance</p><h2>Open a maintenance request for the Ennis CNC area.</h2></div></section>
        <section class="cnc-card">
          <form method="post" action="/maintenance" class="cnc-form two-col">
            <label class="full"><span>Needed</span><input name="title" placeholder="What needs to be done?" required /></label>
            <label class="full"><span>Description</span><textarea name="description" rows="5" required></textarea></label>
            <label><span>Priority</span><select name="priority"><option>Low</option><option>Medium</option><option>High</option></select></label>
            <label><span>Requester Badge</span><input name="requester_id" value="${escapeHtml(req.session.cncUser.employee.emp_id)}" required /></label>
            <label class="full"><span>Requester Name</span><input name="requester_name" value="${escapeHtml(req.session.cncUser.employee.full_name)}" required /></label>
            <label><span>Location</span><select name="location_id" required class="location-select">${locationOptions}</select></label>
            <label><span>Asset</span><select name="asset_id" required class="asset-select">${assetOptions}</select></label>
            <button class="cnc-button" type="submit">Submit Maintenance Request</button>
          </form>
        </section>`,
      }),
    );
    req.session.notice = null;
  } catch (error) {
    next(error);
  }
});

app.post("/maintenance", requireOperator, async (req, res) => {
  try {
    await callCncBridge("submit_maintenance_request", {
      employee: req.session.cncUser.employee,
      requester_id: req.body.requester_id,
      requester_name: req.body.requester_name,
      title: req.body.title,
      description: req.body.description,
      priority: req.body.priority,
      location_id: req.body.location_id,
      asset_id: req.body.asset_id,
      user_email: req.session.cncUser.user_email,
    });
    req.session.notice = { kind: "success", message: "Maintenance request submitted." };
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
  }
  res.redirect("/maintenance");
});

app.get("/contact/it", requireOperator, async (req, res, next) => {
  try {
    const context = await dashboardContext(req);
    const categoryOptions = context.tech_categories
      .map((item) => `<option value="${escapeHtml(item.title)}">${escapeHtml(item.title)}</option>`)
      .join("");
    res.send(
      layout({
        title: "Contact IT",
        sessionData: req.session.cncUser,
        notice: req.session.notice,
        active: "it",
        body: `<section class="cnc-hero compact"><div><p class="eyebrow">IT Support</p><h2>Submit workstation and technical support requests.</h2></div></section>
        <section class="cnc-card">
          <form method="post" action="/contact/it" class="cnc-form">
            <label><span>Technical Category</span><select name="category" required>${categoryOptions}</select></label>
            <label><span>User Email</span><input name="user_email" type="email" value="${escapeHtml(req.session.cncUser.user_email)}" required /></label>
            <label><span>Issue</span><textarea name="issue" rows="6" placeholder="Describe the problem here." required></textarea></label>
            <button class="cnc-button" type="submit">Send Request</button>
          </form>
        </section>`,
      }),
    );
    req.session.notice = null;
  } catch (error) {
    next(error);
  }
});

app.post("/contact/it", requireOperator, async (req, res) => {
  try {
    const result = await callCncBridge("send_it_request", {
      user_email: req.body.user_email,
      user_name: req.session.cncUser.employee.full_name,
      machine_no: req.session.cncUser.machine_no,
      category: req.body.category,
      issue: req.body.issue,
    });
    req.session.notice = {
      kind: "success",
      message: result.queued ? "IT request queued locally. Add `CNC_TIME_IT_WEBHOOK_URL` to deliver automatically." : "IT request delivered.",
    };
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
  }
  res.redirect("/contact/it");
});

app.get("/contact/supervisor", requireOperator, async (req, res, next) => {
  try {
    res.send(
      layout({
        title: "Contact Supervisor",
        sessionData: req.session.cncUser,
        notice: req.session.notice,
        active: "supervisor",
        body: `<section class="cnc-hero compact"><div><p class="eyebrow">Supervisor</p><h2>Send a quick supervisor message with your machine number.</h2></div></section>
        <section class="cnc-card">
          <form method="post" action="/contact/supervisor" class="cnc-form">
            <label><span>Message</span><textarea name="comment" rows="6" placeholder="What do you need help with?" required></textarea></label>
            <button class="cnc-button" type="submit">Send Message</button>
          </form>
        </section>`,
      }),
    );
    req.session.notice = null;
  } catch (error) {
    next(error);
  }
});

app.post("/contact/supervisor", requireOperator, async (req, res) => {
  try {
    const result = await callCncBridge("send_supervisor_message", {
      user_name: req.session.cncUser.employee.full_name,
      machine_no: req.session.cncUser.machine_no,
      comment: req.body.comment,
    });
    req.session.notice = {
      kind: "success",
      message: result.queued
        ? "Supervisor message queued locally. Add `CNC_TIME_SUPERVISOR_WEBHOOK_URL` to deliver automatically."
        : "Supervisor message delivered.",
    };
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
  }
  res.redirect("/contact/supervisor");
});

app.use((error, req, res, _next) => {
  res.status(500).send(
    layout({
      title: "CNC Time Entry Error",
      sessionData: req.session?.cncUser,
      notice: { kind: "warning", message: error.message || "Unexpected error." },
      body: "<section class='cnc-card'><p>Something went wrong while rendering the CNC Time Entry app.</p></section>",
    }),
  );
});

app.listen(port, () => {
  console.log(`CNC Time Entry app listening on http://localhost:${port}`);
  runBackgroundSync("startup");
  setInterval(() => runBackgroundSync("scheduled"), syncIntervalMs).unref();
});
