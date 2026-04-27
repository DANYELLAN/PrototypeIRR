import express from "express";
import session from "express-session";
import path from "path";
import { fileURLToPath } from "url";
import { callCncBridge } from "./cncTimeBridge.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const port = Number(process.env.CNC_TIME_NODE_PORT || 3100);

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

function layout({ title, body, sessionData, notice, active = "" }) {
  const authed = Boolean(sessionData?.employee);
  return `<!doctype html>
  <html lang="en">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <meta name="theme-color" content="#8d1f24" />
      <title>${escapeHtml(title)}</title>
      <link rel="manifest" href="/public/cnc-time.webmanifest" />
      <link rel="stylesheet" href="/public/cnc-time.css" />
      <script defer src="/public/cnc-time.js"></script>
    </head>
    <body>
      <div class="cnc-shell">
        <aside class="cnc-sidebar">
          <div class="cnc-brand">
            <img src="/public/BenoitLogoRegistered-Red.png" alt="Benoit" />
            <div>
              <p class="eyebrow">Installable Web App</p>
              <h1>CNC Time Entry</h1>
            </div>
          </div>
          ${
            authed
              ? `<div class="cnc-user-card">
                  <p class="eyebrow">Signed In</p>
                  <strong>${escapeHtml(sessionData.employee.full_name)}</strong>
                  <p>${escapeHtml(sessionData.employee.emp_id)}</p>
                  <div class="cnc-chip-row">${machineBadge(sessionData)}${shiftBadge(sessionData)}</div>
                </div>
                <nav class="cnc-nav">
                  <a class="${active === "home" ? "active" : ""}" href="/dashboard">Home</a>
                  <a class="${active === "time" ? "active" : ""}" href="/time">Time Entry</a>
                  <a class="${active === "checklist" ? "active" : ""}" href="/checklist">Daily Checklist</a>
                  <a class="${active === "maintenance" ? "active" : ""}" href="/maintenance">Maintenance</a>
                  <a class="${active === "it" ? "active" : ""}" href="/contact/it">Contact IT</a>
                  <a class="${active === "supervisor" ? "active" : ""}" href="/contact/supervisor">Supervisor</a>
                </nav>
                <form method="post" action="/logout"><button class="cnc-button ghost" type="submit">Sign Out</button></form>`
              : `<p class="cnc-sidebar-copy">This version recreates the PowerApp as a browser-based app that can also be installed to a workstation as a PWA.</p>`
          }
        </aside>
        <main class="cnc-main">
          ${noticeMarkup(notice)}
          ${body}
        </main>
      </div>
    </body>
  </html>`;
}

function timeTable(rows) {
  if (!rows?.length) return "<p class='cnc-empty'>No recent records yet.</p>";
  return `<div class="cnc-table-wrap"><table class="cnc-table">
    <thead><tr><th>Date</th><th>Status</th><th>WO</th><th>Detail</th><th>Total</th><th>Qty</th></tr></thead>
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

app.get("/", async (req, res, next) => {
  try {
    if (req.session?.cncUser) return res.redirect("/dashboard");
    const signInContext = await callCncBridge("get_sign_in_context");
    const shiftOptions = signInContext.shift_options
      .map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.title)}</option>`)
      .join("");
    const emailHints = signInContext.station_emails
      .slice(0, 6)
      .map((value) => `<span class="cnc-chip secondary">${escapeHtml(value)}</span>`)
      .join("");

    res.send(
      layout({
        title: "CNC Time Entry Sign In",
        notice: req.session.notice,
        body: `<section class="cnc-hero">
          <div>
            <p class="eyebrow">PowerApp Recreation</p>
            <h2>Badge sign-in, job time, checklist, maintenance, and support in one installable web app.</h2>
            <p>Enter the badge number and the workstation email used by this tablet or PC.</p>
          </div>
          <button class="cnc-button install" id="install-app-button" type="button" hidden>Install App</button>
        </section>
        <section class="cnc-card cnc-login-card">
          <form method="post" action="/login" class="cnc-form">
            <label><span>Badge / ADP Number</span><input name="adp_number" inputmode="numeric" required /></label>
            <label><span>Workstation Email</span><input name="user_email" type="email" placeholder="tablet@company.com" required /></label>
            <label><span>Shift</span><select name="shift_id">${shiftOptions}</select></label>
            <button class="cnc-button" type="submit">Sign In</button>
          </form>
          <div class="cnc-help">
            <p class="eyebrow">Known station emails</p>
            <div class="cnc-chip-row">${emailHints || "<span class='cnc-chip secondary'>No station emails loaded</span>"}</div>
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
      shift_id: req.body.shift_id,
    });
    req.session.notice = { kind: "success", message: "Signed in successfully." };
    res.redirect("/dashboard");
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
    res.redirect("/");
  }
});

app.post("/logout", (req, res) => {
  req.session.destroy(() => {
    res.redirect("/");
  });
});

app.get("/dashboard", requireAuth, async (req, res, next) => {
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
        body: `<section class="cnc-hero">
          <div>
            <p class="eyebrow">Welcome</p>
            <h2>${escapeHtml(sessionData.employee.full_name)}</h2>
            <p>The web app is using the same SharePoint-backed lists that the PowerApp used.</p>
          </div>
          <button class="cnc-button install" id="install-app-button" type="button" hidden>Install App</button>
        </section>
        <section class="cnc-grid">
          ${cards
            .map(
              (card) => `<a class="cnc-card cnc-action-card" href="${card.href}">
                <h3>${escapeHtml(card.title)}</h3>
                <p>${escapeHtml(card.copy)}</p>
              </a>`,
            )
            .join("")}
        </section>
        <section class="cnc-card">
          <div class="cnc-section-header"><h3>Current Status</h3></div>
          ${
            context.active_entry
              ? `<div class="cnc-status-card">
                  <strong>${escapeHtml(context.active_entry.production_number || "In Progress")}</strong>
                  <p>${escapeHtml(context.active_entry.operation_description || context.active_entry.description || "")}</p>
                  <div class="cnc-chip-row"><span class="cnc-chip">${escapeHtml(context.active_entry.status)}</span><span class="cnc-chip secondary">${escapeHtml(context.active_entry.details_type)}</span></div>
                </div>`
              : "<p class='cnc-empty'>No active entry is running right now.</p>"
          }
        </section>
        <section class="cnc-card">
          <div class="cnc-section-header"><h3>Recent Time Records</h3></div>
          ${timeTable(context.recent_entries)}
        </section>`,
      }),
    );
    req.session.notice = null;
  } catch (error) {
    next(error);
  }
});

app.get("/time", requireAuth, async (req, res, next) => {
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
    const miscOptions = context.details_step_two
      .map((item) => `<option value="${escapeHtml(item.type_ii || item.title)}">${escapeHtml(item.type_ii || item.title)}</option>`)
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
        <section class="cnc-grid two">
          <article class="cnc-card">
            <div class="cnc-section-header"><h3>Active Entry</h3></div>
            ${
              context.active_entry
                ? `<div class="cnc-status-card">
                    <strong>${escapeHtml(context.active_entry.production_number || "")}</strong>
                    <p>${escapeHtml(context.active_entry.operation_description || "")}</p>
                    <div class="cnc-chip-row"><span class="cnc-chip">${escapeHtml(context.active_entry.status)}</span><span class="cnc-chip secondary">${escapeHtml(context.active_entry.details_type || "")}</span></div>
                    <div class="cnc-inline-actions">
                      ${
                        context.active_entry.status === "In Progress"
                          ? `<form method="post" action="/time/pause"><input type="hidden" name="entry_id" value="${escapeHtml(context.active_entry.sp_id)}" /><button class="cnc-button ghost" type="submit">Start Lunch</button></form>`
                          : ""
                      }
                      ${
                        context.active_entry.status === "Paused"
                          ? `<form method="post" action="/time/resume"><input type="hidden" name="entry_id" value="${escapeHtml(context.active_entry.sp_id)}" /><button class="cnc-button ghost" type="submit">Resume</button></form>`
                          : ""
                      }
                      <form method="post" action="/time/stop"><input type="hidden" name="entry_id" value="${escapeHtml(context.active_entry.sp_id)}" /><button class="cnc-button danger" type="submit">Stop And Submit</button></form>
                    </div>
                  </div>`
                : "<p class='cnc-empty'>No active entry. Use the start form to begin a direct labor record.</p>"
            }
          </article>
          <article class="cnc-card">
            <div class="cnc-section-header"><h3>Start Direct Time</h3></div>
            <form method="post" action="/time/start" class="cnc-form">
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
              <label><span>Detail Type II</span><select name="detail_type_ii" required>${miscOptions}</select></label>
              <label><span>Hours</span><input name="hours" type="number" min="0" max="16" value="0" required /></label>
              <label><span>Minutes</span><input name="minutes" type="number" min="0" max="55" step="5" value="0" required /></label>
              <label class="full"><span>Comments</span><textarea name="comments" rows="3" required></textarea></label>
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
          ${timeTable(context.recent_entries)}
        </section>`,
      }),
    );
    req.session.notice = null;
  } catch (error) {
    next(error);
  }
});

app.post("/time/start", requireAuth, async (req, res) => {
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

app.post("/time/pause", requireAuth, async (req, res) => {
  try {
    await callCncBridge("pause_for_lunch", { entry_id: req.body.entry_id });
    req.session.notice = { kind: "info", message: "Lunch break started." };
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
  }
  res.redirect("/time");
});

app.post("/time/resume", requireAuth, async (req, res) => {
  try {
    await callCncBridge("resume_from_lunch", { entry_id: req.body.entry_id });
    req.session.notice = { kind: "success", message: "Lunch break ended." };
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
  }
  res.redirect("/time");
});

app.post("/time/stop", requireAuth, async (req, res) => {
  try {
    await callCncBridge("stop_time_entry", { entry_id: req.body.entry_id });
    req.session.notice = { kind: "success", message: "Active time entry submitted." };
  } catch (error) {
    req.session.notice = { kind: "warning", message: error.message };
  }
  res.redirect("/time");
});

app.post("/time/misc", requireAuth, async (req, res) => {
  try {
    await callCncBridge("submit_misc_time", {
      employee: req.session.cncUser.employee,
      shift_id: req.session.cncUser.shift_id,
      machine_no: req.session.cncUser.machine_no,
      detail_type_ii: req.body.detail_type_ii,
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

app.post("/time/manual", requireAuth, async (req, res) => {
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

app.get("/checklist", requireAuth, async (req, res, next) => {
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

app.post("/checklist", requireAuth, async (req, res) => {
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

app.get("/maintenance", requireAuth, async (req, res, next) => {
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

app.post("/maintenance", requireAuth, async (req, res) => {
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

app.get("/contact/it", requireAuth, async (req, res, next) => {
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

app.post("/contact/it", requireAuth, async (req, res) => {
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

app.get("/contact/supervisor", requireAuth, async (req, res, next) => {
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

app.post("/contact/supervisor", requireAuth, async (req, res) => {
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
});
