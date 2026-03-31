import express from "express";
import session from "express-session";
import path from "path";
import { fileURLToPath } from "url";
import { callBridge } from "./pythonBridge.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

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
                `<tr>${headers.map((h) => `<td>${escapeHtml(row[h])}</td>`).join("")}</tr>`,
            )
            .join("")}
        </tbody>
      </table>
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
      <form method="post" action="/logout" class="workflow-nav-logout">
        <button class="button workflow-logout-button" type="submit">Log Out</button>
      </form>
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

function renderWorkflowHeader(req) {
  const inspector = req.session.inspector;
  return `
    <div class="topbar">
      <div class="hero workflow-hero" style="flex:1">
        <h1>AutoIRR Workflow</h1>
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
        <h1>AutoIRR</h1>
        <p>Node.js duplicate of the Streamlit inspection workflow using the same backend logic.</p>
        <div class="badges">
          <span class="badge">Login</span>
          <span class="badge">Workflow</span>
          <span class="badge">Admin</span>
        </div>
      </section>
      ${renderNotice(req.session.notice)}
      <section class="card">
        <h2 class="section-title">Inspector Login</h2>
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
    res.send(layout({ title: "AutoIRR Node", sidebar: baseSidebar(req), content, theme: req.session.themeMode }));
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

app.get("/workflow/inspection", async (req, res, next) => {
  try {
    if (!req.session.inspector || !req.session.sessionRecord) return res.redirect("/");
    const inspector = req.session.inspector;
    const selection = {
      productionNumber: req.query.productionNumber || req.session.selection?.productionNumber || "",
      operationDescription: req.query.operationDescription || req.session.selection?.operationDescription || "",
      recipeName: req.query.recipeName || req.session.selection?.recipeName || "",
      pipeNumber: req.query.pipeNumber || req.session.selection?.pipeNumber || "",
    };
    req.session.selection = selection;

    const workOrders = await callBridge("get_open_work_orders", { branch: inspector.branch });
    const productionNumbers = [...new Set(workOrders.map((item) => item.production_number))].sort();
    if (!selection.productionNumber && productionNumbers.length) {
      selection.productionNumber = productionNumbers[0];
    }
    const connectionTypes = selection.productionNumber
      ? await callBridge("get_connection_types", { production_number: selection.productionNumber, branch: inspector.branch })
      : [];
    if (!selection.operationDescription && connectionTypes.length) {
      selection.operationDescription = connectionTypes[0].operation_description;
    }
    const recipeCandidates = selection.operationDescription
      ? await callBridge("find_recipe_candidates", { operation_description: selection.operationDescription, branch: inspector.branch })
      : [];
    if (!selection.recipeName && recipeCandidates.length) {
      selection.recipeName = recipeCandidates[0].recipe_name;
    }
    req.session.selection = selection;
    const recipeDefinition = selection.recipeName
      ? await callBridge("get_recipe_elements", { recipe_name: selection.recipeName, branch: inspector.branch })
      : null;
    const existingPipe = selection.pipeNumber
      ? await callBridge("get_pipe_unit", {
          production_number: selection.productionNumber,
          operation_description: selection.operationDescription,
          pipe_number: selection.pipeNumber,
        })
      : null;
    const history = existingPipe ? await callBridge("get_pipe_attempt_history", { pipe_unit_id: existingPipe.id }) : [];
    const content = `
      ${renderWorkflowHeader(req)}
      ${renderNotice(req.session.notice)}
      ${renderWorkflowNav("/workflow/inspection")}
      <section class="card">
        <h2 class="section-title">Inspection Entry</h2>
        <form method="get" action="/workflow/inspection" class="form-grid" id="inspection-selection-form">
          <div class="field"><label>Production Number / WO</label><select name="productionNumber" onchange="this.form.operationDescription.value=''; this.form.recipeName.value=''; this.form.submit();">${renderOptions(productionNumbers, selection.productionNumber, (item) => item, (item) => item)}</select></div>
          <div class="field"><label>Connection Type / Operation Description</label><select name="operationDescription" onchange="this.form.recipeName.value=''; this.form.submit();"><option value=""></option>${renderOptions(connectionTypes, selection.operationDescription, (item) => item.operation_description, (item) => item.operation_description)}</select></div>
          <div class="field"><label>Suggested Recipe</label><select name="recipeName" onchange="this.form.submit();"><option value=""></option>${renderOptions(recipeCandidates, selection.recipeName, (item) => item.recipe_name, (item) => item.recipe_name)}</select></div>
          <div class="field"><label>Pipe Number</label><input name="pipeNumber" value="${escapeHtml(selection.pipeNumber)}" /></div>
          <div class="actions"><button class="button" type="submit">Update Pipe Number</button></div>
        </form>
        ${existingPipe ? renderNotice({ kind: "warning", message: `Pipe ${selection.pipeNumber} already exists for this WO/connection. Re-work attempt #${existingPipe.latest_attempt_no + 1}.` }) : ""}
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
                 ${renderTable(recipeDefinition.elements)}
               </details>`
            : renderNotice({ kind: "warning", message: "Select a connection and recipe before preparing the inspection." })
        }
        <form method="post" action="/workflow/prepare" class="actions">
          <button class="button" type="submit" ${recipeDefinition && selection.pipeNumber ? "" : "disabled"}>Prepare Inspection</button>
        </form>
        ${req.session.activeInspection ? `<section class="card nested-card">
             <h3 class="section-title">Current Attempt</h3>
             <p>Attempt #${escapeHtml(req.session.activeInspection.attempt_no)} | ${req.session.activeInspection.is_rework ? "Re-work" : "First inspection"}</p>
             ${
               req.session.activeInspection.inspection_plan?.length
                 ? ""
                 : renderNotice({ kind: "warning", message: "This attempt has no measurement plan yet. Go back to the selection above and make sure a recipe is selected before preparing the inspection." })
             }
             <form method="post" action="/workflow/complete" class="form-grid">
               ${req.session.activeInspection.inspection_plan
                 .map(
                   (element) => `<div class="field measurement-field">
                     <label>${escapeHtml(`${element.element_sequence}. ${element.element_description}`)}</label>
                     <div class="measurement-meta">${renderMeasurementMeta(element)}</div>
                     ${element.notes ? `<p class="measurement-notes">${escapeHtml(element.notes)}</p>` : ""}
                     ${
                       element.capture_type === "boolean"
                         ? `<select name="element_${element.element_sequence}"><option>Yes</option><option>No</option></select>`
                         : `<input name="element_${element.element_sequence}" placeholder="${escapeHtml(element.nominal ?? "")}" />`
                     }
                   </div>`,
                 )
                 .join("")}
               <div class="field"><label>Inspection Outcome</label><select name="disposition"><option value="pass">Pass</option><option value="manager_approved">Pass with Manager Approval</option><option value="rework">Send to Re-work</option><option value="scrapped">Scrap</option></select></div>
               <div class="field"><label>Manager Name</label><input name="manager_name" /></div>
               <div class="field"><label>Manager Item ID</label><input name="manager_item_id" placeholder="Use Admin Tools to create manager PINs first" /></div>
               <div class="field"><label>Manager PIN</label><input name="manager_pin" /></div>
               <div class="field"><label>Tier Code</label><input name="tier_code" /></div>
               <div class="field"><label>Nonconformance</label><textarea name="nonconformance"></textarea></div>
               <div class="field"><label>Immediate Containment</label><textarea name="immediate_containment"></textarea></div>
               <div class="field"><label>Attempt Notes</label><textarea name="notes"></textarea></div>
               <div class="actions"><button class="button" type="submit">Complete Inspection</button></div>
             </form>
           </section>` : ""}
        ${history.length ? `<h3 class="section-title">Pipe History</h3>${renderTable(history)}` : ""}
      </section>
    `;

    req.session.notice = null;
    res.send(layout({ title: "AutoIRR Workflow", sidebar: baseSidebar(req), content, theme: req.session.themeMode }));
  } catch (error) {
    next(error);
  }
});

app.get("/workflow/history", async (req, res, next) => {
  try {
    if (!req.session.inspector || !req.session.sessionRecord) return res.redirect("/");
    const inspector = req.session.inspector;
    const pipeRows = await callBridge("search_pipe_units", {
      branch: inspector.branch,
      production_number: req.query.historyProduction || null,
      pipe_number: req.query.historyPipe || null,
      status: req.query.historyStatus || null,
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
          <div class="field"><label>Filter Production Number</label><input name="historyProduction" value="${escapeHtml(req.query.historyProduction || "")}" /></div>
          <div class="field"><label>Filter Pipe Number</label><input name="historyPipe" value="${escapeHtml(req.query.historyPipe || "")}" /></div>
          <div class="field"><label>Filter Status</label><select name="historyStatus"><option value=""></option><option>in_progress</option><option>completed</option><option>rework</option><option>scrapped</option></select></div>
          <div class="actions"><button class="button" type="submit">Search Pipe History</button></div>
        </form>
        ${renderTable(pipeRows)}
      </section>
    `;
    req.session.notice = null;
    res.send(layout({ title: "Pipe History", sidebar: baseSidebar(req), content, theme: req.session.themeMode }));
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

app.post("/workflow/prepare", async (req, res, next) => {
  try {
    const selection = req.session.selection;
    const inspector = req.session.inspector;
    const sessionRecord = req.session.sessionRecord;
    if (!selection?.recipeName || !selection?.pipeNumber) {
      req.session.notice = { kind: "warning", message: "Choose a recipe and pipe number before preparing an inspection." };
      return res.redirect("/workflow/inspection");
    }
    const recipeDefinition = await callBridge("get_recipe_elements", { recipe_name: selection.recipeName, branch: inspector.branch });
    if (!recipeDefinition?.elements?.length) {
      req.session.notice = { kind: "warning", message: "The selected recipe does not have any elements to inspect." };
      return res.redirect("/workflow/inspection");
    }
    const activeInspection = await callBridge("create_inspection_attempt", {
      params: {
        production_number: selection.productionNumber,
        operation_description: selection.operationDescription,
        pipe_number: selection.pipeNumber,
        branch: inspector.branch,
        session_id: sessionRecord.id,
        inspector,
        cnc_operator: {
          item_id: sessionRecord.cnc_operator_item_id,
          name: sessionRecord.cnc_operator_name,
        },
        recipe_name: selection.recipeName,
        recipe_elements: recipeDefinition,
      },
    });
    req.session.activeInspection = activeInspection;
    req.session.notice = { kind: "info", message: "Inspection prepared for the selected pipe." };
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
    const evaluation = await callBridge("evaluate_measurements", {
      measurements,
      approval_rules: active.approval_rules || [],
    });
    const result = await callBridge("complete_inspection_attempt", {
      params: {
        attempt_id: active.attempt_id,
        pipe_unit_id: active.pipe_unit_id,
        measurements: evaluation.measurements,
        disposition: req.body.disposition,
        notes: req.body.notes || "",
        manager_item_id: req.body.manager_item_id || null,
        manager_name: req.body.manager_name || "",
        manager_pin: req.body.manager_pin || "",
        ncr_data: {
          tier_code: req.body.tier_code || "",
          nonconformance: req.body.nonconformance || "",
          immediate_containment: req.body.immediate_containment || "",
        },
      },
    });
    req.session.activeInspection = null;
    req.session.notice = { kind: "success", message: `Inspection saved. Attempt status: ${result.attempt_status}. Pipe status: ${result.pipe_status}.` };
    res.redirect("/workflow");
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
    const content = `
      <section class="hero">
        <h1>AutoIRR Admin</h1>
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
    `;
    req.session.notice = null;
    res.send(layout({ title: "AutoIRR Admin", sidebar: baseSidebar(req), content, theme: req.session.themeMode }));
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
      title: "AutoIRR Node Error",
      sidebar: baseSidebar(req),
      theme: req.session?.themeMode,
      content: `<section class="card"><h1>Error</h1><p>${escapeHtml(error.message)}</p></section>`,
    }),
  );
});

const port = Number(process.env.NODE_APP_PORT || 3000);
app.listen(port, () => {
  console.log(`AutoIRR Node app listening on http://localhost:${port}`);
});
