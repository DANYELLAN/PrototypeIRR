import { canViewPipeReport } from "./inspectionPermissions.js";
const e = (v) => String(v ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[c]);
const stamp = v => v ? e(new Date(v).toLocaleString()) : "—";
const base = "/workflow/lunch";

export function renderLunchWorksheet(row, values = {}) {
  const pipe = row.source_snapshot;
  return `<h2>Lunch Inspection — Pipe # ${e(pipe.pipe_number)}</h2><p><strong>${e(row.reviewer_name)}</strong> · ADP ${e(row.reviewer_adp)} · ${e(pipe.location_name)} · WO ${e(pipe.production_number)}</p><p>${e(pipe.recipe_name)} · ${e(pipe.operation_description)}</p><p>Full inspection: complete every element. An unchecked visual box records a failed visual inspection.</p><form method="post" action="${base}/complete"><input type="hidden" name="inspection_id" value="${row.id}"><div class="table-wrap inspection-sheet-wrap"><table class="inspection-sheet-table"><thead><tr><th>#</th><th>Element</th><th>DWG DIM</th><th>Gauge</th><th>Lunch Measurement</th></tr></thead><tbody>${row.recipe_snapshot.map(element => {
    const key = `measurement_${element.element_sequence}`;
    return `<tr><td>${e(element.element_sequence)}</td><td>${e(element.element_description)}</td><td>${e(element.dwg_dim)}</td><td>${e(element.gauge)}</td><td>${element.capture_type === "boolean" ? `<label><input type="checkbox" name="${key}" value="Yes" ${values[key] === "Yes" ? "checked" : ""}> Visual pass</label>` : `<input aria-label="${e(element.element_description)}" name="${key}" ${element.capture_type === "numeric" ? 'type="number" step="any"' : 'type="text"'} required value="${e(values[key] || "")}">`}</td></tr>`;
  }).join("")}</tbody></table></div><label>Notes <textarea name="notes">${e(values.notes || "")}</textarea></label><button class="button" type="submit">Submit Lunch Inspection</button></form><p>Saved only to Lunch Inspections history.</p>`;
}

export function renderLunchHistory(data) {
  return `<h1>Lunch Inspections History</h1><p>${data.total} completed lunch inspections · Page ${data.page} of ${data.pages}</p>${data.inspections.map(row => `<section class="card"><h2>Pipe # ${e(row.confirmed_pipe_number)} — ${e(row.status)}</h2><p>${e(row.source_snapshot.location_name)} · WO ${e(row.source_snapshot.production_number)} · ${e(row.source_snapshot.recipe_name)}</p><p>Inspected by ${e(row.reviewer_name)} (ADP ${e(row.reviewer_adp)}, ${e(row.reviewer_title)}) · ${stamp(row.completed_at)}</p><div class="table-wrap"><table class="inspection-sheet-table"><thead><tr><th>#</th><th>Element</th><th>DWG DIM</th><th>Gauge</th><th>Lunch Measurement</th><th>Result</th></tr></thead><tbody>${row.measurements.map(m => `<tr><td>${e(m.element_sequence)}</td><td>${e(m.element_description)}</td><td>${e(m.dwg_dim)}</td><td>${e(m.gauge)}</td><td>${e(m.measured_value)}</td><td>${e(m.pass_fail)}</td></tr>`).join("")}</tbody></table></div><p>${e(row.notes)}</p></section>`).join("") || '<p>No lunch inspections submitted yet.</p>'}<nav>${data.page > 1 ? `<a class="button secondary" href="${base}/history?page=${data.page-1}">← Previous 20</a>` : ""} ${data.page < data.pages ? `<a class="button" href="${base}/history?page=${data.page+1}">Next 20 →</a>` : ""}</nav>`;
}

export function registerLunchInspectionRoutes(app, bridge, layout) {
  const show = (req, res, content, status=200) => res.status(status).send(layout(req, `<section class="card"><p><a href="${base}">Lunch Inspection</a> · <a href="${base}/history">Lunch Inspections History</a> · <a href="/workflow/inspection">Return to Inspection Entry</a></p>${content}</section>`));
  app.use(base, (req, res, next) => {
    res.set("Cache-Control", "no-store");
    if (!req.session?.inspector) return res.status(401).send("Sign in at a station first.");
    next();
  });
  app.get(base, async (req, res, next) => {
    try {
      if (req.session.lunchInspectionId && req.session.lunchReviewerAdp) {
        const row = await bridge("get_lunch", {inspection_id:req.session.lunchInspectionId, reviewer_adp:req.session.lunchReviewerAdp});
        if (row.status === "in_progress") return show(req,res,renderLunchWorksheet(row));
      }
      const locations = await bridge("get_locations");
      const station = req.session.sessionRecord?.location_id;
      show(req,res,`<h1>Lunch Inspection</h1><p>Identify the Quality admin or IT reviewer, then confirm this station’s last completed pipe. This full inspection is recorded separately from production inspections.</p><form method="post" action="${base}/identify"><label>Admin ADP Number <input name="reviewer_adp" required autocomplete="off"></label><label>Station <select name="station_id" required>${locations.map(loc=>`<option value="${loc.id}" ${String(loc.id)===String(station)?"selected":""}>${e(loc.location_name)}</option>`).join("")}</select></label><button class="button" type="submit">Find Last Completed Pipe</button></form>`);
    } catch(err) { next(err); }
  });
  app.post(`${base}/identify`, async (req,res) => {
    try {
      const data = await bridge("lunch_target",{station_id:req.body.station_id,reviewer_adp:req.body.reviewer_adp});
      req.session.lunchPending = {station_id:req.body.station_id, reviewer_adp:data.reviewer.adp_number, source_attempt_id:data.pipe.attempt_id};
      show(req,res,`<h1>Confirm Lunch Inspection Pipe</h1><p>Reviewer: <strong>${e(data.reviewer.name)}</strong> · ADP ${e(data.reviewer.adp_number)}</p><p>Station: ${e(data.pipe.location_name)} · WO ${e(data.pipe.production_number)} · Last completed pipe: <strong># ${e(data.pipe.pipe_number)}</strong> · ${stamp(data.pipe.completed_at)}</p><p>Original inspector: ${e(data.pipe.inspector_name)} · ${e(data.pipe.recipe_name)}</p><form method="post" action="${base}/start"><label>Confirm the pipe number on the pipe <input name="confirmed_pipe_number" required autocomplete="off"></label><button class="button" type="submit">Start Full Lunch Inspection</button></form>`);
    } catch(err) { show(req,res,`<p role="alert">${e(err.message)}</p>`,400); }
  });
  app.post(`${base}/start`, async (req,res) => {
    try {
      if (!req.session.lunchPending) throw new Error("Identify the admin and confirm the last completed pipe first.");
      const row = await bridge("start_lunch",{...req.session.lunchPending,confirmed_pipe_number:req.body.confirmed_pipe_number});
      req.session.lunchInspectionId = row.id;
      req.session.lunchReviewerAdp = row.reviewer_adp;
      req.session.lunchPending = null;
      res.redirect(base);
    } catch(err) { show(req,res,`<p role="alert">${e(err.message)}</p>`,400); }
  });
  app.post(`${base}/complete`, async (req,res) => {
    let row;
    try {
      if (!req.session.lunchInspectionId || String(req.body.inspection_id)!==String(req.session.lunchInspectionId)) return res.sendStatus(403);
      row = await bridge("get_lunch",{inspection_id:req.session.lunchInspectionId,reviewer_adp:req.session.lunchReviewerAdp});
      const values = Object.fromEntries(row.recipe_snapshot.map(el=>[String(el.element_sequence),req.body[`measurement_${el.element_sequence}`] || ""]));
      const saved = await bridge("complete_lunch",{inspection_id:row.id,reviewer_adp:req.session.lunchReviewerAdp,values,notes:req.body.notes || ""});
      req.session.lunchInspectionId = null;
      req.session.lunchReviewerAdp = null;
      show(req,res,`<h1>Lunch Inspection Saved</h1><p>Pipe # ${e(saved.confirmed_pipe_number)} · ${e(saved.status)} · ${e(saved.reviewer_name)}</p><p>Recorded in the separate Lunch Inspections history.</p><a class="button" href="${base}">Finish / Return</a>`);
    } catch(err) { show(req,res,`<p role="alert">${e(err.message)}</p>${row ? renderLunchWorksheet(row,req.body) : ""}`,400); }
  });
  app.get(`${base}/history`, async (req,res) => {
    try {
      const employee = await bridge("get_employee_by_adp",{adp_number:req.session.inspector.adp_number});
      if (!canViewPipeReport(employee)) return res.status(403).send("Lunch Inspections history is available only to Quality admins and IT. Sign in with your own admin account to view it.");
      show(req,res,renderLunchHistory(await bridge("lunch_history",{reviewer_adp:employee.adp_number,page:req.query.page || 1})));
    } catch(err) { show(req,res,`<p role="alert">${e(err.message)}</p>`,400); }
  });
}
