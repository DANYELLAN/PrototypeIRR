export function adminInspectionContent() {
  return `<div class="admin-monitor" data-admin-monitor>
    <div class="admin-monitor-heading"><h2>Inspection Monitoring</h2><a href="/admin">Admin Tools</a></div>
    <div class="admin-monitor-controls"><label><input type="checkbox" data-live-updates checked /> Live updates</label><button class="button secondary compact-button" type="button" data-monitor-refresh>Refresh</button><span role="status" data-monitor-status></span></div>
    <section id="live-inspections"><h3>Ongoing Local Inspections</h3><div data-live-inspections></div></section>
    <section id="lunch-inspections"><h3>Lunch Inspections</h3>
      <form class="admin-lunch-filters" data-lunch-filters><div class="field"><label>Work Order</label><input name="work_order" /></div><div class="field"><label>Result</label><select name="result"><option value="">All</option><option value="pass">Pass</option><option value="fail">Fail</option></select></div><button class="button compact-button" type="submit">Filter</button></form>
      <div class="table-wrap"><table><thead><tr><th>Submitted</th><th>Branch</th><th>WO</th><th>Connection</th><th>Pipe #</th><th>Inspector</th><th>Result</th><th></th></tr></thead><tbody data-admin-lunch-records></tbody></table></div>
      <dialog class="lunch-inspection-dialog" data-lunch-dialog data-record-endpoint="/admin/inspections/lunch/" aria-labelledby="lunch-dialog-title"><header class="lunch-dialog-header"><h3 id="lunch-dialog-title">Lunch Inspection</h3><button type="button" class="lunch-dialog-close" data-lunch-close aria-label="Close lunch inspection">&times;</button></header><div data-lunch-content></div></dialog>
    </section>
  </div>`;
}

export function registerAdminInspections(app, callBridge, renderPage, {registerDraft = true} = {}) {
  app.get("/admin/inspections", async (req, res, next) => {
    try {
      if (!req.session.inspector) return res.redirect("/");
      const employee = await callBridge("get_employee_by_adp", { adp_number: req.session.inspector.adp_number });
      if (!(await callBridge("is_admin_user", { employee }))) return res.status(403).send("Admin access required.");
      req.session.canAccessAdmin = true;
      res.send(renderPage(req, adminInspectionContent()));
    } catch (error) { next(error); }
  });
  const dataRoute = (action, params) => async (req, res) => {
    res.setHeader("Cache-Control", "no-store");
    if (!req.session.inspector?.adp_number) return res.status(401).json({ error: "Sign in to view inspections." });
    try {
      res.json(await callBridge(action, { params: { actor_adp: req.session.inspector.adp_number, ...params(req) } }));
    } catch (error) { res.status(error.message === "Admin access required." ? 403 : 500).json({ error: error.message }); }
  };
  app.get("/admin/inspections/data", dataRoute("get_admin_inspection_data", req => ({ work_order: req.query.work_order || "", result: req.query.result || "" })));
  app.get("/admin/inspections/lunch/:id", dataRoute("get_admin_lunch_inspection", req => ({ record_id: req.params.id })));
  if (registerDraft) app.post("/workflow/live-draft", async (req, res) => {
    if (!req.session.inspector || !req.session.sessionRecord) return res.status(401).json({ error: "Sign in before updating the inspection draft." });
    if (!req.session.breakCsrf || req.body.csrf !== req.session.breakCsrf) return res.status(403).json({ error: "Reload the inspection." });
    if (!req.session.activeInspection || String(req.body.attempt_id) !== String(req.session.activeInspection.attempt_id)) return res.status(409).json({ error: "This pipe is no longer open." });
    try {
      res.json(await callBridge("save_live_draft", { params: {
        attempt_id: req.session.activeInspection.attempt_id, session_id: req.session.sessionRecord.id,
        readings: req.body.readings,
      } }));
    } catch (error) { res.status(409).json({ error: error.message }); }
  });
}
