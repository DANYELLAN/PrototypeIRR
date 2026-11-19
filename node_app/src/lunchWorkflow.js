import { randomUUID } from "node:crypto";
const inspectionPaused = session => Boolean(session.inspectionBreak && !session.inspectionBreak.relief);

export function registerLunchWorkflow(app, callBridge, buildConnectionLabel) {
  const allowed = (req, res) => {
    if (!req.session.inspector || !req.session.sessionRecord) {
      res.status(401).json({ error: "Sign in before opening a lunch inspection." });
      return false;
    }
    return true;
  };
  app.post("/workflow/lunch/open", async (req, res) => {
    if (!allowed(req, res)) return;
    try { req.breakState = await callBridge("inspection_break_state", {session_id:req.session.sessionRecord.id}); }
    catch(error) { return res.status(409).json({error:error.message}); }
    if (!req.session.breakCsrf || req.body.csrf !== req.session.breakCsrf) return res.status(403).json({ error: "Reload the inspection before opening lunch inspection." });
    if ((req.breakState?.mode === "without_relief" || inspectionPaused(req.session))) return res.status(409).json({ error: "End break or add relief before inspecting." });
    try {
      const selection = req.session.selection || {};
      const data = await callBridge("begin_lunch_inspection", { params: {
        session_id: req.session.sessionRecord.id, branch: req.session.inspector.branch,
        production_number: selection.productionNumber || "", operation_description: buildConnectionLabel(selection),
      } });
      req.session.lunchCsrf ||= randomUUID();
      res.json({ ...data, csrf: req.session.lunchCsrf });
    } catch (error) { res.status(400).json({ error: error.message }); }
  });
  app.post("/workflow/lunch/:id", async (req, res) => {
    if (!allowed(req, res)) return;
    try { req.breakState = await callBridge("inspection_break_state", {session_id:req.session.sessionRecord.id}); }
    catch(error) { return res.status(409).json({error:error.message}); }
    if (!req.session.lunchCsrf || req.body.csrf !== req.session.lunchCsrf) return res.status(403).json({ error: "Reopen the lunch inspection before saving." });
    if ((req.breakState?.mode === "without_relief" || inspectionPaused(req.session))) return res.status(409).json({ error: "End break or add relief before inspecting." });
    try {
      const saved = await callBridge("save_lunch_inspection", { params: {
        record_id: req.params.id, session_id: req.session.sessionRecord.id,
        branch: req.session.inspector.branch, adp_number: String(req.body.adp || "").trim(),
        values: req.body.values || {},
      } });
      res.json({ record: saved });
    } catch (error) { res.status(400).json({ error: error.message }); }
  });
  app.get("/workflow/lunch/:id", async (req, res) => {
    if (!allowed(req, res)) return;
    try { req.breakState = await callBridge("inspection_break_state", {session_id:req.session.sessionRecord.id}); }
    catch(error) { return res.status(409).json({error:error.message}); }
    if (!req.session.inspector.branch) return res.status(403).json({ error: "A branch is required to view this record." });
    try {
      res.json({ record: await callBridge("get_lunch_inspection", { params: { record_id: req.params.id, branch: req.session.inspector.branch } }) });
    } catch (error) { res.status(404).json({ error: error.message }); }
  });
}
