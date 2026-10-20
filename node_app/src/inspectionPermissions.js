const historyEditorTitles = new Set([
  "director of quality",
  "quality control specialist",
  "quality manager",
  "quality supervisor",
]);

export function canEditInspectionHistory(employee) {
  return historyEditorTitles.has(String(employee?.adp_job_title_desc || "").trim().toLowerCase());
}

export function canViewPipeReport(employee) {
  if (!employee || (employee.employee_status && employee.employee_status !== "Active")) return false;
  const title = String(employee.adp_job_title_desc || "").trim().toLowerCase();
  const department = String(employee.department || "").trim().toLowerCase();
  return canEditInspectionHistory(employee) || ["it", "it manager", "information technology", "information systems", "integration technologist"].includes(title)
    || ["it", "information technology", "information systems"].includes(department);
}

export function createHistoryEditGuard(callBridge) {
  return async (req, res, next) => {
    res.set("Cache-Control", "no-store");
    if (!req.session?.inspector || !req.session.sessionRecord) return res.redirect("/");
    try {
      // Recheck the authoritative employee record; never trust a posted title or a cached admin permission.
      const employee = await callBridge("get_employee_by_adp", { adp_number: req.session.inspector.adp_number });
      if (!canEditInspectionHistory(employee)) {
        return res.status(403).send("Only Director of Quality, Quality Control Specialist, Quality Manager, and Quality Supervisor may change saved pipe inspections.");
      }
      return next();
    } catch (error) { return next(error); }
  };
}
