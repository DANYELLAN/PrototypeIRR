export function submittedMeasurementValue(element, body) {
  const value = body[`element_${element.element_sequence}`];
  // An omitted/blank visual checkbox is a failure, never a missing measurement.
  if (element.capture_type === "boolean" && (value == null || String(value).trim() === "")) return "No";
  return value;
}

export async function loadNextInspection(session, completed, callBridge, incrementPipeNumber, connectionLabel) {
  const selection = session.selection;
  const current = String(completed.pipe_number || selection?.pipeNumber || "").trim();
  const next = incrementPipeNumber(current);
  session.activeInspection = null;
  session.showInspectionSheet = true;
  if (!selection) throw new Error("Select the work order and connection before loading another pipe.");
  selection.pipeNumber = next;
  if (!next || next === current) throw new Error("Enter the next pipe number in the worksheet header.");
  const inspector = session.inspector;
  const station = session.sessionRecord;
  if (!inspector || !station?.id) throw new Error("An active inspector session is required to load the next pipe.");
  const recipe = await callBridge("get_recipe_elements", { recipe_name: selection.recipeName, branch: inspector.branch });
  if (!recipe?.elements?.length) throw new Error("The selected Digital IRR has no inspection elements.");
  // The backend preserves scope/rotation and resumes in-progress attempts, but rejects completed/scrapped numbers.
  const attempt = await callBridge("create_inspection_attempt", { params: {
    production_number: selection.productionNumber,
    operation_description: connectionLabel(selection),
    pipe_number: next,
    branch: inspector.branch,
    session_id: station.id,
    inspector,
    cnc_operator: { item_id: station.cnc_operator_item_id, name: station.cnc_operator_name },
    recipe_name: selection.recipeName,
    recipe_elements: recipe,
    inspection_scope: selection.inspectionScope || "standard",
  } });
  session.activeInspection = attempt;
  return next;
}
