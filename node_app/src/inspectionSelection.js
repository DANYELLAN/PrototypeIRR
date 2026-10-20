export function resolveWorkOrderSelection(workOrders, requested = {}, previous = {}) {
  const numbers = [...new Set(workOrders.map((item) => item.production_number))].sort();
  const productionNumber = String(requested.productionNumber ?? previous.productionNumber ?? numbers[0] ?? "");
  const operations = workOrders.filter((item) => String(item.production_number) === productionNumber);
  const descriptions = [...new Map(operations.map((item) => [item.operation_description, item])).values()];
  const sameWorkOrder = productionNumber === previous.productionNumber;
  let description = String(requested.operationDescription ?? (sameWorkOrder ? previous.operationDescription : "") ?? "");
  if (!descriptions.some((item) => item.operation_description === description)) description = "";
  // A single fully specified operation is unambiguous; otherwise let the inspector choose.
  const specified = descriptions.filter((item) => {
    const parts = item.inspection_entry || {};
    return parts.size && parts.weight && parts.connection && parts.end_type;
  });
  if (!description && specified.length === 1) description = specified[0].operation_description;
  const operation = descriptions.find((item) => item.operation_description === description);
  const parts = operation?.inspection_entry || {};
  const changed = productionNumber !== previous.productionNumber || description !== previous.operationDescription;
  return {
    descriptions,
    selection: {
      productionNumber,
      operationDescription: description,
      sizeLabel: parts.size || "", weightLabel: parts.weight || "", connectionLabel: parts.connection || "", endType: parts.end_type || "",
      recipeName: String(requested.recipeName ?? (changed ? "" : previous.recipeName) ?? ""),
      pipeNumber: changed ? "" : String(requested.pipeNumber ?? previous.pipeNumber ?? ""),
      inspectionScope: requested.inspectionScope || previous.inspectionScope || "standard",
    },
    changed,
  };
}

export function chooseInspectionRecipe(candidates, requested = "") {
  if (requested && candidates.some((item) => item.recipe_name === requested)) return requested;
  if (candidates.length === 1 && ["alias", "specification"].includes(candidates[0].match_type)) return candidates[0].recipe_name;
  const ranked = candidates.filter((item) => ["alias", "specification"].includes(item.match_type));
  if (ranked.length > 1 && Number(ranked[0].score || 0) > Number(ranked[1].score || 0)) return ranked[0].recipe_name;
  return "";
}
