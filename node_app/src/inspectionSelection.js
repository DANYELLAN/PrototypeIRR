export const INSPECTION_CONNECTION_TYPES = Object.freeze(['BEN-IGT','BEN-EUI','BEN-HT6','BEN-FJ','BEN-SF','BEN-HSF II','BTS-6']);
const canonicalConnection = value => {
  const normalized=String(value ?? '').trim().toUpperCase().replace(/\s*-\s*/g,'-').replace(/\s+/g,' ');
  return INSPECTION_CONNECTION_TYPES.includes(normalized) ? normalized : '';
};

export function resolveWorkOrderSelection(workOrders, requested = {}, previous = {}) {
  const numbers = [...new Set(workOrders.map((item) => item.production_number))].sort();
  const productionNumber = String(requested.productionNumber ?? previous.productionNumber ?? numbers[0] ?? '');
  const operations = workOrders.filter((item) => String(item.production_number) === productionNumber);
  const descriptions = [...new Map(operations.map((item) => [item.operation_description, item])).values()];
  const sameWorkOrder = productionNumber === previous.productionNumber;
  let description = String(requested.operationDescription ?? (sameWorkOrder ? previous.operationDescription : '') ?? '');
  if (!descriptions.some((item) => item.operation_description === description)) description = '';
  const specified = descriptions.filter((item) => {
    const parts = item.inspection_entry || {};
    return parts.size && parts.weight && canonicalConnection(parts.connection) && parts.end_type;
  });
  if (!description && specified.length === 1) description = specified[0].operation_description;
  const operation = descriptions.find((item) => item.operation_description === description);
  const parts = operation?.inspection_entry || {};
  const changed = productionNumber !== previous.productionNumber || description !== previous.operationDescription;
  const specificationContext = JSON.stringify([productionNumber,description]);
  // Submitted fields belong only to the description that rendered them.
  const acceptRequested = !changed || requested.specificationContext === specificationContext;
  const automatic = {sizeLabel:parts.size || '',weightLabel:parts.weight || '',connectionLabel:canonicalConnection(parts.connection),endType:parts.end_type || ''};
  const missingFields=Object.keys(automatic).filter(key=>!automatic[key]);
  const details={};
  for(const key of Object.keys(automatic)) {
    const fallback=acceptRequested && Object.hasOwn(requested,key) ? requested[key] : !changed ? previous[key] : '';
    details[key]=operation ? automatic[key] || String(fallback ?? '').trim() : '';
  }
  details.connectionLabel=canonicalConnection(details.connectionLabel);
  details.endType=['BOX','PIN'].includes(details.endType.toUpperCase()) ? details.endType.toUpperCase() : '';
  const detailsChanged=!changed && Object.keys(details).some(key=>Object.hasOwn(previous,key) && previous[key]!==details[key]);
  return {
    descriptions, missingFields, specificationContext,
    usesManualDetails:missingFields.some(key=>Boolean(details[key])),
    selection: {
      productionNumber,operationDescription:description,...details,
      recipeName:String(requested.recipeName ?? (changed || detailsChanged ? '' : previous.recipeName) ?? ''),
      pipeNumber:changed || detailsChanged ? '' : String(requested.pipeNumber ?? previous.pipeNumber ?? ''),
      inspectionScope:requested.inspectionScope || previous.inspectionScope || 'standard',
    },
    changed:changed || detailsChanged,
  };
}

export function chooseInspectionRecipe(candidates, requested = "") {
  if (requested && candidates.some((item) => item.recipe_name === requested)) return requested;
  if (candidates.length === 1 && ["alias", "specification"].includes(candidates[0].match_type)) return candidates[0].recipe_name;
  const ranked = candidates.filter((item) => ["alias", "specification"].includes(item.match_type));
  if (ranked.length > 1 && Number(ranked[0].score || 0) > Number(ranked[1].score || 0)) return ranked[0].recipe_name;
  return "";
}
