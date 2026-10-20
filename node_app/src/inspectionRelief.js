export const escapeReliefLabel = value => String(value ?? "").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[c]);
export function reliefColumnClass(inspectors) {return inspectors?.length ? 'relief-pipe-column' : '';}
export function reliefColumnLabel(inspectors) {
  return inspectors?.length ? `<small class="relief-pipe-label">Relief: ${inspectors.map(person=>`${escapeReliefLabel(person.name)} (ADP ${escapeReliefLabel(person.adp_number)})`).join(', ')}</small>` : '';
}
