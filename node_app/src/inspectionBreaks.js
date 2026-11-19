import { escapeReliefLabel } from "./inspectionRelief.js";
const e=escapeReliefLabel;
export function renderBreakControls(state, activeInspection = null, csrf = "") {
  return `<section class="inspection-break-controls" id="inspection-break-controls" data-state="${e(JSON.stringify(state))}"><h3 class="section-title inspection-attempt-heading">Current Attempt — <span id="inspection-coverage-label" role="status">Inspecting: ${e(state.acting_name || state.original_name || '')}</span></h3><div class="inspection-action-buttons" role="group" aria-label="Inspection actions"><form method="post" action="/workflow/scope" id="inspection-scope-action-form" class="inspection-scope-toggle-form"><input type="hidden" name="_coverage_version" value="${Number(state.version) || 0}" /><input type="hidden" name="inspectionScope" value="${activeInspection?.inspection_scope === 'full' ? 'standard' : 'full'}" /><button class="button secondary compact-button" type="submit" ${activeInspection ? '' : 'disabled title="Load a pipe to change its inspection scope"'}>${activeInspection?.inspection_scope === 'full' ? 'Switch to Standard Inspection' : 'Switch to Full Inspection'}</button></form><button class="button secondary compact-button" type="button" id="lunch-inspection-button" data-lunch-open>Lunch Inspection</button><button type="button" class="button secondary compact-button" id="inspection-break-button">${state.mode ? 'End Break' : 'Break'}</button></div><p id="inspection-break-error" role="alert"></p></section>
<dialog id="inspection-break-dialog" class="inspection-break-dialog" aria-labelledby="inspection-break-heading"><h2 id="inspection-break-heading">Break</h2><p id="inspection-break-description"></p><div id="inspection-break-choices"><button type="button" class="button" data-break-choice="with_relief">With Relief</button> <button type="button" class="button secondary" data-break-choice="without_relief">Without Relief</button></div><form id="inspection-relief-form" hidden><label>Relief Inspector ADP Number <input name="relief_adp" required autocomplete="off"></label><button type="submit" class="button">Start Break With Relief</button></form><button type="button" class="button" id="inspection-break-end" hidden>End Break</button><button type="button" class="button secondary" id="inspection-relief-arrived" hidden>Relief Arrived</button><button type="button" class="button secondary" id="inspection-break-cancel">Cancel</button><p id="inspection-break-dialog-error" role="alert"></p></dialog><script defer src="/public/inspection-breaks.js?v=2"></script>
<dialog id="lunch-inspection-dialog" class="lunch-inspection-dialog" data-lunch-dialog data-csrf="${e(csrf)}" aria-labelledby="lunch-dialog-title"><header class="lunch-dialog-header"><h3 id="lunch-dialog-title">Lunch Inspection</h3><button type="button" class="lunch-dialog-close" data-lunch-close aria-label="Close lunch inspection">&times;</button></header><div data-lunch-content></div></dialog>`;
}
export function registerInspectionBreakRoutes(app, bridge) {
  const station = (req,res,next) => {
    res.set('Cache-Control','no-store');
    if(!req.session?.inspector || !req.session.sessionRecord?.id) return res.status(401).json({error:'An active inspector station session is required.'});
    next();
  };
  app.get('/workflow/break/state',station,async(req,res)=>{
    try {res.json(await bridge('inspection_break_state',{session_id:req.session.sessionRecord.id}));}
    catch(err){res.status(409).json({error:err.message});}
  });
  for(const action of ['start','end']) app.post(`/workflow/break/${action}`,station,async(req,res)=>{
    try {
      const payload={session_id:req.session.sessionRecord.id,expected_version:req.body.expected_version};
      if(payload.expected_version===undefined) throw new Error('Refresh the break status before continuing.');
      if(action==='start') Object.assign(payload,{mode:req.body.mode,relief_adp:req.body.relief_adp});
      res.json(await bridge(action==='start'?'start_inspection_break':'end_inspection_break',payload));
    } catch(err){res.status(409).json({error:err.message});}
  });
  // Every production request checks persistent station state, including requests from older tabs.
  app.use(['/workflow/inspection','/workflow/start','/workflow/scope','/workflow/complete','/workflow/live-draft','/workflow/lunch/open'],async(req,res,next)=>{
    if(!req.session?.sessionRecord?.id) return next();
    try {
      const state=await bridge('inspection_break_state',{session_id:req.session.sessionRecord.id});
      req.breakState=state;
      if(req.method==='POST' && !req.path.startsWith('/workflow/lunch')) {
        const version=req.body.coverage_version ?? req.body._coverage_version;
        if(state.mode==='without_relief' || (Number(version ?? 0)!==Number(state.version))) {
          const error=state.mode==='without_relief'?'Inspection is paused. Click End Break before continuing.':'Break coverage changed. Review the current inspector before continuing.';
          return res.status(409).send(error);
        }
      }
      next();
    } catch(err){next(err);}
  });
}
