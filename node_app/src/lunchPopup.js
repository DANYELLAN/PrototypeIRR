import { escapeReliefLabel as e } from './inspectionRelief.js';
const base='/workflow/lunch/popup';
export function lunchIdentityForm(){return `<p>Enter the Quality admin or IT ADP number to inspect this entry’s latest completed pipe.</p><form class="lunch-popup-identity" data-lunch-identify><label>Reviewer ADP Number<input name="reviewer_adp" required autocomplete="off" inputmode="numeric"></label><button class="button" type="submit">Load Lunch Inspection</button></form>`;}
export function renderPopupLunch(row,values={}){
 const pipe=row.source_snapshot;
 const original=new Map((pipe.measurements || []).map(item=>[String(item.element_sequence),item]));
 return `<p><strong>Pipe # ${e(pipe.pipe_number)}</strong> · WO ${e(pipe.production_number)} · ${e(pipe.location_name)}</p><p>${e(pipe.recipe_name)} · ${e(pipe.operation_description)}</p><p>Reviewer: <strong>${e(row.reviewer_name)}</strong> · ADP ${e(row.reviewer_adp)}</p><p>Complete every element in the lunch column. Production measurements remain in their original column.</p><form data-lunch-complete class="lunch-popup-save"><input type="hidden" name="inspection_id" value="${Number(row.id)}"><div class="lunch-popup-table-wrap"><table class="lunch-popup-table"><thead><tr><th>#</th><th>Element</th><th>DWG DIM</th><th>Gauge</th><th>Production pipe # ${e(pipe.pipe_number)}<br>${e(pipe.inspector_name)}</th><th>Lunch Inspection<br>${e(row.reviewer_name)}</th></tr></thead><tbody>${row.recipe_snapshot.map(element=>{
 const key=`measurement_${element.element_sequence}`;
 const measurement=original.get(String(element.element_sequence));
 const label=e(element.element_description || `Element ${element.element_sequence}`);
 const input=element.capture_type==='boolean'?`<select aria-label="${label}" name="${key}" required><option value="">Select Pass / Fail</option><option value="Yes" ${values[key]==='Yes'?'selected':''}>Pass</option><option value="No" ${values[key]==='No'?'selected':''}>Fail</option></select>`:`<input aria-label="${label}" name="${key}" ${element.capture_type==='numeric'?`type="text" inputmode="decimal" data-measurement-offset data-nominal="${e(element.nominal ?? '')}" placeholder="Full value or +/- offset"`:'type="text"'} required value="${e(values[key] ?? '')}">`;
 return `<tr><td>${e(element.element_sequence)}</td><td>${label}</td><td>${e(element.dwg_dim)}</td><td>${e(element.gauge)}</td><td class="lunch-reference-value">${measurement?e(measurement.measured_value):'Not inspected'}</td><td>${input}</td></tr>`;
 }).join('')}</tbody></table></div><label>Notes<textarea name="notes" rows="2">${e(values.notes || '')}</textarea></label><button class="button" type="submit">Submit Lunch Inspection</button></form>`;
}
export function registerLunchPopupRoutes(app,bridge,connectionLabel){
 const guard=(req,res,next)=>{
  res.set('Cache-Control','no-store');
  if(!req.session?.inspector || !req.session.sessionRecord?.id)return res.status(401).json({error:'Sign in at an inspection station first.'});next();
 };
 const identity=req=>({session_id:req.session.sessionRecord.id,inspector_adp:req.session.inspector.adp_number});
 const saved=req=>({...identity(req),inspection_id:req.session.lunchPopup.id,reviewer_adp:req.session.lunchPopup.adp});
 app.get(base,guard,async(req,res)=>{
  try{
   if(req.session.lunchPopup){
    const row=await bridge('get_popup_lunch',saved(req));
    if(row.status==='in_progress')return res.json({view:'worksheet',html:renderPopupLunch(row)});
    req.session.lunchPopup=null;
   }
   res.json({view:'identity',html:lunchIdentityForm()});
  }catch(err){res.status(400).json({error:err.message});}
 });
 app.post(`${base}/start`,guard,async(req,res)=>{
  try{
   const adp=String(req.body.reviewer_adp || '').trim();
   if(!adp)throw new Error('Enter the reviewer ADP number.');
   if(req.session.lunchPopup){
    const row=await bridge('get_popup_lunch',saved(req));
    if(row.status==='in_progress'){
     if(String(row.reviewer_adp)!==adp)throw new Error('An unfinished lunch inspection is already assigned to another reviewer. Finish that inspection first.');
     return res.json({view:'worksheet',html:renderPopupLunch(row)});
    }
   }
   const selection=req.session.selection || {};
   const row=await bridge('start_lunch_popup',{...identity(req),reviewer_adp:adp,production_number:selection.productionNumber || '',operation_description:connectionLabel(selection),active_attempt_id:req.session.activeInspection?.attempt_id || null,reference_mode:'completed'});
   req.session.lunchPopup={id:row.id,adp:row.reviewer_adp};
   res.json({view:'worksheet',html:renderPopupLunch(row)});
  }catch(err){res.status(400).json({error:err.message});}
 });
 app.post(`${base}/complete`,guard,async(req,res)=>{
  try{
   if(!req.session.lunchPopup || String(req.body.inspection_id)!==String(req.session.lunchPopup.id))return res.status(403).json({error:'This lunch inspection is not open in this station session.'});
   const row=await bridge('get_popup_lunch',saved(req));
   const values=Object.fromEntries(row.recipe_snapshot.map(element=>[String(element.element_sequence),req.body[`measurement_${element.element_sequence}`] ?? '']));
   const result=await bridge('complete_lunch_popup',{...saved(req),values,notes:req.body.notes || ''});
   res.json({view:'saved',html:`<h3>Lunch Inspection Saved</h3><p>Pipe # ${e(result.confirmed_pipe_number)} · <strong>${e(result.status)}</strong> · ${e(result.reviewer_name)}</p><p>Saved in Lunch Inspections History.</p><button type="button" class="button" data-lunch-close>Return to Inspection Entry</button>`});
  }catch(err){res.status(400).json({error:err.message});}
 });
}
