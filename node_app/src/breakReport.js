import {canViewPipeReport} from './inspectionPermissions.js';
import {escapeReliefLabel as e} from './inspectionRelief.js';
const time=value=>value?`<time datetime="${e(value)}">${e(new Date(value).toLocaleString('en-US',{timeZone:'America/Chicago'}))}</time>`:'Ongoing';
export function breakDuration(seconds){const value=Math.max(0,Math.floor(Number(seconds)||0));return `${Math.floor(value/3600)}h ${String(Math.floor(value%3600/60)).padStart(2,'0')}m ${String(value%60).padStart(2,'0')}s`;}
export function renderAdminTabs(active='breaks',showBreaks=true){return `<nav class="workflow-nav card" aria-label="Admin tabs"><div class="workflow-nav-links"><a class="workflow-tab ${active==='tools'?'active':''}" href="/admin">Admin Tools</a>${showBreaks?`<a class="workflow-tab ${active==='breaks'?'active':''}" href="/admin/breaks">Breaks</a>`:''}</div></nav>`;}
export function renderBreakReport(data,filters={}){
 const url=page=>{const query=new URLSearchParams({...filters,page});return `/admin/breaks?${query}`;};
 return `<section class="card" id="admin-break-report"><h2>Break Times and Relief Coverage</h2><p>${data.total} breaks · ${data.active} active · Page ${data.page} of ${data.pages}</p><p>Times shown in Central Time. Relief coverage runs from the recorded break start through its end.</p><div class="table-wrap" data-break-table tabindex="0"><table><thead><tr><th>Inspector Taking Break</th><th>Station / Branch / Shift</th><th>Break Type</th><th>Break Start</th><th>Break End</th><th>Duration</th><th>Relief Inspector</th><th>Relief Start</th><th>Relief End</th><th>Status</th></tr></thead><tbody>${data.breaks.map(row=>`<tr><td><strong>${e(row.inspector_name)}</strong><br>ADP ${e(row.inspector_adp)}</td><td>${e(row.location_name || 'Unassigned')}<br>${e(row.branch)} · ${e(row.shift)}</td><td>${row.mode==='with_relief'?'With relief':'Without relief'}</td><td>${time(row.started_at)}</td><td>${time(row.ended_at)}</td><td ${!row.ended_at?`data-active-duration="${Number(row.duration_seconds)||0}"`:''}>${breakDuration(row.duration_seconds)}</td><td>${row.mode==='with_relief'?`<strong>${e(row.relief_name)}</strong><br>ADP ${e(row.relief_adp)}`:'No relief'}</td><td>${row.mode==='with_relief'?time(row.started_at):'—'}</td><td>${row.mode==='with_relief'?time(row.ended_at):'—'}</td><td>${row.ended_at?'Completed':'Active'}</td></tr>`).join('') || '<tr><td colspan="10">No breaks match these filters.</td></tr>'}</tbody></table></div><nav class="pipe-report-pagination" aria-label="Break report pages">${data.page>1?`<a class="button secondary" href="${e(url(data.page-1))}">← Previous 20</a>`:'<span>← Previous</span>'}<span>Page ${data.page} of ${data.pages}</span>${data.page<data.pages?`<a class="button" href="${e(url(data.page+1))}">Next 20 →</a>`:'<span>Next →</span>'}</nav></section>`;
}
export function registerBreakReportRoutes(app,bridge,layout){
 const guard=async(req,res,next)=>{
  res.set('Cache-Control','no-store');
  if(!req.session?.inspector)return res.status(401).send('Sign in to view break reports.');
  try{
   const employee=await bridge('get_employee_by_adp',{adp_number:req.session.inspector.adp_number});
   if(!canViewPipeReport(employee))return res.status(403).send('Break reports are available only to Quality admins and IT.');
   req.breakReportReviewer=employee;next();
  }catch(err){next(err);}
 };
 const filters=req=>Object.fromEntries(['station_id','date_from','date_to','employee','status'].map(key=>[key,String(req.query[key]|| (key==='status'?'all':''))]));
 const data=(req,selected)=>bridge('get_break_report',{...selected,page:req.query.page||1,reviewer_adp:req.breakReportReviewer.adp_number});
 app.get('/admin/breaks',guard,async(req,res,next)=>{
  try{
   const selected=filters(req),report=await data(req,selected),locations=await bridge('get_locations');
   const content=`${renderAdminTabs()}<section class="card"><h1>Breaks</h1><form method="get" id="break-report-filters" class="form-grid two"><div class="field"><label>Station<select name="station_id"><option value="">All stations</option>${locations.map(loc=>`<option value="${loc.id}" ${String(loc.id)===selected.station_id?'selected':''}>${e(loc.location_name)}</option>`).join('')}</select></label></div><div class="field"><label>Break start date from<input type="date" name="date_from" value="${e(selected.date_from)}"></label></div><div class="field"><label>Through<input type="date" name="date_to" value="${e(selected.date_to)}"></label></div><div class="field"><label>Inspector or relief name / ADP<input name="employee" value="${e(selected.employee)}"></label></div><div class="field"><label>Status<select name="status">${['all','active','completed'].map(status=>`<option value="${status}" ${status===selected.status?'selected':''}>${status[0].toUpperCase()+status.slice(1)}</option>`).join('')}</select></label></div><div class="actions"><button class="button" type="submit">Apply Filters</button><a class="button secondary" href="/admin/breaks">Clear</a></div></form><p id="break-report-freshness" role="status">Live refresh every 15 seconds; active durations update every second.</p><p id="break-report-error" role="alert"></p></section>${renderBreakReport(report,selected)}<script defer src="/public/admin-break-report.js?v=1"></script>`;
   res.send(layout(req,content));
  }catch(err){next(err);}
 });
 app.get('/admin/breaks/live.json',guard,async(req,res)=>{
  try{const selected=filters(req),report=await data(req,selected);res.json({html:renderBreakReport(report,selected),as_of:report.as_of});}
  catch(err){res.status(400).json({error:err.message});}
 });
}
