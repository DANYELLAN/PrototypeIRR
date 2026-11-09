const escape = value => String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[char]);
export const operatorTimeTabs = [
  {key:'home',href:'/dashboard',label:'Machine Overview'},
  {key:'time',href:'/time',label:'Time Entry'},
  {key:'checklist',href:'/checklist',label:'Daily Checklist'},
  {key:'notifications',href:'/notifications',label:'Notifications'},
  {key:'maintenance',href:'/maintenance',label:'Maintenance'},
  {key:'it',href:'/contact/it',label:'IT Support'},
];
export function timeNavigation(session, hasRole) {
  const items = !session?.employee || hasRole(session,'operator') ? [...operatorTimeTabs] : [];
  for (const [role,key,href,label] of [
    ['exporter','exports','/exports','Exports'],
    ['approver','admin','/admin','Admin Approvals'],
    ['it','click-diagnostics','/admin/click-diagnostics','Button Diagnostics'],
  ]) if(session?.employee && hasRole(session,role))items.push({key,href,label});
  return items;
}
export function renderTimeNavigation(items=operatorTimeTabs) {
  return items.map(item=>`<a class="${item.key==='time'?'active':''}" ${item.key==='time'?'aria-current="page"':''} href="${escape(item.href)}">${escape(item.label)}</a>`).join('');
}
export function activeDowntimeData(timer, values) {
  if(!timer || String(values.active_timer_id)!==String(timer.id))throw new Error('This timer changed. Reopen Log Downtime for the current entry.');
  return {...values,production_number:timer.fields.ProductionNo,operation_id:timer.fields.OperationID,timer_id:timer.id};
}
export function renderActiveTimerActions(timer, context) {
  const paused=timer.status==='Paused';
  const breakType=timer.breaks?.at(-1)?.type;
  const relieved=paused && breakType==='With Relief';
  const reasons=(context.downtime_reasons || []).map(item=>`<option value="${escape(item.code)}">${escape(item.label)}</option>`).join('');
  const downtime=`<details class="cnc-active-downtime"><summary>Log Downtime</summary><form id="active-downtime" data-action="misc" class="cnc-form two-col"><input type="hidden" name="active_timer_id" value="${escape(timer.id)}"><p class="full">For WO ${escape(timer.fields.ProductionNo)} · Operation ${escape(timer.fields.OperationID)}</p><label class="full"><span>Downtime Reason</span><select name="detail_type_ii" required><option value="">Select downtime reason…</option>${reasons}</select></label><label><span>Hours</span><input name="hours" type="number" min="0" max="16" step="1" value="0" required></label><label><span>Minutes</span><input name="minutes" type="number" min="0" max="59" step="1" value="5" required></label><label class="full"><span>Comments</span><textarea name="comments" rows="2"></textarea></label><button class="cnc-button ghost" type="submit">Submit Downtime</button></form></details>`;
  const breaks=paused?`<form data-action="resume" class="cnc-active-downtime lunch-action-tile"><button class="cnc-button ghost" type="submit">${relieved?'End Relieved Break':'Resume Production'}</button></form>${breakType==='No Relief'?'<form data-action="relief_arrived" class="cnc-active-downtime lunch-action-tile"><button class="cnc-button ghost" type="submit">Relief Arrived</button></form>':''}`:`<details class="cnc-active-downtime"><summary>Take Break</summary><form data-action="pause" class="cnc-form two-col"><label class="full"><span>Break Type</span><select name="break_type" required><option value="No Relief">Break With No Relief</option><option value="With Relief">Break With Relief</option></select></label><label class="full"><span>Comments</span><textarea name="comments" rows="2" placeholder="Relief operator name or break note"></textarea></label><button class="cnc-button ghost" type="submit">Start Break</button></form></details><form data-action="pause" class="cnc-active-downtime lunch-action-tile"><input type="hidden" name="break_type" value="Lunch"><button class="cnc-button ghost" type="submit">Lunch</button></form>`;
  return `<div class="cnc-inline-actions active-action-row">${downtime}${breaks}</div><div class="active-stop-and-timers"><form data-action="stop" class="cnc-form two-col tablet-stop-form stop-submit-row"><label><span>Quantity</span><input name="quantity" type="number" min="0" max="1000000" step="1" value="1" required></label><button class="cnc-button danger" type="submit">Stop And Submit</button></form><div id="timer-clock" class="active-time-totals" role="timer" aria-label="Production, break and downtime totals"></div></div>`;
}
