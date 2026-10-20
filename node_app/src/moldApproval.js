import { escapeReliefLabel as e } from './inspectionRelief.js';
export const moldStatusLabel = status => ({pending:'Waiting for Quality',approved_to_run:'Approved to Run',do_not_run:'Do Not Run'}[status] || 'Waiting for Quality');
const responseText = row => row.refresh_error || [row.responder_name, row.response_note, row.responded_at].filter(Boolean).join(' · ');
const deliveryText = row => row.delivery_status === 'failed' ? 'Delivery failed. Contact IT before requesting again.' : row.delivery_status === 'queued' ? 'Queued; has not been sent to Quality.' : '';
export function renderMoldApproval(context, station, notice) {
 const watched = context.requests.find(row => row.id === context.watch_id);
 return `${notice ? `<div class="notice ${e(notice.kind)}" role="status">${e(notice.message)}</div>` : ''}
 <section class="card"><h2>Mold Approval</h2><p>Request Quality approval for a work order and wait for the response before running.</p>
 ${station ? `<p>Station: <strong>${e(station.location_name)}</strong></p><form method="post" action="/workflow/mold-approval" class="form-grid"><div class="field"><label for="mold-work-order">Work Order</label><select id="mold-work-order" name="work_order" required><option value="">Select work order</option>${context.work_orders.map(wo=>`<option value="${e(wo)}">${e(wo)}</option>`).join('')}</select></div><div class="actions"><button class="button" type="submit">Request Mold Approval</button></div></form>` : '<p>Start an inspection station session to request mold approval.</p>'}
 </section>
 ${watched ? `<section class="card" data-mold-watch="${Number(watched.id)}" data-mold-state="${e(watched.status)}"><h3>Work Order ${e(watched.work_order)}</h3><p><strong data-mold-status role="status">${moldStatusLabel(watched.status)}</strong></p><p>${e(deliveryText(watched))}</p><p data-mold-response>${e(responseText(watched))}</p></section>` : ''}
 <section class="card"><h3>Recent Mold Approval Requests</h3><div class="table-wrap"><table><thead><tr><th>Requested</th><th>Station</th><th>Work Order</th><th>Status</th><th>Quality Response</th></tr></thead><tbody>${context.requests.map(row=>`<tr><td>${e(row.created_at)}</td><td>${e(row.machine_no)}</td><td><a href="/workflow/mold-approval?watch=${Number(row.id)}">${e(row.work_order)}</a></td><td data-mold-row-status="${Number(row.id)}">${moldStatusLabel(row.status)}${deliveryText(row) ? `<br>${e(deliveryText(row))}` : ''}</td><td data-mold-row-response="${Number(row.id)}">${e(responseText(row))}</td></tr>`).join('') || '<tr><td colspan="5">No mold approval requests yet.</td></tr>'}</tbody></table></div></section><script defer src="/public/mold-approval.js?v=1"></script>`;
}
export function registerMoldApprovalRoutes(app, bridge, layout) {
 const guard = (req,res,next) => {res.set('Cache-Control','no-store');if(!req.session?.inspector) return res.status(401).send('Sign in to view mold approvals.');next();};
 app.get('/workflow/mold-approval',guard,async(req,res,next)=>{
  try {const context=await bridge('mold_approval_context',{inspector_adp:req.session.inspector.adp_number,request_id:req.query.watch || null});
   const notice=req.session.notice;req.session.notice=null;
   res.send(layout(req,renderMoldApproval(context,req.session.sessionRecord,notice)));
  } catch(err){next(err);}
 });
 app.post('/workflow/mold-approval',guard,async(req,res)=>{
  if(!req.session.sessionRecord?.id) return res.status(403).send('Start an inspection station session first.');
  try {const result=await bridge('request_mold_approval',{inspector_adp:req.session.inspector.adp_number,session_id:req.session.sessionRecord.id,work_order:req.body.work_order});
   req.session.notice={kind:result.submitted || result.delivered ? 'success':'info',message:result.submitted?'Mold approval submitted to the Teams notification flow.':result.delivered?'Mold approval sent to Teams.':'Mold approval saved and queued; has not been sent to Quality.'};
   return res.redirect(`/workflow/mold-approval?watch=${encodeURIComponent(result.id)}`);
  } catch(err){req.session.notice={kind:'warning',message:err.message};return res.redirect('/workflow/mold-approval');}
 });
 app.get('/workflow/mold-approval/status.json',guard,async(req,res)=>{
  try {const row=await bridge('mold_approval_status',{inspector_adp:req.session.inspector.adp_number,request_id:req.query.id});
   res.json({id:row.id,status:row.status,status_label:moldStatusLabel(row.status),responder_name:row.responder_name || '',response_note:row.response_note || '',responded_at:row.responded_at || '',refresh_error:row.refresh_error || ''});
  }catch(err){res.status(400).json({error:err.message});}
 });
 app.post('/api/notifications/:id/decision',async(req,res)=>{
  try {const row=await bridge('mold_approval_decision',{request_id:req.params.id,decision_token:req.body.decision_token || req.body.token || req.get('x-notification-token'),decision:req.body.decision,responder_name:req.body.responder_name || req.body.responder,response_note:req.body.response_note || req.body.note});res.json({ok:true,request:row});}
  catch(err){res.status(400).json({ok:false,error:err.message});}
 });
}
