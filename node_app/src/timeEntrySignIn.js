import {employeeKey, handoffNonce, peerOrigin, validHandoffNonce} from '../../shared/inspectionHandoff.js';

const saveSession = session => new Promise((resolve,reject)=>session.save(error=>error?reject(error):resolve()));
const issuer = () => process.env.CNC_TIME_INTERNAL_ORIGIN || 'http://127.0.0.1:3100';
const escape = value => String(value ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#39;');
const timeCookie = req => String(req.headers.cookie || '').split(';').map(value=>value.trim()).find(value=>value.startsWith('benoit.time.sid='));

export async function endTimeEntrySignIn(req, {request = fetch} = {}) {
  if (req.session.signInSource!=='time-entry') return null;
  const cookie = timeCookie(req);
  const response = await request(new URL('/inspection/end',issuer()), {method:'POST',redirect:'error',signal:AbortSignal.timeout(5000),
    headers:{'Content-Type':'application/json',...(cookie?{Cookie:cookie}:{})},body:JSON.stringify({adp_number:req.session.inspector.adp_number})});
  if (!response.ok) throw Error('Please try signing out again.');
  return peerOrigin(req,'time');
}

export function registerTimeEntrySignIn(app, {callBridge, renderPage, request = fetch, now = Date.now}) {
  const show = (req,res,content,status=200)=>res.status(status).send(renderPage(req,`<section class="card machinist-setup">${content}</section>`));
  const fail = (req,res,error,status=409)=>show(req,res,`<h2>Continue Sign In</h2><p role="alert">${escape(error.message)}</p>${req.session.sessionRecord?'<a class="button" href="/workflow/inspection">Return to Current Inspection</a>':'<a class="button" href="/auth/time-entry/start">Continue</a>'}`,status);
  app.get('/auth/time-entry/start',async(req,res,next)=>{
    res.set({'Cache-Control':'no-store','Referrer-Policy':'no-referrer'});
    try {
      req.session.timeEntryHandoff={state:handoffNonce(),expires:now()+120000};
      await saveSession(req.session);
      const target = new URL('/inspection/authorize',peerOrigin(req,'time'));
      target.searchParams.set('state',req.session.timeEntryHandoff.state);
      res.redirect(target.href);
    } catch(error) { next(error); }
  });
  app.get('/auth/time-entry/callback',async(req,res)=>{
    res.set({'Cache-Control':'no-store','Referrer-Policy':'no-referrer'});
    const pending = req.session.timeEntryHandoff;
    if (!pending || pending.expires<=now() || pending.state!==req.query.state || !validHandoffNonce(req.query.code))
      return fail(req,res,Error('Please continue from sign in again.'));
    delete req.session.timeEntryHandoff;
    try {
      const response=await request(new URL('/inspection/redeem',issuer()),{method:'POST',redirect:'error',signal:AbortSignal.timeout(5000),
        headers:{'Content-Type':'application/json'},body:JSON.stringify({code:req.query.code,state:pending.state})});
      if (!response.ok) throw Error('Please continue from sign in again.');
      const claims=await response.json();
      const setup=await callBridge('prepare_time_entry_inspection',{params:claims});
      if (req.session.sessionRecord && employeeKey(req.session.inspector?.adp_number)!==employeeKey(setup.inspector.adp_number))
        throw Error('This tablet still has another inspector’s session open. End that inspection session before continuing.');
      if (req.session.sessionRecord && String(req.session.sessionRecord.location_id)!==String(setup.location.id))
        throw Error('End the current inspection session before continuing at a different machine.');
      const retained = req.session.sessionRecord ? {...req.session} : {themeMode:req.session.themeMode};
      delete retained.cookie;delete retained.timeEntryHandoff;delete retained.timeEntrySetup;
      await new Promise((resolve,reject)=>req.session.regenerate(error=>error?reject(error):resolve()));
      Object.assign(req.session,retained);
      req.session.timeEntrySetup={claims,setup,csrf:handoffNonce(),expires:now()+15*60*1000};
      await saveSession(req.session);
      res.redirect('/setup/machinist');
    } catch(error) { fail(req,res,error); }
  });
  app.get('/setup/machinist',async(req,res)=>{
    res.set('Cache-Control','no-store');
    const pending=req.session.timeEntrySetup;
    if (!pending || pending.expires<=now()) return res.redirect('/auth/time-entry/start');
    try {
      const operators=await callBridge('get_cnc_operators',{branch:pending.setup.inspector.branch});
      const {inspector,location,shift}=pending.setup;
      show(req,res,`<h2 class="section-title">Choose Machinist</h2><p><strong>${escape(inspector.name)}</strong> · ${escape(location.location_name)} · ${escape(shift)} Shift</p>
        ${pending.error?`<p role="alert" class="notice warning">${escape(pending.error)}</p>`:''}
        <form method="post" action="/setup/machinist" class="form-grid" data-existing-session="${escape(pending.setup.existing_session?.id)}" data-existing-machinist="${escape(pending.setup.existing_session?.cnc_operator_item_id)}"><input type="hidden" name="csrf" value="${escape(pending.csrf)}" />
        <div class="field"><label for="handoff-machinist">Machinist</label><select id="handoff-machinist" name="operator_item_id" required><option value="">Choose machinist…</option>${operators.map(operator=>`<option value="${escape(operator.item_id)}">${escape(operator.name)}</option>`).join('')}</select></div>
        <div class="actions"><button class="button" type="submit">Continue</button></div></form>`);
    } catch(error) { fail(req,res,error); }
  });
  app.post('/setup/machinist',async(req,res)=>{
    res.set('Cache-Control','no-store');
    const pending=req.session.timeEntrySetup;
    if (!pending || pending.expires<=now() || req.body.csrf!==pending.csrf) return fail(req,res,Error('Please continue from sign in again.'),403);
    try {
      const cookie=timeCookie(req);
      const current=await request(new URL('/inspection/validate',issuer()),{method:'POST',redirect:'error',signal:AbortSignal.timeout(5000),
        headers:{'Content-Type':'application/json',...(cookie?{Cookie:cookie}:{})},body:JSON.stringify({adp_number:pending.claims.adp_number})});
      if (!current.ok || !(await current.json()).active) {
        delete req.session.timeEntrySetup;
        return fail(req,res,Error('The sign-in changed. Continue from sign in again.'),403);
      }
      const result=await callBridge('start_time_entry_inspection',{params:{...pending.claims,operator_item_id:req.body.operator_item_id}});
      const sameSession=String(req.session.sessionRecord?.id)===String(result.session.id);
      req.session.inspector=result.inspector;
      req.session.sessionRecord=result.session;
      req.session.sessionShift=result.shift;
      req.session.roleLabel=result.is_manager?'Manager/Supervisor':result.is_admin?'Admin Access Only':'Inspector';
      req.session.canAccessAdmin=result.is_admin;
      req.session.signInSource='time-entry';
      req.session.pendingLoginContext=null;
      if (!sameSession) { req.session.activeInspection=null;req.session.selection=null; }
      delete req.session.timeEntrySetup;
      await saveSession(req.session);
      res.redirect('/workflow/inspection');
    } catch(error) {
      pending.error=error.message;
      res.redirect('/setup/machinist');
    }
  });
}
