import {createHandoffStore, employeeKey, isLoopback, peerOrigin, validHandoffNonce} from '../../shared/inspectionHandoff.js';

const savedSession = (store, id) => new Promise((resolve,reject)=>store.get(id,(error,value)=>error?reject(error):resolve(value)));

export function registerInspectionSignIn(app, {store = createHandoffStore()} = {}) {
  app.get('/api/inspection-sign-in', (_req,res)=>res.set('Cache-Control','no-store').json({enabled:true}));
  app.get('/inspection/open', (req,res,next)=>{
    if (!req.session?.cncUser?.employee) return res.redirect('/');
    try { res.redirect(peerOrigin(req,'inspection')+'/auth/time-entry/start'); } catch(error) { next(error); }
  });
  app.get('/inspection/authorize', (req,res,next)=>{
    res.set({'Cache-Control':'no-store','Referrer-Policy':'no-referrer'});
    if (!req.session?.cncUser?.employee) return res.redirect('/');
    if (!validHandoffNonce(req.query.state)) return res.status(400).send('Sign in again to continue.');
    try {
      const callback = new URL('/auth/time-entry/callback', peerOrigin(req,'inspection'));
      callback.searchParams.set('code',store.issue(req.sessionID,req.session.cncUser,req.query.state));
      callback.searchParams.set('state',req.query.state);
      res.redirect(callback.href);
    } catch(error) { next(error); }
  });
  app.post('/inspection/redeem', async(req,res)=>{
    res.set('Cache-Control','no-store');
    if (!isLoopback(req)) return res.sendStatus(403);
    if (!validHandoffNonce(req.body.code) || !validHandoffNonce(req.body.state)) return res.sendStatus(400);
    const grant = store.take(req.body.code,req.body.state);
    if (!grant) return res.status(401).json({error:'Sign in again to continue.'});
    try {
      const session = await savedSession(req.sessionStore,grant.sessionId);
      if (!session?.cncUser?.employee || employeeKey(session.cncUser.employee.emp_id)!==grant.employee)
        return res.status(401).json({error:'Sign in again to continue.'});
      res.json(grant.claims);
    } catch { res.status(503).json({error:'Sign in is temporarily unavailable.'}); }
  });
  app.post('/inspection/end', (req,res)=>{
    res.set('Cache-Control','no-store');
    if (!isLoopback(req)) return res.sendStatus(403);
    if (employeeKey(req.session?.cncUser?.employee?.emp_id)!==employeeKey(req.body.adp_number)) return res.json({ended:false});
    req.session.destroy(error=>error?res.sendStatus(503):res.json({ended:true}));
  });
  app.post('/inspection/validate', (req,res)=>{
    res.set('Cache-Control','no-store');
    if (!isLoopback(req)) return res.sendStatus(403);
    res.json({active:Boolean(req.session?.cncUser?.employee &&
      employeeKey(req.session.cncUser.employee.emp_id)===employeeKey(req.body.adp_number))});
  });
}
