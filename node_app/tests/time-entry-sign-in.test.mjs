import test from 'node:test';
import assert from 'node:assert/strict';
import express from '../node_modules/express/index.js';
import session from '../node_modules/express-session/index.js';
import {createHandoffStore, handoffNonce, isInspector, peerOrigin} from '../../shared/inspectionHandoff.js';
import {registerInspectionSignIn} from '../../time_entry_app/src/inspectionSignIn.js';
import {registerTimeEntrySignIn, endTimeEntrySignIn} from '../src/timeEntrySignIn.js';

test('inspection stations and inspector job titles automatically continue to inspection',()=>{
  assert.equal(isInspector({employee:{adp_job_title:'Thread Inspector'}}),true);
  assert.equal(isInspector({employee:{job_title:'Senior Quality Inspector'}}),true);
  for(const title of ['CNC Machinist','Quality Manager','IT Manager','Inspection Supervisor',''])
    assert.equal(isInspector({employee:{adp_job_title:title,full_name:'Inspector Example'}}),false,title);
  for(const station of ['Line 1- Inspection 1','Line 1- Inspection 2','Line 2 - Inspection 1','Line 2 - Inspection 2'])
    assert.equal(isInspector({employee:{adp_job_title:'Quality Specialist'},machine_no:station}),true,station);
  assert.equal(isInspector({employee:{adp_job_title:'CNC Machinist'},machine_no:'Line 1 - Sandblast 1'}),false);
});

test('handoff codes expire, are bound to state, and are consumed once',()=>{
  let clock=100;
  const store=createHandoffStore({now:()=>clock,ttl:60});
  const state=handoffNonce(),user={employee:{emp_id:'007'},machine_no:'2',shift_id:41};
  const code=store.issue('session-1',user,state);
  assert.equal(store.take(code,handoffNonce()),null);
  assert.equal(store.take(code,state).employee,'7');
  assert.equal(store.take(code,state),null);
  const expired=store.issue('session-1',user,state);clock=161;
  assert.equal(store.take(expired,state),null);
});

test('unconfigured host headers cannot redirect sign-in codes to another host',()=>{
  const previous=process.env.INSPECTION_PUBLIC_ORIGIN;delete process.env.INSPECTION_PUBLIC_ORIGIN;
  try {
    assert.throws(()=>peerOrigin({hostname:'attacker.invalid',protocol:'https'},'inspection'));
    assert.equal(peerOrigin({hostname:'127.0.0.1',protocol:'https'},'inspection'),'https://127.0.0.1:3444');
  } finally { if(previous===undefined)delete process.env.INSPECTION_PUBLIC_ORIGIN;else process.env.INSPECTION_PUBLIC_ORIGIN=previous; }
});

test('a changed Time Entry sign-in cannot submit a stale machinist selection',async()=>{
  const posts=new Map();let called=0,forwarded;
  registerTimeEntrySignIn({get(){},post(path,handler){posts.set(path,handler);}},{renderPage:(_req,content)=>content,
    callBridge:async()=>{called++;},request:async(_url,options)=>{forwarded=options;return {ok:true,json:async()=>({active:false})};}});
  const req={headers:{cookie:'benoit.time.sid=signed-time-cookie; unrelated=private'},body:{csrf:'test',operator_item_id:'5'},
    session:{timeEntrySetup:{csrf:'test',expires:Date.now()+60000,claims:{adp_number:'TEST-I'}}}};
  const res={set(){return this;},status(code){this.code=code;return this;},send(html){this.html=html;}};
  await posts.get('/setup/machinist')(req,res);
  assert.equal(res.code,403);assert.equal(called,0);assert.equal(req.session.timeEntrySetup,undefined);
  assert.equal(forwarded.headers.Cookie,'benoit.time.sid=signed-time-cookie');
});

test('handoff never replaces another inspector or a different active station',async()=>{
  const gets=new Map();
  registerTimeEntrySignIn({get(path,handler){gets.set(path,handler);},post(){}},{renderPage:(_req,content)=>content,
    callBridge:async()=>({inspector:{adp_number:'TEST-I'},location:{id:2}}),
    request:async()=>({ok:true,json:async()=>({adp_number:'TEST-I'})})});
  for(const [adp,location] of [['OTHER',2],['TEST-I',1]]){
    const state=handoffNonce(),active={attempt_id:11};
    const req={query:{code:handoffNonce(),state},session:{timeEntryHandoff:{state,expires:Date.now()+60000},
      inspector:{adp_number:adp},sessionRecord:{id:9,location_id:location},activeInspection:active}};
    const res={set(){return this;},status(code){this.code=code;return this;},send(html){this.html=html;}};
    await gets.get('/auth/time-entry/callback')(req,res);
    assert.equal(res.code,409);assert.equal(req.session.activeInspection,active);
    assert.equal(req.session.inspector.adp_number,adp);assert.match(res.html,/Return to Current Inspection/);
  }
});

test('actual HTTP handoff goes from Time Entry to machinist selection with identity and station intact',async()=>{
  const time=express(),inspection=express();
  for(const [app,name] of [[time,'benoit.time.sid'],[inspection,'benoit.inspection.sid']]){
    app.use(express.urlencoded({extended:false}));app.use(express.json());
    app.use(session({secret:'test-only-session-secret',name,resave:false,saveUninitialized:false}));
  }
  time.post('/login',(req,res)=>{
    req.session.cncUser={employee:{emp_id:'TEST-I',adp_job_title:req.body.title || 'Thread Inspector'},roles:['operator'],machine_no:req.body.machine_no || 'Line 1- Inspection 2',shift_id:41,shift_title:'Night Shift'};
    res.redirect(isInspector(req.session.cncUser) ? '/inspection/open' : '/dashboard');
  });
  time.post('/logout',(req,res)=>req.session.destroy(()=>res.redirect('/')));
  registerInspectionSignIn(time);
  const calls=[];
  const setup={inspector:{item_id:1,adp_number:'TEST-I',name:'Test Inspector',branch:'Ennis'},location:{id:2,location_name:'Machine 2'},shift:'Night'};
  registerTimeEntrySignIn(inspection,{renderPage:(_req,content)=>content,callBridge:async(action,payload)=>{
    calls.push({action,payload});
    if(action==='prepare_time_entry_inspection')return setup;
    if(action==='get_cnc_operators')return [{item_id:5,name:'Test Machinist'}];
    if(action==='start_time_entry_inspection')return {...setup,session:{id:9,location_name:'Machine 2',cnc_operator_name:'Test Machinist'},is_admin:false,is_manager:false};
    throw Error(action);
  }});
  inspection.get('/workflow/inspection',(req,res)=>res.json({inspector:req.session.inspector,station:req.session.sessionRecord,shift:req.session.sessionShift}));
  inspection.post('/logout',async(req,res)=>{const url=await endTimeEntrySignIn(req);req.session.destroy(()=>res.redirect(url));});
  const servers=[];
  const listen=app=>new Promise(resolve=>{const server=app.listen(0,'127.0.0.1',()=>{servers.push(server);resolve(`http://127.0.0.1:${server.address().port}`);});});
  const timeOrigin=await listen(time),inspectionOrigin=await listen(inspection);
  const settings={CNC_TIME_PUBLIC_ORIGIN:timeOrigin,INSPECTION_PUBLIC_ORIGIN:inspectionOrigin,CNC_TIME_INTERNAL_ORIGIN:timeOrigin};
  const previous=Object.fromEntries(Object.keys(settings).map(key=>[key,process.env[key]]));Object.assign(process.env,settings);
  const cookies=new Map();
  const send=async(url,options={},jar=cookies)=>{
    const response=await fetch(url,{...options,redirect:'manual',headers:{...options.headers,Cookie:[...jar].map(([key,value])=>`${key}=${value}`).join('; ')}});
    for(const cookie of response.headers.getSetCookie()) {const pair=cookie.split(';')[0],i=pair.indexOf('=');jar.set(pair.slice(0,i),pair.slice(i+1));}
    return response;
  };
  try {
    let response=await send(timeOrigin+'/login',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:'adp_number=TEST-I'});
    let url=new URL(response.headers.get('location'),timeOrigin).href;
    let callback;
    for(let index=0;index<8;index++){
      response=await send(url);
      if(url.includes('/auth/time-entry/callback'))callback=url;
      if(response.status===200)break;
      assert.equal(response.status,302,url);
      url=new URL(response.headers.get('location'),url).href;
    }
    const html=await response.text();
    assert.equal(new URL(url).pathname,'/setup/machinist');
    assert.match(html,/Choose Machinist/);assert.match(html,/Test Inspector/);assert.match(html,/Machine 2/);assert.match(html,/Night Shift/);
    assert.doesNotMatch(html,/name="adp_number"|Inspector Login|App Theme/);
    assert.deepEqual(calls[0].payload.params,{adp_number:'TEST-I',machine_no:'Line 1- Inspection 2',shift_title:'Night Shift',shift_id:41});
    const before=calls.length;
    const replay=await send(callback);assert.equal(replay.status,409);assert.equal(calls.length,before);
    const anotherBrowser=await send(callback,{},new Map());assert.equal(anotherBrowser.status,409);assert.equal(calls.length,before);
    const csrf=html.match(/name="csrf" value="([^"]+)"/)[1];
    const forged=await send(inspectionOrigin+'/setup/machinist',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:'csrf=forged&operator_item_id=5'});
    assert.equal(forged.status,403);
    response=await send(inspectionOrigin+'/setup/machinist',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams({csrf,operator_item_id:'5',adp_number:'FORGED',machine_no:'6',shift_id:'40'})});
    assert.equal(response.headers.get('location'),'/workflow/inspection');
    const saved=calls.find(row=>row.action==='start_time_entry_inspection').payload.params;
    assert.equal(saved.adp_number,'TEST-I');assert.equal(saved.machine_no,'Line 1- Inspection 2');assert.equal(saved.shift_id,41);assert.equal(saved.operator_item_id,'5');
    const signedIn=await (await send(inspectionOrigin+'/workflow/inspection')).json();assert.equal(signedIn.inspector.adp_number,'TEST-I');assert.equal(signedIn.shift,'Night');
    const ended=await send(inspectionOrigin+'/logout',{method:'POST'});assert.equal(ended.headers.get('location'),timeOrigin);
    assert.equal((await send(timeOrigin+'/inspection/open')).headers.get('location'),'/');
    const ordinary=await send(timeOrigin+'/login',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:'title=CNC+Machinist&machine_no=2'});
    assert.equal(ordinary.headers.get('location'),'/dashboard');
  } finally {
    for(const [key,value] of Object.entries(previous))if(value===undefined)delete process.env[key];else process.env[key]=value;
    await Promise.all(servers.map(server=>new Promise(resolve=>server.close(resolve))));
  }
});
