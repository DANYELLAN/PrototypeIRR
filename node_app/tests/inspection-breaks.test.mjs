import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import {registerInspectionBreakRoutes,renderBreakControls} from '../src/inspectionBreaks.js';
import {reliefColumnClass,reliefColumnLabel} from '../src/inspectionRelief.js';
import {renderPipeReport} from '../src/pipeReport.js';
test('break API uses the logged-in station, blocks paused/stale production requests and preserves login ownership',async()=>{
 const app=express();app.use(express.json());
 const session={inspector:{adp_number:'OWNER'},sessionRecord:{id:7},activeInspection:{attempt_id:99}};
 app.use((req,res,next)=>{req.session=req.headers['x-anonymous']?{}:session;next();});
 let state={version:0,mode:null,original_name:'Owner',acting_name:'Owner'};const calls=[];
 registerInspectionBreakRoutes(app,async(action,payload)=>{
  calls.push({action,payload});
  if(action==='inspection_break_state')return state;
  assert.equal(payload.session_id,7);
  if(Number(payload.expected_version)!==state.version)throw new Error('Coverage changed');
  state=action==='start_inspection_break'?{...state,version:2,mode:payload.mode}:{...state,version:3,mode:null};
  return state;
 });
 app.post('/workflow/complete',(req,res)=>res.json({saved:true,version:req.breakState.version}));
 app.post('/workflow/live-draft',(req,res)=>res.sendStatus(204));
 const server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));
 const base=`http://127.0.0.1:${server.address().port}`;
 const post=(path,body)=>fetch(base+path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
 try{
  assert.equal((await fetch(base+'/workflow/break/state',{headers:{'x-anonymous':'1'}})).status,401);
  assert.equal((await post('/workflow/break/start',{session_id:999,expected_version:0,mode:'without_relief'})).status,200);
  assert.equal((await post('/workflow/complete',{_coverage_version:2})).status,409);
  assert.equal((await post('/workflow/live-draft',{coverage_version:2})).status,409);
  assert.equal((await post('/workflow/break/end',{expected_version:0})).status,409);
  assert.equal((await post('/workflow/break/end',{expected_version:2})).status,200);
  assert.equal((await post('/workflow/complete',{_coverage_version:2})).status,409);
  assert.equal((await post('/workflow/complete',{_coverage_version:3})).status,200);
  assert.equal(session.inspector.adp_number,'OWNER');assert.equal(session.activeInspection.attempt_id,99);
 }finally{await new Promise(resolve=>server.close(resolve));}
});
test('relief pipe columns show the actor ADP and full borders; break control restores persistent state',()=>{
 const people=[{name:'Relief <Inspector>',adp_number:'R-123'}];
 assert.equal(reliefColumnClass(people),'relief-pipe-column');assert.equal(reliefColumnClass([]),'');
 assert.match(reliefColumnLabel(people),/Relief &lt;Inspector&gt; \(ADP R-123\)/);
 const html=renderPipeReport({stations:[],pipes:[{attempt_id:1,pipe_unit_id:1,pipe_number:'005',relief_inspectors:people,measurements:[{element_sequence:1,element_description:'Diameter',measured_value:1.234,pass_fail:'Pass'}],status:'passed'}],page:1,pages:1,total:1});
 assert.match(html,/<th class="inspection-col-history relief-pipe-column"/);
 assert.match(html,/<td class="inspection-history-cell[^\"]*relief-pipe-column/);
 assert.match(html,/<td class="inspection-history-result-cell relief-pipe-column"/);
 assert.match(renderBreakControls({version:2,mode:'without_relief'}),/>End Break<\/button>/);
});
