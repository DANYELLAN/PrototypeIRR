import assert from 'node:assert/strict';
import test from 'node:test';
import express from 'express';
import {registerMoldApprovalRoutes,renderMoldApproval} from '../src/moldApproval.js';

test('mold routes guard identity, station, polling and preserve callback tokens',async()=>{
 const app=express();app.use(express.json());app.use(express.urlencoded({extended:false}));
 app.use((req,res,next)=>{req.session=req.headers['x-anonymous']?{}:{inspector:{adp_number:'123'},sessionRecord:req.headers['x-no-station']?null:{id:5,location_name:'E-1'}};next();});
 const calls=[];
 registerMoldApprovalRoutes(app,async(action,payload)=>{
  calls.push({action,payload});
  if(action==='mold_approval_context')return {requests:[],work_orders:['WO'],watch_id:null};
  if(action==='request_mold_approval')return {id:7,submitted:true};
  if(action==='mold_approval_status')return {id:7,status:'do_not_run',responder_name:'Quality'};
  if(action==='mold_approval_decision')throw new Error('Invalid mold approval response token.');
  throw new Error(action);
 },(_,content)=>content);
 const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));
 const base=`http://127.0.0.1:${server.address().port}`;
 try {
  for(const url of ['/workflow/mold-approval','/workflow/mold-approval/status.json?id=7'])assert.equal((await fetch(base+url,{headers:{'x-anonymous':'1'}})).status,401);
  assert.equal((await fetch(base+'/workflow/mold-approval',{method:'POST',headers:{'x-no-station':'1'}})).status,403);
  assert.equal(calls.length,0);
  const sent=await fetch(base+'/workflow/mold-approval',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({work_order:'WO',inspector_adp:'forged',session_id:999}),redirect:'manual'});
  assert.equal(sent.status,302);assert.equal(sent.headers.get('location'),'/workflow/mold-approval?watch=7');
  assert.deepEqual(calls[0].payload,{inspector_adp:'123',session_id:5,work_order:'WO'});
  const status=await fetch(base+'/workflow/mold-approval/status.json?id=7');assert.equal(status.headers.get('cache-control'),'no-store');assert.equal((await status.json()).status_label,'Do Not Run');
  const denied=await fetch(base+'/api/notifications/7/decision',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:'bad',decision:'approved'})});assert.equal(denied.status,400);
 }finally{await new Promise(r=>server.close(r));}
});
test('mold form escapes values, shows queued/failed delivery and both Quality outcomes',()=>{
 for(const status of ['pending','approved_to_run','do_not_run']){
  const html=renderMoldApproval({work_orders:['<WO>'],watch_id:7,requests:[{id:7,status,delivery_status:'failed',work_order:'<WO>',responder_name:'<script>',response_note:'<note>'}]},{location_name:'E-1'},null);
  assert.ok(html.includes('&lt;WO&gt;'));assert.ok(!html.includes('<script>'));assert.ok(html.includes('Delivery failed'));
 }
 assert.ok(!renderMoldApproval({work_orders:[],requests:[]},null,null).includes('type="submit"'));
});
