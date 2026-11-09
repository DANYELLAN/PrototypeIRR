import assert from 'node:assert/strict';
import test from 'node:test';
import express from 'express';
import {registerLunchPopupRoutes,renderPopupLunch} from '../src/lunchPopup.js';
const row={id:7,reviewer_adp:'QUALITY',reviewer_name:'Reviewer',status:'in_progress',source_snapshot:{pipe_number:'007',production_number:'WO',inspector_name:'Inspector',measurements:[{element_sequence:1,measured_value:'1.234'}]},recipe_snapshot:[{element_sequence:1,element_description:'Diameter <test>',capture_type:'numeric'},{element_sequence:2,element_description:'Visual',capture_type:'boolean'},{element_sequence:3,element_description:'Rotating',capture_type:'numeric'}]};
test('popup reference column is read only and every lunch element starts blank',()=>{
 const html=renderPopupLunch(row);
 assert.match(html,/Production pipe # 007/);assert.match(html,/1.234/);assert.match(html,/Diameter &lt;test&gt;/);
 for(const key of [1,3])assert.match(html,new RegExp(`name="measurement_${key}"[^>]*required value=""`));
 assert.match(html,/name="measurement_2" required><option value="">Select Pass/);
 assert.doesNotMatch(html,/selected|checked/);
});
test('popup binds source and reviewer to station session, resumes safely and saves separately',async()=>{
 const app=express();app.use(express.urlencoded({extended:false}));
 const state={inspector:{adp_number:'INSPECTOR'},sessionRecord:{id:5},selection:{productionNumber:'WO',operationDescription:'Raw description'},activeInspection:{attempt_id:9}};
 app.use((req,res,next)=>{req.session=req.headers['x-anonymous']?{}:state;next();});
 let saved=false;const calls=[];
 registerLunchPopupRoutes(app,async(action,payload)=>{
  calls.push({action,payload});
  if(action==='start_lunch_popup')return row;
  if(action==='get_popup_lunch')return {...row,status:saved?'failed':'in_progress'};
  if(action==='complete_lunch_popup'){saved=true;return {...row,status:'failed',confirmed_pipe_number:'007'};}
  throw new Error(action);
 },()=> '2.875 7.90# BEN-HT6 BOX');
 const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));
 const base=`http://127.0.0.1:${server.address().port}/workflow/lunch/popup`;
 const post=(path,data)=>fetch(base+path,{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams(data)});
 try{
  assert.equal((await fetch(base,{headers:{'x-anonymous':'1'}})).status,401);
  assert.equal((await fetch(base)).headers.get('cache-control'),'no-store');
  assert.equal((await post('/complete',{inspection_id:7})).status,403);
  assert.equal((await post('/start',{reviewer_adp:'QUALITY',session_id:999,active_attempt_id:999,production_number:'FORGED'})).status,200);
  assert.deepEqual(calls.find(c=>c.action==='start_lunch_popup').payload,{session_id:5,inspector_adp:'INSPECTOR',reviewer_adp:'QUALITY',production_number:'WO',operation_description:'2.875 7.90# BEN-HT6 BOX',active_attempt_id:9,reference_mode:'completed'});
  assert.equal((await post('/start',{reviewer_adp:'OTHER'})).status,400);
  assert.equal(calls.filter(c=>c.action==='start_lunch_popup').length,1);
  assert.equal((await post('/complete',{inspection_id:999})).status,403);
  assert.equal((await post('/complete',{inspection_id:7,reviewer_adp:'FORGED',measurement_1:'1.234',measurement_2:'No',measurement_3:'0'})).status,200);
  const payload=calls.find(c=>c.action==='complete_lunch_popup').payload;
  assert.equal(payload.reviewer_adp,'QUALITY');assert.deepEqual(payload.values,{'1':'1.234','2':'No','3':'0'});
  assert.equal(state.inspector.adp_number,'INSPECTOR');assert.equal(state.activeInspection.attempt_id,9);
  assert.equal((await (await fetch(base)).json()).view,'identity');
 }finally{await new Promise(r=>server.close(r));}
});
