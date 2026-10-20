import assert from "node:assert/strict";
import test from "node:test";
import express from "express";
import {registerLunchInspectionRoutes,renderLunchWorksheet,renderLunchHistory} from "../src/lunchInspections.js";
const plan=[{element_sequence:1,element_description:'Diameter <test>',capture_type:'numeric',dwg_dim:'1.2–1.3',gauge:'Micrometer'},{element_sequence:2,element_description:'Visual',capture_type:'boolean'},{element_sequence:3,element_description:'Rotating check',capture_type:'numeric'}];
const row={id:7,reviewer_adp:'QUALITY',reviewer_name:'Reviewer',recipe_snapshot:plan,source_snapshot:{pipe_number:'005',production_number:'WO',recipe_name:'Recipe'},status:'in_progress'};
test('full blank lunch worksheet, escaped values, separate submitted history',()=>{
 const html=renderLunchWorksheet(row);
 assert.match(html,/Rotating check/);
 assert.match(html,/Diameter &lt;test&gt;/);
 assert.match(html,/name="measurement_1"[^>]*required value=""/);
 assert.doesNotMatch(html,/<input[^>]*\schecked[\s>]/);
 assert.match(html,/\/workflow\/lunch\/complete/);
 const history=renderLunchHistory({inspections:[{...row,status:'failed',confirmed_pipe_number:'005',measurements:[{element_description:'Visual',measured_value:'No',pass_fail:'Fail'}]}],total:1,page:1,pages:1});
 assert.match(history,/Lunch Inspections History/);
 assert.match(history,/Fail/);
});
test('station inspector can identify admin without changing inspector session; history and submit identities guarded',async()=>{
 const app=express();app.use(express.urlencoded({extended:false}));
 const state={inspector:{adp_number:'INSPECTOR'},sessionRecord:{id:5,location_id:2},activeInspection:{attempt_id:100}};
 app.use((req,res,next)=>{req.session=req.headers['x-anonymous']?{}:state;next();});
 const calls=[];
 registerLunchInspectionRoutes(app,async(action,payload)=>{
  calls.push({action,payload});
  if(action==='get_locations')return [{id:2,location_name:'Station'}];
  if(action==='get_employee_by_adp')return {adp_number:'INSPECTOR',adp_job_title_desc:'Inspector'};
  if(action==='lunch_target'){
   if(payload.reviewer_adp!=='QUALITY')throw new Error('Admin required');
   return {reviewer:{adp_number:'QUALITY',name:'Reviewer'},pipe:{attempt_id:9,pipe_number:'005'}};
  }
  if(action==='start_lunch')return row;
  if(action==='get_lunch')return row;
  if(action==='complete_lunch')return {...row,status:'passed',confirmed_pipe_number:'005'};
  throw new Error(action);
 },(_,content)=>content);
 const server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));
 const base=`http://127.0.0.1:${server.address().port}/workflow/lunch`;
 const post=(path,values)=>fetch(base+path,{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams(values),redirect:'manual'});
 try{
  assert.equal((await fetch(base,{headers:{'x-anonymous':'1'}})).status,401);
  assert.equal((await fetch(base+'/history')).status,403);
  assert.equal((await post('/identify',{reviewer_adp:'INSPECTOR',station_id:2})).status,400);
  assert.equal((await post('/identify',{reviewer_adp:'QUALITY',station_id:2})).status,200);
  assert.equal((await post('/start',{confirmed_pipe_number:'005',reviewer_adp:'FORGED'})).status,302);
  assert.equal(calls.find(c=>c.action==='start_lunch').payload.reviewer_adp,'QUALITY');
  assert.equal((await post('/complete',{inspection_id:999})).status,403);
  assert.equal((await post('/complete',{inspection_id:7,reviewer_adp:'FORGED',measurement_1:'1.234',measurement_3:'0'})).status,200);
  const submitted=calls.find(c=>c.action==='complete_lunch').payload;
  assert.equal(submitted.reviewer_adp,'QUALITY');assert.equal(submitted.values['2'],'');
  assert.equal(state.inspector.adp_number,'INSPECTOR');assert.equal(state.activeInspection.attempt_id,100);assert.equal(state.sessionRecord.id,5);
  assert.equal(state.lunchReviewerAdp,null);
 }finally{await new Promise(resolve=>server.close(resolve));}
});

