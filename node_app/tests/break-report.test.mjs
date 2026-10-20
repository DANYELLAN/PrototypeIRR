import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import {registerBreakReportRoutes,renderBreakReport,breakDuration,renderAdminTabs} from '../src/breakReport.js';
const report={total:2,active:1,page:1,pages:2,as_of:'2026-10-19T17:00:00Z',breaks:[{inspector_name:'Inspector <One>',inspector_adp:'I1',mode:'with_relief',relief_name:'Relief Person',relief_adp:'R1',started_at:'2026-10-19T16:30:00Z',ended_at:null,duration_seconds:1800},{inspector_name:'Inspector Two',inspector_adp:'I2',mode:'without_relief',started_at:'2026-10-19T16:00:00Z',ended_at:'2026-10-19T16:10:00Z',duration_seconds:600}]};
test('break table includes identities, timing, ongoing duration, no relief, filtered pagination and escaped text',()=>{
 const html=renderBreakReport(report,{employee:'R1',status:'active'});
 for(const text of ['Inspector &lt;One&gt;','ADP I1','Relief Person','ADP R1','Break Start','Break End','Relief Start','Relief End','No relief','Central Time','data-active-duration="1800"','0h 10m 00s','employee=R1'])assert.ok(html.includes(text),text);
 assert.equal(breakDuration(3661),'1h 01m 01s');assert.match(renderAdminTabs(),/>Breaks<\/a>/);
});
test('admin report page and live endpoint reject non-admins and use fresh directory identity with filters',async()=>{
 const app=express();app.use((req,res,next)=>{req.session=req.headers['x-actor']?{inspector:{adp_number:req.headers['x-actor']},canAccessAdmin:true}:{};next();});const calls=[];
 registerBreakReportRoutes(app,async(action,payload)=>{
  calls.push({action,payload});
  if(action==='get_employee_by_adp')return {adp_number:payload.adp_number,adp_job_title_desc:payload.adp_number==='Q'?'Quality Manager':'Inspector'};
  if(action==='get_break_report')return report;
  if(action==='get_locations')return [{id:1,location_name:'Test Station'}];
  throw new Error(action);
 },(_,content)=>content);
 const server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));const base=`http://127.0.0.1:${server.address().port}`;
 try{
  for(const path of ['/admin/breaks','/admin/breaks/live.json']){
   assert.equal((await fetch(base+path)).status,401);
   assert.equal((await fetch(base+path,{headers:{'x-actor':'I'}})).status,403);
  }
  assert.equal(calls.filter(c=>c.action==='get_break_report').length,0);
  const response=await fetch(base+'/admin/breaks/live.json?reviewer_adp=FORGED&employee=R1&status=active&page=2',{headers:{'x-actor':'Q'}});
  assert.equal(response.status,200);assert.match((await response.json()).html,/Relief Person/);
  const payload=calls.find(c=>c.action==='get_break_report').payload;
  assert.equal(payload.reviewer_adp,'Q');assert.equal(payload.employee,'R1');assert.equal(payload.status,'active');assert.equal(payload.page,'2');
  assert.equal((await fetch(base+'/admin/breaks',{headers:{'x-actor':'Q'}})).status,200);
 }finally{await new Promise(resolve=>server.close(resolve));}
});
