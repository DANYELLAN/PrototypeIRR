import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {renderActiveTiming} from '../public/active-timing.js';
import {renderEntryCompletion} from '../public/entry-completion.js';
import {isSetup,defaultNextDetail} from '../public/setup-handoff.js';
const source=fs.readFileSync(new URL('../src/cncTimeServer.js',import.meta.url),'utf8');
const route=source.slice(source.indexOf('app.get("/time",'),source.indexOf('\nasync function setDailyReviewTarget',source.indexOf('app.get("/time",')));
const bundled='const {context,timer:earlierTabletTimer,review:dailyReview} = await callCncBridge("get_time_page_context", {session:sessionData,labor_date:req.session.dailyReviewDate});';
const original=route.replace(bundled,`const context=await dashboardContext(req);
 const earlierTabletTimer=await callCncBridge('tablet_time_active',{session:sessionData});
 const dailyReview=await callCncBridge('get_daily_time_review',{employee:sessionData.employee,labor_date:req.session.dailyReviewDate});`);

async function render(code){
 let handler,html;const calls=[];
 const context={work_orders:['WO1'],operations:[],details_step_two:[],downtime_reasons:[],recent_entries:[],active_entry:{sp_id:'entry',production_number:'WO1',operation_id:'10',details_type:'Machining',status:'In Progress',start:'2026-11-11T14:00:00Z'}};
 const timer={id:'offline'},review={required:false,entries:[]};
 vm.runInNewContext(code,{app:{get:(_path,_guard,fn)=>handler=fn},requireOperator:()=>{},
  dashboardContext:async()=>{calls.push('get_dashboard_context');return context;},
  callCncBridge:async action=>{calls.push(action);if(action==='get_time_page_context')return {context,timer,review};return action==='tablet_time_active'?timer:review;},
  preparedTabletMarkup:()=>'',hasRole:()=>false,renderActiveTiming,renderEntryCompletion,isSetup,defaultNextDetail,escapeHtml:value=>String(value??''),dailyTimeReviewMarkup:()=>'',optionList:()=>'',operationOptionList:()=>'',detailOptionList:()=>'',layout:config=>config.body,timeTable:()=>'<table>unchanged</table>'});
 await handler({session:{cncUser:{employee:{emp_id:'1'}},dailyReviewDate:'2026-11-10'}},{send:value=>html=value},error=>{throw error;});
 return {html,calls};
}

test('bundling cuts page backend launches from three to one with byte-identical rendered UI',async()=>{
 assert.notEqual(route,original,'comparison must replace the read bundle');
 const before=await render(original),after=await render(route);
 assert.deepEqual(before.calls,['get_dashboard_context','tablet_time_active','get_daily_time_review']);
 assert.deepEqual(after.calls,['get_time_page_context']);
 assert.equal(after.html,before.html);
});
