import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {entryComment} from '../public/entry-comments.js';
const source=fs.readFileSync(new URL('../src/cncTimeServer.js',import.meta.url),'utf8');

test('only automatic descriptions and bare downtime reasons are hidden',()=>{
 assert.equal(entryComment({tran_description:'THREAD BOX',operation_description:'THREAD BOX'}),'');
 assert.equal(entryComment({tran_description:'THREAD BOX',operation_description:'THREAD BOX',user_comments:'THREAD BOX'}),'THREAD BOX');
 assert.equal(entryComment({tran_description:'Old note',user_comments:''}),'');
 assert.equal(entryComment({tran_description:'Actual saved note'}),'Actual saved note');
 assert.equal(entryComment({tran_description:'M1',details_type_ii:'M1'}),'');
});
test('Recent Entries renders blank comments and routes active Edit to the active record',async()=>{
 const ctx={entryComment,escapeHtml:value=>String(value??'').replaceAll('"','&quot;'),optionList:()=>'',operationOptionList:()=>'',detailOptionList:()=>'',downtimeReasonOptionList:()=>''};
 vm.createContext(ctx);vm.runInContext(source.slice(source.indexOf('function timeTable('),source.indexOf('function renderLiveMetric(')),ctx);
 const html=ctx.timeTable([{sp_id:'1',entry_list:'startstop',operation_description:'THREAD BOX',tran_description:'THREAD BOX'}],true,{});
 assert.match(html,/name="comments" value=""/);assert.match(html,/name="entry_list" value="startstop"/);
 let handler,action;
 vm.runInNewContext(source.slice(source.indexOf('app.post("/time/correct",'),source.indexOf('app.post("/time/delete",')),{app:{post:(_path,_guard,fn)=>handler=fn},requireOperator:()=>{},callCncBridge:async value=>{action=value;},setDailyReviewTarget:async()=>{}});
 for(const [list,expected]of [['startstop','edit_active_time_entry'],['timeentry','correct_time_entry']]){
  await handler({body:{entry_id:'1',entry_list:list,comments:'note'},session:{}},{redirect:()=>{}});
  assert.equal(action,expected);
 }
});
