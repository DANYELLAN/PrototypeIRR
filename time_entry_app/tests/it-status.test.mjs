import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';
const source=await readFile(new URL('../src/cncTimeServer.js',import.meta.url),'utf8');
const route=source.slice(source.indexOf('app.post("/contact/it",'),source.indexOf('app.get("/contact/supervisor",'));
test('IT confirmation reports actual delivery status and uses signed-in identity',async()=>{
 for(const result of [{delivered:true,sharepoint_item_id:'25'},{delivered:false,queued:true}]){
  let handler,submitted;
  vm.runInNewContext(route,{app:{post:(_path,_guard,fn)=>handler=fn},requireOperator:()=>{},callCncBridge:async(_action,payload)=>{submitted=payload;return result;}});
  const req={session:{cncUser:{employee:{full_name:'Operator',department:'Quality'},machine_no:'CNC 5'}},body:{user_email:'forged@example.com',user_name:'Forged name',category:'Software',issue:'Issue'}};
  await handler(req,{redirect:path=>assert.equal(path,'/contact/it')});
  assert.equal(submitted.user_name,'Operator');assert.equal(submitted.department,'Quality');assert.equal(submitted.machine_no,'CNC 5');
  assert.equal(submitted.user_email,'');
  assert.equal(req.session.notice.kind,result.delivered?'success':'warning');
  assert.match(req.session.notice.message,result.delivered?/sent to SharePoint Help Desk.*25/:/saved locally and waiting/);
 }
});

test('IT form shows the signed-in name instead of requesting an email',async()=>{
 const getRoute=source.slice(source.indexOf('app.get("/contact/it",'),source.indexOf('app.post("/contact/it",'));
 let handler,result;
 vm.runInNewContext(getRoute,{app:{get:(_path,_guard,fn)=>handler=fn},requireOperator:()=>{},dashboardContext:async()=>({tech_categories:[{title:'Hardware'}]}),escapeHtml:value=>String(value??'').replaceAll('"','&quot;'),layout:value=>value});
 await handler({session:{cncUser:{employee:{full_name:'Test Operator'}},notice:null}},{send:value=>result=value},error=>{throw error;});
 assert.match(result.body,/<span>User Name<\/span>/);
 assert.match(result.body,/name="user_name" value="Test Operator" readonly/);
 assert.doesNotMatch(result.body,/User Email|type="email"|name="user_email"/);
});
