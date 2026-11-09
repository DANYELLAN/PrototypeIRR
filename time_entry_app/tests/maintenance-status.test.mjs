import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';
const source=await readFile(new URL('../src/cncTimeServer.js',import.meta.url),'utf8');
const route=source.slice(source.indexOf('app.post("/maintenance",'),source.indexOf('app.get("/contact/it",'));
test('confirmation distinguishes delivered requests from locally queued requests',async()=>{
 for(const result of [{submitted:true,sharepoint_item_id:'123'},{submitted:false,queued:true}]){
  let handler;
  vm.runInNewContext(route,{app:{post:(_path,_guard,fn)=>handler=fn},requireOperator:()=>{},callCncBridge:async()=>result});
  const req={session:{cncUser:{employee:{emp_id:'TEST'}}},body:{}};
  await handler(req,{redirect:path=>assert.equal(path,'/maintenance')});
  assert.equal(req.session.notice.kind,result.submitted?'success':'warning');
  assert.match(req.session.notice.message,result.submitted?/sent to SharePoint.*123/:/saved locally and waiting/);
 }
});
