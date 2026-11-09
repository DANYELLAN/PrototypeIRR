import test from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {inspectionPlan,nextPipe} from '../public/inspection-plan.js';
import {registerTabletInspection} from '../src/tabletInspection.js';

test('tablet sampling matches the authoritative Python plan across cycle maps and zero/numbered/named pipes',()=>{
 const elements=Array.from({length:14},(_,i)=>({element_sequence:i+1}));
 const contexts=[{recipe:{elements},always_count:9,rotating_count:1},
  {recipe:{elements,sampling_plan:{alwaysMeasureItems:[1,3],rotatingAuditItems:[8,10,14],cycleMap:{'1':[8,10],'2':14,'3':8}}},always_count:9,rotating_count:1}];
 const cases=contexts.flatMap(context=>['0','1','2','3','12','001','PIPE'].flatMap(pipe=>['full','standard'].map(scope=>({context,pipe,scope,attempt:2}))));
 const code='import sys,json;import workflow_db as d\ncases=json.load(sys.stdin)\nprint(json.dumps([[r["element_sequence"] for r in d.build_inspection_plan(c["context"]["recipe"],c["attempt"],c["scope"],int(c["pipe"]) if c["pipe"].isdigit() else None,c["context"]["always_count"],c["context"]["rotating_count"])] for c in cases]))';
 const result=spawnSync('python',['-c',code],{cwd:fileURLToPath(new URL('../../',import.meta.url)),input:JSON.stringify(cases),encoding:'utf8',windowsHide:true});
 assert.equal(result.status,0,result.stderr);
 const expected=JSON.parse(result.stdout);
 cases.forEach((item,index)=>assert.deepEqual(inspectionPlan(item.context,item.pipe,item.scope,item.attempt).map(row=>row.element_sequence),expected[index]));
});
test('next pipe preserves prefixes and zero padding',()=>{
 assert.equal(nextPipe('001'),'002');assert.equal(nextPipe('P009'),'P010');assert.equal(nextPipe('999'),'1000');assert.equal(nextPipe('PIPE'),'');
});
test('foreign context is rejected before invoking an inspection bridge write',async()=>{
 const routes=new Map();registerTabletInspection({get:(path,...handlers)=>routes.set(path,handlers),post:(path,...handlers)=>routes.set(path,handlers)},
  {callBridge:()=>{throw new Error('Must not dispatch');},connectionLabel:()=> 'BOX',nextPipeNumber:()=> '002'});
 const req={session:{inspector:{branch:'Ennis'},sessionRecord:{id:1},selection:{productionNumber:'WO',recipeName:'Recipe',operationDescription:'BOX'}},body:{owner:'wrong'}};
 const res={code:200,status(code){this.code=code;return this;},json(value){this.body=value;return this;}};
 await routes.get('/api/tablet-inspection/pipes')[1](req,res);assert.equal(res.code,409);
});
