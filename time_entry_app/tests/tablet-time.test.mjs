import test from 'node:test';
import assert from 'node:assert/strict';
import {applyLocal,ownerFor} from '../public/tablet-time-store.js';
import {registerTabletTime} from '../src/tabletTime.js';

const session={employee:{emp_id:'000747',full_name:'Test'},machine_no:'Machine 2',shift_id:40};
const owner=ownerFor(session);
function profile(){return {owner,events:[],drafts:{},receipts:[],timer:null,context:{legacy_active:null}};}
function event(kind,at,data={}){return {id:kind+'-'+at,kind,at:`2026-10-21T${at}:00Z`,data};}
test('tablet timer keeps original times and subtracts lunch, not relief breaks',()=>{
 const p=profile();
 applyLocal(p,event('start','10:00',{timer_id:'one',production_number:'WO',operation_id:'10',detail_type:'Production'}));
 applyLocal(p,event('pause','10:30',{timer_id:'one',break_type:'Lunch'}));
 applyLocal(p,event('resume','11:00',{timer_id:'one'}));
 assert.equal(p.timer.break_minutes,30);
 applyLocal(p,event('pause','11:30',{timer_id:'one',break_type:'With Relief',relief_adp:'123'}));
 applyLocal(p,event('resume','11:45',{timer_id:'one'}));
 assert.equal(p.timer.break_minutes,30);assert.equal(p.timer.start,'2026-10-21T10:00:00Z');
 assert.equal(p.events.length,5);
});
test('duplicate starts and stale timer controls cannot modify the current timer',()=>{
 const p=profile();applyLocal(p,event('start','10:00',{timer_id:'one'}));
 assert.throws(()=>applyLocal(p,event('start','10:01',{timer_id:'two'})));
 assert.throws(()=>applyLocal(p,event('stop','11:00',{timer_id:'wrong'})));
 assert.equal(p.events.length,1);assert.equal(p.timer.id,'one');
});
test('operator, machine, and shift form the queue identity',()=>{
 assert.equal(owner,'["747","Machine 2","40"]');
 assert.notEqual(ownerFor({...session,shift_id:41}),owner);
 assert.notEqual(ownerFor({...session,machine_no:'Machine 3'}),owner);
});
function routes(bridge){
 const map=new Map();
 const app={get:(path,...handlers)=>map.set('GET '+path,handlers),post:(path,...handlers)=>map.set('POST '+path,handlers)};
 registerTabletTime(app,{callBridge:bridge,hasRole:()=>true});return map;
}
function response(){return {code:200,body:null,status(code){this.code=code;return this;},json(body){this.body=body;return this;},set(){return this;}};}
test('expired sessions receive JSON 401 and keep the browser queue intact',()=>{
 const handlers=routes(()=>{throw new Error('Must not dispatch');}).get('POST /api/tablet-time/events');
 const res=response();handlers[0]({session:{}},res,()=>{throw new Error('Must not authorize');});
 assert.equal(res.code,401);assert.match(res.body.error,/Sign in again/);
});
test('a queue recorded by another station never reaches the bridge',async()=>{
 const handlers=routes(()=>{throw new Error('Must not dispatch');}).get('POST /api/tablet-time/events');
 const res=response();await handlers[1]({session:{cncUser:session},body:{owner:'other'}},res);
 assert.equal(res.code,409);
});
test('saved entry returns a receipt even if refreshing the daily review fails',async()=>{
 const handlers=routes(async(action)=>{if(action==='tablet_time_event')return {id:'one',approval_id:3};throw new Error('Review unavailable');}).get('POST /api/tablet-time/events');
 const res=response();await handlers[1]({session:{cncUser:session},body:{owner,event:{id:'one'}}},res);
 assert.equal(res.code,200);assert.equal(res.body.id,'one');assert.equal(res.body.review_pending,true);
});


test('Relief Arrived splits the break at arrival, counts relief as production and records the owner return',()=>{
 const p=profile();p.context.employee={emp_id:'747',full_name:'Original Operator'};
 applyLocal(p,event('start','10:00',{timer_id:'one'}));
 applyLocal(p,event('pause','10:10',{timer_id:'one',break_type:'No Relief'}));
 applyLocal(p,event('relief_arrived','10:25',{timer_id:'one',relief_adp:'123',relief_name:'Relief Operator'}));
 assert.equal(p.timer.break_minutes,15);assert.equal(p.timer.breaks[0].end_reason,'relief_arrived');
 assert.equal(p.timer.breaks[1].type,'With Relief');assert.equal(p.timer.breaks[1].relief_adp,'123');
 assert.throws(()=>applyLocal(p,event('relief_arrived','10:26',{timer_id:'one',relief_adp:'456'})));
 applyLocal(p,event('resume','10:50',{timer_id:'one'}));
 assert.equal(p.timer.break_minutes,15);assert.equal(p.timer.breaks[1].minutes,25);
 assert.equal(p.timer.breaks[1].return_adp,'747');assert.equal(p.timer.breaks[1].end_reason,'operator_returned');
});
test('missing, self and stale relief identities cannot start coverage or alter queued events',()=>{
 const p=profile();p.context.employee={emp_id:'000747'};
 applyLocal(p,event('start','10:00',{timer_id:'one'}));
 for(const relief_adp of ['', 'letters','747'])assert.throws(()=>applyLocal(p,event('pause','10:01',{timer_id:'one',break_type:'With Relief',relief_adp})));
 assert.equal(p.timer.status,'In Progress');assert.equal(p.events.length,1);
});

test('relief ADP lookup uses the signed-in operator and rejects unauthenticated requests',async()=>{
 let payload;
 const handlers=routes(async(action,values)=>{assert.equal(action,'tablet_relief_operator');payload=values;return {emp_id:'123',full_name:'Relief Operator'};}).get('POST /api/tablet-time/relief');
 const denied=response();handlers[0]({session:{}},denied,()=>assert.fail('Must not authorize'));
 assert.equal(denied.code,401);
 const res=response();await handlers[1]({session:{cncUser:session},body:{adp_number:'123',original_adp:'FORGED'}},res);
 assert.equal(payload.original_adp,'000747');assert.equal(payload.adp_number,'123');assert.equal(res.body.emp_id,'123');
});
