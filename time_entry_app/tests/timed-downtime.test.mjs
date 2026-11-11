import test from 'node:test';
import assert from 'node:assert/strict';
import {applyLocal} from '../public/tablet-time-store.js';
import {timeTotals} from '../public/active-time-summary.js';
import {renderActiveTimerActions} from '../public/time-entry-ui.js';

const start='2026-11-10T14:00:00Z';
const profile=()=>({timer:{id:'timer',start,status:'In Progress',break_minutes:0,downtime_minutes:0,breaks:[],fields:{ProductionNo:'WO1',OperationID:'10'}},context:{downtime_reasons:[{code:'M1',label:'Machine'}]},events:[],drafts:{}});
const begin={id:'downtime',kind:'downtime_start',at:'2026-11-10T14:10:30Z',data:{timer_id:'timer',detail_type_ii:'M1',comments:'Repair'}};

test('active downtime controls collect reason and comments but never a manual duration',()=>{
 const value=profile();const html=renderActiveTimerActions(value.timer,value.context);
 assert.match(html,/Start Downtime/);assert.match(html,/name="detail_type_ii"/);assert.match(html,/name="comments"/);
 assert.doesNotMatch(html,/name="hours"|name="minutes"|Submit Downtime/);
});
test('timed downtime persists with its timer and deducts only once on end',()=>{
 let value=profile();applyLocal(value,structuredClone(begin));
 assert.equal(value.timer.downtime.start,begin.at);assert.equal(value.timer.downtime.comments,'Repair');
 value=JSON.parse(JSON.stringify(value));
 const end={id:'end',kind:'downtime_end',at:'2026-11-10T14:25:30Z',data:{timer_id:'timer',downtime_id:'downtime'}};
 applyLocal(value,end);
 assert.equal(value.timer.downtime_minutes,15);assert.equal(value.timer.downtime,undefined);
 assert.equal(value.timer.downtimes[0].seconds,900);
 assert.throws(()=>applyLocal(value,end),/session changed/);
 assert.equal(value.timer.downtime_minutes,15);
});
test('production is frozen even when downtime starts partway through a minute',()=>{
 const timing={start,break_minutes:5,downtime_minutes:2,downtime_started_at:begin.at};
 assert.equal(timeTotals(timing,Date.parse('2026-11-10T14:11:01Z')).production,3);
 assert.equal(timeTotals(timing,Date.parse('2026-11-10T14:40:01Z')).production,3);
 assert.equal(timeTotals(timing,Date.parse('2026-11-10T14:40:01Z')).downtime,31);
});
test('stale controls, overlapping breaks, wrong reason and premature end are rejected',()=>{
 const value=profile();
 assert.throws(()=>applyLocal(value,{...begin,data:{...begin.data,timer_id:'other'}}),/timer changed/);
 assert.throws(()=>applyLocal(value,{...begin,data:{...begin.data,detail_type_ii:'INVALID'}}),/valid downtime/);
 applyLocal(value,structuredClone(begin));
 for(const kind of ['pause','stop','downtime_start'])assert.throws(()=>applyLocal(value,{...begin,kind}),/End downtime/);
 assert.throws(()=>applyLocal(value,{id:'end',kind:'downtime_end',at:start,data:{timer_id:'timer',downtime_id:'downtime'}}),/clock/);
 assert.equal(value.events.length,1);
});
