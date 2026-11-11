import test from 'node:test';
import assert from 'node:assert/strict';
import {isSetup,defaultNextDetail} from '../public/setup-handoff.js';
import {renderActiveTimerActions} from '../public/time-entry-ui.js';
import {applyLocal} from '../public/tablet-time-store.js';

const details=[{title:'Set-Up'},{title:'Machining'},{title:'Inspection'}];
const profile=()=>({timer:{id:'setup',status:'In Progress',start:'2026-11-11T14:00:00Z',breaks:[],break_minutes:0,downtime_minutes:0,fields:{DetailsType:'Set-Up',ProductionNo:'WO1',OperationID:'10',OperationDescription:'Thread PIN'}},context:{details,downtime_reasons:[]},events:[],drafts:{}});

test('only setup details show the End Set-Up button; machining is the default choice',()=>{
 for(const name of ['Set-Up','Set Up','SETUP'])assert.equal(isSetup(name),true);
 assert.equal(isSetup('Machining'),false);assert.equal(defaultNextDetail(details),'Machining');
 const value=profile();const html=renderActiveTimerActions(value.timer,value.context);
 assert.equal((html.match(/>End Set-Up</g)||[]).length,1);
 assert.match(html,/<form data-action="end_setup"[^>]*stop-submit-row/);
 assert.match(html,/name="quantity"/);assert.doesNotMatch(html,/Stop And Submit/);
 value.timer.fields.DetailsType='Machining';assert.doesNotMatch(renderActiveTimerActions(value.timer,value.context),/>End Set-Up</);
});
test('confirmation queues one handoff event and preserves job metadata with fresh counters',()=>{
 const value=profile();const event={id:'end',kind:'end_setup',at:'2026-11-11T15:00:00Z',data:{timer_id:'setup',next_timer_id:'new',detail_type:'Inspection'}};
 applyLocal(value,event);
 assert.equal(value.timer.id,'new');assert.equal(value.timer.fields.DetailsType,'Inspection');
 assert.equal(value.timer.fields.OperationDescription,'Thread PIN');assert.equal(value.timer.fields.ProductionNo,'WO1');
 assert.equal(value.timer.start,event.at);assert.equal(value.timer.break_minutes,0);assert.equal(value.events.length,1);
 assert.throws(()=>applyLocal(value,event),/timer changed/);assert.equal(value.events.length,1);
});
test('invalid details, paused setup, downtime and stale controls cannot end setup',()=>{
 for(const update of [value=>value.timer.status='Paused',value=>value.timer.downtime={id:'dt'},value=>value.timer.fields.DetailsType='Machining']){
  const value=profile();update(value);
  assert.throws(()=>applyLocal(value,{id:'end',kind:'end_setup',at:'2026-11-11T15:00:00Z',data:{timer_id:'setup',next_timer_id:'new',detail_type:'Machining'}}));
  assert.equal(value.events.length,0);
 }
});
