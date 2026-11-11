import test from 'node:test';
import assert from 'node:assert/strict';
import {completionMode,pipeQuantity,renderEntryCompletion} from '../public/entry-completion.js';
import {renderActiveTimerActions} from '../public/time-entry-ui.js';
import {applyLocal} from '../public/tablet-time-store.js';

test('every CNC detail including Machining uses a quantity popup, except unchanged Setup',()=>{
 for(const detail of ['Machining','Turn & Bore','Recut','Rebore','Change Over'])assert.equal(completionMode(detail),'quantity');
 assert.equal(completionMode('Set-Up'),'setup');
 const html=renderEntryCompletion({id:'1',detail_type:'Machining'});
 assert.match(html,/>End Entry</);assert.doesNotMatch(html,/Stop And Submit|type="number"|Enter Starting Pipe/);
});
test('pipe details have a starting-pipe action and inclusive, validated counts',()=>{
 for(const detail of ['Inspection','Sandblast','Drift','Stenciling']){
  const html=renderEntryCompletion({id:'1',detail_type:detail});
  assert.match(html,/Enter Starting Pipe/);assert.match(html,/>End Entry</);assert.match(html,/disabled/);
 }
 assert.equal(pipeQuantity('10','20'),11);assert.equal(pipeQuantity('0010','0010'),1);
 assert.throws(()=>pipeQuantity('20','10'));assert.throws(()=>pipeQuantity('10','20.5'));
});
test('setup still has its quantity box beside End Set-Up',()=>{
 const html=renderActiveTimerActions({id:'setup',status:'In Progress',fields:{DetailsType:'Set-Up'}},{downtime_reasons:[]});
 assert.match(html,/name="quantity" type="number"/);assert.match(html,/>End Set-Up</);assert.doesNotMatch(html,/>End Entry</);
});
test('local starting pipe persists and invalid ending pipe cannot stop the timer',()=>{
 const profile={timer:{id:'timer',start:'2026-11-11T14:00:00Z',status:'In Progress',breaks:[],break_minutes:0,downtime_minutes:0,fields:{DetailsType:'Inspection'}},events:[],drafts:{}};
 applyLocal(profile,{id:'first',kind:'starting_pipe',at:'2026-11-11T14:01:00Z',data:{timer_id:'timer',starting_pipe:'10'}});
 assert.equal(JSON.parse(JSON.stringify(profile)).timer.starting_pipe,'10');
 assert.throws(()=>applyLocal(profile,{id:'end',kind:'stop',at:'2026-11-11T15:00:00Z',data:{timer_id:'timer',ending_pipe:'9',completion_version:1}}));
 assert.equal(profile.timer.id,'timer');assert.equal(profile.events.length,1);
});
