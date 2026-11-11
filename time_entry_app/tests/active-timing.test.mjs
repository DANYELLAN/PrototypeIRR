import test from 'node:test';
import assert from 'node:assert/strict';
import {correctedStart,applyTimingAdjustment,renderActiveTiming,startOnExistingDate} from '../public/active-timing.js';
import {lateLunchAvailable} from '../public/lunch-reminder.js';
const at='2026-11-11T19:00:00Z';
const timer=()=>({id:'one',start:'2026-11-11T16:00:00Z',fields:{ProductionNo:'WO1'},status:'In Progress',break_minutes:0,breaks:[]});
test('active editor only offers start correction and missed lunch',()=>{
 const html=renderActiveTiming({id:'one',start:'2026-11-11T16:00:00Z',status:'In Progress'});
 assert.match(html,/type="text" inputmode="numeric" name="start_time" value="10:00"/);assert.match(html,/Save Start Time/);assert.match(html,/Log Lunch \(1 Hour\)/);
 assert.doesNotMatch(html,/type="date|datetime-local/);
 assert.doesNotMatch(html,/name="production_number"|name="quantity"|name="detail_type"/);
});
test('Central start correction rejects future and ambiguous or nonexistent clock times',()=>{
 assert.equal(correctedStart('2026-11-11T08:00',at),'2026-11-11T14:00:00.000Z');
 assert.throws(()=>correctedStart('2026-11-11T14:00',at));
 assert.throws(()=>correctedStart('2026-11-01T01:30','2026-11-01T09:00:00Z'));
 assert.throws(()=>correctedStart('2026-03-08T02:30','2026-03-08T10:00:00Z'));
});
test('missed lunch adds an hour once and does not stop the active timer',()=>{
 const value=timer();applyTimingAdjustment(value,'correct_start',{start_time:'2026-11-11T08:00'},at,'40');
 applyTimingAdjustment(value,'log_lunch',{},at,'40');
 assert.equal(value.break_minutes,60);assert.equal(value.status,'In Progress');assert.equal(value.id,'one');
 assert.throws(()=>applyTimingAdjustment(value,'log_lunch',{},at,'40'));assert.equal(value.break_minutes,60);
});

test('time-only corrections keep the original Central date and reject date changes',()=>{
 const value=timer();applyTimingAdjustment(value,'correct_start',{start_time:'08:00'},at,'40');
 assert.equal(value.start,'2026-11-11T14:00:00.000Z');
 assert.throws(()=>applyTimingAdjustment(value,'correct_start',{start_time:'2026-11-10T08:00'},at,'40'),/date must stay/);
 assert.equal(value.start,'2026-11-11T14:00:00.000Z');
});

test('numeric keyboard time accepts four digits and rejects invalid hours or minutes',()=>{
 const value=timer();applyTimingAdjustment(value,'correct_start',{start_time:'0800'},at,'40');
 assert.equal(value.start,'2026-11-11T14:00:00.000Z');
 for(const invalid of ['2400','0860','800'])assert.throws(()=>applyTimingAdjustment(value,'correct_start',{start_time:invalid},at,'40'));
});

test('AM/PM converts noon and midnight correctly and renders the existing period',()=>{
 const existing='2026-11-11T16:00:00Z',now='2026-11-11T23:00:00Z';
 for(const [time,period,expected]of [['800','AM','14:00'],['230','PM','20:30'],['12:00','AM','06:00'],['1200','PM','18:00']]){
  assert.equal(startOnExistingDate(time,existing,now,period),`2026-11-11T${expected}:00.000Z`);
 }
 for(const [time,period]of [['000','AM'],['1300','PM'],['1260','AM'],['800','']])assert.throws(()=>startOnExistingDate(time,existing,now,period));
 const html=renderActiveTiming({id:'one',start:'2026-11-11T20:30:00Z',status:'In Progress'});
 assert.match(html,/name="start_time" value="2:30"/);assert.match(html,/value="PM" selected/);assert.doesNotMatch(html,/24-hour/);
});
test('late lunch is enabled at exactly 12:30 Central on the reminder day',()=>{
 assert.equal(lateLunchAvailable(new Date('2026-11-11T18:29:59Z'),'40','2026-11-11'),false);
 assert.equal(lateLunchAvailable(new Date('2026-11-11T18:30:00Z'),'40','2026-11-11'),true);
 assert.equal(lateLunchAvailable(new Date('2026-07-11T17:30:00Z'),'40','2026-07-11'),true);
 assert.equal(lateLunchAvailable(new Date('2026-11-11T18:30:00Z'),'41','2026-11-11'),false);
 assert.equal(lateLunchAvailable(new Date('2026-11-12T18:30:00Z'),'40','2026-11-11'),false);
});
