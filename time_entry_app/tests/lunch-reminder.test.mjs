import test from 'node:test';
import assert from 'node:assert/strict';
import {lunchTime,lunchForm,lunchSchedule} from '../public/lunch-reminder.js';
test('lunch is 11:30 in Central standard time regardless of device timezone',()=>{
 assert.deepEqual(lunchTime(new Date('2026-11-05T17:30:00Z')),{day:'2026-11-05',due:true});
 assert.equal(lunchTime(new Date('2026-11-05T17:29:59Z')).due,false);
 assert.equal(lunchTime(new Date('2026-11-05T17:31:00Z')).due,false);
});
test('Central daylight saving time retains the same 11:30 local lunch time',()=>{
 assert.equal(lunchTime(new Date('2026-07-05T16:30:00Z')).due,true);
 assert.equal(lunchTime(new Date('2026-07-05T17:30:00Z')).due,false);
 assert.equal(lunchTime(new Date('2026-11-05T05:30:00Z')).due,false);
});
test('the existing lunch action is selected rather than a relief break',()=>{
 const relief={elements:{break_type:{value:'With Relief'}}};
 const lunch={elements:{break_type:{value:'Lunch'}}};
 assert.equal(lunchForm({querySelectorAll:()=>[relief,lunch]}),lunch);
 assert.equal(lunchForm({querySelectorAll:()=>[relief]}),undefined);
});

test('night shift lunch is 10 PM Central in winter and summer',()=>{
 assert.deepEqual(lunchTime(new Date('2026-11-06T04:00:00Z'),'41'),{day:'2026-11-05',due:true});
 assert.deepEqual(lunchTime(new Date('2026-07-06T03:00:00Z'),41),{day:'2026-07-05',due:true});
 assert.equal(lunchTime(new Date('2026-11-06T03:59:59Z'),'41').due,false);
 assert.equal(lunchTime(new Date('2026-11-06T04:01:00Z'),'41').due,false);
 assert.equal(lunchSchedule('Night Shift').label,'10:00 PM');
});
test('each shift receives only its own lunch reminder',()=>{
 assert.equal(lunchTime(new Date('2026-11-05T17:30:00Z'),'41').due,false);
 assert.equal(lunchTime(new Date('2026-11-06T04:00:00Z'),'40').due,false);
 assert.equal(lunchTime(new Date('2026-11-05T17:30:00Z'),'40').due,true);
});
