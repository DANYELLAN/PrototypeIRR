import test from 'node:test';
import assert from 'node:assert/strict';
import {timeTotals} from '../public/active-time-summary.js';
const now=Date.parse('2026-11-05T12:00:00Z');
const timing={start:'2026-11-05T10:00:00Z',break_minutes:15,downtime_minutes:10};
test('production excludes recorded breaks and downtime',()=>{
 assert.deepEqual(timeTotals(timing,now),{production:95,breaks:15,downtime:10});
});
test('an ongoing uncovered break grows while production stays paused',()=>{
 const paused={...timing,break_started_at:'2026-11-05T11:50:00Z',break_deducts:true};
 assert.deepEqual(timeTotals(paused,now),{production:85,breaks:25,downtime:10});
 assert.deepEqual(timeTotals(paused,now+60000),{production:85,breaks:26,downtime:10});
});
test('relieved coverage continues accumulating production',()=>{
 const relieved={...timing,break_started_at:'2026-11-05T11:50:00Z',break_deducts:false};
 assert.deepEqual(timeTotals(relieved,now),{production:95,breaks:15,downtime:10});
 assert.deepEqual(timeTotals(relieved,now+60000),{production:96,breaks:15,downtime:10});
});
test('corrections and clock differences cannot display negative production',()=>{
 assert.equal(timeTotals({...timing,break_minutes:500},now).production,0);
 assert.equal(timeTotals(timing,Date.parse('2026-11-05T09:00:00Z')).production,0);
});
