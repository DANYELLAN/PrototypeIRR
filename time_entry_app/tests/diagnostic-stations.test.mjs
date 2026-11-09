import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';
import {diagnosticStationView,diagnosticStationHref,diagnosticTime,diagnosticDay,diagnosticDateRange} from '../src/diagnosticStations.js';
test('tabs include configured, observed and unassigned stations with counts',()=>{
 const view=diagnosticStationView([{station:'1'},{station:'1'},{station:'new station'},{station:''}],[{machine_no:'1',label:'CNC 1'},{machine_no:'2',label:'CNC 2'}]);
 assert.deepEqual(view.stations.map(s=>[s.label,s.count]),[['CNC 1',2],['CNC 2',0],['new station',1],['Unassigned',1]]);
});
test('station selection is exact, normalizes hyphen spacing and precedes the display limit',()=>{
 const events=[...Array.from({length:100},(_,id)=>({station:'1',id})),...Array.from({length:900},()=>({station:'10'}))];
 const view=diagnosticStationView(events,[],'1');assert.equal(view.rows.length,100);assert.equal(view.rows[0].id,99);
 assert.equal(diagnosticStationView([{station:'Line 1 - Inspection 1'},{station:'Line 1 - Inspection 2'}],[],'line 1-inspection 1').rows.length,1);
});
test('search stays within the station and unassigned can be isolated',()=>{
 const events=[{station:'1',control:'Start'},{station:'2',control:'Start'},{station:'1',control:'Lunch'},{station:'',control:'Start'}];
 assert.equal(diagnosticStationView(events,[],'1','START').rows.length,1);
 assert.equal(diagnosticStationView(events,[],'__unassigned__').rows.length,1);
 assert.equal(diagnosticStationView(events,[],'','Start').rows.length,3);
});
test('station links safely preserve text filters',()=>{
 const href=diagnosticStationHref('Line 1 & 2','a&b <test>');const url=new URL(href,'http://local');
 assert.equal(url.searchParams.get('station'),'Line 1 & 2');assert.equal(url.searchParams.get('filter'),'a&b <test>');
});
const source=await readFile(new URL('../src/cncTimeServer.js',import.meta.url),'utf8');
const routeStart=source.indexOf('app.get("/admin/click-diagnostics",');
const route=source.slice(routeStart,source.indexOf('\nfunction escapeHtml',routeStart));
const escapeHtml=value=>String(value??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
test('IT-only route retains selected station on refresh and escapes query content',async()=>{
 let handler,guard,result;const requireIT=()=>{};
 vm.runInNewContext(route,{app:{get:(_path,g,fn)=>{guard=g;handler=fn;}},requireIT,diagnosticStationView,diagnosticStationHref,diagnosticTime,escapeHtml,
  clickDiagnostics:{read:async()=>[{station:'1',control:'Start',kind:'click'},{station:'2',control:'Lunch',kind:'click'}]},
  callCncBridge:async()=>({machine_options:[{machine_no:'1',label:'CNC 1'},{machine_no:'2',label:'CNC 2'},{machine_no:'3',label:'CNC 3'}]}),layout:config=>config});
 assert.equal(guard,requireIT);
 await handler({query:{station:'1',filter:'<script>'},session:{cncUser:{roles:['it']}}},{set(){},send:value=>result=value},error=>{throw error;});
 assert.match(result.body,/name="station" value="1"/);assert.match(result.body,/CNC 3/);assert.match(result.body,/aria-current="page"/);
 assert.match(result.body,/No matching activity/);assert.ok(!result.body.includes('<script>'));assert.match(result.body,/&lt;script&gt;/);
});

test('single day is inclusive by Central date, not the UTC calendar day',()=>{
 const events=['2026-11-05T05:59:59Z','2026-11-05T06:00:00Z','2026-11-06T05:59:59Z','2026-11-06T06:00:00Z'].map((received_at,id)=>({received_at,id,station:'1'}));
 const view=diagnosticStationView(events,[],'1','',{from:'2026-11-05',to:'2026-11-05'});
 assert.deepEqual(view.rows.map(r=>r.id),[2,1]);assert.equal(view.stations[0].count,2);
 assert.equal(diagnosticDay(events[0].received_at),'2026-11-04');
 assert.match(diagnosticTime(events[1].received_at),/CST/);
});
test('date boundaries handle both 23-hour and 25-hour Central days',()=>{
 for(const [day,start,end,next] of [['2026-03-08','2026-03-08T06:00:00Z','2026-03-09T04:59:59Z','2026-03-09T05:00:00Z'],['2026-11-01','2026-11-01T05:00:00Z','2026-11-02T05:59:59Z','2026-11-02T06:00:00Z']]){
  const view=diagnosticStationView([start,end,next].map(received_at=>({received_at})),[],'','',{from:day,to:day});
  assert.equal(view.rows.length,2);
 }
});
test('ranges, station selection and search combine before the display limit',()=>{
 const events=[{station:'1',control:'Start',received_at:'2026-11-03T15:00:00Z'},...Array.from({length:1100},()=>({station:'2',control:'Lunch',received_at:'2026-11-05T15:00:00Z'}))];
 const view=diagnosticStationView(events,[],'1','Start',{from:'2026-11-03',to:'2026-11-05'});
 assert.equal(view.rows.length,1);assert.equal(view.matchingCount,1);assert.equal(view.stations.find(s=>s.key==='2').count,0);
 const link=new URL(diagnosticStationHref('1','Start',{from:'2026-11-03',to:'2026-11-05'}),'http://local');
 assert.equal(link.searchParams.get('from'),'2026-11-03');assert.equal(link.searchParams.get('to'),'2026-11-05');
});
test('invalid dates and reversed ranges show an error rather than unfiltered records',()=>{
 for(const dates of [{from:'2026-02-30'},{from:'invalid'},{from:'2026-11-05',to:'2026-11-04'}]){
  assert.ok(diagnosticDateRange(dates.from,dates.to).error);
  const view=diagnosticStationView([{received_at:'2026-11-05T15:00:00Z'}],[],'','',dates);
  assert.equal(view.rows.length,0);assert.ok(view.range.error);
 }
});
