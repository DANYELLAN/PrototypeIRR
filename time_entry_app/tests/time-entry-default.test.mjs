import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';
const source=await readFile(new URL('../src/cncTimeServer.js',import.meta.url),'utf8');
const route=source.slice(source.indexOf('app.get("/time",'),source.indexOf('\nasync function setDailyReviewTarget',source.indexOf('app.get("/time",')));
async function render(query={},earlier=null){
 let handler,result;const actions=[];
 vm.runInNewContext(route,{app:{get:(_path,_guard,fn)=>handler=fn},requireOperator:()=>{},
  dashboardContext:async()=>({work_orders:[],operations:[],details_step_two:[],downtime_reasons:[],recent_entries:[],active_entry:null}),
  callCncBridge:async(action)=>{actions.push(action);return action==='tablet_time_active'?earlier:{required:false,entries:[]};},
  dailyTimeReviewMarkup:()=>'',escapeHtml:value=>String(value??''),layout:config=>config,timeTable:()=>'<table>Review entries</table>'});
 await handler({query,session:{cncUser:{employee:{emp_id:'1'}},notice:null}},{send:value=>result=value,redirect:()=>assert.fail('Time Entry must not switch views')},error=>{throw error;});
 return {result,actions};
}
test('default and historical URLs render the same full Time Entry view without a tablet redirect',async()=>{
 for(const query of [{},{traditional:'1'}]){
  const {result}=await render(query);
  assert.equal(result.active,'time');assert.match(result.body,/action="\/time\/start"/);
  assert.match(result.body,/action="\/time\/manual"/);assert.match(result.body,/action="\/time\/misc"/);
  assert.match(result.body,/Recent Entries/);assert.match(result.body,/Review entries/);
 }
});
test('an earlier server tablet timer remains accessible for recovery',async()=>{
 const {result}=await render({}, {id:'earlier'});
 assert.match(result.body,/data-server-timer="yes"/);
 assert.match(result.body,/Saved offline work is syncing/);
});
test('previous local work is synced inside the normal view and blocks a conflicting timer',async()=>{
 const script=(await readFile(new URL('../public/review-time-recovery.js',import.meta.url),'utf8')).replace(/^import[^\n]+\n/,'');
 for(const profile of [{timer:{id:'old'},events:[]},{timer:null,events:[{id:'unsynced'}]},null]){
  const notice={dataset:{serverTimer:'no'},hidden:true};const button={disabled:false};let embedded;
  await vm.runInNewContext(`(async()=>{${script}})()`,{currentOwner:async()=>'owner',readProfile:async()=>profile,window:{addEventListener:()=>{}},fetch:async()=>{throw new Error('Disconnected');},AbortSignal,document:{getElementById:id=>id==='earlier-tablet-work'?notice:{append:frame=>embedded=frame},querySelectorAll:()=>[button],createElement:()=>({style:{}})}});
  assert.equal(notice.hidden,!profile);assert.equal(button.disabled,Boolean(profile));
  if(profile)assert.equal(embedded.src,'/public/tablet-time.html?embedded=1');
 }
});
test('connected navigation uses the regular server view; only failed navigation uses the tablet shell',async()=>{
 const worker=await readFile(new URL('../public/cnc-time-sw.js',import.meta.url),'utf8');
 for(const offline of [false,true]){
  const handlers=new Map();const network={view:'Review / Corrections'},shell={view:'Offline tablet'};let result,cacheReads=0;
  vm.runInNewContext(worker,{self:{location:{origin:'https://test.local'},addEventListener:(name,fn)=>handlers.set(name,fn)},URL,
   fetch:async()=>{if(offline)throw new Error('Disconnected');return network;},caches:{match:async path=>{assert.equal(path,'/public/tablet-time.html');cacheReads++;return shell;}}});
  handlers.get('fetch')({request:{method:'GET',url:'https://test.local/time',mode:'navigate'},respondWith:promise=>result=promise});
  assert.equal(await result,offline?shell:network);assert.equal(cacheReads,offline?1:0);
 }
 const tablet=await readFile(new URL('../public/tablet-time.js',import.meta.url),'utf8');
 assert.match(tablet,/void returnToReview\(\)/);
});
import {ownerFor} from '../public/tablet-time-store.js';
test('normal connected view prepares offline context without overwriting queued work',async()=>{
 const script=(await readFile(new URL('../public/review-time-recovery.js',import.meta.url),'utf8')).replace(/^import[^\n]+\n/,'');
 const context={employee:{emp_id:'00010'},machine_no:'CNC 1',shift_id:'40',timer:null};
 const owner=ownerFor(context);let stored={owner,timer:{id:'offline-timer'},events:[{id:'waiting'}],drafts:{manual:{minutes:5}},receipts:[]};
 const notice={dataset:{serverTimer:'no',owner},hidden:true};
 await vm.runInNewContext(`(async()=>{${script}})()`,{ownerFor,currentOwner:async()=>owner,readProfile:async()=>stored,
  updateProfile:async(key,change)=>{assert.equal(key,owner);stored=change(stored);return stored;},
  window:{addEventListener:()=>{}},fetch:async()=>({ok:true,json:async()=>context}),AbortSignal,
  document:{getElementById:id=>id==='earlier-tablet-work'?notice:{append:()=>{}},querySelectorAll:()=>[],createElement:()=>({style:{}})}});
 assert.equal(stored.timer.id,'offline-timer');assert.equal(stored.events[0].id,'waiting');assert.equal(stored.drafts.manual.minutes,5);
 assert.equal(stored.context,context);assert.equal(notice.hidden,false);
});
