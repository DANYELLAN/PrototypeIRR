import {ownerFor,currentOwner,readProfile,updateProfile} from './tablet-time-store.js';
const notice=document.getElementById('earlier-tablet-work');
let frame;
function showOfflineWork(){
 if(!notice)return;
 notice.hidden=false;
 document.querySelectorAll('form[action="/time/start"] button[type="submit"]').forEach(button=>{button.disabled=true;button.title='Finish the running offline timer first.';});
 frame=document.createElement('iframe');frame.title='Active time entry and saved work';frame.src='/public/tablet-time.html?embedded=1';
 frame.style.cssText='width:100%;min-height:480px;border:0;display:block';
 document.getElementById('offline-timer-container').append(frame);
}
window.addEventListener('message',event=>{
 if(event.origin!==location.origin||event.source!==frame?.contentWindow||event.data?.type!=='tablet-time-state')return;
 frame.style.height=Math.max(160,Number(event.data.height)||480)+'px';
 if(!event.data.ready)return;
 document.querySelectorAll('form[action="/time/start"] button[type="submit"]').forEach(button=>{button.disabled=Boolean(event.data.hasWork);});
 if(!event.data.hasWork){
  document.getElementById('offline-work-status').textContent='Offline work synced. Continue using Time Entry below.';
  document.querySelectorAll('form[action="/time/start"] button[type="submit"]').forEach(button=>{button.disabled=false;button.removeAttribute('title');});
 }
});
try {
 let owner=await currentOwner();
 let profile=owner?await readProfile(owner):null;
 try {
  let context;
  const prepared=document.getElementById('prepared-tablet-context');
  if(prepared?.textContent){try{context=JSON.parse(prepared.textContent);}catch{}}
  if(!context?.employee?.emp_id){
   const response=await fetch('/api/tablet-time/context',{cache:'no-store',signal:AbortSignal.timeout(10000)});
   if(response.ok)context=await response.json();
  }
  if(context?.employee?.emp_id){
   owner=ownerFor(context);
   if(context.legacy_active?.timing)void import('./active-time-summary.js').then(module=>module.showActiveTimeSummary(context.legacy_active)).catch(()=>{});
   profile=await updateProfile(owner,current=>{
    const value=current||{owner,events:[],drafts:{},receipts:[],timer:null};
    if(!value.events.length)value.timer=context.timer;
    value.context=context;return value;
   });
   await currentOwner(owner);
  }
 } catch { /* Keep already prepared offline work when the connection fails mid-load. */ }
 if(notice?.dataset.owner && owner!==notice.dataset.owner)profile=null;
 if(notice?.dataset.serverTimer==='yes'||profile?.timer||profile?.events?.length)showOfflineWork();
} catch { if(notice?.dataset.serverTimer==='yes')showOfflineWork(); }
