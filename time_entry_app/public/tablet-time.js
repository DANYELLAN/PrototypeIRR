import {installLunchReminder,lunchForm} from './lunch-reminder.js';
import {totalsMarkup,timeTotals} from './active-time-summary.js';
import {createDowntimeDialog} from './timed-downtime.js';
import {createSetupDialog,isSetup} from './setup-handoff.js';
import {createEntryCompletionDialog,completionMode} from './entry-completion.js';
import {createReviewReturn} from "./time-view-return.js";
const embedded=new URLSearchParams(location.search).get('embedded')==='1';
if(embedded)document.body.classList.add('tablet-time-embedded');
import {renderTimeNavigation,renderActiveTimerActions,activeDowntimeData} from "./time-entry-ui.js";
import {ownerFor,currentOwner,readProfile,updateProfile,eventId,applyLocal} from './tablet-time-store.js';
const $=id=>document.getElementById(id);
const escape=value=>String(value??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('"','&quot;');
let owner,profile,connected=false,syncing=false,working=false,booting=false,timerControlsKey='',reliefRequest=null,reliefBusy=false;
const downtimeDialog=createDowntimeDialog(async downtime=>{
 if(!await save('downtime_end',{timer_id:profile?.timer?.id,downtime_id:downtime.id}))throw new Error('Downtime is still being logged. Try ending it again.');
});
const setupDialog=createSetupDialog(async(state,detail)=>{
 if(profile?.timer?.id!==state.timer_id)throw new Error('This timer changed. Reopen End Set-Up.');
 if(!await save('end_setup',{timer_id:state.timer_id,detail_type:detail,quantity:state.quantity,next_timer_id:eventId()}))throw new Error('Set-Up was not ended. Check the message and try again.');
});
const completionDialog=createEntryCompletionDialog(async(state,values)=>{
 if(profile?.timer?.id!==state.timer_id)throw new Error('This timer changed. Reopen its controls.');
 if(!await save(state.kind,{timer_id:state.timer_id,completion_version:1,...values}))throw new Error('The entry was not saved. Check the message and try again.');
});
const message=text=>{$('feedback').textContent=text;};
const request=async(url,options={})=>fetch(url,{...options,cache:'no-store',signal:AbortSignal.timeout(15000)});
let navigationStorage;
try{navigationStorage=window.sessionStorage;}catch{}
const returnToReview=createReviewReturn({request,embedded,storage:navigationStorage,navigate:url=>location.replace(url),
 canNavigate:()=>!working&&!reliefBusy&&!document.querySelector('dialog[open]')&&!document.activeElement?.matches('input,select,textarea')});
function status(){
 const pending=profile?.events.length||0;
 const secure=window.isSecureContext && 'serviceWorker' in navigator;
 const controlled=secure && Boolean(navigator.serviceWorker.controller);
 const label=!secure?'HTTPS setup needed for offline reopening':!profile?'Sign in once to prepare offline time entry':!controlled?'Preparing offline files':connected?'Connected':'Offline — saved on this tablet';
 $('connection').textContent=`${label}${profile?` · ${pending} waiting to upload`:''}`;
 $('connection').className=`tablet-status ${!secure?'setup-required':!connected?'offline':''}`;
 $('pending').textContent=`${pending} action${pending===1?'':'s'} waiting to upload.`;
}
function options(items,value,label=value){return '<option value="">Select…</option>'+items.map(item=>`<option value="${escape(value(item))}">${escape(label(item))}</option>`).join('');}
function operationList(form,selected){
 const wo=form.elements.production_number.value;
 const items=profile.context.operations.filter(item=>item.production_number===wo);
 form.elements.operation_id.innerHTML=options(items,item=>item.operation_id,item=>`${item.operation_id} — ${item.operation_description||item.description}`);
 if(items.some(item=>item.operation_id===selected))form.elements.operation_id.value=selected;
}
function forms(){
 for(const kind of ['start','manual','misc']){
  const form=$(kind);const draft=profile.drafts[kind]||{};
  form.querySelector('.operation-fields').innerHTML='<label>Production Order<select name="production_number" required></select></label><label>Operation<select name="operation_id" required></select></label>';
  form.elements.production_number.innerHTML=options([...new Set(profile.context.operations.map(item=>item.production_number))],item=>item);
  form.elements.production_number.value=draft.production_number||'';
  operationList(form,draft.operation_id);
  if(form.elements.detail_type)form.elements.detail_type.innerHTML=options(profile.context.details,item=>item.title);
  if(form.elements.detail_type_ii)form.elements.detail_type_ii.innerHTML=options(profile.context.downtime_reasons,item=>item.code,item=>item.label);
  for(const [key,value]of Object.entries(draft))if(form.elements[key])form.elements[key].value=value;
 }
}
function timerRender(){
 const timer=profile?.timer;
 $('active').hidden=!timer;$('start-card').hidden=Boolean(timer||profile?.context.legacy_active);$('legacy').hidden=!profile?.context.legacy_active;
 if(!timer){timerControlsKey='';downtimeDialog.hide();return;}
 if(timer.downtime)downtimeDialog.show(timer.downtime);else downtimeDialog.hide();
 $('timer-name').textContent=timer.fields.ProductionNo;
 const operation=profile.context.operations.find(item=>item.production_number===timer.fields.ProductionNo && item.operation_id===timer.fields.OperationID);
 $('timer-description').textContent=operation?.operation_description || operation?.description || timer.fields.TranDescription || `Operation ${timer.fields.OperationID}`;
 const currentBreak=timer.status==='Paused'?timer.breaks.at(-1)?.type:'';
 $('timer-chips').innerHTML=`<span class="cnc-chip">${escape(currentBreak==='With Relief'?'Production continuing — relieved break':timer.status)}</span><span class="cnc-chip secondary">${escape(timer.fields.DetailsType)}</span>${currentBreak?`<span class="cnc-chip secondary">${escape(currentBreak)}</span>`:''}`;
 if(currentBreak==='With Relief')$('timer-chips').innerHTML+=`<span class="cnc-chip secondary">Relief: ${escape(timer.breaks.at(-1).relief_name || 'Pending verification')} · ADP ${escape(timer.breaks.at(-1).relief_adp || '')}</span>`;
 const controlsKey=timer.id+':'+timer.status+':'+currentBreak+':'+(timer.downtime?.id||'')+':'+(timer.starting_pipe??'')+':'+timer.start+':'+timer.break_minutes;
 if(timerControlsKey===controlsKey){clock();return;}
 timerControlsKey=controlsKey;
 $('timer-actions').innerHTML=renderActiveTimerActions(timer,profile.context);
 clock();
}
function clock(){
 const timer=profile?.timer;if(!timer)return;
 $('timer-clock').innerHTML=totalsMarkup(timeTotals({...timer,downtime_started_at:timer.downtime?.start,
  break_started_at:timer.status==='Paused'?timer.breaks.at(-1)?.start:null,
  break_deducts:timer.status==='Paused' && ['Lunch','No Relief'].includes(timer.breaks.at(-1)?.type)}));
}
function render(){
 if(embedded)setTimeout(()=>window.parent.postMessage({type:'tablet-time-state',hasWork:Boolean(profile?.timer||profile?.events?.length),ready:Boolean(profile),height:document.body.scrollHeight},location.origin),0);
 $('workspace').hidden=!profile;$('setup').hidden=Boolean(profile);
 if(!profile){$('identity').textContent='Sign in to prepare this tablet';status();return;}
 $('time-navigation').innerHTML=renderTimeNavigation(profile.context.navigation);
 $('identity').textContent=`${profile.context.employee.full_name} · ${profile.context.machine_no} · ${profile.context.shift_title||profile.context.shift_id}`;
 timerRender();status();
 $('receipts').innerHTML=profile.receipts.slice(-15).reverse().map(item=>`<li>${escape(new Date(item.at).toLocaleString())} — ${escape(item.kind)}: ${item.approval_id?`received, pending supervisor approval (#${escape(item.approval_id)})`:'timer event received'}</li>`).join('');
 $('review').innerHTML=profile.context.review?.required?'<a href="/time?traditional=1">Daily time review required — review and confirm your entries</a>':'';
}
async function prepare(){
 if(booting||syncing)return;booting=true;
 try{
  const response=await request('/api/tablet-time/context');
  if(response.status===401){connected=false;if(profile)message('Sign in again to upload saved work. Your tablet copies are kept.');return;}
  if(!response.ok)throw new Error('Server is unavailable.');
  const context=await response.json();const nextOwner=ownerFor(context);
  const prior=await readProfile(nextOwner);
  profile=await updateProfile(nextOwner,current=>{
   const value=current||{owner:nextOwner,events:[],drafts:{},receipts:[],timer:null};
   if(!value.events.length)value.timer=context.timer;
   value.context=context;return value;
  });
  owner=nextOwner;await currentOwner(owner);connected=true;
  if(prior===undefined)message('Work orders downloaded. This tablet is ready to record time.');
  forms();render();
  void returnToReview();
 }catch{connected=false;render();}finally{booting=false;status();}
}
async function sync(){
 if(syncing||!profile||!navigator.onLine)return;syncing=true;
 try{
  while(true){
   profile=await readProfile(owner);const event=profile.events[0];if(!event)break;
   const response=await request('/api/tablet-time/events',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({owner,event})});
   if(response.status===401){message('Sign in again to upload saved work. Your tablet copies are kept.');connected=false;break;}
   const result=await response.json();
   if(!response.ok){connected=true;message(`Sync needs attention: ${result.error||'Server rejected this event.'} Saved actions remain on this tablet.`);break;}
   if(result.id!==event.id)throw new Error('Server receipt did not match.');
   profile=await updateProfile(owner,current=>{
    current.events=current.events.filter(item=>item.id!==event.id);
    if(!current.receipts.some(item=>item.id===event.id))current.receipts.push({id:event.id,kind:event.kind,at:event.at,approval_id:result.approval_id});
    current.receipts=current.receipts.slice(-100);
    if(result.review)current.context.review=result.review;
    if(!current.events.length)current.timer=result.timer?.status==='Submitted'?null:result.timer;
    return current;
   });connected=true;
  }
 }catch{connected=false;}finally{syncing=false;render();}
}
async function save(kind,data){
 if(working)return;working=true;
 try{
  if(['misc','downtime_start'].includes(kind) && data.active_timer_id)data=activeDowntimeData(profile.timer,data);
  const event={id:eventId(),kind,at:new Date().toISOString(),data};
  if(kind==='start')data.timer_id=eventId();
  if(['pause','relief_arrived','resume','stop','downtime_end','end_setup','starting_pipe','correct_start','log_lunch'].includes(kind)){
   if(data.timer_id && data.timer_id!==profile.timer?.id)throw new Error('This timer changed. Reopen the break controls.');
   data.timer_id=profile.timer?.id;
  }
  if(kind==='misc' && profile.timer && profile.timer.fields.ProductionNo===data.production_number && profile.timer.fields.OperationID===data.operation_id)data.timer_id=profile.timer.id;
  if(['manual','misc'].includes(kind)){
   const minutes=Number(data.hours)*60+Number(data.minutes);if(!Number.isInteger(minutes)||minutes<=0||minutes>960)throw new Error('Enter between 1 and 960 whole minutes.');
  }
  profile=await updateProfile(owner,current=>applyLocal(current,event));
  if(kind==='misc' && data.active_timer_id)document.getElementById('active-downtime')?.reset();
  message(kind==='downtime_end'?'Downtime saved for upload and supervisor approval. Production time resumed.':kind==='resume'?'Break ended. Original operator continuing work.':kind==='relief_arrived'?'Relief recorded. Production time is continuing.':'Saved on this tablet.');forms();render();void sync();return true;
 }catch(error){message(`Could not save: ${error.message}. Keep this screen open and try again.`);return false;}finally{working=false;}
}
document.addEventListener('submit',event=>{
 const form=event.target;const kind=form.dataset.action||(['start','manual','misc'].includes(form.id)?form.id:null);
 if(!kind)return;event.preventDefault();if(!form.reportValidity())return;
 const data=Object.fromEntries(new FormData(form));
 if(['starting_pipe','stop'].includes(kind)&&form.matches('[data-entry-starting-pipe],[data-entry-completion]')){
  const timer=profile?.timer;
  if(!timer||timer.id!==data.timer_id){message('This timer changed. Reopen its controls.');return;}
  completionDialog.show({kind,timer_id:timer.id,mode:completionMode(timer.fields.DetailsType),starting_pipe:timer.starting_pipe,production_number:timer.fields.ProductionNo,operation_id:timer.fields.OperationID});return;
 }
 if(kind==='end_setup'){
  const timer=profile?.timer;
  if(!timer||!isSetup(timer.fields.DetailsType)||timer.status!=='In Progress'||timer.downtime){message('Resume Set-Up and end any downtime before continuing.');return;}
  setupDialog.show({timer_id:timer.id,production_number:timer.fields.ProductionNo,operation_id:timer.fields.OperationID,quantity:data.quantity},profile.context.details);return;
 }
 if(kind==='relief_arrived' || (kind==='pause' && data.break_type==='With Relief')){
  reliefRequest={kind,data:{...data,timer_id:profile.timer?.id}};
  $('relief-adp-form').reset();$('relief-adp-error').textContent='';
  $('relief-adp-heading').textContent=kind==='relief_arrived'?'Relief has arrived':'Start break with relief';
  $('relief-adp-note').textContent=connected?'The ADP number will be verified before relief starts.':'Offline: the ADP number will be verified when these saved actions sync.';
  $('relief-adp-dialog').showModal();return;
 }
 void save(kind,data);
});
$('relief-adp-cancel').addEventListener('click',()=>{$('relief-adp-dialog').close();reliefRequest=null;});
$('relief-adp-dialog').addEventListener('cancel',event=>{if(reliefBusy){event.preventDefault();return;}reliefRequest=null;});
$('relief-adp-form').addEventListener('submit',async event=>{
 event.preventDefault();if(!reliefRequest||working||reliefBusy)return;
 const pendingRelief=reliefRequest;reliefBusy=true;$('relief-adp-cancel').disabled=true;
 const button=event.target.querySelector('[type="submit"]');button.disabled=true;
 try{
  const data={...reliefRequest.data,relief_adp:event.target.elements.relief_adp.value.trim()};
  if(!/^\d+$/.test(data.relief_adp))throw new Error('Enter the relief operator ADP number.');
  if(data.relief_adp.replace(/^0+/,'')===String(profile.context.employee.emp_id).replace(/^0+/,''))throw new Error('The relief operator must be different from the original operator.');
  if(connected){
   let response;
   try{response=await request('/api/tablet-time/relief',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({adp_number:data.relief_adp})});}
   catch{connected=false;}
   if(response){const employee=await response.json();if(!response.ok)throw new Error(employee.error||'Unable to verify the relief operator.');data.relief_adp=employee.emp_id;data.relief_name=employee.full_name;}
  }
  if(reliefRequest!==pendingRelief)return;
  if(await save(pendingRelief.kind,data)){$('relief-adp-dialog').close();reliefRequest=null;}
 }catch(error){$('relief-adp-error').textContent=error.message;}
 finally{button.disabled=false;reliefBusy=false;$('relief-adp-cancel').disabled=false;}
});

document.addEventListener('change',event=>{
 const form=event.target.form;if(!form||!['start','manual','misc'].includes(form.id))return;
 if(event.target.name==='production_number')operationList(form,'');
});
for(const name of ['input','change'])document.addEventListener(name,event=>{
 const form=event.target.form;if(!owner||!form||!['start','manual','misc'].includes(form.id))return;
 const values=Object.fromEntries(new FormData(form));
 void updateProfile(owner,current=>{current.drafts[form.id]=values;return current;}).catch(error=>message(`Draft could not be saved: ${error.message}`));
});
$('sync').addEventListener('click',async()=>{await prepare();await sync();});
$('backup').addEventListener('click',async()=>{
 const current=await readProfile(owner);const url=URL.createObjectURL(new Blob([JSON.stringify(current,null,2)],{type:'application/json'}));
 const anchor=document.createElement('a');anchor.href=url;anchor.download='benoit-tablet-time-recovery.json';anchor.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
});
$('signout').addEventListener('click',async()=>{
 if(profile?.timer){message('Stop the active timer before logging out.');return;}
 if(profile?.events.length && !confirm('Unsynced work will stay saved on this tablet for this operator. Log out?'))return;
 await currentOwner(null);profile=null;owner=null;render();
 try{await request('/logout',{method:'POST'});}catch{}
 location.href='/';
});
window.addEventListener('online',async()=>{await prepare();await sync();});window.addEventListener('offline',()=>{connected=false;status();});
document.addEventListener('visibilitychange',async()=>{if(!document.hidden){await prepare();await sync();}});
setInterval(async()=>{
 clock();
 if(profile&&!syncing){
  try{const response=await fetch('/api/tablet-time/health',{cache:'no-store',signal:AbortSignal.timeout(4000)});connected=response.ok;if(connected)void returnToReview();if(response.status===401)message('Sign in again to upload saved work. Your tablet copies are kept.');}
  catch{connected=false;}
  status();void sync();
 }
},15000);
if('serviceWorker' in navigator && window.isSecureContext){
 navigator.serviceWorker.addEventListener('controllerchange',status);
 navigator.serviceWorker.getRegistrations().then(async registrations=>{
  for(const registration of registrations)if(new URL(registration.active?.scriptURL||location.href).pathname==='/public/cnc-time-sw.js')await registration.unregister();
  await navigator.serviceWorker.register('/cnc-time-sw.js',{scope:'/'});
  await navigator.storage?.persist?.();
 }).catch(()=>message('Offline files could not be prepared. Keep the connection until setup is complete.'));
}
try{owner=await currentOwner();if(owner)profile=await readProfile(owner);if(profile)forms();render();await prepare();await sync();}
catch(error){message(`Tablet storage is unavailable: ${error.message}. Do not close this page until your work is saved.`);$('setup').hidden=false;}
window.tabletTimeReady=true;
window.appLoading?.hide();

installLunchReminder({getActive:()=>{
 if(!profile?.timer||profile.timer.downtime||profile.timer.status!=='In Progress'||working||reliefBusy)return null;
 const form=lunchForm();return form?{form,token:profile.timer.id,owner,shift:profile.context.shift_id,logLunch:async day=>{
  const timer=profile?.timer;if(!timer)throw new Error('This entry changed. Reopen its controls.');
  if(!await save('log_lunch',{timer_id:timer.id,lunch_day:day}))throw new Error('Lunch was not logged. Check the message and try again.');
 }}:null;
}});
