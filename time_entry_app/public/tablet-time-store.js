export const ownerFor=profile=>JSON.stringify([String(profile.employee.emp_id).replace(/^0+/,'')||'0',String(profile.machine_no),String(profile.shift_id)]);
let opening;
function database(){
 if(!opening)opening=new Promise((resolve,reject)=>{
  const request=indexedDB.open('benoit-tablet-time',1);
  request.onupgradeneeded=()=>{request.result.createObjectStore('profiles',{keyPath:'owner'});request.result.createObjectStore('meta');};
  request.onsuccess=()=>resolve(request.result);request.onerror=()=>reject(request.error);
 });
 return opening;
}
export async function readProfile(owner){
 const db=await database();return new Promise((resolve,reject)=>{
  const request=db.transaction('profiles').objectStore('profiles').get(owner);
  request.onsuccess=()=>resolve(request.result);request.onerror=()=>reject(request.error);
 });
}
export async function currentOwner(value){
 const db=await database();return new Promise((resolve,reject)=>{
  const tx=db.transaction('meta',value===undefined?'readonly':'readwrite');
  const store=tx.objectStore('meta');const request=value===undefined?store.get('current'):store.put(value,'current');
  let result;request.onsuccess=()=>{result=request.result;};tx.oncomplete=()=>resolve(result);tx.onerror=()=>reject(tx.error);
 });
}
export async function updateProfile(owner,change){
 const db=await database();return new Promise((resolve,reject)=>{
  const tx=db.transaction('profiles','readwrite');const store=tx.objectStore('profiles');const request=store.get(owner);let result,failure;
  request.onsuccess=()=>{try{result=change(request.result);store.put(result);}catch(error){failure=error;tx.abort();}};
  tx.oncomplete=()=>resolve(result);tx.onabort=()=>reject(failure||tx.error);tx.onerror=()=>reject(tx.error);
 });
}
export function eventId(){
 const bytes=crypto.getRandomValues(new Uint8Array(16));bytes[6]=(bytes[6]&15)|64;bytes[8]=(bytes[8]&63)|128;
 const hex=[...bytes].map(byte=>byte.toString(16).padStart(2,'0')).join('');return `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`;
}
export function applyLocal(profile,event){
 const {kind,data,at}=event;
 const timer=profile.timer;
 if(timer?.downtime && data.timer_id===timer.id && kind!=='downtime_end')throw new Error('End downtime before changing or stopping production.');
 if(kind==='start'){
  if(timer)throw new Error('A timer is already running on this tablet.');
  if(profile.context.legacy_active)throw new Error('Stop the earlier timer in the original time screen first.');
  profile.timer={id:data.timer_id,start:at,status:'In Progress',break_minutes:0,downtime_minutes:0,breaks:[],
   fields:{ProductionNo:data.production_number,OperationID:data.operation_id,DetailsType:data.detail_type}};
 }else if(['correct_start','log_lunch'].includes(kind)){
  if(!timer||timer.id!==data.timer_id)throw new Error('This timer changed. Reopen Edit Time.');
  applyTimingAdjustment(timer,kind,data,at,profile.context.shift_id||profile.shift_id);
 }else if(kind==='starting_pipe'){
  if(!timer||timer.id!==data.timer_id)throw new Error('This timer changed. Reopen its controls.');
  if(completionMode(timer.fields.DetailsType)!=='pipes')throw new Error('This detail type does not use pipe ranges.');
  timer.starting_pipe=pipeNumber(data.starting_pipe);
 }else if(kind==='end_setup'){
  if(!timer||timer.id!==data.timer_id)throw new Error('This timer changed. Reopen End Set-Up.');
  if(timer.status!=='In Progress'||!isSetup(timer.fields.DetailsType))throw new Error('End Set-Up is available only for a running Set-Up entry.');
  if(!profile.context.details.some(item=>item.title===data.detail_type))throw new Error('Select a valid detail type.');
  const quantity=Number(data.quantity??0);
  if((data.quantity!=null&&String(data.quantity).trim()==='')||!Number.isInteger(quantity)||quantity<0||quantity>1000000)throw new Error('Enter a whole Set-Up quantity between 0 and 1000000.');
  if(!data.next_timer_id||data.next_timer_id===timer.id)throw new Error('The next timer ID is invalid.');
  if(!Number.isFinite(Date.parse(at))||Date.parse(at)<Date.parse(timer.start))throw new Error('Check the tablet clock before ending Set-Up.');
  profile.timer={id:data.next_timer_id,start:at,status:'In Progress',break_minutes:0,downtime_minutes:0,breaks:[],
   fields:{...timer.fields,DetailsType:data.detail_type}};
 }else if(['downtime_start','downtime_end'].includes(kind)){
  if(!timer || timer.id!==data.timer_id)throw new Error('This timer changed. Reopen Log Downtime for the current entry.');
  if(kind==='downtime_start'){
   if(timer.status!=='In Progress')throw new Error('Resume production before starting downtime.');
   const reason=profile.context.downtime_reasons.find(item=>item.code===data.detail_type_ii);
   if(!reason)throw new Error('Select a valid downtime reason.');
   timer.downtime={id:event.id,start:at,reason:reason.code,reason_label:reason.label,comments:data.comments||'',production_number:timer.fields.ProductionNo,operation_id:timer.fields.OperationID};
  }else{
   if(!timer.downtime || timer.downtime.id!==data.downtime_id)throw new Error('This downtime session changed. Reopen its controls.');
   const seconds=(Date.parse(at)-Date.parse(timer.downtime.start))/1000;
   if(!Number.isFinite(seconds)||seconds<0)throw new Error('Check the tablet clock before ending downtime.');
   const minutes=Math.floor(seconds/60);
   timer.downtime_minutes+=minutes;
   (timer.downtimes ||= []).push({...timer.downtime,end:at,minutes,seconds});
   delete timer.downtime;
  }
 }else if(['pause','relief_arrived','resume','stop'].includes(kind)){
  if(!timer || timer.id!==data.timer_id)throw new Error('This timer changed. Refresh its controls before continuing.');
  if(kind==='stop'){
   if(completionMode(timer.fields.DetailsType)==='pipes'&&(data.completion_version||timer.starting_pipe!=null||data.ending_pipe!=null))pipeQuantity(timer.starting_pipe,data.ending_pipe);
   else enteredQuantity(data.quantity??0);
  }
  if(kind==='pause'){
   if(timer.status!=='In Progress')throw new Error('This timer is already paused.');
   if(!['Lunch','No Relief','With Relief'].includes(data.break_type))throw new Error('Select a valid break type.');
   if(data.break_type==='With Relief')validateRelief(profile,data);
   timer.breaks.push({type:data.break_type,start:at,comment:data.comments||'',original_adp:profile.context.employee?.emp_id,original_name:profile.context.employee?.full_name,...(data.break_type==='With Relief'?{relief_adp:data.relief_adp,relief_name:data.relief_name||''}:{})});timer.status='Paused';
  }else if(kind==='relief_arrived'){
   if(timer.status!=='Paused'||timer.breaks.at(-1)?.type!=='No Relief')throw new Error('Relief Arrived is available only during a break without relief.');
   validateRelief(profile,data);
   const pause=timer.breaks.at(-1);
   pause.end=at;pause.minutes=Math.max(0,Math.floor((Date.parse(at)-Date.parse(pause.start))/60000));pause.end_reason='relief_arrived';
   timer.break_minutes+=pause.minutes;
   timer.breaks.push({type:'With Relief',start:at,break_started_at:pause.break_started_at||pause.start,comment:data.comments||'',relief_adp:data.relief_adp,relief_name:data.relief_name||'',original_adp:profile.context.employee?.emp_id,original_name:profile.context.employee?.full_name});
  }else{
   if(kind==='resume' && timer.status!=='Paused')throw new Error('This timer is not paused.');
   if(timer.status==='Paused'){
    const pause=timer.breaks.at(-1);pause.end=at;pause.minutes=Math.max(0,Math.floor((Date.parse(at)-Date.parse(pause.start))/60000));
    pause.end_reason=kind==='resume'?'operator_returned':'entry_stopped';
    if(kind==='resume'){pause.return_adp=profile.context.employee?.emp_id;pause.return_name=profile.context.employee?.full_name;}
    if(['Lunch','No Relief'].includes(pause.type))timer.break_minutes+=pause.minutes;
   }
   timer.status=kind==='resume'?'In Progress':'Submitted';
   if(kind==='stop')profile.timer=null;
  }
 }else if(kind==='misc' && timer && data.timer_id===timer.id){
  timer.downtime_minutes+=Number(data.hours||0)*60+Number(data.minutes||0);
 }
 profile.events.push(event);profile.drafts[kind]={};return profile;
}

function validateRelief(profile,data){
 const adp=String(data.relief_adp||'').trim();
 if(!/^\d+$/.test(adp))throw new Error('Enter the relief operator ADP number.');
 if(adp.replace(/^0+/,'')===String(profile.context.employee?.emp_id||'').replace(/^0+/,''))throw new Error('The relief operator must be different from the original operator.');
}
import {isSetup} from './setup-handoff.js';
import {completionMode,pipeNumber,pipeQuantity,enteredQuantity} from './entry-completion.js';
import {applyTimingAdjustment} from './active-timing.js';
