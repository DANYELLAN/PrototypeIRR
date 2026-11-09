import {currentOwner,readProfile,updateProfile,eventId} from './inspection-store.js';
import {inspectionPlan,nextPipe} from './inspection-plan.js';
const $=id=>document.getElementById(id),escape=value=>String(value??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('"','&quot;');
let owner,profile,connected=false,syncing=false,working=false,preparing=false,renderedPipe,renderedScope;
const message=text=>{$('feedback').textContent=text;};
const request=(url,options={})=>fetch(url,{...options,cache:'no-store',signal:AbortSignal.timeout(15000)});
const plan=()=>inspectionPlan(profile.context,profile.pipe_number,profile.scope,profile.attempt_no);
function status(){
 const secure=window.isSecureContext&&'serviceWorker' in navigator;
 const prepared=secure&&Boolean(navigator.serviceWorker.controller);
 const count=profile?.events.length||0;
 $('connection').textContent=`${!secure?'HTTPS setup needed for offline reopening':!profile?'Select a work order to prepare offline inspection':!prepared?'Preparing offline files':connected?'Connected':'Offline — measurements saved on this tablet'}${profile?` · ${count} pipes waiting to upload`:''}`;
 $('connection').className=`tablet-status ${!secure?'setup-required':!connected?'offline':''}`;
 $('pending').textContent=`${count} pipe inspection${count===1?'':'s'} waiting to upload.`;
 $('lunch').disabled=!connected;$('break').disabled=!connected;
}
function validation(){
 let missing=0,failed=0;
 for(const element of plan()){
  const field=document.querySelector(`[data-sequence="${element.element_sequence}"]`);if(!field){missing++;continue;}
  const value=field.value.trim();let pass=null;
  if(!value)missing++;
  else if(element.capture_type==='boolean')pass=value==='Yes';
  else{
   const actual=window.resolveInspectionMeasurement(value,element.nominal).value;
   field.setCustomValidity(value&&!Number.isFinite(actual)?'Enter a full measurement or signed offset.':'');
   const limit=value=>value===null||value===undefined?null:Number(Number(value).toPrecision(15));
   const min=limit(element.min),max=limit(element.max);
   pass=Number.isFinite(actual)&&(min===null||actual>=min)&&(max===null||actual<=max);
  }
  if(pass===false)failed++;
  field.closest('tr').className=pass===null?'':pass?'pass':'fail';
 }
 $('result').textContent=missing?`${missing} required elements remaining.`:failed?`${failed} failed elements. This pipe will be held for rework and Quality review.`:'All required elements pass.';
 return {missing,failed};
}
function rows(){
 renderedPipe=profile.pipe_number;renderedScope=profile.scope;
 $('elements').innerHTML=plan().map(element=>{
  const number=element.element_sequence,value=profile.values[String(number)]??'';
  const control=element.capture_type==='boolean'?`<select data-sequence="${number}" required><option value="">Inspect…</option><option value="Yes" ${value==='Yes'?'selected':''}>Pass</option><option value="No" ${value==='No'?'selected':''}>Fail</option></select>`:
   `<input data-sequence="${number}" data-measurement-offset data-nominal="${escape(element.nominal??'')}" inputmode="decimal" value="${escape(value)}" placeholder="Full value or +/- offset" required>`;
  return `<tr><td>${number}</td><td>${escape(element.element_description)}</td><td>${escape(element.dwg_dim)}</td><td>${escape(element.gauge)}</td><td>${control}</td></tr>`;
 }).join('');
 $('notes').value=profile.notes||'';$('pipe-number').value=profile.pipe_number;
 $('scope').textContent=profile.scope==='full'?'Switch To Standard Inspection':'Switch To Full Inspection';validation();
}
function render(){
 $('workspace').hidden=!profile;$('setup').hidden=Boolean(profile);
 if(profile){
  $('attempt-title').textContent=`Current Attempt — Inspecting: ${profile.context.inspecting}`;
  const details=profile.context;
  const date=new Date(profile.started_at).toLocaleDateString('en-US',{timeZone:'America/Chicago'});
  $('metadata').innerHTML=[['Date',date],['Drawing #',details.recipe.drawing||'—'],['Machine #',details.machine_name||'—'],['Inspector',details.inspecting],['Workorder #',details.production_number],['Connection Type',details.connection]].map(([label,value])=>`<div><strong>${escape(label)}:</strong> ${escape(value)}</div>`).join('');
  $('specification').textContent=`WO ${profile.context.production_number} · ${profile.context.connection} · ${profile.scope==='full'?'Full':'Standard'} Inspection`;
  $('receipts').innerHTML=profile.receipts.slice(-15).reverse().map(receipt=>`<li>Pipe ${escape(receipt.pipe_number)} — ${receipt.pipe_status==='completed'?'received and passed':'received, held for rework'} (attempt ${escape(receipt.attempt_id)})</li>`).join('');
 }
 status();
}
async function prepare(){
 if(preparing||syncing)return;preparing=true;
 try{
  const response=await request('/api/tablet-inspection/context');
  if(!response.ok){const result=await response.json();throw new Error(result.error);}
  const context=await response.json();owner=context.owner;
  profile=await updateProfile(owner,current=>{
   if(current&&(current.events.length||Object.values(current.values).some(Boolean))&&(current.context.recipe_hash!==context.recipe_hash||current.context.coverage_version!==context.coverage_version&&(current.events.length||current.context.inspecting_adp!==context.inspecting_adp)))return current;
   if(current){
    const value={...current,context};
    if(context.active_pipe&&current.pipe_number!==context.pipe_number&&!Object.values(current.values).some(Boolean)){
     Object.assign(value,{pipe_number:context.pipe_number,attempt_no:context.attempt_no||1,scope:context.inspection_scope,values:context.initial_values||{},notes:'',started_at:new Date().toISOString()});
    }
    return value;
   }
   return {owner,context,events:[],receipts:[],drafts:{},pipe_number:context.pipe_number,attempt_no:context.attempt_no||1,scope:context.inspection_scope,values:context.initial_values||{},notes:'',started_at:new Date().toISOString()};
  });
  await currentOwner(owner);connected=true;rows();render();
 }catch(error){connected=false;message(['TypeError','AbortError','TimeoutError'].includes(error.name)?'Connection unavailable. Measurements stay saved on this tablet and will upload when the connection returns.':error.message||'Server is unavailable. Saved measurements have been kept.');render();}finally{preparing=false;}
}
async function sync(){
 if(syncing||!profile||!navigator.onLine)return;syncing=true;
 try{
  while(true){
   profile=await readProfile(owner);const event=profile.events[0];if(!event)break;
   const response=await request('/api/tablet-inspection/pipes',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({owner,event})});
   const result=await response.json();
   if(!response.ok){connected=response.status!==401;message(`Sync needs attention: ${result.error} All pending measurements remain on this tablet.`);break;}
   if(result.id!==event.id)throw new Error('Receipt did not match the saved pipe.');
   profile=await updateProfile(owner,current=>{current.events=current.events.filter(item=>item.id!==event.id);if(!current.receipts.some(item=>item.id===event.id))current.receipts.push(result);current.receipts=current.receipts.slice(-200);return current;});connected=true;
  }
 }catch{connected=false;}finally{syncing=false;render();}
}
async function draft(){
 const values={};for(const field of document.querySelectorAll('[data-sequence]'))values[field.dataset.sequence]=field.value;
 const notes=$('notes').value,expectedPipe=renderedPipe;let stale=false;
 profile=await updateProfile(owner,current=>{
  if(current.pipe_number!==expectedPipe||current.scope!==renderedScope){
   stale=true;
   current.recovery_drafts=current.recovery_drafts||[];
   const index=current.recovery_drafts.findIndex(item=>item.pipe_number===expectedPipe);
   const copy={pipe_number:expectedPipe,values:{...(index>=0?current.recovery_drafts[index].values:{}),...values},notes,saved_at:new Date().toISOString()};
   if(index>=0)current.recovery_drafts[index]=copy;else current.recovery_drafts.push(copy);
   return current;
  }
  current.values={...current.values,...values};current.notes=notes;return current;
 });
 if(stale)throw new Error('Another tab loaded a different pipe. Your draft copy was kept in the recovery download; reload before continuing.');
 validation();
}
for(const name of ['input','change'])$('measurements').addEventListener(name,()=>{void draft().catch(error=>message(`Measurement could not be saved: ${error.message}. Keep this screen open.`));});
$('measurements').addEventListener('submit',async event=>{
 event.preventDefault();if(working)return;working=true;
 try{
  await draft();const result=validation();if(result.missing)throw new Error('Inspect every required element before loading the next pipe.');
  if(!event.target.reportValidity())return;
  if(result.failed&&!profile.notes.trim())throw new Error('Enter an NCR note describing the failed measurement.');
  const number=profile.pipe_number,next=nextPipe(number);
  if(!next)throw new Error('Use a pipe number ending in digits so the next pipe can be loaded.');
  const saved={id:eventId(),at:new Date().toISOString(),started_at:profile.started_at,pipe_number:number,scope:profile.scope,
   values:{...profile.values},notes:profile.notes,recipe_hash:profile.context.recipe_hash,coverage_version:profile.context.coverage_version};
  profile=await updateProfile(owner,current=>{
   if(current.pipe_number!==number||current.scope!==saved.scope)throw new Error('Another tab loaded a different pipe. Reload this worksheet.');
   current.events.push(saved);current.pipe_number=next;current.values={};current.notes='';current.attempt_no=1;current.started_at=new Date().toISOString();return current;
  });
  message(`Pipe ${number} saved on this tablet${result.failed?' for rework and Quality review':''}. Pipe ${next} is ready.`);rows();render();void sync();
 }catch(error){message(error.message);}finally{working=false;}
});
$('load-pipe').addEventListener('submit',async event=>{
 event.preventDefault();const number=$('pipe-number').value.trim();if(!number)return;
 // Drafts are never discarded by changing a number; finish the current pipe first.
 if(Object.values(profile.values).some(Boolean)){message('Complete and save the current pipe before changing its number. Its measurements have been kept.');$('pipe-number').value=profile.pipe_number;return;}
 if(profile.events.some(item=>item.pipe_number===number)||profile.receipts.some(item=>item.pipe_number===number)){message('This pipe is already saved on this tablet. Use the online history workflow to review it.');return;}
 try{
  profile=await updateProfile(owner,current=>{
   if(current.pipe_number!==renderedPipe||Object.values(current.values).some(Boolean))throw new Error('Another tab changed this worksheet. Reload before loading a different pipe.');
   return {...current,pipe_number:number,attempt_no:1,started_at:new Date().toISOString()};
  });rows();render();
 }catch(error){message(error.message);}
});
$('scope').addEventListener('click',async()=>{try{await draft();profile=await updateProfile(owner,current=>({...current,scope:current.scope==='full'?'standard':'full'}));rows();render();}catch(error){message(error.message);}});
for(const button of ['lunch','break'])$(button).addEventListener('click',()=>{
 if(!connected)return;
 if(profile.events.length){message('Sync the saved pipes before starting lunch inspection or changing break coverage.');void sync();return;}
 window.open('/workflow/inspection?tabletAction='+button,'_blank','noopener');
});
$('sync').addEventListener('click',async()=>{await prepare();await sync();});
$('backup').addEventListener('click',async()=>{const data=await readProfile(owner),url=URL.createObjectURL(new Blob([JSON.stringify(data,null,2)],{type:'application/json'}));const link=document.createElement('a');link.href=url;link.download='benoit-inspection-recovery.json';link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);});
window.addEventListener('offline',()=>{connected=false;status();});window.addEventListener('online',async()=>{await prepare();await sync();});
document.addEventListener('visibilitychange',async()=>{if(!document.hidden){await prepare();await sync();}});
setInterval(async()=>{if(profile&&!syncing){try{connected=(await fetch('/api/tablet-inspection/health',{cache:'no-store',signal:AbortSignal.timeout(4000)})).ok;}catch{connected=false;}status();void sync();}},15000);
if('serviceWorker'in navigator&&window.isSecureContext){navigator.serviceWorker.addEventListener('controllerchange',status);navigator.serviceWorker.register('/inspection-sw.js',{scope:'/'}).then(()=>navigator.storage?.persist?.()).catch(()=>message('Offline files could not be prepared. Keep the connection until setup completes.'));}
try{owner=await currentOwner();if(owner)profile=await readProfile(owner);if(profile)rows();render();await prepare();await sync();}catch(error){message(`Tablet storage could not open: ${error.message}`);$('setup').hidden=false;}
window.tabletInspectionReady=true;
