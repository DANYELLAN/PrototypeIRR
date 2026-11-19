import {currentOwner,readProfile} from './inspection-store.js';
// Logging out locks the local pointer; queued records are retained in IndexedDB.
if(document.querySelector('form[action="/login"]'))void currentOwner(null).catch(()=>{});
document.addEventListener('submit',async event=>{
 const form=event.target;
 if(!(form instanceof HTMLFormElement)||event.defaultPrevented)return;
 const route=new URL(form.action,location.href).pathname;
 if(!['/logout','/workflow/end-shift','/login','/setup/machinist'].includes(route))return;
 event.preventDefault();
 try{
  const owner=await currentOwner();const profile=owner?await readProfile(owner):null;
  if(route==='/setup/machinist' && (profile?.events?.length || Object.values(profile?.values || {}).some(Boolean))){
   let savedSession;try{savedSession=JSON.parse(owner)[0];}catch{}
   const sameSetup=String(savedSession)===form.dataset.existingSession && form.dataset.existingMachinist && form.elements.operator_item_id.value===form.dataset.existingMachinist;
   if(!sameSetup){alert('Save and sync the current tablet worksheet before changing its setup. Its measurements have been kept.');return;}
  }
  if(route==='/setup/machinist'){form.submit();return;}
  if(route!=='/login'&&profile?.events?.length){alert('Saved work is still waiting on this tablet. Open the tablet worksheet and sync it before logging out or ending the shift.');return;}
  await currentOwner(null);form.submit();
 }catch{alert('The tablet could not lock its saved workspace. Keep this page open and retry before logging out.');}
});

function openRequestedTabletAction(){
 const action=new URLSearchParams(location.search).get('tabletAction');
 const id=action==='lunch'?'lunch-inspection-button':action==='break'?'inspection-break-button':null;
 if(id)document.getElementById(id)?.click();
}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',openRequestedTabletAction,{once:true});else setTimeout(openRequestedTabletAction,0);
