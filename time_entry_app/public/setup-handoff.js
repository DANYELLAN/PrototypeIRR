export const isSetup=detail=>String(detail||'').toLowerCase().replace(/[^a-z0-9]/g,'')==='setup';
export const defaultNextDetail=details=>details.find(item=>item.title.trim().toLowerCase()==='machining')?.title||'';

export function createSetupDialog(onConfirm) {
 const dialog=document.createElement('dialog');
 dialog.className='cnc-relief-dialog cnc-setup-dialog';
 dialog.setAttribute('aria-labelledby','setup-detail-heading');
 dialog.innerHTML='<div class="relief-dialog-header"><p class="relief-eyebrow">SET-UP COMPLETE</p><h2 id="setup-detail-heading">Confirm detail type</h2><p data-setup-job></p></div><form class="cnc-form relief-dialog-body"><label>Detail Type<select name="detail_type" required></select></label><p data-setup-error role="alert"></p><div class="relief-dialog-actions"><button class="cnc-button ghost" type="button" data-setup-cancel>Cancel</button><button class="cnc-button" type="submit">Confirm</button></div></form>';
 document.body.append(dialog);
 let pending=null,busy=false;
 const select=dialog.querySelector('select');
 const cancel=dialog.querySelector('[data-setup-cancel]');
 cancel.addEventListener('click',()=>{if(!busy){pending=null;dialog.close();}});
 dialog.addEventListener('cancel',event=>{if(busy)event.preventDefault();else pending=null;});
 dialog.querySelector('form').addEventListener('submit',async event=>{
  event.preventDefault();if(!pending||busy||!event.target.reportValidity())return;
  busy=true;cancel.disabled=true;select.disabled=true;const button=event.target.querySelector('[type="submit"]');button.disabled=true;
  dialog.querySelector('[data-setup-error]').textContent='';
  try {await onConfirm(pending,select.value);pending=null;dialog.close();}
  catch(error){dialog.querySelector('[data-setup-error]').textContent=error.message||'Set-Up could not be ended. Try again.';}
  finally {busy=false;cancel.disabled=false;select.disabled=false;button.disabled=false;}
 });
 return {show(state,details){
  if(busy)return;
  pending=state;select.replaceChildren();
  select.add(new Option('Select detail type',''));
  for(const item of details)select.add(new Option(item.title,item.title));
  select.value=defaultNextDetail(details);
  dialog.querySelector('[data-setup-job]').textContent=`WO ${state.production_number} · Operation ${state.operation_id}`;
  dialog.querySelector('[data-setup-error]').textContent='';
  if(!dialog.open)dialog.showModal();select.focus();
 }};
}

if(typeof document!=='undefined'){
 const form=document.querySelector('form[data-setup-handoff]');
 if(form){
  let confirmed=false;
  const popup=createSetupDialog(async(_state,detail)=>{
   form.elements.detail_type.value=detail;confirmed=true;form.requestSubmit();
   return new Promise(()=>{});
  });
  form.addEventListener('submit',event=>{
   if(confirmed)return;
   event.preventDefault();
   popup.show({entry_id:form.elements.entry_id.value,production_number:form.dataset.productionNumber,operation_id:form.dataset.operationId},JSON.parse(form.dataset.setupDetails));
  });
 }
}
