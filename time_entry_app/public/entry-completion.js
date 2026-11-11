const normalize=value=>String(value||'').toLowerCase().replace(/[^a-z0-9]/g,'');
export const completionMode=detail=>normalize(detail)==='setup'?'setup':['inspection','sandblast','drift','stenciling'].includes(normalize(detail))?'pipes':'quantity';
export function pipeNumber(value){
 const text=String(value??'').trim();
 if(text.length>16||!/^[0-9]+$/.test(text)||!Number.isSafeInteger(Number(text)))throw new Error('Enter a whole, non-negative pipe number.');
 return text;
}
export function pipeQuantity(start,end){
 const quantity=Number(pipeNumber(end))-Number(pipeNumber(start))+1;
 if(quantity<1)throw new Error('Ending pipe cannot be less than starting pipe.');
 if(quantity>1000000)throw new Error('The pipe range cannot exceed 1000000 pipes.');
 return quantity;
}
export function enteredQuantity(value){
 if(String(value??'').trim()===''||!Number.isInteger(Number(value))||Number(value)<0||Number(value)>1000000)throw new Error('Enter a whole quantity between 0 and 1000000.');
 return Number(value);
}
const escape=value=>String(value??'').replace(/[&<>"']/g,char=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[char]);

export function renderEntryCompletion(entry,tablet=false){
 const mode=completionMode(entry.detail_type);
 const hasStart=entry.starting_pipe!==null&&entry.starting_pipe!==undefined;
 const metadata=`data-entry-mode="${mode}" data-production-number="${escape(entry.production_number)}" data-operation-id="${escape(entry.operation_id)}" data-starting-pipe="${escape(entry.starting_pipe??'')}"`;
 const identity=`<input type="hidden" name="${tablet?'timer_id':'entry_id'}" value="${escape(entry.id)}">`;
 const form=(action,marker,body)=>`<form ${tablet?`data-action="${action}"`:`method="post" action="${action==='starting_pipe'?'/time/starting-pipe':'/time/stop'}" data-legacy-entry`} ${marker} ${metadata} class="cnc-inline-stop-form stop-submit-row">${identity}${body}</form>`;
 const start=mode==='pipes'?form('starting_pipe','data-entry-starting-pipe',`<input type="hidden" name="starting_pipe"><button class="cnc-button ghost" type="submit">${hasStart?'Change Starting Pipe':'Enter Starting Pipe'}</button>${hasStart?`<span class="cnc-chip secondary">Starting Pipe: ${escape(entry.starting_pipe)}</span>`:''}`):'';
 const end=form('stop','data-entry-completion',`<input type="hidden" name="${mode==='pipes'?'ending_pipe':'quantity'}"><button class="cnc-button danger" type="submit" ${entry.downtime||mode==='pipes'&&!hasStart?'disabled':''} ${mode==='pipes'&&!hasStart?'title="Enter starting pipe first"':''}>End Entry</button>`);
 return start+end;
}

export function createEntryCompletionDialog(onConfirm){
 const dialog=document.createElement('dialog');dialog.className='cnc-relief-dialog cnc-entry-completion-dialog';dialog.setAttribute('aria-labelledby','entry-completion-heading');
 dialog.innerHTML='<div class="relief-dialog-header"><p class="relief-eyebrow">TIME ENTRY</p><h2 id="entry-completion-heading"></h2><p data-completion-job></p></div><form class="cnc-form relief-dialog-body"><p data-completion-start></p><label><span data-completion-label></span><input name="value" type="text" inputmode="numeric" pattern="[0-9]+" autocomplete="off" required></label><output data-completion-preview></output><p data-completion-error role="alert"></p><div class="relief-dialog-actions"><button type="button" class="cnc-button ghost" data-completion-cancel>Cancel</button><button type="submit" class="cnc-button" data-completion-confirm></button></div></form>';
 document.body.append(dialog);let pending=null,busy=false;
 const form=dialog.querySelector('form'),input=form.elements.value,cancel=dialog.querySelector('[data-completion-cancel]'),button=dialog.querySelector('[data-completion-confirm]');
 const preview=()=>{
  const output=dialog.querySelector('[data-completion-preview]');output.textContent='';
  if(pending?.kind==='stop'&&pending.mode==='pipes'&&input.value){try{output.textContent=`Quantity: ${pipeQuantity(pending.starting_pipe,input.value)}`;}catch{}}
 };
 input.addEventListener('input',()=>{dialog.querySelector('[data-completion-error]').textContent='';preview();});
 cancel.addEventListener('click',()=>{if(!busy){pending=null;dialog.close();}});
 dialog.addEventListener('cancel',event=>{if(busy)event.preventDefault();else pending=null;});
 form.addEventListener('submit',async event=>{
  event.preventDefault();if(busy||!pending||!form.reportValidity())return;
  const error=dialog.querySelector('[data-completion-error]');error.textContent='';
  try{
   let values;
   if(pending.kind==='starting_pipe')values={starting_pipe:pipeNumber(input.value)};
   else if(pending.mode==='pipes')values={ending_pipe:pipeNumber(input.value),quantity:pipeQuantity(pending.starting_pipe,input.value)};
   else values={quantity:enteredQuantity(input.value)};
   busy=true;button.disabled=true;cancel.disabled=true;input.disabled=true;
   await onConfirm(pending,values);pending=null;dialog.close();
  }catch(failure){error.textContent=failure.message||'The entry could not be saved. Try again.';}
  finally{busy=false;button.disabled=false;cancel.disabled=false;input.disabled=false;}
 });
 return {show(state){
  if(busy)return;pending=state;form.reset();
  dialog.querySelector('#entry-completion-heading').textContent=state.kind==='starting_pipe'?'Enter starting pipe':state.mode==='pipes'?'Enter ending pipe':'Enter quantity';
  dialog.querySelector('[data-completion-label]').textContent=state.kind==='starting_pipe'?'Starting Pipe Number':state.mode==='pipes'?'Ending Pipe Number':'Quantity';
  dialog.querySelector('[data-completion-job]').textContent=`WO ${state.production_number} · Operation ${state.operation_id}`;
  dialog.querySelector('[data-completion-start]').textContent=state.kind==='stop'&&state.mode==='pipes'?`Starting Pipe: ${state.starting_pipe}`:'';
  dialog.querySelector('[data-completion-error]').textContent='';preview();
  button.textContent=state.kind==='starting_pipe'?'Save Starting Pipe':'Confirm And Submit';
  if(!dialog.open)dialog.showModal();input.focus();
 }};
}

if(typeof document!=='undefined'){
 const forms=document.querySelectorAll('form[data-legacy-entry]');
 if(forms.length){
  const confirmed=new WeakSet();
  const popup=createEntryCompletionDialog(async(state,values)=>{
   const form=state.form;
   for(const [key,value]of Object.entries(values))if(form.elements[key])form.elements[key].value=value;
   confirmed.add(form);form.requestSubmit();return new Promise(()=>{});
  });
  forms.forEach(form=>form.addEventListener('submit',event=>{
   if(confirmed.has(form))return;event.preventDefault();
   popup.show({form,kind:form.hasAttribute('data-entry-starting-pipe')?'starting_pipe':'stop',mode:form.dataset.entryMode,starting_pipe:form.dataset.startingPipe,
    production_number:form.dataset.productionNumber,operation_id:form.dataset.operationId});
  }));
 }
}
