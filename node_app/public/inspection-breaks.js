(() => {
  const controls=document.getElementById('inspection-break-controls');
  if(!controls)return;
  let state=JSON.parse(controls.dataset.state), busy=false;
  const dialog=document.getElementById('inspection-break-dialog');
  const button=document.getElementById('inspection-break-button');
  const label=document.getElementById('inspection-coverage-label');
  const description=document.getElementById('inspection-break-description');
  const choices=document.getElementById('inspection-break-choices');
  const reliefForm=document.getElementById('inspection-relief-form');
  const end=document.getElementById('inspection-break-end');
  const cancel=document.getElementById('inspection-break-cancel');
  const error=document.getElementById('inspection-break-error');
  const dialogError=document.getElementById('inspection-break-dialog-error');
  const syncWorksheet=()=>{
    for(const form of document.querySelectorAll('#inspection-complete-form, .inspection-scope-toggle-form, #inspection-selection-form')) {
      let version=form.querySelector('[name="_coverage_version"]');
      if(!version){version=document.createElement('input');version.type='hidden';version.name='_coverage_version';form.append(version);}
      version.value=state.version;
    }
    const worksheet=document.querySelector('#inspection-complete-form');
    const people=JSON.parse(worksheet?.dataset.reliefInspectors || '[]');
    const outlined=state.mode==='with_relief'||people.length>0;
    for(const cell of worksheet?.querySelectorAll('.inspection-col-current,.inspection-measure-cell,.inspection-result-cell') || [])cell.classList.toggle('relief-pipe-column',outlined);
    const header=worksheet?.querySelector('.inspection-col-current');
    if(header){
      let note=header.querySelector('[data-current-relief-label]');
      if(!note){note=document.createElement('small');note.className='relief-pipe-label';note.dataset.currentReliefLabel='';header.append(note);}
      const shown=state.mode==='with_relief'&&!people.some(p=>p.adp_number===state.relief_adp)?[...people,{name:state.relief_name,adp_number:state.relief_adp}]:people;
      note.textContent=shown.length?'Relief: '+shown.map(p=>`${p.name} (ADP ${p.adp_number})`).join(', '):'';
    }
    const inspector=document.querySelector('[data-current-inspector-name]');
    if(inspector)inspector.textContent=state.acting_name;
  };
  const render=()=>{
    controls.dataset.state=JSON.stringify(state);
    button.textContent=state.mode?'End Break':'Break';
    label.textContent=state.mode==='with_relief'?`Relief: ${state.relief_name} (ADP ${state.relief_adp}) · ${state.original_name} on break`:state.mode==='without_relief'?`${state.original_name} on break — inspection paused`:`Inspecting: ${state.original_name}`;
    choices.hidden=Boolean(state.mode);
    reliefForm.hidden=true;
    end.hidden=state.mode!=='without_relief';
    cancel.hidden=Boolean(state.mode);
    description.textContent=state.mode==='without_relief'?'Inspection is paused. Click End Break when you return.':'Will another inspector cover this break?';
    syncWorksheet();
    if(state.mode==='without_relief'&&!dialog.open)dialog.showModal();
    else if(state.mode!=="without_relief"&&dialog.open)dialog.close();
    window.dispatchEvent(new CustomEvent('inspectioncoveragechange',{detail:state}));
  };
  const refresh=async()=>{
    const response=await fetch('/workflow/break/state',{cache:'no-store'});
    const data=await response.json();if(!response.ok)throw new Error(data.error||'Unable to read break status.');
    if(data.version!==state.version){state=data;render();}
  };
  const transition=async(action,extra={})=>{
    if(busy)return;
    busy=true;error.textContent='';dialogError.textContent='';
    controls.querySelectorAll('button').forEach(el=>el.disabled=true);
    dialog.querySelectorAll('button').forEach(el=>el.disabled=true);
    // Freeze new edits while flushing the previous inspector's last draft and changing coverage.
    const workspace=document.getElementById('inspection-workspace');
    if(workspace)workspace.inert=true;
    try {
      if(window.flushInspectionDraft)await window.flushInspectionDraft();
      const response=await fetch(`/workflow/break/${action}`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({expected_version:state.version,...extra})});
      const data=await response.json();if(!response.ok)throw new Error(data.error||'Unable to change break status.');
      state=data;if(dialog.open)dialog.close();reliefForm.reset();render();
    } catch(err){error.textContent=err.message;dialogError.textContent=err.message;try{await refresh();}catch{}}
    finally{busy=false;if(workspace)workspace.inert=false;controls.querySelectorAll('button').forEach(el=>el.disabled=false);dialog.querySelectorAll('button').forEach(el=>el.disabled=false);}
  };
  button.addEventListener('click',()=>{if(state.mode)void transition('end');else {choices.hidden=false;reliefForm.hidden=true;dialogError.textContent='';dialog.showModal();}});
  end.addEventListener('click',()=>void transition('end'));
  cancel.addEventListener('click',()=>dialog.close());
  dialog.addEventListener('cancel',event=>{if(state.mode==='without_relief'||busy)event.preventDefault();});
  for(const choice of document.querySelectorAll('[data-break-choice]'))choice.addEventListener('click',()=>{
    if(choice.dataset.breakChoice==='without_relief')void transition('start',{mode:'without_relief'});
    else{choices.hidden=true;reliefForm.hidden=false;reliefForm.elements.relief_adp.focus();}
  });
  reliefForm.addEventListener('submit',event=>{event.preventDefault();void transition('start',{mode:'with_relief',relief_adp:reliefForm.elements.relief_adp.value});});
  document.addEventListener('input',event=>{
    if(state.mode!=='with_relief'||!event.target.matches?.('.measurement-input'))return;
    const form=event.target.closest('#inspection-complete-form');if(!form)return;
    const people=JSON.parse(form.dataset.reliefInspectors || '[]');
    if(!people.some(p=>p.adp_number===state.relief_adp))people.push({name:state.relief_name,adp_number:state.relief_adp});
    form.dataset.reliefInspectors=JSON.stringify(people);
  });
  const workspace=document.getElementById('inspection-workspace');
  if(workspace)new MutationObserver(syncWorksheet).observe(workspace.parentElement,{childList:true,subtree:false});
  render();
  setInterval(()=>{if(!document.hidden&&!busy)void refresh().catch(err=>{error.textContent=err.message;});},5000);
})();
