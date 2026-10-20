(() => {
  let timer, pending;
  const running = new Map();
  const send = payload => {
    if (running.has(payload)) return running.get(payload);
    const request = (async () => {
      const response = await fetch('/workflow/live-draft', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload),keepalive:true});
      if (!response.ok) throw new Error('The last measurements could not be saved before changing break coverage. Try again.');
      if (pending === payload) pending = null;
    })();
    running.set(payload,request);
    request.finally(()=>running.delete(payload)).catch(()=>{});
    return request;
  };
  const capture = event => {
    const form=event.target.closest?.('#inspection-complete-form');
    const attempt=form?.querySelector('[name="attempt_id"]')?.value;
    if(!attempt || !event.target.matches('.measurement-input'))return;
    const values={};
    for(const field of form.querySelectorAll('[name^="element_"]'))values[field.name]=field.value;
    const fieldName=event.target.name || event.target.dataset.fullTarget || '';
    pending={attempt_id:attempt,values,client_sequence:Date.now(),coverage_version:Number(form.querySelector('[name="_coverage_version"]')?.value || 0),edited_sequence:fieldName.match(/(\d+)$/)?.[1]};
    clearTimeout(timer);
    const snapshot=pending;
    timer=setTimeout(()=>{if(pending===snapshot)void send(snapshot).catch(()=>{});},600);
  };
  window.flushInspectionDraft=async()=>{
    clearTimeout(timer);
    const current=document.querySelector('#inspection-complete-form [name="attempt_id"]')?.value;
    if(pending && String(pending.attempt_id)!==String(current))pending=null;
    await Promise.all([...running.entries()].filter(([payload])=>String(payload.attempt_id)===String(current)).map(([,request])=>request));
    if(pending)await send(pending);
  };
  document.addEventListener('input',capture);
  document.addEventListener('change',capture);
  window.addEventListener('pagehide',()=>{if(pending)void send(pending).catch(()=>{});});
  setInterval(()=>{
    if(!pending)return;
    const current=document.querySelector('#inspection-complete-form [name="attempt_id"]')?.value;
    if(String(current)!==String(pending.attempt_id)){pending=null;return;}
    void send(pending).catch(()=>{});
  },5000);
})();
