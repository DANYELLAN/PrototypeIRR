(() => {
 const workOrder=document.getElementById('mold-work-order');
 const detail=document.getElementById('mold-work-detail');
 if(workOrder && detail){
  const updateDetails=(changed=false)=>{
   if(changed) detail.value='';
   const available=[];
   for(const option of detail.options){
    if(!option.value) continue;
    const matches=option.dataset.workOrder===workOrder.value;
    option.hidden=!matches;option.disabled=!matches;
    if(matches) available.push(option);
   }
   if(!available.some(option=>option.selected)) detail.value=available.length===1?available[0].value:'';
   detail.disabled=!workOrder.value || !available.length;
   workOrder.setCustomValidity(workOrder.value && !available.length?'This work order has no available details. Select another work order.':'');
  };
  workOrder.addEventListener('change',()=>updateDetails(true));
  updateDetails();
 }

 const panel=document.querySelector('[data-mold-watch]');
 if(!panel || panel.dataset.moldState!=='pending') return;
 let busy=false;
 const refresh=async()=>{
  if(busy || document.hidden) return;
  busy=true;
  try {
   const response=await fetch(`/workflow/mold-approval/status.json?id=${encodeURIComponent(panel.dataset.moldWatch)}`,{cache:'no-store'});
   if(!response.ok) throw new Error('Refresh failed');
   const row=await response.json();
   panel.querySelector('[data-mold-status]').textContent=row.status_label;
   const message=row.refresh_error || [row.responder_name,row.response_note,row.responded_at].filter(Boolean).join(' · ');
   panel.querySelector('[data-mold-response]').textContent=message;
   document.querySelector(`[data-mold-row-status="${row.id}"]`).textContent=row.status_label;
   document.querySelector(`[data-mold-row-response="${row.id}"]`).textContent=message;
   panel.dataset.moldState=row.status;
   if(row.status!=='pending') clearInterval(timer);
  } catch {panel.querySelector('[data-mold-response]').textContent='Unable to refresh the Quality response. Retrying.';}
  finally {busy=false;}
 };
 const timer=setInterval(refresh,10000);
 refresh();
})();
