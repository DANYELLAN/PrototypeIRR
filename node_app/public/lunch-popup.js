(() => {
 const button=document.getElementById('lunch-inspection-button');
 const dialog=document.getElementById('lunch-inspection-dialog');
 if(!button || !dialog)return;
 const content=document.getElementById('lunch-popup-content');
 const error=document.getElementById('lunch-popup-error');
 let busy=false;
 const request=async(path,form)=>{
  const response=await fetch('/workflow/lunch/popup'+path,form?{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams(new FormData(form)),cache:'no-store'}:{cache:'no-store'});
  const data=await response.json();
  if(!response.ok)throw new Error(data.error || 'Unable to load lunch inspection.');
  content.innerHTML=data.html;dialog.dataset.view=data.view;
  content.querySelector('input:not([type="hidden"]),select,button')?.focus();
 };
 button.addEventListener('click',async()=>{
  if(busy)return;
  if(!dialog.open)dialog.showModal();
  if(dialog.dataset.view==='worksheet')return;
  busy=true;error.textContent='';
  try{await request('');}catch(err){error.textContent=err.message;}finally{busy=false;}
 });
 dialog.addEventListener('click',event=>{if(event.target.closest('[data-lunch-close]')&&!busy)dialog.close();});
 dialog.addEventListener('cancel',event=>{if(busy)event.preventDefault();});
 dialog.addEventListener('submit',async event=>{
  const form=event.target;
  if(!form.matches('[data-lunch-identify],[data-lunch-complete]'))return;
  event.preventDefault();if(busy || !form.reportValidity())return;
  const submit=event.submitter;busy=true;error.textContent='';
  if(submit)submit.disabled=true;
  try{
   if(form.matches('[data-lunch-identify]')&&window.flushInspectionDraft)await window.flushInspectionDraft();
   await request(form.matches('[data-lunch-identify]')?'/start':'/complete',form);
  }catch(err){error.textContent=err.message;}finally{busy=false;if(submit)submit.disabled=false;}
 });
})();
