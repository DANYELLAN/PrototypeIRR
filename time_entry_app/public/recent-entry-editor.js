document.addEventListener('click',event=>{
 const edit=event.target.closest('[data-edit-entry]');
 if(edit){
  const dialog=edit.parentElement.querySelector('.cnc-correction-dialog');
  if(!dialog||dialog.open)return;
  dialog.showModal();
  return;
 }
 const cancel=event.target.closest('[data-edit-cancel]');
 if(cancel)cancel.closest('dialog').close();
});

document.querySelectorAll('.cnc-correction-dialog').forEach(dialog=>{
 dialog.addEventListener('close',()=>{
  const form=dialog.querySelector('form');
  if(form.dataset.submitting)return;
  form.reset();
  form.elements.production_number?.dispatchEvent(new Event('change',{bubbles:true}));
 });
 dialog.querySelector('form').addEventListener('submit',event=>{
  // Keep the existing POST; dismiss the top-layer dialog so loading feedback is visible.
  if(event.defaultPrevented)return;
  event.target.dataset.submitting='true';
  dialog.close();
 });
});

window.addEventListener('pageshow',()=>{
 document.querySelectorAll('.cnc-correction-dialog form').forEach(form=>delete form.dataset.submitting);
});
