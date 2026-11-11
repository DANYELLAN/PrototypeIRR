export function createDowntimeDialog(onEnd) {
 const dialog=document.createElement('dialog');
 dialog.className='cnc-downtime-dialog';
 dialog.setAttribute('aria-labelledby','downtime-heading');
 dialog.innerHTML='<div class="downtime-dialog-header"><span class="downtime-pause-mark" aria-hidden="true">II</span><p>PRODUCTION PAUSED</p><h2 id="downtime-heading">Downtime is being logged</h2></div><div class="downtime-dialog-body"><p data-downtime-job></p><strong data-downtime-reason></strong><p data-downtime-comment></p><output class="downtime-clock" role="timer" aria-label="Elapsed downtime">00:00:00</output><p data-downtime-error role="alert"></p><button class="cnc-button" type="button" data-end-downtime>End Downtime &amp; Resume Production</button></div>';
 document.body.append(dialog);
 let active=null,busy=false;
 const button=dialog.querySelector('[data-end-downtime]');
 const tick=()=>{
  if(!active)return;
  const seconds=Math.max(0,Math.floor((Date.now()-Date.parse(active.start))/1000));
  dialog.querySelector('.downtime-clock').textContent=[Math.floor(seconds/3600),Math.floor(seconds/60)%60,seconds%60].map(value=>String(value).padStart(2,'0')).join(':');
 };
 const clock=setInterval(tick,1000);
 window.addEventListener('pagehide',()=>clearInterval(clock),{once:true});
 dialog.addEventListener('cancel',event=>event.preventDefault());
 button.addEventListener('click',async()=>{
  if(busy||!active)return;
  busy=true;button.disabled=true;dialog.querySelector('[data-downtime-error]').textContent='';
  try { await onEnd(active); }
  catch(error) { dialog.querySelector('[data-downtime-error]').textContent=error.message||'Downtime could not be saved. Try again.'; }
  finally {busy=false;button.disabled=false;}
 });
 return {
  show(value){
   active=value;
   dialog.querySelector('[data-downtime-job]').textContent=`WO ${value.production_number} · Operation ${value.operation_id}`;
   dialog.querySelector('[data-downtime-reason]').textContent=value.reason_label||value.reason;
   dialog.querySelector('[data-downtime-comment]').textContent=value.comments||'';
   tick();if(!dialog.open)dialog.showModal();
  },
  hide(){active=null;if(dialog.open)dialog.close();},
 };
}

const marker=document.querySelector('[data-timed-downtime-state]');
if(marker){
 const state=JSON.parse(marker.dataset.timedDowntimeState);
 const popup=createDowntimeDialog(()=>{
  marker.querySelector('form').requestSubmit();
  return new Promise(()=>{});
 });
 popup.show(state);
}
