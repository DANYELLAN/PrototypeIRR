const centralClock=new Intl.DateTimeFormat('en-US',{timeZone:'America/Chicago',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
export function lunchSchedule(shift='40') {
 const night=['41','night shift','night'].includes(String(shift).trim().toLowerCase());
 return night?{key:'night',hour:'22',minute:'00',label:'10:00 PM'}:{key:'day',hour:'11',minute:'30',label:'11:30 AM'};
}
export function lunchTime(date=new Date(),shift='40') {
 const parts=Object.fromEntries(centralClock.formatToParts(date).map(part=>[part.type,part.value]));
 const schedule=lunchSchedule(shift);
 return {day:`${parts.year}-${parts.month}-${parts.day}`,due:parts.hour===schedule.hour&&parts.minute===schedule.minute};
}
export function lateLunchAvailable(date=new Date(),shift='40',day){
 const p=Object.fromEntries(centralClock.formatToParts(date).map(part=>[part.type,part.value]));
 return lunchSchedule(shift).key==='day'&&`${p.year}-${p.month}-${p.day}`===day&&(Number(p.hour)*60+Number(p.minute)>=750);
}
export function lunchForm(root=document) {
 return [...root.querySelectorAll('form[action="/time/pause"],form[data-action="pause"]')].find(form=>form.elements.break_type?.value==='Lunch');
}
// Reuse the existing lunch action, including offline storage and server validation.
export function installLunchReminder({getActive,doc=document,win=window,now=()=>new Date(),storage}={}) {
 if(storage===undefined){try{storage=win.localStorage;}catch{}}
 const seen=new Set();let dialog,pendingDay=null,prompted=null,restoreFocus,logBusy=false;
 const wasSeen=key=>{try{return seen.has(key)||storage?.getItem(key)==='shown';}catch{return seen.has(key);}};
 const dismiss=()=>{dialog?.close();prompted=null;restoreFocus?.focus?.();};
 function check(){
  if(doc.hidden)return;
  const active=getActive(),schedule=lunchSchedule(active?.shift),clock=lunchTime(now(),active?.shift);
  const pendingKey=clock.day+':'+schedule.key;
  if(pendingDay!==pendingKey)pendingDay=null;
  if(dialog?.open){
   if(logBusy)return;
   if(!active||(active.token!==prompted?.token||active.owner!==prompted?.owner))dismiss();
   else dialog.querySelector('[data-lunch-log]').hidden=!active.logLunch||!lateLunchAvailable(now(),active.shift,prompted.day);
   return;
  }
  if(clock.due&&active)pendingDay=pendingKey;
  if(!pendingDay||!active||doc.querySelector('dialog[open]'))return;
  const key=`benoit.time.lunch-reminder:${clock.day}:${active.owner}`;
  if(wasSeen(key))return;
  if(!dialog){
   dialog=doc.createElement('dialog');dialog.id='lunch-reminder-dialog';dialog.className='cnc-relief-dialog cnc-lunch-dialog';
   dialog.setAttribute('aria-labelledby','lunch-reminder-heading');dialog.setAttribute('aria-describedby','lunch-reminder-description');
   dialog.innerHTML=`<div class="relief-dialog-header"><p class="relief-eyebrow">CNC Time Entry / Lunch</p><h2 id="lunch-reminder-heading">It's lunch time</h2><p id="lunch-reminder-description">Do you want to start your lunch now?</p></div><div class="relief-dialog-body"><p class="lunch-reminder-time"></p><p class="lunch-reminder-note">Yes starts your lunch break for the current entry. You can resume production when you return.</p><p data-lunch-error role="alert"></p><div class="relief-dialog-actions"><button type="button" class="cnc-button ghost" data-lunch-no>No</button><button type="button" class="cnc-button" data-lunch-yes>Yes</button><button type="button" class="cnc-button" data-lunch-log hidden>Log Lunch</button></div></div>`;
   doc.body.append(dialog);
   dialog.querySelector('[data-lunch-no]').addEventListener('click',dismiss);
   dialog.addEventListener('cancel',event=>{event.preventDefault();if(!logBusy)dismiss();});
   dialog.querySelector('[data-lunch-log]').addEventListener('click',async()=>{
    if(logBusy)return;
    const current=getActive();
    if(!current||current.token!==prompted?.token||current.owner!==prompted?.owner){dismiss();return;}
    if(!current.logLunch||!lateLunchAvailable(now(),current.shift,prompted.day))return;
    logBusy=true;dialog.querySelectorAll('button').forEach(button=>{button.disabled=true;});
    dialog.querySelector('[data-lunch-error]').textContent='';
    try {await current.logLunch(prompted.day);dismiss();}
    catch(error){dialog.querySelector('[data-lunch-error]').textContent=error.message||'Lunch could not be logged. Try again.';}
    finally{logBusy=false;dialog.querySelectorAll('button').forEach(button=>{button.disabled=false;});}
   });
   dialog.querySelector('[data-lunch-yes]').addEventListener('click',()=>{
    const current=getActive();
    if(!current||(current.token!==prompted?.token||current.owner!==prompted?.owner)){dismiss();return;}
    const form=current.form;dismiss();form.requestSubmit();
   });
  }
  prompted={token:active.token,owner:active.owner,day:clock.day};restoreFocus=doc.activeElement;
  dialog.querySelector('[data-lunch-log]').hidden=!active.logLunch||!lateLunchAvailable(now(),active.shift,clock.day);
  dialog.querySelector('.lunch-reminder-time').innerHTML=`${schedule.label} <span>Central Time</span>`;
  dialog.showModal();dialog.querySelector('[data-lunch-no]').focus();
  seen.add(key);try{storage?.setItem(key,'shown');}catch{}
 }
 const timer=win.setInterval(check,1000);
 doc.addEventListener('visibilitychange',check);check();
 win.addEventListener('pagehide',()=>{win.clearInterval(timer);doc.removeEventListener('visibilitychange',check);},{once:true});
 return check;
}
