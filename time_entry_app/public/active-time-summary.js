export function timeTotals(timing, now=Date.now()) {
 const elapsed=Math.max(0,Math.floor((now-Date.parse(timing.start))/60000));
 const currentBreak=timing.break_started_at&&timing.break_deducts?Math.max(0,Math.floor((now-Date.parse(timing.break_started_at))/60000)):0;
 const breaks=Math.max(0,Number(timing.break_minutes)||0)+currentBreak;
 const downtime=Math.max(0,Number(timing.downtime_minutes)||0);
 return {production:Math.max(0,elapsed-breaks-downtime),breaks,downtime};
}
export function totalsMarkup(totals) {
 return [['Production',totals.production],['Break',totals.breaks],['Downtime',totals.downtime]].map(([label,value])=>`<span><small>${label}</small><strong>${Math.floor(value/60)}h ${value%60}m</strong></span>`).join('');
}
export function showActiveTimeSummary(entry) {
 const form=document.querySelector('form[action="/time/stop"]');
 if(!form||!entry?.timing||form.elements.entry_id.value!==String(entry.sp_id)||!Number.isFinite(Date.parse(entry.timing.start)))return;
 const totals=document.createElement('div');totals.className='active-time-totals';totals.setAttribute('role','timer');totals.setAttribute('aria-label','Production, break and downtime totals');
 const row=document.createElement('div');row.className='active-stop-and-timers';
 form.before(row);row.append(form,totals);
 const render=()=>{totals.innerHTML=totalsMarkup(timeTotals(entry.timing));};
 render();const clock=setInterval(render,1000);
 window.addEventListener('pagehide',()=>clearInterval(clock),{once:true});
}
