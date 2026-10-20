(() => {
 let refreshing=false,receivedAt=performance.now();
 const tick=()=>document.querySelectorAll('[data-active-duration]').forEach(el=>{const seconds=Math.max(0,Math.floor(Number(el.dataset.activeDuration)+(performance.now()-receivedAt)/1000));el.textContent=`${Math.floor(seconds/3600)}h ${String(Math.floor(seconds%3600/60)).padStart(2,'0')}m ${String(seconds%60).padStart(2,'0')}s`;});
 const refresh=async()=>{
  if(refreshing||document.hidden)return;refreshing=true;
  try{
   const response=await fetch('/admin/breaks/live.json'+window.location.search,{cache:'no-store'});
   if(!response.ok)throw new Error(response.status===401||response.status===403?'Report access expired. Sign in again.':'Break report refresh failed. Displayed records may be out of date.');
   const payload=await response.json(),current=document.getElementById('admin-break-report');
   const next=new DOMParser().parseFromString(payload.html,'text/html').getElementById('admin-break-report');
   if(!next)throw new Error('Break report refresh failed.');
   const left=current.querySelector('[data-break-table]').scrollLeft;current.replaceWith(next);next.querySelector('[data-break-table]').scrollLeft=left;
   receivedAt=performance.now();tick();document.getElementById('break-report-error').textContent='';
   document.getElementById('break-report-freshness').textContent=`Updated ${new Date(payload.as_of).toLocaleTimeString('en-US',{timeZone:'America/Chicago'})} · live refresh every 15 seconds`;
  }catch(err){document.getElementById('break-report-error').textContent=err.message;}
  finally{refreshing=false;}
 };
 setInterval(tick,1000);setInterval(refresh,15000);
 document.addEventListener('visibilitychange',()=>{if(!document.hidden)void refresh();});
})();
