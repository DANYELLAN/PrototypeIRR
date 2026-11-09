// Probe the real server page before navigating. A healthy API alone does not prove
// that /time is ready (an older server may redirect it straight back to this shell).
export function createReviewReturn({request,navigate,storage,embedded=false,canNavigate=()=>true,now=()=>Date.now()}) {
 const key='benoit.time.review-navigation-at';
 let checking=false,navigating=false,lastCheck=-Infinity;
 return async()=>{
  if(embedded||checking||navigating||!canNavigate()||now()-lastCheck<10000)return false;
  try { const previous=storage?.getItem(key);if(previous!==null&&previous!==undefined&&now()-Number(previous)<30000)return false; }catch{}
  checking=true;lastCheck=now();
  try {
   const response=await request('/time',{headers:{Accept:'text/html'}});
   if(!response.ok)return false;
   const html=await response.text();
   if(!html.includes('id="recent-entries"')||!html.includes('action="/time/start"')||!canNavigate())return false;
   try { storage?.setItem(key,String(now())); }catch{}
   navigating=true;navigate('/time');return true;
  }catch{return false;}
  finally{checking=false;}
 };
}
