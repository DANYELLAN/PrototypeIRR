(() => {
 const panel=()=>document.getElementById('app-loading');
 let timeout;
 function hide(){clearTimeout(timeout);if(panel())panel().hidden=true;document.querySelector('main')?.removeAttribute('aria-busy');}
 function show(message='Loading Time Entry…'){
  if(!panel())return;
  panel().querySelector('[data-loading-message]').textContent=message;
  panel().hidden=false;document.querySelector('main')?.setAttribute('aria-busy','true');
  clearTimeout(timeout);timeout=setTimeout(hide,30000);
 }
 window.appLoading={show,hide};
 document.addEventListener('DOMContentLoaded',()=>{
  panel()?.querySelector('button').addEventListener('click',hide);
  if(document.body.dataset.tabletLoading!=='true'||window.tabletTimeReady)hide();
  else show();
 });
 window.addEventListener('pageshow',()=>{if(document.body.dataset.tabletLoading!=='true'||window.tabletTimeReady)hide();});
 document.addEventListener('click',event=>{
  if(event.button!==0||event.ctrlKey||event.metaKey||event.shiftKey||event.altKey)return;
  const link=event.target.closest('a[href]');
  if(!link||link.hasAttribute('download')||(link.target&&link.target!=='_self'))return;
  const next=new URL(link.href,location.href);
  if(next.origin!==location.origin||next.protocol!==location.protocol||next.pathname.endsWith('.csv')||(next.pathname===location.pathname&&next.search===location.search&&next.hash))return;
  setTimeout(()=>{if(!event.defaultPrevented)show();},0);
 });
 document.addEventListener('submit',event=>{
  const form=event.target;
  if((form.target&&form.target!=='_self')||form.method==='dialog')return;
  const target=new URL(event.submitter?.getAttribute('formaction')||form.action,location.href);
  if(target.origin!==location.origin||target.pathname==='/export'||target.pathname.endsWith('.csv'))return;
  setTimeout(()=>{if(!event.defaultPrevented)show('Saving your entry…');},0);
 });
})();
