const CACHE='benoit-time-offline-v27';
const SHELL='/public/tablet-time.html';
const ASSETS=[SHELL,'/public/review-time-recovery.js','/public/tablet-time.js','/public/time-entry-ui.js','/public/time-view-return.js','/public/lunch-reminder.js','/public/active-time-summary.js','/public/tablet-session-lock.js','/public/tablet-time-store.js','/public/tablet-time.css','/public/cnc-time.css','/public/cnc-time.js?v=6','/public/click-diagnostics.js?v=1','/public/BenoitIconRegistered-Red.png','/public/BenoitLogoRegistered-Red.png'];
self.addEventListener('install',event=>event.waitUntil(caches.open(CACHE).then(cache=>cache.addAll(ASSETS)).then(()=>self.skipWaiting())));
self.addEventListener('activate',event=>event.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(key=>key.startsWith('cnc-time-')||key.startsWith('benoit-time-offline-')&&key!==CACHE).map(key=>caches.delete(key)))).then(()=>self.clients.claim())));
self.addEventListener('fetch',event=>{
 if(event.request.method!=='GET')return;
 const url=new URL(event.request.url);if(url.origin!==self.location.origin)return;
 if(event.request.mode==='navigate'){
  event.respondWith(fetch(event.request).catch(()=>caches.match(SHELL)));return;
 }
 if(ASSETS.includes(url.pathname+url.search)||ASSETS.includes(url.pathname)){
  // An installed version uses its complete precached set. A changed worker installs
  // the next set before activation, avoiding a half-updated offline interface.
  event.respondWith(caches.match(event.request).then(cached=>cached||fetch(event.request)));
 }
});
