const CACHE='benoit-inspection-offline-v10';
const SHELL='/public/tablet-inspection.html';
const ASSETS=[SHELL,'/public/tablet-inspection.js','/public/tablet-session-lock.js','/public/inspection-store.js','/public/inspection-plan.js','/public/tablet-inspection.css','/public/styles.css','/public/measurement-values.js','/public/measurementKeypad.js','/public/measurementKeypadCore.js'];
self.addEventListener('install',event=>event.waitUntil(caches.open(CACHE).then(cache=>cache.addAll(ASSETS)).then(()=>self.skipWaiting())));
self.addEventListener('activate',event=>event.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(key=>key.startsWith('benoit-inspection-offline-')&&key!==CACHE).map(key=>caches.delete(key)))).then(()=>self.clients.claim())));
self.addEventListener('fetch',event=>{
 if(event.request.method!=='GET')return;const url=new URL(event.request.url);if(url.origin!==self.location.origin)return;
 if(event.request.mode==='navigate')event.respondWith(fetch(event.request).catch(()=>caches.match(SHELL)));
 else if(ASSETS.includes(url.pathname))event.respondWith(caches.match(event.request).then(cached=>cached||fetch(event.request)));
});
