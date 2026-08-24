const CACHE_NAME = "cnc-time-v1";
const ASSETS = [
  "/public/cnc-time.css",
  "/public/cnc-time.js",
  "/public/cnc-time.webmanifest",
  "/public/BenoitLogoRegistered-Red.png",
];

self.addEventListener("install", (event) => {
  event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(ASSETS)));
});

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") return;
  event.respondWith(caches.match(event.request).then((cached) => cached || fetch(event.request)));
});
