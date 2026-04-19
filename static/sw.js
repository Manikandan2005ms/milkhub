const CACHE_NAME = 'milk-hub-pwa-v1';

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(clients.claim());
});

// Since the dashboard relies on live SQL data, caching the entire site isn't fully ideal. 
// We implement a pass-through fetch just to satisfy the native installability criteria!
self.addEventListener('fetch', (event) => {
  event.respondWith(
    fetch(event.request).catch(() => caches.match(event.request))
  );
});
