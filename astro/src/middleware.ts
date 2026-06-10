import { defineMiddleware } from 'astro:middleware';

// Content-Security-Policy.
//
// script-src uses 'unsafe-inline' rather than sha256 hashes because the
// index page embeds a per-request inline boot script (the live /current
// reading serialized into localStorage, see index.astro). Its content
// changes with every reading, so a static hash set can never cover it.
// Hashing it per request would require buffering the streamed HTML in the
// worker and would still leave docs.html (served by the assets binding via
// public/_headers, where only a static value is possible) out of sync.
// Every other directive is strict.
//
// Inventory the policy serves:
// - self: app.js, /_astro/* bundles, /~partytown/* worker scripts, style.css
// - fonts.jamestannahill.com: nhg-display.css + woff2 (style-src + font-src)
// - api.wx.jamestannahill.com: client fetches (connect-src)
// - googletagmanager.com / google-analytics.com: GTM script text and GA
//   beacons are fetched INSIDE partytown's web worker; the blob worker
//   inherits this document CSP, so they need connect-src
// - worker-src 'self' blob:: partytown's web worker
// - img-src 'self' data:: weatherkit.png, og.png, favicons
// - style 'unsafe-inline': inline style attributes in markup + Astro's
//   auto-inlined <style> tags (build.inlineStylesheets defaults to 'auto')
// - static.cloudflareinsights.com (script) + cloudflareinsights.com
//   (connect): Cloudflare Web Analytics beacon, auto-injected at the zone
//   level (only visible on the live site, not in wrangler dev)
// - www.googletagmanager.com (script): Cloudflare's Google Tag Gateway
//   serves a first-party bootstrap (e.g. /vjoz/) that then loads gtm.js
//   from googletagmanager.com on the main thread, also zone-level
//
// Keep this in sync with the copy for /docs.html in public/_headers
// (prerendered, served by the assets binding, which bypasses middleware).
export const CSP = [
  "default-src 'self'",
  "script-src 'self' 'unsafe-inline' https://static.cloudflareinsights.com https://www.googletagmanager.com",
  "style-src 'self' 'unsafe-inline' https://fonts.jamestannahill.com",
  "font-src https://fonts.jamestannahill.com",
  "img-src 'self' data:",
  "connect-src 'self' https://api.wx.jamestannahill.com https://www.googletagmanager.com https://www.google-analytics.com https://cloudflareinsights.com",
  "worker-src 'self' blob:",
  "frame-ancestors 'none'",
  "base-uri 'self'",
  "form-action 'self'",
  "object-src 'none'",
].join('; ');

// Security headers for all server-rendered responses.
export const onRequest = defineMiddleware(async (_context, next) => {
  const response = await next();
  response.headers.set('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
  response.headers.set('X-Content-Type-Options', 'nosniff');
  response.headers.set('X-Frame-Options', 'DENY');
  response.headers.set('Referrer-Policy', 'strict-origin-when-cross-origin');
  response.headers.set('Content-Security-Policy', CSP);
  return response;
});
