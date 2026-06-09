import { defineMiddleware } from 'astro:middleware';

// Security headers for all server-rendered responses.
// No CSP: partytown + GA rely on inline scripts, which makes a meaningful
// CSP nontrivial — intentionally skipped for now.
export const onRequest = defineMiddleware(async (_context, next) => {
  const response = await next();
  response.headers.set('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
  response.headers.set('X-Content-Type-Options', 'nosniff');
  response.headers.set('X-Frame-Options', 'SAMEORIGIN');
  response.headers.set('Referrer-Policy', 'strict-origin-when-cross-origin');
  return response;
});
