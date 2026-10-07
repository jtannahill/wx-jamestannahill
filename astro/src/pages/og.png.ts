// Dynamic OG image - generated at the edge, no AWS dependency.
//
// Pulls /current from the API, renders the same Pillow layout the legacy
// wx_poller Lambda used to bake into S3, but as JSX → SVG → PNG via Satori
// (workers-og). NHG Display TTFs ship in the worker bundle. Cached at the CF
// edge for 5 min so the upstream API hop is rare.
import { ImageResponse } from 'workers-og';
import { env } from 'cloudflare:workers';

import nhgBoldUrl    from '../og-fonts/NHGDisplay-Bold.ttf?url';
import nhgRegularUrl from '../og-fonts/NHGDisplay-Regular.ttf?url';

export const prerender = false;

const W = 1200;
const H = 630;
const PAD = 72;

// Site tokens (public/style.css). Labels sit on --muted, never dimmer: the
// card is usually seen at ~500px wide, where anything fainter disappears.
const COLORS = {
  bg:      '#0a0a0a',
  text:    '#f0f0f0',
  muted:   '#8e8e8e',
  accent:  '#c8b97a',
  divider: '#222222',
};

let _fontCache: { bold: ArrayBuffer; regular: ArrayBuffer } | null = null;

async function loadFonts(origin: string) {
  if (_fontCache) return _fontCache;
  const ASSETS = (env as any).ASSETS as Fetcher | undefined;

  // Use the ASSETS binding directly - fetching the worker's own hostname
  // round-trips through Cloudflare and can return the Worker's HTML handler
  // instead of the static asset.
  const fetchAsset = async (url: string) => {
    const u = new URL(url, origin);
    if (ASSETS) return ASSETS.fetch(new Request(u.toString()));
    return fetch(u);
  };

  const [boldRes, regRes] = await Promise.all([
    fetchAsset(nhgBoldUrl),
    fetchAsset(nhgRegularUrl),
  ]);
  if (!boldRes.ok || !regRes.ok) {
    throw new Error(`font fetch failed: bold=${boldRes.status} regular=${regRes.status}`);
  }
  _fontCache = {
    bold:    await boldRes.arrayBuffer(),
    regular: await regRes.arrayBuffer(),
  };
  return _fontCache;
}

const DIRS = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW'];
const compass = (deg: number | null | undefined) =>
  deg == null ? '' : DIRS[Math.round(Number(deg) / 22.5) % 16];

// Empty-value placeholder, same glyph as EMPTY in public/app.js. Negative
// values use a true minus sign (U+2212); a rounded -0 drops its sign.
const EMPTY = '--';
const withMinus = (s: string) => (/^-0*(\.0+)?$/.test(s) ? s.slice(1) : s.replace(/^-/, '\u2212'));
const fmt = (v: unknown, d = 0) =>
  v == null || Number.isNaN(Number(v)) ? EMPTY : withMinus(Number(v).toFixed(d));

// Station time, DST-aware, same "10:52 AM ET" style as the dashboard header.
const _etClock = new Intl.DateTimeFormat('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' });
const etLabel = (ms: number) => `${_etClock.format(ms)} ET`;

// A reading is usable for a snapshot only if the core hero value is present.
// A degraded render (all "--") must never be edge-cached behind ?v=.
const hasReading = (r: any) =>
  r != null && r.tempf != null && !Number.isNaN(Number(r.tempf));

const LABEL = `font-size:20px;letter-spacing:0.1em;color:${COLORS.muted};`;

// Whitespace between tags becomes phantom text nodes that break Satori's
// "div with >1 child needs display:flex" rule. Build with no whitespace.
const frame = (inner: string) =>
  `<div style="width:${W}px;height:${H}px;background:${COLORS.bg};color:${COLORS.text};font-family:'NHG Display';display:flex;flex-direction:column;position:relative;">` +
    inner +
    `<span style="position:absolute;left:${PAD}px;bottom:44px;${LABEL}">wx.jamestannahill.com</span>` +
  `</div>`;

// Static card: /docs and /embed, and whenever there is no reading to show.
// It carries no numbers and no timestamp, so it can never be stale.
function buildBrandJsx() {
  return frame(
    `<div style="display:flex;padding:56px ${PAD}px 0;${LABEL}">MIDTOWN MANHATTAN, NEW YORK</div>` +
    `<div style="display:flex;flex-direction:column;padding:120px ${PAD}px 0;">` +
      `<span style="font-size:112px;font-weight:400;line-height:1;letter-spacing:-0.03em;">Live weather</span>` +
      `<span style="font-size:32px;color:${COLORS.muted};margin-top:28px;line-height:1.35;max-width:900px;">One private station in Midtown Manhattan, read every 5 minutes, with history, records and forecasts.</span>` +
    `</div>`
  );
}

function buildJsx(reading: any) {
  const tempStr  = `${fmt(reading?.tempf, 0)}°`;
  const condStr  = reading?.condition ?? '';
  // Feels-like only earns a line when it differs from the reading.
  const feelsStr = reading?.feelsLike != null && fmt(reading.feelsLike, 0) !== fmt(reading?.tempf, 0)
    ? `Feels like ${fmt(reading.feelsLike, 0)}°F` : '';

  const metrics: Array<{ label: string; value: string; sub: string }> = [];
  metrics.push({ label: 'HUMIDITY', value: `${fmt(reading?.humidity, 0)}%`,
                 sub: reading?.dewPoint != null ? `Dew point ${fmt(reading.dewPoint, 0)}°F` : '' });
  const dir = compass(reading?.winddir);
  const gust = reading?.windgustmph != null ? `gusts ${fmt(reading.windgustmph, 0)}` : '';
  metrics.push({ label: 'WIND', value: `${fmt(reading?.windspeedmph, 0)} mph`,
                 sub: [dir, gust].filter(Boolean).join(', ') });
  metrics.push({ label: 'PRESSURE', value: fmt(reading?.baromrelin, 2), sub: 'inHg' });
  metrics.push({ label: 'UV INDEX', value: fmt(reading?.uv, 0), sub: '' });

  let uhiPositive = false;
  if (reading?.uhi_delta != null) {
    const uhi = fmt(reading.uhi_delta, 1);
    const sign = uhi.startsWith('−') || /^[0.]+$/.test(uhi) ? '' : '+';
    uhiPositive = sign === '+';
    metrics.push({ label: 'URBAN HEAT', value: `${sign}${uhi}°F`, sub: 'vs airports' });
  } else {
    metrics.push({ label: 'RAIN TODAY', value: `${fmt(reading?.dailyrainin, 2)} in`, sub: '' });
  }

  const slotW = (W - 2 * PAD) / metrics.length;

  // Stamp the reading's own time, never wall-clock: the card must not claim
  // to be fresher than the data it shows. Callers only get here with a reading.
  const ts = reading?.timestamp ? new Date(reading.timestamp).getTime() : NaN;
  const updated = Number.isNaN(ts) ? '' : `Updated ${etLabel(ts)}`;

  const headerHtml =
    `<div style="display:flex;justify-content:space-between;align-items:center;padding:56px ${PAD}px 0;">` +
      `<span style="${LABEL}">MIDTOWN MANHATTAN, NEW YORK</span>` +
      (updated
        ? `<div style="display:flex;align-items:center;font-size:20px;color:${COLORS.muted};">` +
            `<div style="width:8px;height:8px;border-radius:4px;background:${COLORS.accent};margin-right:10px;display:flex;"></div>` +
            `<span>${updated}</span>` +
          `</div>`
        : '') +
    `</div>`;

  const heroHtml =
    `<div style="display:flex;align-items:flex-end;padding:28px ${PAD}px 0;">` +
      `<span style="font-size:184px;font-weight:400;line-height:0.95;letter-spacing:-0.04em;">${tempStr}</span>` +
      `<div style="display:flex;flex-direction:column;margin-left:40px;padding-bottom:26px;">` +
        `<span style="font-size:44px;font-weight:700;line-height:1.1;">${condStr}</span>` +
        (feelsStr ? `<span style="font-size:28px;color:${COLORS.muted};margin-top:8px;">${feelsStr}</span>` : '') +
      `</div>` +
    `</div>`;

  const dividerHtml = `<div style="height:1px;background:${COLORS.divider};margin:44px ${PAD}px 0;display:flex;"></div>`;

  const metricsHtml =
    `<div style="display:flex;padding:30px ${PAD}px 0;">` +
      metrics.map(m =>
        `<div style="width:${slotW}px;display:flex;flex-direction:column;align-items:flex-start;">` +
          `<span style="${LABEL}">${m.label}</span>` +
          `<span style="font-size:56px;font-weight:400;margin-top:12px;line-height:1;letter-spacing:-0.01em;color:${m.label === 'URBAN HEAT' && uhiPositive ? COLORS.accent : COLORS.text};">${m.value}</span>` +
          (m.sub ? `<span style="font-size:20px;color:${COLORS.muted};margin-top:12px;">${m.sub}</span>` : '') +
        `</div>`
      ).join('') +
    `</div>`;

  return frame(headerHtml + heroHtml + dividerHtml + metricsHtml);
}

export async function GET({ request }: { request: Request }) {
  const apiBase = (env as any)?.API_BASE ?? 'https://api.wx.jamestannahill.com';
  const url = new URL(request.url);
  const brand = url.searchParams.get('card') === 'brand';

  // The page uses a cache-busting ?v=<5-min bucket> param so each new reading
  // produces a unique OG URL. Crawlers re-fetch on URL change; the CF edge
  // caches each ?v= value as a separate object. Within a bucket the URL is
  // stable → edge hit → no re-render. New bucket → edge miss → render fresh
  // from a freshly fetched /current. Bypass any internal cache on /current
  // so the rendered snapshot matches the latest poller write.
  const fetchCurrent = async () => {
    const r = await fetch(`${apiBase}/current`, {
      signal: AbortSignal.timeout(3000),
      cf: { cacheTtl: 0, cacheEverything: false } as any,
      headers: { 'cache-control': 'no-cache' },
    });
    return r.ok ? await r.json() : null;
  };

  // One retry on a transient blip: without this, a single failed scrape
  // pins an all-"--" card at the edge for 5 min behind ?v=.
  let reading: any = null;
  if (!brand) {
    try { reading = await fetchCurrent(); } catch {}
    if (!hasReading(reading)) {
      try { reading = await fetchCurrent(); } catch {}
    }
  }

  // Without fonts Satori cannot render; hand scrapers the committed static
  // card instead of a 500.
  let fonts: { bold: ArrayBuffer; regular: ArrayBuffer };
  try { fonts = await loadFonts(url.origin); }
  catch { return Response.redirect(new URL('/og-fallback.png', url.origin).toString(), 302); }
  const { bold, regular } = fonts;

  // No reading means the brand card, never a dated card full of "--".
  const html = brand || !hasReading(reading) ? buildBrandJsx() : buildJsx(reading);

  const r = new ImageResponse(html, {
    width: W,
    height: H,
    fonts: [
      { name: 'NHG Display', data: regular, weight: 400, style: 'normal' },
      { name: 'NHG Display', data: bold,    weight: 700, style: 'normal' },
    ],
  } as any);

  // workers-og sets its own 1-year immutable header. Override:
  // - With ?v= → image is keyed by the bucket; cache aggressively at the
  //   edge AND tell crawlers it's fine to cache (they'd refetch on a new
  //   ?v= anyway because the URL is different).
  // - Without ?v= → no edge cache; render fresh every request (matches the
  //   "actual snapshot" guarantee for crawlers that strip query params).
  const headers = new Headers(r.headers);
  if (brand) {
    // The brand card has no data in it, so it can be cached like a file.
    headers.set('cache-control', 'public, max-age=86400, s-maxage=86400');
  } else if (url.searchParams.has('v') && hasReading(reading)) {
    // Snapshot is real and keyed by the bucket → cache hard at the edge.
    headers.set('cache-control', 'public, max-age=300, s-maxage=300, immutable');
  } else {
    // No ?v=, OR a degraded render: never pin it. The next scrape re-fetches
    // and recovers the current snapshot instead of freezing a broken card.
    headers.set('cache-control', 'public, max-age=0, s-maxage=0, must-revalidate');
  }
  return new Response(r.body, { status: 200, headers });
}
