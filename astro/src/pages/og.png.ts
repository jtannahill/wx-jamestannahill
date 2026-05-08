// Dynamic OG image — generated at the edge, no AWS dependency.
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

const COLORS = {
  bg:      '#0a0a0a',
  text:    '#ebebeb',
  muted:   '#5a5a5a',
  dim:     '#8c8c8c',
  accent:  '#c8b97a',
  divider: '#262626',
};

let _fontCache: { bold: ArrayBuffer; regular: ArrayBuffer } | null = null;

async function loadFonts(origin: string) {
  if (_fontCache) return _fontCache;
  const ASSETS = (env as any).ASSETS as Fetcher | undefined;

  // Use the ASSETS binding directly — fetching the worker's own hostname
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

const fmt = (v: unknown, d = 0) =>
  v == null || Number.isNaN(Number(v)) ? '—' : Number(v).toFixed(d);

function nowET(): string {
  // -4h offset (ET, summer); for an OG image this is close enough year-round.
  const d = new Date(Date.now() - 4 * 60 * 60 * 1000);
  let h = d.getUTCHours();
  const m = String(d.getUTCMinutes()).padStart(2, '0');
  const ampm = h >= 12 ? 'PM' : 'AM';
  h = h % 12 || 12;
  return `${h}:${m} ${ampm} ET`;
}

function buildJsx(reading: any) {
  const tempStr  = `${fmt(reading?.tempf, 0)}°`;
  const condStr  = reading?.condition ?? '';
  const feelsStr = reading?.feelsLike != null ? `Feels like ${fmt(reading.feelsLike, 0)}°F` : '';

  const metrics: Array<{ label: string; value: string; sub: string }> = [];
  metrics.push({ label: 'HUMIDITY', value: `${fmt(reading?.humidity, 0)}%`,
                 sub: reading?.dewPoint != null ? `Dew ${fmt(reading.dewPoint, 0)}°F` : '' });
  metrics.push({ label: 'WIND', value: `${fmt(reading?.windspeedmph, 0)} mph`,
                 sub: `${reading?.windgustmph != null ? `Gust ${fmt(reading.windgustmph, 0)}` : ''}  ${compass(reading?.winddir)}`.trim() });
  metrics.push({ label: 'PRESSURE', value: `${fmt(reading?.baromrelin, 2)}"`, sub: '' });
  metrics.push({ label: 'UV INDEX', value: fmt(reading?.uv, 0), sub: '' });

  if (reading?.uhi_delta != null) {
    const sign = reading.uhi_delta >= 0 ? '+' : '';
    metrics.push({ label: 'URBAN HEAT', value: `${sign}${Number(reading.uhi_delta).toFixed(1)}°F`, sub: 'vs airports' });
  } else {
    metrics.push({ label: 'RAIN TODAY', value: `${fmt(reading?.dailyrainin, 2)}"`, sub: '' });
  }

  const slotW = (W - 2 * PAD) / metrics.length;

  // Whitespace between tags becomes phantom text nodes that break Satori's
  // "div with >1 child needs display:flex" rule. Build with no whitespace.
  const headerHtml =
    `<div style="display:flex;justify-content:space-between;align-items:center;padding:52px ${PAD}px 0;font-size:15px;letter-spacing:0.12em;color:${COLORS.muted};font-weight:400;">` +
      `<span>MIDTOWN MANHATTAN, NEW YORK</span>` +
      `<span>Updated ${nowET()}</span>` +
    `</div>`;

  const heroHtml =
    `<div style="display:flex;align-items:flex-end;padding:36px ${PAD}px 0;">` +
      `<span style="font-size:148px;font-weight:700;line-height:0.95;letter-spacing:-0.04em;">${tempStr}</span>` +
      `<div style="display:flex;flex-direction:column;margin-left:36px;padding-bottom:24px;">` +
        `<span style="font-size:38px;font-weight:700;line-height:1.1;">${condStr}</span>` +
        `<span style="font-size:26px;font-weight:400;color:${COLORS.dim};margin-top:6px;">${feelsStr}</span>` +
      `</div>` +
    `</div>`;

  const dividerHtml = `<div style="height:1px;background:${COLORS.divider};margin:60px ${PAD}px 0;display:flex;"></div>`;

  const metricsHtml =
    `<div style="display:flex;padding:28px ${PAD}px 0;">` +
      metrics.map(m =>
        `<div style="width:${slotW}px;display:flex;flex-direction:column;align-items:center;">` +
          `<span style="font-size:15px;letter-spacing:0.12em;color:${COLORS.muted};font-weight:400;">${m.label}</span>` +
          `<span style="font-size:44px;font-weight:700;margin-top:12px;line-height:1;">${m.value}</span>` +
          (m.sub ? `<span style="font-size:15px;color:${COLORS.dim};margin-top:14px;">${m.sub}</span>` : '') +
        `</div>`
      ).join('') +
    `</div>`;

  const footerHtml =
    `<div style="position:absolute;bottom:0;left:0;width:${W}px;display:flex;flex-direction:column;">` +
      `<span style="padding:0 ${PAD}px 24px;font-size:15px;letter-spacing:0.12em;color:${COLORS.muted};">wx.jamestannahill.com</span>` +
      `<div style="height:3px;background:${COLORS.accent};display:flex;"></div>` +
    `</div>`;

  return (
    `<div style="width:${W}px;height:${H}px;background:${COLORS.bg};color:${COLORS.text};font-family:'NHG Display';display:flex;flex-direction:column;position:relative;">` +
      headerHtml + heroHtml + dividerHtml + metricsHtml + footerHtml +
    `</div>`
  );
}

export async function GET({ request }: { request: Request }) {
  const apiBase = (env as any)?.API_BASE ?? 'https://api.wx.jamestannahill.com';

  let reading: any = null;
  try {
    const r = await fetch(`${apiBase}/current`, {
      signal: AbortSignal.timeout(2500),
      cf: { cacheTtl: 60, cacheEverything: true } as any,
    });
    if (r.ok) reading = await r.json();
  } catch {}

  const { bold, regular } = await loadFonts(new URL(request.url).origin);

  const r = new ImageResponse(buildJsx(reading), {
    width: W,
    height: H,
    fonts: [
      { name: 'NHG Display', data: regular, weight: 400, style: 'normal' },
      { name: 'NHG Display', data: bold,    weight: 700, style: 'normal' },
    ],
  } as any);

  // workers-og sets its own immutable 1-year cache-control. Override it so
  // the OG image refreshes every 5 min as the underlying reading changes.
  const headers = new Headers(r.headers);
  headers.set('cache-control', 'public, max-age=300, s-maxage=300, stale-while-revalidate=600');
  return new Response(r.body, { status: 200, headers });
}
