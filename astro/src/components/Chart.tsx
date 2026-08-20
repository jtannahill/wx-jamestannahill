/** @jsxImportSource preact */
import { useEffect, useRef, useState } from 'preact/hooks';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';

// ── Field config & helpers ───────────────────────────────────────────────────
type FieldKey = 'tempf' | 'humidity' | 'windspeedmph' | 'baromrelin' | 'uhi_delta';

const FIELD_LABELS: Record<string, { label: string; unit: string; decimals: number }> = {
  tempf:        { label: 'Temperature', unit: '°F',  decimals: 1 },
  humidity:     { label: 'Humidity',    unit: '%',   decimals: 0 },
  windspeedmph: { label: 'Wind',        unit: ' mph', decimals: 1 },
  baromrelin:   { label: 'Pressure',    unit: '"',   decimals: 2 },
  uhi_delta:    { label: 'Urban Heat',  unit: '°F',  decimals: 1 },
};

const RANGE_LABELS: Record<number, string> = {
  12: '12 hours', 24: '24 hours', 168: '7 days', 720: '30 days',
};

const toC  = (f: number) => (f - 32) * 5 / 9;
const toDC = (df: number) => df * 5 / 9;
const tempUnitFor = (cel: boolean) => cel ? '°C' : '°F';

// ── Plugins ──────────────────────────────────────────────────────────────────
function makeTooltipPlugin(readings: any[], hours: number, activeField: string, useCelsius: boolean) {
  let el: HTMLDivElement;
  const fmtT = (v: any, d: number) => v == null ? '—' : (useCelsius ? toC(Number(v)) : Number(v)).toFixed(d);
  const fmtD = (v: any, d: number) => v == null ? '—' : (useCelsius ? toDC(Number(v)) : Number(v)).toFixed(d);
  const tu   = tempUnitFor(useCelsius);
  const fmtChartLabel = (ms: number) => {
    const d = new Date(ms);
    if (hours > 168) return `${d.getMonth()+1}/${d.getDate()}`;
    if (hours > 24)  return `${d.getMonth()+1}/${d.getDate()} ${d.getHours()}:00`;
    const h = d.getHours(), ap = h >= 12 ? 'pm' : 'am';
    return `${h===0?12:h>12?h-12:h}${ap}`;
  };
  return {
    hooks: {
      init: [(u: any) => {
        el = document.createElement('div');
        el.className = 'wx-tooltip';
        el.hidden = true;
        u.over.style.overflow = 'visible';
        u.over.appendChild(el);
      }],
      setCursor: [(u: any) => {
        const { left, idx } = u.cursor;
        if (idx == null || idx < 0 || left < 0) { el.hidden = true; return; }
        const r = readings[idx];
        if (!r) { el.hidden = true; return; }
        const active = (v: string) => `<span class="wxt-active">${v}</span>`;
        const rows: string[] = [];
        rows.push(`<div class="wxt-time">${fmtChartLabel(new Date(r.timestamp).getTime())}</div>`);
        if (r.tempf        != null) { const v = fmtT(r.tempf,1)+tu; rows.push(`<div>Temp&ensp;${activeField==='tempf' ? active(v) : v}</div>`); }
        if (r.humidity     != null) rows.push(`<div>RH&emsp;&ensp;${activeField==='humidity'    ? active(r.humidity.toFixed(0)+'%')       : r.humidity.toFixed(0)+'%'}</div>`);
        if (r.windspeedmph != null) rows.push(`<div>Wind&ensp;${activeField==='windspeedmph'  ? active(r.windspeedmph.toFixed(1)+' mph') : r.windspeedmph.toFixed(1)+' mph'}</div>`);
        if (r.baromrelin   != null) rows.push(`<div>Pres&ensp;${activeField==='baromrelin'    ? active(r.baromrelin.toFixed(2)+'"')      : r.baromrelin.toFixed(2)+'"'}</div>`);
        if (r.uhi_delta    != null) { const v = (r.uhi_delta>=0?'+':'')+fmtD(r.uhi_delta,1)+tu; rows.push(`<div>UHI&emsp;&ensp;${activeField==='uhi_delta' ? active(v) : v}</div>`); }
        if ((r.hourlyrainin??0) > 0.005) rows.push(`<div>Rain&ensp;${r.hourlyrainin.toFixed(2)}"/hr</div>`);
        el.innerHTML = rows.join('');
        el.hidden = false;
        const overW = u.over.offsetWidth;
        const tipW  = el.offsetWidth || 130;
        const flip  = left + tipW + 18 > overW;
        el.style.left  = flip ? 'auto' : `${left + 14}px`;
        el.style.right = flip ? `${overW - left + 14}px` : 'auto';
        el.style.top   = '6px';
      }],
    },
  };
}

function makeDrawPlugin(tsArr: number[], baseArr: any[], upperArr: any[], lowerArr: any[]) {
  return {
    hooks: {
      drawClear: [(u: any) => {
        try {
          const { ctx, bbox } = u;
          ctx.save();
          ctx.fillStyle = '#0e0e0e';
          ctx.fillRect(bbox.left, bbox.top, bbox.width, bbox.height);
          ctx.restore();
        } catch(e) { console.warn('[drawClear]', e); }
      }],
      draw: [(u: any) => {
        try {
          const { ctx } = u;
          ctx.save();
          const hasUpper = upperArr && upperArr.some(v => v != null);
          const hasLower = lowerArr && lowerArr.some(v => v != null);
          if (hasUpper && hasLower) {
            ctx.beginPath();
            let started = false;
            for (let i = 0; i < tsArr.length; i++) {
              if (upperArr[i] == null) { started = false; continue; }
              const px = Math.round(u.valToPos(tsArr[i],    'x', true));
              const py = Math.round(u.valToPos(upperArr[i], 'y', true));
              if (!started) { ctx.moveTo(px, py); started = true; } else ctx.lineTo(px, py);
            }
            for (let i = tsArr.length - 1; i >= 0; i--) {
              if (lowerArr[i] == null) continue;
              const px = Math.round(u.valToPos(tsArr[i],    'x', true));
              const py = Math.round(u.valToPos(lowerArr[i], 'y', true));
              ctx.lineTo(px, py);
            }
            ctx.closePath();
            ctx.fillStyle = 'rgba(150,160,190,0.22)';
            ctx.fill();
          }
          const hasBase = baseArr && baseArr.some(v => v != null);
          if (hasBase) {
            ctx.setLineDash([4, 6]);
            ctx.strokeStyle = 'rgba(255,255,255,0.42)';
            ctx.lineWidth   = 1;
            ctx.beginPath();
            let started = false;
            for (let i = 0; i < tsArr.length; i++) {
              if (baseArr[i] == null) { started = false; continue; }
              const px = Math.round(u.valToPos(tsArr[i],   'x', true));
              const py = Math.round(u.valToPos(baseArr[i], 'y', true));
              if (!started) { ctx.moveTo(px, py); started = true; } else ctx.lineTo(px, py);
            }
            ctx.stroke();
          }
          ctx.restore();
        } catch(e) { console.warn('[draw]', e); }
      }],
    },
  };
}

function makeNowLinePlugin() {
  return {
    hooks: {
      draw: [(u: any) => {
        const xs = u.data[0];
        if (!xs || xs.length < 2) return;
        const nowS = Date.now() / 1000;
        if (nowS < xs[0] || nowS > xs[xs.length - 1] + 7200) return;
        let best = 0, bestD = Infinity;
        for (let i = 0; i < xs.length; i++) {
          const d = Math.abs(xs[i] - nowS);
          if (d < bestD) { bestD = d; best = i; }
        }
        const x = u.valToPos(xs[best], 'x', true);
        const { ctx, bbox } = u;
        ctx.save();
        ctx.setLineDash([2, 4]);
        ctx.strokeStyle = 'rgba(200,185,122,0.6)';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(Math.round(x), bbox.top);
        ctx.lineTo(Math.round(x), bbox.top + bbox.height);
        ctx.stroke();
        ctx.restore();
      }],
    },
  };
}

function makeChartGesturesPlugin() {
  let dataMin = 0, dataMax = 0;
  let mousePan: any = null, touchPan: any = null, pinch: any = null;
  const MIN_SPAN_S = 300;
  const ZOOM_SENS  = 0.0014;
  const clamp = (min: number, max: number) => {
    if (min < dataMin) { max += dataMin - min; min = dataMin; }
    if (max > dataMax) { min -= max - dataMax; max = dataMax; }
    return { min: Math.max(min, dataMin), max: Math.min(max, dataMax) };
  };
  const enforceMinSpan = (min: number, max: number) => {
    if (max - min >= MIN_SPAN_S) return { min, max };
    const c = (min + max) / 2;
    return { min: c - MIN_SPAN_S / 2, max: c + MIN_SPAN_S / 2 };
  };
  const zoomAt = (u: any, center: number, factor: number) => {
    const sx = u.scales.x; if (sx.min == null) return;
    const { min, max } = enforceMinSpan(
      center + (sx.min - center) * factor,
      center + (sx.max - center) * factor,
    );
    u.setScale('x', clamp(min, max));
  };
  const panBy = (u: any, dxFrac: number) => {
    const sx = u.scales.x; if (sx.min == null) return;
    const range = sx.max - sx.min;
    const shift = -dxFrac * range;
    u.setScale('x', clamp(sx.min + shift, sx.max + shift));
  };
  const isZoomed = (u: any) => {
    const sx = u.scales.x;
    return sx && sx.min != null && (sx.min > dataMin + 1 || sx.max < dataMax - 1);
  };
  const pinchD = (t: TouchList) => Math.hypot(t[0].clientX - t[1].clientX, t[0].clientY - t[1].clientY);
  return {
    hooks: {
      ready: (u: any) => {
        const over = u.over;
        const xs = u.data[0];
        if (xs && xs.length) { dataMin = xs[0]; dataMax = xs[xs.length - 1]; }
        over.addEventListener('wheel', (e: WheelEvent) => {
          if (Math.abs(e.deltaX) > Math.abs(e.deltaY) * 1.4) {
            e.preventDefault();
            panBy(u, e.deltaX / over.getBoundingClientRect().width);
          } else {
            e.preventDefault();
            const rect = over.getBoundingClientRect();
            const cursorVal = u.posToVal(e.clientX - rect.left, 'x');
            zoomAt(u, cursorVal, Math.exp(e.deltaY * ZOOM_SENS));
          }
        }, { passive: false });
        over.addEventListener('mousedown', (e: MouseEvent) => {
          if (e.button !== 0) return;
          const sx = u.scales.x;
          mousePan = { x: e.clientX, w: over.getBoundingClientRect().width, min: sx.min, max: sx.max };
          over.style.cursor = 'grabbing';
        });
        window.addEventListener('mousemove', (e: MouseEvent) => {
          if (!mousePan) return;
          const dxFrac = (mousePan.x - e.clientX) / mousePan.w;
          const range  = mousePan.max - mousePan.min;
          u.setScale('x', clamp(mousePan.min + dxFrac * range, mousePan.max + dxFrac * range));
        });
        window.addEventListener('mouseup', () => {
          if (!mousePan) return;
          mousePan = null;
          over.style.cursor = isZoomed(u) ? 'grab' : '';
        });
        over.addEventListener('mouseenter', () => {
          if (!mousePan) over.style.cursor = isZoomed(u) ? 'grab' : '';
        });
        over.addEventListener('touchstart', (e: TouchEvent) => {
          if (e.touches.length === 2) {
            const sx = u.scales.x;
            const rect = over.getBoundingClientRect();
            const cx = ((e.touches[0].clientX + e.touches[1].clientX) / 2) - rect.left;
            pinch = { d: pinchD(e.touches), center: u.posToVal(cx, 'x'), min: sx.min, max: sx.max };
            touchPan = null;
          } else if (e.touches.length === 1 && isZoomed(u)) {
            const sx = u.scales.x;
            const t  = e.touches[0];
            touchPan = { x: t.clientX, y: t.clientY, w: over.getBoundingClientRect().width, min: sx.min, max: sx.max, intercepted: false };
            pinch = null;
          }
        }, { passive: true });
        over.addEventListener('touchmove', (e: TouchEvent) => {
          if (pinch && e.touches.length === 2) {
            e.preventDefault();
            const factor = pinch.d / pinchD(e.touches);
            const { min, max } = enforceMinSpan(
              pinch.center + (pinch.min - pinch.center) * factor,
              pinch.center + (pinch.max - pinch.center) * factor,
            );
            u.setScale('x', clamp(min, max));
          } else if (touchPan && e.touches.length === 1) {
            const t  = e.touches[0];
            const dx = t.clientX - touchPan.x, dy = t.clientY - touchPan.y;
            if (!touchPan.intercepted) {
              if (Math.abs(dx) > Math.abs(dy) && Math.abs(dx) > 8) touchPan.intercepted = true;
              else if (Math.abs(dy) > 8) { touchPan = null; return; }
              else return;
            }
            e.preventDefault();
            const dxFrac = -dx / touchPan.w;
            const range  = touchPan.max - touchPan.min;
            u.setScale('x', clamp(touchPan.min + dxFrac * range, touchPan.max + dxFrac * range));
          }
        }, { passive: false });
        over.addEventListener('touchend', (e: TouchEvent) => {
          if (e.touches.length < 2) pinch = null;
          if (e.touches.length === 0) touchPan = null;
        });
      },
      setScale: [(u: any, key: string) => {
        if (key !== 'x' || mousePan) return;
        u.over.style.cursor = isZoomed(u) ? 'grab' : '';
      }],
    },
  };
}

function makeZoomTrackerPlugin(tsArr: number[], onZoomedChange: (zoomed: boolean) => void) {
  const t0 = tsArr.length ? tsArr[0]              : null;
  const t1 = tsArr.length ? tsArr[tsArr.length-1] : null;
  return {
    hooks: {
      setScale: [(u: any, key: string) => {
        if (key !== 'x' || t0 == null) return;
        const sc = u.scales.x;
        const zoomed = sc && (sc.min > t0 + 1 || sc.max < t1! - 1);
        onZoomedChange(!!zoomed);
      }],
    },
  };
}

// ── Export PNG helpers ───────────────────────────────────────────────────────
async function exportChartPng(uplot: any, field: string, hours: number) {
  if (!uplot) return null;
  const src = uplot.ctx.canvas;
  const dpr = window.devicePixelRatio || 1;
  const padTop = 56 * dpr, padBot = 36 * dpr, padX = 24 * dpr;
  const out = document.createElement('canvas');
  out.width  = src.width  + padX * 2;
  out.height = src.height + padTop + padBot;
  const ctx = out.getContext('2d')!;
  ctx.fillStyle = '#0a0a0a';
  ctx.fillRect(0, 0, out.width, out.height);
  ctx.fillStyle = '#e8e0d0';
  ctx.font = `${14 * dpr}px "NHG Display", -apple-system, sans-serif`;
  ctx.textBaseline = 'top';
  const cfg = FIELD_LABELS[field] || { label: field };
  const rangeLabel = RANGE_LABELS[hours] || `${hours}h`;
  ctx.fillText(`${cfg.label.toUpperCase()} · ${rangeLabel}`, padX, 14 * dpr);
  ctx.fillStyle = '#666';
  ctx.font = `${10 * dpr}px "NHG Display", -apple-system, sans-serif`;
  ctx.fillText('MIDTOWN MANHATTAN, NEW YORK', padX, 34 * dpr);
  ctx.drawImage(src, padX, padTop);
  ctx.fillStyle = '#444';
  ctx.font = `${10 * dpr}px "NHG Display", -apple-system, sans-serif`;
  ctx.fillText(`wx.jamestannahill.com · ${new Date().toLocaleString()}`, padX, padTop + src.height + 10 * dpr);
  return new Promise<Blob | null>(res => out.toBlob(b => res(b), 'image/png'));
}

// ── Component ────────────────────────────────────────────────────────────────
const HOURS_OPTIONS = [12, 24, 168, 720];
const FIELD_OPTIONS: FieldKey[] = ['tempf', 'humidity', 'windspeedmph', 'baromrelin', 'uhi_delta'];
const FIELD_BTN_LABELS: Record<FieldKey, string> = {
  tempf: 'Temperature', humidity: 'Humidity', windspeedmph: 'Wind',
  baromrelin: 'Pressure', uhi_delta: 'Urban Heat',
};

interface Props {
  apiBase: string;
}

export default function Chart({ apiBase }: Props) {
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const uplotRef = useRef<any>(null);
  // Honor clicks made before the island hydrated: app.js records them on
  // window.__wxPendingField / __wxPendingHours (see its capture-phase
  // listeners), and we use them as the initial state instead of clobbering
  // the user's choice back to the defaults at boot.
  const [field, setField] = useState<FieldKey>(() => {
    if (typeof window !== 'undefined') {
      const f = (window as any).__wxPendingField;
      if (f && FIELD_OPTIONS.includes(f)) return f as FieldKey;
    }
    return 'tempf';
  });
  const [hours, setHours] = useState<number>(() => {
    if (typeof window !== 'undefined') {
      const h = Number((window as any).__wxPendingHours);
      if (HOURS_OPTIONS.includes(h)) return h;
    }
    return 24;
  });
  const [useCelsius, setUseCelsius] = useState<boolean>(false);
  const [history, setHistory] = useState<any | null>(null);
  const [zoomed, setZoomed] = useState<boolean>(false);
  const [hasBaseline, setHasBaseline] = useState<boolean>(false);
  const [hasRain, setHasRain] = useState<boolean>(false);

  // Initial unit + listen for app.js unit-toggle event
  useEffect(() => {
    setUseCelsius(localStorage.getItem('wx_celsius') === '1');
    const onUnit = (e: any) => setUseCelsius(!!e.detail?.useCelsius);
    document.addEventListener('wx:unitChange', onUnit);
    return () => document.removeEventListener('wx:unitChange', onUnit);
  }, []);

  // Listen for card clicks in the static HTML (cards have data-chart-field)
  useEffect(() => {
    const onFieldEvent = (e: any) => {
      const f = e.detail?.field as FieldKey | undefined;
      if (f && FIELD_OPTIONS.includes(f)) setField(f);
    };
    document.addEventListener('wx:fieldChange', onFieldEvent);
    return () => document.removeEventListener('wx:fieldChange', onFieldEvent);
  }, []);

  // Fetch history when hours changes
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const r = await fetch(`${apiBase}/history?hours=${hours}`);
        if (!r.ok) throw new Error(`history ${r.status}`);
        const j = await r.json();
        if (!cancelled) setHistory(j);
      } catch (e) {
        console.error('[chart] history fetch failed:', e);
      }
    })();
    return () => { cancelled = true; };
  }, [apiBase, hours]);

  // (Re)render uPlot whenever data, field, or unit changes
  useEffect(() => {
    if (!history?.readings?.length || !wrapRef.current) return;
    const wrap = wrapRef.current;
    const cfg = { ...(FIELD_LABELS[field] || { label: field, unit: '', decimals: 1 }) };
    if (useCelsius && (field === 'tempf' || field === 'uhi_delta')) cfg.unit = '°C';
    const readings = history.readings;
    const ts    = readings.map((r: any) => new Date(r.timestamp).getTime() / 1000);
    const vals  = readings.map((r: any) => r[field] ?? null);
    const base  = readings.map((r: any) => r[`baseline_${field}`] ?? null);
    const upper = readings.map((r: any) => { const b = r[`baseline_${field}`], s = r[`baseline_std_${field}`]; return b!=null&&s!=null ? b+s : null; });
    const lower = readings.map((r: any) => { const b = r[`baseline_${field}`], s = r[`baseline_std_${field}`]; return b!=null&&s!=null ? b-s : null; });
    if (useCelsius) {
      const cvt = field === 'tempf' ? toC : field === 'uhi_delta' ? toDC : null;
      if (cvt) {
        for (let i = 0; i < vals.length; i++) {
          if (vals[i]  != null) vals[i]  = cvt(vals[i]);
          if (base[i]  != null) base[i]  = cvt(base[i]);
          if (upper[i] != null) upper[i] = cvt(upper[i]);
          if (lower[i] != null) lower[i] = cvt(lower[i]);
        }
      }
    }
    const rain    = readings.map((r: any) => r.hourlyrainin ?? null);
    const maxRain = Math.max(0.01, ...rain.filter((v: any) => v != null && isFinite(v)));

    setHasBaseline(base.some((v: any) => v != null));
    setHasRain(rain.some((v: any) => v != null && v > 0));

    if (uplotRef.current) { uplotRef.current.destroy(); uplotRef.current = null; }
    wrap.innerHTML = '';
    const W = Math.max(100, Math.floor(wrap.getBoundingClientRect().width) || (window.innerWidth - 48));
    const H = window.innerWidth < 480 ? 180 : 220;

    try {
      uplotRef.current = new uPlot({
        width:  W,
        height: H,
        cursor: { y: false, drag: { x: false, y: false }, points: { size: 0 } } as any,
        legend: { show: false } as any,
        axes: [
          {
            stroke: '#999',
            ticks:  { stroke: '#2a2a2a', width: 1, size: 4 },
            grid:   { stroke: '#1e1e1e', width: 1 },
            values: (_u: any, ticks: number[]) => ticks.map(v => {
              if (v == null) return null;
              const d = new Date(v * 1000);
              if (hours > 24) return `${d.getMonth()+1}/${d.getDate()}`;
              const h = d.getHours(), ap = h >= 12 ? 'pm' : 'am';
              return `${h===0?12:h>12?h-12:h}${ap}`;
            }),
            font: '11px "NHG Display", "Neue Haas Grotesk Display Pro", -apple-system, sans-serif',
            size: 28, gap: 6,
          },
          {
            stroke: '#999',
            ticks:  { stroke: '#2a2a2a', width: 1, size: 4 },
            grid:   { stroke: '#1e1e1e', width: 1 },
            values: (_u: any, ticks: number[]) => ticks.map(v => v != null ? `${Number(v).toFixed(cfg.decimals)}${cfg.unit}` : null),
            font: '11px "NHG Display", "Neue Haas Grotesk Display Pro", -apple-system, sans-serif',
            size: window.innerWidth < 480 ? 48 : 56, gap: 6,
          },
          { show: false, scale: 'rain' },
        ],
        scales: { x: {}, y: { auto: true } as any, rain: { range: [0, maxRain * 14] } as any },
        series: [
          {} as any,
          { stroke: '#c8b97a', width: 2, fill: 'rgba(200,185,122,0.08)' } as any,
          { scale: 'rain', stroke: 'rgba(90,140,210,0.7)', fill: 'rgba(90,140,210,0.20)', width: 1 } as any,
        ],
        plugins: [
          makeDrawPlugin(ts, base, upper, lower),
          makeTooltipPlugin(readings, hours, field, useCelsius),
          makeNowLinePlugin(),
          makeZoomTrackerPlugin(ts, setZoomed),
          makeChartGesturesPlugin(),
        ] as any,
      } as any, [ts, vals, rain] as any, wrap);
      wrap.ondblclick = () => {
        const u = uplotRef.current;
        if (!u || !u.data?.[0]?.length) return;
        const xs = u.data[0];
        u.setScale('x', { min: xs[0], max: xs[xs.length - 1] });
      };
    } catch (e: any) {
      console.error('[wx chart]', e);
      wrap.innerHTML = `<div style="color:#c8b97a;font-size:11px;letter-spacing:0.08em;padding:20px 16px">CHART ERROR - ${e.message}</div>`;
    }
  }, [history, field, useCelsius, hours]);

  // ResizeObserver
  useEffect(() => {
    if (!wrapRef.current) return;
    const wrap = wrapRef.current;
    const ro = new ResizeObserver(() => {
      const u = uplotRef.current;
      if (u) {
        const w = Math.floor(wrap.getBoundingClientRect().width);
        if (w > 0) u.setSize({ width: w, height: window.innerWidth < 480 ? 180 : 220 });
      }
    });
    ro.observe(wrap);
    return () => ro.disconnect();
  }, []);

  const resetZoom = () => {
    const u = uplotRef.current;
    if (!u || !u.data?.[0]?.length) return;
    const xs = u.data[0];
    u.setScale('x', { min: xs[0], max: xs[xs.length - 1] });
  };

  const onCopy = async () => {
    const blob = await exportChartPng(uplotRef.current, field, hours);
    if (!blob) return;
    try {
      if (navigator.clipboard && (window as any).ClipboardItem) {
        await navigator.clipboard.write([new (window as any).ClipboardItem({ 'image/png': blob })]);
        return;
      }
    } catch (e) { console.warn('[copy chart]', e); }
    const url = URL.createObjectURL(blob);
    window.open(url, '_blank', 'noopener,noreferrer');
    setTimeout(() => URL.revokeObjectURL(url), 60_000);
  };

  const onShare = async () => {
    const blob = await exportChartPng(uplotRef.current, field, hours);
    if (!blob) return;
    const file = new File([blob], 'wx-chart.png', { type: 'image/png' });
    const cfg = FIELD_LABELS[field] || { label: field };
    const rangeLabel = RANGE_LABELS[hours] || `${hours}h`;
    const shareData = {
      title: `${cfg.label} · ${rangeLabel} - Midtown Manhattan`,
      text:  `${cfg.label} · ${rangeLabel} - wx.jamestannahill.com`,
      // Trailing slash matches the canonical URL, so X's card scraper resolves
      // the link it was handed rather than following a redirect to find it.
      url:   'https://wx.jamestannahill.com/',
    };
    try {
      if ((navigator as any).canShare && (navigator as any).canShare({ files: [file] })) {
        await (navigator as any).share({ ...shareData, files: [file] });
        return;
      }
      if (navigator.share) { await navigator.share(shareData); return; }
    } catch (e: any) {
      if (e?.name !== 'AbortError') console.warn('[share chart]', e);
    }
    await onCopy();
    const tweetText = `${shareData.text} (chart copied to clipboard, paste into the tweet) #NYwx`;
    window.open(
      // x.com/intent/post is the current endpoint; twitter.com/intent/tweet
      // still works but costs a redirect hop on the way in.
      `https://x.com/intent/post?text=${encodeURIComponent(tweetText)}&url=${encodeURIComponent(shareData.url)}`,
      '_blank', 'noopener,noreferrer,width=600,height=450',
    );
  };

  return (
    <section class="chart-section" id="history">
      <div class="chart-section-header">
        <h2 style="margin:0;font-size:10px;letter-spacing:0.15em;color:#444;font-weight:500">HISTORY</h2>
        <div class="chart-actions">
          {zoomed && <button class="chart-action-btn" title="Reset zoom" onClick={resetZoom}>↻</button>}
          <button class="chart-action-btn" title="Copy chart" onClick={onCopy}>⧉</button>
          <button class="chart-action-btn" title="Share chart" onClick={onShare}>↗</button>
          <a class="source-tag has-tooltip" href="/docs.html#cathouz" data-tooltip="CATHOUZ: this station's callsign (Ambient WS-2902, KNYNEWYO2140). In-house readings, stats, and ML signals. Click for docs.">CATHOUZ</a>
        </div>
      </div>
      <div class="chart-controls">
        <div class="chart-controls-row">
          {FIELD_OPTIONS.map(f => (
            <button class={`chart-btn${f === field ? ' active' : ''}`} data-field={f} onClick={() => setField(f)}>{FIELD_BTN_LABELS[f]}</button>
          ))}
        </div>
        <div class="chart-controls-row range-row">
          {HOURS_OPTIONS.map(h => (
            <button class={`range-btn${h === hours ? ' active' : ''}`} data-hours={h} onClick={() => setHours(h)}>
              {h === 12 ? '12h' : h === 24 ? '24h' : h === 168 ? '7d' : '30d'}
            </button>
          ))}
        </div>
      </div>
      <div ref={wrapRef} class="wx-chart-wrap" />
      <div class="chart-legend">
        <span class="chart-legend-item"><span class="chart-legend-swatch swatch-observed"></span>Observed</span>
        {hasBaseline && (
          <>
            <span class="chart-legend-item"><span class="chart-legend-swatch swatch-baseline"></span>Historical avg</span>
            <span class="chart-legend-item"><span class="chart-legend-swatch swatch-band"></span>±1σ normal range</span>
          </>
        )}
        {hasRain && <span class="chart-legend-item"><span class="chart-legend-swatch swatch-rain"></span>Rainfall</span>}
      </div>
      <div class="chart-hint">Scroll / pinch to zoom · Drag to pan · Double-click to reset</div>
    </section>
  );
}
