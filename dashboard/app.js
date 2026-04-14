const API_BASE = 'https://api.wx.jamestannahill.com';
const REFRESH_MS = 5 * 60 * 1000;

let uplot = null;
let currentField = 'tempf';
let currentHours = 24;
let lastHistory = null;

const FIELD_LABELS = {
  tempf:        { label: 'Temperature', unit: '°F',  decimals: 1 },
  humidity:     { label: 'Humidity',    unit: '%',   decimals: 0 },
  windspeedmph: { label: 'Wind',        unit: ' mph',decimals: 1 },
  baromrelin:   { label: 'Pressure',    unit: '"',   decimals: 2 },
  uhi_delta:    { label: 'Urban Heat',  unit: '°F',  decimals: 1 },
};

const DIR_LABELS = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW'];
function degToCompass(deg) {
  return DIR_LABELS[Math.round(deg / 22.5) % 16];
}

function fmt(val, decimals = 1) {
  if (val == null) return '—';
  return Number(val).toFixed(decimals);
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function fmtChartLabel(ts, hours) {
  const d = new Date(ts);
  if (hours > 168) return `${d.getMonth()+1}/${d.getDate()}`;
  if (hours > 24)  return `${d.getMonth()+1}/${d.getDate()} ${d.getHours()}:00`;
  const h = d.getHours(), ampm = h >= 12 ? 'pm' : 'am';
  return `${h === 0 ? 12 : h > 12 ? h - 12 : h}${ampm}`;
}

// ── Cache (stale-while-revalidate) ───────────────────────────────────────────
const CACHE_TTL = 10 * 60 * 1000; // evict entries older than 10 min

function cacheGet(key) {
  try {
    const raw = localStorage.getItem('wx_' + key);
    if (!raw) return null;
    const { d, t } = JSON.parse(raw);
    if (Date.now() - t > CACHE_TTL) { localStorage.removeItem('wx_' + key); return null; }
    return d;
  } catch { return null; }
}

function cacheSet(key, data) {
  try { localStorage.setItem('wx_' + key, JSON.stringify({ d: data, t: Date.now() })); } catch {}
}

// ── Fetch ─────────────────────────────────────────────────────────────────────
async function fetchCurrent() {
  const resp = await fetch(`${API_BASE}/current`);
  if (!resp.ok) throw new Error(`API error ${resp.status}`);
  return resp.json();
}

async function fetchHistory(hours = 24) {
  const resp = await fetch(`${API_BASE}/history?hours=${hours}`);
  if (!resp.ok) throw new Error(`API error ${resp.status}`);
  return resp.json();
}

// ── Current conditions ────────────────────────────────────────────────────────
function renderCurrent(data) {
  document.getElementById('temp').textContent = fmt(data.tempf, 0);
  document.getElementById('feels-like').textContent = `Feels like ${fmt(data.feelsLike, 0)}°F`;
  document.getElementById('condition').textContent = data.condition || '—';

  const pr = data.percentile_rank;
  document.getElementById('percentile-rank').textContent = pr ? pr.label : '';

  const topAnomaly = data.anomalies?.temp;
  const anomalyEl = document.getElementById('anomaly-headline');
  anomalyEl.textContent = topAnomaly ? topAnomaly.label : '';
  if (topAnomaly && data.baseline_sample_count > 0) {
    const n = data.baseline_sample_count.toLocaleString();
    const src = data.baseline_source === 'era5'
      ? `ERA5 climate reanalysis + ${n} station readings`
      : `${n} station readings`;
    anomalyEl.setAttribute('data-tooltip', `Based on ${src} · 5-min weighted running average`);
  } else {
    anomalyEl.removeAttribute('data-tooltip');
  }

  document.getElementById('wind-speed').textContent = `${fmt(data.windspeedmph, 0)} mph`;
  document.getElementById('wind-detail').textContent =
    `From ${degToCompass(data.winddir)} (${fmt(data.winddir, 0)}°)`;

  document.getElementById('humidity').textContent = `${fmt(data.humidity, 0)}%`;
  document.getElementById('dewpoint').textContent = `Dew point ${fmt(data.dewPoint, 0)}°F`;

  document.getElementById('pressure').textContent = `${fmt(data.baromrelin, 2)}"`;
  const trend = data.pressure_trend || 'steady';
  document.getElementById('pressure-trend').textContent =
    trend === 'rising' ? '↑ Rising' : trend === 'falling' ? '↓ Falling' : '→ Steady';

  document.getElementById('uv').textContent = fmt(data.uv, 0);
  document.getElementById('solar').textContent = `${fmt(data.solarradiation, 0)} W/m²`;

  document.getElementById('rain-hourly').textContent = `${fmt(data.hourlyrainin, 2)}"`;
  document.getElementById('rain-daily').textContent = `Daily: ${fmt(data.dailyrainin, 2)}"`;

  document.getElementById('wind-gust').textContent = `${fmt(data.windgustmph, 0)} mph`;
  document.getElementById('wind-dir').textContent = degToCompass(data.winddir);

  // Comfort Score
  const comfort = data.comfort;
  document.getElementById('comfort-score').textContent = comfort?.score ?? '—';
  document.getElementById('comfort-label').textContent = comfort?.label ?? '—';

  // Rain Probability
  const rp = data.rain_probability;
  document.getElementById('rain-prob').textContent = rp ? `${rp.probability}%` : '—';
  // Spatial boost source annotation: combine label with boost source when present
  const rainProbLabelEl = document.getElementById('rain-prob-label');
  rainProbLabelEl.textContent = rp?.spatial_source
    ? `${rp?.label ?? '—'} · ↑ ${rp.spatial_source}`
    : (rp?.label ?? '—');

  // Urban Heat Island
  const uhi = data.uhi_delta;
  document.getElementById('uhi-delta').textContent =
    uhi != null ? `${uhi >= 0 ? '+' : ''}${uhi.toFixed(1)}°F` : '—';
  document.getElementById('uhi-label').textContent = data.uhi_label ?? 'vs JFK / LGA / EWR';

  // Seasonal UHI average for current month
  const uhiMonthlyEl = document.getElementById('uhi-monthly-avg');
  const seasonal = data.uhi_seasonal_curve;
  if (seasonal && seasonal.length > 0) {
    const curMonth = new Date().getMonth() + 1;
    const entry = seasonal.find(m => m.month === curMonth);
    if (entry && entry.avg_delta != null && entry.sample_count >= 10) {
      const avg = entry.avg_delta;
      uhiMonthlyEl.textContent =
        `Typical ${entry.month_name}: ${avg >= 0 ? '+' : ''}${avg.toFixed(1)}°F`;
    } else {
      uhiMonthlyEl.textContent = '';
    }
  } else {
    uhiMonthlyEl.textContent = '';
  }

  const updated = data.updated_at ? new Date(data.updated_at).toLocaleTimeString() : '—';
  document.getElementById('updated-at').textContent = `Updated ${updated}`;

  renderForecast(data.forecast);

  // Stale / quality warnings
  const banner  = document.getElementById('stale-banner');
  const staleMsg = document.getElementById('stale-msg');
  if (data.quality_flag === 'stuck') {
    staleMsg.textContent = `Sensor may be frozen — last valid reading ${data.data_age_minutes ?? '?'} min ago`;
    banner.hidden = false;
  } else if (data.data_stale) {
    staleMsg.textContent = `Station data is ${data.data_age_minutes ?? '?'} minutes old — sensor may be offline`;
    banner.hidden = false;
  } else if (data.quality_flag === 'range_error') {
    staleMsg.textContent = 'One or more sensor fields returned implausible values and were excluded';
    banner.hidden = false;
  } else {
    banner.hidden = true;
  }
}

// ── Forecast ─────────────────────────────────────────────────────────────────
function renderForecast(forecast) {
  const section = document.getElementById('forecast-section');
  if (!forecast || !forecast.hours?.length) { section.hidden = true; return; }

  section.hidden = false;

  const confidence = forecast.confidence ?? 0;
  const confLabel  = confidence >= 70 ? 'High confidence'
                   : confidence >= 45 ? 'Moderate confidence'
                   : 'Low confidence';
  const metaParts = [
    `${confLabel} (${confidence}%)`,
    forecast.best_match_label ? `best analog: ${forecast.best_match_label}` : '',
  ];
  if (forecast.accuracy && forecast.accuracy.mae_1h_tempf != null) {
    const n = forecast.accuracy.evaluation_count;
    metaParts.push(`±${forecast.accuracy.mae_1h_tempf.toFixed(1)}°F avg error (+1h, n=${n})`);
  }
  document.getElementById('forecast-meta').textContent = metaParts.filter(Boolean).join(' · ');

  const grid = document.getElementById('forecast-grid');
  grid.innerHTML = forecast.hours.map(h => {
    const label = h.offset_hours === 1 ? '+1 hour'
                : h.offset_hours === 2 ? '+2 hours' : '+3 hours';
    return `
      <div class="forecast-card">
        <div class="forecast-offset">${label}</div>
        <div class="forecast-temp">${fmt(h.tempf, 0)}°F</div>
        <div class="forecast-fields">
          <div class="forecast-field">Humidity ${fmt(h.humidity, 0)}%</div>
          <div class="forecast-field">Wind ${fmt(h.windspeedmph, 0)} mph</div>
          <div class="forecast-field">Pressure ${fmt(h.baromrelin, 2)}"</div>
        </div>
      </div>`;
  }).join('');
}

// ── Chart (uPlot) ─────────────────────────────────────────────────────────────
function makeTooltipPlugin(readings, hours, activeField) {
  let el;
  return {
    hooks: {
      init: [u => {
        el = document.createElement('div');
        el.className = 'wx-tooltip';
        el.hidden = true;
        u.over.style.overflow = 'visible';
        u.over.appendChild(el);
      }],
      setCursor: [u => {
        const { left, idx } = u.cursor;
        if (idx == null || idx < 0 || left < 0) { el.hidden = true; return; }
        const r = readings[idx];
        if (!r) { el.hidden = true; return; }

        const active = v => `<span class="wxt-active">${v}</span>`;
        const rows = [];
        rows.push(`<div class="wxt-time">${fmtChartLabel(new Date(r.timestamp).getTime(), hours)}</div>`);
        if (r.tempf        != null) rows.push(`<div>Temp&ensp;${activeField==='tempf'        ? active(r.tempf.toFixed(1)+'°F')        : r.tempf.toFixed(1)+'°F'}</div>`);
        if (r.humidity     != null) rows.push(`<div>RH&emsp;&ensp;${activeField==='humidity'    ? active(r.humidity.toFixed(0)+'%')       : r.humidity.toFixed(0)+'%'}</div>`);
        if (r.windspeedmph != null) rows.push(`<div>Wind&ensp;${activeField==='windspeedmph'  ? active(r.windspeedmph.toFixed(1)+' mph') : r.windspeedmph.toFixed(1)+' mph'}</div>`);
        if (r.baromrelin   != null) rows.push(`<div>Pres&ensp;${activeField==='baromrelin'    ? active(r.baromrelin.toFixed(2)+'"')      : r.baromrelin.toFixed(2)+'"'}</div>`);
        if (r.uhi_delta    != null) rows.push(`<div>UHI&emsp;&ensp;${activeField==='uhi_delta' ? active((r.uhi_delta>=0?'+':'')+r.uhi_delta.toFixed(1)+'°F') : (r.uhi_delta>=0?'+':'')+r.uhi_delta.toFixed(1)+'°F'}</div>`);
        if ((r.hourlyrainin??0) > 0.005) rows.push(`<div>Rain&ensp;${r.hourlyrainin.toFixed(2)}"/hr</div>`);

        el.innerHTML = rows.join('');
        el.hidden = false;

        // Flip left/right so tooltip stays inside the chart
        const overW = u.over.offsetWidth;
        const tipW  = el.offsetWidth || 130;
        const flip  = left + tipW + 18 > overW;
        el.style.left  = flip ? 'auto' : `${left + 14}px`;
        el.style.right = flip ? `${overW - left + 14}px` : 'auto';
        el.style.top   = '6px';
      }]
    }
  };
}

function makeDrawPlugin(tsArr, baseArr, upperArr, lowerArr) {
  return {
    hooks: {
      drawClear: [u => {
        try {
          const { ctx, bbox } = u;
          ctx.save();
          ctx.fillStyle = '#0e0e0e';
          ctx.fillRect(bbox.left, bbox.top, bbox.width, bbox.height);
          ctx.restore();
        } catch(e) { console.warn('[drawClear]', e); }
      }],
      draw: [u => {
        try {
          const { ctx } = u;
          ctx.save();

          // σ band fill: trace upper forward, lower backward, fill closed path
          const hasUpper = upperArr && upperArr.some(v => v != null);
          const hasLower = lowerArr && lowerArr.some(v => v != null);
          if (hasUpper && hasLower) {
            ctx.beginPath();
            let started = false;
            for (let i = 0; i < tsArr.length; i++) {
              if (upperArr[i] == null) { started = false; continue; }
              const px = Math.round(u.valToPos(tsArr[i],    'x', true));
              const py = Math.round(u.valToPos(upperArr[i], 'y', true));
              if (!started) { ctx.moveTo(px, py); started = true; }
              else            ctx.lineTo(px, py);
            }
            for (let i = tsArr.length - 1; i >= 0; i--) {
              if (lowerArr[i] == null) continue;
              const px = Math.round(u.valToPos(tsArr[i],    'x', true));
              const py = Math.round(u.valToPos(lowerArr[i], 'y', true));
              ctx.lineTo(px, py);
            }
            ctx.closePath();
            ctx.fillStyle = 'rgba(110,120,150,0.09)';
            ctx.fill();
          }

          // Dashed baseline
          const hasBase = baseArr && baseArr.some(v => v != null);
          if (hasBase) {
            ctx.setLineDash([4, 6]);
            ctx.strokeStyle = 'rgba(255,255,255,0.20)';
            ctx.lineWidth   = 1;
            ctx.beginPath();
            let started = false;
            for (let i = 0; i < tsArr.length; i++) {
              if (baseArr[i] == null) { started = false; continue; }
              const px = Math.round(u.valToPos(tsArr[i],   'x', true));
              const py = Math.round(u.valToPos(baseArr[i], 'y', true));
              if (!started) { ctx.moveTo(px, py); started = true; }
              else           ctx.lineTo(px, py);
            }
            ctx.stroke();
          }

          ctx.restore();
        } catch(e) { console.warn('[draw]', e); }
      }],
    }
  };
}

function makeNowLinePlugin() {
  return {
    hooks: {
      draw: [u => {
        const xs = u.data[0];
        if (!xs || xs.length < 2) return;
        const nowS = Date.now() / 1000;
        if (nowS < xs[0] || nowS > xs[xs.length - 1] + 7200) return;
        let best = 0, bestD = Infinity;
        xs.forEach((t, i) => { const d = Math.abs(t - nowS); if (d < bestD) { bestD = d; best = i; } });
        const x = Math.round(u.valToPos(xs[best], 'x', true));
        const { ctx, bbox } = u;
        ctx.save();
        ctx.setLineDash([4, 5]);
        ctx.strokeStyle = 'rgba(255,255,255,0.13)';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(x, bbox.top);
        ctx.lineTo(x, bbox.top + bbox.height);
        ctx.stroke();
        ctx.restore();
      }]
    }
  };
}

function renderChart(history, field, hours) {
  if (!history?.readings?.length) return;
  setTimeout(() => _renderChart(history, field, hours), 50);
}

function _renderChart(history, field, hours) {
  const wrap = document.getElementById('wx-chart-wrap');
  const dbg  = document.getElementById('wx-chart-status');
  const cfg      = FIELD_LABELS[field] || { label: field, unit: '', decimals: 1 };
  const readings = history.readings;

  const ts      = readings.map(r => new Date(r.timestamp).getTime() / 1000);
  const vals    = readings.map(r => r[field] ?? null);
  const base    = readings.map(r => r[`baseline_${field}`] ?? null);
  const upper   = readings.map(r => { const b = r[`baseline_${field}`], s = r[`baseline_std_${field}`]; return b!=null&&s!=null ? b+s : null; });
  const lower   = readings.map(r => { const b = r[`baseline_${field}`], s = r[`baseline_std_${field}`]; return b!=null&&s!=null ? b-s : null; });
  const rain    = readings.map(r => r.hourlyrainin ?? null);
  const maxRain = Math.max(0.01, ...rain.filter(v => v != null && isFinite(v)));

  if (uplot) { uplot.destroy(); uplot = null; }
  wrap.innerHTML = '';

  // Show/hide legend rows based on available data
  const hasBaseline = base.some(v => v != null);
  const hasRain     = rain.some(v => v != null && v > 0);
  const legend = document.getElementById('wx-chart-legend');
  if (legend) {
    legend.querySelectorAll('.chart-legend-item').forEach(el => {
      const sw = el.querySelector('.chart-legend-swatch');
      if (!sw) return;
      if (sw.classList.contains('swatch-baseline') || sw.classList.contains('swatch-band'))
        el.style.display = hasBaseline ? '' : 'none';
      if (sw.classList.contains('swatch-rain'))
        el.style.display = hasRain ? '' : 'none';
    });
    legend.style.display = '';
  }

  const W = Math.max(100, wrap.clientWidth || (window.innerWidth - 48));
  const H = window.innerWidth < 480 ? 180 : 220;

  try {
    uplot = new uPlot({
      width:  W,
      height: H,
      cursor: { y: false, drag: { x: false, y: false }, points: { size: 0 } },
      legend: { show: false },
      axes: [
        {
          stroke: '#999',
          ticks:  { stroke: '#2a2a2a', width: 1, size: 4 },
          grid:   { stroke: '#1e1e1e', width: 1 },
          values: (u, ticks) => ticks.map(v => {
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
          values: (u, ticks) => ticks.map(v => v != null ? `${Number(v).toFixed(cfg.decimals)}${cfg.unit}` : null),
          font: '11px "NHG Display", "Neue Haas Grotesk Display Pro", -apple-system, sans-serif',
          size: window.innerWidth < 480 ? 48 : 56, gap: 6,
        },
        { show: false, scale: 'rain' },
      ],
      scales: { x: {}, y: { auto: true }, rain: { range: [0, maxRain * 14] } },
      series: [
        {},
        { stroke: '#c8b97a', width: 2, fill: 'rgba(200,185,122,0.08)' },
        { scale: 'rain', stroke: 'rgba(90,140,210,0.7)', fill: 'rgba(90,140,210,0.20)', width: 1 },
      ],
      plugins: [
        makeDrawPlugin(ts, base, upper, lower),
        makeTooltipPlugin(readings, hours, field),
        makeNowLinePlugin(),
      ],
    }, [ts, vals, rain], wrap);
    if (dbg) dbg.textContent = '';
  } catch (e) {
    console.error('[wx chart]', e);
    if (dbg) dbg.textContent = `ERR: ${e.message}`;
    wrap.innerHTML = `<div style="color:#c8b97a;font-size:11px;letter-spacing:0.08em;padding:20px 16px">CHART ERROR — ${e.message}</div>`;
  }
}

// Resize chart with window
const _chartWrap = document.getElementById('wx-chart-wrap');
new ResizeObserver(() => {
  if (uplot) uplot.setSize({ width: _chartWrap.clientWidth, height: window.innerWidth < 480 ? 180 : 220 });
}).observe(_chartWrap);

// ── Tomorrow forecast (WeatherKit) ───────────────────────────────────────────
function renderTomorrow(nws, attr) {
  const section = document.getElementById('tomorrow-section');
  if (!nws || !nws.detailed) { section.hidden = true; return; }
  document.getElementById('tomorrow-date').textContent = nws.name.toUpperCase();
  document.getElementById('tomorrow-text').textContent = nws.detailed;
  section.hidden = false;

  // Swap in Apple's official logo + link if attribution data is available
  const tag = document.getElementById('wk-attr-tag');
  if (tag && attr) {
    const logoUrl  = attr.logo_dark_2x || attr.logo_square_2x;
    const legalUrl = attr.legal_url || 'https://weatherkit.apple.com/legal-attribution.html';
    tag.outerHTML = `<a id="wk-attr-tag" href="${legalUrl}" target="_blank" rel="noopener"
      class="source-tag source-external has-tooltip"
      style="display:inline-flex;align-items:center;gap:5px;text-decoration:none"
      data-tooltip="Weather data provided by Apple WeatherKit. Tap to view data sources and legal attribution."
      >${logoUrl ? `<img src="${logoUrl}" alt="Apple Weather" style="width:14px;height:14px;border-radius:3px;vertical-align:middle">` : ''}WEATHERKIT</a>`;
  }
}

// ── Today so far ─────────────────────────────────────────────────────────────
function renderTodayContext(current, history) {
  const section = document.getElementById('today-section');
  const textEl  = document.getElementById('today-text');
  if (!history || !history.readings) { section.hidden = true; return; }

  // Filter to readings in the browser's local "today"
  const todayStr = new Date().toLocaleDateString('en-CA'); // YYYY-MM-DD
  const todayReadings = history.readings.filter(r =>
    new Date(r.timestamp).toLocaleDateString('en-CA') === todayStr
  );
  if (todayReadings.length < 3) { section.hidden = true; return; }

  const temps = todayReadings.map(r => r.tempf).filter(v => v != null);
  const gusts = todayReadings.map(r => r.windgustmph).filter(v => v != null);
  const high     = temps.length  ? Math.round(Math.max(...temps))  : null;
  const low      = temps.length  ? Math.round(Math.min(...temps))  : null;
  const maxGust  = gusts.length  ? Math.round(Math.max(...gusts))  : null;
  const rain     = current.dailyrainin ?? 0;

  const sentences = [];

  // Temp range
  if (high != null && low != null) {
    sentences.push(high - low < 3
      ? `Temperatures holding near ${Math.round((high + low) / 2)}°F.`
      : `Temperatures ranging from ${low}°F to ${high}°F so far.`);
  }

  // Wind gusts
  if (maxGust != null && maxGust >= 15) sentences.push(`Wind gusts to ${maxGust} mph.`);

  // Rain
  if (rain > 0.01) sentences.push(`${rain.toFixed(2)}" of rain since midnight.`);

  // Anomaly — only if notable (≥5°F delta)
  const anomaly = current.anomalies?.temp;
  if (anomaly && Math.abs(anomaly.delta) >= 5) {
    const cap = anomaly.label[0].toUpperCase() + anomaly.label.slice(1);
    sentences.push(cap + '.');
  }

  if (!sentences.length) { section.hidden = true; return; }
  textEl.textContent = sentences.join(' ');
  section.hidden = false;
}

// ── Climate Panel ─────────────────────────────────────────────────────────────
function renderClimatePanel(data) {
  const section = document.getElementById('climate-panel');
  const cc = data.climate_context;
  if (!cc || ((!cc.metrics || !Object.keys(cc.metrics).length) && !cc.verdict)) {
    section.hidden = true;
    return;
  }
  section.hidden = false;
  section.classList.add('visible');

  // Update anomaly subline
  const subline = document.getElementById('anomaly-subline');
  if (cc.headline) {
    subline.textContent = cc.headline;
  } else {
    subline.textContent = '';
  }

  const isDaily = cc.mode === 'daily' && cc.verdict;
  const container = document.getElementById('climate-metrics');
  container.innerHTML = '';

  // Color map per metric
  const COLORS = { temp: '#e8c84a', dewpoint: '#4ab8e8', wind: '#888888' };

  if (isDaily) {
    // Daily verdict: high temp, low temp from NOAA
    const verdict = cc.verdict;
    const rows = [
      { key: 'temp_high', label: 'High Temp', unit: '°F', color: COLORS.temp },
      { key: 'temp_low',  label: 'Low Temp',  unit: '°F', color: '#4ab8e8' },
    ];
    rows.forEach(({ key, label, unit, color }) => {
      const m = verdict[key];
      if (!m) return;
      const pct   = m.percentile;
      const since = m.last_exceeded_year ? `since ${m.last_exceeded_year}` : 'on record';

      // Deviation bar: track spans p5→max(p95,value)+buffer; fill avg→today
      let barHtml = '';
      if (m.p5 != null && m.p50 != null && m.p95 != null) {
        const trackMin  = m.p5;
        const trackMax  = Math.max(m.p95, m.value) + (m.p95 - m.p5) * 0.06;
        const span      = trackMax - trackMin;
        const avgPct    = Math.max(1,  Math.min(96, (m.p50  - trackMin) / span * 100));
        const todayPct  = Math.max(3,  Math.min(97, (m.value - trackMin) / span * 100));
        const fillLeft  = Math.min(avgPct, todayPct);
        const fillWidth = Math.abs(todayPct - avgPct);
        // Clamp label anchors so they don't bleed off edges
        const avgLblPct   = Math.max(4,  Math.min(50, avgPct));
        const todayLblPct = Math.max(50, Math.min(93, todayPct));
        barHtml = `
          <div class="dev-bar">
            <div class="dev-bar-axis-labels">
              <span style="left:${avgLblPct}%">avg ${m.p50}°</span>
              <span style="left:${todayLblPct}%;color:${color}">${m.value}°</span>
            </div>
            <div class="dev-bar-track">
              <div class="dev-bar-fill" style="left:${fillLeft}%;width:${fillWidth}%;background:${color}"></div>
              <div class="dev-bar-avg-tick" style="left:${avgPct}%"></div>
              <div class="dev-bar-today-dot" style="left:${todayPct}%;background:${color}"></div>
            </div>
          </div>`;
      }

      container.innerHTML += `
        <div class="climate-metric">
          <div class="climate-metric-row">
            <span class="climate-metric-label">${label}</span>
            <span class="climate-metric-value" style="color:${color}">${m.value}${unit}
              <span class="climate-metric-pct">${pct}th pct · ${since}</span>
            </span>
          </div>
          ${barHtml}
        </div>`;
    });

    // Dew point from ERA5 (live metrics, muted)
    const dp = cc.metrics && cc.metrics.dewpoint;
    if (dp) {
      container.innerHTML += `
        <div class="climate-metric" style="opacity:0.6">
          <div class="climate-metric-row">
            <span class="climate-metric-label">Dew Point <span style="font-size:9px;color:#444">(ERA5)</span></span>
            <span class="climate-metric-value" style="color:#4ab8e8">${dp.value}°F
              <span class="climate-metric-pct">${dp.percentile}th pct</span>
            </span>
          </div>
        </div>`;
    }

    const yrs = verdict.temp_high?.years_of_data ?? 156;
    document.getElementById('climate-footer').textContent =
      `NOAA Central Park 1869–${new Date().getFullYear()} · ${yrs} yrs`;
    document.getElementById('climate-source-tag').setAttribute(
      'data-tooltip',
      'NOAA GHCN-Daily station USC00305801 (Central Park). Daily high/low temperature going back to 1869.'
    );
    document.getElementById('climate-source-tag').textContent = 'NOAA · GHCN';

  } else {
    // Live mode: current percentile for temp, dewpoint, wind from ERA5
    const metrics = cc.metrics || {};
    const order = [
      { key: 'temp',     label: 'Temperature', unit: '°F',  color: COLORS.temp },
      { key: 'dewpoint', label: 'Dew Point',   unit: '°F',  color: COLORS.dewpoint },
      { key: 'wind',     label: 'Wind Speed',  unit: ' mph', color: COLORS.wind },
    ];
    order.forEach(({ key, label, unit, color }) => {
      const m = metrics[key];
      if (!m) return;
      const pct = m.percentile;
      container.innerHTML += `
        <div class="climate-metric">
          <div class="climate-metric-row">
            <span class="climate-metric-label">${label}</span>
            <span class="climate-metric-value" style="color:${color}">${m.value}${unit}
              <span class="climate-metric-pct">${pct}th pct</span>
            </span>
          </div>
          <div class="climate-bar-track">
            <div class="climate-bar-fill" style="width:${pct}%;background:linear-gradient(90deg,#222,${color})"></div>
            <div class="climate-bar-marker" style="left:${pct}%;background:${color}"></div>
          </div>
          <div class="climate-bar-ticks">
            <span>p25: ${m.p25 ?? '—'}</span>
            <span>p50: ${m.p50 ?? '—'}</span>
            <span>p75: ${m.p75 ?? '—'}</span>
          </div>
        </div>`;
    });

    const yrs = metrics.temp?.years_of_data ?? 85;
    document.getElementById('climate-footer').textContent =
      `ERA5 1940–${new Date().getFullYear() - 1} · ${yrs} yrs`;
    document.getElementById('climate-source-tag').setAttribute(
      'data-tooltip',
      'ERA5 reanalysis via Open-Meteo Archive. Hourly temperature, dew point, and wind for this exact lat/lon going back to 1940.'
    );
    document.getElementById('climate-source-tag').textContent = 'ERA5';
  }
}

// ── Summary ───────────────────────────────────────────────────────────────────
function renderSummary(summary) {
  const section = document.getElementById('summary-section');
  if (!summary || !summary.summary) { section.hidden = true; return; }
  section.hidden = false;
  const d = new Date(summary.date + 'T12:00:00');
  const opts = { weekday: 'long', month: 'long', day: 'numeric' };
  document.getElementById('summary-date').textContent = d.toLocaleDateString('en-US', opts);
  document.getElementById('summary-text').textContent = summary.summary;
}

// ── Comfort Calendar ──────────────────────────────────────────────────────────
function renderComfortCalendar(summaries) {
  const section = document.getElementById('comfort-calendar-section');
  if (!summaries || !summaries.length) { section.hidden = true; return; }
  section.hidden = false;

  const sorted = [...summaries].sort((a, b) => a.date.localeCompare(b.date));
  const grid = document.getElementById('comfort-grid');
  grid.innerHTML = sorted.map(s => {
    const score = s.avg_comfort ?? 0;
    const hue   = Math.round(score * 1.2);  // 0=red(0°), 100=green(120°)
    const color = `hsl(${hue}, 55%, 30%)`;
    const d     = new Date(s.date + 'T12:00:00');
    const label = `${d.getMonth() + 1}/${d.getDate()}`;
    const rain  = s.total_rain > 0.01 ? ` · ${Number(s.total_rain).toFixed(2)}"` : '';
    const tip   = `${s.date}: ${score}/100 comfort · ${s.temp_high}°–${s.temp_low}°F${rain}`;
    return `<div class="comfort-cell has-tooltip" style="background:${color}" data-tooltip="${tip}">
      <span class="comfort-cell-label">${label}</span>
    </div>`;
  }).join('');

  // Re-bind tooltip to dynamically created cells
  grid.querySelectorAll('.has-tooltip').forEach(bindTip);

  const avg = Math.round(summaries.reduce((s, d) => s + (d.avg_comfort ?? 0), 0) / summaries.length);
  document.getElementById('comfort-meta').textContent = `${summaries.length}-day avg · ${avg}/100`;
}

// ── Station Records ───────────────────────────────────────────────────────────
function renderStationRecords(records) {
  const section = document.getElementById('records-section');
  if (!records) { section.hidden = true; return; }
  section.hidden = false;

  document.getElementById('records-month').textContent = records.scope === 'all-time' ? 'all time' : (records.month_name || '');

  const items = [];
  if (records.temp_high    != null) items.push({ label: 'HIGH TEMP',     value: `${records.temp_high}°F`,     date: records.temp_high_at });
  if (records.temp_low     != null) items.push({ label: 'LOW TEMP',      value: `${records.temp_low}°F`,      date: records.temp_low_at });
  if (records.max_gust     != null) items.push({ label: 'MAX GUST',      value: `${records.max_gust} mph`,    date: records.max_gust_at });
  if (records.max_rain_rate!= null) items.push({ label: 'PEAK RAIN',     value: `${records.max_rain_rate}"/hr`, date: records.max_rain_rate_at });
  if (records.min_pressure != null) items.push({ label: 'MIN PRESSURE',  value: `${records.min_pressure}"`,   date: records.min_pressure_at });
  if (records.max_pressure != null) items.push({ label: 'MAX PRESSURE',  value: `${records.max_pressure}"`,   date: records.max_pressure_at });

  document.getElementById('records-grid').innerHTML = items.map(it => `
    <div class="record-card">
      <div class="record-label">${it.label}</div>
      <div class="record-value">${it.value}</div>
      <div class="record-date">${it.date || ''}</div>
    </div>`).join('');
}

// ── Nearby Stations ───────────────────────────────────────────────────────────
function renderNearby(stations, snapshotAt) {
  const strip = document.getElementById('nearby-strip');
  const meta = document.getElementById('nearby-meta');
  if (!stations || !stations.length) {
    document.getElementById('nearby-section').style.display = 'none';
    return;
  }
  document.getElementById('nearby-section').style.display = '';
  if (snapshotAt) meta.textContent = new Date(snapshotAt).toLocaleTimeString();

  strip.innerHTML = stations.map(s => {
    const temp = s.temp_f != null ? `${Math.round(s.temp_f)}°` : '–';
    const rain = s.rain_rate_in_hr > 0 ? `${s.rain_rate_in_hr.toFixed(2)}" /hr` : '';
    const dist = s.distance_mi != null ? `${s.distance_mi.toFixed(1)} mi` : '';
    return `<div class="nearby-chip">
      <div class="nearby-chip-id">${s.station_id ?? ''}</div>
      <div class="nearby-chip-temp">${temp}</div>
      ${rain ? `<div class="nearby-chip-rain">${rain}</div>` : ''}
      <div class="nearby-chip-dist">${dist}</div>
    </div>`;
  }).join('');
}

// ── Rain Events ───────────────────────────────────────────────────────────────
// ── Secondary data (comfort calendar) ────────────────────────────────────────
let secondaryLoaded = false;
async function loadSecondaryData() {
  const cachedSummaries = cacheGet('daily_summaries');
  if (cachedSummaries) renderComfortCalendar(cachedSummaries);

  try {
    const sumResp = await fetch(`${API_BASE}/daily-summaries?days=30`);
    if (sumResp.ok) {
      const d = await sumResp.json();
      cacheSet('daily_summaries', d.summaries || []);
      renderComfortCalendar(d.summaries || []);
    }
    secondaryLoaded = true;
  } catch (e) {
    console.error('Secondary data load failed:', e);
  }
}

// ── Refresh ───────────────────────────────────────────────────────────────────
async function refresh(forceHistory = false) {
  try {
    const fetches = [fetchCurrent()];
    if (forceHistory || !lastHistory) fetches.push(fetchHistory(currentHours));
    const [current, history] = await Promise.all(fetches);

    cacheSet('current', current);
    renderCurrent(current);
    renderTomorrow(current.nws_tomorrow, current.wk_attribution);
    renderSummary(current.daily_summary);
    renderClimatePanel(current);
    renderStationRecords(current.station_records);
    renderNearby(current.nearby_stations, null);

    if (history) {
      cacheSet('history_' + currentHours, history);
      lastHistory = history;
    }
    if (lastHistory) renderChart(lastHistory, currentField, currentHours);
    renderTodayContext(current, lastHistory);
  } catch (e) {
    console.error('Refresh failed:', e);
  }
}

// ── Controls ──────────────────────────────────────────────────────────────────
document.querySelectorAll('.chart-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.chart-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    currentField = btn.dataset.field;
    if (lastHistory) renderChart(lastHistory, currentField, currentHours);
  });
});

document.querySelectorAll('.range-btn').forEach(btn => {
  btn.addEventListener('click', async () => {
    document.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    currentHours = parseInt(btn.dataset.hours, 10);
    // Show cached range immediately if available
    const cached = cacheGet('history_' + currentHours);
    if (cached) { lastHistory = cached; renderChart(lastHistory, currentField, currentHours); }
    try {
      const history = await fetchHistory(currentHours);
      cacheSet('history_' + currentHours, history);
      lastHistory = history;
      renderChart(lastHistory, currentField, currentHours);
    } catch (e) { console.error('Range fetch failed:', e); }
  });
});

const refreshBtn = document.getElementById('refresh-btn');
refreshBtn.addEventListener('click', async () => {
  refreshBtn.classList.add('spinning');
  await refresh(true);
  refreshBtn.classList.remove('spinning');
});

// ── Viewport-aware tooltip manager ───────────────────────────────────────────
// Single fixed div — no overflow possible. Replaces CSS ::after approach.
const _tipEl = document.createElement('div');
_tipEl.id = 'wx-tip';
_tipEl.style.display = 'none';
document.body.appendChild(_tipEl);

function _showTip(anchor) {
  const text = anchor.dataset.tooltip;
  if (!text) return;
  _tipEl.textContent = text;
  _tipEl._anchor = anchor;
  _tipEl.style.display = 'block';

  // getBoundingClientRect returns viewport coords — correct for position:fixed
  const r   = anchor.getBoundingClientRect();
  const tw  = _tipEl.offsetWidth;
  const th  = _tipEl.offsetHeight;
  const vw  = window.innerWidth;
  const vh  = window.innerHeight;
  const gap = 8;
  const pad = 10;

  // Prefer below anchor; flip above if bottom clips
  let top = r.bottom + gap;
  if (top + th > vh - pad) top = r.top - th - gap;
  if (top < pad) top = pad;

  // Left-align with anchor; shift if right clips; clamp to left edge
  let left = r.left;
  if (left + tw > vw - pad) left = vw - tw - pad;
  if (left < pad) left = pad;

  _tipEl.style.left = left + 'px';
  _tipEl.style.top  = top  + 'px';
}

function _hideTip() {
  _tipEl.style.display = 'none';
  _tipEl._anchor = null;
}

function bindTip(el) {
  el.addEventListener('mouseenter', () => _showTip(el));
  el.addEventListener('mouseleave', _hideTip);
  // Touch: tap to show/dismiss — do NOT preventDefault so card clicks still fire
  el.addEventListener('touchend', e => {
    if (!el.dataset.tooltip) return;
    const wasThis = _tipEl._anchor === el && _tipEl.style.display !== 'none';
    _hideTip();
    if (!wasThis) {
      // Show after a short delay so the tap doesn't immediately dismiss via the doc listener
      setTimeout(() => {
        _showTip(el);
        const dismiss = ev => {
          if (!el.contains(ev.target)) {
            _hideTip();
            document.removeEventListener('touchstart', dismiss);
          }
        };
        setTimeout(() => document.addEventListener('touchstart', dismiss), 50);
      }, 0);
    }
  }, { passive: true });
}

document.getElementById('anomaly-headline') && bindTip(document.getElementById('anomaly-headline'));
document.querySelectorAll('.has-tooltip').forEach(bindTip);

// ── KPI card click-ins ────────────────────────────────────────────────────────
document.querySelectorAll('.card[data-chart-field]').forEach(card => {
  card.addEventListener('click', () => {
    const field = card.dataset.chartField;
    document.querySelectorAll('.chart-btn').forEach(b => b.classList.remove('active'));
    const btn = document.querySelector(`.chart-btn[data-field="${field}"]`);
    if (btn) btn.classList.add('active');
    currentField = field;
    if (lastHistory) renderChart(lastHistory, currentField, currentHours);
    document.querySelector('.chart-section').scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
});

document.querySelectorAll('.card[data-scroll-to]').forEach(card => {
  card.addEventListener('click', () => {
    const el = document.getElementById(card.dataset.scrollTo);
    if (el && !el.hidden) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
});

// ── Boot ──────────────────────────────────────────────────────────────────────
let _bootCurrent = null;

async function boot() {
  // Phase 0 — paint from cache instantly (0ms on repeat visits)
  const cc = cacheGet('current');
  const ch = cacheGet('history_' + currentHours);
  if (cc) {
    _bootCurrent = cc;
    renderCurrent(cc);
    renderTomorrow(cc.nws_tomorrow, cc.wk_attribution);
    renderSummary(cc.daily_summary);
    renderClimatePanel(cc);
    renderStationRecords(cc.station_records);
    renderNearby(cc.nearby_stations, null);
  }
  if (ch) {
    lastHistory = ch;
    renderChart(ch, currentField, currentHours);
    if (cc) renderTodayContext(cc, ch);
  }

  // Phase 1 — fetch current + history in parallel (not sequential)
  const [rCur, rHist] = await Promise.allSettled([
    fetchCurrent(),
    fetchHistory(currentHours),
  ]);

  if (rCur.status === 'fulfilled') {
    const cur = rCur.value;
    cacheSet('current', cur);
    _bootCurrent = cur;
    renderCurrent(cur);
    renderTomorrow(cur.nws_tomorrow, cur.wk_attribution);
    renderSummary(cur.daily_summary);
    renderClimatePanel(cur);
    renderStationRecords(cur.station_records);
    renderNearby(cur.nearby_stations, null);
  } else {
    console.error('Current fetch failed:', rCur.reason);
  }

  if (rHist.status === 'fulfilled') {
    const hist = rHist.value;
    cacheSet('history_' + currentHours, hist);
    lastHistory = hist;
    renderChart(hist, currentField, currentHours);
    if (_bootCurrent) renderTodayContext(_bootCurrent, lastHistory);
  } else {
    console.error('History fetch failed:', rHist.reason);
  }

  // Phase 2 — secondary data (rain events + calendar), also cached
  loadSecondaryData();
}
boot();
setInterval(refresh, REFRESH_MS);

// Scroll reveal
(function initScrollReveal() {
  if (!('IntersectionObserver' in window)) return;
  const obs = new IntersectionObserver((entries) => {
    entries.forEach(e => { if (e.isIntersecting) { e.target.classList.add('visible'); obs.unobserve(e.target); } });
  }, { threshold: 0.1 });
  document.querySelectorAll('.sr').forEach(el => obs.observe(el));
})();
