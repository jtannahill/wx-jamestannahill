const API_BASE = 'https://api.wx.jamestannahill.com';
const REFRESH_MS = 5 * 60 * 1000;

// currentHours kept so refresh() and the cache key stay aligned with the
// chart island's default range (24h). currentField is no longer used here
// (the island owns chart state); it stays only because card-click handlers
// set it before dispatching the wx:fieldChange event.
let currentField = 'tempf';
let currentHours = 24;
let lastHistory = null;
let _lastSummaries = null;

let useCelsius = localStorage.getItem('wx_celsius') === '1';
function toC(f)        { return f == null ? null : (f - 32) * 5 / 9; }
function toDC(f)       { return f == null ? null : f / 1.8; }
function tempUnit()    { return useCelsius ? '°C' : '°F'; }
function fmtT(f, dec = 0) { return fmt(useCelsius ? toC(f) : f, dec); }
function fmtD(f, dec = 1) { return fmt(useCelsius ? toDC(f) : f, dec); }

// Generic °F → °C swap for absolute-temperature prose (WeatherKit forecast,
// daily summary, today snippets). Rounds to the same decimal precision as
// the source, so "70°F" → "21°C" and "70.5°F" → "21.4°C".
function localizeFTemps(text) {
  if (!text || !useCelsius) return text || '';
  return text.replace(/(-?\d+(?:\.\d+)?)\s*°F/g, (_, n) => {
    const c = (parseFloat(n) - 32) * 5 / 9;
    const dec = n.includes('.') ? 1 : 0;
    return `${c.toFixed(dec)}°C`;
  });
}

// Same idea but for differences ("8.2°F warmer than airports") — divide by 1.8.
function localizeDeltaFTemps(text) {
  if (!text || !useCelsius) return text || '';
  return text.replace(/(-?\d+(?:\.\d+)?)\s*°F/g, (_, n) => {
    const c = parseFloat(n) / 1.8;
    const dec = n.includes('.') ? 1 : 0;
    return `${c.toFixed(dec)}°C`;
  });
}

// Server gives us anomaly labels like "16.3°F above average for 7pm in May"
// with the °F baked in. Recompose in °C when the toggle is on.
function localizeTempAnomalyLabel(temp) {
  if (!temp || !temp.label) return '';
  if (!useCelsius) return temp.label;
  const delta = temp.delta;
  if (delta != null && Math.abs(delta) >= 0.5) {
    const c = (Math.abs(delta) / 1.8).toFixed(1);
    return temp.label.replace(/^\d+(?:\.\d+)?°F/, `${c}°C`);
  }
  // "near average ..." or unknown → fallback to a generic °F → °C swap
  return temp.label.replace(/(-?\d+(?:\.\d+)?)°F/g, (_, n) => {
    const c = (parseFloat(n) / 1.8).toFixed(1);
    return `${c}°C`;
  });
}

const DIR_LABELS = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW'];
function degToCompass(deg) {
  return DIR_LABELS[Math.round(deg / 22.5) % 16];
}

function fmt(val, decimals = 1) {
  if (val == null) return '—';
  return Number(val).toFixed(decimals);
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
  document.getElementById('temp').textContent = fmtT(data.tempf, 0);
  document.getElementById('temp-unit').textContent = tempUnit();
  document.getElementById('feels-like').textContent = `Feels like ${fmtT(data.feelsLike, 0)}${tempUnit()}`;
  document.getElementById('condition').textContent = data.condition || '—';

  // Percentile rank = current temp vs this station's distribution for the
  // month. Prefix it so it can't be misread against the "Warmest June 9th"
  // daily-high percentile above it.
  const pr = data.percentile_rank;
  const monthName = new Date().toLocaleString('en-US', { month: 'long' });
  document.getElementById('percentile-rank').textContent =
    pr ? `Temp vs typical ${monthName}: ${pr.label}` : '';

  const topAnomaly = data.anomalies?.temp;
  const anomalyEl = document.getElementById('anomaly-headline');
  anomalyEl.textContent = localizeTempAnomalyLabel(topAnomaly);
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
  document.getElementById('dewpoint').textContent = `Dew point ${fmtT(data.dewPoint, 0)}${tempUnit()}`;

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
    uhi != null ? `${uhi >= 0 ? '+' : ''}${fmtD(uhi, 1)}${tempUnit()}` : '—';
  document.getElementById('uhi-label').textContent = localizeDeltaFTemps(data.uhi_label) || 'vs JFK / LGA / EWR';

  // Seasonal UHI average for current month
  const uhiMonthlyEl = document.getElementById('uhi-monthly-avg');
  const seasonal = data.uhi_seasonal_curve;
  if (seasonal && seasonal.length > 0) {
    const curMonth = new Date().getMonth() + 1;
    const entry = seasonal.find(m => m.month === curMonth);
    if (entry && entry.avg_delta != null && entry.sample_count >= 10) {
      const avg = entry.avg_delta;
      uhiMonthlyEl.textContent =
        `Typical ${entry.month_name}: ${avg >= 0 ? '+' : ''}${fmtD(avg, 1)}${tempUnit()}`;
    } else {
      uhiMonthlyEl.textContent = '';
    }
  } else {
    uhiMonthlyEl.textContent = '';
  }

  const updated = data.updated_at ? new Date(data.updated_at).toLocaleTimeString() : '—';
  document.getElementById('updated-at').textContent = `Updated ${updated}`;

  renderForecast(data.forecast, data.tempf);

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
function renderForecast(forecast, nowTempF) {
  const section = document.getElementById('forecast-section');
  if (!forecast || !forecast.hours?.length) { section.hidden = true; return; }

  section.hidden = false;

  // Delta line vs current temp ("↓ 2°F from now") so near-identical absolute
  // temps across the three cards still read as a trajectory.
  const deltaLine = (h) => {
    if (nowTempF == null || h.tempf == null) return '';
    const dF = h.tempf - nowTempF;
    const d  = useCelsius ? dF / 1.8 : dF;
    if (Math.abs(d) < 0.5) return `<div class="forecast-delta">→ steady vs now</div>`;
    const arrow = d > 0 ? '↑' : '↓';
    return `<div class="forecast-delta">${arrow} ${Math.abs(d).toFixed(0)}${tempUnit()} from now</div>`;
  };

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
    metaParts.push(`±${fmtD(forecast.accuracy.mae_1h_tempf, 1)}${tempUnit()} avg error (+1h, n=${n})`);
  }
  document.getElementById('forecast-meta').textContent = metaParts.filter(Boolean).join(' · ');

  const grid = document.getElementById('forecast-grid');
  grid.innerHTML = forecast.hours.map(h => {
    const label = h.offset_hours === 1 ? '+1 hour'
                : h.offset_hours === 2 ? '+2 hours' : '+3 hours';
    return `
      <div class="forecast-card">
        <div class="forecast-offset">${label}</div>
        <div>
          <div class="forecast-temp">${fmtT(h.tempf, 0)}${tempUnit()}</div>
          ${deltaLine(h)}
        </div>
        <div class="forecast-fields">
          <div class="forecast-field">Humidity ${fmt(h.humidity, 0)}%</div>
          <div class="forecast-field">Wind ${fmt(h.windspeedmph, 0)} mph</div>
          <div class="forecast-field">Pressure ${fmt(h.baromrelin, 2)}"</div>
        </div>
      </div>`;
  }).join('');
  grid.removeAttribute('aria-busy');
}

// ── Tomorrow forecast (WeatherKit) ───────────────────────────────────────────
function renderTomorrow(nws, attr) {
  const section = document.getElementById('tomorrow-section');
  if (!nws || !nws.detailed) { section.hidden = true; return; }
  document.getElementById('tomorrow-date').textContent = nws.name.toUpperCase();
  document.getElementById('tomorrow-text').textContent = localizeFTemps(nws.detailed);
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
  const high  = temps.length ? Math.round(useCelsius ? toC(Math.max(...temps)) : Math.max(...temps)) : null;
  const low   = temps.length ? Math.round(useCelsius ? toC(Math.min(...temps)) : Math.min(...temps)) : null;
  const maxGust  = gusts.length  ? Math.round(Math.max(...gusts))  : null;
  const rain     = current.dailyrainin ?? 0;

  const sentences = [];

  // Temp range
  if (high != null && low != null) {
    sentences.push(high - low < 3
      ? `Temperatures holding near ${Math.round((high + low) / 2)}${tempUnit()}.`
      : `Temperatures ranging from ${low}${tempUnit()} to ${high}${tempUnit()} so far.`);
  }

  // Wind gusts
  if (maxGust != null && maxGust >= 15) sentences.push(`Wind gusts to ${maxGust} mph.`);

  // Rain
  if (rain > 0.01) sentences.push(`${rain.toFixed(2)}" of rain since midnight.`);

  // Anomaly — only if notable (≥5°F delta)
  const anomaly = current.anomalies?.temp;
  if (anomaly && Math.abs(anomaly.delta) >= 5) {
    const localized = localizeTempAnomalyLabel(anomaly);
    if (localized) sentences.push(localized[0].toUpperCase() + localized.slice(1) + '.');
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
      { key: 'temp_high', label: 'High Temp', color: COLORS.temp },
      { key: 'temp_low',  label: 'Low Temp',  color: '#4ab8e8' },
    ];
    rows.forEach(({ key, label, color }) => {
      const m = verdict[key];
      if (!m) return;
      const pct   = m.percentile;
      const since = m.last_exceeded_year ? `since ${m.last_exceeded_year}` : 'on record';
      const dispVal = fmtT(m.value, 0);
      const dispP50 = fmtT(m.p50, 0);

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
        barHtml = `
          <div class="dev-bar">
            <div class="dev-bar-track">
              <div class="dev-bar-fill" style="left:${fillLeft}%;width:${fillWidth}%;background:${color}"></div>
              <div class="dev-bar-avg-tick" style="left:${avgPct}%">
                <span class="dev-bar-avg-label">avg ${dispP50}°</span>
              </div>
              <div class="dev-bar-today-dot" style="left:${todayPct}%;background:${color}"></div>
            </div>
          </div>`;
      }

      container.innerHTML += `
        <div class="climate-metric">
          <div class="climate-metric-row">
            <span class="climate-metric-label">${label}</span>
            <span class="climate-metric-value" style="color:${color}">${dispVal}${tempUnit()}
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
            <span class="climate-metric-label">Dew Point <a class="era5-chip has-tooltip" href="/docs.html#era5" data-tooltip="ERA5: ECMWF climate reanalysis baseline. Hourly climate history for this exact lat/lon back to 1940. Click for docs.">(ERA5)</a></span>
            <span class="climate-metric-value" style="color:#4ab8e8">${fmtT(dp.value, 0)}${tempUnit()}
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
      { key: 'temp',     label: 'Temperature', isTemp: true,  color: COLORS.temp },
      { key: 'dewpoint', label: 'Dew Point',   isTemp: true,  color: COLORS.dewpoint },
      { key: 'wind',     label: 'Wind Speed',  isTemp: false, color: COLORS.wind },
    ];
    order.forEach(({ key, label, isTemp, color }) => {
      const m = metrics[key];
      if (!m) return;
      const pct  = m.percentile;
      const unit = isTemp ? tempUnit() : ' mph';
      const dispVal = isTemp ? fmtT(m.value, 0) : m.value;
      const dispP25 = isTemp ? fmtT(m.p25, 0) : (m.p25 ?? '—');
      const dispP50 = isTemp ? fmtT(m.p50, 0) : (m.p50 ?? '—');
      const dispP75 = isTemp ? fmtT(m.p75, 0) : (m.p75 ?? '—');
      container.innerHTML += `
        <div class="climate-metric">
          <div class="climate-metric-row">
            <span class="climate-metric-label">${label}</span>
            <span class="climate-metric-value" style="color:${color}">${dispVal}${unit}
              <span class="climate-metric-pct">${pct}th pct</span>
            </span>
          </div>
          <div class="climate-bar-track">
            <div class="climate-bar-fill" style="width:${pct}%;background:linear-gradient(90deg,#222,${color})"></div>
            <div class="climate-bar-marker" style="left:${pct}%;background:${color}"></div>
          </div>
          <div class="climate-bar-ticks">
            <span>p25: ${dispP25}</span>
            <span>p50: ${dispP50}</span>
            <span>p75: ${dispP75}</span>
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

  // Bind tooltips on dynamically created chips (container is rebuilt each
  // render, so no duplicate listeners accumulate)
  container.querySelectorAll('.has-tooltip').forEach(bindTip);
}

// ── Summary ───────────────────────────────────────────────────────────────────
function renderSummary(summary) {
  const section = document.getElementById('summary-section');
  if (!summary || !summary.summary) { section.hidden = true; return; }
  section.hidden = false;
  const d = new Date(summary.date + 'T12:00:00');
  const opts = { weekday: 'long', month: 'long', day: 'numeric' };
  document.getElementById('summary-date').textContent = d.toLocaleDateString('en-US', opts);
  document.getElementById('summary-text').textContent = localizeFTemps(summary.summary);
}

// ── Comfort Calendar ──────────────────────────────────────────────────────────
function renderComfortCalendar(summaries) {
  const section = document.getElementById('comfort-calendar-section');
  if (!summaries || !summaries.length) { section.hidden = true; return; }
  section.hidden = false;

  _lastSummaries = summaries;
  const sorted = [...summaries].sort((a, b) => a.date.localeCompare(b.date));
  const grid = document.getElementById('comfort-grid');
  grid.innerHTML = sorted.map(s => {
    const score = s.avg_comfort ?? 0;
    const hue   = Math.round(score * 1.2);  // 0=red(0°), 100=green(120°)
    const color = `hsl(${hue}, 55%, 30%)`;
    const d     = new Date(s.date + 'T12:00:00');
    const label = `${d.getMonth() + 1}/${d.getDate()}`;
    const rain  = s.total_rain > 0.01 ? ` · ${Number(s.total_rain).toFixed(2)}"` : '';
    const tip   = `${s.date}: ${score}/100 comfort · ${fmtT(s.temp_high,0)}°–${fmtT(s.temp_low,0)}${tempUnit()}${rain}`;
    return `<div class="comfort-cell has-tooltip" style="background:${color}" data-tooltip="${tip}">
      <span class="comfort-cell-label">${label}</span>
    </div>`;
  }).join('');

  grid.removeAttribute('aria-busy');

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
  if (records.temp_high    != null) items.push({ label: 'HIGH TEMP',     value: `${fmtT(records.temp_high, 0)}${tempUnit()}`,     date: records.temp_high_at });
  if (records.temp_low     != null) items.push({ label: 'LOW TEMP',      value: `${fmtT(records.temp_low, 0)}${tempUnit()}`,      date: records.temp_low_at });
  if (records.max_gust     != null) items.push({ label: 'MAX GUST',      value: `${records.max_gust} mph`,    date: records.max_gust_at });
  if (records.max_rain_rate!= null) items.push({ label: 'PEAK RAIN',     value: `${records.max_rain_rate}"/hr`, date: records.max_rain_rate_at });
  if (records.min_pressure != null) items.push({ label: 'MIN PRESSURE',  value: `${records.min_pressure}"`,   date: records.min_pressure_at });
  if (records.max_pressure != null) items.push({ label: 'MAX PRESSURE',  value: `${records.max_pressure}"`,   date: records.max_pressure_at });

  const recordsGrid = document.getElementById('records-grid');
  recordsGrid.innerHTML = items.map(it => `
    <div class="record-card">
      <div class="record-label">${it.label}</div>
      <div class="record-value">${it.value}</div>
      <div class="record-date">${it.date || ''}</div>
    </div>`).join('');
  recordsGrid.removeAttribute('aria-busy');
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
    const temp = s.temp_f != null ? `${fmtT(s.temp_f, 0)}${tempUnit()}` : '–';
    const rain = s.rain_rate_in_hr > 0 ? `${s.rain_rate_in_hr.toFixed(2)}" /hr` : '';
    const dist = s.distance_mi != null ? `${s.distance_mi.toFixed(1)} mi` : '';
    return `<div class="nearby-chip">
      <div class="nearby-chip-id">${s.station_id ?? ''}</div>
      <div class="nearby-chip-temp">${temp}</div>
      ${rain ? `<div class="nearby-chip-rain">${rain}</div>` : ''}
      <div class="nearby-chip-dist">${dist}</div>
    </div>`;
  }).join('');
  strip.removeAttribute('aria-busy');
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
    renderTodayContext(current, lastHistory);
  } catch (e) {
    console.error('Refresh failed:', e);
  }
}

// ── Controls ──────────────────────────────────────────────────────────────────
// Chart field/range controls + chart engine moved into the Preact island
// (src/components/Chart.tsx). app.js dispatches wx:unitChange and
// wx:fieldChange events that the island subscribes to.

const unitToggleBtn = document.getElementById('unit-toggle');
unitToggleBtn.textContent = useCelsius ? '°C' : '°F';
if (useCelsius) unitToggleBtn.classList.add('celsius');
unitToggleBtn.addEventListener('click', () => {
  useCelsius = !useCelsius;
  localStorage.setItem('wx_celsius', useCelsius ? '1' : '0');
  unitToggleBtn.textContent = useCelsius ? '°C' : '°F';
  unitToggleBtn.classList.toggle('celsius', useCelsius);
  document.getElementById('temp-unit').textContent = tempUnit();
  if (_bootCurrent) {
    renderCurrent(_bootCurrent);
    renderClimatePanel(_bootCurrent);
    renderStationRecords(_bootCurrent.station_records);
    renderNearby(_bootCurrent.nearby_stations, null);
    renderTodayContext(_bootCurrent, lastHistory);
  }
  if (_lastSummaries) renderComfortCalendar(_lastSummaries);
  document.dispatchEvent(new CustomEvent('wx:unitChange', { detail: { useCelsius } }));
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

// ── Chart pre-boot click capture ─────────────────────────────────────────────
// The chart island (client:visible) may not have hydrated yet when a range or
// field button is clicked. Record the request on window so the island can use
// it as its initial state instead of clobbering back to the defaults.
// Capture phase so this works whether or not Preact's handlers exist yet.
document.addEventListener('click', (e) => {
  const rangeBtn = e.target.closest?.('.range-btn[data-hours]');
  if (rangeBtn) {
    window.__wxPendingHours = Number(rangeBtn.dataset.hours);
    // Pre-hydration visual feedback (Preact re-syncs classes after boot)
    rangeBtn.parentElement.querySelectorAll('.range-btn').forEach(b => b.classList.toggle('active', b === rangeBtn));
  }
  const fieldBtn = e.target.closest?.('.chart-btn[data-field]');
  if (fieldBtn) {
    window.__wxPendingField = fieldBtn.dataset.field;
    fieldBtn.parentElement.querySelectorAll('.chart-btn').forEach(b => b.classList.toggle('active', b === fieldBtn));
  }
}, true);

// ── KPI card click-ins ────────────────────────────────────────────────────────
document.querySelectorAll('.card[data-chart-field]').forEach(card => {
  card.addEventListener('click', () => {
    const field = card.dataset.chartField;
    currentField = field;
    window.__wxPendingField = field;
    document.dispatchEvent(new CustomEvent('wx:fieldChange', { detail: { field } }));
    const section = document.querySelector('.chart-section');
    if (section) section.scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
});

document.querySelectorAll('.card[data-scroll-to]').forEach(card => {
  card.addEventListener('click', () => {
    const el = document.getElementById(card.dataset.scrollTo);
    if (el && !el.hidden) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
});

// ── Share to X ───────────────────────────────────────────────────────────────
function buildShareText(data) {
  const temp   = fmtT(data.tempf, 0);
  const feels  = fmtT(data.feelsLike, 0);
  const cond   = data.condition || '';
  const anomaly = data.anomalies?.temp?.label || '';
  const hum    = fmt(data.humidity, 0);
  const wind   = `${fmt(data.windspeedmph, 0)} mph ${degToCompass(data.winddir)}`;
  const comfort = data.comfort;
  const rp     = data.rain_probability;

  let line1 = `Midtown Manhattan: ${temp}${tempUnit()}`;
  if (cond) line1 += ` · ${cond}`;
  if (feels !== temp) line1 += ` · Feels ${feels}${tempUnit()}`;
  if (anomaly) line1 += ` · ${anomaly}`;

  let line2 = `${hum}% humidity · Wind ${wind}`;
  if (comfort?.score) line2 += ` · Comfort ${comfort.score} (${comfort.label})`;
  if (rp?.probability >= 30) line2 += ` · ${rp.probability}% rain next hr`;

  // Cache-buster on the URL forces Twitter to do a fresh og.png scrape per tweet
  const v = Math.floor(Date.now() / 1000);
  return `${line1}\n\n${line2}\n\nhttps://wx.jamestannahill.com/?v=${v} #NYC #weather`;
}

document.getElementById('share-btn').addEventListener('click', async () => {
  if (!_bootCurrent) return;
  const text = buildShareText(_bootCurrent);
  const tweetUrl = `https://twitter.com/intent/tweet?text=${encodeURIComponent(text)}`;
  // Stamp a fresh ?v= on index.html's og:image before opening Twitter so the
  // card fetcher sees a URL it hasn't cached and pulls the latest og.png.
  try { await fetch(`${API_BASE}/refresh-og`); } catch (_) { /* non-fatal */ }
  window.open(tweetUrl, '_blank', 'noopener,noreferrer,width=600,height=450');
});

// Chart toolbar — reset / copy / share
function flashBtn(el, label, ms = 1100) {
  if (!el) return;
  const orig = el.textContent;
  el.textContent = label;
  el.classList.add('flash');
  setTimeout(() => { el.textContent = orig; el.classList.remove('flash'); }, ms);
}
document.getElementById('chart-zoom-reset')?.addEventListener('click', () => resetChartZoom());
document.getElementById('chart-copy-btn')?.addEventListener('click', async (e) => {
  const ok = await copyChartImage();
  flashBtn(e.currentTarget, ok ? '✓' : '↗');
});
document.getElementById('chart-share-btn')?.addEventListener('click', async (e) => {
  await shareChartImage();
  flashBtn(e.currentTarget, '✓');
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
    if (cc) renderTodayContext(cc, ch);
  }

  // Phase 1 — fetch current + history in parallel (chart island handles its
  // own chart render; we still need history for renderTodayContext).
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
