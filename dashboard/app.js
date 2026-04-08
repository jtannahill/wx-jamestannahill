const API_BASE = 'https://api.wx.jamestannahill.com';
const REFRESH_MS = 5 * 60 * 1000;

let chart = null;
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

// ── Custom plugin: vertical "now" cursor ──────────────────────────────────────
const nowCursorPlugin = {
  id: 'nowCursor',
  afterDraw(chart) {
    const idx = chart._nowIndex;
    if (idx == null) return;
    const meta = chart.getDatasetMeta(0);
    if (!meta.data[idx]) return;
    const x = meta.data[idx].x;
    const { top, bottom } = chart.chartArea;
    const ctx = chart.ctx;
    ctx.save();
    ctx.strokeStyle = 'rgba(255,255,255,0.20)';
    ctx.lineWidth = 1;
    ctx.setLineDash([4, 5]);
    ctx.beginPath();
    ctx.moveTo(x, top);
    ctx.lineTo(x, bottom);
    ctx.stroke();
    ctx.restore();
  }
};
Chart.register(nowCursorPlugin);

// ── Helpers ───────────────────────────────────────────────────────────────────
function fmtChartLabel(ts, hours) {
  const d = new Date(ts);
  if (hours > 168) return `${d.getMonth()+1}/${d.getDate()}`;
  if (hours > 24)  return `${d.getMonth()+1}/${d.getDate()} ${d.getHours()}:00`;
  const h = d.getHours(), ampm = h >= 12 ? 'pm' : 'am';
  return `${h === 0 ? 12 : h > 12 ? h - 12 : h}${ampm}`;
}

function findNowIndex(readings) {
  const now = Date.now();
  let best = 0, bestDiff = Infinity;
  readings.forEach((r, i) => {
    const diff = Math.abs(new Date(r.timestamp).getTime() - now);
    if (diff < bestDiff) { bestDiff = diff; best = i; }
  });
  return best;
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

// ── Chart ─────────────────────────────────────────────────────────────────────
function renderChart(history, field, hours) {
  const ctx = document.getElementById('wx-chart').getContext('2d');
  const cfg = FIELD_LABELS[field] || { label: field, unit: '', decimals: 1 };
  const readings = history.readings;

  const labels     = readings.map(r => fmtChartLabel(r.timestamp, hours));
  const values     = readings.map(r => r[field] ?? null);
  const baselines  = readings.map(r => r[`baseline_${field}`] ?? null);
  const upperSigma = readings.map(r => {
    const b = r[`baseline_${field}`], s = r[`baseline_std_${field}`];
    return (b != null && s != null) ? b + s : null;
  });
  const lowerSigma = readings.map(r => {
    const b = r[`baseline_${field}`], s = r[`baseline_std_${field}`];
    return (b != null && s != null) ? b - s : null;
  });
  const rainValues = readings.map(r => r.hourlyrainin ?? null);
  const maxRain    = Math.max(...rainValues.filter(v => v != null), 0.01);
  const nowIndex   = hours <= 168 ? findNowIndex(readings) : null;
  const yCallback  = v => `${Number(v).toFixed(cfg.decimals)}${cfg.unit}`;

  // Build crosshair tooltip labels from raw readings
  function buildTooltipLines(dataIndex) {
    const r = readings[dataIndex];
    if (!r) return [];
    const lines = [];
    if (r.tempf        != null) lines.push(`Temp  ${Number(r.tempf).toFixed(1)}°F`);
    if (r.humidity     != null) lines.push(`RH    ${Number(r.humidity).toFixed(0)}%`);
    if (r.windspeedmph != null) lines.push(`Wind  ${Number(r.windspeedmph).toFixed(1)} mph`);
    if (r.baromrelin   != null) lines.push(`Pres  ${Number(r.baromrelin).toFixed(2)}"`);
    if (r.uhi_delta    != null) lines.push(`UHI   ${r.uhi_delta >= 0 ? '+' : ''}${Number(r.uhi_delta).toFixed(1)}°F`);
    if (r.hourlyrainin  > 0.005) lines.push(`Rain  ${Number(r.hourlyrainin).toFixed(2)}"/hr`);
    return lines;
  }

  if (chart) {
    // Fast in-place update — no destroy/recreate
    // Datasets: 0=main, 1=upperSigma, 2=lowerSigma, 3=baseline, 4=rain
    chart.data.labels = labels;
    chart.data.datasets[0].data = values;
    chart.data.datasets[1].data = upperSigma;
    chart.data.datasets[2].data = lowerSigma;
    chart.data.datasets[3].data = baselines;
    chart.data.datasets[4].data = rainValues;
    chart.options.scales.y.ticks.callback = yCallback;
    chart.options.scales.yRain.max = maxRain * 14;
    chart._nowIndex = nowIndex;
    chart._buildTooltipLines = buildTooltipLines;
    chart._hours = hours;
    chart.update('none');
    return;
  }

  chart = new Chart(ctx, {
    data: {
      labels,
      datasets: [
        {
          // 0 — main field line with anomaly fill vs baseline
          type: 'line',
          data: values,
          borderColor: '#c8b97a',
          borderWidth: 1.5,
          pointRadius: 0,
          tension: 0.3,
          spanGaps: true,
          order: 1,
          fill: {
            target: 3,  // fill vs baseline (dataset 3)
            above: 'rgba(200,185,122,0.10)',
            below: 'rgba(100,140,220,0.08)',
          },
        },
        {
          // 1 — upper ±1σ band edge
          type: 'line',
          data: upperSigma,
          borderColor: 'transparent',
          borderWidth: 0,
          pointRadius: 0,
          tension: 0.3,
          spanGaps: true,
          order: 4,
          fill: '+1',  // fill to dataset 2 (lower sigma)
          backgroundColor: 'rgba(120,130,160,0.07)',
        },
        {
          // 2 — lower ±1σ band edge
          type: 'line',
          data: lowerSigma,
          borderColor: 'transparent',
          borderWidth: 0,
          pointRadius: 0,
          tension: 0.3,
          spanGaps: true,
          order: 5,
          fill: false,
        },
        {
          // 3 — baseline dotted line
          type: 'line',
          data: baselines,
          borderColor: 'rgba(255,255,255,0.18)',
          borderWidth: 1,
          borderDash: [4, 4],
          pointRadius: 0,
          tension: 0.3,
          spanGaps: true,
          fill: false,
          order: 2,
        },
        {
          // 4 — rain bars (secondary axis, tiny at bottom)
          type: 'bar',
          data: rainValues,
          backgroundColor: 'rgba(100,150,220,0.40)',
          borderWidth: 0,
          yAxisID: 'yRain',
          order: 3,
          barPercentage: 0.9,
          categoryPercentage: 1.0,
        },
      ],
    },
    options: {
      animation: false,
      responsive: true,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            title: items => {
              const r = readings[items[0].dataIndex];
              return r ? fmtChartLabel(r.timestamp, chart._hours) : '';
            },
            label: item => {
              if (item.datasetIndex !== 0) return null;
              return chart._buildTooltipLines(item.dataIndex);
            },
          },
        },
        nowCursor: {},
      },
      scales: {
        x: {
          ticks: { color: '#666', font: { size: 11 }, maxTicksLimit: 8 },
          grid: { color: '#1a1a1a' },
        },
        y: {
          ticks: { color: '#666', font: { size: 11 }, callback: yCallback },
          grid: { color: '#1a1a1a' },
        },
        yRain: {
          type: 'linear',
          position: 'right',
          min: 0,
          max: maxRain * 14,
          grid: { display: false },
          ticks: { display: false },
        },
      },
    },
  });

  chart._nowIndex = nowIndex;
  chart._buildTooltipLines = buildTooltipLines;
  chart._hours = hours;
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

  // Re-apply mobile tooltip to new elements
  grid.querySelectorAll('.has-tooltip').forEach(enableMobileTooltip);

  const avg = Math.round(summaries.reduce((s, d) => s + (d.avg_comfort ?? 0), 0) / summaries.length);
  document.getElementById('comfort-meta').textContent = `${summaries.length}-day avg · ${avg}/100`;
}

// ── Station Records ───────────────────────────────────────────────────────────
function renderStationRecords(records) {
  const section = document.getElementById('records-section');
  if (!records) { section.hidden = true; return; }
  section.hidden = false;

  document.getElementById('records-month').textContent = records.month_name || '';

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
function renderRainEvents(events) {
  const section = document.getElementById('rain-events-section');
  if (!events || !events.length) { section.hidden = true; return; }
  section.hidden = false;

  const rows = events.map(ev => {
    const d = new Date(ev.start);
    const mo = d.getMonth() + 1, day = d.getDate();
    const h  = d.getHours(), m = String(d.getMinutes()).padStart(2, '0');
    const ampm = h >= 12 ? 'pm' : 'am';
    const hr12 = h === 0 ? 12 : h > 12 ? h - 12 : h;
    const dateStr = `${mo}/${day} ${hr12}:${m}${ampm}`;
    return `<tr>
      <td>${dateStr}</td>
      <td>${ev.duration_min} min</td>
      <td>${Number(ev.total_in).toFixed(2)}"</td>
      <td>${Number(ev.peak_rate).toFixed(2)}"/hr</td>
    </tr>`;
  }).join('');

  document.getElementById('rain-events-table').innerHTML = `
    <thead><tr><th>DATE / TIME</th><th>DURATION</th><th>TOTAL</th><th>PEAK RATE</th></tr></thead>
    <tbody>${rows}</tbody>`;
}

// ── Secondary data (rain events + comfort calendar) ───────────────────────────
let secondaryLoaded = false;
async function loadSecondaryData() {
  try {
    const [evResp, sumResp] = await Promise.all([
      fetch(`${API_BASE}/rain-events?days=30`),
      fetch(`${API_BASE}/daily-summaries?days=30`),
    ]);
    if (evResp.ok) {
      const d = await evResp.json();
      renderRainEvents(d.events || []);
    }
    if (sumResp.ok) {
      const d = await sumResp.json();
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
    const results = await Promise.all(fetches);
    renderCurrent(results[0]);
    renderSummary(results[0].daily_summary);
    renderStationRecords(results[0].station_records);
    renderNearby(results[0].nearby_stations, null);
    if (results[1]) lastHistory = results[1];
    if (lastHistory) renderChart(lastHistory, currentField, currentHours);
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
    lastHistory = null;
    const history = await fetchHistory(currentHours);
    lastHistory = history;
    renderChart(lastHistory, currentField, currentHours);
  });
});

const refreshBtn = document.getElementById('refresh-btn');
refreshBtn.addEventListener('click', async () => {
  refreshBtn.classList.add('spinning');
  await refresh(true);
  refreshBtn.classList.remove('spinning');
});

// Mobile tooltip: tap to show, tap outside to dismiss
// Works for both .anomaly-headline[data-tooltip] and .has-tooltip elements
function enableMobileTooltip(el) {
  el.addEventListener('touchstart', e => {
    if (!el.dataset.tooltip) return;
    e.preventDefault();
    const wasActive = el.classList.contains('tooltip-active');
    document.querySelectorAll('.tooltip-active').forEach(t => t.classList.remove('tooltip-active'));
    if (!wasActive) {
      el.classList.add('tooltip-active');
      const dismiss = ev => {
        if (!el.contains(ev.target)) {
          el.classList.remove('tooltip-active');
          document.removeEventListener('touchstart', dismiss);
        }
      };
      setTimeout(() => document.addEventListener('touchstart', dismiss), 0);
    }
  }, { passive: false });
}
document.getElementById('anomaly-headline') && enableMobileTooltip(document.getElementById('anomaly-headline'));
document.querySelectorAll('.has-tooltip').forEach(enableMobileTooltip);

// ── Boot ──────────────────────────────────────────────────────────────────────
refresh(true);
loadSecondaryData();
setInterval(refresh, REFRESH_MS);
