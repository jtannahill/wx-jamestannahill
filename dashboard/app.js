const API_BASE = 'https://d2d2b3fftwrbn2.cloudfront.net';
const REFRESH_MS = 5 * 60 * 1000;

let chart = null;
let currentField = 'tempf';

const FIELD_LABELS = {
  tempf: { label: 'Temperature', unit: '°F', decimals: 1 },
  humidity: { label: 'Humidity', unit: '%', decimals: 0 },
  windspeedmph: { label: 'Wind', unit: ' mph', decimals: 1 },
  baromrelin: { label: 'Pressure', unit: '"', decimals: 2 },
};

const DIR_LABELS = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW'];
function degToCompass(deg) {
  return DIR_LABELS[Math.round(deg / 22.5) % 16];
}

function fmt(val, decimals = 1) {
  if (val == null) return '—';
  return Number(val).toFixed(decimals);
}

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

function renderCurrent(data) {
  document.getElementById('temp').textContent = fmt(data.tempf, 0);
  document.getElementById('feels-like').textContent = `Feels like ${fmt(data.feelsLike, 0)}°F`;
  document.getElementById('condition').textContent = data.condition || '—';

  const topAnomaly = data.anomalies?.temp;
  document.getElementById('anomaly-headline').textContent =
    topAnomaly ? topAnomaly.label : '';

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

  const updated = data.updated_at ? new Date(data.updated_at).toLocaleTimeString() : '—';
  document.getElementById('updated-at').textContent = `Updated ${updated}`;
}

function renderChart(history, field) {
  const ctx = document.getElementById('wx-chart').getContext('2d');
  const cfg = FIELD_LABELS[field];
  const labels = history.readings.map(r => {
    const d = new Date(r.timestamp);
    return `${d.getHours()}:${String(d.getMinutes()).padStart(2,'0')}`;
  });
  const values = history.readings.map(r => r[field]);

  if (chart) chart.destroy();
  chart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        data: values,
        borderColor: '#c8b97a',
        backgroundColor: 'rgba(200,185,122,0.05)',
        borderWidth: 1.5,
        pointRadius: 0,
        tension: 0.3,
        fill: true,
      }]
    },
    options: {
      responsive: true,
      plugins: { legend: { display: false } },
      scales: {
        x: {
          ticks: { color: '#666', font: { size: 11 }, maxTicksLimit: 8 },
          grid: { color: '#1a1a1a' },
        },
        y: {
          ticks: { color: '#666', font: { size: 11 }, callback: v => `${v}${cfg.unit}` },
          grid: { color: '#1a1a1a' },
        }
      }
    }
  });
}

async function refresh() {
  try {
    const [current, history] = await Promise.all([fetchCurrent(), fetchHistory(24)]);
    renderCurrent(current);
    renderChart(history, currentField);
  } catch (e) {
    console.error('Refresh failed:', e);
  }
}

document.querySelectorAll('.chart-btn').forEach(btn => {
  btn.addEventListener('click', async () => {
    document.querySelectorAll('.chart-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    currentField = btn.dataset.field;
    const history = await fetchHistory(24);
    renderChart(history, currentField);
  });
});

refresh();
setInterval(refresh, REFRESH_MS);
