(function() {
  const API = 'https://api.wx.jamestannahill.com/current';
  const TARGET_ID = 'wx-badge';

  function render(data) {
    const el = document.getElementById(TARGET_ID);
    if (!el) return;

    if (data.tempf == null) { el.textContent = 'Weather unavailable'; return; }
    const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    const parts = [
      `<strong>${Math.round(data.tempf)}°F</strong>`,
      data.windspeedmph != null ? `${Math.round(data.windspeedmph)} mph` : '',
      data.condition ? esc(data.condition) : '',
      data.anomalies?.temp?.label ? `<span style="opacity:0.7;font-style:italic;">${esc(data.anomalies.temp.label)}</span>` : '',
      '<span style="opacity:0.7;font-size:0.85em;">Midtown Manhattan</span>',
    ].filter(Boolean).map((p) => `<span style="white-space:nowrap;">${p}</span>`);

    el.innerHTML = `<span style="font-family:inherit;font-size:inherit;color:inherit;display:inline-flex;flex-wrap:wrap;gap:0 0.75em;align-items:baseline;font-variant-numeric:tabular-nums;">${parts.join('')}</span>`;
  }

  fetch(API)
    .then(r => { if (!r.ok) throw new Error(String(r.status)); return r.json(); })
    .then(render)
    .catch(() => {
      const el = document.getElementById(TARGET_ID);
      if (el) el.textContent = 'Weather unavailable';
    });
})();
