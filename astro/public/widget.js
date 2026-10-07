(function() {
  const API = 'https://api.wx.jamestannahill.com/current';
  const TARGET_ID = 'wx-badge';
  // The API updates every 5 minutes; the badge follows it.
  const REFRESH_MS = 5 * 60 * 1000;

  // Same clean-up the dashboard applies to API strings: clause separators
  // become commas and "10am" reads "10 AM".
  const tidy = (s) => String(s)
    .replace(/\s[\u00b7\u2014]\s/g, ', ')
    .replace(/\b(\d{1,2})(?::(\d{2}))?\s?(am|pm)\b/gi, (m, h, mm, a) => `${h}${mm ? ':' + mm : ''} ${a.toUpperCase()}`);

  function render(data) {
    const el = document.getElementById(TARGET_ID);
    if (!el) return;

    if (data.tempf == null) { el.textContent = 'Weather unavailable'; return; }
    const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    const parts = [
      `<strong>${Math.round(data.tempf)}°F</strong>`,
      data.windspeedmph != null ? `${Math.round(data.windspeedmph)} mph` : '',
      data.condition ? esc(tidy(data.condition)) : '',
      data.anomalies?.temp?.label ? `<span style="opacity:0.75;font-style:italic;">${esc(tidy(data.anomalies.temp.label))}</span>` : '',
      // Never smaller than 11px, and full host colour so it stays legible on light pages.
      '<span style="font-size:max(0.85em, 11px);">Midtown Manhattan</span>',
    ].filter(Boolean).map((p) => `<span style="white-space:nowrap;">${p}</span>`);

    el.innerHTML = `<span style="font-family:inherit;font-size:inherit;color:inherit;display:inline-flex;flex-wrap:wrap;gap:0 0.75em;align-items:baseline;font-variant-numeric:tabular-nums;">${parts.join('')}</span>`;
  }

  let rendered = false;
  function load() {
    fetch(API)
      .then(r => { if (!r.ok) throw new Error(String(r.status)); return r.json(); })
      .then(d => { render(d); rendered = true; })
      .catch(() => {
        // A failed refresh keeps the last good reading on screen.
        if (rendered) return;
        const el = document.getElementById(TARGET_ID);
        if (el) el.textContent = 'Weather unavailable';
      });
  }
  load();
  setInterval(load, REFRESH_MS);
})();
