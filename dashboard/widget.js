(function() {
  const API = 'https://d2d2b3fftwrbn2.cloudfront.net/current';
  const TARGET_ID = 'wx-badge';

  function render(data) {
    const el = document.getElementById(TARGET_ID);
    if (!el) return;

    const temp = Math.round(data.tempf || 0);
    const wind = Math.round(data.windspeedmph || 0);
    const condition = data.condition || '';
    const anomaly = data.anomalies?.temp?.label || '';

    el.innerHTML = `
      <span style="font-family:inherit;font-size:inherit;color:inherit;white-space:nowrap;">
        <strong>${temp}°F</strong>&nbsp;&nbsp;
        ${wind} mph&nbsp;&nbsp;
        ${condition}&nbsp;&nbsp;
        <em style="opacity:0.7;font-style:italic;">${anomaly}</em>&nbsp;&nbsp;
        <span style="opacity:0.4;font-size:0.85em;">Midtown Manhattan</span>
      </span>
    `;
  }

  fetch(API)
    .then(r => r.json())
    .then(render)
    .catch(() => {
      const el = document.getElementById(TARGET_ID);
      if (el) el.innerHTML = '';
    });
})();
