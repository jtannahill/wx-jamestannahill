def build_email(label: str, detail: str, reading: dict, now) -> str:
    def fmt(v, d=1):
        if v is None:
            return '—'
        return f"{v:.{d}f}"

    temp    = reading.get('tempf')
    feels   = reading.get('feelsLike')
    humidity = reading.get('humidity')
    wind    = reading.get('windspeedmph')
    gust    = reading.get('windgustmph')
    pressure = reading.get('baromrelin')
    rain    = reading.get('hourlyrainin')
    time_str = now.strftime('%B %-d, %Y %-I:%M %p ET')

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0a0a0a;font-family:-apple-system,BlinkMacSystemFont,'Helvetica Neue',sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0a0a0a;">
    <tr><td align="center" style="padding:40px 16px;">
      <table width="520" cellpadding="0" cellspacing="0" style="max-width:520px;width:100%;background:#0a0a0a;border:1px solid #222;">

        <tr><td style="padding:28px 28px 0;">
          <p style="margin:0;font-size:10px;letter-spacing:0.15em;color:#555;text-transform:uppercase;">Midtown Manhattan, New York</p>
          <p style="margin:4px 0 0;font-size:11px;color:#444;">{time_str}</p>
        </td></tr>

        <tr><td style="padding:20px 28px 0;">
          <h1 style="margin:0;font-size:26px;font-weight:400;color:#c8b97a;letter-spacing:-0.01em;">{label}</h1>
          <p style="margin:8px 0 0;font-size:17px;color:#e0e0e0;font-style:italic;">{detail}</p>
        </td></tr>

        <tr><td style="padding:28px 28px 0;">
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr>
              <td style="width:50%;vertical-align:top;padding-bottom:24px;">
                <p style="margin:0;font-size:9px;letter-spacing:0.15em;color:#555;text-transform:uppercase;">Temperature</p>
                <p style="margin:5px 0 0;font-size:28px;font-weight:300;color:#f0f0f0;letter-spacing:-0.02em;">{fmt(temp, 0)}°F</p>
                <p style="margin:3px 0 0;font-size:12px;color:#666;">Feels like {fmt(feels, 0)}°F</p>
              </td>
              <td style="width:50%;vertical-align:top;padding-bottom:24px;">
                <p style="margin:0;font-size:9px;letter-spacing:0.15em;color:#555;text-transform:uppercase;">Wind</p>
                <p style="margin:5px 0 0;font-size:28px;font-weight:300;color:#f0f0f0;letter-spacing:-0.02em;">{fmt(wind, 0)} mph</p>
                <p style="margin:3px 0 0;font-size:12px;color:#666;">Gust {fmt(gust, 0)} mph</p>
              </td>
            </tr>
            <tr>
              <td style="vertical-align:top;padding-bottom:8px;">
                <p style="margin:0;font-size:9px;letter-spacing:0.15em;color:#555;text-transform:uppercase;">Humidity</p>
                <p style="margin:5px 0 0;font-size:28px;font-weight:300;color:#f0f0f0;letter-spacing:-0.02em;">{fmt(humidity, 0)}%</p>
              </td>
              <td style="vertical-align:top;padding-bottom:8px;">
                <p style="margin:0;font-size:9px;letter-spacing:0.15em;color:#555;text-transform:uppercase;">Pressure</p>
                <p style="margin:5px 0 0;font-size:28px;font-weight:300;color:#f0f0f0;letter-spacing:-0.02em;">{fmt(pressure, 2)}"</p>
                <p style="margin:3px 0 0;font-size:12px;color:#666;">Rain {fmt(rain, 2)}"</p>
              </td>
            </tr>
          </table>
        </td></tr>

        <tr><td style="padding:20px 28px 28px;border-top:1px solid #1a1a1a;">
          <p style="margin:0;font-size:11px;color:#444;">
            <a href="https://wx.jamestannahill.com" style="color:#666;text-decoration:none;">wx.jamestannahill.com</a>
            &nbsp;·&nbsp;Private station, Midtown Manhattan
          </p>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""
