"""
wx-alerter: Checks weather readings against thresholds and sends SES alerts.
Triggered by EventBridge every 15 minutes.
Debounces via wx-alerts DynamoDB table to prevent repeat alerts.

Thresholds:
  temp_high     : temp anomaly > +10°F above baseline
  temp_low      : temp anomaly < -10°F below baseline
  wind_gust     : gust > avg_windspeed + 20 mph AND gust > 25 mph absolute
  rain_start    : hourlyrainin > 0.01"/hr
  pressure_drop : baromrelin drops > 0.08" in ~1 hour
"""
import os
from datetime import datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Key

from shared.secrets import get_secret
from shared.dynamodb import get_table
from wx_alerter.email_template import build_email

STATION_TZ     = ZoneInfo('America/New_York')
READINGS_TABLE = os.environ.get('READINGS_TABLE', 'wx-readings')
STATS_TABLE    = os.environ.get('STATS_TABLE', 'wx-daily-stats')
ALERTS_TABLE   = os.environ.get('ALERTS_TABLE', 'wx-alerts')
ALERT_FROM     = os.environ.get('ALERT_FROM', 'wx@jamestannahill.com')
ALERT_TO       = os.environ.get('ALERT_TO', 'james@jamestannahill.com')
STATION_SECRET = 'ambient-weather/station-config'

# alert_type -> (human label, cooldown minutes)
ALERT_META = {
    'temp_high':     ('Unusually warm',           120),
    'temp_low':      ('Unusually cold',            120),
    'wind_gust':     ('High wind gust',             60),
    'rain_start':    ('Rain started',               30),
    'pressure_drop': ('Rapid pressure drop',       120),
}

ses = boto3.client('ses', region_name='us-east-1')


def _floatify(obj):
    if isinstance(obj, dict):
        return {k: _floatify(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def handler(event, context):
    station = get_secret(STATION_SECRET)
    mac = station['mac_address']

    readings_table = get_table(READINGS_TABLE)
    stats_table    = get_table(STATS_TABLE)
    alerts_table   = get_table(ALERTS_TABLE)

    # Fetch latest 13 readings (~1 hour at 5-min intervals)
    result = readings_table.query(
        KeyConditionExpression=Key('station_id').eq(mac),
        ScanIndexForward=False,
        Limit=13,
    )
    items = [_floatify(r) for r in result.get('Items', [])]
    if not items:
        return

    reading = next((r for r in items if r.get('tempf') is not None), None)
    if not reading:
        return

    # Baseline for this month/hour (NY local time)
    now = datetime.now(timezone.utc).astimezone(STATION_TZ)
    month_hour = now.strftime('%m-%H')
    stats_resp = stats_table.get_item(Key={'station_id': mac, 'month_hour': month_hour})
    baseline = _floatify(stats_resp.get('Item', {}))

    # --- Compute signals ---

    # Temperature anomaly
    temp_delta = None
    if reading.get('tempf') is not None and baseline.get('avg_tempf') is not None:
        temp_delta = round(reading['tempf'] - baseline['avg_tempf'], 1)

    # Wind gust: statistically significant = gust > avg_wind + 20 mph AND gust > 25 mph
    gust = reading.get('windgustmph')
    avg_wind = baseline.get('avg_windspeedmph', 0) or 0
    gust_delta = round(gust - avg_wind, 1) if gust is not None else None

    # Pressure drop over ~1 hour (compare latest to oldest in the 13-reading window)
    pressure_delta = None
    latest_p  = reading.get('baromrelin')
    oldest_r  = next((r for r in reversed(items) if r.get('baromrelin') is not None), None)
    if latest_p and oldest_r and oldest_r.get('baromrelin'):
        pressure_delta = round(latest_p - oldest_r['baromrelin'], 3)

    # --- Build triggered list ---
    triggered = []

    if temp_delta is not None and temp_delta > 10:
        label = f"{reading['tempf']:.1f}°F — {temp_delta:+.1f}°F vs {now.strftime('%-I%p').lower()} avg"
        triggered.append(('temp_high', temp_delta, label))

    if temp_delta is not None and temp_delta < -10:
        label = f"{reading['tempf']:.1f}°F — {temp_delta:+.1f}°F vs {now.strftime('%-I%p').lower()} avg"
        triggered.append(('temp_low', temp_delta, label))

    if gust is not None and gust_delta is not None and gust > 25 and gust_delta > 20:
        label = f"{gust:.0f} mph gust ({gust_delta:+.0f} mph above {now.strftime('%-I%p').lower()} avg)"
        triggered.append(('wind_gust', gust, label))

    if reading.get('hourlyrainin') and reading['hourlyrainin'] > 0.01:
        label = f"{reading['hourlyrainin']:.2f}\"/hr"
        triggered.append(('rain_start', reading['hourlyrainin'], label))

    if pressure_delta is not None and pressure_delta < -0.08:
        label = f"{abs(pressure_delta):.3f}\" drop in ~1 hour ({reading.get('baromrelin', 0):.2f}\" now)"
        triggered.append(('pressure_drop', pressure_delta, label))

    if not triggered:
        return

    now_iso = now.isoformat()

    for alert_type, value, detail in triggered:
        _, cooldown_min = ALERT_META[alert_type]

        # Debounce check
        resp = alerts_table.get_item(Key={'alert_type': alert_type})
        last = resp.get('Item', {})
        last_alerted = last.get('last_alerted', '')
        if last_alerted:
            try:
                last_dt = datetime.fromisoformat(last_alerted)
                elapsed_min = (now - last_dt).total_seconds() / 60
                if elapsed_min < cooldown_min:
                    print(f"  {alert_type}: skipped (last alert {elapsed_min:.0f}m ago, cooldown {cooldown_min}m)")
                    continue
            except Exception:
                pass

        # Send
        human_label, _ = ALERT_META[alert_type]
        print(f"  {alert_type}: firing — {detail}")
        subject = f"wx alert: {human_label} — Midtown Manhattan"
        html = build_email(human_label, detail, reading, now)
        ses.send_email(
            Source=ALERT_FROM,
            Destination={'ToAddresses': [ALERT_TO]},
            Message={
                'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                'Body':    {'Html': {'Data': html, 'Charset': 'UTF-8'}},
            },
        )

        # Record debounce
        alerts_table.put_item(Item={
            'alert_type':   alert_type,
            'last_alerted': now_iso,
            'last_value':   str(round(float(value), 3)),
        })

    print(f"Alerter done. {len(triggered)} trigger(s) evaluated.")
