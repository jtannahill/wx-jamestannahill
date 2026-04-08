"""
wx-summarizer: Daily weather summary generator.
Runs at 05:00 UTC (midnight ET) every day.

For each of the last 30 days, computes (idempotent — safe to rerun):
  - temp_high, temp_low, temp_avg
  - total_rain (inches), max_gust (mph)
  - avg_comfort (0–100)
  - summary — 2-3 sentence prose description

Results stored in wx-daily-summaries keyed by station_id + date.
The /daily-summaries API route serves these for the comfort calendar and
yesterday's summary on the dashboard.
"""
import os, math
from datetime import datetime, timezone, timedelta, date as date_type
from decimal import Decimal
from zoneinfo import ZoneInfo

from boto3.dynamodb.conditions import Key
from shared.secrets import get_secret
from shared.dynamodb import get_table

READINGS_TABLE  = os.environ.get('READINGS_TABLE',  'wx-readings')
SUMMARIES_TABLE = os.environ.get('SUMMARIES_TABLE', 'wx-daily-summaries')
STATION_TZ      = ZoneInfo('America/New_York')
DAYS_TO_PROCESS = 30   # idempotent — reprocess last 30 days each run


def handler(event, context):
    station = get_secret('ambient-weather/station-config')
    mac     = station['mac_address']
    now_et  = datetime.now(timezone.utc).astimezone(STATION_TZ)

    processed = 0
    for days_ago in range(1, DAYS_TO_PROCESS + 1):
        target = (now_et - timedelta(days=days_ago)).date()
        ok = _process_day(mac, target)
        if ok:
            processed += 1

    print(f"Summarizer complete: {processed}/{DAYS_TO_PROCESS} days written")
    return {"status": "ok", "processed": processed}


def _process_day(mac: str, target: date_type) -> bool:
    """Compute and store the summary for one local calendar day. Returns True on success."""
    tz          = STATION_TZ
    day_start   = datetime.combine(target, datetime.min.time()).replace(tzinfo=tz).astimezone(timezone.utc)
    day_end     = day_start + timedelta(days=1)

    table = get_table(READINGS_TABLE)
    items = []
    kwargs = dict(
        KeyConditionExpression=Key('station_id').eq(mac) &
            Key('timestamp').between(day_start.isoformat(), day_end.isoformat()),
        ScanIndexForward=True,
    )
    while True:
        result = table.query(**kwargs)
        items.extend(result.get('Items', []))
        last = result.get('LastEvaluatedKey')
        if not last:
            break
        kwargs['ExclusiveStartKey'] = last

    readings = [_floatify(r) for r in items if not r.get('quality_flag')]
    if len(readings) < 12:
        return False

    temps  = [r['tempf'] for r in readings if r.get('tempf') is not None]
    rains  = [float(r.get('hourlyrainin') or 0) for r in readings]
    gusts  = [float(r.get('windgustmph')  or 0) for r in readings]

    if not temps:
        return False

    temp_high  = round(max(temps), 1)
    temp_low   = round(min(temps), 1)
    temp_avg   = round(sum(temps) / len(temps), 1)
    total_rain = round(sum(rains) * 5 / 60, 2)   # in/hr × 5-min intervals → inches
    max_gust   = round(max(gusts), 0)

    comfort_scores = [_comfort_score(r) for r in readings]
    avg_comfort    = round(sum(comfort_scores) / len(comfort_scores))

    summary = _make_summary(target, readings, temp_high, temp_low, temp_avg, total_rain, max_gust)

    table_s = get_table(SUMMARIES_TABLE)
    table_s.put_item(Item={
        'station_id':    mac,
        'date':          target.isoformat(),
        'temp_high':     _dec(temp_high),
        'temp_low':      _dec(temp_low),
        'temp_avg':      _dec(temp_avg),
        'total_rain':    _dec(total_rain),
        'max_gust':      _dec(max_gust),
        'avg_comfort':   avg_comfort,
        'reading_count': len(readings),
        'summary':       summary,
    })
    print(f"{target}: high={temp_high}°F low={temp_low}°F rain={total_rain}\" comfort={avg_comfort}")
    return True


# ── Comfort score (no baseline — daily averages don't have per-slot baselines) ─

def _comfort_score(r: dict) -> int:
    feels    = float(r.get('feelsLike') or r.get('tempf') or 65)
    humidity = float(r.get('humidity')  or 50)
    wind     = float(r.get('windspeedmph') or 0)
    uv       = float(r.get('uv')        or 0)
    rain     = float(r.get('hourlyrainin') or 0)

    score = 100.0
    dev   = abs(feels - 71.0)
    score -= min(45, (dev / 1.8) ** 1.5)
    if humidity > 65: score -= (humidity - 65) * 0.5
    elif humidity < 30: score -= (30 - humidity) * 0.3
    if wind > 10: score -= min(20, (wind - 10) * 0.9)
    if uv >= 3:   score -= min(15, (uv - 2) * 0.5)
    if rain > 0.05: score -= 25
    elif rain > 0.01: score -= 15
    return max(0, min(100, round(score)))


# ── Rain event detection ──────────────────────────────────────────────────────

def _detect_rain_events(readings: list) -> list:
    events, in_rain, start, rates = [], False, None, []
    for r in readings:
        rate = float(r.get('hourlyrainin') or 0)
        if rate > 0.01:
            if not in_rain:
                in_rain, start, rates = True, r.get('timestamp', ''), []
            rates.append(rate)
        elif in_rain:
            in_rain = False
            events.append({
                'start':        start,
                'peak_rate':    round(max(rates), 2),
                'duration_min': len(rates) * 5,
                'total_in':     round(sum(rates) * 5 / 60, 2),
            })
            rates = []
    if in_rain and rates:
        events.append({
            'start':        start,
            'peak_rate':    round(max(rates), 2),
            'duration_min': len(rates) * 5,
            'total_in':     round(sum(rates) * 5 / 60, 2),
        })
    return events


# ── Prose generator ───────────────────────────────────────────────────────────

def _make_summary(target, readings, high, low, avg, total_rain, max_gust) -> str:
    spread = round(high - low, 0)
    month  = target.month

    # Sentence 1 — temperature
    if spread < 8:
        s1 = f"Temperatures were steady, ranging {spread:.0f}°F from {low:.0f} to {high:.0f}°F."
    else:
        s1 = f"Temperatures swung from {low:.0f}°F to a high of {high:.0f}°F."

    # Sentence 2 — rain or wind (pick the more notable)
    if total_rain > 0.01:
        events = _detect_rain_events(readings)
        n = len(events)
        if n == 0:
            s2 = f"{total_rain:.2f}\" of rain was recorded."
        elif n == 1:
            peak = events[0]['peak_rate']
            s2 = f"{total_rain:.2f}\" of rain fell in one event (peak {peak:.2f}\"/hr)."
        else:
            s2 = f"{total_rain:.2f}\" of rain fell across {n} separate events."
    elif max_gust > 25:
        s2 = f"Wind gusts reached {max_gust:.0f} mph."
    else:
        s2 = None

    parts = [s1]
    if s2:
        parts.append(s2)
    return ' '.join(parts)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _floatify(obj):
    if isinstance(obj, dict):
        return {k: _floatify(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def _dec(val):
    if val is None:
        return None
    return Decimal(str(round(float(val), 4)))
