"""
wx-records-tracker: Compute per-calendar-month station records.
Runs weekly (Sunday 02:00 UTC).

Scans all 90-day readings, groups by local calendar month, computes:
  temp_high, temp_low, max_gust, max_rain_rate, min/max_pressure
  — with the date each record was set.

Stored in wx-station-records keyed by station_id + month ("01"–"12").
The /current API includes the current month's records on every response.
"""
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo
from collections import defaultdict

from boto3.dynamodb.conditions import Key
from shared.secrets import get_secret
from shared.dynamodb import get_table

READINGS_TABLE = os.environ.get('READINGS_TABLE', 'wx-readings')
RECORDS_TABLE  = os.environ.get('RECORDS_TABLE',  'wx-station-records')
STATION_TZ     = ZoneInfo('America/New_York')
MONTH_NAMES    = ['Jan','Feb','Mar','Apr','May','Jun',
                  'Jul','Aug','Sep','Oct','Nov','Dec']


def handler(event, context):
    station = get_secret('ambient-weather/station-config')
    mac     = station['mac_address']

    table = get_table(READINGS_TABLE)
    since = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    items, kwargs = [], dict(
        KeyConditionExpression=Key('station_id').eq(mac) & Key('timestamp').gte(since),
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
    print(f"Processing {len(readings)} clean readings")

    # Group by local calendar month
    by_month = defaultdict(list)
    for r in readings:
        ts_str = r.get('timestamp', '')
        try:
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            local = ts.astimezone(STATION_TZ)
            by_month[str(local.month).zfill(2)].append((r, local))
        except Exception:
            pass

    records_table = get_table(RECORDS_TABLE)
    for month, group in by_month.items():
        rec = _compute_records(group)
        records_table.put_item(Item={
            'station_id':   mac,
            'month':        month,
            'month_name':   MONTH_NAMES[int(month) - 1],
            'sample_count': len(group),
            'computed_at':  datetime.now(timezone.utc).isoformat(),
            **{k: (_dec(v) if isinstance(v, float) else v)
               for k, v in rec.items()},
        })
        print(f"Month {month}: {rec}")

    return {"status": "ok", "months_processed": len(by_month)}


def _compute_records(group: list) -> dict:
    """group = list of (reading_dict, local_datetime) tuples."""
    rec = {}

    temps     = [(r['tempf'],        dt.date().isoformat()) for r, dt in group if r.get('tempf') is not None]
    gusts     = [(r['windgustmph'],  dt.date().isoformat()) for r, dt in group if r.get('windgustmph') is not None]
    rains     = [(r['hourlyrainin'], dt.date().isoformat()) for r, dt in group if r.get('hourlyrainin') is not None]
    pressures = [(r['baromrelin'],   dt.date().isoformat()) for r, dt in group if r.get('baromrelin') is not None]

    if temps:
        hot  = max(temps,  key=lambda x: x[0])
        cold = min(temps,  key=lambda x: x[0])
        rec['temp_high']    = round(float(hot[0]),  1)
        rec['temp_high_at'] = hot[1]
        rec['temp_low']     = round(float(cold[0]), 1)
        rec['temp_low_at']  = cold[1]

    if gusts:
        windy = max(gusts, key=lambda x: x[0])
        rec['max_gust']    = round(float(windy[0]), 0)
        rec['max_gust_at'] = windy[1]

    if rains:
        heavy = max(rains, key=lambda x: x[0])
        if float(heavy[0]) > 0.01:
            rec['max_rain_rate']    = round(float(heavy[0]), 2)
            rec['max_rain_rate_at'] = heavy[1]

    if pressures:
        lo = min(pressures, key=lambda x: x[0])
        hi = max(pressures, key=lambda x: x[0])
        rec['min_pressure']    = round(float(lo[0]), 2)
        rec['min_pressure_at'] = lo[1]
        rec['max_pressure']    = round(float(hi[0]), 2)
        rec['max_pressure_at'] = hi[1]

    return rec


def _floatify(obj):
    if isinstance(obj, dict):
        return {k: _floatify(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def _dec(val):
    return Decimal(str(round(float(val), 4))) if val is not None else None
