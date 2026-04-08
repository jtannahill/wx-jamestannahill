"""
Analog Forecast — runs every 30 minutes via EventBridge.

Algorithm:
  1. Fetch all 90-day readings from DynamoDB, bucket into 1-hour averages.
  2. Current fingerprint = last 6 hourly buckets (normalized).
  3. For every historical 6-hour window that has 3 hours of data following it,
     compute normalized Euclidean distance to the current fingerprint.
  4. Take the top-5 closest analogs.
  5. Average their subsequent 3-hour trajectories → ensemble forecast.
  6. Write result to wx-forecasts (one item per station, overwritten each run).

Also evaluates the previous forecast against actual readings and writes
per-evaluation accuracy + running MAE to wx-forecast-accuracy.

This never runs synchronously — the API reads the pre-computed results.
"""
import os, math
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from collections import defaultdict

import boto3
from boto3.dynamodb.conditions import Key

from shared.secrets import get_secret
from shared.dynamodb import get_table

READINGS_TABLE  = os.environ.get('READINGS_TABLE',  'wx-readings')
FORECASTS_TABLE = os.environ.get('FORECASTS_TABLE', 'wx-forecasts')
ACCURACY_TABLE  = os.environ.get('ACCURACY_TABLE',  'wx-forecast-accuracy')

FORECAST_FIELDS = ['tempf', 'humidity', 'windspeedmph', 'baromrelin']
WINDOW_HOURS    = 6
FORECAST_HOURS  = 3
TOP_N           = 5

# Normalization ranges — physical bounds for Midtown Manhattan
_RANGES = {
    'tempf':        (-20.0, 105.0),
    'humidity':     (  0.0, 100.0),
    'windspeedmph': (  0.0,  45.0),
    'baromrelin':   ( 28.5,  31.0),
}


def handler(event, context):
    station = get_secret('ambient-weather/station-config')
    mac     = station['mac_address']

    # ── Fetch all 90-day readings ─────────────────────────────────────────────
    readings_table = get_table(READINGS_TABLE)
    since = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()

    raw_items = []
    kwargs = dict(
        KeyConditionExpression=Key('station_id').eq(mac) & Key('timestamp').gte(since),
        ScanIndexForward=True,
    )
    while True:
        result = readings_table.query(**kwargs)
        raw_items.extend(result.get('Items', []))
        last = result.get('LastEvaluatedKey')
        if not last:
            break
        kwargs['ExclusiveStartKey'] = last

    print(f"Fetched {len(raw_items)} readings")

    floated = [_floatify(r) for r in raw_items]
    hourly  = _to_hourly(floated)
    keys    = sorted(hourly.keys())
    n       = len(keys)

    min_needed = WINDOW_HOURS + FORECAST_HOURS + 24  # at least a day of history
    if n < min_needed:
        print(f"Not enough hourly buckets ({n} < {min_needed})")
        return

    # ── Evaluate previous forecast against actuals ────────────────────────────
    try:
        forecasts_table = get_table(FORECASTS_TABLE)
        prev_resp = forecasts_table.get_item(Key={'station_id': mac})
        prev_item = prev_resp.get('Item')
        if prev_item:
            _evaluate_forecast_accuracy(mac, _floatify(prev_item), hourly)
    except Exception as e:
        print(f"Accuracy evaluation failed (non-fatal): {e}")

    # ── Current fingerprint: last WINDOW_HOURS buckets ───────────────────────
    current = [hourly[k] for k in keys[-WINDOW_HOURS:]]

    # ── Search for analog windows ─────────────────────────────────────────────
    # Exclude the last (WINDOW_HOURS + FORECAST_HOURS) buckets from candidates
    # so we're never "forecasting" with data that overlaps the present.
    search_limit = n - WINDOW_HOURS - FORECAST_HOURS
    candidates   = []

    for i in range(search_limit):
        window   = [hourly[k] for k in keys[i:i + WINDOW_HOURS]]
        dist     = _distance(current, window)
        if dist is None:
            continue
        future   = [hourly[k] for k in keys[i + WINDOW_HOURS:i + WINDOW_HOURS + FORECAST_HOURS]]
        candidates.append((dist, keys[i], future))

    if not candidates:
        print("No valid analog candidates")
        return

    candidates.sort(key=lambda x: x[0])
    top = candidates[:TOP_N]
    print(f"Top analog distances: {[round(c[0],4) for c in top]}")
    print(f"Best match window starts: {top[0][1]}")

    # ── Average the forecast trajectories ────────────────────────────────────
    forecast = []
    for hr in range(FORECAST_HOURS):
        bucket = {}
        for field in FORECAST_FIELDS:
            vals = [
                c[2][hr][field] for c in top
                if len(c[2]) > hr and c[2][hr].get(field) is not None
            ]
            bucket[field] = round(sum(vals) / len(vals), 1) if vals else None
        bucket['offset_hours'] = hr + 1
        bucket['timestamp']    = (datetime.now(timezone.utc) + timedelta(hours=hr + 1)).isoformat()
        forecast.append(bucket)

    # Confidence: lower avg_distance = better match = higher confidence
    avg_dist   = sum(c[0] for c in top) / len(top)
    confidence = max(0, min(100, round(100 - avg_dist * 400)))

    # Human-readable best match label
    try:
        best_dt    = datetime.fromisoformat(top[0][1])
        best_label = best_dt.strftime('%-b %-d at %-I%p').lower()
    except Exception:
        best_label = top[0][1]

    # ── Write to wx-forecasts ─────────────────────────────────────────────────
    forecasts_table = get_table(FORECASTS_TABLE)
    forecasts_table.put_item(Item={
        'station_id':    mac,
        'computed_at':   datetime.now(timezone.utc).isoformat(),
        'forecast':      _dec_deep(forecast),
        'best_match_ts': top[0][1],
        'best_match_label': best_label,
        'avg_distance':  _dec(avg_dist),
        'confidence':    confidence,
        'analogs_found': len(candidates),
    })
    print(f"Forecast written. Confidence: {confidence}. Best match: {best_label}")


# ── Forecast accuracy evaluation ──────────────────────────────────────────────

def _evaluate_forecast_accuracy(mac: str, prev_forecast: dict, hourly: dict):
    """
    Compare the previous forecast to actual readings.
    Only evaluates if the forecast is 55–240 minutes old (actuals available
    for +1h but we haven't missed the +3h window).
    """
    computed_at_str = prev_forecast.get('computed_at')
    prev_hours      = prev_forecast.get('forecast', [])
    if not computed_at_str or not prev_hours:
        return

    computed_dt = _parse_ts(computed_at_str)
    if not computed_dt:
        return

    age_min = (datetime.now(timezone.utc) - computed_dt).total_seconds() / 60
    if age_min < 55 or age_min > 240:
        # Too recent (actuals not available) or too stale (already evaluated / missed)
        return

    errors = {}
    for h_item in prev_hours:
        offset = h_item.get('offset_hours')
        if not offset:
            continue
        target_dt  = computed_dt + timedelta(hours=offset)
        bucket_key = target_dt.replace(minute=0, second=0, microsecond=0).isoformat()

        # Accept ±1 bucket (±1 hour) to account for clock drift
        actual = None
        for adj in [0, -1, 1]:
            adj_key = (target_dt + timedelta(hours=adj)).replace(minute=0, second=0, microsecond=0).isoformat()
            if adj_key in hourly:
                actual = hourly[adj_key]
                break
        if not actual:
            continue

        for field in FORECAST_FIELDS:
            f_val = h_item.get(field)
            a_val = actual.get(field)
            if f_val is not None and a_val is not None:
                errors[f'mae_{offset}h_{field}'] = abs(float(f_val) - float(a_val))

    if not errors:
        print("Accuracy evaluation: no matched buckets")
        return

    # Write per-evaluation entry
    evaluated_at = datetime.now(timezone.utc).isoformat()
    acc_table    = get_table(ACCURACY_TABLE)
    acc_table.put_item(Item={
        'station_id':           mac,
        'evaluated_at':         evaluated_at,
        'forecast_computed_at': computed_at_str,
        **{k: _dec(v) for k, v in errors.items()},
    })

    # Update running entry (simple incremental mean)
    resp   = acc_table.get_item(Key={'station_id': mac, 'evaluated_at': 'running'})
    run    = _floatify(resp.get('Item', {}))
    count  = int(run.get('evaluation_count', 0))
    new_count = count + 1
    running = {'station_id': mac, 'evaluated_at': 'running', 'evaluation_count': new_count}
    for k, v in errors.items():
        old = float(run.get(k, v))
        running[k] = _dec((old * count + v) / new_count)
    acc_table.put_item(Item=running)

    headline = {k: round(v, 2) for k, v in errors.items() if '1h_tempf' in k}
    print(f"Accuracy written (n={new_count}): {headline}")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_hourly(readings: list) -> dict:
    """Bucket readings into 1-hour averages keyed by floor-hour ISO string."""
    buckets = defaultdict(list)
    for r in readings:
        try:
            dt     = datetime.fromisoformat(r.get('timestamp', ''))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            bucket = dt.replace(minute=0, second=0, microsecond=0).isoformat()
            buckets[bucket].append(r)
        except Exception:
            pass

    result = {}
    for bucket, group in buckets.items():
        avg = {}
        for f in FORECAST_FIELDS:
            vals = [r[f] for r in group if r.get(f) is not None]
            avg[f] = round(sum(vals) / len(vals), 2) if vals else None
        result[bucket] = avg
    return result


def _norm(val, field):
    if val is None:
        return None
    lo, hi = _RANGES[field]
    return max(0.0, min(1.0, (float(val) - lo) / (hi - lo)))


def _distance(a_window: list, b_window: list) -> float | None:
    """Normalized Euclidean distance. Returns None if too many values missing."""
    total, count = 0.0, 0
    for a, b in zip(a_window, b_window):
        for f in FORECAST_FIELDS:
            na, nb = _norm(a.get(f), f), _norm(b.get(f), f)
            if na is not None and nb is not None:
                total += (na - nb) ** 2
                count += 1
    # Require at least 50% field coverage across the window
    if count < len(FORECAST_FIELDS) * WINDOW_HOURS * 0.5:
        return None
    return math.sqrt(total / count)


def _parse_ts(ts_str: str):
    try:
        dt = datetime.fromisoformat(ts_str)
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    except Exception:
        return None


def _floatify(obj):
    if isinstance(obj, dict):
        return {k: _floatify(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def _dec(val):
    if val is None:
        return None
    return Decimal(str(round(float(val), 6)))


def _dec_deep(obj):
    if isinstance(obj, list):
        return [_dec_deep(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _dec_deep(v) for k, v in obj.items()}
    if isinstance(obj, float):
        return _dec(obj)
    return obj
