"""
Analog Forecast — runs every 30 minutes via EventBridge.

Algorithm:
  1. Fetch all 90-day readings from DynamoDB, bucket into 1-hour averages.
  2. Current fingerprint = last 6 hourly buckets (normalized).
  3. For every historical 6-hour window that has 3 hours of data following it,
     compute normalized Euclidean distance to the current fingerprint.
  4. Take the top-5 closest analogs.
  5. Add the analogs' average change over the next 3 hours to the current
     reading (their absolute values would inherit their season's level).
  6. Write result to wx-forecasts (one item per station, overwritten each run).

Each forecast also joins a 'pending' queue on that item. Once a queued
forecast's +3h hour has closed, it is scored against the actual readings and
per-evaluation accuracy + running MAE are written to wx-forecast-accuracy.

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

# A forecast is scored once its +3h hour has closed, so it waits about 4 hours
# in the pending queue: 8 runs at the 30-minute cadence, plus headroom.
EVAL_AFTER_MIN  = 240
DROP_AFTER_MIN  = 24 * 60
PENDING_MAX     = 12

FIELD_LIMITS    = {'humidity': (0, 100), 'windspeedmph': (0, None)}

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

    # ── Score queued forecasts whose +3h hour has passed ──────────────────────
    # wx-forecasts holds one row that this run overwrites, so the forecast it
    # replaces is only 30 minutes old. Forecasts wait in a 'pending' queue on
    # that row until all three target hours have actuals.
    pending = []
    try:
        forecasts_table = get_table(FORECASTS_TABLE)
        prev_item = forecasts_table.get_item(Key={'station_id': mac}).get('Item')
        if prev_item:
            prev = _floatify(prev_item)
            pending = prev.get('pending')
            if pending is None and prev.get('computed_at'):
                pending = [{'computed_at': prev['computed_at'], 'forecast': prev.get('forecast', [])}]
            ready, pending = split_pending(pending or [], datetime.now(timezone.utc))
            for entry in ready:
                _record_accuracy(mac, entry['computed_at'], forecast_errors(entry, hourly))
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
        candidates.append((dist, keys[i], future, window[-1]))

    if not candidates:
        print("No valid analog candidates")
        return

    candidates.sort(key=lambda x: x[0])
    top = candidates[:TOP_N]
    print(f"Top analog distances: {[round(c[0],4) for c in top]}")
    print(f"Best match window starts: {top[0][1]}")

    # ── Ensemble: current value plus the analogs' average change ─────────────
    forecast = delta_forecast(current[-1], [(c[3], c[2]) for c in top])
    for bucket in forecast:
        bucket['timestamp'] = (datetime.now(timezone.utc) + timedelta(hours=bucket['offset_hours'])).isoformat()

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
    computed_at = datetime.now(timezone.utc).isoformat()
    pending = (pending + [{'computed_at': computed_at, 'forecast': forecast}])[-PENDING_MAX:]
    forecasts_table = get_table(FORECASTS_TABLE)
    forecasts_table.put_item(Item={
        'station_id':    mac,
        'computed_at':   computed_at,
        'forecast':      _dec_deep(forecast),
        'pending':       _dec_deep(pending),
        'best_match_ts': top[0][1],
        'best_match_label': best_label,
        'avg_distance':  _dec(avg_dist),
        'confidence':    confidence,
        'analogs_found': len(candidates),
    })
    print(f"Forecast written. Confidence: {confidence}. Best match: {best_label}")


# ── Forecast accuracy evaluation ──────────────────────────────────────────────

def delta_forecast(current_last: dict, analogs: list) -> list:
    """
    Ensemble forecast from (window_last, future) analog pairs. Each field is
    the current value plus the mean change the analogs showed from the end of
    their window. Copying the analogs' absolute values instead inherited their
    level: with a summer-heavy 90-day pool, October forecasts ran 4 to 10F warm
    (14-day backtest, +1h temperature MAE 5.4F absolute vs 1.05F as a change).
    Falls back to the analogs' mean level when the current value is missing.
    """
    forecast = []
    for hr in range(FORECAST_HOURS):
        bucket = {}
        for field in FORECAST_FIELDS:
            now_v = current_last.get(field)
            deltas = [
                fut[hr][field] - last[field] for last, fut in analogs
                if len(fut) > hr and fut[hr].get(field) is not None and last.get(field) is not None
            ]
            levels = [fut[hr][field] for _, fut in analogs if len(fut) > hr and fut[hr].get(field) is not None]
            if now_v is not None and deltas:
                val = now_v + sum(deltas) / len(deltas)
            elif levels:
                val = sum(levels) / len(levels)
            else:
                bucket[field] = None
                continue
            lo, hi = FIELD_LIMITS.get(field, (None, None))
            if lo is not None: val = max(lo, val)
            if hi is not None: val = min(hi, val)
            bucket[field] = round(val, 2 if field == 'baromrelin' else 1)
        bucket['offset_hours'] = hr + 1
        forecast.append(bucket)
    return forecast


def split_pending(pending: list, now: datetime) -> tuple[list, list]:
    """
    Split queued forecasts into (ready to score, keep waiting). A forecast is
    ready once EVAL_AFTER_MIN have passed, so its +3h hour has full actuals;
    one older than DROP_AFTER_MIN is discarded unscored.
    """
    ready, keep = [], []
    for entry in pending:
        computed = _parse_ts(entry.get('computed_at') or '')
        if not computed:
            continue
        age_min = (now - computed).total_seconds() / 60
        if age_min > DROP_AFTER_MIN:
            continue
        (ready if age_min >= EVAL_AFTER_MIN else keep).append(entry)
    return ready, keep


def forecast_errors(entry: dict, hourly: dict) -> dict:
    """Absolute error per offset and field, keyed mae_<offset>h_<field>."""
    computed_dt = _parse_ts(entry.get('computed_at') or '')
    if not computed_dt:
        return {}
    errors = {}
    for h_item in entry.get('forecast', []):
        offset = h_item.get('offset_hours')
        if not offset:
            continue
        offset = int(offset)
        target_dt = computed_dt + timedelta(hours=offset)

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
    return errors


def _record_accuracy(mac: str, computed_at: str, errors: dict):
    """Write one evaluation row and fold it into the running means."""
    if not errors:
        print(f"Accuracy evaluation: no matched buckets for forecast {computed_at}")
        return

    evaluated_at = datetime.now(timezone.utc).isoformat()
    acc_table    = get_table(ACCURACY_TABLE)
    acc_table.put_item(Item={
        'station_id':           mac,
        'evaluated_at':         evaluated_at,
        'forecast_computed_at': computed_at,
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
