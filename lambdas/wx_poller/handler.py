import os, json, time, math
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from boto3.dynamodb.conditions import Key
from shared.secrets import get_secret
from shared.dynamodb import get_table
from shared.uhi import fetch_uhi
from wx_poller.og_image import generate_og
from wx_poller.validation import validate_reading, detect_stuck
from wx_poller.nearby import fetch_nearby

STATION_TZ = ZoneInfo('America/New_York')

READINGS_TABLE     = os.environ.get('READINGS_TABLE', 'wx-readings')
STATS_TABLE        = os.environ.get('STATS_TABLE', 'wx-daily-stats')
UHI_SEASONAL_TABLE = os.environ.get('UHI_SEASONAL_TABLE', 'wx-uhi-seasonal')
STAT_FIELDS        = ['tempf', 'feelsLike', 'humidity', 'windspeedmph', 'baromrelin', 'uv']
STD_FIELDS         = ['tempf', 'humidity', 'windspeedmph', 'baromrelin']  # fields that need std dev for chart bands
MAX_SAMPLE_COUNT   = 8640  # 30 days × 288 readings/day


def handler(event, context):
    creds   = get_secret('ambient-weather/api-keys')
    station = get_secret('ambient-weather/station-config')
    mac     = station['mac_address']
    wu_key  = creds.get('wu_api_key', '')

    raw = fetch_reading(mac, creds['api_key'], creds['application_key'])
    if not raw:
        print("No reading returned from Ambient API")
        return

    # --- Validate ranges -------------------------------------------------------
    cleaned, issues = validate_reading(raw)
    if issues:
        print(f"Validation issues: {issues}")

    # --- Stuck sensor detection (query last 8 stored readings) -----------------
    recent = _fetch_recent(mac, n=8)  # oldest-first list of floatified readings
    quality_flag = None

    if detect_stuck(recent + [cleaned], field='tempf'):
        quality_flag = 'stuck'
        print(f"Stuck sensor detected for {mac} — skipping stats update")
    elif issues:
        quality_flag = 'range_error'

    # --- Fetch UHI delta (non-critical) ----------------------------------------
    uhi_delta = None
    if cleaned.get('tempf') is not None and quality_flag is None:
        uhi_data = fetch_uhi(float(cleaned['tempf']))
        if uhi_data.get('uhi_delta') is not None:
            uhi_delta = uhi_data['uhi_delta']

    # --- Write to DynamoDB (always store, even if flagged) ---------------------
    now = datetime.now(timezone.utc)
    _write_reading(mac, now, cleaned, quality_flag=quality_flag, uhi_delta=uhi_delta)

    # --- Fetch and store nearby WU stations (non-critical) --------------------
    if quality_flag is None and wu_key:
        nearby = fetch_nearby(wu_key, limit=20)
        if nearby:
            _write_nearby_snapshot(mac, now, nearby)

    # --- Update rolling stats only for clean outdoor readings -----------------
    if cleaned.get('tempf') is not None and quality_flag is None:
        month_hour = compute_month_hour(now)
        update_rolling_stats(get_table(STATS_TABLE), mac, month_hour, cleaned)

        # Update UHI seasonal rolling average
        if uhi_delta is not None:
            local_now = now.astimezone(STATION_TZ)
            _update_uhi_seasonal(mac, str(local_now.month).zfill(2), uhi_delta)

    # --- OG image -------------------------------------------------------------
    try:
        from wx_api.anomaly import condition_label
        condition = condition_label(cleaned)
    except Exception:
        condition = None
    try:
        generate_og(cleaned, condition)
    except Exception as e:
        print(f"OG image generation failed (non-fatal): {e}")

    flag_str = f" [quality_flag={quality_flag}]" if quality_flag else ""
    uhi_str  = f" [uhi_delta={uhi_delta}]" if uhi_delta is not None else ""
    print(f"Wrote reading for {mac} at {now.isoformat()}{flag_str}{uhi_str}")


def fetch_reading(mac: str, api_key: str, application_key: str) -> dict | None:
    resp = requests.get(
        f"https://api.ambientweather.net/v1/devices/{mac}",
        params={'apiKey': api_key, 'applicationKey': application_key, 'limit': 1},
        timeout=10,
    )
    resp.raise_for_status()
    readings = resp.json()
    return readings[0] if readings else None


def compute_month_hour(dt: datetime) -> str:
    local = dt.astimezone(STATION_TZ)
    return local.strftime('%m-%H')


def _fetch_recent(mac: str, n: int = 8) -> list:
    """Return the last N readings from DynamoDB, oldest-first, as plain dicts."""
    table = get_table(READINGS_TABLE)
    result = table.query(
        KeyConditionExpression=Key('station_id').eq(mac),
        ScanIndexForward=False,
        Limit=n,
    )
    items = result.get('Items', [])
    floated = []
    for item in reversed(items):  # oldest first
        floated.append({
            k: float(v) if hasattr(v, 'is_finite') else v
            for k, v in item.items()
        })
    return floated


def _write_reading(station_id: str, now: datetime, reading: dict,
                   quality_flag: str | None = None, uhi_delta: float | None = None):
    table = get_table(READINGS_TABLE)
    item = {
        'station_id':     station_id,
        'timestamp':      now.isoformat(),
        'tempf':          _decimal(reading.get('tempf')),
        'feelsLike':      _decimal(reading.get('feelsLike')),
        'humidity':       _decimal(reading.get('humidity')),
        'dewPoint':       _decimal(reading.get('dewPoint')),
        'windspeedmph':   _decimal(reading.get('windspeedmph')),
        'windgustmph':    _decimal(reading.get('windgustmph')),
        'winddir':        _decimal(reading.get('winddir')),
        'baromrelin':     _decimal(reading.get('baromrelin')),
        'solarradiation': _decimal(reading.get('solarradiation')),
        'uv':             _decimal(reading.get('uv')),
        'hourlyrainin':   _decimal(reading.get('hourlyrainin')),
        'dailyrainin':    _decimal(reading.get('dailyrainin')),
        'ttl':            int(time.time()) + (90 * 86400),
    }
    if quality_flag:
        item['quality_flag'] = quality_flag
    if uhi_delta is not None:
        item['uhi_delta'] = _decimal(uhi_delta)
    table.put_item(Item=item)


def update_rolling_stats(table, station_id: str, month_hour: str, reading: dict):
    resp  = table.get_item(Key={'station_id': station_id, 'month_hour': month_hour})
    item  = resp.get('Item', {})
    count = int(item.get('sample_count', 0))

    new_vals = {f: float(reading.get(f) or 0) for f in STAT_FIELDS}

    if count == 0:
        avg_vals  = {f'avg_{f}': new_vals[f] for f in STAT_FIELDS}
        var_vals  = {f'var_{f}': 0.0 for f in STD_FIELDS}
        std_vals  = {f'std_{f}': 0.0 for f in STD_FIELDS}
        new_count = 1
    elif count < MAX_SAMPLE_COUNT:
        avg_vals = {
            f'avg_{f}': (float(item.get(f'avg_{f}', 0)) * count + new_vals[f]) / (count + 1)
            for f in STAT_FIELDS
        }
        # Welford's online variance: new_var = (old_var*n + (x-old_mean)*(x-new_mean)) / (n+1)
        var_vals, std_vals = {}, {}
        for f in STD_FIELDS:
            old_avg = float(item.get(f'avg_{f}', 0))
            new_avg = avg_vals[f'avg_{f}']
            old_var = float(item.get(f'var_{f}', 0))
            new_var = max(0.0, (old_var * count + (new_vals[f] - old_avg) * (new_vals[f] - new_avg)) / (count + 1))
            var_vals[f'var_{f}'] = new_var
            std_vals[f'std_{f}'] = math.sqrt(new_var)
        new_count = count + 1
    else:
        alpha    = 1 / MAX_SAMPLE_COUNT
        avg_vals = {
            f'avg_{f}': alpha * new_vals[f] + (1 - alpha) * float(item.get(f'avg_{f}', 0))
            for f in STAT_FIELDS
        }
        # EMA variance: var_new = (1-α)*var_old + α*(x - mean_old)²
        var_vals, std_vals = {}, {}
        for f in STD_FIELDS:
            old_avg = float(item.get(f'avg_{f}', 0))
            old_var = float(item.get(f'var_{f}', 0))
            new_var = max(0.0, (1 - alpha) * old_var + alpha * (new_vals[f] - old_avg) ** 2)
            var_vals[f'var_{f}'] = new_var
            std_vals[f'std_{f}'] = math.sqrt(new_var)
        new_count = MAX_SAMPLE_COUNT

    table.put_item(Item={
        'station_id':   station_id,
        'month_hour':   month_hour,
        **{k: _decimal(v) for k, v in avg_vals.items()},
        **{k: _decimal(v) for k, v in var_vals.items()},
        **{k: _decimal(v) for k, v in std_vals.items()},
        'sample_count': new_count,
        'source':       'station',
    })


def _update_uhi_seasonal(station_id: str, month: str, delta: float):
    """Rolling average of UHI delta per calendar month (01–12)."""
    table = get_table(UHI_SEASONAL_TABLE)
    resp  = table.get_item(Key={'station_id': station_id, 'month': month})
    item  = resp.get('Item', {})
    count = int(item.get('sample_count', 0))

    if count == 0:
        new_avg = delta
    else:
        old_avg = float(item.get('avg_delta', 0))
        # Cap at 10,000 samples per month so very old data doesn't dominate forever
        eff = min(count, 10000)
        new_avg = (old_avg * eff + delta) / (eff + 1)

    table.put_item(Item={
        'station_id':   station_id,
        'month':        month,
        'avg_delta':    _decimal(new_avg),
        'sample_count': count + 1,
        'updated_at':   datetime.now(timezone.utc).isoformat(),
    })


def _write_nearby_snapshot(station_id: str, now, nearby: list):
    """Write the latest nearby station snapshot to wx-nearby-snapshots."""
    import json as _json
    table = get_table(os.environ.get('NEARBY_TABLE', 'wx-nearby-snapshots'))
    table.put_item(Item={
        'station_id':    station_id,
        'snapshot_at':   now.isoformat(),
        'stations_json': _json.dumps(nearby),
        'station_count': len(nearby),
        'ttl':           int(__import__('time').time()) + (30 * 86400),
    })


def _decimal(val):
    if val is None:
        return None
    from decimal import Decimal
    return Decimal(str(val))
