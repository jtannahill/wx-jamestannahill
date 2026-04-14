import os, json
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal

STATION_TZ = ZoneInfo('America/New_York')
from shared.secrets import get_secret
from shared.dynamodb import get_table
from wx_api.anomaly import compute_anomalies, pressure_trend, condition_label, percentile_rank
from wx_api.climate_context import live_context, daily_verdict, anomaly_headline
from shared.uhi import fetch_uhi
from wx_api.ml import comfort_score, rain_probability
from wx_api.nearby import nearby_route, _fetch_nearby_snapshot
from boto3.dynamodb.conditions import Key

READINGS_TABLE     = os.environ.get('READINGS_TABLE',     'wx-readings')
STATS_TABLE        = os.environ.get('STATS_TABLE',        'wx-daily-stats')
FORECASTS_TABLE    = os.environ.get('FORECASTS_TABLE',    'wx-forecasts')
ACCURACY_TABLE     = os.environ.get('ACCURACY_TABLE',     'wx-forecast-accuracy')
UHI_SEASONAL_TABLE = os.environ.get('UHI_SEASONAL_TABLE', 'wx-uhi-seasonal')
SUMMARIES_TABLE    = os.environ.get('SUMMARIES_TABLE',    'wx-daily-summaries')
RECORDS_TABLE      = os.environ.get('RECORDS_TABLE',      'wx-station-records')
CLIMATE_DOY_TABLE    = os.environ.get('CLIMATE_DOY_TABLE',    'wx-climate-doy')
CLIMATE_HOURLY_TABLE = os.environ.get('CLIMATE_HOURLY_TABLE', 'wx-climate-hourly')
STATION_SECRET     = 'ambient-weather/station-config'

_MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Content-Type": "application/json",
}


def handler(event, context):
    path = event.get('rawPath', '/')

    if path == '/current':
        return _current()
    elif path == '/history':
        params = event.get('queryStringParameters') or {}
        hours = int(params.get('hours', 24))
        return _history(min(hours, 720))  # max 30 days
    elif path == '/rain-events':
        params = event.get('queryStringParameters') or {}
        days = int(params.get('days', 30))
        return _rain_events_route(min(days, 90))
    elif path == '/daily-summaries':
        params = event.get('queryStringParameters') or {}
        days = int(params.get('days', 90))
        return _daily_summaries_route(min(days, 365))
    elif path == '/nearby':
        try:
            station = get_secret(STATION_SECRET)
            station_id = station['mac_address']
            return _resp(200, nearby_route(station_id))
        except Exception as e:
            print(f"Nearby route error: {e}")
            return _resp(500, {'error': 'nearby unavailable'})
    else:
        return _resp(404, {"error": "Not found"})


def _current():
    station = get_secret(STATION_SECRET)
    mac = station['mac_address']
    table = get_table(READINGS_TABLE)

    result = table.query(
        KeyConditionExpression=Key('station_id').eq(mac),
        ScanIndexForward=False,
        Limit=10,
    )
    items = result.get('Items', [])
    if not items:
        return _resp(503, {"error": "No data available"})

    floated = [_floatify(r) for r in items]
    recent = floated

    # Prefer: clean + has outdoor data → has outdoor data → latest
    reading = (
        next((r for r in floated if r.get('tempf') is not None and not r.get('quality_flag')), None)
        or next((r for r in floated if r.get('tempf') is not None), None)
        or floated[0]
    )

    now = datetime.now(timezone.utc)

    # Staleness: how old is the most recent reading?
    try:
        reading_ts   = datetime.fromisoformat(reading['timestamp'])
        if reading_ts.tzinfo is None:
            reading_ts = reading_ts.replace(tzinfo=timezone.utc)
        age_minutes  = (now - reading_ts).total_seconds() / 60
        data_stale   = age_minutes > 15
    except Exception:
        age_minutes  = None
        data_stale   = False

    local_now = now.astimezone(STATION_TZ)
    month_hour = local_now.strftime('%m-%H')
    doy      = local_now.strftime("%m%d")        # e.g. "0413"
    doy_hour = f"{doy}-{local_now.hour:02d}"     # e.g. "0413-14"
    stats_table = get_table(STATS_TABLE)
    stats_resp = stats_table.get_item(Key={'station_id': mac, 'month_hour': month_hour})
    baseline = _floatify(stats_resp.get('Item', {}))

    anomalies = compute_anomalies(reading, baseline, local_now.month, local_now.hour) if baseline else {}
    trend     = pressure_trend(recent)
    label     = condition_label(reading)
    baseline_source = baseline.get('source', 'none') if baseline else 'none'

    # ML signals
    uhi             = fetch_uhi(reading['tempf']) if reading.get('tempf') is not None else {}
    comfort         = comfort_score(reading, baseline, local_now.month)
    pct_rank        = percentile_rank(reading, baseline, local_now.month) if baseline else None
    nearby          = _fetch_nearby_snapshot(mac)
    rain_prob       = rain_probability(reading, recent, nearby)
    forecast        = _fetch_forecast(mac)
    uhi_seasonal    = _fetch_uhi_seasonal(mac)
    station_records = _fetch_station_records(mac, local_now.month)
    daily_summary   = _fetch_latest_summary(mac)
    climate_doy_stats    = _fetch_climate_doy(mac, doy)
    climate_hourly_stats = _fetch_climate_hourly(mac, doy_hour)

    # Climate context — live percentile + daily verdict
    climate_live    = live_context(reading, climate_hourly_stats, doy)
    today_high      = daily_summary.get("temp_high")  if daily_summary else None
    today_low       = daily_summary.get("temp_low")   if daily_summary else None
    climate_verdict = daily_verdict(today_high, today_low, climate_doy_stats, doy) if today_high else None
    climate_mode    = "daily" if climate_verdict else "live"
    climate_headline = anomaly_headline(climate_live, climate_verdict)

    climate_context = {
        "mode":     climate_mode,
        "headline": climate_headline,
        "metrics":  climate_live["metrics"] if climate_live else {},
        "verdict":  climate_verdict,
    }

    body = {
        **{k: v for k, v in reading.items() if k not in ('station_id', 'ttl')},
        "condition": label,
        "pressure_trend": trend,
        "anomalies": anomalies,
        "baseline_source": baseline_source,
        "baseline_sample_count": int(baseline.get('sample_count', 0)) if baseline else 0,
        "station":               station.get('label', 'Midtown Manhattan, New York'),
        "updated_at":            reading.get('timestamp'),
        "data_stale":            data_stale,
        "data_age_minutes":      round(age_minutes) if age_minutes is not None else None,
        "quality_flag":          reading.get('quality_flag'),
        "comfort":               comfort,
        "percentile_rank":       pct_rank,
        "climate_context":       climate_context,
        "rain_probability":      rain_prob,
        "forecast":              forecast,
        "uhi_seasonal_curve":    uhi_seasonal,
        "station_records":       station_records,
        "daily_summary":         daily_summary,
        "nearby_stations":       nearby[:8],
        **uhi,
    }
    return _resp(200, body)


def _history(hours: int):
    station = get_secret(STATION_SECRET)
    mac = station['mac_address']
    table = get_table(READINGS_TABLE)

    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    # Paginate through all results (DynamoDB query returns max 1MB per call)
    items = []
    kwargs = dict(
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

    floated = [_floatify(r) for r in items]

    # Downsample for longer ranges to keep payload manageable
    # >7 days → daily averages, >24h → hourly averages, <=24h → raw (5-min)
    if hours > 168:
        floated = _downsample(floated, bucket_hours=24)
    elif hours > 24:
        floated = _downsample(floated, bucket_hours=1)

    # Attach per-slot baselines so the chart can draw the overlay
    floated = _attach_baselines(floated, mac)

    return _resp(200, {"readings": floated, "count": len(floated), "hours": hours})


def _downsample(readings: list, bucket_hours: int) -> list:
    """Average readings into time buckets. Returns one point per bucket."""
    from collections import defaultdict
    NUMERIC = ['tempf', 'feelsLike', 'humidity', 'dewPoint', 'windspeedmph',
               'windgustmph', 'baromrelin', 'solarradiation', 'uv',
               'hourlyrainin', 'dailyrainin', 'uhi_delta']
    buckets = defaultdict(list)
    for r in readings:
        ts = r.get('timestamp', '')
        try:
            dt = datetime.fromisoformat(ts)
            # Floor to bucket boundary
            bucket_start = dt.replace(
                hour=(dt.hour // bucket_hours) * bucket_hours,
                minute=0, second=0, microsecond=0
            )
            buckets[bucket_start.isoformat()].append(r)
        except Exception:
            continue

    result = []
    for ts in sorted(buckets):
        group = buckets[ts]
        avg = {'timestamp': ts}
        for field in NUMERIC:
            vals = [r[field] for r in group if r.get(field) is not None]
            avg[field] = round(sum(vals) / len(vals), 2) if vals else None
        result.append(avg)
    return result


def _fetch_forecast(mac: str) -> dict | None:
    """Read the latest pre-computed analog forecast and running accuracy."""
    try:
        table = get_table(FORECASTS_TABLE)
        resp  = table.get_item(Key={'station_id': mac})
        item  = resp.get('Item')
        if not item:
            return None
        f = _floatify(item)

        # Load running forecast accuracy (non-critical)
        accuracy = None
        try:
            acc_table = get_table(ACCURACY_TABLE)
            acc_resp  = acc_table.get_item(Key={'station_id': mac, 'evaluated_at': 'running'})
            acc_item  = acc_resp.get('Item')
            if acc_item:
                a     = _floatify(acc_item)
                count = int(a.get('evaluation_count', 0))
                mae_1 = a.get('mae_1h_tempf')
                if mae_1 is not None and count >= 5:
                    accuracy = {
                        'mae_1h_tempf':     round(mae_1, 1),
                        'evaluation_count': count,
                    }
        except Exception as ae:
            print(f"Accuracy fetch (non-fatal): {ae}")

        return {
            'computed_at':      f.get('computed_at'),
            'confidence':       f.get('confidence'),
            'best_match_label': f.get('best_match_label'),
            'analogs_found':    f.get('analogs_found'),
            'hours':            f.get('forecast', []),
            'accuracy':         accuracy,
        }
    except Exception as e:
        print(f"Forecast fetch failed: {e}")
        return None


def _fetch_uhi_seasonal(mac: str) -> list | None:
    """Return list of 12 monthly UHI averages from wx-uhi-seasonal."""
    try:
        table = get_table(UHI_SEASONAL_TABLE)
        resp  = table.query(
            KeyConditionExpression=Key('station_id').eq(mac),
        )
        items = [_floatify(i) for i in resp.get('Items', [])]
        if not items:
            return None

        result = []
        for item in sorted(items, key=lambda x: x.get('month', '00')):
            m = int(item.get('month', 0))
            if 1 <= m <= 12:
                result.append({
                    'month':      m,
                    'month_name': _MONTH_NAMES[m - 1],
                    'avg_delta':  round(float(item['avg_delta']), 1) if item.get('avg_delta') is not None else None,
                    'sample_count': int(item.get('sample_count', 0)),
                })
        return result or None
    except Exception as e:
        print(f"UHI seasonal fetch (non-fatal): {e}")
        return None


def _attach_baselines(readings: list, mac: str) -> list:
    """Attach baseline_* values to each reading bucket for chart overlay."""
    if not readings:
        return readings

    import boto3 as _boto3
    BASELINE_FIELDS = ['tempf', 'humidity', 'windspeedmph', 'baromrelin']

    # Collect unique month-hour keys (NY local time)
    mh_to_indices: dict = {}
    for i, r in enumerate(readings):
        try:
            dt = datetime.fromisoformat(r.get('timestamp', ''))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            local = dt.astimezone(STATION_TZ)
            mh = local.strftime('%m-%H')
            mh_to_indices.setdefault(mh, []).append(i)
        except Exception:
            pass

    if not mh_to_indices:
        return readings

    # Batch-fetch baselines (DynamoDB batch_get_item, max 100 per request)
    dynamo = _boto3.resource('dynamodb', region_name='us-east-1')
    all_keys = [{'station_id': mac, 'month_hour': mh} for mh in mh_to_indices]
    baseline_cache: dict = {}

    for i in range(0, len(all_keys), 100):
        resp = dynamo.batch_get_item(
            RequestItems={STATS_TABLE: {'Keys': all_keys[i:i + 100]}}
        )
        for item in resp.get('Responses', {}).get(STATS_TABLE, []):
            f = _floatify(item)
            baseline_cache[f['month_hour']] = f

    # Attach baseline mean and std to each reading
    for mh, indices in mh_to_indices.items():
        b = baseline_cache.get(mh, {})
        for idx in indices:
            for field in BASELINE_FIELDS:
                readings[idx][f'baseline_{field}']     = b.get(f'avg_{field}')
                readings[idx][f'baseline_std_{field}'] = b.get(f'std_{field}')

    return readings


def _fetch_station_records(mac: str, month: int) -> dict | None:
    """Fetch pre-computed records for the given calendar month."""
    try:
        table = get_table(RECORDS_TABLE)
        resp  = table.get_item(Key={'station_id': mac, 'month': str(month).zfill(2)})
        item  = resp.get('Item')
        if not item:
            return None
        f = _floatify(item)
        return {k: v for k, v in f.items() if k not in ('station_id',)}
    except Exception as e:
        print(f"Records fetch (non-fatal): {e}")
        return None


def _fetch_latest_summary(mac: str) -> dict | None:
    """Fetch the most recent daily summary."""
    try:
        table = get_table(SUMMARIES_TABLE)
        resp  = table.query(
            KeyConditionExpression=Key('station_id').eq(mac),
            ScanIndexForward=False,
            Limit=1,
        )
        items = resp.get('Items', [])
        if not items:
            return None
        f = _floatify(items[0])
        return {k: v for k, v in f.items() if k not in ('station_id',)}
    except Exception as e:
        print(f"Daily summary fetch (non-fatal): {e}")
        return None


def _detect_rain_events(readings: list) -> list:
    """Detect discrete rain events from a chronological reading list."""
    events, in_rain, start, rates = [], False, None, []
    for r in readings:
        rate = float(r.get('hourlyrainin') or 0)
        if rate > 0.01:
            if not in_rain:
                in_rain, start, rates = True, r.get('timestamp', ''), []
            rates.append(rate)
        elif in_rain:
            in_rain = False
            if rates:
                events.append({
                    'start':        start,
                    'peak_rate':    round(max(rates), 2),
                    'duration_min': len(rates) * 5,
                    'total_in':     round(sum(rates) * 5 / 60, 3),
                })
            rates = []
    if in_rain and rates:
        events.append({
            'start':        start,
            'peak_rate':    round(max(rates), 2),
            'duration_min': len(rates) * 5,
            'total_in':     round(sum(rates) * 5 / 60, 3),
        })
    return events


def _rain_events_route(days: int) -> dict:
    """Scan the last N days of readings and return detected rain events."""
    try:
        station = get_secret(STATION_SECRET)
        mac     = station['mac_address']
        table   = get_table(READINGS_TABLE)
        since   = (datetime.now(timezone.utc) - timedelta(hours=days * 24)).isoformat()
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

        readings = [_floatify(r) for r in items]
        events   = _detect_rain_events(readings)
        # Return newest first
        return _resp(200, {"events": list(reversed(events)), "days": days})
    except Exception as e:
        print(f"Rain events route error: {e}")
        return _resp(500, {"error": str(e)})


def _daily_summaries_route(days: int) -> dict:
    """Return precomputed daily summaries for the last N days."""
    try:
        station = get_secret(STATION_SECRET)
        mac     = station['mac_address']
        table   = get_table(SUMMARIES_TABLE)
        since   = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
        resp    = table.query(
            KeyConditionExpression=Key('station_id').eq(mac) & Key('date').gte(since),
            ScanIndexForward=True,
        )
        summaries = [
            {k: v for k, v in _floatify(item).items() if k != 'station_id'}
            for item in resp.get('Items', [])
        ]
        return _resp(200, {"summaries": summaries, "count": len(summaries)})
    except Exception as e:
        print(f"Daily summaries route error: {e}")
        return _resp(500, {"error": str(e)})


def _floatify(obj):
    if isinstance(obj, dict):
        return {k: _floatify(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_floatify(v) for v in obj]
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def _fetch_climate_doy(mac: str, doy: str) -> dict | None:
    """Fetch NOAA per-DOY stats from wx-climate-doy."""
    try:
        table = get_table(CLIMATE_DOY_TABLE)
        resp  = table.get_item(Key={"station_id": mac, "doy": doy})
        item  = resp.get("Item")
        return _floatify(item) if item else None
    except Exception as e:
        print(f"Climate DOY fetch (non-fatal): {e}")
        return None


def _fetch_climate_hourly(mac: str, doy_hour: str) -> dict | None:
    """Fetch ERA5 per-DOY-hour stats from wx-climate-hourly."""
    try:
        table = get_table(CLIMATE_HOURLY_TABLE)
        resp  = table.get_item(Key={"station_id": mac, "doy_hour": doy_hour})
        item  = resp.get("Item")
        return _floatify(item) if item else None
    except Exception as e:
        print(f"Climate hourly fetch (non-fatal): {e}")
        return None


def _resp(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": CORS_HEADERS,
        "body": json.dumps(body),
    }
