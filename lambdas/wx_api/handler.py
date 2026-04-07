import os, json
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from shared.secrets import get_secret
from shared.dynamodb import get_table
from wx_api.anomaly import compute_anomalies, pressure_trend, condition_label
from boto3.dynamodb.conditions import Key

READINGS_TABLE = os.environ.get('READINGS_TABLE', 'wx-readings')
STATS_TABLE = os.environ.get('STATS_TABLE', 'wx-daily-stats')
STATION_SECRET = 'ambient-weather/station-config'

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
        return _history(min(hours, 72))
    else:
        return _resp(404, {"error": "Not found"})


def _current():
    station = get_secret(STATION_SECRET)
    mac = station['mac_address']
    table = get_table(READINGS_TABLE)

    result = table.query(
        KeyConditionExpression=Key('station_id').eq(mac),
        ScanIndexForward=False,
        Limit=5,
    )
    items = result.get('Items', [])
    if not items:
        return _resp(503, {"error": "No data available"})

    reading = _floatify(items[0])
    recent = [_floatify(r) for r in items]

    now = datetime.now(timezone.utc)
    month_hour = now.strftime('%m-%H')
    stats_table = get_table(STATS_TABLE)
    stats_resp = stats_table.get_item(Key={'station_id': mac, 'month_hour': month_hour})
    baseline = _floatify(stats_resp.get('Item', {}))

    anomalies = compute_anomalies(reading, baseline, now.month, now.hour) if baseline else {}
    trend = pressure_trend(recent)
    label = condition_label(reading)
    baseline_source = baseline.get('source', 'none') if baseline else 'none'

    body = {
        **{k: v for k, v in reading.items() if k not in ('station_id', 'ttl')},
        "condition": label,
        "pressure_trend": trend,
        "anomalies": anomalies,
        "baseline_source": baseline_source,
        "station": station.get('label', 'Midtown Manhattan, New York'),
        "updated_at": reading.get('timestamp'),
    }
    return _resp(200, body)


def _history(hours: int):
    station = get_secret(STATION_SECRET)
    mac = station['mac_address']
    table = get_table(READINGS_TABLE)

    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    result = table.query(
        KeyConditionExpression=Key('station_id').eq(mac) & Key('timestamp').gte(since),
        ScanIndexForward=True,
    )
    items = [_floatify(r) for r in result.get('Items', [])]
    return _resp(200, {"readings": items, "count": len(items)})


def _floatify(obj):
    if isinstance(obj, dict):
        return {k: _floatify(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def _resp(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": CORS_HEADERS,
        "body": json.dumps(body),
    }
