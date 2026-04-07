import os, json, time
import requests
from datetime import datetime, timezone
from shared.secrets import get_secret
from shared.dynamodb import get_table

READINGS_TABLE = os.environ.get('READINGS_TABLE', 'wx-readings')
STATS_TABLE = os.environ.get('STATS_TABLE', 'wx-daily-stats')
STAT_FIELDS = ['tempf', 'feelsLike', 'humidity', 'windspeedmph', 'baromrelin', 'uv']
MAX_SAMPLE_COUNT = 8640  # 30 days × 288 readings/day


def handler(event, context):
    creds = get_secret('ambient-weather/api-keys')
    station = get_secret('ambient-weather/station-config')
    mac = station['mac_address']

    reading = fetch_reading(mac, creds['api_key'], creds['application_key'])
    if not reading:
        print("No reading returned from Ambient API")
        return

    now = datetime.now(timezone.utc)
    _write_reading(mac, now, reading)
    month_hour = compute_month_hour(now)
    update_rolling_stats(get_table(STATS_TABLE), mac, month_hour, reading)
    print(f"Wrote reading for {mac} at {now.isoformat()}")


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
    return dt.strftime('%m-%H')  # e.g. "04-09"


def _write_reading(station_id: str, now: datetime, reading: dict):
    table = get_table(READINGS_TABLE)
    table.put_item(Item={
        'station_id': station_id,
        'timestamp': now.isoformat(),
        'tempf': _decimal(reading.get('tempf')),
        'feelsLike': _decimal(reading.get('feelsLike')),
        'humidity': _decimal(reading.get('humidity')),
        'dewPoint': _decimal(reading.get('dewPoint')),
        'windspeedmph': _decimal(reading.get('windspeedmph')),
        'windgustmph': _decimal(reading.get('windgustmph')),
        'winddir': _decimal(reading.get('winddir')),
        'baromrelin': _decimal(reading.get('baromrelin')),
        'solarradiation': _decimal(reading.get('solarradiation')),
        'uv': _decimal(reading.get('uv')),
        'hourlyrainin': _decimal(reading.get('hourlyrainin')),
        'dailyrainin': _decimal(reading.get('dailyrainin')),
        'ttl': int(time.time()) + (90 * 86400),
    })


def update_rolling_stats(table, station_id: str, month_hour: str, reading: dict):
    resp = table.get_item(Key={'station_id': station_id, 'month_hour': month_hour})
    item = resp.get('Item', {})
    count = int(item.get('sample_count', 0))

    new_vals = {f: float(reading.get(f) or 0) for f in STAT_FIELDS}

    if count == 0:
        avg_vals = {f'avg_{f}': new_vals[f] for f in STAT_FIELDS}
        new_count = 1
    elif count < MAX_SAMPLE_COUNT:
        avg_vals = {
            f'avg_{f}': (float(item.get(f'avg_{f}', 0)) * count + new_vals[f]) / (count + 1)
            for f in STAT_FIELDS
        }
        new_count = count + 1
    else:
        alpha = 1 / MAX_SAMPLE_COUNT
        avg_vals = {
            f'avg_{f}': alpha * new_vals[f] + (1 - alpha) * float(item.get(f'avg_{f}', 0))
            for f in STAT_FIELDS
        }
        new_count = MAX_SAMPLE_COUNT

    table.put_item(Item={
        'station_id': station_id,
        'month_hour': month_hour,
        **avg_vals,
        'sample_count': new_count,
        'source': 'station',
    })


def _decimal(val):
    if val is None:
        return None
    from decimal import Decimal
    return Decimal(str(val))
