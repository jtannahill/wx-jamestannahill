"""
Backfills historical readings from Ambient Weather API into wx-readings and wx-daily-stats.
Run once from local machine: python3 scripts/backfill.py

Ambient Weather stores up to 1 year of history. The API returns up to 288 readings per
call, paginated backwards using the endTime parameter (milliseconds since epoch).
"""
import sys, json, time, boto3, requests
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, 'lambdas')
from shared.secrets import get_secret
from shared.dynamodb import get_table

READINGS_TABLE = 'wx-readings'
STATS_TABLE = 'wx-daily-stats'
STAT_FIELDS = ['tempf', 'feelsLike', 'humidity', 'windspeedmph', 'baromrelin', 'uv']
MAX_SAMPLE_COUNT = 8640
BATCH_LIMIT = 288
RATE_LIMIT_SLEEP = 1.1  # Ambient Weather rate limit: 1 req/sec


def _decimal(val):
    if val is None:
        return None
    return Decimal(str(val))


def fetch_batch(mac, api_key, app_key, end_time_ms=None):
    params = {
        'apiKey': api_key,
        'applicationKey': app_key,
        'limit': BATCH_LIMIT,
    }
    if end_time_ms:
        params['endTime'] = end_time_ms
    resp = requests.get(
        f"https://api.ambientweather.net/v1/devices/{mac}",
        params=params,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def write_reading(table, station_id, reading):
    # Ambient Weather timestamps are in ms epoch in 'dateutc' field
    ts_ms = reading.get('dateutc')
    if not ts_ms:
        return
    ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()

    table.put_item(Item={
        'station_id': station_id,
        'timestamp': ts,
        'tempf':         _decimal(reading.get('tempf')),
        'feelsLike':     _decimal(reading.get('feelsLike')),
        'humidity':      _decimal(reading.get('humidity')),
        'dewPoint':      _decimal(reading.get('dewPoint')),
        'windspeedmph':  _decimal(reading.get('windspeedmph')),
        'windgustmph':   _decimal(reading.get('windgustmph')),
        'winddir':       _decimal(reading.get('winddir')),
        'baromrelin':    _decimal(reading.get('baromrelin')),
        'solarradiation':_decimal(reading.get('solarradiation')),
        'uv':            _decimal(reading.get('uv')),
        'hourlyrainin':  _decimal(reading.get('hourlyrainin')),
        'dailyrainin':   _decimal(reading.get('dailyrainin')),
        'ttl': int(time.time()) + (90 * 86400),
    })
    return ts


def update_stats(stats_table, station_id, reading, ts):
    if reading.get('tempf') is None:
        return
    dt = datetime.fromisoformat(ts)
    month_hour = dt.strftime('%m-%H')

    resp = stats_table.get_item(Key={'station_id': station_id, 'month_hour': month_hour})
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

    stats_table.put_item(Item={
        'station_id': station_id,
        'month_hour': month_hour,
        **{k: _decimal(v) for k, v in avg_vals.items()},
        'sample_count': new_count,
        'source': 'station',
    })


def main():
    creds = get_secret('ambient-weather/api-keys')
    station = get_secret('ambient-weather/station-config')
    mac = station['mac_address']

    readings_table = get_table(READINGS_TABLE)
    stats_table = get_table(STATS_TABLE)

    total = 0
    end_time_ms = None
    oldest_ts = None

    print(f"Backfilling historical readings for {mac}...")

    while True:
        batch = fetch_batch(mac, creds['api_key'], creds['application_key'], end_time_ms)
        if not batch:
            print("No more data.")
            break

        # Ambient returns newest-first; process oldest-first for correct rolling avg order
        batch_sorted = sorted(batch, key=lambda r: r.get('dateutc', 0))

        for reading in batch_sorted:
            ts = write_reading(readings_table, mac, reading)
            if ts:
                update_stats(stats_table, mac, reading, ts)
                oldest_ts = ts
                total += 1

        print(f"  Wrote {len(batch_sorted)} readings (oldest so far: {oldest_ts}, total: {total})")

        if len(batch) < BATCH_LIMIT:
            print("Reached end of available history.")
            break

        # Next page: endTime = oldest reading in this batch - 1ms
        oldest_in_batch = min(r.get('dateutc', 0) for r in batch)
        if end_time_ms and oldest_in_batch >= end_time_ms:
            print("No older data available — station history starts here.")
            break
        end_time_ms = oldest_in_batch - 1
        time.sleep(RATE_LIMIT_SLEEP)

    print(f"\nDone. Wrote {total} readings. Oldest: {oldest_ts}")


if __name__ == '__main__':
    main()
