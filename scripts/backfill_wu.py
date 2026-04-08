"""
Backfills historical readings from Weather Underground PWS history into wx-readings and wx-daily-stats.
Run: PYTHONPATH=lambdas python3 scripts/backfill_wu.py

WU stores up to ~2 years of PWS history at 5-min resolution.
We fetch day by day, going back up to 90 days (our DynamoDB TTL).
"""
import sys, json, time, boto3, requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal

sys.path.insert(0, 'lambdas')
from shared.secrets import get_secret
from shared.dynamodb import get_table

WU_API_KEY = '517b29c7000d4e68bb29c7000dae680a'
WU_STATION = 'KNYNEWYO2140'
READINGS_TABLE = 'wx-readings'
STATS_TABLE = 'wx-daily-stats'
STAT_FIELDS = ['tempf', 'feelsLike', 'humidity', 'windspeedmph', 'baromrelin', 'uv']
MAX_SAMPLE_COUNT = 8640
STATION_TZ = ZoneInfo('America/New_York')
DAYS_BACK = 90


def _decimal(val):
    if val is None:
        return None
    return Decimal(str(val))


def _feels_like(obs):
    imp = obs.get('imperial', {})
    # Use heat index above 80°F, wind chill below 50°F, otherwise temp
    temp = imp.get('tempAvg')
    if temp is None:
        return None
    if temp >= 80:
        return imp.get('heatindexAvg', temp)
    if temp <= 50:
        return imp.get('windchillAvg', temp)
    return temp


def fetch_day(date_str):
    """date_str: YYYYMMDD"""
    resp = requests.get(
        'https://api.weather.com/v2/pws/history/all',
        params={
            'stationId': WU_STATION,
            'format': 'json',
            'units': 'e',
            'date': date_str,
            'apiKey': WU_API_KEY,
        },
        timeout=15,
    )
    if resp.status_code == 204:
        return []
    resp.raise_for_status()
    return resp.json().get('observations', [])


def write_reading(table, station_id, obs):
    ts_utc = obs.get('obsTimeUtc')
    if not ts_utc:
        return None
    try:
        dt = datetime.fromisoformat(ts_utc.replace('Z', '+00:00'))
    except Exception:
        return None
    ts = dt.isoformat()

    imp = obs.get('imperial', {})
    table.put_item(Item={
        'station_id': station_id,
        'timestamp': ts,
        'tempf':          _decimal(imp.get('tempAvg')),
        'feelsLike':      _decimal(_feels_like(obs)),
        'humidity':       _decimal(obs.get('humidityAvg')),
        'dewPoint':       _decimal(imp.get('dewptAvg')),
        'windspeedmph':   _decimal(imp.get('windspeedAvg')),
        'windgustmph':    _decimal(imp.get('windgustHigh')),
        'winddir':        _decimal(obs.get('winddrctAvg')),
        'baromrelin':     _decimal(round((imp.get('pressureMax', 0) + imp.get('pressureMin', 0)) / 2, 3) if imp.get('pressureMax') else None),
        'solarradiation': _decimal(obs.get('solarRadiationHigh')),
        'uv':             _decimal(obs.get('uvHigh')),
        'hourlyrainin':   _decimal(imp.get('precipRate')),
        'dailyrainin':    _decimal(imp.get('precipTotal')),
        'ttl': int(time.time()) + (90 * 86400),
    })
    return ts, dt


def update_stats(stats_table, station_id, obs, dt):
    imp = obs.get('imperial', {})
    if imp.get('tempAvg') is None:
        return

    local = dt.astimezone(STATION_TZ)
    month_hour = local.strftime('%m-%H')

    resp = stats_table.get_item(Key={'station_id': station_id, 'month_hour': month_hour})
    item = resp.get('Item', {})
    count = int(item.get('sample_count', 0))

    new_vals = {
        'tempf':        float(imp.get('tempAvg') or 0),
        'feelsLike':    float(_feels_like(obs) or 0),
        'humidity':     float(obs.get('humidityAvg') or 0),
        'windspeedmph': float(imp.get('windspeedAvg') or 0),
        'baromrelin':   float(((imp.get('pressureMax', 0) + imp.get('pressureMin', 0)) / 2) if imp.get('pressureMax') else 0),
        'uv':           float(obs.get('uvHigh') or 0),
    }

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
    station = get_secret('ambient-weather/station-config')
    station_id = station['mac_address']

    readings_table = get_table(READINGS_TABLE)
    stats_table = get_table(STATS_TABLE)

    today = datetime.now(timezone.utc).date()
    total = 0
    empty_days = 0

    for days_ago in range(1, DAYS_BACK + 1):
        day = today - timedelta(days=days_ago)
        date_str = day.strftime('%Y%m%d')

        try:
            obs_list = fetch_day(date_str)
        except Exception as e:
            print(f"  {date_str}: fetch failed ({e})")
            time.sleep(1)
            continue

        if not obs_list:
            empty_days += 1
            if empty_days >= 3:
                print(f"  3 consecutive empty days — reached start of station history.")
                break
            print(f"  {date_str}: no data")
            continue

        empty_days = 0
        day_count = 0
        for obs in obs_list:
            result = write_reading(readings_table, station_id, obs)
            if result:
                ts, dt = result
                update_stats(stats_table, station_id, obs, dt)
                day_count += 1

        total += day_count
        print(f"  {date_str}: {day_count} readings  (total: {total})")
        time.sleep(0.5)

    print(f"\nDone. Wrote {total} readings across {DAYS_BACK} days.")


if __name__ == '__main__':
    main()
