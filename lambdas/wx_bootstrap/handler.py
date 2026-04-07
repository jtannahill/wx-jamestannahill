import os
from shared.secrets import get_secret
from shared.dynamodb import get_table
from wx_bootstrap.weatherkit import build_jwt, fetch_historical_comparisons, parse_comparisons

BASELINES_TABLE = os.environ.get('BASELINES_TABLE', 'wx-baselines')
STATS_TABLE = os.environ.get('STATS_TABLE', 'wx-daily-stats')

MONTHS = [f"{m:02d}" for m in range(1, 13)]


def handler(event, context):
    creds = get_secret('weatherkit/credentials')
    station = get_secret('ambient-weather/station-config')

    token = build_jwt(
        team_id=creds['team_id'],
        key_id=creds['key_id'],
        service_id=creds['service_id'],
        private_key_pem=creds['private_key'],
    )

    lat = station['latitude']
    lon = station['longitude']
    station_id = station['mac_address']
    baselines_table = get_table(BASELINES_TABLE)
    stats_table = get_table(STATS_TABLE)

    for month in MONTHS:
        print(f"Fetching WeatherKit baselines for month {month}...")
        try:
            response = fetch_historical_comparisons(lat, lon, token)
            averages = parse_comparisons(response)
        except Exception as e:
            print(f"WeatherKit failed for month {month}: {e}")
            continue

        baselines_table.put_item(Item={
            'station_id': station_id,
            'month': month,
            **{k: _decimal(v) for k, v in averages.items()},
        })

        for hour in range(24):
            month_hour = f"{month}-{hour:02d}"
            stats_resp = stats_table.get_item(
                Key={'station_id': station_id, 'month_hour': month_hour}
            )
            if 'Item' not in stats_resp:
                stats_table.put_item(Item={
                    'station_id': station_id,
                    'month_hour': month_hour,
                    'avg_tempf': _decimal(averages['avg_tempf']),
                    'avg_feelsLike': _decimal(averages['avg_tempf']),
                    'avg_humidity': _decimal(averages['avg_humidity']),
                    'avg_windspeedmph': _decimal(averages.get('avg_windspeedmph', 0)),
                    'avg_baromrelin': _decimal(29.92),
                    'avg_uv': _decimal(0),
                    'sample_count': 0,
                    'source': 'weatherkit',
                })
        print(f"Month {month}: seeded baselines + 24 wx-daily-stats slots")

    print("Bootstrap complete.")


def _decimal(val):
    from decimal import Decimal
    return Decimal(str(val)) if val is not None else None
