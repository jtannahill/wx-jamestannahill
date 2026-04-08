import os
from shared.secrets import get_secret
from shared.dynamodb import get_table
from wx_bootstrap.openmeteo import fetch_month_normals

STATS_TABLE = os.environ.get('STATS_TABLE', 'wx-daily-stats')

MONTHS = list(range(1, 13))


def handler(event, context):
    station = get_secret('ambient-weather/station-config')
    lat = station['latitude']
    lon = station['longitude']
    station_id = station['mac_address']

    stats_table = get_table(STATS_TABLE)

    for month in MONTHS:
        print(f"Fetching Open-Meteo ERA5 normals for month {month:02d}...")
        try:
            hourly_normals = fetch_month_normals(lat, lon, month)
        except Exception as e:
            print(f"Open-Meteo failed for month {month:02d}: {e}")
            continue

        for hour, normals in hourly_normals.items():
            month_hour = f"{month:02d}-{hour:02d}"

            # Seed wx-daily-stats only if slot is empty (don't overwrite real station data)
            stats_resp = stats_table.get_item(
                Key={'station_id': station_id, 'month_hour': month_hour}
            )
            if 'Item' not in stats_resp:
                stats_table.put_item(Item={
                    'station_id': station_id,
                    'month_hour': month_hour,
                    **{k: _decimal(v) for k, v in normals.items()},
                    'sample_count': 288,  # Weight ERA5 as ~1 day of readings so real data blends in gradually
                    'source': 'open-meteo-era5',
                })

        print(f"Month {month:02d}: seeded {len(hourly_normals)} hour slots")

    print("Bootstrap complete.")


def _decimal(val):
    from decimal import Decimal
    return Decimal(str(val)) if val is not None else None
