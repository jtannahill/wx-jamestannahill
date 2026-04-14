"""
wx-climate-updater: Nightly refresh of climate history tables.
Runs at 06:00 UTC (after wx_summarizer at 05:00 UTC).

Daily: Re-fetches ERA5 for yesterday's calendar date, recomputes hourly
  stats for that DOY and updates wx-climate-hourly (24 rows).

Monthly (1st of each month): Re-downloads NOAA CSV to pick up the ~2-week
  data lag, recomputes and updates all 366 rows of wx-climate-doy.
"""
import os
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal

from shared.secrets import get_secret
from shared.dynamodb import get_table
from wx_climate_bootstrap.noaa import fetch_noaa_csv, parse_noaa_csv, compute_doy_stats
from wx_climate_bootstrap.era5 import fetch_month_era5

CLIMATE_DOY_TABLE    = os.environ.get("CLIMATE_DOY_TABLE",    "wx-climate-doy")
CLIMATE_HOURLY_TABLE = os.environ.get("CLIMATE_HOURLY_TABLE", "wx-climate-hourly")
STATION_TZ           = ZoneInfo("America/New_York")


def _decimalize(obj):
    if isinstance(obj, dict):
        return {k: _decimalize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decimalize(v) for v in obj]
    if isinstance(obj, float):
        return Decimal(str(round(obj, 4)))
    if isinstance(obj, int):
        return Decimal(str(obj))
    return obj


def handler(event, context):
    station = get_secret("ambient-weather/station-config")
    mac     = station["mac_address"]
    lat     = float(station["latitude"])
    lon     = float(station["longitude"])

    now_et    = datetime.now(timezone.utc).astimezone(STATION_TZ)
    yesterday = (now_et - timedelta(days=1)).date()
    month     = yesterday.month
    doy       = f"{yesterday.month:02d}{yesterday.day:02d}"

    # ── Daily: refresh yesterday's DOY in ERA5 hourly table ──────────────────
    print(f"Updater: refreshing ERA5 for month {month:02d} (yesterday = {yesterday})")
    try:
        slots        = fetch_month_era5(lat, lon, month)
        hourly_table = get_table(CLIMATE_HOURLY_TABLE)
        updated      = 0
        for doy_hour, stats in slots.items():
            # Only update slots belonging to yesterday's DOY
            if not doy_hour.startswith(doy):
                continue
            hourly_table.put_item(Item={
                "station_id": mac,
                "doy_hour":   doy_hour,
                **_decimalize(stats),
            })
            updated += 1
        print(f"ERA5 updater: {updated} hourly slots refreshed for DOY {doy}")
    except Exception as e:
        print(f"ERA5 update failed (non-fatal): {e}")

    # ── Monthly (1st): refresh NOAA ───────────────────────────────────────────
    if now_et.day == 1:
        print("Updater: 1st of month — refreshing NOAA CSV...")
        try:
            csv_text  = fetch_noaa_csv()
            by_doy    = parse_noaa_csv(csv_text)
            doy_table = get_table(CLIMATE_DOY_TABLE)
            written   = 0
            for row_doy, records in by_doy.items():
                if not records:
                    continue
                stats = compute_doy_stats(records)
                doy_table.put_item(Item={
                    "station_id": mac,
                    "doy":        row_doy,
                    **_decimalize(stats),
                })
                written += 1
            print(f"NOAA updater: {written} DOY rows refreshed")
        except Exception as e:
            print(f"NOAA update failed (non-fatal): {e}")

    return {"status": "ok", "date": yesterday.isoformat()}
