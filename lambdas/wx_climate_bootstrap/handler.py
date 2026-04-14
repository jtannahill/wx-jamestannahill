"""
wx-climate-bootstrap: One-time (re-runnable) bootstrap for climate history tables.

Step 1 — NOAA GHCN-Daily: Downloads Central Park CSV (~2MB), parses all rows
  back to 1869, writes 366 rows to wx-climate-doy.

Step 2 — ERA5: Fetches full monthly hourly data 1940–present (12 API calls),
  writes ~8,784 rows to wx-climate-hourly.

Idempotent: uses put_item (overwrites existing rows safely).
Config: 1024MB, 900s timeout, arm64.
"""
import os
from decimal import Decimal
from shared.secrets import get_secret
from shared.dynamodb import get_table
from wx_climate_bootstrap.noaa import fetch_noaa_csv, parse_noaa_csv, compute_doy_stats
from wx_climate_bootstrap.era5 import fetch_month_era5

CLIMATE_DOY_TABLE    = os.environ.get("CLIMATE_DOY_TABLE",    "wx-climate-doy")
CLIMATE_HOURLY_TABLE = os.environ.get("CLIMATE_HOURLY_TABLE", "wx-climate-hourly")


def _decimalize(obj):
    """Recursively convert floats/ints to Decimal for DynamoDB."""
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

    run_noaa = event.get("noaa", True)
    run_era5 = event.get("era5", True)

    # ── Step 1: NOAA ─────────────────────────────────────────────────────────
    if run_noaa:
        print("NOAA: downloading Central Park CSV...")
        csv_text = fetch_noaa_csv()
        by_doy   = parse_noaa_csv(csv_text)
        print(f"NOAA: parsed {len(by_doy)} calendar dates")

        doy_table = get_table(CLIMATE_DOY_TABLE)
        written   = 0
        for doy, records in by_doy.items():
            if not records:
                continue
            stats = compute_doy_stats(records)
            item  = {
                "station_id": mac,
                "doy":        doy,
                **_decimalize(stats),
            }
            doy_table.put_item(Item=item)
            written += 1
        print(f"NOAA: wrote {written} rows to {CLIMATE_DOY_TABLE}")

    # ── Step 2: ERA5 ─────────────────────────────────────────────────────────
    if run_era5:
        hourly_table  = get_table(CLIMATE_HOURLY_TABLE)
        total_written = 0

        for month in range(1, 13):
            print(f"ERA5: fetching month {month:02d}...")
            try:
                slots = fetch_month_era5(lat, lon, month)
            except Exception as e:
                print(f"ERA5: month {month:02d} failed — {e}")
                continue

            for doy_hour, stats in slots.items():
                item = {
                    "station_id": mac,
                    "doy_hour":   doy_hour,
                    **_decimalize(stats),
                }
                hourly_table.put_item(Item=item)
                total_written += 1

            print(f"ERA5: month {month:02d} → {len(slots)} slots written")

        print(f"ERA5: total {total_written} rows written to {CLIMATE_HOURLY_TABLE}")

    return {"status": "ok", "noaa": run_noaa, "era5": run_era5}
