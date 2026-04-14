"""
NOAA GHCN-Daily parser for Central Park station USC00305801.
Downloads CSV from NOAA CDN and parses per-DOY annual records.

Units in raw CSV:
  TMAX, TMIN: tenths of degrees Celsius  (e.g. 200 = 20.0°C = 68.0°F)
  AWND:       tenths of meters per second (e.g. 45  = 4.5 m/s  = 10.1 mph)
"""
import requests
from collections import defaultdict


NOAA_CSV_URL = (
    "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily"
    "/access/USC00305801.csv"
)


def _tenths_c_to_f(tenths_c: float) -> float:
    return tenths_c / 10 * 9 / 5 + 32


def _tenths_ms_to_mph(tenths_ms: float) -> float:
    return tenths_ms / 10 * 2.237


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    k = (p / 100) * (n - 1)
    lo = int(k)
    hi = lo + 1
    if hi >= n:
        return round(s[lo], 2)
    return round(s[lo] + (k - lo) * (s[hi] - s[lo]), 2)


def fetch_noaa_csv() -> str:
    """Download the Central Park GHCN-Daily CSV. Returns raw text."""
    resp = requests.get(NOAA_CSV_URL, timeout=60)
    resp.raise_for_status()
    return resp.text


def parse_noaa_csv(csv_text: str) -> dict[str, list[dict]]:
    """
    Parse GHCN-Daily CSV text into per-DOY annual records.

    Returns dict keyed by doy string ("0413"), values are lists of
    {year, tmax, tmin, awnd} sorted by year descending.
    TMAX/TMIN missing rows are skipped. AWND may be None.
    """
    import csv
    from io import StringIO

    by_doy: dict[str, list[dict]] = defaultdict(list)
    reader = csv.DictReader(StringIO(csv_text))

    for row in reader:
        date_str = row.get("DATE", "").strip()
        if not date_str or len(date_str) != 10:
            continue
        try:
            year = int(date_str[:4])
        except ValueError:
            continue

        doy = date_str[5:7] + date_str[8:10]  # "MMDD"

        tmax_raw = row.get("TMAX", "").strip()
        tmin_raw = row.get("TMIN", "").strip()
        awnd_raw = row.get("AWND", "").strip()

        if not tmax_raw or not tmin_raw:
            continue

        try:
            tmax_f = round(_tenths_c_to_f(float(tmax_raw)), 1)
            tmin_f = round(_tenths_c_to_f(float(tmin_raw)), 1)
        except ValueError:
            continue

        awnd_mph = None
        if awnd_raw:
            try:
                awnd_mph = round(_tenths_ms_to_mph(float(awnd_raw)), 1)
            except ValueError:
                pass

        by_doy[doy].append({"year": year, "tmax": tmax_f, "tmin": tmin_f, "awnd": awnd_mph})

    # Sort each DOY by year descending (for fast last_exceeded_year lookup)
    for doy in by_doy:
        by_doy[doy].sort(key=lambda r: r["year"], reverse=True)

    return dict(by_doy)


def compute_doy_stats(records: list[dict]) -> dict:
    """
    Compute percentile stats and record extremes for one DOY's annual records.

    Input: list of {year, tmax, tmin, awnd} for a single DOY.
    Returns: dict ready to write to wx-climate-doy (minus station_id/doy keys).
    """
    tmax_vals  = [r["tmax"]  for r in records if r.get("tmax")  is not None]
    tmin_vals  = [r["tmin"]  for r in records if r.get("tmin")  is not None]
    awnd_vals  = [r["awnd"]  for r in records if r.get("awnd")  is not None]

    def _percs(vals, prefix):
        return {
            f"p5_{prefix}":  _percentile(vals, 5),
            f"p25_{prefix}": _percentile(vals, 25),
            f"p50_{prefix}": _percentile(vals, 50),
            f"p75_{prefix}": _percentile(vals, 75),
            f"p95_{prefix}": _percentile(vals, 95),
        }

    record_high_temp, record_high_year = None, None
    if tmax_vals:
        record_high_temp = max(tmax_vals)
        record_high_year = next(r["year"] for r in records if r.get("tmax") == record_high_temp)

    record_low_temp, record_low_year = None, None
    if tmin_vals:
        record_low_temp = min(tmin_vals)
        record_low_year = next(r["year"] for r in records if r.get("tmin") == record_low_temp)

    record_high_wind, record_high_wind_year = None, None
    if awnd_vals:
        record_high_wind = max(awnd_vals)
        record_high_wind_year = next(r["year"] for r in records if r.get("awnd") == record_high_wind)

    return {
        **_percs(tmax_vals, "tmax"),
        **_percs(tmin_vals, "tmin"),
        **_percs(awnd_vals, "awnd"),
        "record_high_temp":      record_high_temp,
        "record_high_year":      record_high_year,
        "record_low_temp":       record_low_temp,
        "record_low_year":       record_low_year,
        "record_high_wind":      record_high_wind,
        "record_high_wind_year": record_high_wind_year,
        "sample_count":          len(tmax_vals),
        "annual_highs":          records,
    }
