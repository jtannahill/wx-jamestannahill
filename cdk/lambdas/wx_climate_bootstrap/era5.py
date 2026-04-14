"""
ERA5 hourly fetcher via Open-Meteo Archive API.
Covers 1940–present at any lat/lon. Free, no auth required.

Strategy: fetch one full calendar month at a time across all years 1940–(current-1).
Each call returns hourly data for e.g. all Aprils 1940–2023. Group by MM-DD-HH,
compute p25/p50/p75/mean/std for temp, dewpoint, and wind.

Dew point is derived from temperature + relative humidity via the Magnus formula.
"""
import math
import requests
from collections import defaultdict
from datetime import date
import calendar as _cal

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
START_YEAR  = 1940
ERA5_VARIABLES = [
    "temperature_2m",
    "relativehumidity_2m",
    "windspeed_10m",
]


def _c_to_f(celsius: float) -> float:
    return celsius * 9 / 5 + 32


def _dewpoint_f(temp_c: float, rh: float) -> float:
    """Magnus formula: dew point in °F from temp (°C) and relative humidity (%)."""
    a, b = 17.625, 243.04
    alpha = math.log(rh / 100) + (a * temp_c) / (b + temp_c)
    dp_c = b * alpha / (a - alpha)
    return _c_to_f(dp_c)


def _percentile(values: list, p: float):
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


def _compute_stats(vals: list[float], prefix: str) -> dict:
    """Compute p25/p50/p75/mean/std for vals, keyed with the given prefix."""
    if not vals:
        return {f"p25_{prefix}": None, f"p50_{prefix}": None,
                f"p75_{prefix}": None, f"mean_{prefix}": None, f"std_{prefix}": None}
    mean = sum(vals) / len(vals)
    std = math.sqrt(sum((v - mean) ** 2 for v in vals) / len(vals))
    return {
        f"p25_{prefix}": _percentile(vals, 25),
        f"p50_{prefix}": _percentile(vals, 50),
        f"p75_{prefix}": _percentile(vals, 75),
        f"mean_{prefix}": round(mean, 2),
        f"std_{prefix}":  round(std, 2),
    }


def compute_hourly_stats(values: list[float]) -> dict:
    """
    Compute distribution stats for temperature values (°F) at a single DOY-hour slot.
    Returns dict with p25_tempf/p50_tempf/p75_tempf/mean_tempf/std_tempf/sample_count keys.
    """
    stats = _compute_stats(values, "tempf")
    stats["sample_count"] = len(values)
    return stats


def fetch_month_era5(lat: float, lon: float, month: int) -> dict:
    """
    Fetch all hourly ERA5 data for a calendar month across START_YEAR–(current_year-1).

    Returns dict keyed by doy_hour string ("0413-14"), value is
    {p25_tempf, p50_tempf, p75_tempf, mean_tempf, std_tempf,
     p25_dewptf, p50_dewptf, p75_dewptf, mean_dewptf, std_dewptf,
     p25_windmph, p50_windmph, p75_windmph, mean_windmph, std_windmph,
     sample_count}.
    """
    end_year   = date.today().year - 1
    last_day   = _cal.monthrange(end_year, month)[1]
    start_date = f"{START_YEAR}-{month:02d}-01"
    end_date   = f"{end_year}-{month:02d}-{last_day:02d}"

    resp = requests.get(
        ARCHIVE_URL,
        params={
            "latitude":         lat,
            "longitude":        lon,
            "start_date":       start_date,
            "end_date":         end_date,
            "hourly":           ",".join(ERA5_VARIABLES),
            "timezone":         "America/New_York",
            "temperature_unit": "celsius",   # keep as Celsius for dewpoint calc
            "windspeed_unit":   "mph",
        },
        timeout=120,
    )
    resp.raise_for_status()
    data   = resp.json()
    hourly = data.get("hourly", {})
    times  = hourly.get("time", [])

    temp_vals  = hourly.get("temperature_2m",     [])
    rh_vals    = hourly.get("relativehumidity_2m", [])
    wind_vals  = hourly.get("windspeed_10m",      [])

    # Accumulate per doy_hour slot
    slot_temps  = defaultdict(list)
    slot_dewpts = defaultdict(list)
    slot_winds  = defaultdict(list)

    for i, ts in enumerate(times):
        # ts like "1940-04-13T09:00"
        try:
            date_part, time_part = ts.split("T")
            mm   = date_part[5:7]
            dd   = date_part[8:10]
            hour = int(time_part[:2])
        except (ValueError, IndexError):
            continue

        doy_hour = f"{mm}{dd}-{hour:02d}"

        temp_c = temp_vals[i] if i < len(temp_vals) else None
        rh     = rh_vals[i]   if i < len(rh_vals)   else None
        wind   = wind_vals[i] if i < len(wind_vals)  else None

        if temp_c is not None:
            slot_temps[doy_hour].append(_c_to_f(temp_c))
            if rh is not None and rh > 0:
                slot_dewpts[doy_hour].append(_dewpoint_f(temp_c, rh))
        if wind is not None:
            slot_winds[doy_hour].append(wind)

    # Compute stats per slot
    result = {}
    all_slots = set(slot_temps) | set(slot_winds)

    for slot in all_slots:
        temps  = slot_temps.get(slot, [])
        dewpts = slot_dewpts.get(slot, [])
        winds  = slot_winds.get(slot, [])

        result[slot] = {
            **_compute_stats(temps,  "tempf"),
            **_compute_stats(dewpts, "dewptf"),
            **_compute_stats(winds,  "windmph"),
            "sample_count": max(len(temps), len(dewpts), len(winds)),
        }

    return result
