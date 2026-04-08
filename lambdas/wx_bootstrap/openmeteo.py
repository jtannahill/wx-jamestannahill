"""
Fetches historical climate normals from the Open-Meteo Archive API (ERA5 reanalysis).
Free, no auth required. Covers 1940–present at any lat/lon.

Strategy: for each of the 12 months, fetch the 14th of that month for each of
2019–2023 (5 years). Compute per-hour averages across those 5 days to get
rough climate normals for each month×hour bucket.
"""
import requests
from collections import defaultdict

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

VARIABLES = [
    "temperature_2m",        # → avg_tempf
    "apparent_temperature",  # → avg_feelsLike
    "relativehumidity_2m",   # → avg_humidity
    "windspeed_10m",         # → avg_windspeedmph (km/h → mph)
    "surface_pressure",      # → avg_baromrelin (hPa → inHg)
    "shortwave_radiation",   # → avg_solarradiation (W/m²)
]

YEARS = [2019, 2020, 2021, 2022, 2023]


def _c_to_f(c: float) -> float:
    return c * 9 / 5 + 32


def _kmh_to_mph(kmh: float) -> float:
    return kmh * 0.621371


def _hpa_to_inhg(hpa: float) -> float:
    return hpa * 0.02953


def fetch_month_normals(lat: float, lon: float, month: int) -> dict[int, dict]:
    """
    Returns a dict keyed by hour (0-23), each value being a dict of averaged fields.
    Averages across YEARS for the 14th of `month`.
    """
    # Fetch one multi-year request per month (5 separate dates)
    hourly_by_hour: dict[int, list[dict]] = defaultdict(list)

    for year in YEARS:
        date = f"{year}-{month:02d}-14"
        resp = requests.get(
            ARCHIVE_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "start_date": date,
                "end_date": date,
                "hourly": ",".join(VARIABLES),
                "timezone": "America/New_York",
                "temperature_unit": "fahrenheit",
                "windspeed_unit": "mph",
                "precipitation_unit": "inch",
            },
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])

        for i, ts in enumerate(times):
            # ts like "2021-04-14T09:00"
            hour = int(ts.split("T")[1].split(":")[0])
            reading = {}
            for var in VARIABLES:
                val = hourly.get(var, [None] * 24)[i]
                reading[var] = val
            hourly_by_hour[hour].append(reading)

    # Average across years for each hour
    result = {}
    for hour in range(24):
        samples = hourly_by_hour.get(hour, [])
        if not samples:
            continue

        def avg(field):
            vals = [s[field] for s in samples if s.get(field) is not None]
            return sum(vals) / len(vals) if vals else None

        temp_f = avg("temperature_2m")   # already °F (requested fahrenheit)
        feels_f = avg("apparent_temperature")  # already °F
        wind_mph = avg("windspeed_10m")  # already mph (requested mph)
        pressure_hpa = avg("surface_pressure")

        result[hour] = {
            "avg_tempf": round(temp_f, 2) if temp_f is not None else None,
            "avg_feelsLike": round(feels_f, 2) if feels_f is not None else None,
            "avg_humidity": round(avg("relativehumidity_2m"), 1) if avg("relativehumidity_2m") else None,
            "avg_windspeedmph": round(wind_mph, 2) if wind_mph is not None else None,
            "avg_baromrelin": round(_hpa_to_inhg(pressure_hpa), 3) if pressure_hpa else None,
            "avg_solarradiation": round(avg("shortwave_radiation"), 1) if avg("shortwave_radiation") else None,
            "avg_uv": 0,  # ERA5 doesn't have UV; station will fill this in
        }

    return result
