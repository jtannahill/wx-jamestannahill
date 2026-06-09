"""
Fetch nearby Weather Underground (api.weather.com) PWS stations and
compute bearing/distance from the home station.

The old v2/pws/observations/nearby endpoint now returns 401 for PWS keys,
so this is a two-step flow against endpoints the key still authorizes:

  1. v3/location/near (product=pws)        -> nearest station IDs
  2. v2/pws/observations/current (per ID)  -> live observation, fetched
     concurrently; dead stations answer 204 and are skipped.
"""
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

HOME_LAT = 40.7549
HOME_LON = -73.984
WU_NEAR_URL    = "https://api.weather.com/v3/location/near"
WU_CURRENT_URL = "https://api.weather.com/v2/pws/observations/current"
MAX_OBS_AGE_SEC = 2 * 3600  # ignore observations older than 2 hours


def _safe_float(val, default=0.0):
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def fetch_nearby(api_key: str, limit: int = 20) -> list[dict]:
    """
    Fetch nearby WU stations. Returns a list of normalized dicts, sorted
    by distance, excluding the home station itself.
    Returns [] on any error — this is a non-critical enrichment.

    Each dict has:
      station_id, neighborhood, lat, lon, bearing_deg, distance_mi,
      temp_f, humidity, wind_speed_mph, wind_dir, rain_rate_in_hr,
      pressure_in, observed_at
    """
    try:
        resp = requests.get(
            WU_NEAR_URL,
            params={
                'geocode': f'{HOME_LAT},{HOME_LON}',
                'product': 'pws',
                'format': 'json',
                'apiKey': api_key,
            },
            timeout=8,
        )
        resp.raise_for_status()
        location = resp.json().get('location', {})
    except Exception as e:
        print(f"WU near fetch failed (non-critical): {e}")
        return []

    station_ids = [sid for sid in (location.get('stationId') or []) if sid]
    station_ids = station_ids[:limit]
    if not station_ids:
        return []

    results = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(_fetch_station_obs, api_key, sid) for sid in station_ids]
        for f in as_completed(futures):
            obs = f.result()
            if obs:
                results.append(obs)

    results.sort(key=lambda s: s['distance_mi'])
    return results


def _fetch_station_obs(api_key: str, station_id: str) -> dict | None:
    """Fetch one station's current observation. Returns None on 204/stale/error."""
    try:
        resp = requests.get(
            WU_CURRENT_URL,
            params={
                'stationId': station_id,
                'format': 'json',
                'units': 'e',
                'apiKey': api_key,
            },
            timeout=8,
        )
        if resp.status_code == 204:  # station registered but not reporting
            return None
        resp.raise_for_status()
        observations = resp.json().get('observations') or []
        if not observations:
            return None
        return _normalize_obs(observations[0])
    except Exception as e:
        print(f"WU nearby: station {station_id} fetch failed (non-critical): {e}")
        return None


def _normalize_obs(obs: dict) -> dict | None:
    """Normalize a WU current observation. Returns None if unusable."""
    try:
        imp = obs.get('imperial', {})
        lat = obs.get('lat')
        lon = obs.get('lon')
        if lat is None or lon is None:
            return None
        lat, lon = float(lat), float(lon)

        dist = _haversine_mi(HOME_LAT, HOME_LON, lat, lon)
        if dist < 0.05:  # exclude the home station itself
            return None

        if _obs_is_stale(obs.get('obsTimeUtc')):
            return None

        return {
            'station_id':      obs.get('stationID', ''),
            'neighborhood':    obs.get('neighborhood', ''),
            'lat':             round(lat, 4),
            'lon':             round(lon, 4),
            'bearing_deg':     round(_bearing(HOME_LAT, HOME_LON, lat, lon), 1),
            'distance_mi':     round(dist, 2),
            'temp_f':          imp.get('temp'),
            'humidity':        obs.get('humidity'),
            'wind_speed_mph':  imp.get('windSpeed'),
            'wind_dir':        obs.get('winddir'),
            'rain_rate_in_hr': _safe_float(imp.get('precipRate'), 0.0),
            'pressure_in':     imp.get('pressure'),
            'observed_at':     obs.get('obsTimeLocal', ''),
        }
    except Exception as e:
        print(f"WU nearby: skipping malformed observation: {e}")
        return None


def _obs_is_stale(obs_time_utc: str | None) -> bool:
    """True if the observation timestamp is older than MAX_OBS_AGE_SEC."""
    if not obs_time_utc:
        return False  # no timestamp — keep the observation
    try:
        dt = datetime.fromisoformat(str(obs_time_utc).replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - dt).total_seconds()
        return age > MAX_OBS_AGE_SEC
    except Exception:
        return False


def _bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compass bearing in degrees (0=N, 90=E) from (lat1,lon1) to (lat2,lon2)."""
    dlon  = math.radians(lon2 - lon1)
    lat1r = math.radians(lat1)
    lat2r = math.radians(lat2)
    x = math.sin(dlon) * math.cos(lat2r)
    y = math.cos(lat1r) * math.sin(lat2r) - math.sin(lat1r) * math.cos(lat2r) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _haversine_mi(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in miles."""
    R    = 3958.8
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))
