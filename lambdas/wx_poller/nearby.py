"""
Fetch nearby Weather Underground (api.weather.com) PWS stations and
compute bearing/distance from the home station.
"""
import math
import requests

HOME_LAT = 40.7549
HOME_LON = -73.984
WU_NEARBY_URL = "https://api.weather.com/v2/pws/observations/nearby"


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
            WU_NEARBY_URL,
            params={
                'geocode': f'{HOME_LAT},{HOME_LON}',
                'limit': limit,
                'units': 'e',
                'format': 'json',
                'apiKey': api_key,
            },
            timeout=8,
        )
        resp.raise_for_status()
        observations = resp.json().get('observations', [])
    except Exception as e:
        print(f"WU nearby fetch failed (non-critical): {e}")
        return []

    results = []
    for obs in observations:
        imp = obs.get('imperial', {})
        lat = obs.get('lat')
        lon = obs.get('lon')
        if lat is None or lon is None:
            continue
        lat, lon = float(lat), float(lon)
        dist = _haversine_mi(HOME_LAT, HOME_LON, lat, lon)
        if dist < 0.05:          # exclude home station
            continue
        results.append({
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
            'rain_rate_in_hr': float(imp.get('precipRate') or 0.0),
            'pressure_in':     imp.get('pressure'),
            'observed_at':     obs.get('obsTimeLocal', ''),
        })

    results.sort(key=lambda s: s['distance_mi'])
    return results


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
