import json
import urllib.request
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_FORECAST_URL = 'https://api.weather.gov/gridpoints/OKX/34,37/forecast'
_UA = 'wx.jamestannahill.com (james@jamestannahill.com)'
_TZ = ZoneInfo('America/New_York')


def fetch_tomorrow_forecast() -> dict | None:
    """
    Fetch tomorrow's daytime NWS forecast period for Midtown Manhattan (OKX 34,37).
    Returns dict or None on any error.
    """
    req = urllib.request.Request(
        _FORECAST_URL,
        headers={'User-Agent': _UA, 'Accept': 'application/geo+json'},
    )
    with urllib.request.urlopen(req, timeout=6) as resp:
        data = json.loads(resp.read())

    tomorrow = (datetime.now(_TZ) + timedelta(days=1)).date()
    for period in data['properties']['periods']:
        if not period.get('isDaytime', False):
            continue
        start = datetime.fromisoformat(period['startTime']).astimezone(_TZ)
        if start.date() == tomorrow:
            return {
                'name':      period['name'],          # e.g. "Thursday"
                'temp_high': period['temperature'],   # °F
                'wind':      period.get('windSpeed', ''),
                'wind_dir':  period.get('windDirection', ''),
                'short':     period['shortForecast'],
                'detailed':  period['detailedForecast'],
            }
    return None
