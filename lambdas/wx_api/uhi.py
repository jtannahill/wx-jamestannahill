"""
Urban Heat Island Differential.
Fetches current METAR temps from JFK/LGA/EWR via NOAA aviationweather.gov
and computes the delta between this Midtown station and the airport average.
Uses urllib (no extra deps) with a short timeout so it never blocks the API.
"""
import json
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

_ASOS_URL = (
    'https://aviationweather.gov/api/data/metar'
    '?ids=KJFK,KLGA,KEWR&format=json&taf=false'
)


def fetch_uhi(station_tempf: float) -> dict:
    """
    Returns dict with uhi_delta, uhi_airport_avg_f, uhi_label.
    Returns {} on any failure — UHI is non-critical; never raise.
    """
    try:
        req  = Request(_ASOS_URL, headers={'User-Agent': 'wx.jamestannahill.com/1.0'})
        with urlopen(req, timeout=4) as resp:
            data = json.loads(resp.read().decode())

        if not isinstance(data, list) or not data:
            return {}

        temps_f = []
        for obs in data:
            tc = obs.get('temp')
            if tc is not None:
                temps_f.append(float(tc) * 9 / 5 + 32)

        if not temps_f:
            return {}

        airport_avg = round(sum(temps_f) / len(temps_f), 1)
        delta       = round(station_tempf - airport_avg, 1)
        direction   = 'warmer' if delta >= 0 else 'cooler'

        return {
            'uhi_delta':          delta,
            'uhi_airport_avg_f':  airport_avg,
            'uhi_label':          f"{abs(delta):.1f}°F {direction} than airports",
        }

    except (URLError, HTTPError, OSError) as e:
        print(f"UHI fetch failed: {e}")
        return {}
    except Exception as e:
        print(f"UHI unexpected error: {e}")
        return {}
