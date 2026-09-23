"""NWS active weather alerts for the station coordinates."""
import json
import urllib.request

_POINT = '40.7549,-73.984'
_URL = f'https://api.weather.gov/alerts/active?point={_POINT}'
_UA = 'wx.jamestannahill.com (james@jamestannahill.com)'


def fetch_active_alerts() -> list[dict]:
    """
    Return simplified active NWS alerts affecting the station point.
    Returns [] on any error or when no alerts are in effect.
    """
    req = urllib.request.Request(
        _URL,
        headers={'User-Agent': _UA, 'Accept': 'application/geo+json'},
    )
    try:
        with urllib.request.urlopen(req, timeout=6) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"NWS alerts fetch failed (non-critical): {e}")
        return []

    alerts = []
    for feature in data.get('features', []):
        props = feature.get('properties') or {}
        if props.get('status') != 'Actual':
            continue
        alerts.append({
            'event':       props.get('event'),
            'headline':    props.get('headline'),
            'severity':    props.get('severity'),
            'urgency':     props.get('urgency'),
            'description': (props.get('description') or '')[:280],
            'expires':     props.get('expires'),
            'url':         props.get('@id'),
        })
    return alerts
