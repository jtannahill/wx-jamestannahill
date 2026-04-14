"""
WeatherKit REST API client — tomorrow's forecast narrative for wx.jamestannahill.com.

Uses ES256 JWT auth (Apple Developer: Team P3ZC6ZG46V, Service com.jamestannahill.weather).
Credentials loaded from Secrets Manager: weatherkit/credentials
"""

import json
import time
import base64
import urllib.request
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

_TZ  = ZoneInfo('America/New_York')
_LAT = 40.75
_LON = -73.98
_WK_URL = (
    f'https://weatherkit.apple.com/api/v1/weather/en/{_LAT}/{_LON}'
    f'?dataSets=forecastDaily&timezone=America/New_York'
)
_WK_ATTR_URL = 'https://weatherkit.apple.com/api/v1/attribution/en'

# In-memory caches (survive warm Lambda invocations)
_jwt_cache      = {'token': None, 'exp': 0}
_forecast_cache = {'data': None, 'ts': 0}
_attr_cache     = {'data': None, 'ts': 0}
_FORECAST_TTL   = 900    # 15 minutes
_ATTR_TTL       = 86400  # 24 hours — attribution assets rarely change

_CONDITION = {
    'Clear': 'Clear', 'MostlyClear': 'Mostly clear', 'PartlyCloudy': 'Partly cloudy',
    'MostlyCloudy': 'Mostly cloudy', 'Cloudy': 'Cloudy', 'Overcast': 'Overcast',
    'Foggy': 'Foggy', 'Haze': 'Hazy', 'Smoky': 'Smoky', 'Breezy': 'Breezy',
    'Windy': 'Windy', 'Hot': 'Hot', 'Drizzle': 'Drizzle', 'Rain': 'Rain',
    'HeavyRain': 'Heavy rain', 'SunShowers': 'Sun showers',
    'PartlyCloudyWithRain': 'Partly cloudy with rain',
    'MostlyCloudyWithRain': 'Mostly cloudy with rain',
    'IsolatedThunderstorms': 'Isolated thunderstorms',
    'ScatteredThunderstorms': 'Scattered thunderstorms',
    'Thunderstorms': 'Thunderstorms', 'SevereThunderstorm': 'Severe thunderstorm',
    'Sleet': 'Sleet', 'FreezingDrizzle': 'Freezing drizzle',
    'FreezingRain': 'Freezing rain', 'WintryMix': 'Wintry mix',
    'Snow': 'Snow', 'HeavySnow': 'Heavy snow', 'Flurries': 'Snow flurries',
    'SunFlurries': 'Sunny with flurries', 'BlowingSnow': 'Blowing snow',
    'Blizzard': 'Blizzard', 'Tornado': 'Tornado', 'TropicalStorm': 'Tropical storm',
    'Hurricane': 'Hurricane',
}

_DIRS = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW']


def _deg_to_cardinal(deg: float) -> str:
    return _DIRS[round(deg / 22.5) % 16]


def _c_to_f(c: float) -> int:
    return round(c * 9 / 5 + 32)


def _kmh_to_mph(kmh: float) -> int:
    return round(kmh * 0.621371)


def _make_jwt(creds: dict) -> str:
    key_id     = creds['key_id']
    team_id    = creds['team_id']
    service_id = creds['service_id']
    private_key = load_pem_private_key(creds['private_key'].encode(), password=None)

    now = int(time.time())
    header  = {'alg': 'ES256', 'kid': key_id, 'id': f'{team_id}.{service_id}'}
    payload = {'iss': team_id, 'iat': now, 'exp': now + 1800, 'sub': service_id}

    def _b64(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b'=').decode()

    h = _b64(json.dumps(header,  separators=(',', ':')).encode())
    p = _b64(json.dumps(payload, separators=(',', ':')).encode())
    sig_der = private_key.sign(f'{h}.{p}'.encode(), ec.ECDSA(hashes.SHA256()))
    r, s    = decode_dss_signature(sig_der)
    sig     = _b64(r.to_bytes(32, 'big') + s.to_bytes(32, 'big'))
    return f'{h}.{p}.{sig}'


def _get_jwt(creds: dict) -> str:
    now = int(time.time())
    if _jwt_cache['token'] and now < _jwt_cache['exp'] - 120:
        return _jwt_cache['token']
    token = _make_jwt(creds)
    _jwt_cache.update({'token': token, 'exp': now + 1800})
    return token


def _build_narrative(day: dict) -> str:
    sentences = []

    cond_code = day.get('conditionCode', '')
    cond_text = _CONDITION.get(cond_code, '')
    high_c    = day.get('temperatureMax')
    low_c     = day.get('temperatureMin')
    high_f    = _c_to_f(high_c) if high_c is not None else None
    low_f     = _c_to_f(low_c)  if low_c  is not None else None

    # Sentence 1: condition woven into temperature — matches "Temperatures swung to 57°F" style
    _rain_conditions = {'Rain','HeavyRain','Drizzle','SunShowers','MostlyCloudyWithRain',
                        'PartlyCloudyWithRain','Thunderstorms','ScatteredThunderstorms',
                        'IsolatedThunderstorms','SevereThunderstorm','FreezingRain',
                        'FreezingDrizzle','Sleet','WintryMix'}
    _snow_conditions = {'Snow','HeavySnow','Flurries','SunFlurries','BlowingSnow','Blizzard'}

    if cond_code in _rain_conditions:
        lead = f'Rain expected' if cond_code in ('Rain','HeavyRain') else cond_text
        sentences.append(f'{lead}, with temperatures reaching {high_f}°F.' if high_f else f'{lead}.')
    elif cond_code in _snow_conditions:
        sentences.append(f'{cond_text} expected, with a high near {high_f}°F.' if high_f else f'{cond_text} expected.')
    elif cond_text and high_f:
        sentences.append(f'{cond_text} with a high of {high_f}°F.')
    elif high_f:
        sentences.append(f'High of {high_f}°F.')

    # Sentence 2: overnight low
    if low_f is not None:
        sentences.append(f'Temperatures drop to around {low_f}°F overnight.')

    # Sentence 3: precipitation probability (only when not already implied by condition)
    precip = day.get('precipitationChance', 0)
    p_type = day.get('precipitationType', 'clear')
    if precip >= 0.25 and p_type not in ('clear', 'none', None, '') \
            and cond_code not in _rain_conditions | _snow_conditions:
        sentences.append(f'{round(precip * 100)}% chance of {p_type}.')

    # Sentence 4: wind
    day_fc  = day.get('daytimeForecast') or {}
    wind_kh = day_fc.get('windSpeedMax') or day.get('windSpeedMax')
    gust_kh = day_fc.get('windGustSpeedMax') or day.get('windGustSpeedMax')
    wind_d  = day_fc.get('windDirection')
    if wind_kh:
        mph  = _kmh_to_mph(wind_kh)
        card = f'{_deg_to_cardinal(wind_d)} ' if wind_d is not None else ''
        gust_mph = _kmh_to_mph(gust_kh) if gust_kh else None
        if gust_mph and gust_mph > mph + 4:
            sentences.append(f'{card}winds gusting to {gust_mph} mph.')
        elif mph >= 8:
            sentences.append(f'{card}winds around {mph} mph.')

    return ' '.join(sentences)


def fetch_attribution(creds: dict) -> dict | None:
    """
    Fetch WeatherKit attribution assets (logo URLs + legal page URL).
    Apple requires these to be displayed alongside any WeatherKit data.
    https://developer.apple.com/documentation/weatherkitrestapi/attribution
    """
    now = time.time()
    if _attr_cache['data'] and now - _attr_cache['ts'] < _ATTR_TTL:
        return _attr_cache['data']

    token = _get_jwt(creds)
    req = urllib.request.Request(_WK_ATTR_URL, headers={
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json',
    })
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"WeatherKit attribution fetch failed: {e}")
        return None

    # Apple returns partial paths — prepend base URL
    base = 'https://weatherkit.apple.com'
    def _url(key):
        v = data.get(key, '')
        return (base + v) if v and v.startswith('/') else v or None

    result = {
        'logo_dark_2x':   _url('logoDark@2x'),
        'logo_light_2x':  _url('logoLight@2x'),
        'logo_square_2x': _url('logoSquare@2x'),
        'legal_url':      data.get('legalPageURL') or 'https://weatherkit.apple.com/legal-attribution.html',
        'service_name':   data.get('serviceName', 'Weather'),
    }
    _attr_cache.update({'data': result, 'ts': now})
    return result


def fetch_tomorrow_forecast(creds: dict) -> dict | None:
    """
    Fetch tomorrow's WeatherKit daily forecast for the station location.
    Returns dict with name, temp_high, temp_low, short, detailed — or None.
    """
    now = time.time()
    if _forecast_cache['data'] and now - _forecast_cache['ts'] < _FORECAST_TTL:
        return _forecast_cache['data']

    tz       = _TZ
    tomorrow_date = (datetime.now(tz) + timedelta(days=1)).date()

    token = _get_jwt(creds)
    req = urllib.request.Request(_WK_URL, headers={
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json',
    })
    with urllib.request.urlopen(req, timeout=8) as resp:
        data = json.loads(resp.read())

    days = data.get('forecastDaily', {}).get('days', [])
    # Find the day whose forecastStart falls on tomorrow
    day = None
    for d in days:
        start_str = d.get('forecastStart', '')
        if start_str:
            start_dt = datetime.fromisoformat(start_str).astimezone(tz)
            if start_dt.date() == tomorrow_date:
                day = d
                break
    if day is None and len(days) > 1:
        day = days[1]   # fallback: second period is usually tomorrow
    if not day:
        return None

    tomorrow = datetime.now(tz) + timedelta(days=1)
    high_c    = day.get('temperatureMax')
    low_c     = day.get('temperatureMin')
    cond_code = day.get('conditionCode', '')

    result = {
        'name':      tomorrow.strftime('%A').upper(),   # e.g. "THURSDAY"
        'temp_high': _c_to_f(high_c) if high_c is not None else None,
        'temp_low':  _c_to_f(low_c)  if low_c  is not None else None,
        'short':     _CONDITION.get(cond_code, cond_code),
        'detailed':  _build_narrative(day),
        'source':    'weatherkit',
    }

    _forecast_cache.update({'data': result, 'ts': now})
    return result
