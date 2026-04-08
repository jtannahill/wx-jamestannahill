"""
Proprietary ML signals for wx.jamestannahill.com.

comfort_score()   — Microclimate Comfort Score (0–100), season-weighted,
                    calibrated to this station's own baseline distribution.

rain_probability() — P(rain next 60 min) via logistic regression on
                    pressure trend, humidity, and dew-point depression.
                    Heuristic coefficients ship with the code; wx-ml-fitter
                    replaces them with data-fitted values weekly.
"""
import os, math, datetime


# ── Rain probability coefficients ─────────────────────────────────────────────
# Default: meteorologically grounded heuristics (NYC ~120 rain-days/yr).
# wx-ml-fitter overwrites these in wx-ml-models; we load them once per
# Lambda cold start so every warm invocation uses the latest fitted values.

_HEURISTIC_W = [1.6, -9.0, -1.0, 0.15, -0.05]
_HEURISTIC_B = -2.1

_rain_w  = None   # set on first call or cold-start load
_rain_b  = None
_coeff_source = 'heuristic'


def _load_rain_coefficients():
    """Try to load fitted coefficients from wx-ml-models. Silently falls back."""
    global _rain_w, _rain_b, _coeff_source
    try:
        import boto3
        from decimal import Decimal

        models_table = os.environ.get('MODELS_TABLE', 'wx-ml-models')
        table = boto3.resource('dynamodb', region_name='us-east-1').Table(models_table)
        resp  = table.get_item(Key={'model_id': 'rain_probability'})
        item  = resp.get('Item')

        if item and item.get('weights') and item.get('bias') is not None:
            _rain_w = [float(x) for x in item['weights']]
            _rain_b = float(item['bias'])
            n       = int(item.get('training_samples', 0))
            f1      = float(item.get('f1', 0))
            _coeff_source = f"fitted (n={n}, f1={f1:.3f})"
            print(f"rain_probability: loaded {_coeff_source}")
            return

    except Exception as e:
        print(f"rain_probability: could not load fitted coefficients ({e}), using heuristic")

    _rain_w = list(_HEURISTIC_W)
    _rain_b = _HEURISTIC_B


# ── Comfort Score ─────────────────────────────────────────────────────────────

def comfort_score(reading: dict, baseline: dict, month: int) -> dict:
    """
    Microclimate Comfort Score 0–100 for Midtown Manhattan.
    100 = ideal outdoor conditions. Penalizes: extreme feels-like, high
    humidity, strong wind, high UV, rain. Anomaly-adjusted against the
    station's own rolling baseline.
    """
    feels    = float(reading.get('feelsLike') or reading.get('tempf') or 65)
    humidity = float(reading.get('humidity') or 50)
    wind     = float(reading.get('windspeedmph') or 0)
    uv       = float(reading.get('uv') or 0)
    rain     = float(reading.get('hourlyrainin') or 0)

    is_summer = 5 <= month <= 9    # May–Sep
    is_winter = month <= 2 or month >= 12

    score = 100.0

    # Feels-like: ideal 66–76°F. Non-linear penalty widens with deviation.
    dev    = abs(feels - 71.0)
    score -= min(45, (dev / 1.8) ** 1.5)

    # Humidity: ideal 35–60%
    if humidity > 65:
        score -= (humidity - 65) * 0.5 * (1.3 if is_summer else 1.0)
    elif humidity < 30:
        score -= (30 - humidity) * 0.3

    # Wind: ideal < 10 mph. More punishing in winter.
    if wind > 10:
        score -= min(20, (wind - 10) * (1.4 if is_winter else 0.9))

    # UV: only matters if UV is meaningful. Summer-amplified.
    if uv >= 3:
        score -= min(15, (uv - 2) * (1.3 if is_summer else 0.5))

    # Rain: scaled by rate
    if rain > 0.05:
        score -= 25
    elif rain > 0.01:
        score -= 15

    # Anomaly penalty: if conditions are extremely unusual, extra discomfort
    if baseline:
        avg_t = baseline.get('avg_tempf')
        if avg_t is not None:
            anomaly = abs(float(reading.get('tempf', avg_t)) - float(avg_t))
            if anomaly > 12:
                score -= (anomaly - 12) * 0.4

    final = max(0, min(100, round(score)))

    if   final >= 85: label = 'Excellent'
    elif final >= 70: label = 'Good'
    elif final >= 50: label = 'Fair'
    elif final >= 30: label = 'Poor'
    else:             label = 'Harsh'

    return {'score': final, 'label': label}


# ── Rain Probability ──────────────────────────────────────────────────────────

def _sigmoid(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


def rain_probability(reading: dict, recent_readings: list, nearby: list | None = None) -> dict:
    """
    Returns {'probability': 0-100, 'label': str, 'coeff_source': str,
             'spatial_boost': float, 'spatial_source': str|None}.
    recent_readings: newest-first list (from DynamoDB query).
    nearby: optional list of nearby station dicts for spatial rain boost.
    """
    global _rain_w, _rain_b
    if _rain_w is None:
        _load_rain_coefficients()

    humidity   = float(reading.get('humidity') or 50)
    tempf      = float(reading.get('tempf') or 60)
    dewpoint   = float(reading.get('dewPoint') or tempf - 20)
    barom      = reading.get('baromrelin')
    rain_now   = float(reading.get('hourlyrainin') or 0)

    # Hour-of-day cyclic encoding
    try:
        ts   = datetime.datetime.fromisoformat(reading.get('timestamp', ''))
        hour = ts.hour
    except Exception:
        hour = 12

    # Pressure trend: compare latest to reading ~1 hour ago (index 10–12)
    pressure_delta = 0.0
    if barom is not None:
        old_p = next(
            (r.get('baromrelin') for r in recent_readings[9:13]
             if r.get('baromrelin') is not None),
            None,
        )
        if old_p is not None:
            pressure_delta = float(barom) - float(old_p)

    # Feature vector
    humidity_norm       = (humidity - 50.0) / 50.0
    dew_depression      = max(0.0, tempf - dewpoint)
    dew_depression_norm = (dew_depression - 20.0) / 20.0
    sin_h               = math.sin(2 * math.pi * hour / 24)
    cos_h               = math.cos(2 * math.pi * hour / 24)

    z = sum(w * f for w, f in zip(_rain_w, [humidity_norm, pressure_delta,
                                             dew_depression_norm, sin_h, cos_h])) + _rain_b

    # Boost if it's already raining (persistence)
    if rain_now > 0.01:
        z += 2.5

    base_prob = _sigmoid(z)

    # Spatial boost from nearby upwind stations
    spatial_boost_val = 0.0
    spatial_source = None
    if nearby:
        from wx_api.nearby import spatial_rain_boost
        wind_dir = reading.get('winddir', 0) or 0
        spatial_boost_val, spatial_source = spatial_rain_boost(nearby, wind_dir)
        if spatial_boost_val > 0:
            base_prob = min(0.99, base_prob + spatial_boost_val)

    prob = max(1, min(99, round(base_prob * 100)))

    if   prob <  10: label = 'Unlikely'
    elif prob <  30: label = 'Slight chance'
    elif prob <  55: label = 'Possible'
    elif prob <  75: label = 'Likely'
    else:            label = 'Very likely'

    return {
        'probability':    prob,
        'label':          label,
        'coeff_source':   _coeff_source,
        'spatial_boost':  round(spatial_boost_val, 3),
        'spatial_source': spatial_source,
    }
