import math
from datetime import datetime, timezone


def percentile_rank(reading: dict, baseline: dict, month: int) -> dict | None:
    """
    Approximate percentile rank of current temperature vs. historical readings
    for this month+hour, using normal distribution with season-calibrated σ.
    Returns {'rank': 0-100, 'label': str} or None if data unavailable.
    """
    tempf = reading.get('tempf')
    avg   = baseline.get('avg_tempf')
    if tempf is None or avg is None:
        return None

    # Season-calibrated σ for NYC (°F)
    if month in (12, 1, 2):   sigma = 9.5   # winter — high variability
    elif month in (3, 4, 5):  sigma = 8.0   # spring
    elif month in (6, 7, 8):  sigma = 6.0   # summer — more stable
    else:                     sigma = 8.5   # fall

    z    = (float(tempf) - float(avg)) / sigma
    rank = round(_normal_cdf(z) * 100)
    rank = max(1, min(99, rank))

    # Label
    if rank >= 95:   label = f"one of the warmest {_ordinal(rank)} on record here"
    elif rank >= 80: label = f"warmer than {rank}% of readings"
    elif rank >= 60: label = f"slightly warm ({rank}th percentile)"
    elif rank >= 40: label = f"near median ({rank}th percentile)"
    elif rank >= 20: label = f"slightly cool ({rank}th percentile)"
    elif rank >= 5:  label = f"cooler than {100-rank}% of readings"
    else:            label = f"one of the coldest {_ordinal(100-rank)} on record here"

    return {'rank': rank, 'label': label}


def _normal_cdf(z: float) -> float:
    """Approximation of the standard normal CDF using math.erf."""
    return (1.0 + math.erf(z / math.sqrt(2))) / 2.0


def _ordinal(n: int) -> str:
    if 11 <= n % 100 <= 13:
        return f"{n}th"
    return f"{n}{['th','st','nd','rd','th'][min(n%10,4)]}"


def compute_anomalies(current: dict, baseline: dict, month: int, hour: int) -> dict:
    month_name = datetime(2026, month, 1).strftime('%B')
    hour_label = datetime(2026, 1, 1, hour).strftime('%-I%p').lower()  # e.g. "9am"

    def delta_label(field: str, unit: str) -> dict | None:
        cur_raw = current.get(field)
        if cur_raw is None:
            return None
        cur = float(cur_raw)
        avg_raw = baseline.get(f'avg_{field}')
        if avg_raw is None:
            return None
        avg = float(avg_raw)
        delta = round(cur - avg, 1)
        if abs(delta) < 0.5:
            text = f"near average for {hour_label} in {month_name}"
        elif delta > 0:
            text = f"{abs(delta)}{unit} above average for {hour_label} in {month_name}"
        else:
            text = f"{abs(delta)}{unit} below average for {hour_label} in {month_name}"
        return {"delta": delta, "label": text}

    return {k: v for k, v in {
        "temp": delta_label("tempf", "°F"),
        "humidity": delta_label("humidity", "%"),
        "wind": delta_label("windspeedmph", " mph"),
        "uv": delta_label("uv", " pts"),
    }.items() if v is not None}


def pressure_trend(recent_readings: list) -> str:
    if len(recent_readings) < 2:
        return "steady"
    values = [float(r.get("baromrelin") or 0) for r in recent_readings]
    delta = values[-1] - values[0]
    if delta > 0.02:
        return "rising"
    if delta < -0.02:
        return "falling"
    return "steady"


def condition_label(reading: dict) -> str:
    rain = float(reading.get("hourlyrainin") or 0)
    humidity = float(reading.get("humidity") or 0)
    solar = float(reading.get("solarradiation") or 0)
    uv = float(reading.get("uv") or 0)

    if rain > 0.01:
        return "Rainy"
    if humidity > 80:
        return "Overcast"
    if solar > 300 and uv >= 3:
        return "Sunny"
    if solar > 100:
        return "Partly Cloudy"
    return "Cloudy"
