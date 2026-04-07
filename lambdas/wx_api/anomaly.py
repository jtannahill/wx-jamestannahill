from datetime import datetime, timezone


def compute_anomalies(current: dict, baseline: dict, month: int, hour: int) -> dict:
    month_name = datetime(2026, month, 1).strftime('%B')
    hour_label = datetime(2026, 1, 1, hour).strftime('%-I%p').lower()  # e.g. "9am"

    def delta_label(field: str, unit: str) -> dict:
        cur = float(current.get(field) or 0)
        avg = float(baseline.get(f'avg_{field}') or 0)
        delta = round(cur - avg, 1)
        if abs(delta) < 0.5:
            text = f"near average for {hour_label} in {month_name}"
        elif delta > 0:
            text = f"{abs(delta)}{unit} above average for {hour_label} in {month_name}"
        else:
            text = f"{abs(delta)}{unit} below average for {hour_label} in {month_name}"
        return {"delta": delta, "label": text}

    return {
        "temp": delta_label("tempf", "°F"),
        "humidity": delta_label("humidity", "%"),
        "wind": delta_label("windspeedmph", " mph"),
        "uv": delta_label("uv", " pts"),
    }


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
