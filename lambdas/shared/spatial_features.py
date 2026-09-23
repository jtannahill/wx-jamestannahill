"""Spatial features from nearby PWS snapshots for rain probability ML."""
import math


def spatial_feature_vector(
    nearby: list[dict] | None,
    wind_dir_deg: float | None,
    home_pressure: float | None,
) -> list[float]:
    """
    Returns three normalized features:
      [0] upwind_rain_norm   — max upwind rain rate / 0.5 in/hr
      [1] nearby_humidity_norm — max nearby humidity deviation from 50%
      [2] pressure_grad_norm — home minus avg nearby pressure, scaled
    """
    if not nearby:
        return [0.0, 0.0, 0.0]

    upwind_rain = 0.0
    humidity_max = None
    pressures: list[float] = []

    for s in nearby:
        hum = s.get('humidity')
        if hum is not None:
            humidity_max = max(humidity_max or float(hum), float(hum))

        pres = s.get('pressure_in')
        if pres is not None:
            pressures.append(float(pres))

        rate = float(s.get('rain_rate_in_hr') or 0.0)
        if rate <= 0.01 or wind_dir_deg is None:
            continue
        bearing = float(s.get('bearing_deg') or 0)
        angle_diff = abs(((bearing - float(wind_dir_deg) + 180) % 360) - 180)
        if angle_diff < 60:
            upwind_rain = max(upwind_rain, rate)

    upwind_rain_norm = min(1.0, upwind_rain / 0.5)
    humidity_norm = ((humidity_max or 50.0) - 50.0) / 50.0 if humidity_max is not None else 0.0

    pressure_grad_norm = 0.0
    if home_pressure is not None and pressures:
        avg_nearby = sum(pressures) / len(pressures)
        pressure_grad_norm = max(-1.0, min(1.0, (float(home_pressure) - avg_nearby) / 0.05))

    return [upwind_rain_norm, humidity_norm, pressure_grad_norm]
