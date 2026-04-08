"""
Source data validation for wx station readings.

validate_reading()  — clamps out-of-range fields to None, returns issue list
detect_stuck()      — True if sensor is frozen (N consecutive identical values)
"""

# Physical valid ranges for Midtown Manhattan
VALID_RANGES = {
    'tempf':          (-40.0,  130.0),
    'feelsLike':      (-60.0,  150.0),
    'humidity':       (  0.0,  100.0),
    'dewPoint':       (-40.0,   90.0),
    'windspeedmph':   (  0.0,  200.0),
    'windgustmph':    (  0.0,  250.0),
    'winddir':        (  0.0,  360.0),
    'baromrelin':     ( 27.0,   32.0),
    'solarradiation': (  0.0, 2000.0),
    'uv':             (  0.0,   16.0),
    'hourlyrainin':   (  0.0,   10.0),
    'dailyrainin':    (  0.0,   30.0),
}

# N identical non-None values in a row = stuck sensor (5 min × 6 = 30 min)
STUCK_THRESHOLD = 6


def validate_reading(reading: dict) -> tuple[dict, list[str]]:
    """
    Returns (cleaned_reading, issues).
    Out-of-range fields are set to None. Reading is always stored — issues are
    recorded as a quality_flag rather than dropped entirely.
    """
    issues = []
    cleaned = dict(reading)

    for field, (lo, hi) in VALID_RANGES.items():
        val = reading.get(field)
        if val is None:
            continue
        try:
            fval = float(val)
        except (TypeError, ValueError):
            cleaned[field] = None
            issues.append(f"{field}=non-numeric")
            continue
        if not (lo <= fval <= hi):
            cleaned[field] = None
            issues.append(f"{field}={fval:.1f} out of [{lo},{hi}]")

    return cleaned, issues


def detect_stuck(readings: list, field: str = 'tempf') -> bool:
    """
    Returns True if the last STUCK_THRESHOLD readings all have identical
    non-None values for `field` — indicating a frozen sensor.
    `readings` should be ordered oldest-first.
    """
    vals = [r.get(field) for r in readings if r.get(field) is not None]
    if len(vals) < STUCK_THRESHOLD:
        return False
    last_n = [round(float(v), 1) for v in vals[-STUCK_THRESHOLD:]]
    return len(set(last_n)) == 1
