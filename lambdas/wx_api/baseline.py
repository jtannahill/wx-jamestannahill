import calendar
import math
from datetime import datetime


def month_hour_key(month: int, hour: int) -> str:
    return f"{month:02d}-{hour:02d}"


def blend_plan(local_dt: datetime) -> tuple[str, str, float]:
    """
    Which two month-hour baselines to blend for a local timestamp, and the
    weight on the adjacent one.

    Baselines are stored per calendar month, so a lookup by month alone steps
    at local midnight on the 1st (Oct 1 2026: 10.4F at 4am). Treat each
    month's mean as belonging to mid-month and interpolate linearly toward
    the neighbouring month: before mid-month blend with the previous month,
    after it with the next. Both sides of a month boundary reach weight 0.5,
    so the curve is continuous.
    """
    days = calendar.monthrange(local_dt.year, local_dt.month)[1]
    pos = (local_dt.day - 0.5) / days          # 0..1 through the month
    own = month_hour_key(local_dt.month, local_dt.hour)
    if pos < 0.5:
        other_month = 12 if local_dt.month == 1 else local_dt.month - 1
        weight = 0.5 - pos
    else:
        other_month = 1 if local_dt.month == 12 else local_dt.month + 1
        weight = pos - 0.5
    return own, month_hour_key(other_month, local_dt.hour), weight


def blend(own: dict, other: dict | None, weight: float) -> dict:
    """
    Blend two baseline items. Means interpolate linearly; standard
    deviations interpolate through variance. Every other key (source,
    sample_count, month_hour) comes from the month that owns the timestamp.
    A missing or partial neighbour falls back to the owning month's value.
    """
    if not own:
        return own
    if not other or weight <= 0:
        return dict(own)
    out = dict(own)
    for key, a in own.items():
        b = other.get(key)
        if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
            continue
        if key.startswith('avg_'):
            out[key] = (1 - weight) * a + weight * b
        elif key.startswith('std_'):
            out[key] = math.sqrt((1 - weight) * a * a + weight * b * b)
    return out


def period_label(local_dt: datetime) -> str:
    """'early October', 'mid October' or 'late October' for anomaly copy."""
    month = local_dt.strftime('%B')
    if local_dt.day <= 10:
        return f"early {month}"
    if local_dt.day <= 20:
        return f"mid {month}"
    return f"late {month}"
