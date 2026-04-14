"""
climate_context.py — Historical climate context for the /current API response.

live_context(reading, doy_hour_stats, doy)
  → percentile rank for current temp/dewpoint/wind vs. ERA5 hourly distribution
  → mode = "live"

daily_verdict(today_high, today_low, doy_stats, doy)
  → "Warmest April 13th since 1923" claims from NOAA 156-year distribution
  → mode = "daily"

anomaly_headline(live, verdict)
  → punchy headline string; prefers verdict when available
"""
import math
from datetime import datetime


def _normal_cdf(z: float) -> float:
    return (1.0 + math.erf(z / math.sqrt(2))) / 2.0


def _ordinal(n: int) -> str:
    if 11 <= n % 100 <= 13:
        return f"{n}th"
    return f"{n}{['th','st','nd','rd','th'][min(n % 10, 4)]}"


def _doy_label(doy: str) -> str:
    """Convert DOY string like "0413" to "April 13th"."""
    try:
        dt = datetime.strptime(doy, "%m%d")
        day = dt.day
        return f"{dt.strftime('%B')} {_ordinal(day)}"
    except ValueError:
        return doy


def _metric_context(value: float, stats: dict, prefix: str, years: int) -> dict | None:
    mean = stats.get(f"mean_{prefix}")
    std  = stats.get(f"std_{prefix}")
    if mean is None or std is None or float(std) == 0:
        return None

    z          = (value - float(mean)) / float(std)
    percentile = round(_normal_cdf(z) * 100)
    percentile = max(1, min(99, percentile))

    if percentile >= 90:
        label = f"{_ordinal(percentile)} percentile — exceptionally high"
    elif percentile >= 75:
        label = f"{_ordinal(percentile)} percentile — above normal"
    elif percentile >= 25:
        label = f"{_ordinal(percentile)} percentile — near normal"
    elif percentile >= 10:
        label = f"{_ordinal(percentile)} percentile — below normal"
    else:
        label = f"{_ordinal(percentile)} percentile — exceptionally low"

    return {
        "value":        round(value, 1),
        "percentile":   percentile,
        "label":        label,
        "p25":          float(stats[f"p25_{prefix}"]) if stats.get(f"p25_{prefix}") is not None else None,
        "p50":          float(stats[f"p50_{prefix}"]) if stats.get(f"p50_{prefix}") is not None else None,
        "p75":          float(stats[f"p75_{prefix}"]) if stats.get(f"p75_{prefix}") is not None else None,
        "years_of_data": years,
    }


def live_context(reading: dict, doy_hour_stats: dict | None, doy: str) -> dict | None:
    """
    Compare current reading against ERA5 hourly distribution for this DOY+hour.
    Returns None if stats are unavailable.
    """
    if not doy_hour_stats:
        return None

    years   = int(doy_hour_stats.get("sample_count", 85))
    metrics = {}

    tempf = reading.get("tempf")
    if tempf is not None:
        m = _metric_context(float(tempf), doy_hour_stats, "tempf", years)
        if m:
            metrics["temp"] = m

    dewpt = reading.get("dewPoint")
    if dewpt is not None:
        m = _metric_context(float(dewpt), doy_hour_stats, "dewptf", years)
        if m:
            metrics["dewpoint"] = m

    wind = reading.get("windspeedmph")
    if wind is not None:
        m = _metric_context(float(wind), doy_hour_stats, "windmph", years)
        if m:
            metrics["wind"] = m

    if not metrics:
        return None

    headline = None
    if "temp" in metrics:
        pct      = metrics["temp"]["percentile"]
        yrs      = metrics["temp"]["years_of_data"]
        headline = (
            f"Currently {_ordinal(pct)} percentile for this hour on "
            f"{_doy_label(doy)} in {yrs} years of records"
        )

    return {"mode": "live", "metrics": metrics, "headline": headline}


def daily_verdict(
    today_high: float | None,
    today_low:  float | None,
    doy_stats:  dict | None,
    doy:        str,
) -> dict | None:
    """
    Compare today's confirmed high/low against NOAA 156-year distribution.
    Returns None if stats are unavailable or today_high is None.
    """
    if not doy_stats or today_high is None:
        return None

    annual_highs = doy_stats.get("annual_highs", [])
    annual_highs = sorted(annual_highs, key=lambda r: r["year"], reverse=True)
    if not annual_highs:
        return None

    years_of_data = int(doy_stats.get("sample_count", len(annual_highs)))
    result        = {}

    # ── Temperature high ──────────────────────────────────────────────────────
    tmax_vals = [float(r["tmax"]) for r in annual_highs if r.get("tmax") is not None]
    if tmax_vals:
        rank = sum(1 for v in tmax_vals if v < today_high)
        pct  = max(1, min(99, round(rank / len(tmax_vals) * 100)))

        # Scan year-descending list for most recent year that exceeded today
        last_exceeded = None
        for rec in annual_highs:
            if rec.get("tmax") is not None and float(rec["tmax"]) > today_high:
                last_exceeded = int(rec["year"])
                break

        earliest_year = int(annual_highs[-1]["year"]) if annual_highs else 1869
        if last_exceeded:
            label = f"Warmest {_doy_label(doy)} since {last_exceeded}"
        else:
            label = f"Warmest {_doy_label(doy)} on record (since {earliest_year})"

        result["temp_high"] = {
            "value":               round(today_high, 1),
            "percentile":          pct,
            "last_exceeded_year":  last_exceeded,
            "label":               label,
            "years_of_data":       years_of_data,
            "p5":   round(float(doy_stats["p5_tmax"]),  1) if doy_stats.get("p5_tmax")  is not None else None,
            "p50":  round(float(doy_stats["p50_tmax"]), 1) if doy_stats.get("p50_tmax") is not None else None,
            "p95":  round(float(doy_stats["p95_tmax"]), 1) if doy_stats.get("p95_tmax") is not None else None,
        }

    # ── Temperature low ───────────────────────────────────────────────────────
    if today_low is not None:
        tmin_vals = [float(r["tmin"]) for r in annual_highs if r.get("tmin") is not None]
        if tmin_vals:
            rank = sum(1 for v in tmin_vals if v < today_low)
            pct  = max(1, min(99, round(rank / len(tmin_vals) * 100)))

            last_exceeded_low = None
            for rec in annual_highs:
                if rec.get("tmin") is not None and float(rec["tmin"]) > today_low:
                    last_exceeded_low = int(rec["year"])
                    break

            earliest_year_low = int(annual_highs[-1]["year"]) if annual_highs else 1869
            if last_exceeded_low:
                label_low = f"Warmest low on {_doy_label(doy)} since {last_exceeded_low}"
            else:
                label_low = f"Warmest low on {_doy_label(doy)} on record (since {earliest_year_low})"

            result["temp_low"] = {
                "value":              round(today_low, 1),
                "percentile":         pct,
                "p5":   round(float(doy_stats["p5_tmin"]),  1) if doy_stats.get("p5_tmin")  is not None else None,
                "p50":  round(float(doy_stats["p50_tmin"]), 1) if doy_stats.get("p50_tmin") is not None else None,
                "p95":  round(float(doy_stats["p95_tmin"]), 1) if doy_stats.get("p95_tmin") is not None else None,
                "last_exceeded_year": last_exceeded_low,
                "label":              label_low,
                "years_of_data":      years_of_data,
            }

    # ── Wind (placeholder — actual avg_wind wired in handler.py) ─────────────
    # awnd_vals available but wind_today not passed to this function
    # The handler.py will call this after adding wind from daily_summary

    return result or None


def anomaly_headline(live: dict | None, verdict: dict | None) -> str | None:
    """
    Returns the most prominent headline string.
    Prefers verdict (NOAA "warmest since YEAR") over live percentile.
    """
    if verdict and verdict.get("temp_high"):
        t   = verdict["temp_high"]
        pct = t["percentile"]
        if t.get("last_exceeded_year"):
            return f"{t['label']} · {_ordinal(pct)} percentile"
        return t["label"]

    if live and live.get("metrics", {}).get("temp"):
        t = live["metrics"]["temp"]
        return (
            f"Currently {_ordinal(t['percentile'])} percentile for this hour"
            f" · {t['years_of_data']} yrs"
        )

    return None
