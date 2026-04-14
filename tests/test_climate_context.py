import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))

from wx_api.climate_context import live_context, daily_verdict, anomaly_headline

# ── Fixtures ─────────────────────────────────────────────────────────────────

HOURLY_STATS = {
    "p25_tempf": 55.0, "p50_tempf": 62.0, "p75_tempf": 68.0,
    "mean_tempf": 62.0, "std_tempf": 8.0,
    "p25_dewptf": 38.0, "p50_dewptf": 44.0, "p75_dewptf": 51.0,
    "mean_dewptf": 44.0, "std_dewptf": 7.0,
    "p25_windmph": 5.0, "p50_windmph": 9.0, "p75_windmph": 14.0,
    "mean_windmph": 9.0, "std_windmph": 4.0,
    "sample_count": 85,
}

DOY_STATS = {
    "sample_count": 156,
    "p50_tmax": 62.0,
    "annual_highs": [
        {"year": 2024, "tmax": 65.0, "tmin": 48.0, "awnd": 9.0},
        {"year": 2020, "tmax": 58.0, "tmin": 42.0, "awnd": 7.0},
        {"year": 1987, "tmax": 72.0, "tmin": 55.0, "awnd": 11.0},
        {"year": 1950, "tmax": 74.0, "tmin": 53.0, "awnd": 13.0},
        {"year": 1940, "tmax": 60.0, "tmin": 45.0, "awnd": 8.0},
    ],
}

# ── live_context ──────────────────────────────────────────────────────────────

def test_live_context_returns_three_metrics():
    reading = {"tempf": 72.0, "dewPoint": 52.0, "windspeedmph": 10.0}
    result = live_context(reading, HOURLY_STATS, "0413")
    assert "temp" in result["metrics"]
    assert "dewpoint" in result["metrics"]
    assert "wind" in result["metrics"]

def test_live_context_percentile_above_mean():
    reading = {"tempf": 78.0, "dewPoint": 44.0, "windspeedmph": 9.0}
    result = live_context(reading, HOURLY_STATS, "0413")
    # 78°F is well above mean of 62°F → should be high percentile
    assert result["metrics"]["temp"]["percentile"] > 75

def test_live_context_percentile_at_median():
    reading = {"tempf": 62.0, "dewPoint": 44.0, "windspeedmph": 9.0}
    result = live_context(reading, HOURLY_STATS, "0413")
    # At the mean → should be near 50th
    pct = result["metrics"]["temp"]["percentile"]
    assert 40 <= pct <= 60

def test_live_context_includes_distribution_bounds():
    reading = {"tempf": 72.0, "dewPoint": 52.0, "windspeedmph": 10.0}
    result = live_context(reading, HOURLY_STATS, "0413")
    t = result["metrics"]["temp"]
    assert t["p25"] == 55.0
    assert t["p50"] == 62.0
    assert t["p75"] == 68.0
    assert t["years_of_data"] == 85

def test_live_context_headline_contains_percentile():
    reading = {"tempf": 72.0, "dewPoint": 52.0, "windspeedmph": 10.0}
    result = live_context(reading, HOURLY_STATS, "0413")
    assert "percentile" in result["headline"].lower()
    assert "85" in result["headline"]

def test_live_context_none_stats_returns_none():
    reading = {"tempf": 72.0, "dewPoint": 52.0, "windspeedmph": 10.0}
    assert live_context(reading, None, "0413") is None

# ── daily_verdict ─────────────────────────────────────────────────────────────

def test_daily_verdict_finds_last_exceeded_year():
    # today_high=70°F → exceeded by 1987 (72°F) and 1950 (74°F) → last exceeded = 1987
    result = daily_verdict(70.0, 45.0, DOY_STATS, "0413")
    assert result["temp_high"]["last_exceeded_year"] == 1987

def test_daily_verdict_label_warmest_since():
    result = daily_verdict(70.0, 45.0, DOY_STATS, "0413")
    assert "since 1987" in result["temp_high"]["label"]

def test_daily_verdict_record_high():
    # today_high=80°F exceeds all historical (max is 74°F) → on record
    result = daily_verdict(80.0, 45.0, DOY_STATS, "0413")
    assert result["temp_high"]["last_exceeded_year"] is None
    assert "record" in result["temp_high"]["label"].lower()

def test_daily_verdict_percentile_above_most():
    # today_high=70°F exceeds 3 of 5 historical highs (58, 60, 65) → ~60th pct
    result = daily_verdict(70.0, 45.0, DOY_STATS, "0413")
    pct = result["temp_high"]["percentile"]
    assert 50 <= pct <= 80

def test_daily_verdict_includes_years_of_data():
    result = daily_verdict(70.0, 45.0, DOY_STATS, "0413")
    assert result["temp_high"]["years_of_data"] == 156

# ── anomaly_headline ──────────────────────────────────────────────────────────

def test_anomaly_headline_prefers_verdict():
    reading = {"tempf": 72.0, "dewPoint": 52.0, "windspeedmph": 10.0}
    live    = live_context(reading, HOURLY_STATS, "0413")
    verdict = daily_verdict(70.0, 45.0, DOY_STATS, "0413")
    headline = anomaly_headline(live, verdict)
    assert "since 1987" in headline

def test_anomaly_headline_falls_back_to_live():
    reading = {"tempf": 72.0, "dewPoint": 52.0, "windspeedmph": 10.0}
    live    = live_context(reading, HOURLY_STATS, "0413")
    headline = anomaly_headline(live, None)
    assert "percentile" in headline.lower()

def test_anomaly_headline_none_when_no_data():
    assert anomaly_headline(None, None) is None
