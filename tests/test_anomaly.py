import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))

from wx_api.anomaly import compute_anomalies, pressure_trend, condition_label

def test_compute_anomalies_above_average():
    current = {"tempf": 70.0, "humidity": 45.0, "windspeedmph": 15.0, "uv": 6.0}
    baseline = {"avg_tempf": 60.0, "avg_humidity": 55.0, "avg_windspeedmph": 10.0, "avg_uv": 4.0}
    result = compute_anomalies(current, baseline, month=4, hour=9)
    assert result["temp"]["delta"] == 10.0
    assert "above average" in result["temp"]["label"]
    assert result["humidity"]["delta"] == -10.0
    assert "below average" in result["humidity"]["label"]

def test_compute_anomalies_no_delta():
    current = {"tempf": 60.0, "humidity": 55.0, "windspeedmph": 10.0, "uv": 4.0}
    baseline = {"avg_tempf": 60.0, "avg_humidity": 55.0, "avg_windspeedmph": 10.0, "avg_uv": 4.0}
    result = compute_anomalies(current, baseline, month=4, hour=9)
    assert result["temp"]["delta"] == 0.0
    assert "near average" in result["temp"]["label"]

def test_pressure_trend_rising():
    readings = [
        {"baromrelin": 29.85},
        {"baromrelin": 29.90},
        {"baromrelin": 29.95},
    ]
    assert pressure_trend(readings) == "rising"

def test_pressure_trend_falling():
    readings = [
        {"baromrelin": 29.95},
        {"baromrelin": 29.90},
        {"baromrelin": 29.85},
    ]
    assert pressure_trend(readings) == "falling"

def test_condition_label_sunny():
    reading = {"uv": 5, "solarradiation": 400, "hourlyrainin": 0, "humidity": 40}
    assert condition_label(reading) == "Sunny"

def test_condition_label_rainy():
    reading = {"uv": 0, "solarradiation": 50, "hourlyrainin": 0.1, "humidity": 90}
    assert condition_label(reading) == "Rainy"
