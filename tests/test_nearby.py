import sys, os, math
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))

from wx_poller.nearby import _bearing, _haversine_mi, fetch_nearby

def test_bearing_due_east():
    b = _bearing(40.75, -74.0, 40.75, -73.9)
    assert 85 < b < 95, f"Expected ~90, got {b}"

def test_bearing_due_north():
    b = _bearing(40.75, -74.0, 40.85, -74.0)
    assert b < 5 or b > 355, f"Expected ~0/360, got {b}"

def test_haversine_central_park():
    d = _haversine_mi(40.755, -73.984, 40.785, -73.968)
    assert 2.0 < d < 3.5, f"Expected ~2.5 mi, got {d}"

def test_fetch_nearby_returns_list_on_api_success():
    from unittest.mock import patch, MagicMock
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        'observations': [
            {
                'stationID': 'KNYNEWYO2140',
                'neighborhood': 'Midtown Manhattan',
                'lat': 40.760,
                'lon': -73.990,
                'winddir': 90,
                'humidity': 42,
                'obsTimeLocal': '2026-04-08 12:00:00',
                'imperial': {
                    'temp': 64, 'windSpeed': 8, 'windGust': 12,
                    'pressure': 30.22, 'precipRate': 0.0,
                },
            }
        ]
    }
    with patch('wx_poller.nearby.requests.get', return_value=mock_resp):
        result = fetch_nearby('fake_key', limit=5)
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]['station_id'] == 'KNYNEWYO2140'
    assert result[0]['temp_f'] == 64
    assert 'bearing_deg' in result[0]
    assert 'distance_mi' in result[0]

def test_fetch_nearby_returns_empty_on_error():
    from unittest.mock import patch
    with patch('wx_poller.nearby.requests.get', side_effect=Exception("timeout")):
        result = fetch_nearby('fake_key')
    assert result == []

def test_fetch_nearby_excludes_home_station():
    from unittest.mock import patch, MagicMock
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        'observations': [
            {
                'stationID': 'HOME',
                'neighborhood': 'Midtown',
                'lat': 40.7549,
                'lon': -73.984,
                'winddir': 90, 'humidity': 42,
                'obsTimeLocal': '2026-04-08 12:00:00',
                'imperial': {'temp': 64, 'windSpeed': 8, 'windGust': 12,
                             'pressure': 30.22, 'precipRate': 0.0},
            }
        ]
    }
    with patch('wx_poller.nearby.requests.get', return_value=mock_resp):
        result = fetch_nearby('fake_key')
    assert result == []

def test_fetch_nearby_handles_trace_precipitation():
    from unittest.mock import patch, MagicMock
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        'observations': [
            {
                'stationID': 'KNYTEST01',
                'neighborhood': 'Hell\'s Kitchen',
                'lat': 40.760, 'lon': -73.990,
                'winddir': 90, 'humidity': 42,
                'obsTimeLocal': '2026-04-08 12:00:00',
                'imperial': {
                    'temp': 64, 'windSpeed': 8, 'windGust': 12,
                    'pressure': 30.22, 'precipRate': 'T',  # WU trace value
                },
            }
        ]
    }
    with patch('wx_poller.nearby.requests.get', return_value=mock_resp):
        result = fetch_nearby('fake_key')
    assert len(result) == 1
    assert result[0]['rain_rate_in_hr'] == 0.0  # trace → 0.0, no crash


# ── spatial_rain_boost tests ──────────────────────────────────────────────────
from wx_api.nearby import spatial_rain_boost

def test_boost_is_zero_when_no_nearby():
    boost, label = spatial_rain_boost([], wind_dir_deg=270)
    assert boost == 0.0
    assert label is None

def test_boost_is_zero_when_wind_dir_none():
    nearby = [{'bearing_deg': 270, 'distance_mi': 0.5,
               'rain_rate_in_hr': 0.5, 'neighborhood': "Hell's Kitchen"}]
    boost, label = spatial_rain_boost(nearby, wind_dir_deg=None)
    assert boost == 0.0

def test_boost_is_zero_when_station_not_raining():
    nearby = [{'bearing_deg': 270, 'distance_mi': 0.5,
               'rain_rate_in_hr': 0.0, 'neighborhood': "Hell's Kitchen"}]
    boost, label = spatial_rain_boost(nearby, wind_dir_deg=270)
    assert boost == 0.0

def test_boost_is_zero_when_station_downwind():
    # Wind from 270° (west); station to the EAST (bearing ~90°) is downwind
    nearby = [{'bearing_deg': 90, 'distance_mi': 0.5,
               'rain_rate_in_hr': 0.5, 'neighborhood': 'Gramercy'}]
    boost, label = spatial_rain_boost(nearby, wind_dir_deg=270)
    assert boost == 0.0

def test_boost_positive_upwind_station_raining():
    # Wind from 270°, station to the west (bearing 270°) is raining
    nearby = [{'bearing_deg': 270, 'distance_mi': 0.5,
               'rain_rate_in_hr': 0.5, 'neighborhood': "Hell's Kitchen"}]
    boost, label = spatial_rain_boost(nearby, wind_dir_deg=270)
    assert boost > 0
    assert boost <= 0.35
    assert label == "Hell's Kitchen"

def test_boost_capped_at_035():
    # Very close, heavy rain
    nearby = [{'bearing_deg': 90, 'distance_mi': 0.1,
               'rain_rate_in_hr': 2.0, 'neighborhood': 'Murray Hill'}]
    boost, label = spatial_rain_boost(nearby, wind_dir_deg=90)
    assert boost == 0.35

def test_boost_uses_closest_upwind_station():
    nearby = [
        {'bearing_deg': 270, 'distance_mi': 2.0,
         'rain_rate_in_hr': 0.5, 'neighborhood': 'Upper West Side'},
        {'bearing_deg': 270, 'distance_mi': 0.3,
         'rain_rate_in_hr': 0.5, 'neighborhood': "Hell's Kitchen"},
    ]
    boost, label = spatial_rain_boost(nearby, wind_dir_deg=270)
    assert label == "Hell's Kitchen"  # closer station wins (higher boost)
