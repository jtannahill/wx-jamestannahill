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

def _make_wu_mocks(station_ids, observations_by_id, near_status=200):
    """
    Build a requests.get side_effect serving the two-phase WU flow:
    v3/location/near first, then v2/pws/observations/current per station.
    observations_by_id values: dict (observation), 204 (not reporting).
    """
    from unittest.mock import MagicMock

    def _get(url, params=None, timeout=None):
        resp = MagicMock()
        if 'location/near' in url:
            resp.status_code = near_status
            resp.json.return_value = {'location': {'stationId': station_ids}}
        else:
            obs = observations_by_id.get(params['stationId'])
            if obs == 204:
                resp.status_code = 204
            else:
                resp.status_code = 200
                resp.json.return_value = {'observations': [obs]}
        return resp

    return _get

def _obs(station_id, lat=40.760, lon=-73.990, precip_rate=0.0):
    return {
        'stationID': station_id,
        'neighborhood': 'Midtown Manhattan',
        'lat': lat,
        'lon': lon,
        'winddir': 90,
        'humidity': 42,
        'obsTimeLocal': '2026-04-08 12:00:00',
        'imperial': {
            'temp': 64, 'windSpeed': 8, 'windGust': 12,
            'pressure': 30.22, 'precipRate': precip_rate,
        },
    }

def test_fetch_nearby_returns_list_on_api_success():
    from unittest.mock import patch
    side_effect = _make_wu_mocks(
        ['KNYNEWYO2140'], {'KNYNEWYO2140': _obs('KNYNEWYO2140')})
    with patch('wx_poller.nearby.requests.get', side_effect=side_effect):
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
    from unittest.mock import patch
    side_effect = _make_wu_mocks(
        ['HOME'], {'HOME': _obs('HOME', lat=40.7549, lon=-73.984)})
    with patch('wx_poller.nearby.requests.get', side_effect=side_effect):
        result = fetch_nearby('fake_key')
    assert result == []

def test_fetch_nearby_skips_204_stations():
    from unittest.mock import patch
    side_effect = _make_wu_mocks(
        ['KDEAD01', 'KNYTEST01'],
        {'KDEAD01': 204, 'KNYTEST01': _obs('KNYTEST01')})
    with patch('wx_poller.nearby.requests.get', side_effect=side_effect):
        result = fetch_nearby('fake_key')
    assert len(result) == 1
    assert result[0]['station_id'] == 'KNYTEST01'

def test_fetch_nearby_skips_stale_observations():
    from unittest.mock import patch
    stale = _obs('KSTALE01')
    stale['obsTimeUtc'] = '2020-01-01T00:00:00Z'
    side_effect = _make_wu_mocks(['KSTALE01'], {'KSTALE01': stale})
    with patch('wx_poller.nearby.requests.get', side_effect=side_effect):
        result = fetch_nearby('fake_key')
    assert result == []

def test_fetch_nearby_sorted_by_distance():
    from unittest.mock import patch
    far  = _obs('KFAR01',  lat=40.80, lon=-73.95)
    near = _obs('KNEAR01', lat=40.76, lon=-73.99)
    side_effect = _make_wu_mocks(
        ['KFAR01', 'KNEAR01'], {'KFAR01': far, 'KNEAR01': near})
    with patch('wx_poller.nearby.requests.get', side_effect=side_effect):
        result = fetch_nearby('fake_key')
    assert [s['station_id'] for s in result] == ['KNEAR01', 'KFAR01']

def test_fetch_nearby_handles_trace_precipitation():
    from unittest.mock import patch
    side_effect = _make_wu_mocks(
        ['KNYTEST01'], {'KNYTEST01': _obs('KNYTEST01', precip_rate='T')})
    with patch('wx_poller.nearby.requests.get', side_effect=side_effect):
        result = fetch_nearby('fake_key')
    assert len(result) == 1
    assert result[0]['rain_rate_in_hr'] == 0.0  # trace value, no crash


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
