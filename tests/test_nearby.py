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


# ── network_variance ─────────────────────────────────────────────────────────
from wx_api.nearby import network_variance, MIN_SIGMA_F, MAD_TO_SIGMA


def _st(sid, temp):
    return {'station_id': sid, 'temp_f': temp}


def test_variance_deltas_are_signed_against_home():
    stations, _ = network_variance(70.0, [_st('A', 72.0), _st('B', 68.0), _st('C', 70.0)])
    by_id = {s['station_id']: s for s in stations}
    assert by_id['A']['temp_delta_f'] == 2.0
    assert by_id['B']['temp_delta_f'] == -2.0
    assert by_id['C']['temp_delta_f'] == 0.0


def test_variance_summary_matches_hand_computation():
    # temps 68, 70, 72 -> median 70, deviations 2/0/2 -> MAD 2 -> sigma 2.965
    _, v = network_variance(72.0, [_st('A', 68.0), _st('B', 70.0), _st('C', 72.0)])
    assert v['count'] == 3
    assert v['network_median_f'] == 70.0
    assert v['network_mean_f'] == 70.0
    assert v['network_spread_f'] == 4.0
    assert abs(v['network_sigma_f'] - 2 * MAD_TO_SIGMA) < 0.01
    assert v['home_delta_f'] == 2.0
    assert abs(v['home_ratio'] - 2 / (2 * MAD_TO_SIGMA)) < 0.01


def test_ratio_scales_with_network_spread():
    """The same 2F delta means more when the neighbours agree closely."""
    tight = [_st('A', 69.8), _st('B', 70.0), _st('C', 70.2)]
    loose = [_st('A', 65.0), _st('B', 70.0), _st('C', 75.0)]
    _, v_tight = network_variance(72.0, tight)
    _, v_loose = network_variance(72.0, loose)
    assert v_tight['home_ratio'] > v_loose['home_ratio']


def test_single_bad_sensor_does_not_swamp_the_ratio():
    """
    The motivating case, taken from live data: eight stations agreeing within
    a couple of degrees plus one reading 19F low. A mean-and-stdev yardstick
    lets that one station inflate sigma and mute everyone else. The robust
    one should barely move.
    """
    clean  = [_st(str(i), t) for i, t in enumerate([92, 88, 87, 94, 86, 90, 89, 91])]
    fouled = clean + [_st('bad', 74.0)]
    _, v_clean  = network_variance(93.2, clean)
    _, v_fouled = network_variance(93.2, fouled)
    assert abs(v_fouled['network_sigma_f'] - v_clean['network_sigma_f']) < 1.0
    assert abs(v_fouled['home_ratio'] - v_clean['home_ratio']) < 0.5
    # The plain stdev, kept for reference, does get dragged. That contrast is
    # the whole reason the ratio is not built on it.
    assert v_fouled['network_stdev_f'] > v_clean['network_stdev_f'] * 1.5


def test_bad_sensor_is_flagged_as_an_outlier():
    stations, v = network_variance(93.2, [
        _st(str(i), t) for i, t in enumerate([92, 88, 87, 94, 86, 90, 89, 91])
    ] + [_st('bad', 74.0)])
    by_id = {s['station_id']: s for s in stations}
    assert by_id['bad']['is_outlier'] is True
    assert v['outlier_count'] == 1
    assert all(not s['is_outlier'] for s in stations if s['station_id'] != 'bad')


def test_sigma_is_floored_so_ratio_cannot_explode():
    identical = [_st(str(i), 70.0) for i in range(5)]
    _, v = network_variance(70.5, identical)
    assert v['network_sigma_f'] == round(MIN_SIGMA_F, 2)
    assert abs(v['home_ratio'] - (0.5 / MIN_SIGMA_F)) < 0.01


def test_ratios_are_none_below_minimum_sample():
    stations, v = network_variance(70.0, [_st('A', 72.0), _st('B', 68.0)])
    assert v['network_sigma_f'] is None
    assert v['home_ratio'] is None
    assert v['verdict'] is None
    # Deltas still render: the caller never has to branch on sample size.
    assert stations[0]['temp_delta_f'] == 2.0
    assert stations[0]['temp_ratio'] is None
    # Center and spread survive, they only need two points.
    assert v['network_median_f'] == 70.0


def test_variance_tolerates_missing_and_junk_temps():
    stations, v = network_variance(70.0, [
        _st('A', 72.0), _st('B', None), _st('C', 'n/a'), _st('D', 68.0), _st('E', 70.0),
    ])
    assert v['count'] == 3
    by_id = {s['station_id']: s for s in stations}
    assert by_id['B']['temp_delta_f'] is None
    assert by_id['C']['temp_ratio'] is None
    assert by_id['C']['is_outlier'] is False
    assert by_id['A']['temp_delta_f'] == 2.0


def test_variance_handles_no_home_temp():
    """Without our reading the network still describes itself."""
    stations, v = network_variance(None, [_st('A', 72.0), _st('B', 68.0), _st('C', 70.0)])
    assert v['home_ratio'] is None
    assert v['home_temp_f'] is None
    assert v['network_median_f'] == 70.0
    assert v['network_sigma_f'] is not None
    assert all(s['temp_delta_f'] is None for s in stations)
    # Station ratios are network-relative, so they survive a missing home temp.
    assert all(s['temp_ratio'] is not None for s in stations)


def test_variance_handles_empty_station_list():
    stations, v = network_variance(70.0, [])
    assert stations == []
    assert v['count'] == 0
    assert v['home_ratio'] is None
    assert v['outlier_count'] == 0


def test_verdict_wording_tracks_direction_and_magnitude():
    pack = [_st('A', 69.0), _st('B', 70.0), _st('C', 71.0)]
    _, warm = network_variance(80.0, pack)
    assert 'warmer' in warm['verdict']
    _, cool = network_variance(60.0, [_st('A', 69.0), _st('B', 70.0), _st('C', 71.0)])
    assert 'cooler' in cool['verdict']
    _, flat = network_variance(70.0, [_st('A', 65.0), _st('B', 70.0), _st('C', 75.0)])
    assert flat['verdict'] == 'in line with the network'
