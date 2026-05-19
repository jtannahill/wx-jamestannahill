import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))

from unittest.mock import patch, MagicMock

from wx_records_tracker.handler import merge_all_time, handler


# --- merge_all_time: the monotonic core ------------------------------------

def test_merge_all_time_from_empty_seeds_observed():
    observed = {
        'temp_high': 89.6, 'temp_high_at': '2026-04-16',
        'temp_low': 10.0,  'temp_low_at':  '2026-01-30',
    }
    out = merge_all_time(None, observed)
    assert out['temp_high'] == 89.6 and out['temp_high_at'] == '2026-04-16'
    assert out['temp_low'] == 10.0 and out['temp_low_at'] == '2026-01-30'


def test_merge_all_time_raises_high_and_takes_new_date():
    existing = {'temp_high': 89.6, 'temp_high_at': '2026-04-16'}
    observed = {'temp_high': 92.1, 'temp_high_at': '2026-05-18'}
    out = merge_all_time(existing, observed)
    assert out['temp_high'] == 92.1
    assert out['temp_high_at'] == '2026-05-18'


def test_merge_all_time_never_lowers_a_high_when_scan_is_cooler():
    # THE regression bug: April's 92.1 must survive a cooler later scan.
    existing = {'temp_high': 92.1, 'temp_high_at': '2026-05-18'}
    observed = {'temp_high': 80.0, 'temp_high_at': '2026-06-02'}
    out = merge_all_time(existing, observed)
    assert out['temp_high'] == 92.1
    assert out['temp_high_at'] == '2026-05-18'


def test_merge_all_time_lows_only_fall():
    existing = {'temp_low': 10.0, 'temp_low_at': '2026-01-30'}
    assert merge_all_time(existing, {'temp_low': 5.0, 'temp_low_at': '2026-02-03'})['temp_low'] == 5.0
    assert merge_all_time(existing, {'temp_low': 22.0, 'temp_low_at': '2026-05-01'})['temp_low'] == 10.0
    assert merge_all_time(existing, {'temp_low': 22.0, 'temp_low_at': '2026-05-01'})['temp_low_at'] == '2026-01-30'


def test_merge_all_time_tie_keeps_existing_date():
    existing = {'temp_high': 89.6, 'temp_high_at': '2026-04-16'}
    observed = {'temp_high': 89.6, 'temp_high_at': '2026-04-20'}
    out = merge_all_time(existing, observed)
    assert out['temp_high_at'] == '2026-04-16'


def test_merge_all_time_preserves_field_absent_from_scan():
    # April's gust record must not vanish when April leaves the 90-day window.
    existing = {'max_gust': 76.0, 'max_gust_at': '2026-04-26'}
    observed = {'temp_high': 70.0, 'temp_high_at': '2026-07-01'}
    out = merge_all_time(existing, observed)
    assert out['max_gust'] == 76.0
    assert out['max_gust_at'] == '2026-04-26'
    assert out['temp_high'] == 70.0


def test_merge_all_time_min_pressure_falls_max_pressure_rises():
    existing = {'min_pressure': 28.89, 'min_pressure_at': '2026-03-17',
                'max_pressure': 30.32, 'max_pressure_at': '2026-03-02'}
    observed = {'min_pressure': 29.50, 'min_pressure_at': '2026-06-01',
                'max_pressure': 30.55, 'max_pressure_at': '2026-06-09'}
    out = merge_all_time(existing, observed)
    assert out['min_pressure'] == 28.89 and out['min_pressure_at'] == '2026-03-17'
    assert out['max_pressure'] == 30.55 and out['max_pressure_at'] == '2026-06-09'


def test_merge_all_time_is_monotonic_over_repeated_cooler_scans():
    rec = merge_all_time(None, {'temp_high': 92.1, 'temp_high_at': '2026-05-18'})
    for d in ('2026-06-01', '2026-07-01', '2026-08-01'):
        rec = merge_all_time(rec, {'temp_high': 75.0, 'temp_high_at': d})
    assert rec['temp_high'] == 92.1
    assert rec['temp_high_at'] == '2026-05-18'


# --- handler: persists the monotonic ALL row -------------------------------

def _reading(ts, **kw):
    base = {'station_id': 'AA:BB', 'timestamp': ts, 'tempf': 70.0,
            'windgustmph': 10.0, 'hourlyrainin': 0.0, 'baromrelin': 30.0}
    base.update(kw)
    return base


@patch('wx_records_tracker.handler.get_secret', return_value={'mac_address': 'AA:BB'})
@patch('wx_records_tracker.handler.get_table')
def test_handler_persists_monotonic_all_time_row(mock_get_table, _sec):
    readings_tbl = MagicMock()
    readings_tbl.query.return_value = {
        'Items': [
            _reading('2026-05-10T16:00:00+00:00', tempf=70.0),
            _reading('2026-05-18T19:00:00+00:00', tempf=88.0),  # scan peak only 88
        ]
    }
    records_tbl = MagicMock()
    # Persisted ALL row already holds a hotter April reading.
    records_tbl.get_item.return_value = {
        'Item': {'station_id': 'AA:BB', 'month': 'ALL',
                 'temp_high': 92.1, 'temp_high_at': '2026-04-16'}
    }
    mock_get_table.side_effect = lambda name: (
        readings_tbl if 'readings' in name else records_tbl
    )

    handler({}, None)

    all_writes = [c.kwargs['Item'] for c in records_tbl.put_item.call_args_list
                  if c.kwargs['Item'].get('month') == 'ALL']
    assert all_writes, "handler must write an ALL row"
    row = all_writes[-1]
    # April's 92.1 must survive even though this scan peaked at 88.
    assert float(row['temp_high']) == 92.1
    assert row['temp_high_at'] == '2026-04-16'
    assert row['scope'] == 'all-time'
