import sys, os, json, time
from unittest.mock import patch, MagicMock, call
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))

from wx_poller.handler import fetch_reading, compute_month_hour, update_rolling_stats

def test_compute_month_hour():
    from datetime import datetime, timezone
    dt = datetime(2026, 4, 7, 9, 35, 0, tzinfo=timezone.utc)
    assert compute_month_hour(dt) == "04-09"

def test_fetch_reading_calls_ambient_api():
    with patch('wx_poller.handler.requests') as mock_requests:
        mock_resp = MagicMock()
        mock_resp.json.return_value = [{"tempf": 61.3, "humidity": 52}]
        mock_requests.get.return_value = mock_resp
        result = fetch_reading("AABBCC", "api_key_val", "app_key_val")
    mock_requests.get.assert_called_once()
    call_args = mock_requests.get.call_args
    assert "AABBCC" in call_args[0][0]
    assert result["tempf"] == 61.3

def test_update_rolling_stats_first_reading():
    mock_table = MagicMock()
    mock_table.get_item.return_value = {}  # no existing item
    reading = {"tempf": 61.3, "feelsLike": 58.1, "humidity": 52,
               "windspeedmph": 12.4, "baromrelin": 29.92, "uv": 4}
    update_rolling_stats(mock_table, "AA:BB:CC", "04-09", reading)
    put_call = mock_table.put_item.call_args[1]['Item']
    assert put_call['avg_tempf'] == 61.3
    assert put_call['sample_count'] == 1
    assert put_call['source'] == 'station'

def test_update_rolling_stats_incremental_average():
    mock_table = MagicMock()
    mock_table.get_item.return_value = {
        'Item': {
            'station_id': 'AA:BB:CC',
            'month_hour': '04-09',
            'avg_tempf': 60.0,
            'avg_feelsLike': 57.0,
            'avg_humidity': 50.0,
            'avg_windspeedmph': 10.0,
            'avg_baromrelin': 29.90,
            'avg_uv': 3.0,
            'sample_count': 100,
            'source': 'station'
        }
    }
    reading = {"tempf": 62.0, "feelsLike": 59.0, "humidity": 54,
               "windspeedmph": 14.0, "baromrelin": 29.94, "uv": 5}
    update_rolling_stats(mock_table, "AA:BB:CC", "04-09", reading)
    put_call = mock_table.put_item.call_args[1]['Item']
    expected_avg = (60.0 * 100 + 62.0) / 101
    assert abs(put_call['avg_tempf'] - expected_avg) < 0.001
    assert put_call['sample_count'] == 101
