import sys, os, json, io
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import wx_api.weatherkit as wk


def _hourly_payload(n=24, start=None):
    start = start or datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    hours = []
    for i in range(n):
        dt = start + timedelta(hours=i)
        hours.append({
            'forecastStart': dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'temperature': 20.0 + i * 0.5,          # Celsius
            'conditionCode': 'PartlyCloudy',
            'precipitationChance': 0.15,
        })
    return {'forecastHourly': {'hours': hours}}


def _mock_urlopen(payload):
    class _Resp(io.BytesIO):
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False
    return lambda req, timeout=None: _Resp(json.dumps(payload).encode())


def setup_function(_):
    wk._hourly_cache.update({'data': None, 'ts': 0})


def test_fetch_hourly_returns_12_well_formed_entries():
    with patch.object(wk, '_get_jwt', return_value='tok'), \
         patch.object(wk.urllib.request, 'urlopen', _mock_urlopen(_hourly_payload())):
        result = wk.fetch_hourly_forecast({'fake': 'creds'})
    assert isinstance(result, list)
    assert len(result) == 12
    for entry in result:
        assert set(entry) == {'time', 'tempf', 'condition', 'precip_prob'}
        assert entry['time'].endswith('Z')
        assert isinstance(entry['tempf'], int)
        assert entry['condition'] == 'PartlyCloudy'
        assert 0.0 <= entry['precip_prob'] <= 1.0
    # 20°C → 68°F
    assert result[0]['tempf'] == 68


def test_fetch_hourly_skips_past_hours():
    start = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=3)
    with patch.object(wk, '_get_jwt', return_value='tok'), \
         patch.object(wk.urllib.request, 'urlopen', _mock_urlopen(_hourly_payload(start=start))):
        result = wk.fetch_hourly_forecast({'fake': 'creds'})
    first = datetime.fromisoformat(result[0]['time'].replace('Z', '+00:00'))
    now_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    assert first >= now_hour


def test_fetch_hourly_uses_cache_within_ttl():
    with patch.object(wk, '_get_jwt', return_value='tok'), \
         patch.object(wk.urllib.request, 'urlopen', _mock_urlopen(_hourly_payload())) as first:
        wk.fetch_hourly_forecast({'fake': 'creds'})
    # Second call must not hit the network
    with patch.object(wk.urllib.request, 'urlopen', side_effect=AssertionError('network hit')):
        result = wk.fetch_hourly_forecast({'fake': 'creds'})
    assert len(result) == 12


def test_fetch_hourly_returns_none_on_empty_payload():
    with patch.object(wk, '_get_jwt', return_value='tok'), \
         patch.object(wk.urllib.request, 'urlopen', _mock_urlopen({'forecastHourly': {'hours': []}})):
        result = wk.fetch_hourly_forecast({'fake': 'creds'})
    assert result is None
