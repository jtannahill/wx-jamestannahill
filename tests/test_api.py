import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))

from unittest.mock import patch, MagicMock
from decimal import Decimal

def _make_reading(tempf=61.3, timestamp="2026-04-07T14:35:00+00:00"):
    return {
        'station_id': 'AA:BB:CC',
        'timestamp': timestamp,
        'tempf': Decimal(str(tempf)),
        'feelsLike': Decimal('58.1'),
        'humidity': Decimal('52'),
        'dewPoint': Decimal('43.2'),
        'windspeedmph': Decimal('12.4'),
        'windgustmph': Decimal('18.1'),
        'winddir': Decimal('270'),
        'baromrelin': Decimal('29.92'),
        'solarradiation': Decimal('380'),
        'uv': Decimal('4'),
        'hourlyrainin': Decimal('0'),
        'dailyrainin': Decimal('0.12'),
    }

def test_current_endpoint_returns_200():
    from wx_api.handler import handler
    with patch('wx_api.handler.get_secret') as mock_secret, \
         patch('wx_api.handler.get_table') as mock_table_fn:
        mock_secret.return_value = {'mac_address': 'AA:BB:CC', 'label': 'Midtown Manhattan'}
        mock_table = MagicMock()
        mock_table_fn.return_value = mock_table
        mock_table.query.return_value = {'Items': [_make_reading()]}
        mock_table.get_item.return_value = {
            'Item': {
                'avg_tempf': Decimal('60'), 'avg_humidity': Decimal('55'),
                'avg_windspeedmph': Decimal('10'), 'avg_uv': Decimal('3'),
                'avg_feelsLike': Decimal('57'), 'avg_baromrelin': Decimal('29.90'),
                'sample_count': 100, 'source': 'station'
            }
        }
        event = {'rawPath': '/current', 'requestContext': {'http': {'method': 'GET'}}}
        result = handler(event, {})
    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    assert 'tempf' in body
    assert 'anomalies' in body
    assert 'updated_at' in body

def test_history_endpoint_returns_readings_array():
    from wx_api.handler import handler
    with patch('wx_api.handler.get_secret') as mock_secret, \
         patch('wx_api.handler.get_table') as mock_table_fn:
        mock_secret.return_value = {'mac_address': 'AA:BB:CC', 'label': 'Midtown Manhattan'}
        mock_table = MagicMock()
        mock_table_fn.return_value = mock_table
        mock_table.query.return_value = {'Items': [_make_reading(), _make_reading(tempf=62.0)]}
        event = {
            'rawPath': '/history',
            'requestContext': {'http': {'method': 'GET'}},
            'queryStringParameters': {'hours': '24'},
        }
        result = handler(event, {})
    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    assert 'readings' in body
    assert body['count'] == 2

def test_unknown_route_returns_404():
    from wx_api.handler import handler
    event = {'rawPath': '/unknown', 'requestContext': {'http': {'method': 'GET'}}}
    with patch('wx_api.handler.get_secret'), patch('wx_api.handler.get_table'):
        result = handler(event, {})
    assert result['statusCode'] == 404
