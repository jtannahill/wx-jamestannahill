import json
from unittest.mock import patch, MagicMock
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))

from shared.secrets import get_secret

def test_get_secret_parses_json():
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {
        'SecretString': '{"api_key": "abc123", "application_key": "def456"}'
    }
    with patch('shared.secrets.boto3') as mock_boto3:
        mock_boto3.client.return_value = mock_client
        result = get_secret('ambient-weather/api-keys')
    assert result['api_key'] == 'abc123'
    assert result['application_key'] == 'def456'


from shared.dynamodb import get_table

def test_get_table_returns_table_resource():
    with patch('shared.dynamodb.boto3') as mock_boto3:
        mock_resource = MagicMock()
        mock_boto3.resource.return_value = mock_resource
        mock_resource.Table.return_value = MagicMock()
        table = get_table('wx-readings')
    mock_boto3.resource.assert_called_once_with('dynamodb', region_name='us-east-1')
    mock_resource.Table.assert_called_once_with('wx-readings')
