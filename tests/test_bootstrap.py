import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))

from unittest.mock import patch, MagicMock
import jwt

def test_build_weatherkit_token_has_correct_claims():
    from wx_bootstrap.weatherkit import build_jwt
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.serialization import (
        Encoding, PrivateFormat, NoEncryption
    )
    private_key = ec.generate_private_key(ec.SECP256R1())
    pem = private_key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()).decode()

    token = build_jwt(
        team_id="P3ZC6ZG46V",
        key_id="TESTKEY001",
        service_id="com.jamestannahill.wx",
        private_key_pem=pem,
    )
    decoded = jwt.decode(token, options={"verify_signature": False})
    assert decoded['iss'] == "P3ZC6ZG46V"
    assert decoded['sub'] == "com.jamestannahill.wx"

def test_parse_comparisons_extracts_temp():
    from wx_bootstrap.weatherkit import parse_comparisons
    mock_response = {
        "historicalComparisons": {
            "comparisons": [
                {"condition": "temperatureMax", "historicAverages": {"daily": {"value": 62.5}}},
                {"condition": "temperatureMin", "historicAverages": {"daily": {"value": 44.2}}},
                {"condition": "precipitationAmount", "historicAverages": {"daily": {"value": 0.12}}},
                {"condition": "humidity", "historicAverages": {"daily": {"value": 55.0}}},
            ]
        }
    }
    result = parse_comparisons(mock_response)
    assert abs(result['avg_tempf'] - (62.5 + 44.2) / 2) < 0.01
    assert result['avg_humidity'] == 55.0
    assert result['avg_precipprob'] == 0.12
