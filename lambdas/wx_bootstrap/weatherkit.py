import time
import requests
import jwt

WEATHERKIT_BASE = "https://weatherkit.apple.com/api/v1"


def build_jwt(team_id: str, key_id: str, service_id: str, private_key_pem: str) -> str:
    now = int(time.time())
    headers = {
        "alg": "ES256",
        "kid": key_id,
        "id": f"{team_id}.{service_id}",
    }
    payload = {
        "iss": team_id,
        "iat": now,
        "exp": now + 3600,
        "sub": service_id,
    }
    return jwt.encode(payload, private_key_pem, algorithm="ES256", headers=headers)


def fetch_historical_comparisons(lat: float, lon: float, token: str) -> dict:
    url = f"{WEATHERKIT_BASE}/weather/en/{lat}/{lon}"
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        params={
            "dataSets": "historicalComparisons",
            "timezone": "America/New_York",
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def parse_comparisons(response: dict) -> dict:
    comparisons = {
        c["condition"]: c["historicAverages"]["daily"]["value"]
        for c in response.get("historicalComparisons", {}).get("comparisons", [])
    }
    temp_max = comparisons.get("temperatureMax", 0)
    temp_min = comparisons.get("temperatureMin", 0)
    return {
        "avg_tempf": (temp_max + temp_min) / 2,
        "avg_humidity": comparisons.get("humidity", 0),
        "avg_precipprob": comparisons.get("precipitationAmount", 0),
        "avg_windspeedmph": comparisons.get("windSpeed", 0),
    }
