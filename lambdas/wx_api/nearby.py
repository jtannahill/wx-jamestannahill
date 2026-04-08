"""
Nearby station helpers for the wx-api Lambda.

spatial_rain_boost()       — compute upwind rain boost for rain_probability
_fetch_nearby_snapshot()   — read latest snapshot from wx-nearby-snapshots
nearby_route()             — handler for GET /nearby
"""
import json, math, os
from shared.dynamodb import get_table
from boto3.dynamodb.conditions import Key

NEARBY_TABLE = os.environ.get('NEARBY_TABLE', 'wx-nearby-snapshots')


def spatial_rain_boost(nearby: list[dict], wind_dir_deg: float | None) -> tuple[float, str | None]:
    """
    Returns (boost, source_label).
    boost is 0.0–0.35, added directly to sigmoid probability.
    source_label is the neighborhood name of the driving station, or None.

    Upwind = bearing from Midtown to neighbor ≈ wind_from_dir (within ±60°).
    Wind direction convention: the direction FROM which wind blows.
    """
    if not nearby or wind_dir_deg is None:
        return 0.0, None

    best_boost = 0.0
    best_label = None

    for s in nearby:
        rate = float(s.get('rain_rate_in_hr') or 0.0)
        if rate <= 0.01:
            continue
        bearing    = s.get('bearing_deg', 0)
        angle_diff = abs(((bearing - wind_dir_deg + 180) % 360) - 180)
        if angle_diff >= 60:
            continue
        dist  = max(float(s.get('distance_mi') or 1.0), 0.1)
        boost = min(0.35, math.sqrt(rate) * 0.25 / dist)
        if boost > best_boost:
            best_boost = boost
            best_label = s.get('neighborhood') or s.get('station_id', '')

    return round(best_boost, 3), best_label


def _fetch_nearby_snapshot(station_id: str) -> list[dict]:
    """
    Read the most recent nearby snapshot from wx-nearby-snapshots.
    Returns [] if none exists or on any error.
    """
    try:
        table  = get_table(NEARBY_TABLE)
        result = table.query(
            KeyConditionExpression=Key('station_id').eq(station_id),
            ScanIndexForward=False,
            Limit=1,
        )
        items = result.get('Items', [])
        if not items:
            return []
        stations_json = items[0].get('stations_json', '[]')
        return json.loads(stations_json)
    except Exception as e:
        print(f"_fetch_nearby_snapshot failed: {e}")
        return []


def nearby_route(station_id: str) -> dict:
    """
    Returns the latest nearby snapshot as a JSON-serialisable dict.
    Shape: {stations: [...], snapshot_at: str, count: int}
    """
    try:
        table  = get_table(NEARBY_TABLE)
        result = table.query(
            KeyConditionExpression=Key('station_id').eq(station_id),
            ScanIndexForward=False,
            Limit=1,
        )
        items = result.get('Items', [])
        if not items:
            return {'stations': [], 'count': 0, 'snapshot_at': None}
        item = items[0]
        stations = json.loads(item.get('stations_json', '[]'))
        return {
            'stations':    stations,
            'count':       len(stations),
            'snapshot_at': item.get('snapshot_at'),
        }
    except Exception as e:
        print(f"nearby_route failed: {e}")
        return {'stations': [], 'count': 0, 'snapshot_at': None, 'error': str(e)}
