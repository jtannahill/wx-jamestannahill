# Nearby Manhattan Stations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fetch live readings from nearby Manhattan Weather Underground stations on every 5-min poll, expose them via a `/nearby` API route, display a neighborhood comparison strip on the dashboard, and immediately improve rain probability accuracy with a real-time spatial rain boost from upwind stations.

**Architecture:** `wx-poller` calls the WU "nearby stations" endpoint once per poll (one HTTP request returns ~15–20 stations), stores the snapshot in `wx-nearby-snapshots` (DynamoDB, 30-day TTL). `wx-api` reads the latest snapshot for `/nearby` and `/current`, computes a spatial rain boost if any upwind station is raining, and folds it into the `rain_probability` signal. The dashboard renders a compact comparison strip and annotates the RAIN NEXT HR card when the boost is active.

**Tech Stack:** Python 3.12, AWS Lambda (arm64), DynamoDB on-demand, Weather Underground `api.weather.com/v2/pws/observations/nearby`, `unittest.mock`, Chart.js (no changes), vanilla JS.

**Phase 2 (not in this plan — requires 30 days of accumulation):** Once `wx-nearby-snapshots` has 30+ days of history, retrain `wx-ml-fitter` with spatial features: upwind rain rate, nearby humidity max, spatial pressure gradient. The storage built here is the foundation for that.

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `lambdas/wx_poller/nearby.py` | `fetch_nearby()`, `_bearing()`, `_haversine_mi()` |
| Create | `lambdas/wx_api/nearby.py` | `spatial_rain_boost()`, `_fetch_nearby_snapshot()`, `nearby_route()` |
| Create | `tests/test_nearby.py` | Unit tests for geometry + boost logic |
| Modify | `lambdas/wx_poller/handler.py` | Call `fetch_nearby`, write snapshot to DynamoDB |
| Modify | `lambdas/wx_api/handler.py` | `/nearby` route, pass nearby to `rain_probability`, include in `/current` |
| Modify | `lambdas/wx_api/ml.py` | Accept `nearby` param in `rain_probability()`, apply spatial boost |
| Modify | `cdk/wx_stack.py` | `wx-nearby-snapshots` table, env vars, Secrets policy |
| Modify | `dashboard/index.html` | Nearby comparison strip section |
| Modify | `dashboard/app.js` | `renderNearby()`, fetch `/nearby`, annotate rain prob card |
| Modify | `dashboard/style.css` | Nearby section + chip styles |
| Modify | `dashboard/docs.html` | Document feature + spatial boost |

---

## Task 1: WU fetch module + geometry helpers

**Files:**
- Create: `lambdas/wx_poller/nearby.py`
- Create: `tests/test_nearby.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_nearby.py
import sys, os, math
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))

from wx_poller.nearby import _bearing, _haversine_mi, fetch_nearby

def test_bearing_due_east():
    # Point directly east should give ~90°
    b = _bearing(40.75, -74.0, 40.75, -73.9)
    assert 85 < b < 95, f"Expected ~90, got {b}"

def test_bearing_due_north():
    b = _bearing(40.75, -74.0, 40.85, -74.0)
    assert b < 5 or b > 355, f"Expected ~0/360, got {b}"

def test_haversine_central_park():
    # Central Park (40.785, -73.968) is ~2.5 miles from Midtown (40.755, -73.984)
    d = _haversine_mi(40.755, -73.984, 40.785, -73.968)
    assert 2.0 < d < 3.5, f"Expected ~2.5 mi, got {d}"

def test_fetch_nearby_returns_list_on_api_success():
    from unittest.mock import patch, MagicMock
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        'observations': [
            {
                'stationID': 'KNYNEWYO2140',
                'neighborhood': 'Midtown Manhattan',
                'lat': 40.760,
                'lon': -73.990,
                'winddir': 90,
                'humidity': 42,
                'obsTimeLocal': '2026-04-08 12:00:00',
                'imperial': {
                    'temp': 64, 'windSpeed': 8, 'windGust': 12,
                    'pressure': 30.22, 'precipRate': 0.0,
                },
            }
        ]
    }
    with patch('wx_poller.nearby.requests.get', return_value=mock_resp):
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
    # A station at exactly the home lat/lon (distance ~0) should be excluded
    from unittest.mock import patch, MagicMock
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        'observations': [
            {
                'stationID': 'HOME',
                'neighborhood': 'Midtown',
                'lat': 40.7549,   # HOME_LAT
                'lon': -73.984,   # HOME_LON
                'winddir': 90, 'humidity': 42,
                'obsTimeLocal': '2026-04-08 12:00:00',
                'imperial': {'temp': 64, 'windSpeed': 8, 'windGust': 12,
                             'pressure': 30.22, 'precipRate': 0.0},
            }
        ]
    }
    with patch('wx_poller.nearby.requests.get', return_value=mock_resp):
        result = fetch_nearby('fake_key')
    assert result == []
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/jamest/wx-jamestannahill
PYTHONPATH=lambdas pytest tests/test_nearby.py -v
```

Expected: `ModuleNotFoundError: No module named 'wx_poller.nearby'`

- [ ] **Step 3: Create `lambdas/wx_poller/nearby.py`**

```python
"""
Fetch nearby Weather Underground (api.weather.com) PWS stations and
compute bearing/distance from the home station.
"""
import math
import requests

HOME_LAT = 40.7549
HOME_LON = -73.984
WU_NEARBY_URL = "https://api.weather.com/v2/pws/observations/nearby"


def fetch_nearby(api_key: str, limit: int = 20) -> list[dict]:
    """
    Fetch nearby WU stations. Returns a list of normalized dicts, sorted
    by distance, excluding the home station itself.
    Returns [] on any error — this is a non-critical enrichment.

    Each dict has:
      station_id, neighborhood, lat, lon, bearing_deg, distance_mi,
      temp_f, humidity, wind_speed_mph, wind_dir, rain_rate_in_hr,
      pressure_in, observed_at
    """
    try:
        resp = requests.get(
            WU_NEARBY_URL,
            params={
                'geocode': f'{HOME_LAT},{HOME_LON}',
                'limit': limit,
                'units': 'e',
                'format': 'json',
                'apiKey': api_key,
            },
            timeout=8,
        )
        resp.raise_for_status()
        observations = resp.json().get('observations', [])
    except Exception as e:
        print(f"WU nearby fetch failed (non-critical): {e}")
        return []

    results = []
    for obs in observations:
        imp = obs.get('imperial', {})
        lat = obs.get('lat')
        lon = obs.get('lon')
        if lat is None or lon is None:
            continue
        lat, lon = float(lat), float(lon)
        dist = _haversine_mi(HOME_LAT, HOME_LON, lat, lon)
        if dist < 0.05:          # exclude home station
            continue
        results.append({
            'station_id':      obs.get('stationID', ''),
            'neighborhood':    obs.get('neighborhood', ''),
            'lat':             round(lat, 4),
            'lon':             round(lon, 4),
            'bearing_deg':     round(_bearing(HOME_LAT, HOME_LON, lat, lon), 1),
            'distance_mi':     round(dist, 2),
            'temp_f':          imp.get('temp'),
            'humidity':        obs.get('humidity'),
            'wind_speed_mph':  imp.get('windSpeed'),
            'wind_dir':        obs.get('winddir'),
            'rain_rate_in_hr': float(imp.get('precipRate') or 0.0),
            'pressure_in':     imp.get('pressure'),
            'observed_at':     obs.get('obsTimeLocal', ''),
        })

    results.sort(key=lambda s: s['distance_mi'])
    return results


def _bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compass bearing in degrees (0=N, 90=E) from (lat1,lon1) to (lat2,lon2)."""
    dlon  = math.radians(lon2 - lon1)
    lat1r = math.radians(lat1)
    lat2r = math.radians(lat2)
    x = math.sin(dlon) * math.cos(lat2r)
    y = math.cos(lat1r) * math.sin(lat2r) - math.sin(lat1r) * math.cos(lat2r) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _haversine_mi(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in miles."""
    R    = 3958.8
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH=lambdas pytest tests/test_nearby.py -v
```

Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add lambdas/wx_poller/nearby.py tests/test_nearby.py
git commit -m "feat: WU nearby stations fetch module with bearing/distance geometry"
```

---

## Task 2: Spatial rain boost module + tests

**Files:**
- Create: `lambdas/wx_api/nearby.py`
- Modify: `tests/test_nearby.py` (add boost tests)

The spatial boost answers: *"Is any station upwind of Midtown currently raining?"*
Wind direction in meteorology is the direction FROM which wind blows. If wind is from 270° (west), upwind stations are to the west — their bearing from Midtown is also ~270°. So: upwind = `|bearing_to_neighbor − wind_from_dir| < 60°`.

Boost formula: `min(0.35, sqrt(rain_rate) * 0.25 / distance_mi)`. Decays with distance, scales with intensity. A station 0.5 mi away raining at 0.5"/hr → boost of `sqrt(0.5)*0.25/0.5 = 0.354`.

- [ ] **Step 1: Add boost tests to `tests/test_nearby.py`**

Append to the existing file:

```python
# ── spatial_rain_boost tests ──────────────────────────────────────────────────
from wx_api.nearby import spatial_rain_boost

def test_boost_is_zero_when_no_nearby():
    boost, label = spatial_rain_boost([], wind_dir_deg=270)
    assert boost == 0.0
    assert label is None

def test_boost_is_zero_when_wind_dir_none():
    nearby = [{'bearing_deg': 270, 'distance_mi': 0.5,
               'rain_rate_in_hr': 0.5, 'neighborhood': 'Hell\'s Kitchen'}]
    boost, label = spatial_rain_boost(nearby, wind_dir_deg=None)
    assert boost == 0.0

def test_boost_is_zero_when_station_not_raining():
    nearby = [{'bearing_deg': 270, 'distance_mi': 0.5,
               'rain_rate_in_hr': 0.0, 'neighborhood': 'Hell\'s Kitchen'}]
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
               'rain_rate_in_hr': 0.5, 'neighborhood': 'Hell\'s Kitchen'}]
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
         'rain_rate_in_hr': 0.5, 'neighborhood': 'Hell\'s Kitchen'},
    ]
    boost, label = spatial_rain_boost(nearby, wind_dir_deg=270)
    assert label == "Hell's Kitchen"  # closer station wins
```

- [ ] **Step 2: Run new tests to verify they fail**

```bash
PYTHONPATH=lambdas pytest tests/test_nearby.py -k "boost" -v
```

Expected: `ImportError: cannot import name 'spatial_rain_boost' from 'wx_api.nearby'`

- [ ] **Step 3: Create `lambdas/wx_api/nearby.py`**

```python
"""
Nearby station helpers for the wx-api Lambda.

spatial_rain_boost()       — compute upwind rain boost for rain_probability
_fetch_nearby_snapshot()   — read latest snapshot from wx-nearby-snapshots
nearby_route()             — handler for GET /nearby
"""
import json, math, os
from datetime import datetime, timezone

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
        bearing  = s.get('bearing_deg', 0)
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
```

- [ ] **Step 4: Run all nearby tests**

```bash
PYTHONPATH=lambdas pytest tests/test_nearby.py -v
```

Expected: all 13 tests pass

- [ ] **Step 5: Commit**

```bash
git add lambdas/wx_api/nearby.py tests/test_nearby.py
git commit -m "feat: spatial rain boost + nearby snapshot reader"
```

---

## Task 3: CDK infrastructure + WU key in Secrets Manager

**Files:**
- Modify: `cdk/wx_stack.py`

- [ ] **Step 1: Add WU API key to the existing Secrets Manager secret**

First, read the current secret value:

```bash
aws secretsmanager get-secret-value \
  --secret-id ambient-weather/api-keys \
  --region us-east-1 \
  --query 'SecretString' --output text
```

Note the current `api_key` and `application_key` values, then update the secret to add `wu_api_key`:

```bash
aws secretsmanager update-secret \
  --secret-id ambient-weather/api-keys \
  --region us-east-1 \
  --secret-string '{
    "api_key": "<existing_api_key>",
    "application_key": "<existing_application_key>",
    "wu_api_key": "517b29c7000d4e68bb29c7000dae680a"
  }'
```

Expected output: `{"ARN": "arn:aws:secretsmanager:us-east-1:...", "Name": "ambient-weather/api-keys", ...}`

- [ ] **Step 2: Add `wx-nearby-snapshots` table and env vars to CDK stack**

In `cdk/wx_stack.py`, after the `self.station_records_table` block (around line 247), add:

```python
        # --- Nearby stations snapshots table (latest ~30 days, one row per poll) ---
        self.nearby_table = dynamodb.Table(
            self, "WxNearbySnapshots",
            table_name="wx-nearby-snapshots",
            partition_key=dynamodb.Attribute(name="station_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="snapshot_at", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )
```

After the `self.poller_fn.add_environment("UHI_SEASONAL_TABLE", ...)` line, add:

```python
        # Poller writes nearby snapshots
        self.nearby_table.grant_read_write_data(self.poller_fn)
        self.poller_fn.add_environment("NEARBY_TABLE", self.nearby_table.table_name)

        # API reads nearby snapshots
        self.nearby_table.grant_read_data(self.api_fn)
        self.api_fn.add_environment("NEARBY_TABLE", self.nearby_table.table_name)
```

In `http_api.add_routes(...)` block, add the nearby route:

```python
        http_api.add_routes(path="/nearby", methods=[apigwv2.HttpMethod.GET], integration=lambda_integration)
```

- [ ] **Step 3: Deploy**

```bash
cd /Users/jamest/wx-jamestannahill/cdk
npx cdk deploy --require-approval never 2>&1 | tail -20
```

Expected: `✅  WxStack` with `wx-nearby-snapshots` table created

- [ ] **Step 4: Verify table exists**

```bash
aws dynamodb describe-table --table-name wx-nearby-snapshots --region us-east-1 \
  --query 'Table.TableStatus' --output text
```

Expected: `ACTIVE`

- [ ] **Step 5: Commit**

```bash
git add cdk/wx_stack.py
git commit -m "feat: wx-nearby-snapshots table + /nearby API route + NEARBY_TABLE env var"
```

---

## Task 4: Poller — fetch nearby and write snapshot

**Files:**
- Modify: `lambdas/wx_poller/handler.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_poller.py`:

```python
def test_handler_calls_fetch_nearby_and_writes_snapshot():
    """Poller should call fetch_nearby and write to nearby table on each clean reading."""
    import os
    os.environ['NEARBY_TABLE'] = 'wx-nearby-snapshots'

    fake_reading = {
        'tempf': 62.0, 'feelsLike': 59.0, 'humidity': 45,
        'dewPoint': 40.0, 'windspeedmph': 8.0, 'windgustmph': 12.0,
        'winddir': 270, 'baromrelin': 30.22, 'solarradiation': 400.0,
        'uv': 5, 'hourlyrainin': 0.0, 'dailyrainin': 0.0,
        'dateutc': 1744200000000,
    }
    mock_nearby = [
        {'station_id': 'KNYTEST01', 'neighborhood': 'Hell\'s Kitchen',
         'bearing_deg': 270.0, 'distance_mi': 0.4, 'temp_f': 60,
         'humidity': 50, 'wind_speed_mph': 7, 'wind_dir': 270,
         'rain_rate_in_hr': 0.0, 'pressure_in': 30.20, 'observed_at': '2026-04-08 12:00:00'}
    ]

    with patch('wx_poller.handler.get_secret', side_effect=[
        {'api_key': 'k', 'application_key': 'ak', 'wu_api_key': 'wk'},
        {'mac_address': 'AA:BB:CC:DD:EE:FF'},
    ]), \
    patch('wx_poller.handler.fetch_reading', return_value=fake_reading), \
    patch('wx_poller.handler.validate_reading', return_value=(fake_reading, [])), \
    patch('wx_poller.handler.detect_stuck', return_value=False), \
    patch('wx_poller.handler.fetch_uhi', return_value={'uhi_delta': 2.1}), \
    patch('wx_poller.handler.fetch_nearby', return_value=mock_nearby), \
    patch('wx_poller.handler.get_table') as mock_get_table, \
    patch('wx_poller.handler.update_rolling_stats'), \
    patch('wx_poller.handler.generate_og'):
        mock_table = MagicMock()
        mock_table.query.return_value = {'Items': []}
        mock_table.get_item.return_value = {}
        mock_get_table.return_value = mock_table

        from wx_poller.handler import handler
        handler({}, {})

    # put_item should have been called for the reading AND the nearby snapshot
    put_calls = mock_table.put_item.call_args_list
    nearby_calls = [c for c in put_calls
                    if c[1].get('Item', {}).get('stations_json') is not None]
    assert len(nearby_calls) == 1
    import json
    stored = json.loads(nearby_calls[0][1]['Item']['stations_json'])
    assert stored[0]['station_id'] == 'KNYTEST01'
```

- [ ] **Step 2: Run test to verify it fails**

```bash
PYTHONPATH=lambdas pytest tests/test_poller.py::test_handler_calls_fetch_nearby_and_writes_snapshot -v
```

Expected: FAIL (`AttributeError` or `AssertionError` — `fetch_nearby` not imported)

- [ ] **Step 3: Update `lambdas/wx_poller/handler.py`**

Add import at top of file (after existing imports):

```python
from wx_poller.nearby import fetch_nearby
```

Change the `handler()` function — replace the `get_secret` calls and add nearby fetch. Find:

```python
def handler(event, context):
    creds   = get_secret('ambient-weather/api-keys')
    station = get_secret('ambient-weather/station-config')
    mac     = station['mac_address']
```

Replace with:

```python
def handler(event, context):
    creds   = get_secret('ambient-weather/api-keys')
    station = get_secret('ambient-weather/station-config')
    mac     = station['mac_address']
    wu_key  = creds.get('wu_api_key', '')
```

After the `_write_reading(...)` call and before the rolling stats block, add:

```python
    # --- Fetch and store nearby WU stations (non-critical) --------------------
    if quality_flag is None and wu_key:
        nearby = fetch_nearby(wu_key, limit=20)
        if nearby:
            _write_nearby_snapshot(mac, now, nearby)
```

Add the `_write_nearby_snapshot` function at the bottom of the file (before `_decimal`):

```python
def _write_nearby_snapshot(station_id: str, now, nearby: list):
    """Write the latest nearby station snapshot to wx-nearby-snapshots."""
    import json as _json
    table = get_table(os.environ.get('NEARBY_TABLE', 'wx-nearby-snapshots'))
    table.put_item(Item={
        'station_id':   station_id,
        'snapshot_at':  now.isoformat(),
        'stations_json': _json.dumps(nearby),
        'station_count': len(nearby),
        'ttl':          int(__import__('time').time()) + (30 * 86400),
    })
```

- [ ] **Step 4: Run test to verify it passes**

```bash
PYTHONPATH=lambdas pytest tests/test_poller.py::test_handler_calls_fetch_nearby_and_writes_snapshot -v
```

Expected: PASS

- [ ] **Step 5: Run full test suite**

```bash
PYTHONPATH=lambdas pytest tests/ -v --tb=short
```

Expected: all existing tests still pass

- [ ] **Step 6: Commit**

```bash
git add lambdas/wx_poller/handler.py
git commit -m "feat: poller fetches and stores nearby WU station snapshots"
```

---

## Task 5: Update `rain_probability()` to accept and apply spatial boost

**Files:**
- Modify: `lambdas/wx_api/ml.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_api.py`:

```python
def test_rain_probability_with_nearby_upwind_rain():
    """Spatial boost should increase rain probability when upwind station is raining."""
    import os
    os.environ['MODELS_TABLE'] = 'wx-ml-models'
    from wx_api.ml import rain_probability

    reading = {
        'tempf': 62, 'humidity': 75, 'dewPoint': 52,
        'baromrelin': 30.10, 'hourlyrainin': 0.0,
        'winddir': 270,
        'timestamp': '2026-04-08T14:00:00+00:00',
    }
    recent = []

    # Without nearby: baseline probability
    result_no_nearby = rain_probability(reading, recent, nearby=None)
    prob_base = result_no_nearby['probability']

    # Upwind station raining (bearing 270°, wind from 270°)
    nearby_raining = [
        {'bearing_deg': 270, 'distance_mi': 0.5,
         'rain_rate_in_hr': 0.3, 'neighborhood': 'Hell\'s Kitchen'}
    ]
    result_with_nearby = rain_probability(reading, recent, nearby=nearby_raining)
    prob_boosted = result_with_nearby['probability']

    assert prob_boosted > prob_base
    assert result_with_nearby.get('spatial_boost') is not None
    assert result_with_nearby['spatial_source'] == "Hell's Kitchen"

def test_rain_probability_no_boost_downwind_rain():
    """No spatial boost when raining station is downwind."""
    from wx_api.ml import rain_probability

    reading = {
        'tempf': 62, 'humidity': 75, 'dewPoint': 52,
        'baromrelin': 30.10, 'hourlyrainin': 0.0,
        'winddir': 270,
        'timestamp': '2026-04-08T14:00:00+00:00',
    }
    # Station to the east (bearing 90°) while wind is from west (270°) — downwind
    nearby_downwind = [
        {'bearing_deg': 90, 'distance_mi': 0.5,
         'rain_rate_in_hr': 0.5, 'neighborhood': 'Gramercy'}
    ]
    result_no = rain_probability(reading, [], nearby=None)
    result_dw = rain_probability(reading, [], nearby=nearby_downwind)
    assert result_dw['probability'] == result_no['probability']
    assert result_dw.get('spatial_boost') is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH=lambdas pytest tests/test_api.py::test_rain_probability_with_nearby_upwind_rain tests/test_api.py::test_rain_probability_no_boost_downwind_rain -v
```

Expected: FAIL (`TypeError: rain_probability() got an unexpected keyword argument 'nearby'`)

- [ ] **Step 3: Update `lambdas/wx_api/ml.py`**

Change the `rain_probability` signature and body. Find:

```python
def rain_probability(reading: dict, recent_readings: list) -> dict:
    """
    Returns {'probability': 0-100, 'label': str, 'coeff_source': str}.
    recent_readings: newest-first list (from DynamoDB query).
    """
```

Replace with:

```python
def rain_probability(reading: dict, recent_readings: list,
                     nearby: list | None = None) -> dict:
    """
    Returns probability of rain in next 60 minutes (0–100).
    nearby: optional list of nearby station dicts (from wx-nearby-snapshots).
            When provided, upwind stations that are raining add a spatial boost.
    """
```

At the end of the function, find:

```python
    # Boost if it's already raining (persistence)
    if rain_now > 0.01:
        z += 2.5

    prob = max(1, min(99, round(_sigmoid(z) * 100)))

    if   prob <  10: label = 'Unlikely'
    elif prob <  30: label = 'Slight chance'
    elif prob <  55: label = 'Possible'
    elif prob <  75: label = 'Likely'
    else:            label = 'Very likely'

    return {'probability': prob, 'label': label, 'coeff_source': _coeff_source}
```

Replace with:

```python
    # Boost if it's already raining (persistence)
    if rain_now > 0.01:
        z += 2.5

    base_prob = _sigmoid(z)

    # Spatial boost: upwind stations currently raining
    spatial_boost_val = 0.0
    spatial_label     = None
    if nearby:
        from wx_api.nearby import spatial_rain_boost
        wind_dir = reading.get('winddir')
        if wind_dir is not None:
            spatial_boost_val, spatial_label = spatial_rain_boost(nearby, float(wind_dir))

    final_prob = min(0.99, base_prob + spatial_boost_val)
    prob = max(1, min(99, round(final_prob * 100)))

    if   prob <  10: label = 'Unlikely'
    elif prob <  30: label = 'Slight chance'
    elif prob <  55: label = 'Possible'
    elif prob <  75: label = 'Likely'
    else:            label = 'Very likely'

    return {
        'probability':   prob,
        'label':         label,
        'coeff_source':  _coeff_source,
        'spatial_boost': round(spatial_boost_val * 100) if spatial_boost_val > 0 else None,
        'spatial_source': spatial_label,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH=lambdas pytest tests/test_api.py::test_rain_probability_with_nearby_upwind_rain tests/test_api.py::test_rain_probability_no_boost_downwind_rain -v
```

Expected: 2 passed

- [ ] **Step 5: Run full test suite**

```bash
PYTHONPATH=lambdas pytest tests/ -v --tb=short
```

Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add lambdas/wx_api/ml.py
git commit -m "feat: rain_probability accepts nearby stations for spatial upwind rain boost"
```

---

## Task 6: API handler — wire `/nearby` route and pass nearby to `/current`

**Files:**
- Modify: `lambdas/wx_api/handler.py`

- [ ] **Step 1: Add import and NEARBY_TABLE env var**

At the top of `handler.py`, add to imports:

```python
from wx_api.nearby import nearby_route, _fetch_nearby_snapshot
```

After the `RECORDS_TABLE` line (around line 20), add:

```python
NEARBY_TABLE       = os.environ.get('NEARBY_TABLE',       'wx-nearby-snapshots')
```

- [ ] **Step 2: Add `/nearby` route to the `handler()` dispatcher**

Find:

```python
    elif path == '/daily-summaries':
        params = event.get('queryStringParameters') or {}
        days = int(params.get('days', 90))
        return _daily_summaries_route(min(days, 365))
    else:
        return _resp(404, {"error": "Not found"})
```

Replace with:

```python
    elif path == '/daily-summaries':
        params = event.get('queryStringParameters') or {}
        days = int(params.get('days', 90))
        return _daily_summaries_route(min(days, 365))
    elif path == '/nearby':
        return _resp(200, nearby_route(get_secret(STATION_SECRET)['mac_address']))
    else:
        return _resp(404, {"error": "Not found"})
```

- [ ] **Step 3: Pass nearby to `/current` and include in response**

In `_current()`, find the ML signals block:

```python
    # ML signals
    uhi             = fetch_uhi(reading['tempf']) if reading.get('tempf') is not None else {}
    comfort         = comfort_score(reading, baseline, local_now.month)
    pct_rank        = percentile_rank(reading, baseline, local_now.month) if baseline else None
    rain_prob       = rain_probability(reading, recent)
```

Replace with:

```python
    # ML signals
    uhi             = fetch_uhi(reading['tempf']) if reading.get('tempf') is not None else {}
    comfort         = comfort_score(reading, baseline, local_now.month)
    pct_rank        = percentile_rank(reading, baseline, local_now.month) if baseline else None
    nearby          = _fetch_nearby_snapshot(mac)
    rain_prob       = rain_probability(reading, recent, nearby=nearby)
```

In the `body` dict, after `"daily_summary": daily_summary,`, add:

```python
        "nearby_stations":  nearby[:8] if nearby else [],  # cap at 8 for payload size
```

- [ ] **Step 4: Deploy the API Lambda**

```bash
cd /Users/jamest/wx-jamestannahill/cdk
npx cdk deploy --require-approval never 2>&1 | tail -10
```

- [ ] **Step 5: Trigger a poller run to populate `wx-nearby-snapshots`**

```bash
aws lambda invoke \
  --function-name $(aws lambda list-functions --region us-east-1 \
    --query 'Functions[?contains(FunctionName, `WxPoller`)].FunctionName' \
    --output text) \
  --invocation-type Event \
  --region us-east-1 \
  --payload '{}' /dev/null
echo "Poller triggered"
sleep 20
```

- [ ] **Step 6: Verify `/nearby` returns data**

```bash
curl -s "https://api.wx.jamestannahill.com/nearby" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print('count:', d['count']); [print(s['neighborhood'], s['temp_f'], '°F', s['distance_mi'], 'mi') for s in d['stations'][:5]]"
```

Expected: 5–15 nearby stations listed with temperatures and distances

- [ ] **Step 7: Verify `/current` includes nearby_stations**

```bash
curl -s "https://api.wx.jamestannahill.com/current" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); n=d.get('nearby_stations',[]); print('nearby count:', len(n)); rp=d.get('rain_probability',{}); print('rain_prob:', rp.get('probability'), 'boost:', rp.get('spatial_boost'), 'from:', rp.get('spatial_source'))"
```

Expected: `nearby count: 5-8`, `boost: None` (unless it's raining nearby)

- [ ] **Step 8: Commit**

```bash
git add lambdas/wx_api/handler.py lambdas/wx_api/nearby.py
git commit -m "feat: /nearby route + nearby stations in /current + spatial boost wired"
```

---

## Task 7: Dashboard — neighborhood comparison strip

**Files:**
- Modify: `dashboard/index.html`
- Modify: `dashboard/app.js`
- Modify: `dashboard/style.css`

- [ ] **Step 1: Add nearby section to `dashboard/index.html`**

After the conditions-grid closing `</section>` tag (line ~126) and before the comfort-calendar-section, add:

```html
    <section class="nearby-section" id="nearby-section" hidden>
      <div class="section-header">
        <span class="section-title">NEARBY STATIONS</span>
        <span class="section-meta" id="nearby-meta"></span>
      </div>
      <div class="nearby-strip" id="nearby-strip"></div>
    </section>
```

- [ ] **Step 2: Add `renderNearby()` to `dashboard/app.js`**

Add after `renderRainEvents()` and before `loadSecondaryData()`:

```javascript
// ── Nearby stations ───────────────────────────────────────────────────────────
function renderNearby(stations, myTemp) {
  const section = document.getElementById('nearby-section');
  if (!stations || !stations.length) { section.hidden = true; return; }
  section.hidden = false;

  document.getElementById('nearby-meta').textContent =
    `${stations.length} stations · WU network`;

  const strip = document.getElementById('nearby-strip');
  strip.innerHTML = stations.map(s => {
    const temp    = s.temp_f != null ? `${Math.round(s.temp_f)}°F` : '—';
    const delta   = (myTemp != null && s.temp_f != null)
      ? s.temp_f - myTemp : null;
    const deltaStr = delta != null
      ? `<span class="nearby-delta ${delta >= 0 ? 'warm' : 'cool'}">${delta >= 0 ? '+' : ''}${Math.round(delta)}°</span>`
      : '';
    const rainDot = s.rain_rate_in_hr > 0.01
      ? '<span class="nearby-rain-dot" title="Currently raining">●</span>' : '';
    const hood = (s.neighborhood || s.station_id || '').replace('Manhattan', '').replace(', NY', '').trim();
    const dist = s.distance_mi != null ? `${s.distance_mi.toFixed(1)} mi` : '';
    return `<div class="nearby-chip">
      <div class="nearby-hood">${hood}${rainDot}</div>
      <div class="nearby-temp">${temp}${deltaStr}</div>
      <div class="nearby-dist">${dist}</div>
    </div>`;
  }).join('');
}
```

- [ ] **Step 3: Update `renderCurrent()` to call `renderNearby()` and annotate rain prob**

In `renderCurrent(data)`, after the UHI monthly average block, add:

```javascript
  // Nearby stations
  renderNearby(data.nearby_stations || [], data.tempf);

  // Annotate rain probability card with spatial boost
  const rpCard = document.getElementById('rain-prob-label');
  if (data.rain_probability?.spatial_boost) {
    const src = data.rain_probability.spatial_source || 'nearby';
    rpCard.textContent = `${data.rain_probability.label} · rain approaching from ${src}`;
  } else {
    rpCard.textContent = data.rain_probability?.label ?? '—';
  }
```

Note: the existing `rain-prob-label` assignment (`document.getElementById('rain-prob-label').textContent = rp?.label ?? '—';`) must be **removed** since this block replaces it. Find and delete:

```javascript
  document.getElementById('rain-prob-label').textContent = rp?.label ?? '—';
```

- [ ] **Step 4: Add styles to `dashboard/style.css`**

Before `footer { ... }`:

```css
/* ── Nearby stations ─────────────────────────────────────────────────────── */
.nearby-section  { margin-bottom: 48px; }
.nearby-strip    { display: flex; gap: 8px; flex-wrap: wrap; }
.nearby-chip {
  background: var(--surface);
  border: 1px solid var(--border);
  padding: 14px 16px;
  min-width: 110px;
}
.nearby-hood {
  font-size: 10px; letter-spacing: 0.10em; color: var(--muted);
  margin-bottom: 6px; white-space: nowrap; overflow: hidden;
  text-overflow: ellipsis; max-width: 140px;
}
.nearby-temp {
  font-size: 22px; font-weight: 400; letter-spacing: -0.01em;
  display: flex; align-items: baseline; gap: 5px;
}
.nearby-delta   { font-size: 13px; }
.nearby-delta.warm { color: #c8956a; }
.nearby-delta.cool { color: #7aacca; }
.nearby-dist    { font-size: 11px; color: #444; margin-top: 4px; }
.nearby-rain-dot { color: #7aacca; font-size: 10px; margin-left: 4px; }
```

- [ ] **Step 5: Open dashboard locally and verify**

```bash
open /Users/jamest/wx-jamestannahill/dashboard/index.html
```

Verify: NEARBY STATIONS strip appears below conditions-grid with chips showing neighborhood, temp, delta, distance. Rain dot appears on any station currently raining.

- [ ] **Step 6: Commit**

```bash
git add dashboard/index.html dashboard/app.js dashboard/style.css
git commit -m "feat: nearby stations comparison strip + spatial rain boost annotation"
```

---

## Task 8: Deploy dashboard + docs update

**Files:**
- Modify: `dashboard/docs.html`

- [ ] **Step 1: Add NEARBY STATIONS section to `docs.html`**

After the CHART ANOMALY BANDS section (before the API section), add:

```html
    <h2>NEARBY STATIONS</h2>
    <p>On every 5-minute poll, <code>wx-poller</code> calls the Weather Underground station network API to fetch observations from all public PWS within range of Midtown Manhattan:</p>
    <pre><code>GET https://api.weather.com/v2/pws/observations/nearby
    ?geocode=40.7549,-73.984&limit=20&units=e&format=json&apiKey=...</code></pre>
    <p>One HTTP request returns 15–20 nearby stations. The full snapshot is stored in <code>wx-nearby-snapshots</code> (DynamoDB, 30-day TTL). The dashboard shows the 8 nearest as a comparison strip with temperature delta and a rain indicator dot.</p>

    <h2>SPATIAL RAIN BOOST</h2>
    <p>The nearby station data immediately improves rain probability accuracy. On every <code>/current</code> request, the API checks whether any <em>upwind</em> station is currently raining and, if so, adds a boost directly to the model's output probability.</p>
    <p><strong>Upwind definition:</strong> Wind direction in meteorology is the direction FROM which wind blows. A neighboring station is upwind if its bearing from Midtown falls within ±60° of the current wind direction.</p>
    <p><strong>Boost formula:</strong></p>
    <pre><code>boost = min(0.35, sqrt(rain_rate_in_hr) × 0.25 / distance_mi)</code></pre>
    <p>The boost decays with distance and scales with rainfall intensity. A station 0.5 miles upwind raining at 0.3"/hr adds +0.17 to the base sigmoid probability. The dashboard annotates the RAIN NEXT HR card with the source neighborhood: <em>"Slight chance · rain approaching from Hell's Kitchen."</em></p>
    <p>This is a real-time improvement that does not require retraining the logistic regression model. The accumulated snapshot history in <code>wx-nearby-snapshots</code> will later enable a Phase 2 enhancement: retraining the model with spatial features (nearby humidity max, upwind pressure trend) once 30+ days of data have accumulated.</p>
```

Also add to the API table a `/nearby` row:

```html
        <tr>
          <td><code>GET /nearby</code></td>
          <td>Latest snapshot of nearby WU stations: neighborhood, temp, rain rate, wind, distance, bearing. Updated every 5 minutes.</td>
        </tr>
```

Also update the DynamoDB list in INFRASTRUCTURE to add:

```
<code>wx-nearby-snapshots</code> (30-day TTL, one snapshot per poll)
```

And update the Lambda list to mention the poller now fetches nearby stations.

- [ ] **Step 2: Sync dashboard to S3**

```bash
aws s3 sync /Users/jamest/wx-jamestannahill/dashboard/ s3://wx-jamestannahill-dashboard/ \
  --exclude "*.DS_Store" --cache-control "no-cache" --region us-east-1

aws cloudfront create-invalidation \
  --distribution-id E2OIRPWQ2L8LB6 \
  --paths "/*" \
  --region us-east-1 \
  --query 'Invalidation.Status' --output text
```

Expected: `InProgress`

- [ ] **Step 3: Verify live site**

```bash
curl -s "https://wx.jamestannahill.com/docs.html" | grep -c "NEARBY STATIONS"
curl -s "https://api.wx.jamestannahill.com/nearby" | python3 -c "import sys,json; d=json.load(sys.stdin); print('OK:', d['count'], 'stations')"
```

Expected: `1` and `OK: N stations`

- [ ] **Step 4: Final commit**

```bash
git add dashboard/docs.html
git commit -m "docs: nearby stations + spatial rain boost documentation"
```

---

## Self-Review

**Spec coverage:**
- ✅ WU nearby fetch (`wx_poller/nearby.py`)
- ✅ Snapshot storage (`wx-nearby-snapshots`, TTL, poller integration)
- ✅ `/nearby` API route
- ✅ `nearby_stations` in `/current` response (capped at 8)
- ✅ Spatial rain boost in `rain_probability()` — immediate prediction improvement
- ✅ Dashboard comparison strip with temp delta + rain dot
- ✅ RAIN NEXT HR card annotation when boost active
- ✅ Phase 2 (ML retraining with spatial features) documented but deferred — requires 30-day accumulation
- ✅ Docs updated

**Placeholder scan:** No TBDs. All code is complete.

**Type consistency:** `nearby` is always `list[dict]` or `None`. `spatial_rain_boost()` always returns `(float, str | None)`. `rain_probability()` signature is backward-compatible (`nearby` is optional, defaults to `None`).
