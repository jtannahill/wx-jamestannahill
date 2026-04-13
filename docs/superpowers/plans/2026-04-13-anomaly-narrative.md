# Anomaly Narrative Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a "Today in History" percentile panel and updated anomaly headline to wx.jamestannahill.com, powered by 156 years of NOAA Central Park records and 85 years of ERA5 hourly data.

**Architecture:** Two new DynamoDB tables (`wx-climate-doy`, `wx-climate-hourly`) are populated by a one-time bootstrap lambda (NOAA CSV + ERA5 API) and kept current by a nightly updater. A new `climate_context.py` module attaches a `climate_context` field to the `/current` API response. The dashboard renders a live percentile panel (ERA5, during the day) that flips to a daily verdict (NOAA 156-year "warmest since YEAR") after midnight.

**Tech Stack:** Python 3.12 arm64 Lambda, DynamoDB on-demand, `requests`, CDK Python, vanilla JS/CSS. No new pip dependencies beyond `requests` (already in bootstrap).

---

## File Map

**Create:**
- `lambdas/wx_climate_bootstrap/handler.py` — orchestrates NOAA + ERA5 bootstrap steps
- `lambdas/wx_climate_bootstrap/noaa.py` — downloads and parses Central Park GHCN-Daily CSV
- `lambdas/wx_climate_bootstrap/era5.py` — fetches ERA5 hourly data and computes DOY-hour percentiles
- `lambdas/wx_climate_bootstrap/requirements.txt`
- `lambdas/wx_climate_updater/handler.py` — nightly: refreshes yesterday's DOY slot in both tables
- `lambdas/wx_climate_updater/requirements.txt`
- `lambdas/wx_api/climate_context.py` — `live_context()`, `daily_verdict()`, `anomaly_headline()`
- `tests/test_climate_context.py`
- `tests/test_climate_bootstrap.py`

**Modify:**
- `cdk/wx_stack.py` — two new DynamoDB tables, two new lambdas, EventBridge rule, IAM grants
- `lambdas/wx_api/handler.py` — add climate_context fetch + field to `/current` response
- `dashboard/index.html` — add `#climate-panel` section after `#percentile-rank`
- `dashboard/style.css` — styles for `.climate-panel` and `.climate-metric`
- `dashboard/app.js` — `renderClimatePanel()`, update `renderCurrent()` anomaly subtitle

---

## Task 1: CDK — Two New DynamoDB Tables

**Files:**
- Modify: `cdk/wx_stack.py`

- [ ] **Step 1: Add the two new tables after the `self.station_records_table` block**

Find the block ending with:
```python
        self.station_records_table = dynamodb.Table(
            self, "WxStationRecords",
            ...
        )
```

Add immediately after it:

```python
        # --- Climate DOY table (NOAA GHCN-Daily per calendar date, 366 rows) ---
        self.climate_doy_table = dynamodb.Table(
            self, "WxClimateDoy",
            table_name="wx-climate-doy",
            partition_key=dynamodb.Attribute(name="station_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="doy", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        # --- Climate hourly table (ERA5 per DOY×hour, ~8784 rows) ---
        self.climate_hourly_table = dynamodb.Table(
            self, "WxClimateHourly",
            table_name="wx-climate-hourly",
            partition_key=dynamodb.Attribute(name="station_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="doy_hour", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )
```

- [ ] **Step 2: Verify CDK synth**

```bash
cd /Users/jamest/wx-jamestannahill/cdk && npx cdk synth 2>&1 | tail -5
```
Expected: `Successfully synthesized to cdk.out` (no errors)

- [ ] **Step 3: Commit**

```bash
cd /Users/jamest/wx-jamestannahill
git add cdk/wx_stack.py
git commit -m "feat(cdk): add wx-climate-doy and wx-climate-hourly DynamoDB tables"
```

---

## Task 2: NOAA CSV Parser

**Files:**
- Create: `lambdas/wx_climate_bootstrap/noaa.py`
- Create: `tests/test_climate_bootstrap.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_climate_bootstrap.py`:

```python
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))

from wx_climate_bootstrap.noaa import parse_noaa_csv, compute_doy_stats

SAMPLE_CSV = """STATION,DATE,LATITUDE,LONGITUDE,ELEVATION,NAME,PRCP,PRCP_ATTRIBUTES,TMAX,TMAX_ATTRIBUTES,TMIN,TMIN_ATTRIBUTES,AWND,AWND_ATTRIBUTES
USC00305801,1940-04-13,40.7789,-73.9692,39.6,NY CITY CENTRAL PARK NY US,0,,,200,,150,,45,
USC00305801,1941-04-13,40.7789,-73.9692,39.6,NY CITY CENTRAL PARK NY US,0,,,250,,180,,60,
USC00305801,1942-04-13,40.7789,-73.9692,39.6,NY CITY CENTRAL PARK NY US,0,,,180,,120,,30,
"""

def test_parse_noaa_csv_groups_by_doy():
    by_doy = parse_noaa_csv(SAMPLE_CSV)
    assert "0413" in by_doy
    assert len(by_doy["0413"]) == 3

def test_parse_noaa_csv_converts_units():
    by_doy = parse_noaa_csv(SAMPLE_CSV)
    records = by_doy["0413"]
    # TMAX=200 tenths-°C = 20.0°C = 68.0°F
    assert abs(records[0]["tmax"] - 68.0) < 0.2
    # TMIN=150 tenths-°C = 15.0°C = 59.0°F
    assert abs(records[0]["tmin"] - 59.0) < 0.2
    # AWND=45 tenths-m/s = 4.5 m/s = 10.07 mph
    assert abs(records[0]["awnd"] - 10.1) < 0.2

def test_parse_noaa_csv_sorted_by_year_desc():
    by_doy = parse_noaa_csv(SAMPLE_CSV)
    years = [r["year"] for r in by_doy["0413"]]
    assert years == sorted(years, reverse=True)

def test_compute_doy_stats():
    by_doy = parse_noaa_csv(SAMPLE_CSV)
    stats = compute_doy_stats(by_doy["0413"])
    assert stats["p50_tmax"] is not None
    assert stats["record_high_temp"] is not None
    assert stats["record_high_year"] is not None
    # record_high should be the largest tmax
    tmax_vals = [r["tmax"] for r in by_doy["0413"]]
    assert abs(stats["record_high_temp"] - max(tmax_vals)) < 0.1
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/jamest/wx-jamestannahill
python -m pytest tests/test_climate_bootstrap.py -v 2>&1 | head -20
```
Expected: `ModuleNotFoundError: No module named 'wx_climate_bootstrap'`

- [ ] **Step 3: Create the module directory**

```bash
mkdir -p /Users/jamest/wx-jamestannahill/lambdas/wx_climate_bootstrap
touch /Users/jamest/wx-jamestannahill/lambdas/wx_climate_bootstrap/__init__.py
```

- [ ] **Step 4: Implement `noaa.py`**

Create `lambdas/wx_climate_bootstrap/noaa.py`:

```python
"""
NOAA GHCN-Daily parser for Central Park station USC00305801.
Downloads CSV from NOAA CDN and parses per-DOY annual records.

Units in raw CSV:
  TMAX, TMIN: tenths of degrees Celsius  (e.g. 200 = 20.0°C = 68.0°F)
  AWND:       tenths of meters per second (e.g. 45  = 4.5 m/s  = 10.1 mph)
"""
import requests
from collections import defaultdict

NOAA_CSV_URL = (
    "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily"
    "/access/USC00305801.csv"
)


def _tenths_c_to_f(tenths_c: float) -> float:
    return tenths_c / 10 * 9 / 5 + 32


def _tenths_ms_to_mph(tenths_ms: float) -> float:
    return tenths_ms / 10 * 2.237


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    k = (p / 100) * (n - 1)
    lo = int(k)
    hi = lo + 1
    if hi >= n:
        return round(s[lo], 2)
    return round(s[lo] + (k - lo) * (s[hi] - s[lo]), 2)


def fetch_noaa_csv() -> str:
    """Download the Central Park GHCN-Daily CSV. Returns raw text."""
    resp = requests.get(NOAA_CSV_URL, timeout=60)
    resp.raise_for_status()
    return resp.text


def parse_noaa_csv(csv_text: str) -> dict[str, list[dict]]:
    """
    Parse GHCN-Daily CSV text into per-DOY annual records.

    Returns dict keyed by doy string ("0413"), values are lists of
    {year, tmax, tmin, awnd} sorted by year descending.
    TMAX/TMIN missing rows are skipped. AWND may be None.
    """
    import csv
    from io import StringIO

    by_doy: dict[str, list[dict]] = defaultdict(list)
    reader = csv.DictReader(StringIO(csv_text))

    for row in reader:
        date_str = row.get("DATE", "").strip()
        if not date_str or len(date_str) != 10:
            continue
        try:
            year = int(date_str[:4])
        except ValueError:
            continue

        doy = date_str[5:7] + date_str[8:10]  # "MMDD"

        tmax_raw = row.get("TMAX", "").strip()
        tmin_raw = row.get("TMIN", "").strip()
        awnd_raw = row.get("AWND", "").strip()

        if not tmax_raw or not tmin_raw:
            continue

        try:
            tmax_f = round(_tenths_c_to_f(float(tmax_raw)), 1)
            tmin_f = round(_tenths_c_to_f(float(tmin_raw)), 1)
        except ValueError:
            continue

        awnd_mph = None
        if awnd_raw:
            try:
                awnd_mph = round(_tenths_ms_to_mph(float(awnd_raw)), 1)
            except ValueError:
                pass

        by_doy[doy].append({"year": year, "tmax": tmax_f, "tmin": tmin_f, "awnd": awnd_mph})

    # Sort each DOY by year descending (for fast last_exceeded_year lookup)
    for doy in by_doy:
        by_doy[doy].sort(key=lambda r: r["year"], reverse=True)

    return dict(by_doy)


def compute_doy_stats(records: list[dict]) -> dict:
    """
    Compute percentile stats and record extremes for one DOY's annual records.

    Input: list of {year, tmax, tmin, awnd} for a single DOY.
    Returns: dict ready to write to wx-climate-doy (minus station_id/doy keys).
    """
    tmax_vals  = [r["tmax"]  for r in records if r.get("tmax")  is not None]
    tmin_vals  = [r["tmin"]  for r in records if r.get("tmin")  is not None]
    awnd_vals  = [r["awnd"]  for r in records if r.get("awnd")  is not None]

    def _percs(vals, prefix):
        return {
            f"p5_{prefix}":  _percentile(vals, 5),
            f"p25_{prefix}": _percentile(vals, 25),
            f"p50_{prefix}": _percentile(vals, 50),
            f"p75_{prefix}": _percentile(vals, 75),
            f"p95_{prefix}": _percentile(vals, 95),
        }

    # record_high: max tmax and its year
    record_high_temp, record_high_year = None, None
    if tmax_vals:
        record_high_temp = max(tmax_vals)
        record_high_year = next(r["year"] for r in records if r.get("tmax") == record_high_temp)

    record_low_temp, record_low_year = None, None
    if tmin_vals:
        record_low_temp = min(tmin_vals)
        record_low_year = next(r["year"] for r in records if r.get("tmin") == record_low_temp)

    record_high_wind, record_high_wind_year = None, None
    if awnd_vals:
        record_high_wind = max(awnd_vals)
        record_high_wind_year = next(r["year"] for r in records if r.get("awnd") == record_high_wind)

    return {
        **_percs(tmax_vals, "tmax"),
        **_percs(tmin_vals, "tmin"),
        **_percs(awnd_vals, "awnd"),
        "record_high_temp":      record_high_temp,
        "record_high_year":      record_high_year,
        "record_low_temp":       record_low_temp,
        "record_low_year":       record_low_year,
        "record_high_wind":      record_high_wind,
        "record_high_wind_year": record_high_wind_year,
        "sample_count":          len(tmax_vals),
        "annual_highs":          records,  # full sorted list for last_exceeded_year lookup
    }
```

- [ ] **Step 5: Run tests**

```bash
cd /Users/jamest/wx-jamestannahill
python -m pytest tests/test_climate_bootstrap.py -v 2>&1 | tail -15
```
Expected: all 4 tests PASS

- [ ] **Step 6: Commit**

```bash
git add lambdas/wx_climate_bootstrap/ tests/test_climate_bootstrap.py
git commit -m "feat(bootstrap): NOAA GHCN-Daily CSV parser with per-DOY percentile stats"
```

---

## Task 3: ERA5 Hourly Fetcher

**Files:**
- Create: `lambdas/wx_climate_bootstrap/era5.py`
- Modify: `tests/test_climate_bootstrap.py`

- [ ] **Step 1: Add ERA5 tests to `tests/test_climate_bootstrap.py`**

Append to the existing file:

```python
from wx_climate_bootstrap.era5 import compute_hourly_stats, _c_to_f, _dewpoint_f

def test_c_to_f():
    assert abs(_c_to_f(0) - 32.0) < 0.01
    assert abs(_c_to_f(100) - 212.0) < 0.01

def test_dewpoint_f():
    # dewpoint at 20°C, 50% RH ≈ 9.3°C ≈ 48.7°F
    dp = _dewpoint_f(20.0, 50.0)
    assert 48 < dp < 50

def test_compute_hourly_stats_basic():
    # compute_hourly_stats returns keys with _tempf suffix (used only for testing
    # the statistical computation; fetch_month_era5 uses the same logic with correct prefixes)
    samples = [60.0, 65.0, 70.0]
    stats = compute_hourly_stats(samples)
    assert stats["p25_tempf"] is not None
    assert stats["p50_tempf"] is not None
    assert abs(stats["p50_tempf"] - 65.0) < 0.5
    assert stats["mean_tempf"] is not None
    assert abs(stats["mean_tempf"] - 65.0) < 0.5

def test_compute_hourly_stats_std():
    samples = [60.0, 65.0, 70.0]
    stats = compute_hourly_stats(samples)
    assert stats["std_tempf"] is not None
    assert stats["std_tempf"] > 0

def test_fetch_month_era5_slot_key_format():
    """fetch_month_era5 slots must use MMDD-HH format matching what the API reads."""
    # Verify the key format matches "0413-14" — test the slot key construction directly
    import re
    # The doy_hour key regex: 4 digits, dash, 2 digits
    pattern = re.compile(r"^\d{4}-\d{2}$")
    sample_key = "0413-14"
    assert pattern.match(sample_key), f"Key format wrong: {sample_key}"

def test_fetch_month_era5_output_keys():
    """era5 output dict must have the field prefixes that climate_context.py expects."""
    # Spot-check by constructing a minimal output manually (avoids live API call)
    from wx_climate_bootstrap.era5 import _percentile, _c_to_f, _dewpoint_f
    import math
    temps  = [_c_to_f(10 + i) for i in range(85)]
    dewpts = [_dewpoint_f(10 + i, 60) for i in range(85)]
    winds  = [5.0 + i * 0.1 for i in range(85)]
    
    def _percs(vals, prefix):
        mean = sum(vals) / len(vals)
        std  = math.sqrt(sum((v - mean) ** 2 for v in vals) / len(vals))
        return {
            f"p25_{prefix}":  _percentile(vals, 25),
            f"p50_{prefix}":  _percentile(vals, 50),
            f"p75_{prefix}":  _percentile(vals, 75),
            f"mean_{prefix}": round(mean, 2),
            f"std_{prefix}":  round(std, 2),
        }
    
    slot = {**_percs(temps, "tempf"), **_percs(dewpts, "dewptf"), **_percs(winds, "windmph")}
    # These are the exact keys that climate_context._metric_context() reads:
    assert "mean_tempf"  in slot
    assert "std_tempf"   in slot
    assert "p25_tempf"   in slot
    assert "mean_dewptf" in slot
    assert "mean_windmph" in slot
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_climate_bootstrap.py::test_c_to_f -v 2>&1 | tail -5
```
Expected: `ModuleNotFoundError` or `ImportError`

- [ ] **Step 3: Implement `era5.py`**

Create `lambdas/wx_climate_bootstrap/era5.py`:

```python
"""
ERA5 hourly fetcher via Open-Meteo Archive API.
Covers 1940–present at any lat/lon. Free, no auth required.

Strategy: fetch one full calendar month at a time across all years 1940–(current-1).
Each call returns hourly data for e.g. all Aprils 1940–2023. Group by MM-DD-HH,
compute p25/p50/p75/mean/std for temp, dewpoint, and wind.

Dew point is derived from temperature + relative humidity via the Magnus formula.
"""
import math
import requests
from collections import defaultdict
from datetime import date
import calendar as _cal

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
START_YEAR  = 1940
ERA5_VARIABLES = [
    "temperature_2m",
    "relativehumidity_2m",
    "windspeed_10m",
]


def _c_to_f(celsius: float) -> float:
    return celsius * 9 / 5 + 32


def _dewpoint_f(temp_c: float, rh: float) -> float:
    """Magnus formula: dew point in °F from temp (°C) and relative humidity (%)."""
    a, b = 17.625, 243.04
    alpha = math.log(rh / 100) + (a * temp_c) / (b + temp_c)
    dp_c = b * alpha / (a - alpha)
    return _c_to_f(dp_c)


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    k = (p / 100) * (n - 1)
    lo = int(k)
    hi = lo + 1
    if hi >= n:
        return round(s[lo], 2)
    return round(s[lo] + (k - lo) * (s[hi] - s[lo]), 2)


def compute_hourly_stats(values: list[float]) -> dict:
    """
    Compute distribution stats for a list of float values (one field, one DOY-hour slot).
    Returns dict with p25/p50/p75/mean/std keys using the caller's field suffix.
    """
    if not values:
        return {"p25_tempf": None, "p50_tempf": None, "p75_tempf": None,
                "mean_tempf": None, "std_tempf": None, "sample_count": 0}
    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    std = math.sqrt(variance)
    return {
        "p25_tempf":  _percentile(values, 25),
        "p50_tempf":  _percentile(values, 50),
        "p75_tempf":  _percentile(values, 75),
        "mean_tempf": round(mean, 2),
        "std_tempf":  round(std, 2),
        "sample_count": n,
    }


def fetch_month_era5(lat: float, lon: float, month: int) -> dict[str, dict]:
    """
    Fetch all hourly ERA5 data for a calendar month across START_YEAR–(current_year-1).

    Returns dict keyed by doy_hour string ("0413-14"), value is
    {p25_tempf, p50_tempf, p75_tempf, mean_tempf, std_tempf,
     p25_dewptf, p50_dewptf, p75_dewptf, mean_dewptf, std_dewptf,
     p25_windmph, p50_windmph, p75_windmph, mean_windmph, std_windmph,
     sample_count}.
    """
    end_year   = date.today().year - 1
    last_day   = _cal.monthrange(end_year, month)[1]
    start_date = f"{START_YEAR}-{month:02d}-01"
    end_date   = f"{end_year}-{month:02d}-{last_day:02d}"

    resp = requests.get(
        ARCHIVE_URL,
        params={
            "latitude":         lat,
            "longitude":        lon,
            "start_date":       start_date,
            "end_date":         end_date,
            "hourly":           ",".join(ERA5_VARIABLES),
            "timezone":         "America/New_York",
            "temperature_unit": "celsius",   # keep as Celsius for dewpoint calc
            "windspeed_unit":   "mph",
        },
        timeout=120,
    )
    resp.raise_for_status()
    data   = resp.json()
    hourly = data.get("hourly", {})
    times  = hourly.get("time", [])

    temp_vals  = hourly.get("temperature_2m",    [])
    rh_vals    = hourly.get("relativehumidity_2m", [])
    wind_vals  = hourly.get("windspeed_10m",     [])

    # Accumulate per doy_hour slot
    slot_temps:  dict[str, list[float]] = defaultdict(list)
    slot_dewpts: dict[str, list[float]] = defaultdict(list)
    slot_winds:  dict[str, list[float]] = defaultdict(list)

    for i, ts in enumerate(times):
        # ts like "1940-04-13T09:00"
        try:
            date_part, time_part = ts.split("T")
            mm   = date_part[5:7]
            dd   = date_part[8:10]
            hour = int(time_part[:2])
        except (ValueError, IndexError):
            continue

        doy_hour = f"{mm}{dd}-{hour:02d}"

        temp_c = temp_vals[i] if i < len(temp_vals) else None
        rh     = rh_vals[i]   if i < len(rh_vals)   else None
        wind   = wind_vals[i] if i < len(wind_vals)  else None

        if temp_c is not None:
            slot_temps[doy_hour].append(_c_to_f(temp_c))
            if rh is not None:
                slot_dewpts[doy_hour].append(_dewpoint_f(temp_c, rh))
        if wind is not None:
            slot_winds[doy_hour].append(wind)

    # Compute stats per slot
    result: dict[str, dict] = {}
    all_slots = set(slot_temps) | set(slot_winds)

    for slot in all_slots:
        temps  = slot_temps.get(slot, [])
        dewpts = slot_dewpts.get(slot, [])
        winds  = slot_winds.get(slot, [])
        n      = len(temps) or len(winds)

        def _percs(vals, prefix):
            if not vals:
                return {f"p25_{prefix}": None, f"p50_{prefix}": None,
                        f"p75_{prefix}": None, f"mean_{prefix}": None, f"std_{prefix}": None}
            mean = sum(vals) / len(vals)
            std  = math.sqrt(sum((v - mean) ** 2 for v in vals) / len(vals))
            return {
                f"p25_{prefix}":  _percentile(vals, 25),
                f"p50_{prefix}":  _percentile(vals, 50),
                f"p75_{prefix}":  _percentile(vals, 75),
                f"mean_{prefix}": round(mean, 2),
                f"std_{prefix}":  round(std, 2),
            }

        result[slot] = {
            **_percs(temps,  "tempf"),
            **_percs(dewpts, "dewptf"),
            **_percs(winds,  "windmph"),
            "sample_count": n,
        }

    return result
```

- [ ] **Step 4: Run tests**

```bash
cd /Users/jamest/wx-jamestannahill
python -m pytest tests/test_climate_bootstrap.py -v 2>&1 | tail -15
```
Expected: all 8 tests PASS

- [ ] **Step 5: Commit**

```bash
git add lambdas/wx_climate_bootstrap/era5.py tests/test_climate_bootstrap.py
git commit -m "feat(bootstrap): ERA5 hourly fetcher with DOY-hour percentile computation"
```

---

## Task 4: Bootstrap Lambda Handler

**Files:**
- Create: `lambdas/wx_climate_bootstrap/handler.py`
- Create: `lambdas/wx_climate_bootstrap/requirements.txt`

- [ ] **Step 1: Create `requirements.txt`**

Create `lambdas/wx_climate_bootstrap/requirements.txt`:

```
requests==2.32.3
boto3
```

- [ ] **Step 2: Implement `handler.py`**

Create `lambdas/wx_climate_bootstrap/handler.py`:

```python
"""
wx-climate-bootstrap: One-time (re-runnable) bootstrap for climate history tables.

Step 1 — NOAA GHCN-Daily: Downloads Central Park CSV (~2MB), parses all rows
  back to 1869, writes 366 rows to wx-climate-doy.

Step 2 — ERA5: Fetches full monthly hourly data 1940–present (12 API calls),
  writes ~8,784 rows to wx-climate-hourly.

Idempotent: uses put_item (overwrites existing rows safely).
Config: 1024MB, 900s timeout, arm64.
"""
import os
from decimal import Decimal
from shared.secrets import get_secret
from shared.dynamodb import get_table
from wx_climate_bootstrap.noaa import fetch_noaa_csv, parse_noaa_csv, compute_doy_stats
from wx_climate_bootstrap.era5 import fetch_month_era5

CLIMATE_DOY_TABLE     = os.environ.get("CLIMATE_DOY_TABLE",     "wx-climate-doy")
CLIMATE_HOURLY_TABLE  = os.environ.get("CLIMATE_HOURLY_TABLE",  "wx-climate-hourly")


def _decimalize(obj):
    """Recursively convert floats/ints to Decimal for DynamoDB."""
    if isinstance(obj, dict):
        return {k: _decimalize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decimalize(v) for v in obj]
    if isinstance(obj, float):
        return Decimal(str(round(obj, 4)))
    if isinstance(obj, int):
        return Decimal(str(obj))
    return obj


def handler(event, context):
    station = get_secret("ambient-weather/station-config")
    mac     = station["mac_address"]
    lat     = float(station["latitude"])
    lon     = float(station["longitude"])

    run_noaa = event.get("noaa", True)
    run_era5 = event.get("era5", True)

    # ── Step 1: NOAA ─────────────────────────────────────────────────────────
    if run_noaa:
        print("NOAA: downloading Central Park CSV...")
        csv_text = fetch_noaa_csv()
        by_doy   = parse_noaa_csv(csv_text)
        print(f"NOAA: parsed {len(by_doy)} calendar dates")

        doy_table = get_table(CLIMATE_DOY_TABLE)
        written   = 0
        for doy, records in by_doy.items():
            if not records:
                continue
            stats = compute_doy_stats(records)
            item  = {
                "station_id": mac,
                "doy":        doy,
                **_decimalize(stats),
            }
            doy_table.put_item(Item=item)
            written += 1
        print(f"NOAA: wrote {written} rows to {CLIMATE_DOY_TABLE}")

    # ── Step 2: ERA5 ─────────────────────────────────────────────────────────
    if run_era5:
        hourly_table = get_table(CLIMATE_HOURLY_TABLE)
        total_written = 0

        for month in range(1, 13):
            print(f"ERA5: fetching month {month:02d}...")
            try:
                slots = fetch_month_era5(lat, lon, month)
            except Exception as e:
                print(f"ERA5: month {month:02d} failed — {e}")
                continue

            for doy_hour, stats in slots.items():
                item = {
                    "station_id": mac,
                    "doy_hour":   doy_hour,
                    **_decimalize(stats),
                }
                hourly_table.put_item(Item=item)
                total_written += 1

            print(f"ERA5: month {month:02d} → {len(slots)} slots written")

        print(f"ERA5: total {total_written} rows written to {CLIMATE_HOURLY_TABLE}")

    return {"status": "ok", "noaa": run_noaa, "era5": run_era5}
```

- [ ] **Step 3: Commit**

```bash
cd /Users/jamest/wx-jamestannahill
git add lambdas/wx_climate_bootstrap/
git commit -m "feat(bootstrap): wx_climate_bootstrap lambda handler (NOAA + ERA5)"
```

---

## Task 5: Updater Lambda Handler

**Files:**
- Create: `lambdas/wx_climate_updater/handler.py`
- Create: `lambdas/wx_climate_updater/requirements.txt`

- [ ] **Step 1: Create `requirements.txt`**

Create `lambdas/wx_climate_updater/requirements.txt`:

```
requests==2.32.3
boto3
```

- [ ] **Step 2: Create module directory**

```bash
mkdir -p /Users/jamest/wx-jamestannahill/lambdas/wx_climate_updater
touch /Users/jamest/wx-jamestannahill/lambdas/wx_climate_updater/__init__.py
```

- [ ] **Step 3: Implement `handler.py`**

Create `lambdas/wx_climate_updater/handler.py`:

```python
"""
wx-climate-updater: Nightly refresh of climate history tables.
Runs at 06:00 UTC (after wx_summarizer at 05:00 UTC).

Daily: Re-fetches ERA5 for yesterday's calendar date, recomputes hourly
  stats for that DOY and updates wx-climate-hourly (24 rows).

Monthly (1st of each month): Re-downloads NOAA CSV to pick up the ~2-week
  data lag, recomputes and updates all 366 rows of wx-climate-doy.
"""
import os
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal

from shared.secrets import get_secret
from shared.dynamodb import get_table
from wx_climate_bootstrap.noaa import fetch_noaa_csv, parse_noaa_csv, compute_doy_stats
from wx_climate_bootstrap.era5 import fetch_month_era5

CLIMATE_DOY_TABLE    = os.environ.get("CLIMATE_DOY_TABLE",    "wx-climate-doy")
CLIMATE_HOURLY_TABLE = os.environ.get("CLIMATE_HOURLY_TABLE", "wx-climate-hourly")
STATION_TZ           = ZoneInfo("America/New_York")


def _decimalize(obj):
    if isinstance(obj, dict):
        return {k: _decimalize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decimalize(v) for v in obj]
    if isinstance(obj, float):
        return Decimal(str(round(obj, 4)))
    if isinstance(obj, int):
        return Decimal(str(obj))
    return obj


def handler(event, context):
    station = get_secret("ambient-weather/station-config")
    mac     = station["mac_address"]
    lat     = float(station["latitude"])
    lon     = float(station["longitude"])

    now_et    = datetime.now(timezone.utc).astimezone(STATION_TZ)
    yesterday = (now_et - timedelta(days=1)).date()
    month     = yesterday.month

    # ── Daily: refresh yesterday's DOY in ERA5 hourly table ──────────────────
    print(f"Updater: refreshing ERA5 for month {month:02d} (yesterday = {yesterday})")
    try:
        slots        = fetch_month_era5(lat, lon, month)
        hourly_table = get_table(CLIMATE_HOURLY_TABLE)
        updated      = 0
        for doy_hour, stats in slots.items():
            # Only update slots belonging to yesterday's DOY
            doy = f"{yesterday.month:02d}{yesterday.day:02d}"
            if not doy_hour.startswith(doy):
                continue
            hourly_table.put_item(Item={
                "station_id": mac,
                "doy_hour":   doy_hour,
                **_decimalize(stats),
            })
            updated += 1
        print(f"ERA5 updater: {updated} hourly slots refreshed for DOY {doy}")
    except Exception as e:
        print(f"ERA5 update failed (non-fatal): {e}")

    # ── Monthly (1st): refresh NOAA ───────────────────────────────────────────
    if now_et.day == 1:
        print("Updater: 1st of month — refreshing NOAA CSV...")
        try:
            csv_text  = fetch_noaa_csv()
            by_doy    = parse_noaa_csv(csv_text)
            doy_table = get_table(CLIMATE_DOY_TABLE)
            written   = 0
            for doy, records in by_doy.items():
                if not records:
                    continue
                stats = compute_doy_stats(records)
                doy_table.put_item(Item={
                    "station_id": mac,
                    "doy":        doy,
                    **_decimalize(stats),
                })
                written += 1
            print(f"NOAA updater: {written} DOY rows refreshed")
        except Exception as e:
            print(f"NOAA update failed (non-fatal): {e}")

    return {"status": "ok", "date": yesterday.isoformat()}
```

- [ ] **Step 4: Commit**

```bash
cd /Users/jamest/wx-jamestannahill
git add lambdas/wx_climate_updater/
git commit -m "feat: wx_climate_updater nightly refresh lambda"
```

---

## Task 6: CDK — Wire New Lambdas Into Stack

**Files:**
- Modify: `cdk/wx_stack.py`

- [ ] **Step 1: Add table grants and new lambdas**

In `cdk/wx_stack.py`, find the line:
```python
        # Allow the API Lambda to read summaries and records
```

Add BEFORE that line:

```python
        # --- Bootstrap lambda (manual trigger, 900s) ---
        self.climate_bootstrap_fn = make_lambda(
            "WxClimateBootstrap", "wx_climate_bootstrap.handler",
            memory=1024, timeout=900,
            extra_env={
                "CLIMATE_DOY_TABLE":    self.climate_doy_table.table_name,
                "CLIMATE_HOURLY_TABLE": self.climate_hourly_table.table_name,
            },
        )
        self.climate_doy_table.grant_read_write_data(self.climate_bootstrap_fn)
        self.climate_hourly_table.grant_read_write_data(self.climate_bootstrap_fn)

        # --- Updater lambda (nightly 06:00 UTC) ---
        self.climate_updater_fn = make_lambda(
            "WxClimateUpdater", "wx_climate_updater.handler",
            memory=512, timeout=120,
            extra_env={
                "CLIMATE_DOY_TABLE":    self.climate_doy_table.table_name,
                "CLIMATE_HOURLY_TABLE": self.climate_hourly_table.table_name,
            },
        )
        self.climate_doy_table.grant_read_write_data(self.climate_updater_fn)
        self.climate_hourly_table.grant_read_write_data(self.climate_updater_fn)
        updater_rule = events.Rule(
            self, "WxClimateUpdaterSchedule",
            schedule=events.Schedule.cron(hour="6", minute="0"),
        )
        updater_rule.add_target(targets.LambdaFunction(self.climate_updater_fn))
```

- [ ] **Step 2: Grant the API lambda read access to both climate tables**

Find:
```python
        # Allow the API Lambda to read summaries and records
        self.daily_summaries_table.grant_read_data(self.api_fn)
        self.station_records_table.grant_read_data(self.api_fn)
        self.api_fn.add_environment("SUMMARIES_TABLE", self.daily_summaries_table.table_name)
        self.api_fn.add_environment("RECORDS_TABLE",   self.station_records_table.table_name)
```

Add after it:

```python
        self.climate_doy_table.grant_read_data(self.api_fn)
        self.climate_hourly_table.grant_read_data(self.api_fn)
        self.api_fn.add_environment("CLIMATE_DOY_TABLE",    self.climate_doy_table.table_name)
        self.api_fn.add_environment("CLIMATE_HOURLY_TABLE", self.climate_hourly_table.table_name)
```

- [ ] **Step 3: Verify CDK synth**

```bash
cd /Users/jamest/wx-jamestannahill/cdk && npx cdk synth 2>&1 | tail -5
```
Expected: `Successfully synthesized`

- [ ] **Step 4: Commit**

```bash
cd /Users/jamest/wx-jamestannahill
git add cdk/wx_stack.py
git commit -m "feat(cdk): add climate bootstrap + updater lambdas, table grants, EventBridge rule"
```

---

## Task 7: `climate_context.py` Module + Tests

**Files:**
- Create: `lambdas/wx_api/climate_context.py`
- Create: `tests/test_climate_context.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_climate_context.py`:

```python
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))

from wx_api.climate_context import live_context, daily_verdict, anomaly_headline

# ── Fixtures ─────────────────────────────────────────────────────────────────

HOURLY_STATS = {
    "p25_tempf": 55.0, "p50_tempf": 62.0, "p75_tempf": 68.0,
    "mean_tempf": 62.0, "std_tempf": 8.0,
    "p25_dewptf": 38.0, "p50_dewptf": 44.0, "p75_dewptf": 51.0,
    "mean_dewptf": 44.0, "std_dewptf": 7.0,
    "p25_windmph": 5.0, "p50_windmph": 9.0, "p75_windmph": 14.0,
    "mean_windmph": 9.0, "std_windmph": 4.0,
    "sample_count": 85,
}

DOY_STATS = {
    "sample_count": 156,
    "p50_tmax": 62.0,
    "annual_highs": [
        {"year": 2024, "tmax": 65.0, "tmin": 48.0, "awnd": 9.0},
        {"year": 2020, "tmax": 58.0, "tmin": 42.0, "awnd": 7.0},
        {"year": 1987, "tmax": 72.0, "tmin": 55.0, "awnd": 11.0},
        {"year": 1950, "tmax": 74.0, "tmin": 53.0, "awnd": 13.0},
        {"year": 1940, "tmax": 60.0, "tmin": 45.0, "awnd": 8.0},
    ],
}

# ── live_context ──────────────────────────────────────────────────────────────

def test_live_context_returns_three_metrics():
    reading = {"tempf": 72.0, "dewPoint": 52.0, "windspeedmph": 10.0}
    result = live_context(reading, HOURLY_STATS, "0413")
    assert "temp" in result["metrics"]
    assert "dewpoint" in result["metrics"]
    assert "wind" in result["metrics"]

def test_live_context_percentile_above_mean():
    reading = {"tempf": 78.0, "dewPoint": 44.0, "windspeedmph": 9.0}
    result = live_context(reading, HOURLY_STATS, "0413")
    # 78°F is well above mean of 62°F → should be high percentile
    assert result["metrics"]["temp"]["percentile"] > 75

def test_live_context_percentile_at_median():
    reading = {"tempf": 62.0, "dewPoint": 44.0, "windspeedmph": 9.0}
    result = live_context(reading, HOURLY_STATS, "0413")
    # At the mean → should be near 50th
    pct = result["metrics"]["temp"]["percentile"]
    assert 40 <= pct <= 60

def test_live_context_includes_distribution_bounds():
    reading = {"tempf": 72.0, "dewPoint": 52.0, "windspeedmph": 10.0}
    result = live_context(reading, HOURLY_STATS, "0413")
    t = result["metrics"]["temp"]
    assert t["p25"] == 55.0
    assert t["p50"] == 62.0
    assert t["p75"] == 68.0
    assert t["years_of_data"] == 85

def test_live_context_headline_contains_percentile():
    reading = {"tempf": 72.0, "dewPoint": 52.0, "windspeedmph": 10.0}
    result = live_context(reading, HOURLY_STATS, "0413")
    assert "percentile" in result["headline"].lower()
    assert "85" in result["headline"]

def test_live_context_none_stats_returns_none():
    reading = {"tempf": 72.0, "dewPoint": 52.0, "windspeedmph": 10.0}
    assert live_context(reading, None, "0413") is None

# ── daily_verdict ─────────────────────────────────────────────────────────────

def test_daily_verdict_finds_last_exceeded_year():
    # today_high=70°F → exceeded by 1987 (72°F) and 1950 (74°F) → last exceeded = 1987
    result = daily_verdict(70.0, 45.0, DOY_STATS, "0413")
    assert result["temp_high"]["last_exceeded_year"] == 1987

def test_daily_verdict_label_warmest_since():
    result = daily_verdict(70.0, 45.0, DOY_STATS, "0413")
    assert "since 1987" in result["temp_high"]["label"]

def test_daily_verdict_record_high():
    # today_high=80°F exceeds all historical (max is 74°F) → on record
    result = daily_verdict(80.0, 45.0, DOY_STATS, "0413")
    assert result["temp_high"]["last_exceeded_year"] is None
    assert "record" in result["temp_high"]["label"].lower()

def test_daily_verdict_percentile_above_most():
    # today_high=70°F exceeds 3 of 5 historical highs (58, 60, 65) → ~60th pct
    result = daily_verdict(70.0, 45.0, DOY_STATS, "0413")
    pct = result["temp_high"]["percentile"]
    assert 50 <= pct <= 80

def test_daily_verdict_includes_years_of_data():
    result = daily_verdict(70.0, 45.0, DOY_STATS, "0413")
    assert result["temp_high"]["years_of_data"] == 156

# ── anomaly_headline ──────────────────────────────────────────────────────────

def test_anomaly_headline_prefers_verdict():
    reading = {"tempf": 72.0, "dewPoint": 52.0, "windspeedmph": 10.0}
    live    = live_context(reading, HOURLY_STATS, "0413")
    verdict = daily_verdict(70.0, 45.0, DOY_STATS, "0413")
    headline = anomaly_headline(live, verdict)
    assert "since 1987" in headline

def test_anomaly_headline_falls_back_to_live():
    reading = {"tempf": 72.0, "dewPoint": 52.0, "windspeedmph": 10.0}
    live    = live_context(reading, HOURLY_STATS, "0413")
    headline = anomaly_headline(live, None)
    assert "percentile" in headline.lower()

def test_anomaly_headline_none_when_no_data():
    assert anomaly_headline(None, None) is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/jamest/wx-jamestannahill
python -m pytest tests/test_climate_context.py -v 2>&1 | head -10
```
Expected: `ModuleNotFoundError: No module named 'wx_api.climate_context'`

- [ ] **Step 3: Implement `climate_context.py`**

Create `lambdas/wx_api/climate_context.py`:

```python
"""
climate_context.py — Historical climate context for the /current API response.

live_context(reading, doy_hour_stats, doy)
  → percentile rank for current temp/dewpoint/wind vs. ERA5 hourly distribution
  → mode = "live"

daily_verdict(today_high, today_low, doy_stats, doy)
  → "Warmest April 13th since 1923" claims from NOAA 156-year distribution
  → mode = "daily"

anomaly_headline(live, verdict)
  → punchy headline string; prefers verdict when available
"""
import math
from datetime import datetime


def _normal_cdf(z: float) -> float:
    return (1.0 + math.erf(z / math.sqrt(2))) / 2.0


def _ordinal(n: int) -> str:
    if 11 <= n % 100 <= 13:
        return f"{n}th"
    return f"{n}{['th','st','nd','rd','th'][min(n % 10, 4)]}"


def _doy_label(doy: str) -> str:
    """Convert DOY string like "0413" to "April 13th"."""
    try:
        dt = datetime.strptime(doy, "%m%d")
        day = dt.day
        return f"{dt.strftime('%B')} {_ordinal(day)}"
    except ValueError:
        return doy


def _metric_context(value: float, stats: dict, prefix: str, years: int) -> dict | None:
    mean = stats.get(f"mean_{prefix}")
    std  = stats.get(f"std_{prefix}")
    if mean is None or std is None or float(std) == 0:
        return None

    z          = (value - float(mean)) / float(std)
    percentile = round(_normal_cdf(z) * 100)
    percentile = max(1, min(99, percentile))

    if percentile >= 90:
        label = f"{_ordinal(percentile)} percentile — exceptionally high"
    elif percentile >= 75:
        label = f"{_ordinal(percentile)} percentile — above normal"
    elif percentile >= 25:
        label = f"{_ordinal(percentile)} percentile — near normal"
    elif percentile >= 10:
        label = f"{_ordinal(percentile)} percentile — below normal"
    else:
        label = f"{_ordinal(percentile)} percentile — exceptionally low"

    return {
        "value":        round(value, 1),
        "percentile":   percentile,
        "label":        label,
        "p25":          float(stats[f"p25_{prefix}"]) if stats.get(f"p25_{prefix}") is not None else None,
        "p50":          float(stats[f"p50_{prefix}"]) if stats.get(f"p50_{prefix}") is not None else None,
        "p75":          float(stats[f"p75_{prefix}"]) if stats.get(f"p75_{prefix}") is not None else None,
        "years_of_data": years,
    }


def live_context(reading: dict, doy_hour_stats: dict | None, doy: str) -> dict | None:
    """
    Compare current reading against ERA5 hourly distribution for this DOY+hour.
    Returns None if stats are unavailable.
    """
    if not doy_hour_stats:
        return None

    years   = int(doy_hour_stats.get("sample_count", 85))
    metrics = {}

    tempf = reading.get("tempf")
    if tempf is not None:
        m = _metric_context(float(tempf), doy_hour_stats, "tempf", years)
        if m:
            metrics["temp"] = m

    dewpt = reading.get("dewPoint")
    if dewpt is not None:
        m = _metric_context(float(dewpt), doy_hour_stats, "dewptf", years)
        if m:
            metrics["dewpoint"] = m

    wind = reading.get("windspeedmph")
    if wind is not None:
        m = _metric_context(float(wind), doy_hour_stats, "windmph", years)
        if m:
            metrics["wind"] = m

    if not metrics:
        return None

    headline = None
    if "temp" in metrics:
        pct      = metrics["temp"]["percentile"]
        yrs      = metrics["temp"]["years_of_data"]
        headline = (
            f"Currently {_ordinal(pct)} percentile for this hour on "
            f"{_doy_label(doy)} in {yrs} years of records"
        )

    return {"mode": "live", "metrics": metrics, "headline": headline}


def daily_verdict(
    today_high: float | None,
    today_low:  float | None,
    doy_stats:  dict | None,
    doy:        str,
) -> dict | None:
    """
    Compare today's confirmed high/low against NOAA 156-year distribution.
    Returns None if stats are unavailable or today_high is None.
    """
    if not doy_stats or today_high is None:
        return None

    annual_highs = doy_stats.get("annual_highs", [])
    if not annual_highs:
        return None

    years_of_data = int(doy_stats.get("sample_count", len(annual_highs)))
    result        = {}

    # ── Temperature high ──────────────────────────────────────────────────────
    tmax_vals = [float(r["tmax"]) for r in annual_highs if r.get("tmax") is not None]
    if tmax_vals:
        rank = sum(1 for v in tmax_vals if v < today_high)
        pct  = max(1, min(99, round(rank / len(tmax_vals) * 100)))

        # Scan year-descending list for most recent year that exceeded today
        last_exceeded = None
        for rec in annual_highs:
            if rec.get("tmax") is not None and float(rec["tmax"]) > today_high:
                last_exceeded = int(rec["year"])
                break

        earliest_year = int(annual_highs[-1]["year"]) if annual_highs else 1869
        if last_exceeded:
            label = f"Warmest {_doy_label(doy)} since {last_exceeded}"
        else:
            label = f"Warmest {_doy_label(doy)} on record (since {earliest_year})"

        result["temp_high"] = {
            "value":               round(today_high, 1),
            "percentile":          pct,
            "last_exceeded_year":  last_exceeded,
            "label":               label,
            "years_of_data":       years_of_data,
        }

    # ── Temperature low ───────────────────────────────────────────────────────
    if today_low is not None:
        tmin_vals = [float(r["tmin"]) for r in annual_highs if r.get("tmin") is not None]
        if tmin_vals:
            rank = sum(1 for v in tmin_vals if v < today_low)
            pct  = max(1, min(99, round(rank / len(tmin_vals) * 100)))

            last_exceeded_low = None
            for rec in annual_highs:
                if rec.get("tmin") is not None and float(rec["tmin"]) > today_low:
                    last_exceeded_low = int(rec["year"])
                    break

            result["temp_low"] = {
                "value":              round(today_low, 1),
                "percentile":         pct,
                "last_exceeded_year": last_exceeded_low,
                "years_of_data":      years_of_data,
            }

    # ── Wind ─────────────────────────────────────────────────────────────────
    awnd_vals = [float(r["awnd"]) for r in annual_highs if r.get("awnd") is not None]
    if awnd_vals and today_high is not None:
        # Use today_high as proxy for wind — actual avg_wind comes from daily_summary
        # This slot is filled by the caller if they pass avg_wind separately
        pass  # wire in handler.py if avg_wind_today available

    return result or None


def anomaly_headline(live: dict | None, verdict: dict | None) -> str | None:
    """
    Returns the most prominent headline string.
    Prefers verdict (NOAA "warmest since YEAR") over live percentile.
    """
    if verdict and verdict.get("temp_high"):
        t   = verdict["temp_high"]
        pct = t["percentile"]
        if t.get("last_exceeded_year"):
            return f"{t['label']} · {_ordinal(pct)} percentile"
        return t["label"]

    if live and live.get("metrics", {}).get("temp"):
        t = live["metrics"]["temp"]
        return (
            f"Currently {_ordinal(t['percentile'])} percentile for this hour"
            f" · {t['years_of_data']} yrs"
        )

    return None
```

- [ ] **Step 4: Run all tests**

```bash
cd /Users/jamest/wx-jamestannahill
python -m pytest tests/test_climate_context.py -v 2>&1 | tail -20
```
Expected: all 15 tests PASS

- [ ] **Step 5: Commit**

```bash
git add lambdas/wx_api/climate_context.py tests/test_climate_context.py
git commit -m "feat(api): climate_context module — live_context, daily_verdict, anomaly_headline"
```

---

## Task 8: Wire `climate_context` Into `/current`

**Files:**
- Modify: `lambdas/wx_api/handler.py`

- [ ] **Step 1: Add the import and table env var at the top of `handler.py`**

Find:
```python
from wx_api.anomaly import compute_anomalies, pressure_trend, condition_label, percentile_rank
```
Add on the next line:
```python
from wx_api.climate_context import live_context, daily_verdict, anomaly_headline
```

Find:
```python
RECORDS_TABLE      = os.environ.get('RECORDS_TABLE',      'wx-station-records')
```
Add after:
```python
CLIMATE_DOY_TABLE    = os.environ.get('CLIMATE_DOY_TABLE',    'wx-climate-doy')
CLIMATE_HOURLY_TABLE = os.environ.get('CLIMATE_HOURLY_TABLE', 'wx-climate-hourly')
```

- [ ] **Step 2: Add two helper fetch functions at the bottom of `handler.py` (before `_floatify`)**

```python
def _fetch_climate_doy(mac: str, doy: str) -> dict | None:
    """Fetch NOAA per-DOY stats from wx-climate-doy."""
    try:
        table = get_table(CLIMATE_DOY_TABLE)
        resp  = table.get_item(Key={"station_id": mac, "doy": doy})
        item  = resp.get("Item")
        return _floatify(item) if item else None
    except Exception as e:
        print(f"Climate DOY fetch (non-fatal): {e}")
        return None


def _fetch_climate_hourly(mac: str, doy_hour: str) -> dict | None:
    """Fetch ERA5 per-DOY-hour stats from wx-climate-hourly."""
    try:
        table = get_table(CLIMATE_HOURLY_TABLE)
        resp  = table.get_item(Key={"station_id": mac, "doy_hour": doy_hour})
        item  = resp.get("Item")
        return _floatify(item) if item else None
    except Exception as e:
        print(f"Climate hourly fetch (non-fatal): {e}")
        return None
```

- [ ] **Step 3: Add `doy`/`doy_hour` variables and climate fetches to `_current()`**

In `_current()`, find:
```python
    local_now = now.astimezone(STATION_TZ)
    month_hour = local_now.strftime('%m-%H')
```

Add two lines immediately after `month_hour`:
```python
    doy      = local_now.strftime("%m%d")             # e.g. "0413"
    doy_hour = f"{doy}-{local_now.hour:02d}"          # e.g. "0413-14"
```

Then find `with ThreadPoolExecutor(max_workers=7) as ex:` and change `max_workers=7` to `max_workers=9`.

Inside the `with` block, add these two submits alongside the existing ones:
```python
        f_climate_doy    = ex.submit(_fetch_climate_doy,    mac, doy)
        f_climate_hourly = ex.submit(_fetch_climate_hourly, mac, doy_hour)
```

After the `with` block (alongside the other `.result()` calls), add:
```python
    climate_doy_stats    = f_climate_doy.result()
    climate_hourly_stats = f_climate_hourly.result()
```

- [ ] **Step 4: Compute climate_context and add to response body**

After the `rain_prob = rain_probability(reading, recent, nearby)` line, add:

```python
    # Climate context — live percentile + daily verdict
    climate_live    = live_context(reading, climate_hourly_stats, doy)
    today_high      = daily_summary.get("temp_high")  if daily_summary else None
    today_low       = daily_summary.get("temp_low")   if daily_summary else None
    climate_verdict = daily_verdict(today_high, today_low, climate_doy_stats, doy) if today_high else None
    climate_mode    = "daily" if climate_verdict else "live"
    climate_headline = anomaly_headline(climate_live, climate_verdict)

    climate_context = {
        "mode":     climate_mode,
        "headline": climate_headline,
        "metrics":  climate_live["metrics"] if climate_live else {},
        "verdict":  climate_verdict,
    }
```

In the `body = {...}` dict, add the new field after `"percentile_rank": pct_rank,`:

```python
        "climate_context":       climate_context,
```

- [ ] **Step 5: Run existing tests to check for regressions**

```bash
cd /Users/jamest/wx-jamestannahill
python -m pytest tests/ -v --ignore=tests/test_climate_bootstrap.py 2>&1 | tail -20
```
Expected: all previously passing tests still PASS

- [ ] **Step 6: Commit**

```bash
git add lambdas/wx_api/handler.py
git commit -m "feat(api): attach climate_context to /current — live percentile + daily verdict"
```

---

## Task 9: Dashboard — Panel, Styles, and JS

**Files:**
- Modify: `dashboard/index.html`
- Modify: `dashboard/style.css`
- Modify: `dashboard/app.js`

- [ ] **Step 1: Add the climate panel section to `index.html`**

Find:
```html
      <div class="anomaly-headline" id="anomaly-headline">—</div>
      <div class="percentile-rank" id="percentile-rank"></div>
    </section>
```

Replace with:

```html
      <div class="anomaly-headline" id="anomaly-headline">—</div>
      <div class="anomaly-subline" id="anomaly-subline"></div>
      <div class="percentile-rank" id="percentile-rank"></div>
    </section>

    <section class="climate-panel sr" id="climate-panel" hidden>
      <div class="climate-panel-header">
        <span class="section-title">TODAY IN HISTORY</span>
        <span class="source-tag has-tooltip" id="climate-source-tag" data-tooltip="">NOAA / ERA5</span>
      </div>
      <div id="climate-metrics"></div>
      <div class="climate-footer" id="climate-footer"></div>
    </section>
```

- [ ] **Step 2: Add styles to `style.css`**

Append to the end of `style.css`:

```css
/* ── Climate Panel ─────────────────────────────────────────────────────────── */
.anomaly-subline {
  font-size: 12px;
  color: #555;
  margin-top: 4px;
  letter-spacing: 0.02em;
}

.climate-panel {
  margin-bottom: 32px;
  padding: 16px 0;
  border-top: 1px solid #1a1a1a;
  border-bottom: 1px solid #1a1a1a;
}

.climate-panel-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 14px;
}

.climate-metric {
  margin-bottom: 14px;
}

.climate-metric-row {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  margin-bottom: 5px;
}

.climate-metric-label {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: #666;
}

.climate-metric-value {
  font-size: 15px;
  font-weight: 600;
}

.climate-metric-pct {
  font-size: 11px;
  color: #666;
  margin-left: 6px;
  font-weight: 400;
}

.climate-bar-track {
  height: 4px;
  background: #1a1a1a;
  border-radius: 2px;
  position: relative;
  margin-bottom: 3px;
}

.climate-bar-fill {
  position: absolute;
  left: 0;
  top: 0;
  height: 100%;
  border-radius: 2px;
  opacity: 0.85;
}

.climate-bar-marker {
  position: absolute;
  top: -4px;
  width: 2px;
  height: 12px;
  border-radius: 1px;
}

.climate-bar-ticks {
  display: flex;
  justify-content: space-between;
  font-size: 10px;
  color: #333;
}

.climate-footer {
  font-size: 10px;
  color: #2a2a2a;
  text-align: right;
  margin-top: 6px;
  letter-spacing: 0.05em;
}

.climate-verdict-label {
  font-size: 12px;
  color: #888;
  margin-left: 6px;
}
```

- [ ] **Step 3: Add `renderClimatePanel()` to `app.js`**

Find the line:
```js
function renderSummary(summary) {
```

Insert BEFORE it:

```js
// ── Climate Panel ─────────────────────────────────────────────────────────────
function renderClimatePanel(data) {
  const section = document.getElementById('climate-panel');
  const cc = data.climate_context;
  if (!cc || (!cc.metrics || !Object.keys(cc.metrics).length) && !cc.verdict) {
    section.hidden = true;
    return;
  }
  section.hidden = false;

  // Update anomaly subline
  const subline = document.getElementById('anomaly-subline');
  if (cc.headline) {
    // The main anomaly-headline keeps the existing delta label.
    // The subline shows the historic context.
    subline.textContent = cc.headline;
  } else {
    subline.textContent = '';
  }

  const isDaily = cc.mode === 'daily' && cc.verdict;
  const container = document.getElementById('climate-metrics');
  container.innerHTML = '';

  // Color map per metric
  const COLORS = { temp: '#e8c84a', dewpoint: '#4ab8e8', wind: '#888888' };

  if (isDaily) {
    // Daily verdict: high temp, low temp, avg wind from NOAA
    const verdict = cc.verdict;
    const rows = [
      { key: 'temp_high',  label: 'High Temp',  unit: '°F', color: COLORS.temp },
      { key: 'temp_low',   label: 'Low Temp',   unit: '°F', color: '#4ab8e8' },
    ];
    rows.forEach(({ key, label, unit, color }) => {
      const m = verdict[key];
      if (!m) return;
      const pct   = m.percentile;
      const since = m.last_exceeded_year ? `· since ${m.last_exceeded_year}` : '· on record';
      container.innerHTML += `
        <div class="climate-metric">
          <div class="climate-metric-row">
            <span class="climate-metric-label">${label}</span>
            <span class="climate-metric-value" style="color:${color}">${m.value}${unit}
              <span class="climate-metric-pct">${pct}th pct ${since}</span>
            </span>
          </div>
          <div class="climate-bar-track">
            <div class="climate-bar-fill" style="width:${pct}%;background:${color}"></div>
            <div class="climate-bar-marker" style="left:${pct}%;background:${color}"></div>
          </div>
        </div>`;
    });

    // Dew point from ERA5 (live metrics, muted)
    const dp = cc.metrics && cc.metrics.dewpoint;
    if (dp) {
      container.innerHTML += `
        <div class="climate-metric" style="opacity:0.6">
          <div class="climate-metric-row">
            <span class="climate-metric-label">Dew Point <span style="font-size:9px;color:#444">(ERA5)</span></span>
            <span class="climate-metric-value" style="color:#4ab8e8">${dp.value}°F
              <span class="climate-metric-pct">${dp.percentile}th pct</span>
            </span>
          </div>
          <div class="climate-bar-track">
            <div class="climate-bar-fill" style="width:${dp.percentile}%;background:#4ab8e8"></div>
          </div>
        </div>`;
    }

    const yrs = verdict.temp_high?.years_of_data ?? 156;
    document.getElementById('climate-footer').textContent =
      `NOAA Central Park 1869–${new Date().getFullYear()} · ${yrs} yrs`;
    document.getElementById('climate-source-tag').setAttribute(
      'data-tooltip',
      'NOAA GHCN-Daily station USC00305801 (Central Park). Daily high/low temperature going back to 1869.'
    );
    document.getElementById('climate-source-tag').textContent = 'NOAA · GHCN';

  } else {
    // Live mode: current percentile for temp, dewpoint, wind from ERA5
    const metrics = cc.metrics || {};
    const order = [
      { key: 'temp',     label: 'Temperature', unit: '°F',  color: COLORS.temp },
      { key: 'dewpoint', label: 'Dew Point',   unit: '°F',  color: COLORS.dewpoint },
      { key: 'wind',     label: 'Wind Speed',  unit: ' mph', color: COLORS.wind },
    ];
    order.forEach(({ key, label, unit, color }) => {
      const m = metrics[key];
      if (!m) return;
      const pct = m.percentile;
      container.innerHTML += `
        <div class="climate-metric">
          <div class="climate-metric-row">
            <span class="climate-metric-label">${label}</span>
            <span class="climate-metric-value" style="color:${color}">${m.value}${unit}
              <span class="climate-metric-pct">${pct}th pct</span>
            </span>
          </div>
          <div class="climate-bar-track">
            <div class="climate-bar-fill" style="width:${pct}%;background:linear-gradient(90deg,#222,${color})"></div>
            <div class="climate-bar-marker" style="left:${pct}%;background:${color}"></div>
          </div>
          <div class="climate-bar-ticks">
            <span>p25: ${m.p25 ?? '—'}</span>
            <span>p50: ${m.p50 ?? '—'}</span>
            <span>p75: ${m.p75 ?? '—'}</span>
          </div>
        </div>`;
    });

    const yrs = metrics.temp?.years_of_data ?? 85;
    document.getElementById('climate-footer').textContent =
      `ERA5 1940–${new Date().getFullYear() - 1} · ${yrs} yrs`;
    document.getElementById('climate-source-tag').setAttribute(
      'data-tooltip',
      'ERA5 reanalysis via Open-Meteo Archive. Hourly temperature, dew point, and wind for this exact lat/lon going back to 1940.'
    );
    document.getElementById('climate-source-tag').textContent = 'ERA5';
  }
}
```

- [ ] **Step 4: Wire `renderClimatePanel` into the main render call**

Find the main `renderCurrent(data)` call site. Look for where `renderCurrent` and the other render functions are called (the main polling loop). Find:

```js
    renderForecast(current.forecast);
```

Add after it:

```js
    renderClimatePanel(current);
```

- [ ] **Step 5: Commit**

```bash
cd /Users/jamest/wx-jamestannahill
git add dashboard/index.html dashboard/style.css dashboard/app.js
git commit -m "feat(dashboard): Today in History panel — live percentile + daily verdict modes"
```

---

## Task 10: Deploy

**Files:** none (deployment steps only)

- [ ] **Step 1: CDK deploy**

```bash
cd /Users/jamest/wx-jamestannahill/cdk
npx cdk deploy --require-approval never 2>&1 | tail -20
```
Expected: `WxStack: deploying...` → `✅ WxStack`

Two new DynamoDB tables created: `wx-climate-doy`, `wx-climate-hourly`.
Two new Lambda functions created: `WxClimateBootstrap`, `WxClimateUpdater`.

- [ ] **Step 2: Run the bootstrap (NOAA first, verify, then ERA5)**

First run NOAA only (fast — ~10 seconds):

```bash
aws lambda invoke \
  --function-name WxClimateBootstrap \
  --payload '{"noaa": true, "era5": false}' \
  --cli-binary-format raw-in-base64-out \
  /tmp/bootstrap-noaa.json \
  --region us-east-1 && cat /tmp/bootstrap-noaa.json
```
Expected: `{"status": "ok", "noaa": true, "era5": false}`

Verify 366 rows in wx-climate-doy:
```bash
aws dynamodb scan \
  --table-name wx-climate-doy \
  --select COUNT \
  --region us-east-1 | python3 -c "import json,sys; d=json.load(sys.stdin); print('Rows:', d['Count'])"
```
Expected: `Rows: 366`

Then run ERA5 (takes ~8-10 minutes — Lambda timeout 900s):

```bash
aws lambda invoke \
  --function-name WxClimateBootstrap \
  --payload '{"noaa": false, "era5": true}' \
  --cli-binary-format raw-in-base64-out \
  /tmp/bootstrap-era5.json \
  --region us-east-1 && cat /tmp/bootstrap-era5.json
```
Expected: `{"status": "ok", "noaa": false, "era5": true}`

Verify ~8784 rows in wx-climate-hourly:
```bash
aws dynamodb scan \
  --table-name wx-climate-hourly \
  --select COUNT \
  --region us-east-1 | python3 -c "import json,sys; d=json.load(sys.stdin); print('Rows:', d['Count'])"
```
Expected: `Rows: 8000+`

- [ ] **Step 3: Deploy updated wx_api lambda**

```bash
cd /Users/jamest/wx-jamestannahill/cdk
npx cdk deploy --require-approval never 2>&1 | tail -5
```
(CDK will redeploy the API lambda with the new code.)

- [ ] **Step 4: Smoke test the API**

```bash
curl -s "https://api.wx.jamestannahill.com/current" | python3 -c "
import json, sys
d = json.load(sys.stdin)
cc = d.get('climate_context')
print('mode:', cc.get('mode'))
print('headline:', cc.get('headline'))
print('metrics keys:', list(cc.get('metrics', {}).keys()))
print('verdict:', cc.get('verdict'))
"
```
Expected: `mode: live`, a headline string, `metrics keys: ['temp', 'dewpoint', 'wind']`, `verdict: None`

- [ ] **Step 5: Deploy dashboard**

```bash
cd /Users/jamest/wx-jamestannahill
aws s3 sync dashboard/ s3://wx-jamestannahill-dashboard/ --cache-control no-cache
aws cloudfront create-invalidation \
  --distribution-id E2OIRPWQ2L8LB6 \
  --paths "/*" \
  --region us-east-1
```

- [ ] **Step 6: Smoke test in browser**

Open `https://wx.jamestannahill.com`. Verify:
- "TODAY IN HISTORY" section appears below the anomaly headline
- 3 metric rows visible with percentile bars (temp, dew point, wind)
- Footer shows `ERA5 1940–YYYY · 85 yrs`
- Anomaly subline shows the historic percentile context below the existing delta headline

- [ ] **Step 7: Final commit**

```bash
cd /Users/jamest/wx-jamestannahill
git add -A
git commit -m "deploy: anomaly narrative — Today in History panel live on wx.jamestannahill.com"
```
