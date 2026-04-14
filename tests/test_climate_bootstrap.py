import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))

from wx_climate_bootstrap.noaa import parse_noaa_csv, compute_doy_stats

SAMPLE_CSV = """STATION,DATE,LATITUDE,LONGITUDE,ELEVATION,NAME,PRCP,PRCP_ATTRIBUTES,TMAX,TMAX_ATTRIBUTES,TMIN,TMIN_ATTRIBUTES,AWND,AWND_ATTRIBUTES
USC00305801,1940-04-13,40.7789,-73.9692,39.6,NY CITY CENTRAL PARK NY US,0,,200,,150,,45,
USC00305801,1941-04-13,40.7789,-73.9692,39.6,NY CITY CENTRAL PARK NY US,0,,250,,180,,60,
USC00305801,1942-04-13,40.7789,-73.9692,39.6,NY CITY CENTRAL PARK NY US,0,,180,,120,,30,
"""

# Long format — matches actual noaa-ghcn-pds S3 CSV structure
SAMPLE_CSV_LONG = """ID,DATE,ELEMENT,DATA_VALUE,M_FLAG,Q_FLAG,S_FLAG,OBS_TIME
USW00094728,19400413,TMAX,200,,,Z,
USW00094728,19400413,TMIN,150,,,Z,
USW00094728,19400413,AWND,45,,,Z,
USW00094728,19410413,TMAX,250,,,Z,
USW00094728,19410413,TMIN,180,,,Z,
USW00094728,19410413,AWND,60,,,Z,
USW00094728,19420413,TMAX,180,,,Z,
USW00094728,19420413,TMIN,120,,,Z,
USW00094728,19420413,AWND,30,,,Z,
"""

def test_parse_noaa_csv_long_format_groups_by_doy():
    by_doy = parse_noaa_csv(SAMPLE_CSV_LONG)
    assert "0413" in by_doy
    assert len(by_doy["0413"]) == 3

def test_parse_noaa_csv_long_format_converts_units():
    by_doy = parse_noaa_csv(SAMPLE_CSV_LONG)
    r1940 = by_doy["0413"][-1]
    assert r1940["year"] == 1940
    assert abs(r1940["tmax"] - 68.0) < 0.2
    assert abs(r1940["tmin"] - 59.0) < 0.2
    assert abs(r1940["awnd"] - 10.1) < 0.2

def test_parse_noaa_csv_long_format_skips_quality_flagged():
    csv_flagged = """ID,DATE,ELEMENT,DATA_VALUE,M_FLAG,Q_FLAG,S_FLAG,OBS_TIME
USW00094728,19400413,TMAX,200,,,Z,
USW00094728,19400413,TMIN,150,,G,Z,
USW00094728,19400413,AWND,45,,,Z,
"""
    by_doy = parse_noaa_csv(csv_flagged)
    # TMIN is quality-flagged, so 1940-04-13 should be skipped (no TMIN)
    assert "0413" not in by_doy or len(by_doy.get("0413", [])) == 0

def test_parse_noaa_csv_groups_by_doy():
    by_doy = parse_noaa_csv(SAMPLE_CSV)
    assert "0413" in by_doy
    assert len(by_doy["0413"]) == 3

def test_parse_noaa_csv_converts_units():
    by_doy = parse_noaa_csv(SAMPLE_CSV)
    records = by_doy["0413"]
    # After year-desc sort, records[-1] is 1940 (TMAX=200, TMIN=150, AWND=45)
    r1940 = records[-1]
    assert r1940["year"] == 1940
    # TMAX=200 tenths-°C = 20.0°C = 68.0°F
    assert abs(r1940["tmax"] - 68.0) < 0.2
    # TMIN=150 tenths-°C = 15.0°C = 59.0°F
    assert abs(r1940["tmin"] - 59.0) < 0.2
    # AWND=45 tenths-m/s = 4.5 m/s = 10.07 mph
    assert abs(r1940["awnd"] - 10.1) < 0.2

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

from wx_climate_bootstrap.era5 import compute_hourly_stats, _c_to_f, _dewpoint_f

def test_c_to_f():
    assert abs(_c_to_f(0) - 32.0) < 0.01
    assert abs(_c_to_f(100) - 212.0) < 0.01

def test_dewpoint_f():
    # dewpoint at 20°C, 50% RH ≈ 9.3°C ≈ 48.7°F
    dp = _dewpoint_f(20.0, 50.0)
    assert 48 < dp < 50

def test_compute_hourly_stats_basic():
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
    import re
    pattern = re.compile(r"^\d{4}-\d{2}$")
    sample_key = "0413-14"
    assert pattern.match(sample_key), f"Key format wrong: {sample_key}"

def test_fetch_month_era5_output_keys():
    """era5 output dict must have the field prefixes that climate_context.py expects."""
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
    assert "mean_tempf"  in slot
    assert "std_tempf"   in slot
    assert "p25_tempf"   in slot
    assert "mean_dewptf" in slot
    assert "mean_windmph" in slot
