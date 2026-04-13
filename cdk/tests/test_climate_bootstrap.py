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
