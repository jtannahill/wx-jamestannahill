import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))

from datetime import datetime
from wx_api.baseline import blend_plan, blend, period_label
from wx_api.anomaly import compute_anomalies

SEP = {"month_hour": "09-04", "avg_tempf": 67.6, "std_tempf": 5.3, "sample_count": 30, "source": "seed"}
OCT = {"month_hour": "10-04", "avg_tempf": 57.2, "std_tempf": 2.4, "sample_count": 20, "source": "seed"}


def test_plan_first_of_month_blends_with_previous():
    own, other, w = blend_plan(datetime(2026, 10, 1, 4))
    assert (own, other) == ("10-04", "09-04")
    assert 0.48 < w <= 0.5


def test_plan_last_of_month_blends_with_next():
    own, other, w = blend_plan(datetime(2026, 9, 30, 4))
    assert (own, other) == ("09-04", "10-04")
    assert 0.48 < w <= 0.5


def test_plan_wraps_year():
    assert blend_plan(datetime(2026, 1, 2, 9))[1] == "12-09"
    assert blend_plan(datetime(2026, 12, 30, 9))[1] == "01-09"


def test_boundary_is_continuous():
    # The 3.5 hours either side of midnight Oct 1 should agree to well under
    # a degree; the unblended lookup stepped 10.4F here.
    _, _, w_sep = blend_plan(datetime(2026, 9, 30, 23))
    _, _, w_oct = blend_plan(datetime(2026, 10, 1, 0))
    before = blend(SEP, OCT, w_sep)["avg_tempf"]
    after = blend(OCT, SEP, w_oct)["avg_tempf"]
    assert abs(before - after) < 0.5


def test_mid_month_is_own_value():
    own, other, w = blend_plan(datetime(2026, 10, 16, 4))
    assert w < 0.02
    assert abs(blend(OCT, SEP, w)["avg_tempf"] - OCT["avg_tempf"]) < 0.2


def test_blend_keeps_metadata_and_std_through_variance():
    out = blend(OCT, SEP, 0.5)
    assert out["sample_count"] == 20 and out["source"] == "seed" and out["month_hour"] == "10-04"
    assert abs(out["avg_tempf"] - 62.4) < 1e-9
    assert abs(out["std_tempf"] - ((0.5 * 2.4**2 + 0.5 * 5.3**2) ** 0.5)) < 1e-9


def test_blend_missing_neighbour_falls_back():
    assert blend(OCT, None, 0.4) == OCT
    assert blend(OCT, {"avg_tempf": None}, 0.4)["avg_tempf"] == OCT["avg_tempf"]
    assert blend({}, SEP, 0.4) == {}


def test_period_label_and_anomaly_copy():
    assert period_label(datetime(2026, 10, 1)) == "early October"
    assert period_label(datetime(2026, 10, 15)) == "mid October"
    assert period_label(datetime(2026, 10, 25)) == "late October"
    r = compute_anomalies({"tempf": 70.0}, {"avg_tempf": 60.0}, 10, 9, period="early October")
    assert r["temp"]["label"] == "10.0°F above average for 9am in early October"
