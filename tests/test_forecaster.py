from datetime import datetime, timezone, timedelta

from wx_forecaster.handler import split_pending, forecast_errors, PENDING_MAX


NOW = datetime(2026, 10, 18, 23, 0, tzinfo=timezone.utc)


def _entry(minutes_ago, tempf=60.0):
    computed = NOW - timedelta(minutes=minutes_ago)
    return {
        'computed_at': computed.isoformat(),
        'forecast': [
            {'offset_hours': h, 'tempf': tempf + h, 'humidity': 50.0,
             'windspeedmph': 5.0, 'baromrelin': 30.0}
            for h in (1, 2, 3)
        ],
    }


# --- split_pending: which queued forecasts are ready to score -----------------

def test_forecast_younger_than_four_hours_waits():
    ready, keep = split_pending([_entry(30), _entry(200)], NOW)
    assert ready == []
    assert len(keep) == 2


def test_forecast_older_than_four_hours_is_scored_and_removed():
    old, young = _entry(250), _entry(30)
    ready, keep = split_pending([old, young], NOW)
    assert ready == [old]
    assert keep == [young]


def test_forecast_older_than_a_day_is_dropped_unscored():
    ready, keep = split_pending([_entry(25 * 60)], NOW)
    assert ready == []
    assert keep == []


def test_entries_without_a_timestamp_are_dropped():
    ready, keep = split_pending([{'forecast': []}], NOW)
    assert ready == [] and keep == []


def test_queue_cap_covers_the_wait_at_30_minute_cadence():
    # 4 hours of 30-minute runs must fit, or forecasts fall off before scoring
    assert PENDING_MAX >= 9


# --- forecast_errors: absolute error per offset and field --------------------

def _hourly_for(entry, tempf_actual):
    computed = datetime.fromisoformat(entry['computed_at'])
    out = {}
    for h in (1, 2, 3):
        key = (computed + timedelta(hours=h)).replace(minute=0, second=0, microsecond=0).isoformat()
        out[key] = {'tempf': tempf_actual, 'humidity': 52.0, 'windspeedmph': None, 'baromrelin': 30.1}
    return out


def test_errors_are_absolute_differences_per_offset():
    entry = _entry(250, tempf=60.0)          # forecasts 61, 62, 63
    errors = forecast_errors(entry, _hourly_for(entry, 62.0))
    assert errors['mae_1h_tempf'] == 1.0
    assert errors['mae_2h_tempf'] == 0.0
    assert errors['mae_3h_tempf'] == 1.0
    assert round(errors['mae_1h_humidity'], 6) == 2.0


def test_missing_actual_field_is_skipped_not_zero():
    entry = _entry(250)
    errors = forecast_errors(entry, _hourly_for(entry, 62.0))
    assert 'mae_1h_windspeedmph' not in errors


def test_no_matching_hours_gives_no_errors():
    assert forecast_errors(_entry(250), {}) == {}


# --- delta_forecast: current value plus the analogs' change -------------------

from wx_forecaster.handler import delta_forecast


def _analog(last, future_vals, field='tempf'):
    return ({field: last}, [{field: v} for v in future_vals])


def test_forecast_starts_from_current_value_not_analog_level():
    # analogs ran 10F warmer than now but warmed by +1, +2, +3
    analogs = [_analog(70.0, [71.0, 72.0, 73.0]), _analog(72.0, [73.0, 74.0, 75.0])]
    out = delta_forecast({'tempf': 52.0}, analogs)
    assert [b['tempf'] for b in out] == [53.0, 54.0, 55.0]
    assert [b['offset_hours'] for b in out] == [1, 2, 3]


def test_humidity_is_clamped_to_0_100_and_wind_to_non_negative():
    hum = delta_forecast({'humidity': 98.0}, [_analog(50.0, [60.0, 60.0, 60.0], 'humidity')])
    assert hum[0]['humidity'] == 100.0
    wind = delta_forecast({'windspeedmph': 1.0}, [_analog(10.0, [5.0, 5.0, 5.0], 'windspeedmph')])
    assert wind[0]['windspeedmph'] == 0.0


def test_missing_current_value_falls_back_to_analog_average():
    out = delta_forecast({'tempf': None}, [_analog(70.0, [71.0, 72.0, 73.0])])
    assert out[0]['tempf'] == 71.0


def test_field_missing_everywhere_is_none():
    out = delta_forecast({}, [({}, [{}, {}, {}])])
    assert out[0]['tempf'] is None
