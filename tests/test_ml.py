import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))

from wx_api.ml import rain_probability

# Minimal reading fixture — valid enough for all feature extraction
_BASE_READING = {
    'humidity':     75,
    'tempf':        62,
    'dewPoint':     55,
    'baromrelin':   29.95,
    'hourlyrainin': 0.0,
    'winddir':      270,
    'timestamp':    '2026-04-08T12:00:00',
}
_RECENT = []  # no pressure-trend history needed for these tests


def test_rain_probability_no_nearby_unchanged():
    """Calling with nearby=None must return the same probability as omitting the arg,
    spatial_boost must be 0.0, and spatial_source must be None."""
    result_default = rain_probability(_BASE_READING, _RECENT)
    result_none    = rain_probability(_BASE_READING, _RECENT, nearby=None)
    result_empty   = rain_probability(_BASE_READING, _RECENT, nearby=[])

    assert result_none['probability']    == result_default['probability']
    assert result_none['spatial_boost']  == 0.0
    assert result_none['spatial_source'] is None

    assert result_empty['probability']   == result_default['probability']
    assert result_empty['spatial_boost'] == 0.0
    assert result_empty['spatial_source'] is None


def test_rain_probability_with_upwind_rain():
    """An upwind raining station (bearing == winddir) must produce a positive boost
    and a non-None source label, and the returned probability must be >= the base."""
    base_result = rain_probability(_BASE_READING, _RECENT, nearby=None)
    base_prob   = base_result['probability']

    nearby = [{
        'station_id':     'X',
        'neighborhood':   "Hell's Kitchen",
        'bearing_deg':    270,       # station lies to the west
        'distance_mi':    0.5,
        'rain_rate_in_hr': 0.1,      # raining
    }]
    # reading winddir=270 means wind is FROM the west, so that station is upwind
    result = rain_probability(_BASE_READING, _RECENT, nearby=nearby)

    assert result['spatial_boost'] > 0,           "Expected positive spatial boost"
    assert result['spatial_source'] is not None,  "Expected a source label"
    assert result['probability'] >= base_prob,    "Boosted probability should not fall below base"


def test_rain_probability_spatial_boost_capped():
    """A very close, heavily raining upwind station must not push spatial_boost above 0.35."""
    nearby = [{
        'station_id':     'HEAVYRAIN',
        'neighborhood':   'Murray Hill',
        'bearing_deg':    270,
        'distance_mi':    0.1,       # very close
        'rain_rate_in_hr': 5.0,      # extremely heavy rain
    }]
    result = rain_probability(_BASE_READING, _RECENT, nearby=nearby)

    assert result['spatial_boost'] <= 0.35, (
        f"spatial_boost {result['spatial_boost']} exceeds cap of 0.35"
    )
