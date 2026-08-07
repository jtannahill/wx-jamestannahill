"""
Nearby station helpers for the wx-api Lambda.

spatial_rain_boost()       — compute upwind rain boost for rain_probability
_fetch_nearby_snapshot()   — read latest snapshot from wx-nearby-snapshots
network_variance()         — per-station and network-level dispersion ratios
nearby_route()             — handler for GET /nearby
"""
import json, math, os, statistics
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


# Minimum nearby stations needed before a dispersion figure means anything.
# With two readings a "spread" is just the gap between them.
MIN_VARIANCE_SAMPLE = 3

# Floor on the network sigma, in degrees F. Cheap PWS thermometers quantize
# to 0.1F, so a genuinely tight network can report sigma near zero and blow
# the ratio up to nonsense. 0.3F is roughly the sensor noise floor.
MIN_SIGMA_F = 0.3

# Scaling constant that puts the median absolute deviation on the same footing
# as a standard deviation for normally distributed data, so a "sigma" derived
# from MAD is directly comparable to the textbook one.
MAD_TO_SIGMA = 1.4826

# A station this far from the network center is almost certainly a bad sensor
# rather than real microclimate: an unshaded thermometer in afternoon sun, or
# one reporting from indoors. Flagged, not dropped.
OUTLIER_RATIO = 3.0


def _station_temps(stations: list[dict]) -> list[float]:
    """Usable temp_f values from a nearby snapshot, in station order."""
    out = []
    for s in stations or []:
        t = s.get('temp_f')
        if t is None:
            continue
        try:
            out.append(float(t))
        except (TypeError, ValueError):
            continue
    return out


def _variance_verdict(ratio: float | None) -> str | None:
    """Plain-language read on how far the home station sits from the pack."""
    if ratio is None:
        return None
    a = abs(ratio)
    side = 'warmer' if ratio > 0 else 'cooler'
    if a < 0.5:
        return 'in line with the network'
    if a < 1.0:
        return f'slightly {side} than the network'
    if a < 2.0:
        return f'{side} than the network'
    return f'sharply {side} than the network'


def network_variance(home_temp_f, stations: list[dict]) -> tuple[list[dict], dict]:
    """
    Express each nearby reading, and our own, as a ratio against the spread
    of the nearby network itself.

    A raw delta ("that station is 2F warmer") is not interpretable on its
    own: 2F is unremarkable on a windy afternoon when the boroughs disagree
    by 5F, and it is a strong signal on a calm night when they agree within
    half a degree. So the yardstick is the network's own spread, measured
    across the nearby stations only, with the home station excluded so it
    cannot inflate the very dispersion it is being judged against.

    That spread is measured robustly: median as the center, and a scaled
    median absolute deviation as the unit. The network is a handful of
    consumer-grade PWS units and reliably includes at least one that is
    sitting in direct sun or reporting from indoors. On live data a single
    such station, 19F off the pack, tripled a mean-and-stdev sigma, which
    dragged every other station's ratio toward zero and hid the very
    divergence the ratio exists to show. Median and MAD ignore it. The
    classical mean and stdev are still reported for reference.

    Returns (stations, summary).

    Each returned station gains:
      temp_delta_f  station temp minus home temp, in F
      temp_ratio    that delta in units of network sigma (signed, 2dp)
      is_outlier    True if this station is more than OUTLIER_RATIO from the
                    network center, i.e. suspect hardware rather than weather

    summary keys:
      count             nearby stations with a usable temperature
      network_median_f  median of the nearby temps, the ratio's center
      network_sigma_f   scaled MAD of the nearby temps, the ratio's unit
      network_mean_f    plain mean, for reference
      network_stdev_f   plain population stdev, for reference
      network_spread_f  max minus min across the nearby temps
      outlier_count     stations beyond OUTLIER_RATIO of the center
      home_temp_f       our station
      home_delta_f      home minus network median
      home_ratio        home_delta_f in units of network sigma
      verdict           short plain-language label, or None

    Ratios are None (not 0) when there are too few samples to support them.
    The stations list is returned enriched either way, so callers never have
    to branch on sample size to render the deltas.
    """
    stations = list(stations or [])
    try:
        home = float(home_temp_f)
    except (TypeError, ValueError):
        home = None

    temps = _station_temps(stations)
    count = len(temps)

    summary = {
        'count':            count,
        'network_median_f': None,
        'network_sigma_f':  None,
        'network_mean_f':   None,
        'network_stdev_f':  None,
        'network_spread_f': None,
        'outlier_count':    0,
        'home_temp_f':      round(home, 1) if home is not None else None,
        'home_delta_f':     None,
        'home_ratio':       None,
        'verdict':          None,
    }

    if count == 0:
        for s in stations:
            s['temp_delta_f'] = None
            s['temp_ratio']   = None
            s['is_outlier']   = False
        return stations, summary

    center = statistics.median(temps)
    summary['network_median_f'] = round(center, 1)
    summary['network_mean_f']   = round(statistics.fmean(temps), 1)
    summary['network_spread_f'] = round(max(temps) - min(temps), 1)

    have_sigma = count >= MIN_VARIANCE_SAMPLE
    sigma = None
    if have_sigma:
        mad = statistics.median([abs(t - center) for t in temps])
        sigma = max(mad * MAD_TO_SIGMA, MIN_SIGMA_F)
        summary['network_sigma_f'] = round(sigma, 2)
        summary['network_stdev_f'] = round(statistics.pstdev(temps), 2)

    if home is not None:
        summary['home_delta_f'] = round(home - center, 1)
        if have_sigma:
            summary['home_ratio'] = round((home - center) / sigma, 2)
            summary['verdict']    = _variance_verdict(summary['home_ratio'])

    outliers = 0
    for s in stations:
        t = s.get('temp_f')
        try:
            t = float(t) if t is not None else None
        except (TypeError, ValueError):
            t = None
        if t is None:
            s['temp_delta_f'] = None
            s['temp_ratio']   = None
            s['is_outlier']   = False
            continue

        # The delta a reader cares about is against our station, since that
        # is the reading the page is built around. The ratio, though, is
        # measured from the network center: a station's credibility is a
        # question about the network, not about us.
        s['temp_delta_f'] = round(t - home, 1) if home is not None else None
        if have_sigma:
            ratio = (t - center) / sigma
            s['temp_ratio'] = round(ratio, 2)
            s['is_outlier'] = abs(ratio) >= OUTLIER_RATIO
            outliers += 1 if s['is_outlier'] else 0
        else:
            s['temp_ratio'] = None
            s['is_outlier'] = False

    summary['outlier_count'] = outliers
    return stations, summary


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


def nearby_route(station_id: str, home_temp_f=None) -> dict:
    """
    Returns the latest nearby snapshot as a JSON-serialisable dict.
    Shape: {stations: [...], snapshot_at: str, count: int, temp_variance: {...}}

    home_temp_f is optional. Without it the stations come back unenriched and
    temp_variance reports nulls, so /nearby stays usable on its own.
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
            return {'stations': [], 'count': 0, 'snapshot_at': None,
                    'temp_variance': network_variance(home_temp_f, [])[1]}
        item = items[0]
        stations = json.loads(item.get('stations_json', '[]'))
        stations, variance = network_variance(home_temp_f, stations)
        return {
            'stations':      stations,
            'count':         len(stations),
            'snapshot_at':   item.get('snapshot_at'),
            'temp_variance': variance,
        }
    except Exception as e:
        print(f"nearby_route failed: {e}")
        return {'stations': [], 'count': 0, 'snapshot_at': None,
                'temp_variance': network_variance(None, [])[1], 'error': str(e)}
