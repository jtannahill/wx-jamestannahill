"""
wx-ml-fitter: Fits logistic regression coefficients for rain probability.

Scans all 90-day readings, labels each with whether measurable rain
occurred in the following 60 minutes, then fits logistic regression via
mini-batch gradient descent with L2 regularization.

Features (same as heuristic model in ml.py):
  humidity_norm       = (humidity - 50) / 50
  pressure_delta      = Δbaromrelin over ~1 hr
  dew_depression_norm = (temp - dewpoint - 20) / 20
  sin_hour            = sin(2π · hour / 24)
  cos_hour            = cos(2π · hour / 24)

Stores fitted weights + bias to wx-ml-models. On next wx-api cold start,
ml.py will load them and replace the heuristic coefficients.

Scheduled: EventBridge weekly (Sunday 03:00 UTC).
"""
import os, math, json
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from boto3.dynamodb.conditions import Key
from shared.secrets import get_secret
from shared.dynamodb import get_table

READINGS_TABLE = os.environ.get('READINGS_TABLE', 'wx-readings')
MODELS_TABLE   = os.environ.get('MODELS_TABLE',   'wx-ml-models')
STATION_SECRET = 'ambient-weather/station-config'

LEARNING_RATE = 0.05
EPOCHS        = 300
L2_LAMBDA     = 0.01
RAIN_THRESH   = 0.01   # in/hr


def handler(event, context):
    station = get_secret(STATION_SECRET)
    mac     = station['mac_address']

    readings = _fetch_all_readings(mac)
    print(f"Fetched {len(readings)} readings")

    if len(readings) < 500:
        print("Insufficient data — skipping fit")
        return {"status": "insufficient_data"}

    labeled = _build_labeled_dataset(readings)
    n_pos   = sum(y for _, y in labeled)
    print(f"Labeled dataset: {len(labeled)} examples, {n_pos} positive ({100*n_pos/len(labeled):.1f}%)")

    if len(labeled) < 200:
        print("Too few labeled examples — skipping fit")
        return {"status": "insufficient_labeled"}

    weights, bias = _fit(labeled)
    print(f"Fitted weights: {[round(w, 4) for w in weights]}, bias: {round(bias, 4)}")

    metrics = _evaluate(labeled, weights, bias)
    print(f"Metrics: accuracy={metrics['accuracy']:.3f}  precision={metrics['precision']:.3f}  "
          f"recall={metrics['recall']:.3f}  f1={metrics['f1']:.3f}")

    # Only store if the fitted model has a meaningful F1 (better than trivial baseline)
    if metrics['f1'] < 0.05:
        print(f"F1 too low ({metrics['f1']:.3f}) — discarding fit, heuristic remains active")
        return {"status": "low_f1", **{k: round(v, 4) if isinstance(v, float) else v
                                       for k, v in metrics.items()}}

    table = get_table(MODELS_TABLE)
    table.put_item(Item={
        'model_id':        'rain_probability',
        'station_id':      mac,
        'weights':         [_dec(w) for w in weights],
        'bias':            _dec(bias),
        'trained_at':      datetime.now(timezone.utc).isoformat(),
        'training_samples': len(labeled),
        'positive_samples': n_pos,
        **{k: _dec(v) if isinstance(v, float) else v for k, v in metrics.items()},
    })
    print("Coefficients stored in wx-ml-models")

    return {
        "status": "ok",
        "weights": [round(w, 4) for w in weights],
        "bias":    round(bias, 4),
        **{k: round(v, 4) if isinstance(v, float) else v for k, v in metrics.items()},
    }


# ── Dataset construction ──────────────────────────────────────────────────────

def _build_labeled_dataset(readings: list) -> list:
    """
    For each clean reading, extract features and label: did it rain (>0.01 in/hr)
    in any of the next 12 readings (≈ 60 minutes at 5-min intervals)?
    """
    labeled = []
    n = len(readings)

    for i, r in enumerate(readings):
        if r.get('quality_flag'):
            continue

        features = _features(r, readings, i)
        if features is None:
            continue

        # Label: measurable rain in any reading within the next 60 min
        label = 0
        for j in range(i + 1, min(i + 13, n)):
            fr = readings[j]
            fr_ts = _parse_ts(fr.get('timestamp', ''))
            r_ts  = _parse_ts(r.get('timestamp', ''))
            if fr_ts and r_ts and (fr_ts - r_ts).total_seconds() > 3600:
                break
            if float(fr.get('hourlyrainin') or 0) > RAIN_THRESH:
                label = 1
                break

        labeled.append((features, label))

    return labeled


def _features(r: dict, readings: list, idx: int):
    """Return feature vector or None if required fields are missing."""
    humidity = r.get('humidity')
    tempf    = r.get('tempf')
    dewpoint = r.get('dewPoint')
    ts_str   = r.get('timestamp')

    if any(v is None for v in [humidity, tempf, dewpoint, ts_str]):
        return None

    humidity = float(humidity)
    tempf    = float(tempf)
    dewpoint = float(dewpoint)

    # Pressure delta vs reading ~1 hour ago (indices 9–13 back)
    pressure_delta = 0.0
    barom = r.get('baromrelin')
    if barom is not None:
        lo, hi = max(0, idx - 13), max(0, idx - 9)
        old_p = next(
            (readings[j].get('baromrelin') for j in range(lo, hi)
             if readings[j].get('baromrelin') is not None),
            None,
        )
        if old_p is not None:
            pressure_delta = float(barom) - float(old_p)

    ts   = _parse_ts(ts_str)
    hour = ts.hour if ts else 12

    return [
        (humidity - 50.0) / 50.0,                    # humidity_norm
        pressure_delta,                               # pressure_delta (in Hg)
        (max(0.0, tempf - dewpoint) - 20.0) / 20.0,  # dew_depression_norm
        math.sin(2 * math.pi * hour / 24),            # sin_hour
        math.cos(2 * math.pi * hour / 24),            # cos_hour
    ]


# ── Logistic regression ───────────────────────────────────────────────────────

def _sigmoid(z: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-z))
    except OverflowError:
        return 0.0 if z < 0 else 1.0


def _fit(labeled: list) -> tuple[list, float]:
    """
    Gradient descent with class weighting to handle rain/no-rain imbalance.
    Positive class (rain) is upweighted by n_neg/n_pos so the model optimises
    for balanced precision/recall rather than converging to "always no-rain".
    Returns (weights, bias).
    """
    n_feat  = len(labeled[0][0])
    n_pos   = sum(y for _, y in labeled) or 1
    n_neg   = len(labeled) - n_pos
    pos_w   = n_neg / n_pos   # class weight for positive examples
    # Initialise near the heuristic values so we converge faster
    w = [1.6, -9.0, -1.0, 0.15, -0.05][:n_feat] + [0.0] * max(0, n_feat - 5)
    b = -2.1

    n = len(labeled)
    for epoch in range(EPOCHS):
        dw = [0.0] * n_feat
        db = 0.0
        loss = 0.0

        for feats, y in labeled:
            cw   = pos_w if y == 1 else 1.0   # class weight
            z    = sum(w[j] * feats[j] for j in range(n_feat)) + b
            pred = _sigmoid(z)
            err  = cw * (pred - y)
            for j in range(n_feat):
                dw[j] += err * feats[j]
            db += err

            eps   = 1e-10
            loss -= cw * (y * math.log(pred + eps) + (1 - y) * math.log(1 - pred + eps))

        for j in range(n_feat):
            w[j] -= LEARNING_RATE * (dw[j] / n + L2_LAMBDA * w[j])
        b -= LEARNING_RATE * (db / n)

        if epoch % 100 == 0:
            print(f"Epoch {epoch:3d}: weighted-loss={loss/n:.4f}")

    return w, b


def _log_loss(labeled, w, b):
    n     = len(labeled)
    total = 0.0
    eps   = 1e-10
    for feats, y in labeled:
        z = sum(w[j] * feats[j] for j in range(len(w))) + b
        p = _sigmoid(z)
        total -= y * math.log(p + eps) + (1 - y) * math.log(1 - p + eps)
    return total / n


def _evaluate(labeled, w, b):
    tp = tn = fp = fn = 0
    for feats, y in labeled:
        z    = sum(w[j] * feats[j] for j in range(len(w))) + b
        pred = 1 if _sigmoid(z) >= 0.5 else 0
        if   y == 1 and pred == 1: tp += 1
        elif y == 0 and pred == 0: tn += 1
        elif y == 0 and pred == 1: fp += 1
        else:                      fn += 1

    n         = tp + tn + fp + fn or 1
    accuracy  = (tp + tn) / n
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall    = tp / (tp + fn) if (tp + fn) else 0.0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    return {
        'accuracy': accuracy, 'precision': precision,
        'recall': recall,     'f1': f1,
        'tp': tp, 'tn': tn, 'fp': fp, 'fn': fn,
    }


# ── Fetch helpers ─────────────────────────────────────────────────────────────

def _fetch_all_readings(mac: str) -> list:
    table = get_table(READINGS_TABLE)
    since = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    items, kwargs = [], dict(
        KeyConditionExpression=Key('station_id').eq(mac) & Key('timestamp').gte(since),
        ScanIndexForward=True,
    )
    while True:
        result = table.query(**kwargs)
        items.extend(result.get('Items', []))
        last = result.get('LastEvaluatedKey')
        if not last:
            break
        kwargs['ExclusiveStartKey'] = last
    return [_floatify(r) for r in items]


def _parse_ts(ts_str: str):
    try:
        dt = datetime.fromisoformat(ts_str)
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    except Exception:
        return None


def _floatify(obj):
    if isinstance(obj, dict):
        return {k: _floatify(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def _dec(val):
    if val is None:
        return None
    return Decimal(str(round(float(val), 8)))
