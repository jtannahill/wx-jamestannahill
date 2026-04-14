"""
Generates a 1200x630 OG image PNG with current weather conditions.
Uploads to the dashboard S3 bucket as og.png, then patches the og:image
and twitter:image URLs in index.html with a cache-busting timestamp so
Twitter always fetches the latest image when a user composes a share.
"""
import io
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import boto3
from PIL import Image, ImageDraw, ImageFont

DASHBOARD_BUCKET = os.environ.get('DASHBOARD_BUCKET', 'wx-jamestannahill-dashboard')

DIR_LABELS = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW']

def _deg_to_compass(deg):
    if deg is None:
        return ''
    return DIR_LABELS[round(float(deg) / 22.5) % 16]


def _font(name: str, size: int) -> ImageFont.FreeTypeFont:
    here = Path(__file__).parent
    return ImageFont.truetype(str(here / name), size)


def _fmt(val, decimals=0):
    if val is None:
        return '—'
    return f'{float(val):.{decimals}f}'


def _text_w(draw, text, font):
    bb = draw.textbbox((0, 0), text, font=font)
    return bb[2] - bb[0]


def generate_og(reading: dict, condition: str, uhi_delta: float | None = None) -> None:
    W, H = 1200, 630
    BG     = (10, 10, 10)
    TEXT   = (235, 235, 235)
    MUTED  = (90, 90, 90)
    DIM    = (140, 140, 140)
    ACCENT = (200, 185, 122)   # gold
    DIVIDER= (38, 38, 38)

    img  = Image.new('RGB', (W, H), BG)
    draw = ImageDraw.Draw(img)

    f_temp  = _font('NHGDisplay-Bold.ttf',    148)
    f_val   = _font('NHGDisplay-Bold.ttf',     44)
    f_cond  = _font('NHGDisplay-Bold.ttf',     38)
    f_body  = _font('NHGDisplay-Regular.ttf',  26)
    f_label = _font('NHGDisplay-Regular.ttf',  15)

    PAD = 72

    # ── Top bar ───────────────────────────────────────────────────────────────
    draw.text((PAD, 52), 'MIDTOWN MANHATTAN, NEW YORK', font=f_label, fill=MUTED)

    # Updated time (right-aligned)
    ET = timezone(timedelta(hours=-4))
    now_str = datetime.now(ET).strftime('%-I:%M %p ET')
    upd = f'Updated {now_str}'
    draw.text((W - PAD - _text_w(draw, upd, f_label), 52), upd, font=f_label, fill=MUTED)

    # ── Temperature (left) ────────────────────────────────────────────────────
    temp_str = f"{_fmt(reading.get('tempf'), 0)}°"
    draw.text((PAD, 88), temp_str, font=f_temp, fill=TEXT)

    # Bounding box so condition anchors to the right of the big number
    t_bb  = draw.textbbox((PAD, 88), temp_str, font=f_temp)
    temp_w = t_bb[2] - t_bb[0]
    cx = PAD + temp_w + 44

    # Condition + feels like (vertically centered in the temp block)
    if condition:
        draw.text((cx, 140), condition, font=f_cond, fill=TEXT)

    feels = reading.get('feelsLike')
    if feels is not None:
        draw.text((cx, 195), f"Feels like {_fmt(feels, 0)}°F", font=f_body, fill=DIM)

    # ── Divider ───────────────────────────────────────────────────────────────
    div_y = 300
    draw.rectangle([(PAD, div_y), (W - PAD, div_y + 1)], fill=DIVIDER)

    # ── Metrics row ───────────────────────────────────────────────────────────
    # Build metric list: (LABEL, value_str, sub_str)
    metrics = []

    # Humidity
    hum = reading.get('humidity')
    dew = reading.get('dewPoint')
    metrics.append(('HUMIDITY', f"{_fmt(hum, 0)}%",
                    f"Dew {_fmt(dew, 0)}°F" if dew is not None else ''))

    # Wind
    wind  = reading.get('windspeedmph')
    gust  = reading.get('windgustmph')
    wdir  = reading.get('winddir')
    compass = _deg_to_compass(wdir)
    gust_str = f"Gust {_fmt(gust, 0)}" if gust is not None else ''
    metrics.append(('WIND', f"{_fmt(wind, 0)} mph",
                    f"{gust_str}  {compass}".strip()))

    # Pressure
    baro = reading.get('baromrelin')
    metrics.append(('PRESSURE', f'{_fmt(baro, 2)}"', ''))

    # UV
    uv = reading.get('uv')
    metrics.append(('UV INDEX', _fmt(uv, 0), ''))

    # UHI delta if available, else daily rain
    if uhi_delta is not None:
        sign = '+' if uhi_delta >= 0 else ''
        metrics.append(('URBAN HEAT', f'{sign}{uhi_delta:.1f}°F', 'vs airports'))
    else:
        rain = reading.get('dailyrainin')
        metrics.append(('RAIN TODAY', f'{_fmt(rain, 2)}"', ''))

    # Layout: center each metric within equal slots
    n       = len(metrics)
    slot_w  = (W - 2 * PAD) // n
    lbl_y   = div_y + 26
    val_y   = div_y + 50
    sub_y   = div_y + 102

    for i, (label, val, sub) in enumerate(metrics):
        mx = PAD + i * slot_w + slot_w // 2   # center of slot

        lw = _text_w(draw, label, f_label)
        draw.text((mx - lw // 2, lbl_y), label, font=f_label, fill=MUTED)

        vw = _text_w(draw, val, f_val)
        draw.text((mx - vw // 2, val_y), val, font=f_val, fill=TEXT)

        if sub:
            sw = _text_w(draw, sub, f_label)
            draw.text((mx - sw // 2, sub_y), sub, font=f_label, fill=DIM)

    # ── Bottom accent + attribution ───────────────────────────────────────────
    draw.rectangle([(0, H - 3), (W, H)], fill=ACCENT)
    draw.text((PAD, H - PAD + 4), 'wx.jamestannahill.com', font=f_label, fill=MUTED)

    # ── Upload ────────────────────────────────────────────────────────────────
    buf = io.BytesIO()
    img.save(buf, format='PNG', optimize=True)
    buf.seek(0)

    s3 = boto3.client('s3', region_name='us-east-1')
    s3.put_object(
        Bucket=DASHBOARD_BUCKET,
        Key='og.png',
        Body=buf.read(),
        ContentType='image/png',
        CacheControl='max-age=300',
    )

    # ── Patch index.html with a versioned og:image URL ────────────────────────
    # Twitter caches OG images by URL; appending ?v=<timestamp> forces a fresh
    # fetch each time the poller runs (every 5 minutes).
    try:
        ts = int(time.time())
        versioned = f'https://wx.jamestannahill.com/og.png?v={ts}'
        html_obj  = s3.get_object(Bucket=DASHBOARD_BUCKET, Key='index.html')
        html      = html_obj['Body'].read().decode('utf-8')

        # Replace both og:image and twitter:image content values
        html = re.sub(
            r'(content="https://wx\.jamestannahill\.com/og\.png)(?:\?v=\d+)?(")',
            rf'\g<1>?v={ts}\2',
            html,
        )

        s3.put_object(
            Bucket=DASHBOARD_BUCKET,
            Key='index.html',
            Body=html.encode('utf-8'),
            ContentType='text/html',
            CacheControl='no-cache, no-store, must-revalidate',
        )
        print(f"[og_image] Patched index.html og:image URLs with ?v={ts}")
    except Exception as e:
        print(f"[og_image] index.html patch failed (non-fatal): {e}")
