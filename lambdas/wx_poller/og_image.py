"""
Generates a 1200x630 OG image PNG with current weather conditions.
Uploads to the dashboard S3 bucket as og.png.
"""
import os
import io
import boto3
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

DASHBOARD_BUCKET = os.environ.get('DASHBOARD_BUCKET', 'wx-jamestannahill-dashboard')

DIR_LABELS = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW']

def _deg_to_compass(deg):
    if deg is None:
        return ''
    return DIR_LABELS[round(float(deg) / 22.5) % 16]


def _font(name: str, size: int) -> ImageFont.FreeTypeFont:
    here = Path(__file__).parent
    path = here / name
    return ImageFont.truetype(str(path), size)


def generate_og(reading: dict, condition: str) -> None:
    W, H = 1200, 630
    BG = (10, 10, 10)
    MUTED = (100, 100, 100)
    TEXT = (240, 240, 240)
    GOLD = (200, 185, 122)
    BORDER = (34, 34, 34)

    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    # Subtle bottom border accent
    draw.rectangle([(0, H - 3), (W, H)], fill=GOLD)

    font_bold = _font("NHGDisplay-Bold.ttf", 120)
    font_bold_sm = _font("NHGDisplay-Bold.ttf", 48)
    font_reg = _font("NHGDisplay-Regular.ttf", 28)
    font_label = _font("NHGDisplay-Regular.ttf", 18)

    pad = 72

    # Station label (top left)
    draw.text((pad, pad), "MIDTOWN MANHATTAN, NEW YORK", font=font_label, fill=MUTED)

    # Temperature (big)
    temp = reading.get('tempf')
    temp_str = f"{round(float(temp))}°" if temp is not None else "—°"
    draw.text((pad, 130), temp_str, font=font_bold, fill=TEXT)

    # Feels like + condition (below temp)
    feels = reading.get('feelsLike')
    feels_str = f"Feels like {round(float(feels))}°F" if feels is not None else ""
    draw.text((pad, 270), feels_str, font=font_reg, fill=MUTED)

    cond_str = condition or ""
    draw.text((pad, 310), cond_str, font=font_bold_sm, fill=TEXT)

    # Right side: wind + humidity
    rx = W - pad - 260
    # Wind
    wind = reading.get('windspeedmph')
    wind_dir = reading.get('winddir')
    wind_str = f"{round(float(wind))} mph" if wind is not None else "— mph"
    compass = _deg_to_compass(wind_dir)
    draw.text((rx, 130), "WIND", font=font_label, fill=MUTED)
    draw.text((rx, 158), wind_str, font=font_bold_sm, fill=TEXT)
    draw.text((rx, 216), compass, font=font_reg, fill=MUTED)

    # Humidity
    hum = reading.get('humidity')
    hum_str = f"{round(float(hum))}%" if hum is not None else "—%"
    draw.text((rx, 290), "HUMIDITY", font=font_label, fill=MUTED)
    draw.text((rx, 318), hum_str, font=font_bold_sm, fill=TEXT)

    # Bottom attribution
    draw.text((pad, H - pad - 24), "wx.jamestannahill.com", font=font_label, fill=MUTED)

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)

    s3 = boto3.client('s3', region_name='us-east-1')
    s3.put_object(
        Bucket=DASHBOARD_BUCKET,
        Key="og.png",
        Body=buf.read(),
        ContentType="image/png",
        CacheControl="max-age=300",
    )
