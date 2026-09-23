"""
wx-archiver: Export yesterday's readings to S3 for long-term retention.

DynamoDB wx-readings has a 90-day TTL; this archives daily JSONL before expiry.
Scheduled daily at 04:30 UTC (after midnight ET summarizer).
"""
import gzip
import io
import json
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Key

from shared.dynamodb import get_table
from shared.secrets import get_secret

READINGS_TABLE = os.environ.get('READINGS_TABLE', 'wx-readings')
ARCHIVE_BUCKET = os.environ.get('ARCHIVE_BUCKET', 'wx-readings-archive')
STATION_SECRET = 'ambient-weather/station-config'
STATION_TZ = ZoneInfo('America/New_York')

_s3 = boto3.client('s3', region_name='us-east-1')


def handler(event, context):
    station = get_secret(STATION_SECRET)
    mac = station['mac_address']

    # Archive the previous local calendar day
    local_yesterday = (datetime.now(STATION_TZ) - timedelta(days=1)).date()
    start_local = datetime.combine(local_yesterday, datetime.min.time(), tzinfo=STATION_TZ)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc).isoformat()
    end_utc = end_local.astimezone(timezone.utc).isoformat()

    readings = _fetch_range(mac, start_utc, end_utc)
    if not readings:
        print(f"No readings for {local_yesterday} — skipping")
        return {'status': 'empty', 'date': local_yesterday.isoformat()}

    key = f"readings/{mac}/{local_yesterday.year}/{local_yesterday.month:02d}/{local_yesterday.day:02d}.jsonl.gz"
    body = _to_gz_jsonl(readings)

    _s3.put_object(
        Bucket=ARCHIVE_BUCKET,
        Key=key,
        Body=body,
        ContentType='application/gzip',
        ContentEncoding='gzip',
        Metadata={'station_id': mac, 'date': local_yesterday.isoformat(), 'count': str(len(readings))},
    )
    print(f"Archived {len(readings)} readings → s3://{ARCHIVE_BUCKET}/{key}")
    return {'status': 'ok', 'date': local_yesterday.isoformat(), 'count': len(readings), 'key': key}


def _fetch_range(mac: str, start_iso: str, end_iso: str) -> list:
    table = get_table(READINGS_TABLE)
    items, kwargs = [], dict(
        KeyConditionExpression=Key('station_id').eq(mac) & Key('timestamp').between(start_iso, end_iso),
        ScanIndexForward=True,
    )
    while True:
        result = table.query(**kwargs)
        items.extend(result.get('Items', []))
        last = result.get('LastEvaluatedKey')
        if not last:
            break
        kwargs['ExclusiveStartKey'] = last
    return [_serialize(item) for item in items]


def _serialize(item: dict) -> dict:
    out = {}
    for k, v in item.items():
        if k in ('station_id', 'ttl'):
            continue
        if hasattr(v, '__float__'):
            out[k] = float(v)
        else:
            out[k] = v
    return out


def _to_gz_jsonl(readings: list) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb') as gz:
        for row in readings:
            gz.write((json.dumps(row, separators=(',', ':')) + '\n').encode())
    return buf.getvalue()
