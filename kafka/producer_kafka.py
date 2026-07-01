import csv
import io
import json
import os
import sys
from pathlib import Path

import boto3
from kafka import KafkaProducer

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.env import minio_access_key, minio_endpoint, minio_secret_key

TOPIC = 'p2p_transactions'
BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka_broker:29092')
BUCKET = os.getenv('MINIO_BUCKET', 'raw-bronze')
PREFIX = os.getenv('MINIO_PREFIX', 'landing/p2p_transfers/')
ENDPOINT = minio_endpoint()
MESSAGE_LIMIT = int(os.getenv('KAFKA_MESSAGE_LIMIT', '100001'))
FLUSH_EVERY = int(os.getenv('KAFKA_FLUSH_EVERY', '40000'))


def s3_client():
    return boto3.client(
        's3',
        endpoint_url=ENDPOINT,
        aws_access_key_id=minio_access_key(),
        aws_secret_access_key=minio_secret_key(),
    )


def clean_amount(val):
    try:
        return float(str(val).replace(',', '.').strip().strip('"'))
    except (TypeError, ValueError):
        return 0.0


def parse_timestamp(val):
    if val is None or str(val).strip() == '':
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def row_to_event(row: dict, seq_id: int) -> dict:
    """CSV из generate_fake_data → JSON под схему streaming_kafka."""
    receiver = row.get('receiver_id', '')
    if isinstance(receiver, str):
        receiver = receiver.strip()

    return {
        'tx_id': seq_id,
        'status': row.get('status', ''),
        'amount': clean_amount(row.get('amount', 0)),
        'timestamp': parse_timestamp(row.get('timestamp')),
        'receiver_id': receiver,
    }


def iter_rows_from_minio(client):
    paginator = client.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv'):
                keys.append(key)
    if not keys:
        raise FileNotFoundError(
            f'В s3://{BUCKET}/{PREFIX} нет CSV. сначала запустите generate_fake_data.py.'
        )

    for key in sorted(keys):
        print(f'читаем s3://{BUCKET}/{key}')
        response = client.get_object(Bucket=BUCKET, Key=key)
        text = io.TextIOWrapper(response['Body'], encoding='utf-8')
        yield from csv.DictReader(text)


producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP],
    api_version=(3, 7, 0),
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
)

client = s3_client()
sent = 0

for i, row in enumerate(iter_rows_from_minio(client)):
    producer.send(TOPIC, value=row_to_event(row, seq_id=i + 1))
    sent = i + 1
    if i > 0 and i % FLUSH_EVERY == 0:
        producer.flush()
        print(f'отправлено {i} сообщений')
    if i >= MESSAGE_LIMIT - 1:
        break

producer.flush()
print(f'готово: отправлено {sent} сообщений в топик {TOPIC} из MinIO')
