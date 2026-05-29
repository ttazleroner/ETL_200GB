import csv
import json
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['kafka_broker:29092'],
    api_version=(3, 7, 0),
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def clean_data(val):
    try:
        return float(val.replace(',', '.')).strip()
    except:
        return 0.0
with open('p2p_transfers.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        row['amount'] = clean_data(row['amount'])
        producer.send('p2p_transfers', value=row)
        if i % 40000 == 0:
            print(f"отправлено {row} сообщений")    
            producer.flush()
        if i > 100000:
            break
producer.flush()
print("сообщения отправлены")
