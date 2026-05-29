import json
from kafka import KafkaConsumer

topic='p2p_transfers'
group_id='p2p_group'

def json(data):
    try:
        return json.json(data.decode('utf-8'))
    except Exception as e:
        print(f'не получилось распарсить {e}')
        return None

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=['kafka_broker:29092'],
    api_version=(3, 7, 0),
    value_deserializer=json,
    group_id=group_id,
    enable_auto_commit=True,
    auto_offset_reset='earliest'
)
try:
    for message in consumer:
        data = message.value
        if not data: continue
        try:
            p2p_amount=str(data.get('amount',0)).replace(',', '.')
            amount=float(p2p_amount)
            if amount > 50000:
                print(f'вери биг транзакция: {amount} | юзер: {data.get("user")}')
        except ValueError as e:
            print(f'кривые данные {e}')
except KeyboardInterrupt:
    print("стоп")
finally:
    consumer.close()