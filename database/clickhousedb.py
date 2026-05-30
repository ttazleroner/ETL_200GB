import clickhouse_connect
from datetime import datetime

client = clickhouse_connect.get_client(
    host='clickhouse', 
    port=8123, 
    username='admin', 
    password='admin_pass'
)

client.command("""
    CREATE TABLE IF NOT EXISTS test_table (
        tx_id Int64,
        currency String,
        timestamp DateTime
    ) 
    ENGINE = MergeTree() 
    ORDER BY (timestamp, tx_id)
""")

print("clickhouse database is created")

print("генерация 100,000 строк")
rows = [
    [i, f'USD_{i}', datetime.now()]
    for i in range(100000)
]

client.insert('test_table', rows, column_names=['tx_id', 'currency', 'timestamp'])
print(" 100к строк улетели")

print("\n физические куски данных на диске:")
parts_info = client.query("""
    SELECT name, rows, bytes_on_disk 
    FROM system.parts 
    WHERE table = 'test_table' AND active = 1
""")
for part in parts_info.result_set:
    print(f"кусок: {part[0]} | строк: {part[1]} | размер: {part[2]} байт")

print("\n данные в клике:")
result = client.query("SELECT * FROM test_table LIMIT 5")
for row in result.result_set:
    print(row)