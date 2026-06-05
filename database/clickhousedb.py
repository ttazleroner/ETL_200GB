import clickhouse_connect
from datetime import datetime

client = clickhouse_connect.get_client(
    host='clickhouse', 
    port=8123, 
    username='admin', 
    password='admin_pass'
)

# client.command("DROP TABLE IF EXISTS default.windowed_stats_ch")

client.command("""
    CREATE TABLE IF NOT EXISTS default.windowed_stats_ch (
        window_start DateTime,
        window_end DateTime,
        status String,
        total_sum Float64,
        tx_count Int64,
        currency String
    ) 
    ENGINE = MergeTree() 
    ORDER BY (window_start, status)
""")