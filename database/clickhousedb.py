import os
import sys
from pathlib import Path

import clickhouse_connect

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.env import clickhouse_password

client = clickhouse_connect.get_client(
    host='clickhouse',
    port=8123,
    username=os.getenv('CLICKHOUSE_USER', 'admin'),
    password=clickhouse_password(),
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
