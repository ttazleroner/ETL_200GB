import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.env import (
    iceberg_db_password,
    iceberg_warehouse,
    minio_access_key,
    minio_endpoint,
    minio_secret_key,
)

ICEBERG_PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.6.0"
]

minio_access_key = minio_access_key()
minio_secret_key = minio_secret_key()
db_pass = iceberg_db_password()

spark = SparkSession.builder \
    .appName("IcebergTesting") \
    .config("spark.jars.packages", ",".join(ICEBERG_PACKAGES)) \
    .config("spark.sql.catalog.demo.jdbc.password", db_pass) \
    .config("spark.sql.catalog.demo.jdbc.schema-version", "V1") \
    \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "jdbc") \
    .config("spark.sql.catalog.demo.uri", "jdbc:postgresql://postgres:5432/airflow") \
    .config("spark.sql.catalog.demo.jdbc.user", "airflow") \
    .config("spark.sql.catalog.demo.warehouse", iceberg_warehouse()) \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
    \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint()) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key ) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

spark.sql("""
    CREATE OR REPLACE VIEW demo.p2p_transfers_view AS
    SELECT tx_id, amount, currency, status, timestamp
    FROM demo.p2p_transfers
""")

spark.sql("""
    SELECT * FROM demo.p2p_transfers LIMIT 20
""").show()

spark.sql("""
    SELECT file_path, record_count FROM demo.p2p_transfers.files
    FROM demo.p2p_transfers
""")