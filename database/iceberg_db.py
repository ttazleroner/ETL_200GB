from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
# .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \ ЭТО К СПАРКСЕССИИ.БИЛДЕР, А НЕ КОНФИГУРАЦИЯ КАТАЛОГА
# .config("spark.jars.packages", ",".join(ICEBERG_PACKAGES)) \ ЕСЛИ ХОТИТЕ К АИРФЛОУ ПОДКЛЮЧИТЬ
minio_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
minio_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "adminadmin")
DB_PASS = "airflow"

ICEBERG_PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.6.0"
]

spark = SparkSession.builder \
    .appName("IcebergTesting") \
    .config("spark.sql.catalog.demo.jdbc.password", DB_PASS) \
    .config("spark.sql.catalog.demo.jdbc.schema-version", "V1") \
    \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "jdbc") \
    .config("spark.sql.catalog.demo.uri", "jdbc:postgresql://postgres:5432/airflow") \
    .config("spark.sql.catalog.demo.jdbc.user", "airflow") \
    .config("spark.sql.catalog.demo.warehouse", "s3a://raw-bronze/warehouse") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
    \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key ) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    \
    .getOrCreate()

print('iceberg is already used')

# spark.sql("""DROP TABLE IF EXISTS demo.p2p_transfers""")
# spark.sql("""DROP TABLE IF EXISTS demo.dlq_transfers""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.p2p_transfers (
        tx_id STRING,
        sender_id STRING,
        receiver_id STRING,
        amount DOUBLE,
        currency STRING,
        status STRING,
        timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (status)
""")

spark.sql("""
    ALTER TABLE demo.p2p_transfers SET TBLPROPERTIES (
        'write.target-file-size-bytes' = '134217728',
        'write.distribution-mode' = 'hash',
        'write.spark.fanout.enabled' = 'true'
    )
""")

# #очистка
# spark.sql("CALL demo.system.rewrite_data_files(table => 'demo.p2p_transfers')")
# spark.sql("CALL demo.system.expire_snapshots(table => 'demo.p2p_transfers', retain_last => 5)")
# spark.sql("CALL demo.system.remove_orphan_files(table => 'demo.p2p_transfers')")

spark.sql("""
    SELECT sender_id, tx_id, timestamp, amount,
        AVG (amount) OVER (PARTITION BY tx_id) AS user_avg_amount,
        amount - AVG(amount) OVER (PARTITION BY sender_id) AS diff_from_avg
    FROM demo.p2p_transfers
""")

spark.sql("""
    SELECT file_path, record_count, file_size_in_bytes, partition 
    FROM demo.p2p_transfers.files
""").show(truncate=False) 

spark.sql("""SELECT * FROM demo.p2p_transfers LIMIT 20""").show(truncate=False)

spark.sql("""
    SELECT status, COUNT(*) AS count
    FROM demo.p2p_transfers
    GROUP BY status
""").show()

spark.sql("""
    SELECT * FROM demo.p2p_transfers
    WHERE amount > 10000 AND currency = 'USD'
""")

spark.sql("""
    SELECT * FROM demo.p2p_transfers
    WHERE status = 'Unknown'
""").show()
