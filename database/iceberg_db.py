from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

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
    .config("spark.jars.packages", ",".join(ICEBERG_PACKAGES)) \
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
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

print('iceberg is already used')

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

minio_data = "s3a://raw-bronze/landing/p2p_transfers/chunk_1.csv"
df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(minio_data)

df_iceberg = df.select(
    F.col('tx_id').cast('string'),
    F.col('sender_id').cast('string'),
    F.col('receiver_id').cast('string'),
    F.col('amount').cast('double'),
    F.col('currency').cast('string'),
    F.col('status').cast('string'),
    F.col('timestamp').cast('timestamp'),
)

df_iceberg.writeTo("demo.p2p_transfers").append()
print('данные в iceberg')

#очистка
spark.sql("CALL demo.system.rewrite_data_files(table => 'demo.p2p_transfers')")
spark.sql("CALL demo.system.expire_snapshots(table => 'demo.p2p_transfers', retain_last => 5)")
spark.sql("CALL demo.system.remove_orphan_files(table => 'demo.p2p_transfers')")


spark.sql("""
    SELECT file_path, record_count, file_size_in_bytes, partition 
    FROM demo.p2p_transfers.files
""").show(truncate=False) 