import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_ACCESS = "slavakoder"
MINIO_SECRET = "slavakoder"
DB_PASS = "airflow"

fixedsuka = ['NULL']

spark = SparkSession.builder \
    .appName('cleandata') \
    .config('spark.driver.memory', '4g') \
    .config('spark.executor.memory', '4g') \
    .config('spark.shuffle.partitions', '8') \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "4g") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.sql.catalog.demo.type", "jdbc") \
    .config("spark.sql.catalog.demo.uri", "jdbc:postgresql://postgres:5432/airflow") \
    .config("spark.sql.catalog.demo.jdbc.user", "airflow") \
    .config("spark.sql.catalog.demo.jdbc.password", DB_PASS) \
    .config("spark.sql.catalog.demo.warehouse", "s3a://raw-bronze/warehouse") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()
print('запускаемся')

spark.sparkContext.setLogLevel('WARN')

raw_data = 's3a://raw-bronze/landing/p2p_transfers/*.csv'
# raw_data = "s3a://raw-bronze/landing/p2p_transfers/chunk_1.csv"
dlq_data = 's3a://raw-bronze/dlq/dlq_transfers/'


rename_dict = ('Unknown')

column_kolonki = ['tx_id', 'sender_id', 'timestamp', 'status', 'receiver_id', 'amount', 'currency']

ddl_schema = "tx_id STRING, sender_id STRING, receiver_id STRING, amount STRING, currency STRING, status STRING, timestamp LONG"

df = spark.read.csv(raw_data, header=True, schema=ddl_schema)


df = (df
    .withColumn('timestamp', F.from_unixtime(F.col('timestamp')).cast('timestamp'))
    .withColumn('sender_id', F.regexp_replace(F.col('sender_id'), r'\s+', ''))
    .withColumn('receiver_id', F.regexp_replace(F.col('receiver_id'), r'\s+', ''))
    .withColumn('amount', F.regexp_replace(F.col('amount'), ',', '.').cast('double'))
    .withColumn('timestamp', F.coalesce(F.col('timestamp'), F.lit('1970-01-01 00:00:00')))
    .withColumn('status', F.coalesce(F.col('status'), F.lit('Unknown')))
    .withColumn('status', F.trim(F.col('status')))
) 
for kolonki in column_kolonki:
    df = df.withColumn(kolonki, F.trim(F.col(kolonki)))
df = df.fillna('1970-01-01 00:00:00', subset=['timestamp'])
df = df.dropDuplicates(['tx_id',])
df = df.replace(['', 'N/A', 'NULL ', 'NULL', ' NULL', ' null', ' '], 'Unknown', subset=['status'])
df = df.sortWithinPartitions('sender_id')
df_kruto = df.filter(
    (F.col('amount') > 0) & 
    (F.col('status').isNotNull()) &
    (F.col('status') != 'Unknown') &
    (F.col('timestamp') > '1970-01-01 00:00:00')
)
df_zalupa = df.filter(
    (F.col('amount') < 0) |
    (F.col('status').isNull()) |
    (F.col('status') == 'Unknown') |
    (F.col('timestamp') < '1970-01-01 00:00:00') |
    (F.col('timestamp').isNull())
)





df_final = df_kruto.select(
    F.col("tx_id").cast("string"),
    F.col("sender_id").cast("string"),
    F.col("receiver_id").cast("string"),
    F.col("amount").cast("double"),
    F.col("currency").cast("string"),
    F.col("status").cast("string"),
    F.col("timestamp").cast("timestamp")
)





df_dlq = df_zalupa.withColumn("dlq_processed_at", F.current_timestamp())
df_dlq.write.mode("append").parquet("s3a://raw-bronze/logical_dlq/")
df_final.writeTo('demo.p2p_transfers').append()
df.show(5)