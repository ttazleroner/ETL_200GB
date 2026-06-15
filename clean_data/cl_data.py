import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_ACCESS = "slavakoder"
MINIO_SECRET = "slavakoder"
DB_PASS = "airflow"


spark = SparkSession.builder \
    .appName('cleandata') \
    .config('spark.driver.memory', '4g') \
    .config('spark.executor.memory', '6g') \
    .config("spark.memory.offHeap.size", "4g") \
    .config('spark.sql.shuffle.partitions', '64') \
    .config('spark.shuffle.partitions', '64') \
    .config('spark.executor.cores', '2') \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.hadoop.fs.s3a.committer.name", "magic") \
    .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true") \
    .config("spark.hadoop.fs.s3a.impl.disable.cache", "true") \
    .config("spark.memory.offHeap.size", "3g") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.multipart.size", "32M") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "200") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "600000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.attempts.maximum", "20") \
    .config("spark.hadoop.fs.s3a.multipart.size", "32M") \
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

# spark.sql (" DROP TABLE IF EXISTS demo.p2p_transfers")

column_kolonki = ['tx_id', 'sender_id', 'timestamp', 'status', 'receiver_id', 'amount', 'currency']
ddl_schema = "tx_id STRING, sender_id STRING, receiver_id STRING, amount STRING, currency STRING, status STRING, timestamp LONG"


Path = spark._jvm.org.apache.hadoop.fs.Path
FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
URI = spark._jvm.java.net.URI
fs = FileSystem.get(URI("s3a://raw-bronze/"), spark._jsc.hadoopConfiguration())
status_list = fs.listStatus(Path("s3a://raw-bronze/landing/p2p_transfers/"))
files = [str(f.getPath()) for f in status_list if f.getPath().getName().endswith('.csv')]

for index, file_path in enumerate(files, 1):
    df = spark.read.csv(file_path, header=True, schema=ddl_schema)
    df = (df
    .withColumn('timestamp', F.from_unixtime(F.col('timestamp')).cast('timestamp'))
    .withColumn('sender_id', F.regexp_replace(F.col('sender_id'), r'\s+', ''))
    .withColumn('receiver_id', F.regexp_replace(F.col('receiver_id'), r'\s+', ''))
    .withColumn('amount', F.regexp_replace(F.col('amount'), ',', '.').cast('double'))
    .withColumn('timestamp', F.coalesce(F.col('timestamp'), F.lit('1970-01-01 00:00:00')))
    .withColumn('status', F.coalesce(F.col('status'), F.lit('Unknown')))
    .withColumn('status', F.trim(F.col('status')))
    )

    df = df.select([F.trim(F.col(c)).alias(c) if c in column_kolonki else F.col(c) for c in df.columns])
    df = df.fillna('1970-01-01 00:00:00', subset=['timestamp'])
    # df = df.dropDuplicates(['tx_id',])
    df = df.replace(['', 'N/A', 'NULL ', 'NULL', ' NULL', ' null', ' '], 'Unknown', subset=['status'])

    df_kruto = df.filter(
        (F.col('amount') > 0) & 
        (F.col('status').isNotNull()) &
        (F.col('status') != 'Unknown') &
        (F.col('timestamp') > '1970-01-01 00:00:00')
    )
    df_zalupa = df.filter(
        (F.col('amount') < 0) |
        (F.col('amount').isNull()) |
        (F.col('status').isNull()) |
        (F.col('status') == 'Unknown') |
        (F.col('timestamp') < '1970-01-01 00:00:00') |
        (F.col('timestamp').isNull())
    ).withColumn('dlq_processed_at', F.current_timestamp())

    df_final = df_kruto.select(
        F.col("tx_id").cast("string"),
        F.col("sender_id").cast("string"),
        F.col("receiver_id").cast("string"),
        F.col("amount").cast("double"),
        F.col("currency").cast("string"),
        F.col("status").cast("string"),
        F.col("timestamp").cast("timestamp")
    )

    df_zalupa \
        .repartition(4) \
        .write.mode("append") \
        .parquet("s3a://raw-bronze/logical_dlq/")
    df_final = df_final.repartition(2).sortWithinPartitions('status')
    if index == 1:
        df_final.writeTo('demo.p2p_transfers').createOrReplace()
    else:
        df_final.writeTo('demo.p2p_transfers').append()
    df.unpersist()
    df_final.unpersist()
    spark.catalog.clearCache()
spark.stop()