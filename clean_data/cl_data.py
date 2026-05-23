import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_ACCESS = "slavakoder"
MINIO_SECRET = "slavakoder"
DB_PASS = "airflow"

fixedsuka = ['NULL']

spark = SparkSession.builder \
    .appName('cleandata') \
    .config('spark.driver.memory', '2g') \
    .config('spark.executor.memory', '2g') \
    .config('spark.shuffle.partitions', '8') \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
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

ddl_schema = "tx_id STRING, sender_id STRING, receiver_id STRING, amount DOUBLE, currency STRING, status STRING, timestamp LONG"

df = spark.read.csv(raw_data, header=True, schema=ddl_schema)


df = (df
    .withColumn('timestamp', F.from_unixtime(F.col('timestamp')).cast('timestamp'))
    .withColumn('sender_id', F.regexp_replace(F.col('sender_id'), r'\s+', ''))
    .withColumn('receiver_id', F.regexp_replace(F.col('receiver_id'), r'\s+', ''))
)
df = df.fillna('1970-01-01 00:00:00', subset=['timestamp'])
df.show(5)