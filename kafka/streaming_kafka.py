from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

minio_access_key = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_USER", "slavakoder"))
minio_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_PASSWORD", "slavakoder"))
db_pass = os.getenv("ICEBERG_DB_PASS", "airflow")
minio_bucket = os.getenv("MINIO_BUCKET", "raw-bronze")
warehouse = f"s3a://{minio_bucket}/warehouse"
checkpoints = f"s3a://{minio_bucket}/checkpoints/multi_sink_V4"

spark = SparkSession.builder \
    .appName('stream_to_iceberg') \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.postgresql:postgresql:42.6.0,"
            "com.clickhouse:clickhouse-jdbc:0.5.0") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "jdbc") \
    .config("spark.sql.catalog.demo.uri", "jdbc:postgresql://postgres:5432/airflow") \
    .config("spark.sql.catalog.demo.jdbc.user", "airflow") \
    .config("spark.sql.catalog.demo.jdbc.password", db_pass) \
    .config("spark.sql.catalog.demo.warehouse", warehouse) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.db.windowed_stats (
        window struct<start:timestamp, end:timestamp>,
        status STRING,
        total_sum DOUBLE,
        tx_count BIGINT,
        currency STRING
    ) USING iceberg
""")

spark.sql("""
    ALTER TABLE demo.db.windowed_stats SET TBLPROPERTIES (
        'write.upsert.enabled'='true',
        'write.merge.mode'='merge-on-read',
        'write.update.mode'='merge-on-read',
        'write.delete.mode'='merge-on-read'
    )
""")

df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka_broker:29092') \
    .option('subscribe', 'p2p_transactions') \
    .option('startingOffsets', 'earliest') \
    .option('maxOffsetsPerTrigger', 10000) \
    .option('failOnDataLoss', 'false') \
    .load()

my_schema = 'tx_id INT, status STRING, amount DOUBLE, timestamp LONG, receiver_id STRING, currency STRING'

df_pars = df.select(F.from_json(F.col('value').cast('string'), my_schema).alias('data')).select('data.*') \
    .withColumn('event_time', F.from_unixtime(F.col('timestamp')).cast('timestamp')) 


dim = spark.table("demo.p2p_transfers").select(
    F.col("receiver_id"),
    F.col("currency").alias('valuta'),
).dropDuplicates(["receiver_id"])

df_enriched = df_pars \
    .withColumn("receiver_id", F.trim(F.col("receiver_id"))) \
    .join(F.broadcast(dim), on="receiver_id", how="left") \
    .withColumn("final_valuta", F.coalesce(F.col("currency"), F.col("valuta"))) \
    .drop("currency", "valuta") \
    .withColumnRenamed("final_valuta", "currency")

df_winda = df_enriched \
    .withWatermark('event_time', '15 minutes') \
    .groupBy(
        F.window(F.col('event_time'), '10 minutes', '1 minutes'),
        F.col('status'),
        F.col('currency'),
    ) \
    .agg(
        F.sum('amount').alias('total_sum'),
        F.count('tx_id').alias('tx_count')
    )

def write_to_iceberg_and_clickhouse(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    batch_df.writeTo("demo.db.windowed_stats").append()

    ch_df = batch_df \
        .withColumn("window_start", F.col("window.start")) \
        .withColumn("window_end", F.col("window.end")) \
        .drop("window")

    ch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
        .option("dbtable", "windowed_stats_ch") \
        .option("user", "admin") \
        .option("password", "admin_pass") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()

query = df_winda.writeStream \
    .foreachBatch(write_to_iceberg_and_clickhouse) \
    .outputMode("append") \
    .option("checkpointLocation", checkpoints) \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()