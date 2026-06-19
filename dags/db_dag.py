from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
import os
import boto3
import dotenv
from botocore.client import Config
import shutil

def_args = {
    'owner': 'главный',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=25)
}

def end_message():
    print('данные готовы')

with DAG(
    'spark_pipeline',
    default_args=def_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:
    
    iceberg_task = BashOperator(
        task_id='iceberg_spark_job',
        bash_command="""
set -euo pipefail

docker exec -i \
  -e ICEBERG_DB_PASS="$ICEBERG_DB_PASS" \
  -e AWS_ACCESS_KEY_ID="$MINIO_USER" \
  -e AWS_SECRET_ACCESS_KEY="$MINIO_PASSWORD" \
  spark_single spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0 \
  --conf spark.driver.memory=6g \
  --conf spark.sql.shuffle.partitions=64 \
  /home/jovyan/work/database/iceberg_db.py
""".strip(),
    )
    start_task = BashOperator(
        task_id='start_spark',
        bash_command= 'echo "додониднид"'
    )

    end_task = PythonOperator(
        task_id='end_message',
        python_callable=end_message
    )
    start_task >> iceberg_task >> end_task