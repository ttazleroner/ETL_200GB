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

def minio_data():
    endpoint = 'http://minio:9000'
    minio_key = os.getenv('MINIO_USER')
    minio_secretkey = os.getenv('MINIO_PASSWORD')
    bucket2_name = 'clear_data'

    s3 = boto3.resource('s3',
    endpoint_url=endpoint,
    aws_access_key_id=minio_key,
    aws_secret_access_key=minio_secretkey,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
    )
    try:
        s3.meta.client.head_bucket(Bucket=bucket2_name)
    except:
        s3.create_bucket(Bucket=bucket2_name)

def end_message():
    print('данные готовы')

with DAG(
    'spark_dag',
    default_args=def_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    
    start_task = BashOperator(
        task_id='start_spark',
        bash_command= 'echo "додониднид"'
    )

    spark_clean = BashOperator(
        task_id='spark_clean',
        bash_command='docker exec spark_single spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0 /home/jovyan/work/clean_data/cl_data.py'
    )

    end_task = PythonOperator(
        task_id='end_spark',
        python_callable=end_message
    )

    start_task >> spark_clean >> end_task