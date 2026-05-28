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
    'start_date': datetime(2026,1,1)
}

check_table = (
    "docker exec -e ICEBERG_DB_PASS='airflow' "
    "-e AWS_ACCESS_KEY_ID=\"$MINIO_USER\" -e AWS_SECRET_ACCESS_KEY=\"$MINIO_PASSWORD\" "
    "spark_single spark-submit "
    "--packages "
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.postgresql:postgresql:42.6.0 "
    "/home/jovyan/work/database/iceberg_db.py"
)

def start_msg():
    print('проверка таблицы')
def end_msg():
        print('просмотр таблицы готов')


with DAG(
    'check_table',
    default_args=def_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    
    check_table_task = BashOperator(
        task_id='check_table',
        bash_command=check_table
    )

    end_task = PythonOperator(
        task_id='end_message',
        python_callable=end_msg
    )

    start_task = PythonOperator(
        task_id='start_message',
        python_callable=start_msg
    )
    start_task >> check_table_task >> end_task
