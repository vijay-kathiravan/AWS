from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteBucketOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        's3_bucket_operations',
        default_args=default_args,
        description='DAG for S3 bucket and object operations',
        schedule="@once",
        catchup=False,
        tags=['s3', 'demo'],
) as dag:
    #task1
    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        aws_conn_id="aws",
        bucket_name="vijayairflowcreationbucket",
    )
    #task2
    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        aws_conn_id="aws",
        bucket_name="vijayairflowdeletion",
        force_delete=True,
    )
    #task3
    list_keys = S3ListOperator(
        task_id="list_keys",
        aws_conn_id="aws",
        bucket="vijaylistkeysairflow",
    )

    create_bucket >> delete_bucket >> list_keys