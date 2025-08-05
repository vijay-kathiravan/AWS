from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.kinesis import FirehoseHook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaCreateFunctionOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.operators.redshift_cluster import RedshiftCreateClusterOperator, RedshiftDeleteClusterOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from datetime import datetime, timedelta
import boto3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG('spotify_streaming_analytics',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    def create_kinesis_stream(**kwargs):
        client = boto3.client('kinesis')
        client.create_stream(StreamName='spotify_stream', ShardCount=1)

    def delete_kinesis_stream(**kwargs):
        client = boto3.client('kinesis')
        client.delete_stream(StreamName='spotify_stream', EnforceConsumerDeletion=True)

    create_kinesis = PythonOperator(
        task_id='create_kinesis_stream',
        python_callable=create_kinesis_stream
    )

    # Ingest data via Kinesis (custom logic or use Firehose operator)
    # ... (your ingestion logic here)

    delete_kinesis = PythonOperator(
        task_id='delete_kinesis_stream',
        python_callable=delete_kinesis_stream
    )

    # Glue Crawler for Bronze Layer
    create_bronze_crawler = AwsGlueCrawlerOperator(
        task_id='create_bronze_crawler',
        config={'Name': 'bronze-crawler', 'Role': 'your-glue-role', 'DatabaseName': 'bronze_db', 'Targets': {'S3Targets': [{'Path': 's3://your-bucket/bronze/'}]}}
    )

    # Glue ETL Job (PySpark)
    run_glue_etl = AwsGlueJobOperator(
        task_id='run_glue_etl',
        job_name='spotify-etl-job',
        script_location='s3://your-bucket/scripts/spotify_etl.py',
        script_args={'--input_path': 's3://your-bucket/bronze/', '--output_path': 's3://your-bucket/silver/'}
    )

    # Glue Crawler for Silver Layer
    create_silver_crawler = AwsGlueCrawlerOperator(
        task_id='create_silver_crawler',
        config={'Name': 'silver-crawler', 'Role': 'your-glue-role', 'DatabaseName': 'silver_db', 'Targets': {'S3Targets': [{'Path': 's3://your-bucket/silver/'}]}}
    )

    # Lambda to remove outliers and load to Redshift
    run_lambda = AwsLambdaInvokeFunctionOperator(
        task_id='run_lambda',
        function_name='remove_outliers_and_load_to_redshift',
        payload='{}'
    )

    # Athena Query
    run_athena_query = AthenaOperator(
        task_id='run_athena_query',
        query='SELECT * FROM gold_table LIMIT 10;',
        database='gold_db',
        output_location='s3://your-bucket/athena-results/'
    )

    # DAG dependencies
    create_kinesis >> delete_kinesis >> create_bronze_crawler >> run_glue_etl >> create_silver_crawler >> run_lambda >> run_athena_query