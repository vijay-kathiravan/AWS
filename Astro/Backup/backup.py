from airflow.sdk import DAG, task
import pandas as pd
import io
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import json
import requests
import time
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator, S3ListOperator, S3CreateObjectOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.operators.redshift_cluster import RedshiftCreateClusterOperator, RedshiftDeleteClusterOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from datetime import datetime, timedelta
import shutil
import boto3
# Configuration
STREAM_NAME = 'localhost-data-stream'
FIREHOSE_NAME = 'localhost-firehose-stream'
S3_BUCKET_BRONZE_ = "bronze{}".format(datetime.now().strftime('%Y%m%d%H%H%H'))
S3_BUCKET_BRONZE = S3_BUCKET_BRONZE_
S3_BUCKET_SILVER_ = "silver{}".format(datetime.now().strftime('%Y%m%d%H%H%H'))
S3_BUCKET_SILVER = S3_BUCKET_SILVER_
S3_BUCKET_GOLD_ = "gold{}".format(datetime.now().strftime('%Y%m%d%H%H'))
S3_BUCKET_GOLD = S3_BUCKET_GOLD_
S3_PREFIX = 'data/'
AWS_REGION = 'ca-central-1'
LOCALHOST_URL = 'http://host.docker.internal:8888/spotify'
DATABASE_NAME = 'spotify_database'
CRAWLER_NAME = 'spotify-bronze-crawler'
GLUE_JOB_NAME = 'spotify-json-to-parquet'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG('spotify',
         default_args=default_args,
         schedule=None,
         catchup=False) as dag:

    # Create Bronze S3 Bucket
    create_bronze_bucket = S3CreateBucketOperator(
        task_id='create_bronze_bucket',
        bucket_name=S3_BUCKET_BRONZE,
        aws_conn_id='aws_default',
    )

    # Create Silver S3 Bucket
    create_silver_bucket = S3CreateBucketOperator(
        task_id='create_silver_bucket',
        bucket_name=S3_BUCKET_SILVER,
        aws_conn_id='aws_default',
    )
    create_gold_bucket = S3CreateBucketOperator(
        task_id='create_gold_bucket',
        bucket_name=S3_BUCKET_GOLD,
        aws_conn_id='aws_default',
    )

    def create_kinesis_resources_func():
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        kinesis_client = session.client('kinesis')
        firehose_client = session.client('firehose')

        kinesis_client.create_stream(StreamName=STREAM_NAME, ShardCount=1)
        waiter = kinesis_client.get_waiter('stream_exists')
        waiter.wait(StreamName=STREAM_NAME, WaiterConfig={'Delay': 10, 'MaxAttempts': 3})

        stream_description = kinesis_client.describe_stream(StreamName=STREAM_NAME)
        stream_arn = stream_description['StreamDescription']['StreamARN']
        account_id = stream_arn.split(':')[4]

        firehose_config = {
            'DeliveryStreamName': FIREHOSE_NAME,
            'DeliveryStreamType': 'KinesisStreamAsSource',
            'KinesisStreamSourceConfiguration': {
                'KinesisStreamARN': stream_arn,
                'RoleARN': f'arn:aws:iam::{account_id}:role/firehose_delivery_role'
            },
            'ExtendedS3DestinationConfiguration': {
                'RoleARN': f'arn:aws:iam::{account_id}:role/firehose_delivery_role',
                'BucketARN': f'arn:aws:s3:::{S3_BUCKET_BRONZE}',
                'Prefix': S3_PREFIX + 'year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/',
                'ErrorOutputPrefix': 'errors/',
                'BufferingHints': {
                    'SizeInMBs': 1,
                    'IntervalInSeconds': 60
                },
                'CompressionFormat': 'GZIP',
                'ProcessingConfiguration': {
                    'Enabled': False
                }
            }
        }

        firehose_client.create_delivery_stream(**firehose_config)
        return {"stream_arn": stream_arn, "firehose_name": FIREHOSE_NAME}

    def stream_localhost_data_func(**context):
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        kinesis_client = session.client('kinesis')

        response = requests.get(f"{LOCALHOST_URL}", timeout=30)
        csv_data = io.StringIO(response.text)
        df = pd.read_csv(csv_data)
        records_to_send = df.to_dict('records')

        batch_size = 500
        for i in range(0, len(records_to_send), batch_size):
            batch = records_to_send[i:i + batch_size]
            kinesis_records = []

            for idx, record in enumerate(batch):
                enriched_record = {
                    'spotify_data': record,
                    'timestamp': datetime.now().isoformat(),
                    'source': 'localhost:8888/spotify',
                    'batch_id': context['run_id'],
                    'record_id': f"{context['run_id']}_{i + idx}",
                    'track_id': record.get('track_id', 'unknown')
                }

                kinesis_records.append({
                    'Data': json.dumps(enriched_record, default=str),
                    'PartitionKey': str(hash(record.get('track_id', str(idx))) % 10)
                })

            kinesis_client.put_records(
                Records=kinesis_records,
                StreamName=STREAM_NAME
            )

    def delete_kinesis_resources_func():
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        kinesis_client = session.client('kinesis')
        firehose_client = session.client('firehose')

        time.sleep(30)

        firehose_client.delete_delivery_stream(
            DeliveryStreamName=FIREHOSE_NAME,
            AllowForceDelete=True
        )

        time.sleep(30)
        kinesis_client.delete_stream(StreamName=STREAM_NAME)

    # Create Bronze Crawler
    create_bronze_crawler = GlueCrawlerOperator(
        task_id='create_bronze_crawler',
        config={
            'Name': CRAWLER_NAME,
            'Role': 'arn:aws:iam::207567758295:role/GlueServiceRole',
            'DatabaseName': DATABASE_NAME,
            'Targets': {
                'S3Targets': [
                    {
                        'Path': f's3://{S3_BUCKET_BRONZE}/',
                        'Exclusions': [
                            '**/_SUCCESS',
                            '**/errors/**',
                            '**/.spark-staging/**'
                        ]
                    }
                ]
            },
            'RecrawlPolicy': {
                'RecrawlBehavior': 'CRAWL_EVERYTHING'
            },
            'SchemaChangePolicy': {
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'LOG'
            },
            'Configuration': '{"Version":1.0,"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}'
        },
        aws_conn_id='aws_default',
    )
    def delete_crawler_func():
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        glue_client = session.client('glue')

        glue_client.delete_crawler(Name=CRAWLER_NAME)
    def delete_glue_job_func():
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        glue_client = session.client('glue')

        glue_client.delete_job(JobName=GLUE_JOB_NAME)

    # Upload Glue Script
    upload_glue_script = S3CreateObjectOperator(
        task_id='upload_glue_script',
        s3_bucket=S3_BUCKET_BRONZE,
        s3_key='glue_spotify_transform.py',
        data=("""
        # glue_spotify_transform.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE_NAME', 'TABLE_NAME', 'TARGET_BUCKET'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data using Glue Catalog (from crawler metadata)
datasource = glueContext.create_dynamic_frame.from_catalog(
    database=args['DATABASE_NAME'],
    table_name=args['TABLE_NAME']
)

print(f"Schema from catalog: {datasource.schema()}")
print(f"Original record count: {datasource.count()}")

# Convert to Spark DataFrame for easier manipulation
df = datasource.toDF()

# Flatten spotify_data struct using catalog schema
spotify_df = df.select(
    F.col("spotify_data.*"),  # Extract all nested fields based on catalog schema
    F.col("timestamp"),
    F.col("batch_id"),
    F.col("record_id")
)

# Simple null cleaning
cleaned_df = spotify_df.filter(
    F.col("track_id").isNotNull() &
    F.col("track_name").isNotNull()
).dropDuplicates(['track_id'])

print(f"Cleaned record count: {cleaned_df.count()}")

# Convert back to DynamicFrame for Glue optimizations
cleaned_dynamic_frame = DynamicFrame.fromDF(cleaned_df, glueContext, "cleaned_data")

# Write to Silver bucket as Parquet using Glue
glueContext.write_dynamic_frame.from_options(
    frame=cleaned_dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": f"s3://{args['TARGET_BUCKET']}/spotify-parquet/",
        "partitionKeys": ["track_genre"]  # Partition by genre
    },
    format="parquet",
    format_options={
        "compression": "snappy"
    }
)

print(f"Data successfully written to s3://{args['TARGET_BUCKET']}/spotify-parquet/")

job.commit()
        """),
        aws_conn_id='aws_default',
    )

    # Run Glue Transform
    def create_glue_job():
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        glue_client = session.client('glue')

        job_config = {
            'Name': GLUE_JOB_NAME,
            'Role': 'arn:aws:iam::207567758295:role/GlueServiceRole',
            'Command': {
                'Name': 'glueetl',
                'ScriptLocation': f's3://bronze20250808000000/glue_spotify_transform.py',  # Will be updated at runtime
                'PythonVersion': '3'
            },
            'GlueVersion': '3.0',
            'MaxRetries': 0,
            'Timeout': 60,
            'MaxCapacity': 2.0
        }

        glue_client.create_job(**job_config)

    def run_glue_job(**context):
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        glue_client = session.client('glue')

        # Just start the job run
        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                '--DATABASE_NAME': DATABASE_NAME,
                '--TABLE_NAME': 'bronze20250808000000',
                '--TARGET_BUCKET': 'silver20250808000000'
            }
        )

        job_run_id = response['JobRunId']

    # Monitor until completion (same monitoring code as above)
    # ... monitoring code ...
    # Create Kinesis Resources
    create_kinesis_resources = PythonOperator(
        task_id='create_kinesis_resources',
        python_callable=create_kinesis_resources_func,
    )

    # Stream Data
    stream_localhost_data = PythonOperator(
        task_id='stream_localhost_data',
        python_callable=stream_localhost_data_func,
    )

    # Delete Kinesis Resources
    delete_kinesis_resources = PythonOperator(
        task_id='delete_kinesis_resources',
        python_callable=delete_kinesis_resources_func,
    )
    delete_crawler = PythonOperator(
        task_id='delete_crawler',
        python_callable=delete_crawler_func,
    )
    delete_glue_job = PythonOperator(
        task_id='delete_glue_job',
        python_callable=delete_glue_job_func,
    )
    create_glue_job = PythonOperator(
        task_id='create_glue_job',
        python_callable=create_glue_job,
    )
    run_glue_job = PythonOperator(
        task_id='run_glue_job',
        python_callable=run_glue_job,
    )

    # Task Dependencies
    create_bronze_bucket >> create_kinesis_resources >> stream_localhost_data >> delete_kinesis_resources >> create_bronze_crawler >> create_silver_bucket >> delete_crawler >> upload_glue_script >> create_glue_job >> run_glue_job >> create_gold_bucket >> delete_glue_job