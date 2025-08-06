from airflow.sdk import DAG, task
import pandas as pd
import io
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import json
import requests
import time
from airflow.providers.amazon.aws.hooks.kinesis import FirehoseHook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator, S3ListOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaCreateFunctionOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.operators.redshift_cluster import RedshiftCreateClusterOperator, RedshiftDeleteClusterOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from datetime import datetime, timedelta
import shutil
import boto3

# Configuration
S3Bucketname_bronze = f"BronzeBucket{datetime.now().strftime('%Y%m%d')}"
STREAM_NAME = 'localhost-data-stream'
FIREHOSE_NAME = 'localhost-firehose-stream'
#S3_BUCKET = S3Bucketname_bronze
S3_BUCKET_BRONZE = "bronze{}".format(datetime.now().strftime('%Y%m%d%H%M%S'))
S3_BUCKET_SILVER = "SILVER{}".format(datetime.now().strftime('%Y%m%d%H%M%S'))
S3_BUCKET_GOLD = "GOLD{}".format(datetime.now().strftime('%Y%m%d%H%M%S'))
S3_PREFIX = 'data/'
AWS_REGION = 'ca-central-1'
LOCALHOST_URL = 'http://host.docker.internal:8888/spotify'  # Your streaming endpoint


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
    default_args=default_args, schedule= None, catchup= False
) as dag:
    def S3bucketCreation(S3_BUCKET):
        create_s3_bucket = S3CreateBucketOperator(
            task_id='create_bucket',
            bucket_name=S3_BUCKET,
            aws_conn_id='aws_default',
        )
    S3bucketCreation(S3_BUCKET_BRONZE)
    def create_kinesis_resources():
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
                'BucketARN': f'arn:aws:s3:::{S3_BUCKET}',
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

    def stream_localhost_data(**context):
        import pandas as pd
        import io

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

    def monitor_pipeline(**context):
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        cloudwatch = session.client('cloudwatch')
        kinesis_client = session.client('kinesis')

        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=10)

        kinesis_metrics = cloudwatch.get_metric_statistics(
            Namespace='AWS/Kinesis',
            MetricName='IncomingRecords',
            Dimensions=[{'Name': 'StreamName', 'Value': STREAM_NAME}],
            StartTime=start_time,
            EndTime=end_time,
            Period=300,
            Statistics=['Sum']
        )

        total_records = sum([point['Sum'] for point in kinesis_metrics['Datapoints']])
        stream_info = kinesis_client.describe_stream(StreamName=STREAM_NAME)
        stream_status = stream_info['StreamDescription']['StreamStatus']

        return {
            'total_records': total_records,
            'stream_status': stream_status,
            'timestamp': datetime.now().isoformat()
        }
    def delete_kinesis_resources():
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        kinesis_client = session.client('kinesis')
        firehose_client = session.client('firehose')

        # Wait 2 minutes for data to transfer
        time.sleep(60)

        # Delete Firehose delivery stream
        firehose_client.delete_delivery_stream(
            DeliveryStreamName=FIREHOSE_NAME,
            AllowForceDelete=True
        )

        # Wait 1 minute for Firehose deletion
        time.sleep(60)

        # Delete Kinesis data stream
        kinesis_client.delete_stream(StreamName=STREAM_NAME)
    def Glue_Data_Cleaning(**context):


    # Task definitions
    create_resources = PythonOperator(
        task_id='create_kinesis_resources',
        python_callable=create_kinesis_resources,
        dag=dag
    )

    stream_data = PythonOperator(
        task_id='stream_localhost_data',
        python_callable=stream_localhost_data,
        dag=dag
    )

    monitor_task = PythonOperator(
        task_id='monitor_pipeline',
        python_callable=monitor_pipeline,
        dag=dag
    )
    delete_resources = PythonOperator(
        task_id='delete_kinesis_resources',
        python_callable=delete_kinesis_resources,
        dag=dag
)

    # Set dependencies
    create_resources >> stream_data >> monitor_task >> delete_resources
