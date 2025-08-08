from airflow.sdk import DAG
import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator,S3CreateObjectOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from datetime import datetime, timedelta
import json, io, requests, time, shutil

# #AWS - Airflow Config
# {
#     "aws_access_key_id": "",
#     "aws_secret_access_key": "",
#     "region_name": "ca-central-1"
# }
#Requirements folder
#apache-airflow-providers-amazon

# Configuration
STREAM_NAME = 'localhost-data-stream'
FIREHOSE_NAME = 'localhost-firehose-stream'
S3_BUCKET_BRONZE_ = "bronzevijayspotifyair"
S3_BUCKET_BRONZE = S3_BUCKET_BRONZE_
S3_BUCKET_SILVER_ = "silvervijayspotifyair"
S3_BUCKET_SILVER = S3_BUCKET_SILVER_
S3_BUCKET_GOLD_ = "goldvijayspotifyair"
S3_BUCKET_GOLD = S3_BUCKET_GOLD_
S3_PREFIX = 'data/'
AWS_REGION = 'ca-central-1'
LOCALHOST_URL = 'http://host.docker.internal:8888/spotify'
DATABASE_NAME = 'spotify_database'
DATABASE_NAME_SILVER = 'spotify_database_silver'
CRAWLER_NAME = 'spotify-bronze-crawler'
CRAWLER_NAME_SILVER = 'spotify-silver-crawler'
GLUE_JOB_NAME = 'spotify-json-to-parquet'
REDSHIFT_NAMESPACE = 'redshiftspotify'
REDSHIFT_WORKGROUP = 'redshiftspotifywg'
REDSHIFT_DATABASE = 'dev'
REDSHIFT_SCHEMA = 'redshiftspotifyschema'
REDSHIFT_TABLE = 'redshiftspotifytable'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Function to create GlueCrawlerOperator
def make_glue_crawler_operator(
        task_id,
        crawler_name,
        role_arn,
        database_name,
        s3_path,
        aws_conn_id='aws_default'
):
    return GlueCrawlerOperator(
        task_id=task_id,
        config={
            'Name': crawler_name,
            'Role': role_arn,
            'DatabaseName': database_name,
            'Targets': {
                'S3Targets': [
                    {
                        'Path': s3_path,
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
        aws_conn_id=aws_conn_id,
    )

# Function to create delete crawler function
def make_delete_crawler_func(crawler_name):
    def delete_crawler_func():
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        glue_client = session.client('glue')

        print(f"Deleting crawler: {crawler_name}")
        glue_client.delete_crawler(Name=crawler_name)
        print(f"Crawler {crawler_name} deleted successfully")

    return delete_crawler_func

with (DAG('spotify',
         default_args=default_args,
         schedule=None,
         catchup=False) as dag):

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

        time.sleep(10)

        firehose_client.delete_delivery_stream(
            DeliveryStreamName=FIREHOSE_NAME,
            AllowForceDelete=True
        )

        time.sleep(10)
        kinesis_client.delete_stream(StreamName=STREAM_NAME)

    def delete_glue_job_func():
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        glue_client = session.client('glue')

        glue_client.delete_job(JobName=GLUE_JOB_NAME)

    def create_glue_job():
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        glue_client = session.client('glue')

        job_config = {
            'Name': GLUE_JOB_NAME,
            'Role': 'arn:aws:iam::207567758295:role/GlueServiceRole',
            'Command': {
                'Name': 'glueetl',
                'ScriptLocation': f's3://{S3_BUCKET_BRONZE}/glue_spotify_transform.py',
                'PythonVersion': '3'
            },
            'GlueVersion': '3.0',
            'MaxRetries': 0,
            'Timeout': 60,
            'MaxCapacity': 2.0
        }

        print(f"Creating Glue job: {GLUE_JOB_NAME}")

        # Check if job already exists
        existing_jobs = glue_client.get_jobs()
        job_names = [job['Name'] for job in existing_jobs['Jobs']]

        if GLUE_JOB_NAME in job_names:
            print(f"Glue job {GLUE_JOB_NAME} already exists, deleting first")
            glue_client.delete_job(JobName=GLUE_JOB_NAME)
            time.sleep(10)  # Wait for deletion to complete

        glue_client.create_job(**job_config)
        print(f"Glue job {GLUE_JOB_NAME} created successfully")
        return {"job_name": GLUE_JOB_NAME, "status": "created"}

    def run_glue_job(**context):
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        glue_client = session.client('glue')

        print(f"Starting Glue job: {GLUE_JOB_NAME}")
        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                '--DATABASE_NAME': DATABASE_NAME,
                '--TABLE_NAME': S3_BUCKET_BRONZE,  # Use actual table name
                '--TARGET_BUCKET': S3_BUCKET_SILVER
            }
        )

        job_run_id = response['JobRunId']
        print(f"Glue job started with run ID: {job_run_id}")

        # Monitor job completion - THIS IS CRITICAL
        max_wait_time = 1800  # 30 minutes
        wait_interval = 30    # 30 seconds
        elapsed_time = 0

        while elapsed_time < max_wait_time:
            job_run = glue_client.get_job_run(JobName=GLUE_JOB_NAME, RunId=job_run_id)
            job_state = job_run['JobRun']['JobRunState']

            print(f"Job state: {job_state} (elapsed: {elapsed_time}s)")

            if job_state == 'SUCCEEDED':
                print("Glue job completed successfully")
                return {"job_run_id": job_run_id, "status": "success", "state": job_state}
            elif job_state in ['FAILED', 'STOPPED', 'TIMEOUT']:
                error_message = job_run['JobRun'].get('ErrorMessage', 'No error message available')
                raise Exception(f"Glue job failed: {job_state} - {error_message}")

            time.sleep(wait_interval)
            elapsed_time += wait_interval

        # If we reach here, the job timed out
        raise Exception(f"Glue job timed out after {max_wait_time} seconds")

    def create_redshift_serverless_func():
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        redshift_serverless_client = session.client('redshift-serverless')

        # Create namespace
        namespace_config = {
            'namespaceName': REDSHIFT_NAMESPACE,
            'adminUsername': 'admin',
            'adminUserPassword': 'TempPassword123!',
            'dbName': REDSHIFT_DATABASE,
            'defaultIamRoleArn': 'arn:aws:iam::207567758295:role/RedshiftServerlessRole',
            'iamRoles': ['arn:aws:iam::207567758295:role/RedshiftServerlessRole']
        }
        print(f"Creating Redshift Serverless namespace: {REDSHIFT_NAMESPACE}")
        redshift_serverless_client.create_namespace(**namespace_config)

        # Wait for namespace to be available
        max_wait_time = 600  # 10 minutes
        wait_interval = 30
        elapsed_time = 0

        while elapsed_time < max_wait_time:
            namespace_response = redshift_serverless_client.get_namespace(namespaceName=REDSHIFT_NAMESPACE)
            status = namespace_response['namespace']['status']
            print(f"Namespace status: {status} (elapsed: {elapsed_time}s)")

            if status == 'AVAILABLE':
                break
            elif status in ['DELETING', 'MODIFYING']:
                time.sleep(wait_interval)
                elapsed_time += wait_interval
            else:
                time.sleep(wait_interval)
                elapsed_time += wait_interval

        # Create workgroup
        workgroup_config = {
            'workgroupName': REDSHIFT_WORKGROUP,
            'namespaceName': REDSHIFT_NAMESPACE,
            'baseCapacity': 32,
            'enhancedVpcRouting': False,
            'publiclyAccessible': True
        }

        print(f"Creating Redshift Serverless workgroup: {REDSHIFT_WORKGROUP}")
        redshift_serverless_client.create_workgroup(**workgroup_config)

        # Wait for workgroup to be available
        elapsed_time = 0
        while elapsed_time < max_wait_time:
            workgroup_response = redshift_serverless_client.get_workgroup(workgroupName=REDSHIFT_WORKGROUP)
            status = workgroup_response['workgroup']['status']
            print(f"Workgroup status: {status} (elapsed: {elapsed_time}s)")

            if status == 'AVAILABLE':
                break
            else:
                time.sleep(wait_interval)
                elapsed_time += wait_interval

        print("Redshift Serverless cluster created successfully")
        return {"namespace": REDSHIFT_NAMESPACE, "workgroup": REDSHIFT_WORKGROUP}

    def create_redshift_schema_func():
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        redshift_data_client = session.client('redshift-data')

        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {REDSHIFT_SCHEMA};"

        print(f"Creating schema: {REDSHIFT_SCHEMA}")
        response = redshift_data_client.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP,
            Database=REDSHIFT_DATABASE,
            Sql=create_schema_sql
        )

        query_id = response['Id']
        print(f"Schema creation query started: {query_id}")

        # Wait for query completion
        max_wait_time = 300  # 5 minutes
        wait_interval = 10
        elapsed_time = 0

        while elapsed_time < max_wait_time:
            status_response = redshift_data_client.describe_statement(Id=query_id)
            status = status_response['Status']
            print(f"Schema creation status: {status} (elapsed: {elapsed_time}s)")

            if status == 'FINISHED':
                print(f"Schema {REDSHIFT_SCHEMA} created successfully")
                return {"query_id": query_id, "status": "success", "schema": REDSHIFT_SCHEMA}
            elif status in ['FAILED', 'ABORTED']:
                error = status_response.get('Error', 'Unknown error')
                raise Exception(f"Schema creation failed: {error}")

            time.sleep(wait_interval)
            elapsed_time += wait_interval

        raise Exception("Schema creation timed out")

    def create_redshift_table_func():
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        redshift_data_client = session.client('redshift-data')

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE} (
            artists VARCHAR(1000),
            album_name VARCHAR(1000),
            track_name VARCHAR(1000),
            popularity INTEGER,
            duration_ms BIGINT,
            loudness DECIMAL(10,6),
            danceability DECIMAL(10,6),
            track_genre VARCHAR(200),
            tempo DECIMAL(10,6)
        );
        """

        print(f"Creating table {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE}")
        response = redshift_data_client.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP,
            Database=REDSHIFT_DATABASE,
            Sql=create_table_sql
        )

        query_id = response['Id']
        print(f"Table creation query started: {query_id}")

        # Wait for query completion
        max_wait_time = 300  # 5 minutes
        wait_interval = 10
        elapsed_time = 0

        while elapsed_time < max_wait_time:
            status_response = redshift_data_client.describe_statement(Id=query_id)
            status = status_response['Status']
            print(f"Query status: {status} (elapsed: {elapsed_time}s)")

            if status == 'FINISHED':
                print("Table created successfully")
                return {"query_id": query_id, "status": "success"}
            elif status in ['FAILED', 'ABORTED']:
                error = status_response.get('Error', 'Unknown error')
                raise Exception(f"Table creation failed: {error}")

            time.sleep(wait_interval)
            elapsed_time += wait_interval

        raise Exception("Table creation timed out")

    def load_data_to_redshift_func():
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        redshift_data_client = session.client('redshift-data')

        # COPY command to load data from S3 to Redshift
        copy_sql = f"""
        COPY {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE} 
        (artists,album_name,track_name,popularity,duration_ms,track_genre)
        FROM 's3://{S3_BUCKET_GOLD}/athena-results/'
        IAM_ROLE 'arn:aws:iam::207567758295:role/RedshiftServerlessRole'
        FORMAT AS PARQUET;
        """
        #loudness,danceability,,tempo
        print(f"Loading data from S3 to Redshift table {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE}")
        response = redshift_data_client.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP,
            Database=REDSHIFT_DATABASE,
            Sql=copy_sql
        )

        query_id = response['Id']
        print(f"Data load query started: {query_id}")

        # Wait for query completion
        max_wait_time = 1800  # 30 minutes
        wait_interval = 30
        elapsed_time = 0

        while elapsed_time < max_wait_time:
            status_response = redshift_data_client.describe_statement(Id=query_id)
            status = status_response['Status']
            print(f"Data load status: {status} (elapsed: {elapsed_time}s)")

            if status == 'FINISHED':
                # Get row count
                count_response = redshift_data_client.execute_statement(
                    WorkgroupName=REDSHIFT_WORKGROUP,
                    Database=REDSHIFT_DATABASE,
                    Sql=f"SELECT COUNT(*) FROM {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE};"
                )
                print(f"Data loaded successfully. Query ID: {query_id}")
                return {"query_id": query_id, "status": "success"}
            elif status in ['FAILED', 'ABORTED']:
                error = status_response.get('Error', 'Unknown error')
                raise Exception(f"Data load failed: {error}")

            time.sleep(wait_interval)
            elapsed_time += wait_interval

        raise Exception("Data load timed out")

    def delete_redshift_serverless_func():
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        redshift_serverless_client = session.client('redshift-serverless')

        print(f"Deleting Redshift Serverless workgroup: {REDSHIFT_WORKGROUP}")
        redshift_serverless_client.delete_workgroup(workgroupName=REDSHIFT_WORKGROUP)

        # Wait for workgroup deletion
        max_wait_time = 600  # 10 minutes
        wait_interval = 30
        elapsed_time = 0

        while elapsed_time < max_wait_time:
            workgroups = redshift_serverless_client.list_workgroups()
            workgroup_names = [wg['workgroupName'] for wg in workgroups['workgroups']]

            if REDSHIFT_WORKGROUP not in workgroup_names:
                print("Workgroup deleted successfully")
                break

            time.sleep(wait_interval)
            elapsed_time += wait_interval

        print(f"Deleting Redshift Serverless namespace: {REDSHIFT_NAMESPACE}")
        redshift_serverless_client.delete_namespace(namespaceName=REDSHIFT_NAMESPACE)

        print("Redshift Serverless resources deleted successfully")
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
from awsglue.dynamicframe import DynamicFrame

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
    )

    # Create Bronze and Silver Crawlers using the function
    create_bronze_crawler = make_glue_crawler_operator(
        task_id='create_bronze_crawler',
        crawler_name=CRAWLER_NAME,
        role_arn='arn:aws:iam::207567758295:role/GlueServiceRole',
        database_name=DATABASE_NAME,
        s3_path=f's3://{S3_BUCKET_BRONZE}/'
    )

    create_silver_crawler = make_glue_crawler_operator(
        task_id='create_silver_crawler',
        crawler_name=CRAWLER_NAME_SILVER,
        role_arn='arn:aws:iam::207567758295:role/GlueServiceRole',
        database_name=DATABASE_NAME_SILVER,
        s3_path=f's3://{S3_BUCKET_SILVER}/'
    )
    # artists, album_name, track_name, popularity, duration_ms, danceability, loudness, danceability, track_genre, tempo
    def athena_query():
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', region_name=AWS_REGION)
        session = aws_hook.get_session()
        athena_client = session.client('athena', region_name=AWS_REGION)
        query = f"""
        UNLOAD (
            SELECT artists, album_name, track_name, popularity, duration_ms, track_genre FROM {S3_BUCKET_SILVER}
        )
        TO 's3://{S3_BUCKET_GOLD}/athena-results/'
        WITH (format = 'PARQUET', compression = 'snappy')
        """

        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': DATABASE_NAME_SILVER
            },
            ResultConfiguration={
                'OutputLocation': f's3://{S3_BUCKET_GOLD}/athena-query-results/'
            }
        )

        query_execution_id = response['QueryExecutionId']
        print(f"Query started: {query_execution_id}")
    # Create Python Operators
    create_kinesis_resources = PythonOperator(
        task_id='create_kinesis_resources',
        python_callable=create_kinesis_resources_func,
    )
    stream_localhost_data = PythonOperator(
        task_id='stream_localhost_data',
        python_callable=stream_localhost_data_func,
    )
    delete_kinesis_resources = PythonOperator(
        task_id='delete_kinesis_resources',
        python_callable=delete_kinesis_resources_func,
    )
    # Create delete crawler operators using the function
    delete_crawler = PythonOperator(
        task_id='delete_crawler',
        python_callable=make_delete_crawler_func(CRAWLER_NAME),
    )
    delete_crawler_silver = PythonOperator(
        task_id='delete_crawler_silver',
        python_callable=make_delete_crawler_func(CRAWLER_NAME_SILVER),
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
    athena_query = PythonOperator(
        task_id='athena_query',
        python_callable=athena_query,
    )
    create_redshift_serverless_func = PythonOperator(
        task_id='create_redshift_serverless_func',
        python_callable=create_redshift_serverless_func,
    )
    create_redshift_schema = PythonOperator(
        task_id='create_redshift_schema',
        python_callable=create_redshift_schema_func,
    )
    create_redshift_table = PythonOperator(
        task_id='create_redshift_table',
        python_callable=create_redshift_table_func,
    )
    load_data_to_redshift = PythonOperator(
        task_id='load_data_to_redshift',
        python_callable=load_data_to_redshift_func,
    )
    delete_redshift_serverless = PythonOperator(
        task_id='delete_redshift_serverless',
        python_callable=delete_redshift_serverless_func,
    )
    # Task Dependencies
    create_bronze_bucket >> create_kinesis_resources >> stream_localhost_data >> delete_kinesis_resources >> create_bronze_crawler >> create_silver_bucket >> delete_crawler >> upload_glue_script >> create_glue_job >> run_glue_job >> create_gold_bucket >> delete_glue_job >> create_silver_crawler >> delete_crawler_silver >> athena_query >> create_redshift_serverless_func >> create_redshift_schema >> create_redshift_table >> load_data_to_redshift >> delete_redshift_serverless