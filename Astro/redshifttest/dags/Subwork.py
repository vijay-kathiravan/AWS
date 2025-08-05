from pickle import FALSE
import kagglehub
from airflow.sdk import DAG, task
import os
from flask import Flask, Response
import pandas as pd
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
import shutil
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
    default_args=default_args, schedule= None, catchup= FALSE
) as dag:
    def hosting_csv_data():
        """
        This function is here to host the given CSV file into a API based streaming which will
        be fed to Kinesis data stream
        :return: None
        """
        # Download the dataset
        path = kagglehub.dataset_download("maharshipandya/-spotify-tracks-dataset")
        print("Path to dataset files:", path)

        # List files in the downloaded dataset directory
        files = os.listdir(path)
        print("Files in dataset:", files)

        # Copy each CSV file to the current working directory
        for file in files:
            if file.endswith('.csv'):
                src = os.path.join(path, file)
                dst = os.path.join(os.getcwd(), file)
                shutil.copy(src, dst)
                print(f"Copied {file} to {dst}")
    hosting_csv_data()
    # def hosting_kinesis_data():
    #
    #     app = Flask("__main__")
    #
    #     @app.route('/spotify-data')
    #     def get_csv():
    #         df = pd.read_csv("dataset.csv")
    #         return Response(df.to_csv(index=False), mimetype='text/csv')
    #
    #     if __name__ == '__main__':
    #         app.run(host='0.0.0.0', port=8080)
    #         hosting_csv_data()
    # hosting_kinesis_data()