from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('airflow',
         default_args=default_args,
         description='DAG for redshift bucket and object operations',
         schedule="@once",  # Correct parameter name
         catchup=False
) as dag:
    create_tmp_table_data_api = RedshiftDataOperator(
        task_id="create_tmp_table_data_api",
        region_name="ca-central-1",
        #cluster_identifier="airflow",
        workgroup_name="airflow",
        database='dev',
       # db_user='airflow',
        sql="""
            CREATE TABLE tmp_world (
                id INTEGER,
                first_name VARCHAR(100),
                age INTEGER
            );""",
        wait_for_completion=True,
        session_keep_alive_seconds=600,
        aws_conn_id="aws",
    )