from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

with DAG(dag_id="redshift", start_date=datetime(2021, 1, 1), schedule=None) as dag:
    create_tmp_table_data_api = RedshiftDataOperator(
        task_id="create_tmp_table_data_api",
        postgres_conn_id="redshift",
        cluster_identifier='airflow',
        database='dev',
        db_user='airflow',
        sql=[
            """
            CREATE TEMPORARY TABLE tmp_people (
        id INTEGER,
        first_name VARCHAR(100),
        age INTEGER
        );
            """
        ],
        wait_for_completion=True,
        session_keep_alive_seconds=600,
    )
    create_tmp_table_data_api
#     insert_data_reuse_session = RedshiftDataOperator(
#     task_id="insert_data_reuse_session",
#     sql=[
#         "INSERT INTO tmp_people VALUES ( 1, 'Bob', 30);",
#         "INSERT INTO tmp_people VALUES ( 2, 'Alice', 35);",
#         "INSERT INTO tmp_people VALUES ( 3, 'Charlie', 40);",
#     ],
#     wait_for_completion=True,
#     session_id="{{ task_instance.xcom_pull(task_ids='create_tmp_table_data_api', key='session_id') }}",
# )