import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag

@dag(schedule=None, start_date=pendulum.datetime(2025, 1, 1,tz='UTC'), catchup=False)
def create_table():
    "Table Creation"
    #Task - 1
    create_table= SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS hammered (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
    )
    #SET DEPENDENCIES
    return create_table
sql_dag = create_table()