from http.client import responses
import requests
from airflow.sdk import dag,task
import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
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
    @task.sensor(poke_interval=30,timeout=300)
    def is_api_available(): -> PokeReturnValue:
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(response.status_code)
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        return PokeReturnValue(is_done=condition, com_value=fake_user)


