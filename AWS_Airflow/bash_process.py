import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator


@dag(schedule=None, start_date=pendulum.datetime(2025, 1, 1,tz='UTC'), catchup=False)
def bash_dag():
    bashtask = BashOperator(
        task_id='bash_Task',
        bash_command='echo "Hello World!"',
    )
    list_file = BashOperator(
        task_id='list_file',
        bash_command='ls -l',
    )
    #Set Dependencies
    return bashtask >> list_file
bash_dag = bash_dag()