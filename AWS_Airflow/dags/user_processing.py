import pendulum
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator
@dag(schedule=None, start_date=pendulum.datetime(2025, 1, 1,tz='UTC'), catchup=False)
def user_processing():
    def print_hello():
        print("Hello from the print hello!")
    def print_goodbye():
        print("Goodby!")
    #Create Hell0 task
    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=print_hello,
    )
    goodbye_task = PythonOperator(
        task_id="goodbye_task",
        python_callable=print_goodbye,
    )
    #set Dependencies
    return hello_task >> goodbye_task
simpledag = user_processing()