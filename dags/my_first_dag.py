from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

def print_hello():
    print("What's up Airflow!")


def print_goodbye():
    print('GoodBye!')

with DAG(
    'sample_dag',
    description='A simple DAG',
    schedule_interval='0 0 * * *',
    start_date=datetime(2025, 2, 2),
    catchup=False,
) as dag:

    task1 = PythonOperator(task_id='print_hello_task',
                           python_callable=print_hello,
                           dag=dag)

    task2 = PythonOperator(task_id='print_goodbye_task',
                           python_callable=print_goodbye,
                           dag=dag)

task1 >> task2