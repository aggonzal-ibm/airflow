from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    description='My DAG with two tasks',
    schedule_interval=None,
    catchup=False
)

task1 = BashOperator(
    task_id='task1',
    bash_command='echo "Hello from task 1"',
    dag=dag
)

task2 = BashOperator(
    task_id='task2',
    bash_command='sleep 500 && echo "Hello from task 2"',
    dag=dag
)

task1 >> task2