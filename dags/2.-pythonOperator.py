from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

def print_hello():
    print("************PRobando el operador PythonOperator************")


with DAG(dag_id = "pythonoperator",
         description = "Utilizando el operador PythonOperator",
         start_date = datetime(2023, 2, 15),
         schedule_interval = "@once",
) as dag:
    t1 = PythonOperator(task_id = "hello_with_python",
                        python_callable = print_hello)