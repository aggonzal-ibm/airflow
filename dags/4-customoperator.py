from airflow import DAG
from datetime import datetime
from hellooperator import HelloOperator

with DAG(dag_id = "customoperator",
         description = "Utilizando el operador CustomOperator",
         start_date = datetime(2023, 2, 16)
) as dag:
    t1 = HelloOperator(task_id = "hello_with_customoperator", name = "Andy")
    

