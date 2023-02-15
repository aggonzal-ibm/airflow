from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

with DAG(dag_id = "bashoperator",
         description = "Utilizando el operador BashOperator",
         start_date = datetime(2023, 2, 15),
) as dag:
    t1 = BashOperator(task_id = "hello_with_bash",
                      bash_command = "echo 'Hello World!'")