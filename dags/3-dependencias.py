from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def print_hello():
    print("************PRobando el operador PythonOperator************")


with DAG(dag_id = "dependencias",
         description = "Creando dependencias entre tareas",
         start_date = datetime(2023, 2, 15),
         schedule_interval = "@once",
) as dag:
    t1 = PythonOperator(task_id = "tarea1",
                        python_callable = print_hello)
    
    t2 = BashOperator(task_id = "tarea2",
                      bash_command = "echo ' tarea2!'")
    
    t3 = BashOperator(task_id = "tarea3",
                      bash_command = "echo ' tarea3!'")
    
    t4 = BashOperator(task_id = "tarea4",
                      bash_command = "echo ' tarea4!'")
    
    t1 >> t2 >> [t3,t4]