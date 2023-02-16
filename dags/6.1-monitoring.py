from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def myfunc():
    pass

with DAG(dag_id = "Monitoreo",
         description="Monitoreo de tareas",
         schedule_interval = "@daily",
         start_date = datetime(2022, 1, 1),
         end_date = datetime(2023, 2, 1),
      ) as dag:
    
    t1 = BashOperator(
    task_id='tarea1',
    bash_command='sleep 1 && echo "Primera tarea !"')
    
    t2 = BashOperator(
    task_id='tarea2',
    bash_command='sleep 1 && echo "Segunda tarea "!')
    
    t3 = BashOperator(
    task_id='tarea3',
    bash_command='sleep 1 && echo "Tercera tarea "')
    
    t4 = PythonOperator(
    task_id='tarea4',
    python_callable = myfunc)
    
    
    t5 = BashOperator(
    task_id='tarea5',
    bash_command='sleep 1 && echo "Cuarta tarea "')
    
    t1 >> t2 >> t3 >> t4 >> t5
    
    