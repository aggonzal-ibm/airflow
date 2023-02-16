from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(dag_id = "orquestacion",
         description="Orquestacion de tareas",
         schedule_interval = "@daily",
         start_date = datetime(2023, 2, 1),
         end_date = datetime(2023, 2, 5),
         default_args ={"depends_on_past": True}, max_active_runs =1 ) as dag:
    
    t1 = BashOperator(
    task_id='tarea1',
    bash_command='sleep 1 && echo "tarea1"')
    
    t2 = BashOperator(
    task_id='tarea2',
    bash_command='sleep 1 && echo "tarea2"')
    
    t3 = BashOperator(
    task_id='tarea3',
    bash_command='sleep 1 && echo "tarea3"')
    
    t4 = BashOperator(
    task_id='tarea4',
    bash_command='sleep 1 && echo "tarea4"')
    
    
    
    
    
    t1 >> t2 >> [t3,t4]