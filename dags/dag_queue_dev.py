from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
import json
import os
import time
from datetime import datetime
import requests
import subprocess

start_date = datetime.today()

QUEUE_FILE = os.path.join(os.path.dirname(__file__), 'queue.json')

dag_id = 'process_trainning'
url = 'http://localhost:8080/api/experimental/dags/{}/dag_runs'.format(dag_id)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 0,
    'catchup':False
}

dag = DAG(
    'process_trainning',
    default_args=default_args,
    description='Process JSON queue file',
    schedule_interval=timedelta(seconds=10),
    max_active_runs=1
)

def read_json_file(ti):
    
    with open(QUEUE_FILE) as file:
        data = json.load(file)
        print("Elementos en el archivo JSON:", data)
        # Get items with status 'I' and sort by date in ascending order
        items = sorted(filter(lambda x: x['Status'] == 'I', data), key=lambda x: x['date'])
        print("Objetos en lista:", items)
        if len(items) > 0:
            # Save the first item in XCom for downstream tasks
            ti.xcom_push(key='json_item', value=items[0])
        else:
            print('No items with status "I" found in JSON file')


def get_dag_runs(dag_id):
    url = 'http://172.18.0.5:8080/api/experimental/dags/{}/dag_runs'.format(dag_id)
    cmd = f"curl -X GET {url}"
    result = subprocess.run(cmd.split(), capture_output=True)
    if result.returncode == 0:
        dag_runs = json.loads(result.stdout)
        for run in dag_runs:
            if run['state'] == 'running':
                return run['execution_date']
        return dag_runs
    else:
        print(result.stderr)
        return None

def get_task_status(dag_id, task_id):
    execution_date = get_dag_runs(dag_id)
    url = f'http://172.18.0.5:8080/api/experimental/dags/{dag_id}/dag_runs/{execution_date}/tasks/{task_id}'
    response = requests.get(url)
    if response.status_code == 200:
        task_status = response.json()['state']
        return f'Task {task_id} status is {task_status}'
    else:
        return 'Error getting task status'


        
def process_json_item(ti):
    # Retrieve the JSON item saved in XCom
    #colocar validacion del estado en running
    
    status = get_task_status('process_trainning', 'process_json_item_task')  
    print("El estado actual de la tarea de entrenamiento es: ",  status)
    while status == 'running':
        print('DAG is running. Waiting...')
        time.sleep(60) # Espera 1 minuto antes de volver a verificar     
    
    item = ti.xcom_pull(key='json_item', task_ids='read_json_file_task')
    if item:
        print(f'Processing item with projectID "{item["projectID"]}"...')
        # Update the item status to 'P'
        item['Status'] = 'P'
        file_path = QUEUE_FILE
        with open(file_path) as file:
            data = json.load(file)
            # Find the item to delete and remove it
            for i in range(len(data)):
                if data[i]['projectID'] == item['projectID']:
                    del data[i]
                    break
            else:
                print(f'Item with projectID "{item["projectID"]}" not found in JSON file')
                return
        # Rewrite the file without the deleted item
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        # Wait for 30 seconds
        print('Traning for 30 seconds...')
       
        time.sleep(30)
        print(f'Item with projectID "{item["projectID"]}" processed successfully')
    else:
        print('No items with status "I" found in JSON file')      

read_json_file_task = PythonOperator(
    task_id='read_json_file_task',
    python_callable=read_json_file,
    provide_context=True,
    dag=dag,
)

process_json_item_task = PythonOperator(
    task_id='process_json_item_task',
    python_callable=process_json_item,
    provide_context=True,
    dag=dag,
)

read_json_file_task >> process_json_item_task