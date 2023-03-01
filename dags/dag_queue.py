from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
import json
import os
import time
from datetime import datetime
start_date = datetime.today()

QUEUE_FILE = os.path.join(os.path.dirname(__file__), 'queue.json')
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
    'json_processing',
    default_args=default_args,
    description='Process JSON queue file',
    schedule_interval=timedelta(seconds=10),
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

"""def process_json_item(ti):
    # Retrieve the JSON item saved in XCom
    item = ti.xcom_pull(key='json_item', task_ids='read_json_file_task')
    if item:
        print(f'Processing item with projectID "{item["projectID"]}"...')
        # Update the item status to 'P'
        item['Status'] = 'P'
        file_path = QUEUE_FILE
        print("Item Modificado:", item)
        with open(file_path, 'w') as file:
            json.dump(item, file, indent=4)
        # Wait for 180 seconds
        print('Waiting for 180 seconds...')
        time.sleep(30)
        # Update the item status to 'F'
        item['Status'] = 'F'
        with open(file_path, 'w') as file:
            json.dump(item, file, indent=4)
        print(f'Item with projectID "{item["projectID"]}" processed successfully')
    else:
        print('No items with status "I" found in JSON file')"""
        
def process_json_item(ti):
    # Retrieve the JSON item saved in XCom
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
        print('Waiting for 30 seconds...')
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
