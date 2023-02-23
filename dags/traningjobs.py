from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import datetime as dt
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import XCom
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import time
from airflow.operators.bash_operator import BashOperator
from flask import Flask, request,jsonify
import csv
from datetime import datetime
from queue import Queue
import threading
from airflow.operators.python_operator import ShortCircuitOperator
import json
import os

app = Flask(__name__)
request_queue = Queue()
lock = threading.Lock()

QUEUE_FILE = os.path.join(os.path.dirname(__file__), 'queue.json')
# Verificar si el archivo de cola existe, crearlo si no
if not os.path.isfile(QUEUE_FILE):
    with open(QUEUE_FILE, 'w') as f:
        f.write("[]")
        f.close()
        
def procesar_cola(ti):
    # Obtenemos la cola ordenada de XCom
    queue_contents = ti.xcom_pull(key='queue_contents', task_ids=['read_queue_task'])
    print(f"Cola de solicitudes: {queue_contents}")        
    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}


def read_queue(ti):
    with lock:
        # Leer el archivo JSON donde se guarda la cola
        with open(QUEUE_FILE, 'r') as f:
            queue_list = json.load(f)

        # Ordenar la cola por fecha
        queue_list_sorted = sorted(queue_list, key=lambda x: x['date'])

        # Guardar la cola ordenada en un XCom
       
        ti.xcom_push(key='queue_contents', value=queue_list_sorted)

        print(f"Cola de solicitudes: {queue_list_sorted}")
     
        
        
with DAG('read_queue', default_args=default_args, schedule_interval=timedelta(seconds=180)) as dag:
    read_queue_task = PythonOperator(
        task_id='read_queue_task',
        python_callable=read_queue,
        dag=dag
    )
    
        # Definir la tarea que procesa los Xcom
    procesar_cola_task = PythonOperator(
        task_id='procesar_cola',
        python_callable=procesar_cola,
        provide_context=True
    )
    
    read_queue_task>>procesar_cola_task 
       


def write_queue(queue_list):
    with open(QUEUE_FILE, 'w') as f:
        json.dump(queue_list, f)
        f.close()
            
            
@app.route('/enqueue_request', methods=['POST'])
def enqueue_request_endpoint():
    request_data = request.json
    with lock:
        if request_queue.qsize() < 10:
            request_queue.put(request_data)
        else:
            # Eliminar el elemento más antiguo de la cola para hacer espacio
            request_queue.get()
        # Creamos una lista con todos los elementos de la cola
        queue_list = list(request_queue.queue)
        # Guardar la cola en el archivo
        write_queue(queue_list)
    response = {'status': 'success', 'message': 'Request encolada correctamente', 'queue': queue_list}
    return jsonify(response), 200

class StartFlaskServerOperator(BaseOperator):
    def execute(self, context):
        self.log.info("Iniciando el servidor Flask...")
        # Iniciamos el servidor de Flask
        app.run(host='0.0.0.0', port=5098)
        self.log.info("Servidor de Flask iniciado.")
        return 'Servidor de Flask iniciado correctamente.'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime.today()
}

with DAG('flask_example', default_args=default_args, schedule_interval=None) as dag:
    # Usamos nuestro operador personalizado para iniciar el servidor de Flask
    start_flask_server = StartFlaskServerOperator(
        task_id='start_flask_server',
        dag=dag,
    )
    
 
    
    # Definimos la dependencia entre tareas con el operador de tubería >>
    # 
start_flask_server

