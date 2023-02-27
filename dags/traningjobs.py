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
from airflow.models import TaskInstance
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState
from airflow.operators.dummy_operator import DummyOperator
from airflow.api.common.experimental.get_task_instance import get_task_instance

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
    print(f"Cola de solicitudes en Xcom: {queue_contents}")        


    
def training(ti):
    # Obtenemos la cola ordenada de XCom
    queue_contents = ti.xcom_pull(key='queue_contents', task_ids=['read_queue_task'])
    print(f"Cola de solicitudes Traning: {queue_contents}")      
    
    if isinstance(queue_contents, str):
        queue_contents = json.loads(queue_contents)
        print(f"Cola de solicitudes Training: {queue_contents}")

# Filtrar las solicitudes pendientes de entrenamiento
    queue = [elem for elem in queue_contents[0] if elem["Status"] == "I"]
    if not queue:
        print("No hay solicitudes pendientes de entrenamiento")
        return
    
    
    
    
    
    # Tomar el primer elemento de queue_contents y marcarlo como en proceso
    queue_elem = queue[0]
    queue_elem["Status"] = "P"
    print("Antes de escribir",queue_elem)
    update_queue(queue_elem)

    print(f"Inicia entrenamiento de {queue_elem['projectID']} con fecha {queue_elem['date']}")
    
  
    try_number = 1
    dag_id = 'process_jobs'
    task_id = 'training'        
    execution_date = datetime(2023,2,27)

    estado = get_task_status(dag_id, task_id) 
    print("Se inicia Entrenamiento-----> Estado Actual: ",estado)
   
    time.sleep(360)
    


    # Marcar el elemento como finalizado
    queue_elem["Status"] = "F"
    update_queue(queue_elem)

    print(f"Finaliza entrenamiento de {queue_elem['projectID']} con fecha {queue_elem['date']}")
    
    
    estado = get_task_status(dag_id, task_id) 
    print("Despues",estado)
   
    print ("Finaliza entrenamiento ")
 
    
def get_task_status(dag_id, task_id):
    """ Returns the status of the last dag run for the given dag_id
    1. The code is very similar to the above function, I use it as the foundation for many similar problems/solutions
    2. The key difference is that in the return statement, we can directly access the .get_task_instance passing our desired task_id and its state


    Args:
        dag_id (str): The dag_id to check
        task_id (str): The task_id to check
    Returns:
        List - The status of the last dag run for the given dag_id
    """
    last_dag_run = DagRun.find(dag_id=dag_id)
    last_dag_run.sort(key=lambda x: x.execution_date, reverse=True)
    return last_dag_run[0].get_task_instance(task_id).state
    
    
    
    
    
    
    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime.today(),
    'retry_delay': timedelta(seconds=10)
    
}


def read_queue(ti):
  
    # Leer el archivo JSON donde se guarda la cola
    with open(QUEUE_FILE, 'r') as f:
        queue_list = json.load(f)
    print(f"Cola de solicitudes ReadQueue2: {queue_list}")
    # Ordenar la cola por fecha
    #queue_list= sorted(queue_dict.values(), key=lambda x: datetime.strptime(x['date'], '%m/%d/%Y:%H:%M'), reverse=True)

    # Guardar la cola ordenada en un XCom

    ti.xcom_push(key='queue_contents', value=queue_list)

    print(f"Cola de solicitudes: {queue_list}")
     
        
        
"""  Original DAG
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
    
    read_queue_task>>procesar_cola_task """
    

with DAG('process_jobs', default_args=default_args, schedule_interval=timedelta(seconds=60)) as dag:
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
    
    
    training_task = PythonOperator(
        task_id='training',
        python_callable=training,
        provide_context=True
    )
    
   
    
    
    
    read_queue_task>>procesar_cola_task >>training_task    
       


"""def write_queue(queue_list):
    with open(QUEUE_FILE, 'w') as f:
        json.dump(queue_list, f)
        f.close()"""
def write_queue(queue_list):
    with open(QUEUE_FILE, 'r+') as f:
        # Cargamos la cola actual desde el archivo JSON
        try:
            content = json.load(f)
        except json.decoder.JSONDecodeError:
            # Si el archivo está vacío, escribimos una lista vacía en el archivo
            f.write("[]")
            f.seek(0)
            content = json.load(f)
        
        if isinstance(content, list):
            # Añadimos el último elemento de la nueva cola a la cola existente
            content.append(queue_list)
        else:
            # Si el archivo JSON está vacío, creamos una nueva lista con el primer elemento
            content = [queue_list]
            
        # Nos ubicamos al inicio del archivo y escribimos la nueva cola ordenada por fecha
        f.seek(0)
        f.truncate()
        json.dump(content, f, indent=4)
        
def update_queue(updated_elem):
    with open(QUEUE_FILE, 'r+') as f:
        # Cargamos la cola actual desde el archivo JSON
        content = json.load(f)

        # Buscamos el elemento que queremos actualizar
        for i, elem in enumerate(content):
            if elem['projectID'] == updated_elem['projectID']:
                # Actualizamos el elemento encontrado con el nuevo estado
                content[i] = updated_elem
                break

        # Nos ubicamos al inicio del archivo y escribimos la nueva cola ordenada por fecha
        f.seek(0)
        f.truncate()
        json.dump(content, f, indent=4)
            
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

with DAG('server-up', default_args=default_args, schedule_interval=None) as dag:
    # Usamos nuestro operador personalizado para iniciar el servidor de Flask
    start_flask_server = StartFlaskServerOperator(
        task_id='start_flask_server',
        dag=dag,
    )
    
 
    
    # Definimos la dependencia entre tareas con el operador de tubería >>
    # 
