from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from flask import Flask, request, jsonify
from werkzeug.serving import run_simple

app = Flask(__name__)

@app.route('/', methods=['POST'])
def hello_world():
    name = request.form.get('name')
    return f'Hola {name}!'

@app.route('/hello', methods=['GET'])
def hello():
    return jsonify({'message': 'Hola Mundo'})

@app.route('/training', methods=['POST'])
def training():
    data = request.json
    project_id = data['projectID']
    # Aquí podríamos hacer algo con el ID del proyecto, como crear un nuevo proyecto en una base de datos
    response_data = {'projectID': project_id}
    return jsonify(response_data)

def handle_request():
    run_simple('0.0.0.0', 5000, app)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    'flask_example2',
    default_args=default_args,
    schedule_interval=None,
)

# Creamos una tarea PythonOperator que inicia el servidor de Flask
start_flask_server = PythonOperator(
    task_id='start_flask_server',
    python_callable=handle_request,
    dag=dag,
)

# Creamos una tarea BashOperator que espera 30 segundos hasta que el servidor de Flask se inicie
wait = BashOperator(
    task_id='wait',
    bash_command='sleep 30',
    dag=dag,
)

# Creamos una tarea PythonOperator que escucha las solicitudes POST entrantes en el servidor de Flask
listen_requests = PythonOperator(
    task_id='listen_requests',
    python_callable=handle_request,
    dag=dag,
)

# Definimos la dependencia entre tareas con el operador de tubería >>
start_flask_server >> wait >> listen_requests