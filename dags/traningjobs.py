from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from api import app
from airflow.operators.bash_operator import BashOperator
from flask import Flask, request
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

class StartFlaskServerOperator(BaseOperator):
    def execute(self, context):
        self.log.info("Iniciando el servidor Flask...")
        # Iniciamos el servidor de Flask
        app.run(host='0.0.0.0', port=5000)
        self.log.info("Servidor de Flask iniciado.")
        return 'Servidor de Flask iniciado correctamente.'
    
class PrintProjectIdOperator(BaseOperator):
    def execute(self, context):
        # Obtenemos el valor del parámetro projectID de la solicitud POST
        project_id = request.form.get('projectID')
        # Imprimimos el valor del parámetro projectID en la consola de Airflow
        print(f'El valor del parámetro projectID es: {project_id}')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    'flask_example',
    default_args=default_args,
    schedule_interval=None,
)

# Usamos nuestro operador personalizado para iniciar el servidor de Flask
start_flask_server = StartFlaskServerOperator(
    task_id='start_flask_server',
    dag=dag,
)




# Definimos la dependencia entre tareas con el operador de tubería >>
start_flask_server 

