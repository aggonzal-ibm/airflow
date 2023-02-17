from flask import Flask, request,jsonify
import csv
from datetime import datetime

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
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Escribir datos en archivo CSV
    with open('project_data.csv', mode='a', newline='') as csv_file:
        fieldnames = ['projectID', 'date']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        
        # Escribir encabezados solo si el archivo es nuevo
        if csv_file.tell() == 0:
            writer.writeheader()
        
        # Escribir datos en una nueva fila
        writer.writerow({'projectID': project_id, 'date': current_time})
        
    response_data = {'projectID': project_id}
    return jsonify(response_data)