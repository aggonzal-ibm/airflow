import requests
import json
import time
from request_status_job import RequestTaskStatus


def fake_train():
    # Realiza una solicitud HTTP GET para verificar el estado del servidor
    url = 'https://a389dc65-ce93-458f-af66-b51fc5ce2c97.mock.pstmn.io/training'
    response = requests.post(url)
    if response.status_code == 200:
        print("El servidor está listo para realizar la tarea de entrenamiento")
        print("Entrenamiento iniciado")
        
        # Espera 3 minutos
        for i in range(180):
            time.sleep(1)
            print("Han pasado", i+1, "segundos")
        print("Entrenamiento finalizado")

        # Llama a la función de callback después de que el entrenamiento haya finalizado
        url = 'https://a389dc65-ce93-458f-af66-b51fc5ce2c97.mock.pstmn.io/training'
        data = {'data': 'datos de salida'}
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, data=json.dumps(data), headers=headers)
        print("Respuesta del servidor:", response.json())
    else:
        print("El servidor no está listo para realizar la tarea de entrenamiento")



