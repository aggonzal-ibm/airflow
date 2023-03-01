import requests

class RequestTaskStatus:
    @staticmethod
    def get_task_status(dag_id, task_id, execution_date, airflow_host='localhost', airflow_port=8080):
        endpoint = f"http://{airflow_host}:{airflow_port}/api/v1/dags/{dag_id}/dagRuns/{execution_date}/taskInstances/{task_id}"
        response = requests.get(endpoint, auth=('airflow', 'airflow'))
        print(response)
        if response.status_code == 200:
            json_data = response.json()
            state = json_data["state"]
            return state
        else:
            return None


