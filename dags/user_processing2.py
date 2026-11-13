
from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.standard.operators.python import PythonOperator

API_URL = "https://raw.githubusercontent.com/FatimaMoqran/airflow-datasets/refs/heads/main/fakeuser.json"

@dag(
    dag_id="user_processing2",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["user_processing2", "user"],
)
def user_processing2():

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        import requests

        response = requests.get(API_URL, timeout=10)
        print(response.status_code)

        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None

        return PokeReturnValue(
            is_done=condition,
            xcom_value=fake_user
        )
        

    def _extract_user(ti):
        fake_user = ti.xcom_pull(task_ids="is_api_available")
        #import requests
        #fake_user = requests.get(API_URL, timeout=10).json()


        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
        }

    extract_user = PythonOperator(
        task_id="extract_user",
        python_callable=_extract_user,
    )

    create_table >> is_api_available() >> extract_user

user_processing2()