
from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue

API_URL = "https://raw.githubusercontent.com/FatimaMoqran/airflow-datasets/refs/heads/main/fakeuser.json"


@dag(
    dag_id="user_processing",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["user_processing", "user"],
)
def user_processing():

    # 1. Création de la table PostgreSQL
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

    # 2. Vérification de la disponibilité de l'API
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

    # 3. Extraction des informations utilisateur
    @task
    def extract_user(fake_user):
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
        }

    # 4. Sauvegarde des informations dans un CSV
    @task
    def process_user(user_info):
        import csv
        from datetime import datetime

        # Simulation temporaire pour le test
       # user_info = {
        #    "id": 7,
        #    "firstname": "Cristiano",
        #    "lastname": "Ronaldo",
        #    "email": "CR7@Stark.com"
        #}

        user_info["created_at"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        with open("/tmp/user_info.csv", "w", newline="") as fichier:
            writer = csv.DictWriter(
                fichier,
                fieldnames=[
                    "id",
                    "firstname",
                    "lastname",
                    "email",
                    "created_at"
                ]
            )

            writer.writeheader()
            writer.writerow(user_info)

        return user_info
    
    @task
    def store_user():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="postgres")

        hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER",
            filename="/tmp/user_info.csv",
        )


    # 5. Dépendances entre les tâches
    fake_user = is_api_available()
    user_info = extract_user(fake_user)
    processed_user = process_user(user_info)
    stored_user = store_user()

    create_table >> fake_user
    processed_user >> stored_user



user_processing()
