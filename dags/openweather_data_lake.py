from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import subprocess
import os

# ===================================================================
#           Fonctions des modules du pipeline
# ===================================================================

def ensure_buckets_exist():
    """
    Vérifie que les buckets nécessaires existent, et les crée si besoin.
    """
    endpoint_url = "http://localstack-data-lake-project:4566"  # Remplacez par l'URL correcte si nécessaire
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="test",
        aws_secret_access_key="test"
    )
    required_buckets = ["raw", "staging", "curated"]
    existing_buckets = [bucket['Name'] for bucket in s3_client.list_buckets().get('Buckets', [])]

    for bucket in required_buckets:
        if bucket not in existing_buckets:
            s3_client.create_bucket(Bucket=bucket)
            print(f"[INFO] Bucket '{bucket}' créé.")
        else:
            print(f"[INFO] Bucket '{bucket}' existe déjà.")

def run_data_recovery():
    """
    Exécute le script data-recovery.py pour collecter les données
    depuis les APIs et les stocker dans le bucket RAW.
    """
    script_path = "/opt/airflow/src/data-recovery.py"
    subprocess.run(["python", script_path], check=True)

def run_data_preprocessing():
    """
    Exécute le script data-preprocessing.py pour traiter les données
    dans le bucket RAW et les pousser vers le bucket STAGING.
    """
    script_path = "/opt/airflow/src/data-preprocessing.py"
    subprocess.run(["python", script_path], check=True)

def run_data_classification():
    """
    Exécute le script data-classification.py pour classifier les données
    dans le bucket STAGING et les transférer vers le bucket CURATED.
    """
    script_path = "/opt/airflow/src/data-classification.py"
    subprocess.run(["python", script_path], check=True)

# ===================================================================
#           Définition du DAG Openweather Data Lake
# ===================================================================

with DAG(
    "openweather_data_lake",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    description="Pipeline to process OpenWeather data into a data lake (RAW -> STAGING -> CURATED)",
    schedule_interval="0 */6 * * *",  # Planification quotidienne
    start_date=datetime(2024, 1, 1),  # Date de début
    catchup=False,  # Ne pas rattraper les exécutions manquées
) as dag:

    # Tâche 0 : Initialiser les buckets (RAW, STAGING, CURATED)
    task_initialize_buckets = PythonOperator(
        task_id="initialize_buckets",
        python_callable=ensure_buckets_exist,
    )

    # Tâche 1 : Collecte des données dans le bucket RAW
    task_data_to_raw = PythonOperator(
        task_id="data_to_raw",
        python_callable=run_data_recovery,
    )

    # Tâche 2 : Prétraitement des données et transfert vers STAGING
    task_raw_to_staging = PythonOperator(
        task_id="raw_to_staging",
        python_callable=run_data_preprocessing,
    )

    # Tâche 3 : Classification des données et transfert vers CURATED
    task_staging_to_curated = PythonOperator(
        task_id="staging_to_curated",
        python_callable=run_data_classification,
    )

    # Dépendances entre les tâches
    task_initialize_buckets >> task_data_to_raw >> task_raw_to_staging >> task_staging_to_curated