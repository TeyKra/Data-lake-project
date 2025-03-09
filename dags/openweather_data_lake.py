from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import subprocess
import os

# ===================================================================
#           PIPELINE MODULE FUNCTIONS
# ===================================================================
def ensure_buckets_exist():
    """
    Checks that the required S3 buckets exist, and creates them if necessary.
    
    This function connects to the S3 service (using LocalStack in this case),
    verifies that the buckets "raw", "staging", and "curated" exist, and creates
    any missing buckets.
    """
    endpoint_url = "http://localstack-data-lake-project:4566"  # Adjust the endpoint URL if needed
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
            print(f"[INFO] Bucket '{bucket}' created.")
        else:
            print(f"[INFO] Bucket '{bucket}' already exists.")

def run_data_recovery():
    """
    Executes the data-recovery.py script to collect data from APIs
    and store it in the RAW bucket.
    
    The script is executed as a subprocess, and errors are raised if the script fails.
    """
    script_path = "/opt/airflow/src/data-recovery.py"
    subprocess.run(["python", script_path], check=True)

def run_data_preprocessing():
    """
    Executes the data-preprocessing.py script to process the data in the RAW bucket
    and push it to the STAGING bucket.
    
    The script is executed as a subprocess, ensuring the processing workflow is performed.
    """
    script_path = "/opt/airflow/src/data-preprocessing.py"
    subprocess.run(["python", script_path], check=True)

def run_data_classification():
    """
    Executes the data-classification.py script to classify data in the STAGING bucket
    and transfer it to the CURATED bucket.
    
    This step organizes the data by applying classification logic, ensuring it is ready
    for final use.
    """
    script_path = "/opt/airflow/src/data-classification.py"
    subprocess.run(["python", script_path], check=True)

# ===================================================================
#           DEFINITION OF THE OPENWEATHER DATA LAKE DAG
# ===================================================================
with DAG(
    "openweather_data_lake",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    description="Pipeline to process OpenWeather data into a data lake (RAW -> STAGING -> CURATED)",
    schedule_interval="0 */6 * * *",  # Uncomment to schedule the DAG (e.g., daily)
    start_date=datetime(2024, 1, 1),  # Start date for the DAG
    catchup=False,  # Do not catch up on missed DAG runs
) as dag:

    # -------------------------------------------------------------------
    #   TASK 0: INITIALIZE BUCKETS (RAW, STAGING, CURATED)
    # -------------------------------------------------------------------
    task_initialize_buckets = PythonOperator(
        task_id="initialize_buckets",
        python_callable=ensure_buckets_exist,
    )

    # -------------------------------------------------------------------
    #   TASK 1: DATA RECOVERY - COLLECT DATA INTO THE RAW BUCKET
    # -------------------------------------------------------------------
    task_data_to_raw = PythonOperator(
        task_id="data_to_raw",
        python_callable=run_data_recovery,
    )

    # -------------------------------------------------------------------
    #   TASK 2: DATA PREPROCESSING - PROCESS DATA AND TRANSFER TO STAGING
    # -------------------------------------------------------------------
    task_raw_to_staging = PythonOperator(
        task_id="raw_to_staging",
        python_callable=run_data_preprocessing,
    )

    # -------------------------------------------------------------------
    #   TASK 3: DATA CLASSIFICATION - CLASSIFY DATA AND TRANSFER TO CURATED
    # -------------------------------------------------------------------
    task_staging_to_curated = PythonOperator(
        task_id="staging_to_curated",
        python_callable=run_data_classification,
    )

    # ===================================================================
    #           DEFINE TASK DEPENDENCIES
    # ===================================================================
    task_initialize_buckets >> task_data_to_raw >> task_raw_to_staging >> task_staging_to_curated
