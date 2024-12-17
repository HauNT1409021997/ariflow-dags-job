from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

def download_file_from_gcs(bucket_name, object_name, local_path):
    # Initialize the GCS Hook
    hook = GCSHook()

    # Log the download attempt
    logging.info(f"Starting download from GCS bucket '{bucket_name}' object '{object_name}' to local path '{local_path}'.")

    try:
        # Download the file
        hook.download(bucket_name, object_name, local_path)

        # Log success
        logging.info(f"Successfully downloaded '{object_name}' from bucket '{bucket_name}' to '{local_path}'.")
    
    except Exception as e:
        # Log failure
        logging.error(f"Failed to download '{object_name}' from bucket '{bucket_name}'. Error: {str(e)}")
        raise

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gcs_dynamic_file_retriever',
    default_args=default_args,
    description='A dynamic DAG to get files from GCS',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task to download file from GCS using PythonOperator
    download_task = PythonOperator(
        task_id='download_gcs_file',
        python_callable=download_file_from_gcs,
        op_args=['nth-20241216-0922', '/people-100.csv', '/temp/people-100.csv'],
    )

