from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GoogleCloudStorageToLocalOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from datetime import timedelta

import os

# Function to get the list of files from the GCS bucket dynamically
def get_gcs_files(bucket_name, prefix=""):
    hook = GCSHook()
    # List objects in the bucket with the given prefix
    files = hook.list(bucket_name, prefix=prefix)
    return files

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'gcs_dynamic_file_retriever',
    default_args=default_args,
    description='A dynamic DAG to get files from GCS',
    schedule_interval=None,  # Set to None for manual trigger or use a cron expression
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Define the bucket name and prefix (optional)
    bucket_name = 'nth-20241216-0922'
    prefix = '/people-100.csv'  # Modify to your prefix or leave empty

    # Get a list of files in the GCS bucket dynamically
    files = get_gcs_files(bucket_name, prefix)

    # Create tasks dynamically for each file
    download_tasks = []
    for file in files:
        task = GoogleCloudStorageToLocalOperator(
            task_id=f'download_{os.path.basename(file)}',
            bucket_name=bucket_name,
            object_name=file,
            local_file=f'/tmp/{os.path.basename(file)}',  # Local path to save the file
        )
        download_tasks.append(task)

    # Optionally, you can set dependencies if necessary
    # For example, if you need tasks to run sequentially, you can add:
    # download_tasks[0] >> download_tasks[1] >> ...
