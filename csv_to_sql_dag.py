from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

def download_file_from_gcs(bucket_name, object_name, local_path):
    hook = GCSHook()
    hook.download(bucket_name, object_name, local_path)

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

    # Example task using PythonOperator to download the file from GCS
    download_task = PythonOperator(
        task_id='download_gcs_file',
        python_callable=download_file_from_gcs,
        op_args=['your-bucket-name', 'path/to/file', '/path/to/local/file'],
    )
