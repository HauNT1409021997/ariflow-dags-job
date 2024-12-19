import logging
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.hooks.postgres_hook import PostgresHook
import psycopg2.extras

# Configure logging
logger = logging.getLogger("airflow")
logger.setLevel(logging.INFO)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 16),
    'retries': 1,
}

# Define the DAG with parameters
dag = DAG(
    'dynamic_csv_to_dynamic_table_with_logs',
    default_args=default_args,
    schedule_interval=None,  # Only triggered manually
    params={  # Default values for parameters
        "gcs_bucket": "nth-20241216-0922",
        "csv_object_name": "people-100.csv",
        "table_name": "t_people_info",  # Dynamic table name
        "postgres_host": "my-postgresql.data-process.svc.cluster.local",  # PostgreSQL host
        "postgres_user": "testuser",  # PostgreSQL username
        "postgres_password": "testpass",  # PostgreSQL password
        "postgres_schema": "postgres",  # PostgreSQL schema
    },
)

# Function to create table based on CSV columns
def create_table_from_csv(**kwargs):
    try:
        local_csv_path = kwargs['ti'].xcom_pull(task_ids='set_paths', key='local_csv_path')  # Pull from XCom
        table_name = kwargs['params']['table_name']
        postgres_user = kwargs['params']['postgres_user']
        postgres_password = kwargs['params']['postgres_password']
        postgres_host = kwargs['params']['postgres_host']
        postgres_schema = kwargs['params']['postgres_schema']
        
        logger.info(f"Starting table creation. Table name: {table_name}, CSV path: {local_csv_path}")

        df = pd.read_csv(local_csv_path)
        logger.info(f"CSV columns: {list(df.columns)}")

        columns = ", ".join([f"{col} TEXT" for col in df.columns])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {postgres_schema}.{table_name} ({columns});"
        logger.info(f"Create Table Query: {create_table_query}")

        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        postgres_hook.run(create_table_query)
        logger.info(f"Table {table_name} created successfully.")
    except Exception as e:
        logger.error(f"Failed to create table: {str(e)}")
        raise


# Function to insert data from CSV into the table
def insert_data_from_csv(**kwargs):
    try:
        local_csv_path = kwargs['ti'].xcom_pull(task_ids='set_paths', key='local_csv_path')
        table_name = kwargs['params']['table_name']
        postgres_user = kwargs['params']['postgres_user']
        postgres_password = kwargs['params']['postgres_password']
        postgres_host = kwargs['params']['postgres_host']
        postgres_schema = kwargs['params']['postgres_schema']
        
        logger.info(f"Starting data insertion. Table name: {table_name}, CSV path: {local_csv_path}")

        df = pd.read_csv(local_csv_path)
        logger.info(f"CSV contains {len(df)} rows.")

        insert_data_query = f"INSERT INTO {postgres_schema}.{table_name} VALUES %s"
        data = [tuple(row) for row in df.to_numpy()]
        logger.info(f"Data to insert: {data[:5]}... (showing first 5 rows)")

        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()

        psycopg2.extras.execute_values(cursor, insert_data_query, data)
        connection.commit()
        logger.info(f"Data inserted into table {table_name} successfully.")
    except Exception as e:
        logger.error(f"Failed to insert data into table {table_name}: {str(e)}")
        raise

# Dynamic paths and parameters
def set_dynamic_paths(**kwargs):
    try:
        bucket = kwargs['params']['gcs_bucket']
        object_name = kwargs['params']['csv_object_name']
        local_csv_path = f"/tmp/{object_name.split('/')[-1]}"
        kwargs['ti'].xcom_push(key='local_csv_path', value=local_csv_path)  # Explicit XCom push
        logger.info(f"Dynamic paths set. GCS bucket: {bucket}, Object name: {object_name}, Local path: {local_csv_path}")
    except Exception as e:
        logger.error(f"Failed to set dynamic paths: {str(e)}")
        raise


# Task to set up paths dynamically
set_paths_task = PythonOperator(
    task_id='set_paths',
    python_callable=set_dynamic_paths,
    provide_context=True,
    dag=dag,
)

# Task to download CSV file from GCP
download_csv_task = GCSToLocalFilesystemOperator(
    task_id='download_csv_file',
    bucket="{{ params.gcs_bucket }}",  # Dynamically read bucket name
    object_name="{{ params.csv_object_name }}",  # Dynamically read object name
    filename="{{ task_instance.xcom_pull(task_ids='set_paths', key='local_csv_path') }}",  # Pull from XCom
    dag=dag,
    gcp_conn_id="google_cloud_default"
)


# Task to create table
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table_from_csv,
    provide_context=True,
    dag=dag,
)

# Task to insert data
insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data_from_csv,
    provide_context=True,
    dag=dag,
)

# Task sequence: Ensure that dynamic paths are set before downloading CSV, creating table, and inserting data
set_paths_task >> download_csv_task >> create_table_task >> insert_data_task
