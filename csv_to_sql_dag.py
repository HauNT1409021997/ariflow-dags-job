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
        "gcs_bucket": "nth-20241225-0138",
        "csv_object_name": "people-100.csv",
        "table_name": "t_people_info",  # Dynamic table name
        "postgres_host": "data-storage-postgresql.data-storage.svc.cluster.local",  # PostgreSQL host
        "postgres_user": "testuser",  # PostgreSQL username
        "postgres_password": "testpass",  # PostgreSQL password
        "postgres_schema": "testdb",  # PostgreSQL schema
    },
)

# Utility function to convert column names to snake_case
def to_snake_case(name):
    import re
    name = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    name = re.sub(r'[^a-z0-9_]', '', name)  # Remove invalid characters
    return name

# Function to create table based on CSV columns
def create_table_from_csv(**kwargs):
    try:
        local_csv_path = kwargs['ti'].xcom_pull(task_ids='set_paths', key='local_csv_path')
        table_name = kwargs['params']['table_name']
        postgres_schema = kwargs['params']['postgres_schema']

        logger.info(f"Starting table creation. Table name: {table_name}, CSV path: {local_csv_path}")

        df = pd.read_csv(local_csv_path)
        logger.info(f"CSV columns: {list(df.columns)}")

        # Convert columns to snake_case
        df.columns = [to_snake_case(col) for col in df.columns]
        logger.info(f"Converted columns to snake_case: {list(df.columns)}")

        # Define the columns for table creation
        columns = ", ".join([f'"{col}" TEXT' for col in df.columns])

        # Create schema if it doesn't exist
        create_schema_query = f'CREATE SCHEMA IF NOT EXISTS "{postgres_schema}";'

        # Drop the table if it exists
        drop_table_query = f'DROP TABLE IF EXISTS "{postgres_schema}"."{table_name}";'

        # Create the table
        create_table_query = f'CREATE TABLE IF NOT EXISTS "{postgres_schema}"."{table_name}" ({columns});'

        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        postgres_hook.run(create_schema_query)  # Ensure schema exists

        # Drop and recreate the table
        postgres_hook.run(drop_table_query)  # Drop table if it exists
        postgres_hook.run(create_table_query)  # Create the table

        logger.info(f"Table {table_name} dropped and recreated successfully in schema {postgres_schema}.")
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

        # Convert columns to snake_case
        df.columns = [to_snake_case(col) for col in df.columns]
        logger.info(f"Converted columns to snake_case: {list(df.columns)}")

        # Replace NaN values with None for SQL compatibility
        df = df.where(pd.notnull(df), None)

        # Insert data query with explicit column names
        insert_data_query = f"INSERT INTO {postgres_schema}.{table_name} ({', '.join([f'\"{col}\"' for col in df.columns])}) VALUES %s"
        logger.info(f"Insert data query: {insert_data_query}")

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
