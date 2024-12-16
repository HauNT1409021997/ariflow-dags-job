from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 16),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'create_and_insert_sql_from_csv_gcs',
    default_args=default_args,
    schedule_interval='@once',
)

# Function to create table based on CSV columns
def create_table_from_csv(**kwargs):
    file_path = kwargs['ti'].xcom_pull(task_ids='download_csv_file')
    df = pd.read_csv(file_path)
    columns = ", ".join([f"{col} TEXT" for col in df.columns])
    create_table_query = f"CREATE TABLE IF NOT EXISTS my_table ({columns});"

    postgres_hook = PostgresHook(postgres_conn_id='your_postgres_connection')
    postgres_hook.run(create_table_query)

# Function to insert data from CSV into the table
def insert_data_from_csv(**kwargs):
    file_path = kwargs['ti'].xcom_pull(task_ids='download_csv_file')
    df = pd.read_csv(file_path)
    insert_data_query = f"INSERT INTO my_table VALUES %s"
    data = [tuple(row) for row in df.to_numpy()]

    postgres_hook = PostgresHook(postgres_conn_id='your_postgres_connection')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    psycopg2.extras.execute_values(cursor, insert_data_query, data)
    connection.commit()

# Task to download CSV file from GCS
download_csv_task = GCSToLocalOperator(
    task_id='download_csv_file',
    bucket='nth-20241216-0922',
    object_name='/people-100.csv',
    filename='/people-100.csv',
    dag=dag,
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

# Setting the task sequence
download_csv_task >> create_table_task >> insert_data_task