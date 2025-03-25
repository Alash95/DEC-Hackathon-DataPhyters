from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from extract import fetch_all_data, rank_and_sort_schools
from transform import transform_schools_data, clean_college_data
from load import load_data
from config import DATABASE_URI, URL

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'college_scorecard_etl',
    default_args=default_args,
    description='ETL pipeline for College Scorecard data',
    schedule_interval='@daily',
    catchup=False
)

# Task 1: Extract data from the API
def extract_task(**kwargs):
    all_data = fetch_all_data(URL)
    ranked_schools = rank_and_sort_schools(all_data)
    kwargs['ti'].xcom_push(key='ranked_schools', value=ranked_schools)

extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_task,
    provide_context=True,
    dag=dag
)

# Task 2: Transform data
def transform_task(**kwargs):
    ranked_schools = kwargs['ti'].xcom_pull(task_ids='extract_data', key='ranked_schools')
    transformed_data = transform_schools_data(ranked_schools)
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_task,
    provide_context=True,
    dag=dag
)

# Task 3: Clean data
def clean_task(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data')
    cleaned_data = clean_college_data(*transformed_data)
    kwargs['ti'].xcom_push(key='cleaned_data', value=cleaned_data)

clean = PythonOperator(
    task_id='clean_data',
    python_callable=clean_task,
    provide_context=True,
    dag=dag
)

# Task 4: Load data into PostgreSQL
def load_task(**kwargs):
    cleaned_data = kwargs['ti'].xcom_pull(task_ids='clean_data', key='cleaned_data')
    load_data(*cleaned_data, connection_string=DATABASE_URI)

load = PythonOperator(
    task_id='load_data',
    python_callable=load_task,
    provide_context=True,
    dag=dag
)

# Define task dependencies
extract >> transform >> clean >> load