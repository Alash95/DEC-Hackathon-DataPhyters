import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from etl import (
    extract_college_data,
    transform_college_data,
    load_college_data,
    validate_data
)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'college_scorecard_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for College Scorecard data',
    schedule_interval=timedelta(days=30),  # Run monthly
    catchup=False
)

# Define tasks using PythonOperator
extract_task = PythonOperator(
    task_id='extract_college_data',
    python_callable=extract_college_data,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_extracted_data',
    python_callable=validate_data,
    op_args=['{{ task_instance.xcom_pull(task_ids="extract_college_data") }}'],
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_college_data',
    python_callable=transform_college_data,
    op_args=['{{ task_instance.xcom_pull(task_ids="validate_extracted_data") }}'],
    dag=dag
)

load_task = PythonOperator(
    task_id='load_college_data',
    python_callable=load_college_data,
    op_args=['{{ task_instance.xcom_pull(task_ids="transform_college_data") }}'],
    dag=dag
)

# Set task dependencies
extract_task >> validate_task >> transform_task >> load_task