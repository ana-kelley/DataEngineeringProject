from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from stream_ingest import kafka_consumer  # Kafka ingestion logic
from transform import transform_data  # Data transformation logic
from load_db import load_data  # Database loading logic

# Default arguments for the DAG, defining retry logic, email notifications, etc.
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Tasks do not depend on past runs
    'start_date': datetime(2024, 10, 30),  # Start date of the DAG
    'email': ['airflow@example.com'],  # Email for notifications
    'email_on_failure': False,  # Disable email on task failure
    'email_on_retry': False,  # Disable email on task retry
    'retries': 1,  # Number of retries for a failed task
    'retry_delay': timedelta(minutes=1),  # Delay between retries
}

# Define the DAG (Directed Acyclic Graph) for the pipeline
dag = DAG(
    'stream_ingest_dag',  # Unique identifier for the DAG
    default_args=default_args,  # Default arguments applied to all tasks
    description='Ingest, transform, and load traffic data',  # Description of the DAG
    schedule_interval=timedelta(days=1),  # Run the DAG daily
)

# Task 1: Consume data from Kafka
kafka_cons = PythonOperator(
    task_id='consume_data',  # Unique identifier for the task
    python_callable=kafka_consumer,  # Function to execute
    dag=dag,  # Associate the task with the DAG
)

# Task 2: Transform data
transform_task = PythonOperator(
    task_id='transform_data',  # Unique identifier for the task
    python_callable=transform_data,  # Function to execute
    dag=dag,  # Associate the task with the DAG
)

# Task 3: Load data into MySQL
load_task = PythonOperator(
    task_id='load_data',  # Unique identifier for the task
    python_callable=load_data,  # Function to execute
    dag=dag,  # Associate the task with the DAG
)

# Define the task sequence
# Kafka ingestion runs first, followed by transformation, and then loading into MySQL
kafka_cons >> transform_task >> load_task
