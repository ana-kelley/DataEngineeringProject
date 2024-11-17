from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from stream_ingest import kafka_consumer
from transform import transform_data
from load_db import load_data  

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 30),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'stream_ingest_dag',
    default_args=default_args,
    description='Ingest, transform, and load traffic data',
    schedule_interval=timedelta(days=1),
)

# Existing task: Consume data from Kafka
kafka_cons = PythonOperator(
    task_id='consume_data',
    python_callable=kafka_consumer,
    dag=dag,
)

# New task: Transform data
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# New task: Load data into MySQL
load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Define the task pipeline
kafka_cons >> transform_task >> load_task
