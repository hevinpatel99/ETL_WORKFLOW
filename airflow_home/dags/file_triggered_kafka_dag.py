import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kafka_pipeline_trigger_dag',
    default_args=default_args,
    description='DAG to run Kafka streaming pipeline when multiple files are detected',
    schedule_interval="*/5 * * * *",  # Set to None for manual trigger
    catchup=False
)

FILE_DIRECTORY = os.getenv('FILE_DIRECTORY')
FILE_PATH = os.getenv('FILE_PATH')
PROJECT_PATH = os.getenv('PROJECT_PATH')

if not FILE_DIRECTORY:
    raise ValueError("FILE_DIRECTORY environment variable is not set!")


# Sensor task to check if multiple files exist
file_sensor_task = FileSensor(
    task_id='wait_for_files',
    fs_conn_id='fs_default',  # You need to set this in Airflow Connections
    filepath=FILE_DIRECTORY,
    poke_interval=5,  # Check every 5 seconds
    timeout=50,  # Fail after 10 minutes if files not found
    mode='poke',  # Continues checking at intervals
    dag=dag
)

# List of scripts in order
scripts = ["data_ingestion.py", "data_validation.py", "data_transformation.py", "data_load.py"]

# Create tasks dynamically for each script
previous_task = file_sensor_task  # Start after file detection

for script in scripts:
    script_path = os.path.join(FILE_PATH, script)

    # Modified bash command to add PROJECT_PATH to PYTHONPATH
    bash_command = f'export PYTHONPATH="{PROJECT_PATH}:$PYTHONPATH" && python3 {script_path}'

    task = BashOperator(
        task_id=script.replace(".py", ""),  # Task ID cannot have dots
        bash_command=bash_command,
        dag=dag
    )

    # Define dependencies (sequential execution)
    previous_task >> task
    previous_task = task  # Update previous task for the next iteration

send_email = EmailOperator(
    task_id='send_email_notification',
    to=['hevin.mulani@sahanasystem.com'],
    subject='Airflow DAG: kafka_pipeline_dag Completed Successfully',
    html_content='The Kafka pipeline DAG has completed successfully.',
    dag=dag
)

previous_task >> send_email
