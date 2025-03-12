import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # Ensures that tasks do not depend on previous DAG runs
    'start_date': datetime(2024, 1, 1),  # DAG execution start date
    'retries': 1,  # Number of retries in case of failure
    'retry_delay': timedelta(minutes=5),  # Delay between retries
    'email': ['hevin.softvan@gmail.com'],  # Email recipient for notifications
    'email_on_failure': True,  # Send email notification on task failure
    'email_on_retry': True,  # Send email notification on retry
}

# Define the DAG
dag = DAG(
    'kafka_pipeline_dag',  # DAG name
    default_args=default_args,
    description='DAG to run Kafka streaming pipeline',  # Description of the DAG
    schedule_interval=None,  # No automatic scheduling; it must be triggered manually
    catchup=False,  # Prevents backfilling of past runs
)

# Environment variables
FILE_PATH = os.getenv('FILE_PATH')  # Directory where scripts are located
PROJECT_PATH = os.getenv('PROJECT_PATH')  # Project root directory

# List of Python scripts to be executed in sequence
scripts = ["data_ingestion.py", "data_validation.py", "data_transformation.py", "data_load.py"]

# Initialize variable to track the previous task for setting dependencies
previous_task = None

# Dynamically create BashOperator tasks for each script in the list
for script in scripts:
    script_path = os.path.join(FILE_PATH, script)  # Full path to the script

    # Bash command to run the Python script with the correct PYTHONPATH
    bash_command = f'export PYTHONPATH="{PROJECT_PATH}:$PYTHONPATH" && python3 {script_path}'

    # Create a BashOperator task for the script
    task = BashOperator(
        task_id=script.replace(".py", ""),  # Task ID cannot contain dots
        bash_command=bash_command,  # Command to execute
        dag=dag
    )

    # Establish dependencies to ensure sequential execution
    if previous_task:
        previous_task >> task  # The current task runs after the previous one

    previous_task = task  # Update previous task for the next iteration

# Email notification task to be executed after the final script
send_email = EmailOperator(
    task_id='send_email_notification',
    to=['hevin.mulani@sahanasystem.com'],  # Recipient of the email
    subject='Airflow DAG: kafka_pipeline_dag Completed Successfully',  # Email subject
    html_content='The Kafka pipeline DAG has completed successfully.',  # Email body
    dag=dag
)

# Ensure the email notification task runs after the last script execution
previous_task >> send_email
