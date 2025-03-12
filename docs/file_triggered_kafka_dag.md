# Kafka Pipeline Trigger DAG

***

### Overview

- This Airflow DAG (kafka_pipeline_trigger_dag) is designed to automate the execution of a Kafka streaming pipeline when
  multiple files are detected in a specified directory. The pipeline consists of several sequential tasks, including
  data ingestion, validation, transformation, and loading.

### import statements

```python

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor

```

## DAG Configuration

#### description of your default_args dictionary in Apache Airflow:

- owner: 'airflow'

    - Specifies the owner of the DAG. This is typically used for organizational purposes.

- depends_on_past: False

    - Determines whether a task should wait for the previous run to succeed before running again.
    - False means that tasks do not depend on past runs and can run independently.

- start_date: datetime(2024, 1, 1)

    - Defines the date and time when the DAG should start running.
    - Tasks will be scheduled from this date onwards.

- retries: 1

    - Specifies the number of times a task should be retried in case of failure.
    - In this case, a failed task will be retried once.

- retry_delay: timedelta(minutes=5)
    - Defines the waiting period before retrying a failed task.
    - Here, Airflow will wait for 5 minutes before retrying the task.

```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
```

## Environment Variables

- FILE_DIRECTORY: Directory to monitor for incoming files.
- FILE_PATH: Path where the scripts are located.
- PROJECT_PATH: Path to the project directory (added to PYTHONPATH).

```python

FILE_DIRECTORY = os.getenv('FILE_DIRECTORY')
FILE_PATH = os.getenv('FILE_PATH')
PROJECT_PATH = os.getenv('PROJECT_PATH')

if not FILE_DIRECTORY:
    raise ValueError("FILE_DIRECTORY environment variable is not set!")

```

### File Sensor Task

- This FileSensor task waits for the existence of specific files in a directory before proceeding with the workflow. If
  the files are not found within the timeout period, the task fails, preventing dependent tasks from executing.

#### description of your FileSensor task in Apache Airflow:

- task_id='wait_for_files'

    - This assigns a unique ID to the sensor task, making it identifiable within the DAG.

- fs_conn_id='fs_default'

    - Specifies the Airflow connection ID for the filesystem where the files are expected.
    - This connection must be configured in Airflow Connections for the sensor to work.

- filepath=FILE_DIRECTORY

    - Defines the directory or file path where the sensor will check for the presence of files.
    - FILE_DIRECTORY should be defined earlier in the script, pointing to the target location.

- poke_interval=5

    - The sensor will check for file existence every 5 seconds.

- timeout=50

    - The sensor will keep checking for a maximum of 50 seconds.
    - If the file is not found within this time, the task will fail.

- mode='poke'

    - The sensor operates in poke mode, meaning it actively checks for the file at regular intervals until it is found
      or the timeout occurs.

- dag=dag

    - Associates this task with a specific DAG (dag).
    - Ensures the sensor runs as part of the defined DAG workflow.

```python

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


```

#### Data Processing Tasks

- Executes the following scripts sequentially:
    - data_ingestion.py
    - data_validation.py
    - data_transformation.py
    - data_load.py

```python

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


```

#### Email Notification Task

- This EmailOperator sends a notification email when the Kafka pipeline DAG completes successfully, ensuring that
  stakeholders are informed about the workflow execution.


- task_id='send_email_notification'

    - This assigns a unique identifier to the task within the DAG.
- to=['hevin.mulani@sahanasystem.com']

    - Specifies the recipient(s) of the email notification.
    - Multiple recipients can be added as a list.
- subject='Airflow DAG: kafka_pipeline_dag Completed Successfully'

- Defines the email subject.
    - Here, it indicates that the Kafka pipeline DAG has completed successfully.
- html_content='The Kafka pipeline DAG has completed successfully.'

    - Sets the email body content in HTML format.
    - Can be modified to include dynamic content like logs or execution details.
- dag=dag

    - Associates this task with the specified DAG (dag) to ensure it's executed within the workflow.
- previous_task >> send_email

    - Defines a task dependency.
    - send_email will only execute after previous_task completes successfully.

```python

send_email = EmailOperator(
    task_id='send_email_notification',
    to=['hevin.mulani@sahanasystem.com'],
    subject='Airflow DAG: kafka_pipeline_dag Completed Successfully',
    html_content='The Kafka pipeline DAG has completed successfully.',
    dag=dag
)

previous_task >> send_email


```