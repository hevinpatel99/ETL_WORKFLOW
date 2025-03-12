from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Function to be executed by the PythonOperator
def my_task():
    """Prints a message to indicate task execution."""
    print("Hello from Airflow DAG!")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Ensures tasks do not depend on previous DAG runs
    'start_date': datetime(2024, 3, 1),  # DAG execution start date
    'retries': 1,  # Number of retries in case of failure
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Define the DAG
dag = DAG(
    'my_first_dag',  # DAG name
    default_args=default_args,
    description='My first DAG in the project',  # Description of the DAG
    schedule_interval=None,  # No automatic scheduling; must be triggered manually
)

# Create a PythonOperator task
task = PythonOperator(
    task_id='print_hello',  # Task name
    python_callable=my_task,  # Function to execute
    dag=dag,
)

# Display task in Airflow UI
task
