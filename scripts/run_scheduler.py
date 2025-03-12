import os
import subprocess
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Ensure AIRFLOW_HOME is an absolute path
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
os.environ["AIRFLOW_HOME"] = AIRFLOW_HOME

print(f"Starting Airflow scheduler at {AIRFLOW_HOME}...")
subprocess.run(["airflow", "scheduler"])

