import os
import subprocess
from dotenv import load_dotenv, find_dotenv

# Load environment variables from .env file
# Find and load the .env file
env_path = find_dotenv()
if env_path:
    print(f"Loading .env file from: {env_path}")
    load_dotenv(env_path)
else:
    print("No .env file found!")

# Ensure AIRFLOW_HOME is set
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
if not AIRFLOW_HOME:
    raise ValueError("ERROR: AIRFLOW_HOME is not set. Check your .env file.")

# Print for debugging
print(f"Loaded AIRFLOW_HOME from .env: {AIRFLOW_HOME}")


# Set the environment variable
os.environ["AIRFLOW_HOME"] = AIRFLOW_HOME

print(f"AIRFLOW_HOME set to: {AIRFLOW_HOME}")

# Initialize Airflow database
print("Initializing Airflow database...")
subprocess.run(["airflow", "db", "init"])


