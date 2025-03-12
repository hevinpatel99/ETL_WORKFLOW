import os
import subprocess
import signal
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Ensure AIRFLOW_HOME is an absolute path
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
os.environ["AIRFLOW_HOME"] = AIRFLOW_HOME

# Function to find and kill any existing Airflow webserver processes
def kill_airflow_webserver():
    try:
        # Find the process IDs of running Airflow webserver processes
        result = subprocess.run(["pgrep", "-f", "airflow webserver"], capture_output=True, text=True)
        pids = result.stdout.strip().split("\n")

        if pids and pids[0]:  # If there are running processes
            print(f"Killing existing Airflow webserver processes: {pids}")
            for pid in pids:
                os.kill(int(pid), signal.SIGTERM)  # Send termination signal
        else:
            print("No stale Airflow webserver processes found.")
    except Exception as e:
        print(f"Error while killing Airflow webserver: {e}")

# Kill any existing Airflow webserver processes
kill_airflow_webserver()

# Start the new Airflow webserver
print(f"Starting Airflow webserver at {AIRFLOW_HOME}...")
subprocess.run(["airflow", "webserver", "--port", "8086"])
