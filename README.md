# ETL-workflow

***

## Project Description

This project is a data pipeline orchestration system built using Apache Airflow and Apache Kafka. It automates ETL (
Extract, Transform, Load) processes, handles real-time data streaming, and integrates with various data sources for
processing and validation.

- `Airflow` is used to schedule, monitor, and manage complex workflows.
- `Kafka` enables real-time data streaming and message queue management.
- `ETL` scripts process and clean data before loading it into a database or data warehouse.

***

## Project Objectives

- Automate ETL Workflows – Schedule and monitor data pipelines efficiently.
- Real-time Data Processing – Stream and process data using Kafka.
- Data Validation & Cleaning – Ensure data quality before loading.
- Scalability & Modularity – Use a structured framework for easy expansion.
- Logging & Monitoring – Maintain logs for debugging and tracking pipeline execution.

***

## How It Works

- `Data Ingestion`  Extracts data from APIs, databases, or Kafka topics.
- `Data Validation`  Ensures correctness before loading.
- `Data Transformation` Cleans and structures data using ETL scripts.
- `Data Loading` Stores processed data into a database or cloud storage.
- `Orchestration with Airflow ` Schedules and monitors pipeline execution.

***

## Technology Stack

- `Apache Airflow` - Workflow automation & scheduling.
- `Apache Kafka/Confluent kafka` – Data streaming & message queues.
- `Python & Panda` – Core programming language for ETL scripts & data manipulation.
- `Cassandra(Distributed NoSQL Database)` – For data storage.
- `Docker & Docker Compose` – Containers encapsulate the Airflow, Kafka, and Cassandra environments for setup.

***

## Project Structure

```toml   

my_airflow_project/
│── airflow_home/  # Airflow home directory
│   ├── dags/  # Stores DAGs (Directed Acyclic Graphs) for workflows
│   ├── logs/  # Airflow logs directory
│   ├── config/  # Configuration files for Airflow setup  
│── base_files/  # Contains core utility files for logging, validation, and sanitization
│   ├── database_config.py  # Database connection settings and configurations
│   ├── file_validation_config.py  # Configuration rules for file validation
│   ├── logger_setup.py  # Logger setup for structured logging across the project
│   ├── sanitize_process.py  # Functions for data sanitization and cleaning
│   ├── validation.py  # Generic validation functions for data and file integrity
│── docs/  # markdown files for etl scripts
│── etl/  # ETL (Extract, Transform, Load) processing scripts
│   ├── data_ingestion.py  # Handles data extraction from sources (APIs, databases, etc.)
│   ├── data_load.py  # Loads processed data into target storage (DB, data warehouse, etc.)
│   ├── data_transformation.py  # Transforms raw data into required format
│   ├── data_validation.py  # Ensures data integrity and correctness before loading
│── kafka/  # Kafka-related constants and utility functions
│   ├── kafka_constants.py  # Stores Kafka topic names, broker details, and configurations
│   ├── kafka_utility.py  # Helper functions for Kafka producer/consumer
│── kafka_operation/  # Kafka management scripts
│   ├── check_kafka_topic.py  # Script to check the existence of a Kafka topic
│   ├── delete_kafka_topic.py  # Script to delete a Kafka topic
│── logs_files/  # Directory to store logs for monitoring and debugging
│── raw_data/  # Stores raw data before processing
│── scripts/  # Utility scripts for managing the Airflow environment
│   ├── setup_airflow.py  # Initializes and sets up the Airflow environment
│   ├── run_webserver.py  # Starts the Airflow webserver
│   ├── run_scheduler.py  # Starts the Airflow scheduler for DAG execution
│── venv/  # Virtual environment directory (for Python dependencies)
│──.env  # Environment variables file (API keys, database credentials, etc.)
│──.gitignore  # Specifies files and directories to ignore in Git
│── docker - compose.yml  # Docker Compose configuration for running Airflow and related services
│── requirements.txt  # List of required Python dependencies
│── README.md  # Project documentation

```

***

## 🚀 Setup Instructions

### Create the Project Directory

- Run the following commands:

```bash

mkdir my_airflow_project
cd my_airflow_project
touch .env requirements.txt README.md

```

### Configure Environment Variables

- Create a .env file inside my_airflow_project/

```python
AIRFLOW_HOME =./ project_path / ETL_workflow / airflow_home
AIRFLOW__CORE__LOAD_EXAMPLES = False
PROJECT_PATH =./ project_path / ETL_workflow
FILE_PATH =./ project_path / ETL_workflow / etl
FILE_DIRECTORY =./ project_path / ETL_workflow / raw_data

```

### Install Dependencies

- Add the following dependencies to requirements.txt

```text

apache - airflow
python - dotenv
confluent - kafka
cassandra - driver
pandas

```

- Install the dependencies:

```bash

pip install -r requirements.txt 
```

***

## Setup Airflow Services

- create a below directories inside a my_airflow_project.

```bash

mkdir airflow_home 
cd airflow_home 
mkdir dags logs config 

```

### Initialize Airflow Environment Setup

- This script initializes the Apache Airflow environment by loading environment variables, verifying configurations, and
  setting up the Airflow database. It ensures a smooth setup before running Airflow workflows.

#### Loads Environment Variables

- Uses the .env file to configure AIRFLOW_HOME.
- Automatically finds and loads the .env file.
- Ensures AIRFLOW_HOME is set; raises an error if missing.

#### Sets Up Airflow Environment

- Configures AIRFLOW_HOME as an environment variable.
- Verifies if Airflow can locate and use the correct directory.

#### Initializes the Airflow Database

- Runs airflow db init to create necessary Airflow metadata tables.
- Ensures Airflow is ready for DAG execution.

### Contents of scripts/setup_airflow.py

```python

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

```

#### Run the Setup Script

```bash

python3 scripts/setup_airflow.py

```

- After running the script, check if Airflow is properly initialized:

```bash

airflow db check

```

#### Airflow Webserver Management Script

- This script automates the startup of the Apache Airflow webserver, ensuring a clean and error-free launch. It kills
  any existing webserver processes before starting a fresh instance, preventing conflicts and port binding issues.

#### Loads Environment Variables

- Uses the .env file to configure AIRFLOW_HOME.
- Ensures AIRFLOW_HOME is set correctly as an absolute path.

#### Finds & Kills Existing Webserver Processes

- Checks if an Airflow webserver is already running using pgrep.
- If found, terminates the stale process to free up the port.

#### Starts a New Airflow Webserver

- Runs airflow webserver on port 8086 (modifiable).
- Ensures a fresh, conflict-free webserver instance.

### Contents of scripts/run_webserver.py

```python

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

```

#### Run the webserver Script

```bash

python3 scripts/run_webserver.py

```

- Access the Airflow UI
    - Once the webserver starts, open http://localhost:8086 in your browser.

#### Airflow Scheduler Startup Script

- This script automates the startup of the Apache Airflow scheduler, ensuring that DAGs (Directed Acyclic Graphs) are
  continuously monitored and executed as per their schedules.

#### Loads Environment Variables

- Reads configuration from the .env file.
- Ensures AIRFLOW_HOME is correctly set as an absolute path.

#### Sets Up the Airflow Environment

- Assigns AIRFLOW_HOME to the system environment variables.
- Ensures Airflow uses the correct directory for DAGs, logs, and metadata.

#### Starts the Airflow Scheduler

- Executes the airflow scheduler command.
- Ensures DAGs are scheduled and executed based on their defined intervals.

### Contents of scripts/run_scheduler.py:

```python

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


```

#### Run the scheduler Script

```bash

python3 scripts/run_scheduler.py

```

### You can check the airflow cli command for reference

- [airflow CLI command](helper/airflow_cli.txt)

***

## Docker Compose Configuration for ETL Pipeline (docker-compose.yml)

- This Docker Compose file sets up an Apache Kafka & Cassandra-based ETL pipeline with Zookeeper for managing Kafka
  brokers.
- It automates the deployment of essential services in a containerized environment.

### Service Breakdown

#### Services & Components

## Zookeeper (zookeeper)

- Zookeeper manages Kafka metadata, broker election, and health.
- Exposes port 2182 on the host, mapping to 2181 inside the container.

## Kafka

- Message broker for real-time data streaming.
- Connects to Zookeeper (zookeeper:2181).
- Listens on port 9092 for producing/consuming messages.
- Advertises itself as localhost:9092 for client connections.

## Cassandra (cassandra)

- Runs a Cassandra NoSQL database for storing ETL-processed data.
- Uses a persistent volume (cassandra-data) to store database files.
- The container is accessible via port 9042.
- Part of kafka-cassandra-network with alias cassandra.

```yaml

version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    container_name: etl_zookeeper-container
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      4LW_COMMANDS_WHITELIST: "ruok,stat"
    ports:
      - "2182:2181"
    networks:
      - kafka-cassandra-network
    restart: unless-stopped

  kafka:
    image: confluentinc/cp-kafka:latest
    container_name: etl_kafka-container
    depends_on:
      - zookeeper
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "9092:9092"
    networks:
      - kafka-cassandra-network
    restart: unless-stopped

  cassandra:
    image: cassandra:latest
    container_name: etl-cassandra-container
    environment:
      - CASSANDRA_CLUSTER_NAME=TestCluster
      - CASSANDRA_LISTEN_ADDRESS=etl-cassandra-container
      - CASSANDRA_BROADCAST_ADDRESS=etl-cassandra-container
      - CASSANDRA_RPC_ADDRESS=0.0.0.0
    ports:
      - "9042:9042"
    volumes:
      - cassandra-data:/var/lib/cassandra
    networks:
      kafka-cassandra-network:
        aliases:
          - cassandra
    restart: unless-stopped

volumes:
  cassandra-data:

networks:
  kafka-cassandra-network:
    driver: bridge


```

### How to set up it

#### Start the Services

```bash

docker-compose up -d
```

- This runs the services in the background (-d for detached mode).

#### Check Running Containers

```bash

docker ps
```

### You can check the kafka cli command for reference

[kafka CLI command](helper/kafka_cli.txt)

### You can check the cassandra cli command  for reference

[cassandra CLI command](helper/cassandra_cli.txt)

***

## Refer the documents for ETL Workflow scripts

### Components:

#### File Ingestion

1. **File Ingestion Pipeline**
    - **Description**: This component reads `.txt` files from the source directory and streams the raw content to the
      `raw_topic` Kafka topic.
    - **Kafka Topic**: `raw_topic`

- Refer the data ingestion doc - [data_ingestion.md](docs/data_ingestion.md)

***

#### File Validation

**Data Validation Pipeline**

- **Description**: Validates the schema of the incoming data, checks for required fields, and sends the validated
  data to the `validated-data` Kafka topic.
- **Kafka Topic**: `validated-data`

- Refer the data ingestion doc - [data_validation.md](docs/data_validation.md)

***

#### File Transformation

**Data Transformation Pipeline**

- **Description**: Consumes messages from the`validated-data` topic, to clean and structure the data,
  enrichment, and sends the processed data to the `sanitized-data` Kafka topic.
- **Kafka Topic**: `sanitized-data`

- Refer the data Transformation doc - [data_transformation.md](docs/data_transformation.md)

***

#### File Loading

**Data Loading Pipeline**

- **Description**: Consumes messages from the `sanitized-data` topic and loads the data to target systems like
  cassandra.
- **Target Systems**: cassandra

- Refer the data Loading doc - [data_load.md](docs/data_load.md)

***
**Error Handling Pipeline**

- **Description**: Listens to the `errors` Kafka topic and handles errors by logging them to a file or an error
  management system.
- **Kafka Topic**: `dead-letter-queue`

***

#### Validate & Sanitize Data Process

- Refer the validation & Sanitization process

    - [validation.md](docs/data_validation.md)
    - [sanitize_process.md](docs/sanitize_process.md)











