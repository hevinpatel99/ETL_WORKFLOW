# Data Load Process

## Overview

- The DataLoadingProcess script is designed to consume sanitized data from a Kafka topic and insert it into a Cassandra
  database.

## Functionality Breakdown

### Class: DataLoadingProcess

- Consumes sanitized data from Kafka and inserts it into a Cassandra database.

### Data Loading Script

- This script is responsible for consume the sanitizing data from a Kafka topic using Confluent Kafka and insert it into
  a Cassandra database

### Imports Statements

```python

import os
import time
import uuid
from io import StringIO

import pandas as pd
from cassandra.cluster import Cluster
from confluent_kafka import KafkaException, KafkaError

from base_files.database_config import TABLE_CONFIG
from base_files.logger_setup import setup_logger
from kafka.kafka_constants import SANITIZED_TOPIC
from kafka.kafka_utility import create_kafka_consumer, send_to_dlq

```

### Logger Setup

```python
logger = setup_logger("data_loading")

```

### Retry Mechanism

- Defines max_retries = 3 to limit the number of retries in case of failures.
- Uses retry_backoff = 2 seconds to introduce a delay between retries to prevent overwhelming the system.

```python
max_retries = 3
retry_backoff = 2
```

#### Initializes constructor

- Creates a Kafka Consumer.
- Establishes a connection to a Cassandra cluster.
- Creates a session to interact with Cassandra.
- Ensure keyspace exists; create it if not.
- Sets the keyspace to the specified healthcare_data (default).
- Topic for sanitized messages consume.

#### Parameters:

- `keyspace` (str, optional): The name of the Cassandra keyspace. Default is 'healthcare_data'.

```python

def __init__(self, keyspace='healthcare_data'):
    self.consumer = create_kafka_consumer(
        "valid-consumer-group", logger
    )  # Kafka producer instance

    # Connect cassandra cluster
    self.cluster = Cluster(['127.0.0.1'], port=9042)
    self.session = self.cluster.connect()

    # Ensure keyspace exists; create it if not
    self.create_keyspace_if_not_exists(keyspace)  # Create keyspace(database)
    self.session.set_keyspace(keyspace)

    self.sanitized_topic = SANITIZED_TOPIC  # Source topic for sanitized messages consume
```

#### **Cassandra Keyspace Management**

**Method**: create_keyspace_if_not_exists(self, keyspace):

- This function checks if a given Cassandra keyspace exists. If it does not exist, the function attempts to create it.

### steps

- Check Keyspace Existence: The function attempts to use the specified keyspace.
- Handle Exception: If an error occurs (likely due to the keyspace not existing), it logs the error.
- Create Keyspace: If the keyspace does not exist, the function calls self.create_keyspace(keyspace) to create it.

#### Code Snippet:

#### Parameters:

- `keyspace` (str): The name of the Cassandra keyspace.

```python

def create_keyspace_if_not_exists(self, keyspace):
    """Checks if the keyspace exists. If not, creates it"""
    try:
        self.session.execute(f"USE {keyspace}")
    except Exception as e:
        logger.info(f"Critical error during message production: {e}")
        # If it doesn't exist, catch the exception and create the keyspace
        logger.info(f"Keyspace '{keyspace}' does not exist. Creating it now...")
        self.create_keyspace(keyspace)
    pass

```

#### **Creating a Keyspace in Cassandra**

**Method**: create_keyspace(self, keyspace):

- This function is create a keyspace in Apache Cassandra using Python.

#### Code Snippet:

#### Parameters:

- `keyspace` (str): The name of the Cassandra keyspace.

```python
def create_keyspace(self, keyspace):
    """Create the keyspace."""
    try:
        create_keyspace_query = f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class': 'NetworkTopologyStrategy', 'datacenter1': 3}};
            """
        self.session.execute(create_keyspace_query)
        logger.info(f"Keyspace '{keyspace}' created successfully.")
    except Exception as e:
        print(f"Error creating keyspace '{keyspace}': {e}")
```

#### **Consuming messages from a Kafka topic (sanitized_topic)**

**Method**: consume_messages(self)

- This method subscribes to a Kafka topic (validated_topic) to consume file messages & process the data for
  sanitization.

### Steps:

#### Kafka Consumer Subscription

- Subscribes to a Kafka topic (SANITIZED_TOPIC) to consume file messages.
- Logs subscription status.

#### Polling Messages

- Continuously polls for messages from Kafka.
- Uses a timeout mechanism (idle_threshold) to stop the consumer if no new messages arrive after 10 consecutive
  attempts.

#### Message Processing

- Extracts file name and file content from Kafka messages.
- Processes the file using self.process_data_insertion(file_content, file_name).
- Checks for empty files and sends them to DLQ if necessary.

#### Error Handling & Retries

- Implements exponential backoff for retries (2^retries seconds).
- Retries failed messages up to 3 times before sending them to the DLQ.
- Logs detailed error messages for debugging.

#### Kafka Commit Handling

- Commits messages only after successful processing to prevent data loss.
- If processing fails, the consumer does not commit the message, allowing retries.

#### Graceful Shutdown

- The consumer closes properly after processing all messages.
- Ensures Kafka resources are released after completion.

```python

def consume_messages(self):
    """Consumes sanitized messages from Kafka and inserts them into Cassandra."""

    idle_count = 0
    idle_threshold = 10  # Stop after 10 consecutive idle polls

    try:
        # Subscribe to the Kafka topic
        self.consumer.subscribe([self.sanitized_topic])

        logger.info(f"Consumer subscribed to topic: {self.sanitized_topic}")

        while True:
            # Poll for messages from Kafka
            msg = self.consumer.poll(timeout=1.0)  # Timeout is in seconds

            if msg is None:
                # Check no message received within the timeout period
                idle_count += 1
                logger.info(f"No message received within {idle_count} timeout.")
                if idle_count >= idle_threshold:
                    logger.info("No new messages detected. Stopping the consumer.")
                    break
                continue
            elif msg.error():
                # If there is an error in the message
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition reached
                    logger.info(f"End of partition reached for {msg.topic()} "
                                f"partition {msg.partition()} at offset {msg.offset()}")
                else:
                    raise KafkaException(msg.error())
            else:

                idle_count = 0  # Reset idle counter on successful message receiving
                logger.info(f"Received message from topic: {msg.topic()}, partition: {msg.partition()}, "
                            f"offset: {msg.offset()} with key: {msg.key().decode('utf-8')}")

                file_name = msg.key().decode('utf-8') if msg.key() else None

                file_content = msg.value().decode('utf-8') if msg.value() else None

                success = False
                for attempt in range(max_retries):
                    try:
                        self.process_data_insertion(file_content, file_name)
                        success = True
                        break  # Exit retry loop on success
                    except Exception as e:
                        logger.error(f"Attempt {attempt + 1}/{max_retries} failed for {file_name}: {e}")
                        time.sleep(retry_backoff * (2 ** attempt))  # Exponential backoff

                    # Process the file content
                if not success:
                    logger.error(f"All {max_retries} retry attempts failed. Sending {file_name} to DLQ.")
                    send_to_dlq(file_name, file_content, "Processing failed after retries.", logger)
                # Process the file content

                try:
                    self.consumer.commit(asynchronous=False)
                except KafkaException as e:
                    logger.error(f"Error committing Kafka offset: {e}")

    except Exception as e:
        logger.error(f"An error occurred while consuming messages: {e}")
        raise
    finally:
        # Close the consumer to release resources
        self.consumer.close()
        logger.info("Consumer closed successfully.")

```

#### **Data insertion process **

**Method**: process_data_insertion(self, input_file_content, file_name):

- This function use for processes and sanitizes incoming data before inserting it into a Cassandra database.
- It handles file content in JSON format, converts it into a Pandas DataFrame, performs data sanitization, and attempts
  insertion into storage with retry logic.

### Steps:

#### Validation

- Checks if the file content is empty and logs an error if so.

#### Error Handling & Retries

- Implements exponential backoff for retries (2^retries seconds).
- Retries failed messages up to 3 times before sending them to the DLQ.
- Logs detailed error messages for debugging.

#### Data Conversion and Sanitization

- Converts the JSON content into a Pandas DataFrame.
- Formats date columns to YYYY-MM-DD if they exist.
- Renames column "Q" to "QUANTITY" for consistency.
- Logs DataFrame content and data types.

#### Data Insertion:

- Calls insert_data_in_storage(file_name, data_frame) to store processed data.
- If processing is successful, exits the retry loop.

#### Parameters:

- `input_file_content` (str): JSON-formatted string representing the data.
- `file_name` (str): The name of the file.

```python
def process_data_insertion(self, input_file_content, file_name):
    """Processes and sanitizes the incoming data before inserting it into Cassandra."""
    try:
        logger.info(f"Processing file: {file_name}.")

        if not input_file_content:
            logger.error("Received empty file content.")
            return

        success = False
        for attempt in range(max_retries):
            try:
                logger.info(f"Processing file: {file_name} from text to CSV. Attempt {attempt + 1}/{max_retries}")
                try:
                    # Convert content into DataFrame
                    # data_frame = pd.read_json(StringIO(input_file_content), lines=True,  dtype={"DATE": str})
                    data_frame = pd.read_json(StringIO(input_file_content), lines=True)
                    for col in data_frame.select_dtypes(include=['datetime64[ns]']):
                        data_frame[col] = data_frame[col].dt.strftime('%Y-%m-%d')

                except ValueError as e:
                    logger.error(f"Error parsing  data: {e}")
                    return

                if data_frame.empty:
                    logger.warning("Received an empty DataFrame after  parsing. Skipping processing.")
                    return

                # Standardize column names
                data_frame.rename(columns={"Q": "QUANTITY"}, inplace=True)

                print(f"DataFrame - loading script  /n {data_frame}")

                print(f"Data column : {data_frame.dtypes}")

                self.insert_data_in_storage(file_name, data_frame)
                success = True
                break  # Exit retry loop if sanitization is successful

            except Exception as e:
                logger.error(f"Error processing file {file_name}: {e}")
                time.sleep(retry_backoff * (2 ** attempt))  # Exponential backoff before retrying
        if not success:
            logger.error(f"Sanitization failed for {file_name} after {max_retries} attempts. Sending to DLQ.")
            send_to_dlq(file_name, input_file_content, "Sanitization failed after retries", logger)

    except Exception as e:
        logger.error(f"Error processing file {file_name}: {e}")

```

#### **Data Insertion**

**Method**:insert_data_in_storage(self, file_name, data_frame):

- This function determines the correct Cassandra table based on the file name and inserts the corresponding data from a
  DataFrame into the appropriate table.

### Steps:

- The function extracts the base file name (without extension).
- It checks if the file name matches one of the predefined keys in table_mapping.
- If a match is found, it calls **self.process_data(data_frame, table_name)** to insert the data into the corresponding
  Cassandra table.
- If no matching table is found, it logs a warning message indicating that no table was identified for the provided
  file.

#### Parameters:

- `file_name` (str): The name of the file.
- `data_frame` (pandas.DataFrame): The processed data.

```python
def insert_data_in_storage(self, file_name, data_frame):
    """Determines the correct Cassandra table for the data and inserts it."""

    name = os.path.splitext(file_name)[0]

    table_mapping = {
        "allergies": "patient_allergies",
        "patients": "patient_info",
        "familyhistory": "patient_family_history",
        "problems": "patient_diagnoses",
        "procedures": "patient_procedure",
        "refills": "patient_refills",
        "labs": "patient_labs",
        "meds": "patient_meds",
        "vitals": "patient_vitals",
        "socialhistory": "patient_social_history",
    }
    table_name = table_mapping.get(name)

    if table_name:
        self.process_data(data_frame, table_name)
    else:
        logger.warning(f"No matching table found for file: {file_name}")
```

#### **Data Process**

**Method**:process_data(self, data_frame, table_name):

- This function is responsible for processing and inserting data into a specified Cassandra table.
- It dynamically creates the table based on predefined configurations and inserts data from either a Pandas DataFrame or
  a dictionary.

### Steps:

- Retrieves the configuration for the specified table_name from the TABLE_CONFIG dictionary.
    - [View Config File Here](/home/dev1070/Hevin_1070/hevin.softvan@gmail.com/projects/Python_Workspace/ETL_workflow/base_files/database_config.py)
- Calls create_table_dynamically() to ensure the table is created based on the configuration.
- If data_frame is a Pandas DataFrame:
    - Iterates through its rows.
    - Converts each row into a dictionary.
    - Inserts the dictionary into the table using insert_data_into_table().
- If data_frame is already a dictionary:
    - Directly inserts it into the table.
- If data_frame is neither a DataFrame nor a dictionary, prints an error message indicating an invalid data type.


#### Parameters:

- `data_frame` (pandas.DataFrame or dict): The data to be inserted.
- `table_name` (str): The target Cassandra table.

```python
 def process_data(self, data_frame, table_name):
    """Processes and inserts data into the specified Cassandra table."""
    config = TABLE_CONFIG.get(table_name, {})
    self.create_table_dynamically(config, table_name)

    # Iterate through DataFrame rows and insert each row as a dictionary
    if isinstance(data_frame, pd.DataFrame):
        for _, row in data_frame.iterrows():
            self.insert_data_into_table(table_name, row.to_dict())  # ✅ Convert row to dict
    elif isinstance(data_frame, dict):
        self.insert_data_into_table(table_name, data_frame)  # ✅ Already a dict
    else:
        print(f"Invalid data type for '{table_name}': {type(data_frame)}")

```

#### **Dynamic Table Creation Process**

**Method**:create_table_dynamically(self, config, table_name):

- Creates a table dynamically from a config file.

- This function is used to create a SQL table dynamically based on the configuration provided.
- It constructs the table schema from a dictionary containing column names and their respective data types.

### Steps:

- Validates Configuration: If config is empty, the function prints a message and exits.
- Generates SQL Query: It constructs the CREATE TABLE SQL query dynamically based on the config dictionary.
- Executes SQL Query: The function executes the generated query using self.session.execute(create_table_query).
- Logs Output: Prints the generated query and a success message upon table creation.

#### Parameters:

- `config` (dict): The schema configuration for the table.
- `table_name` (str): The name of the table.

```python
def create_table_dynamically(self, config, table_name):
    """Creates a table dynamically from a config file."""

    if not config:
        print(f"No configuration found for table '{table_name}'")
        return

    columns_query = ", ".join([f"{col} {dtype}" for col, dtype in config.items()])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_query});"

    print(f"Create Table Query : {create_table_query}")

    # Execute query
    self.session.execute(create_table_query)
    print(f"Table '{table_name}' created successfully!")
```

#### **Data insertion in cassandra**

**Method**:insert_data_into_table(self, table_name, data):

- Creates a table dynamically from a config file.

- This function designed to insert a record into a specified Cassandra table dynamically.
- It ensures that an ID is assigned to the record if not provided and executes an INSERT query with the provided data.

### Steps:

#### Fetch Table Configuration

- The function retrieves the configuration for the specified table from TABLE_CONFIG.
- If no configuration is found, it prints an error message and exits.

#### Ensure ID Presence

- If the table configuration includes an ID column but the provided data lacks an ID field, the function generates a
  unique UUID.

#### Construct the INSERT Query

- Extracts column names from the data dictionary.
- Constructs the INSERT INTO query dynamically using placeholders (%s) for values.
- Converts dictionary values into a tuple for execution.

#### Execute the Query

- Uses self.session.execute() to insert the data into the Cassandra table.
- Prints a success message with the inserted data.

#### Parameters:

- `table_name` (str): The name of the table.
- `data` (dict): A dictionary representing a single record.

```python
def insert_data_into_table(self, table_name, data):
    """Inserts a record into the specified Cassandra table."""

    config = TABLE_CONFIG.get(table_name)
    if not config:
        print(f"No configuration found for table '{table_name}'")
        return

    # Ensure 'ID' is present; if not, generate a unique UUID
    if 'ID' in config:
        if "ID" not in data:
            data["ID"] = uuid.uuid4()  # Generates a unique ID

    # Extract columns and values from the dictionary
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["%s"] * len(data))
    values = tuple(data.values())

    # Construct INSERT query dynamically
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"

    # Execute query
    self.session.execute(insert_query, values)
    print(f"Data inserted into '{table_name}': {data}")

```

### Main Execution

- Consuming Messages – consumer.consume_messages() likely reads messages from a Kafka topic.

```python
# Main function to run the consumer
if __name__ == "__main__":
    consumer = DataLoadingProcess()
    consumer.consume_messages()
```
