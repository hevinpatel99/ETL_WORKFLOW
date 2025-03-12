# Data Ingestion Process

## Overview

- This script reads text files from a specified directory and publishes their content to a Kafka topic using a Kafka
  producer.
- After successfully sending the data, the script deletes the processed files.

> **📝 Note:**  
> For reusable constants and utility functions, refer to:

#### constant use in kafka script

- [KAFKA CONSTANTS](../kafka/kafka_constants.py)

#### Utility methods like consumer creation, producer creation, dead letter queue logic that use in kafka script

- [KAFKA UTILITY](../kafka/kafka_utility.py)

#### Logger setup

- [LOGGER SETUP](../base_files/logger_setup.py)

- Always use these files to avoid duplicate code and maintain consistency.

## Installation

1. Install dependencies:
   ```bash
   pip install confluent-kafka
   ```
2. Ensure Kafka is running and the required topic is created.

## Usage

Run the script using:

```bash
python3 data_ingestion.py
```

Ensure the `RAW_DATA_DIR` contains text files before execution.

## Functionality Breakdown

### Class: `FileIngestionProcess`

- Handles reading files from a directory and publishing their content to Kafka.

# Data Ingestion Script

- This script is responsible for ingesting data into a Kafka topic using Confluent Kafka. It includes logging, Kafka
  producer setup, and message delivery reporting.

### Imports Statements

```python
import os
import time
from confluent_kafka import KafkaException

from base_files.logger_setup import setup_logger
from kafka.kafka_constants import RAW_TOPIC, RAW_DATA_DIR
from kafka.kafka_utility import create_kafka_producer, delivery_report
```

### Logger Setup

```python
logger = setup_logger("data_ingestion")
```

### Python Constructor Method: `__init__`

#### What is `__init__`?

- The `__init__` method is a special method (constructor) in Python classes that is automatically called when a new
  object/instance of the class is created. It initializes the attributes of the class.

#### Initializes constructor

- Initializes with input directory.
- Creates a Kafka producer.
- Sets the Kafka topic.

```python
    def __init__(self, input_dir):
    self.input_dir = input_dir  # Directory containing files
    self.producer = create_kafka_producer(logger)  # Kafka producer instance
    self.topic = RAW_TOPIC  # Kafka topic to publish messages
```

### **Fetching .txt Files from the Directory**

**Method**: get_text_files_from_directory(self)

- This method scans the specified input directory and retrieves all .txt files.

#### Steps:

- Lists all files in the directory.
- Filters files ending with `.txt`.
- Returns the list of absolute file paths.
- Handles any exceptions that may occur while accessing the directory.

Code Snippet:

- Fetches `.txt` files from the directory.
- Logs file count.

```python

def get_text_files_from_directory(self):
    """Fetches all .txt files from the input directory."""
    try:

        text_files = [
            os.path.join(self.input_dir, file_name)
            for file_name in os.listdir(self.input_dir)
            if file_name.endswith('.txt')
        ]

        logger.info(f"Total .txt files found: {len(text_files)}")
        return text_files

    except Exception as ex:
        logger.error(f"Error while listing files in the directory: {ex}", exc_info=True)
        raise

```

### **Publishing File Content to Kafka**

**Method**: publish_file_content_to_kafka(self)

- This method reads each .txt file, publishes its content to a Kafka topic, and deletes the file after successful
  processing.

#### Steps:

* Calls get_text_files_from_directory() to get the list of files.
* If no valid .txt files are found, logs a warning and exits.
* Iterates through each file in the list
    - Skips empty files to avoid sending empty messages.
    - Extracts the filename and sets it as the Kafka message key.
    - Sends the file content to Kafka.
    - Deletes the file after successful processing.

- Calls `flush()` on the Kafka producer to ensure all messages are sent before the script exits.
- Logs any errors encountered during the process.

```python
def publish_file_content_to_kafka(self):
    """Reads each file and sends its content to Kafka."""
    try:
        # Read files from directory
        text_files = self.get_text_files_from_directory()
        if not text_files:
            logger.warning("No valid text files found for processing.")
            return

        logger.info(f"Processing {len(text_files)} file(s)...")

        for file_path in text_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    file_content = file.read().strip()

                if not file_content:
                    logger.warning(f"Skipping empty file: {file_path}")
                    continue

                file_name = os.path.basename(file_path)
                key = file_name.encode('utf-8')

                try:
                    self.producer.produce(
                        self.topic,
                        key=key,
                        value=file_content,
                        callback=lambda err, msg: delivery_report(err, msg, logger)
                    )
                    logger.info(f"File '{file_path}' sent to Kafka topic '{self.topic}'")

                except KafkaException as ke:
                    logger.error(f"Failed to send message for file {file_name}: {ke}", exc_info=True)
                    continue  # Skip processing this file

                time.sleep(1)

                # Delete file after successful processing
                os.remove(file_path)
                logger.info(f"Deleted processed file: {file_path}")

            except Exception as ex:
                logger.error(f"Error processing file {file_path}: {ex}", exc_info=True)
                continue

        # Ensure all messages are flushed before exiting
        self.producer.flush()
        logger.info("All messages sent successfully.")

except KafkaException as ke:
logger.error(f"Kafka error occurred: {ke}", exc_info=True)
raise

except Exception as ex:
logger.error(f"An error occurred while sending messages: {ex}", exc_info=True)
raise



```

### ** Main Execution**

- Checks if the input directory exists.
- Runs the ingestion pipeline.

```python
if __name__ == "__main__":
    try:
        if not os.path.isdir(RAW_DATA_DIR):
            raise ValueError(f"Input directory does not exist: {RAW_DATA_DIR}")

        file_ingestion_pipeline = FileIngestionProcess(RAW_DATA_DIR)
        file_ingestion_pipeline.publish_file_content_to_kafka()

    except Exception as ex:
        logger.critical(f"Fatal error in file ingestion pipeline: {ex}", exc_info=True)
        raise
```

## Error Handling

- Logs errors related to file access, Kafka, or processing failures.
- Skips empty or unreadable files.

## Example Execution

1. Place `.txt` files in `RAW_DATA_DIR`.
2. Run the script.
3. Check Kafka topic for messages.


