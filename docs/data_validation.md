# Data Validation Process

## Overview

- This Python script is responsible for consuming messages from a Kafka topic, processing file content, handling errors,
  and ensuring validated data is processed correctly.
- The consumer subscribes to a Kafka topic, extracts file content, applies validation, and retries failed messages
  before sending them to a Dead Letter Queue (dlq_topic) if necessary.

## Functionality Breakdown

### Class: FileValidationProcess

- Kafka Consumer to process file messages from Kafka, validate, and send to another topic.

# Data Validation Script

- This script is responsible process the validating data into a Kafka topic using Confluent Kafka. It includes logging,
  Kafka
  producer setup, and message delivery reporting.

### Imports Statements

```python
import csv
import time
from io import StringIO

import pandas as pd
from confluent_kafka import KafkaException, KafkaError

from base_files.logger_setup import setup_logger
from base_files.validation import DataValidation
from kafka.kafka_constants import RAW_TOPIC, VALIDATED_TOPIC
from kafka.kafka_utility import create_kafka_producer, create_kafka_consumer, send_to_dlq, delivery_report
```

### Logger Setup

```python
logger = setup_logger("data_validation")
```

#### Initializes constructor

- Creates a Kafka producer.
- Creates a Kafka Consumer.
- Sets the Kafka topics for consumes raw data & produce validated data.
- Initialize validated data list.

#### Parameters:

- `group_id` The consumer group ID used to create a Kafka consumer.

```python

def __init__(self, group_id):
    self.producer = create_kafka_producer(logger)  # Kafka producer instance
    self.consumer = create_kafka_consumer(group_id, logger)  # Kafka consumer instance for consumer 1
    self.topic = RAW_TOPIC  # Source topic for raw messages consume
    self.validated_topic = VALIDATED_TOPIC  # Target topic for validated data produce
    self.validated_data_list = []
```

#### **Consuming messages from a Kafka topic (RAW_TOPIC)**

**Method**: consume_messages(self)

- This method subscribes to a Kafka topic (RAW_TOPIC) to consume file messages & process the data gor validation.

### Steps:

#### Kafka Consumer Subscription

- Subscribes to a Kafka topic (RAW_TOPIC) to consume file messages.
- Logs subscription status.

#### Polling Messages

- Continuously polls for messages from Kafka.
- Uses a timeout mechanism (idle_threshold) to stop the consumer if no new messages arrive after 10 consecutive
  attempts.

#### Message Processing

- Extracts file name and file content from Kafka messages.
- Processes the file using process_file_content().
- Checks for empty files and sends them to DLQ if necessary.

#### Error Handling & Retries

- Implements exponential backoff for retries (2^retries seconds).
- Retries failed messages up to 3 times before sending them to the DLQ.
- Logs detailed error messages for debugging.

#### Kafka Commit Handling

- Commits messages only after successful processing to prevent data loss.
- If processing fails, the consumer does not commit the message, allowing retries.

#### Graceful Shutdown

- The consumer closes properly after processing all messages.-
- Ensures Kafka resources are released after completion.

#### Code Snippet:

```python

def consume_messages(self):
    """Consumes messages from Kafka and processes the file content."""
    max_retries = 3
    retry_backoff = 2
    idle_count = 0
    idle_threshold = 10

    try:
        # Subscribe to the Kafka topic for consuming messages
        self.consumer.subscribe([self.topic])
        logger.info(f"Consumer subscribed to topic: {self.topic}")

        while True:
            # Poll for messages from Kafka with increased timeout
            msg = self.consumer.poll(timeout=1.0)  # Increased poll timeout from 1.0 to 5.0
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
                logger.info(f"Received message from topic: {msg.topic()}, partition: {msg.partition()}, "
                            f"offset: {msg.offset()} with key: {msg.key().decode('utf-8')}")

                file_name = msg.key().decode('utf-8') if msg.key() else None
                file_content = msg.value().decode('utf-8') if msg.value() else None

                if not file_content:
                    logger.error(f"Received empty file: {file_name}. Skipping processing.")
                    send_to_dlq(file_name, file_content, "Empty file received", logger)
                    return

                retries = 0
                while retries < max_retries:
                    try:
                        self.process_file_content(file_content, file_name)
                        self.consumer.commit(asynchronous=False)
                        logger.info(f"Successfully processed file: {file_name}")
                        break  # Exit retry loop on success
                    except Exception as e:
                        retries += 1
                        wait_time = retry_backoff ** retries  # Exponential backoff
                        logger.warning(f"Retry {retries}/{max_retries} for {file_name}. "
                                       f"Waiting {wait_time} seconds before retrying.")
                        time.sleep(wait_time)

                        # Move to DLQ if all retries fail
                        if retries == max_retries:
                            logger.error(
                                f"Failed to process {file_name} after {max_retries} retries. Sending to DLQ.")
                            send_to_dlq(file_name, file_content, str(e), logger)

    except Exception as e:
        logger.error(f"An error occurred while consuming messages: {e}", exc_info=True)

    finally:
        # Close the consumer to release resources
        self.consumer.close()
        logger.info("Consumer closed successfully.")

```

#### **Parses text file content and validates it before producing to Kafka.**

**Method**: process_file_content(self, file_content, file_name)

- This method processes tab-separated text files received from Kafka, validates them, and converts them into JSON format
  for further processing.

### Steps:

#### File Format Validation

- Ensures the file extension is .txt.
- If an unsupported file type is detected, an error is logged.

#### Delimiter Handling

- Uses csv.Sniffer() to detect the delimiter in the first line.
- If the delimiter is not tab (\t), it is converted manually to tab (\t).

#### DataFrame Conversion

- Converts the text content into a Pandas DataFrame.
- Uses the detected delimiter for parsing.

#### Column Cleanup & Standardization

- Removes unwanted columns (GENDER, TIMESTAMP, YOB) if present.
- Converts column headers to uppercase for consistency.

#### Data Validation

- Uses the DataValidation module to check data integrity.
- If validation fails, the file is sent to the DLQ.
- [View Validation Document](/home/dev1070/Hevin_1070/hevin.softvan@gmail.com/projects/Python_Workspace/ETL_workflow/documents/validation.md)

#### JSON Conversion & Storage

- Converts the validated DataFrame into JSON format (newline-delimited).
- Stores the file name and JSON data for later Kafka production.

#### Parameters:

- `file_content`  The raw content of the file to be processed.
- `file_name` The name of the file being processed.

#### Return:

- `None` If an error occurs during processing or after validation, the method returns None.
- The data is stored in `self.validated_data_list`, which is a list of dictionaries with file name and corresponding
  JSON data for future Kafka production.

```python

def process_file_content(self, file_content, file_name):
    """Parses text file content and validates it before producing to Kafka."""

    try:
        logger.info(f"Parsing file: {file_name}")

        if not file_name.endswith('.txt'):
            raise ValueError(f"Unsupported file type: {file_name}")

        # Handle delimiter
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(file_content.splitlines()[0])
        delimiter = dialect.delimiter
        print(f"Detected delimiter: '{delimiter}'")

        if delimiter != '\t':
            logger.warning(f"Detected delimiter is not tab: '{delimiter}', converting to '\\t'")
            delimiter = '\t'  # Manually set the delimiter to tab

        # Convert text content into DataFrame
        data_frame = pd.read_csv(StringIO(file_content), sep=delimiter, dtype=str)  # Adjusted for CSV format

        # Drop unwanted columns
        columns_to_drop = [col for col in ['GENDER', 'TIMESTAMP', 'YOB'] if col in data_frame.columns]
        if columns_to_drop:
            data_frame = data_frame.drop(columns=columns_to_drop)

        # Convert headers to uppercase
        data_frame.columns = [col.upper() for col in data_frame.columns]

        # Perform Validate data
        data_validation = DataValidation(data_frame, file_name)
        if not data_validation.validate_data():
            logger.error(f"Data validation failed for file: {file_name}, Skipping message production.")
            send_to_dlq(file_name, file_content, "Data validation failed", logger)
            return
        print(f"Data validation before : {data_frame}")

        # Convert to JSON format as records (list of dictionaries)
        json_data = data_frame.to_json(orient="records",
                                       lines=True)

        output_file_name = file_name.replace('.txt', '.json')
        logger.info(f"Output filename : {output_file_name}")
        print(f"Data validation : {json_data}")

        # Store validated data for later Kafka production.
        self.validated_data_list.append({"file_name": output_file_name, "json_data": json_data})
        logger.info(f"Validated file: {file_name}, prepared for Kafka production.")

    except Exception as e:
        logger.error(f"Error processing file {file_name}: {e}", exc_info=True)
        return None

```

### **Publishing Validated Json to Kafka**

**Method**: produce_validated_data(self)

- This method is sending validated JSON data to a Kafka topic.
- It ensures that all records are successfully produced and logged while handling errors gracefully..

### Steps:

#### Kafka Message Production

- Iterates through the list of validated records (self.validated_data_list).
- Produces each record to the Kafka topic (VALIDATED_TOPIC).

#### Key-Value Pair in Kafka

- Uses file name as the key.
- Uses validated JSON data as the value.

#### Error Handling & Logging

- If message production fails, logs the error with details.
- If an unexpected error occurs, logs a critical error message.

#### Kafka Producer Flush

- Ensures all messages are successfully sent before finishing execution.

```python

def produce_validated_data(self):
    """Produces validated JSON data to Kafka."""
    try:
        for record in self.validated_data_list:
            try:
                self.producer.produce(
                    self.validated_topic,
                    key=record["file_name"],
                    value=record["json_data"].encode('utf-8'),
                    callback=lambda err, msg: delivery_report(err, msg, logger)
                )
            except Exception as e:
                logger.error(f"Error producing record with key {record['file_name']}: {e}", exc_info=True)

        # Ensure all messages are sent
        self.producer.flush()
        logger.info("All validated records successfully produced to Kafka.")

    except Exception as e:
        logger.error(f"Critical error during message production: {e}", exc_info=True)

```

### ** Main Execution**

- Consuming Messages – consumer.consume_messages() likely reads messages from a Kafka topic.
- Producing Validated Data – consumer.produce_validated_data() likely sends processed/validated data to another topic.

```python
# Main function to run the consumer
if __name__ == "__main__":
    consumer = FileValidationProcess('file-ingestion-consumer')
    consumer.consume_messages()
    consumer.produce_validated_data()
```

## Example Execution

1. Consuming Messages
2. Run the script.
3. Check Kafka topic for messages.