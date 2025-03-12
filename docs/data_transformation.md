# Data Transformation Process

## Overview

- This project implements a Kafka consumer that reads messages containing file content from a Kafka topic('
  validated_topic'), sanitizes the data, and publishes the processed data to another Kafka topic('sanitized_topic').
- Failed messages are sent to a Dead Letter Queue (dlq_topic).

## Functionality Breakdown

### Class: DataTransformationProcess

- Kafka Consumer to process file messages from Kafka, sanitize, and send to another topic.

### Data Validation Script

- This script is responsible for process the sanitizing data into a Kafka topic using Confluent Kafka. It includes
  logging, Kafka
  producer setup, and message delivery reporting.

### Imports Statements

```python
import time
from io import StringIO

import pandas as pd
from confluent_kafka import KafkaException, KafkaError

from base_files.logger_setup import setup_logger
from base_files.sanitize_process import DataSanitization
from kafka.kafka_constants import VALIDATED_TOPIC, SANITIZED_TOPIC, DLQ_TOPIC
from kafka.kafka_utility import create_kafka_producer, create_kafka_consumer, send_to_dlq, delivery_report

```

### Logger Setup

```python
logger = setup_logger("data_transformation")

```

### Retry Mechanism

- Defines max_retries = 3 to limit the number of retries in case of failures.
- Uses retry_backoff = 2 seconds to introduce a delay between retries to prevent overwhelming the system.

```python
max_retries = 3
retry_backoff = 2
```

#### Initializes constructor

- Creates a Kafka producer.
- Creates a Kafka Consumer.
- Sets the Kafka topics for consumes validated data & produce sanitized data.
- Initialize sanitized data list.

```python

def __init__(self):
    self.producer = create_kafka_producer(logger)  # Kafka producer instance
    self.consumer = create_kafka_consumer(
        "valid-consumer-group", logger
    )  # Kafka consumer instance
    self.valid_topic = VALIDATED_TOPIC  # Source topic for raw messages
    self.sanitized_topic = SANITIZED_TOPIC  # Target topic for sanitized data
    self.sanitized_data_list = []

```

#### **Consuming messages from a Kafka topic (VALIDATED_TOPIC)**

**Method**: consume_messages(self)

- This method subscribes to a Kafka topic (validated_topic) to consume file messages & process the data for
  sanitization.

### Steps:

#### Kafka Consumer Subscription

- Subscribes to a Kafka topic (VALIDATED_TOPIC) to consume file messages.
- Logs subscription status.

#### Polling Messages

- Continuously polls for messages from Kafka.
- Uses a timeout mechanism (idle_threshold) to stop the consumer if no new messages arrive after 10 consecutive
  attempts.

#### Message Processing

- Extracts file name and file content from Kafka messages.
- Processes the file using process_data_sanitization().
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

#### Code Snippet:

```python

def consume_messages(self):
    """consumes messages from kafka and processes the file content indefinitely."""

    idle_count = 0
    idle_threshold = 10

    try:
        # subscribe to the kafka topic
        self.consumer.subscribe([self.valid_topic])

        logger.info(f"consumer subscribed to topic: {self.valid_topic}")

        while true:
            # poll for messages from kafka
            msg = self.consumer.poll(timeout=1.0)  # timeout in seconds

            if msg is none:
                # check no message received within the timeout period
                idle_count += 1
                logger.info(f"no message received within {idle_count} timeout.")
                if idle_count >= idle_threshold:
                    logger.info("no new messages detected. stopping the consumer.")
                    break
                continue
            elif msg.error():
                if msg.error().code() == kafkaerror._partition_eof:
                    logger.info(f"end of partition reached for {msg.topic()} "
                                f"partition {msg.partition()} at offset {msg.offset()}")
                else:
                    raise kafkaexception(msg.error())
            else:
                # process the message
                logger.info(f"received message from topic: {msg.topic()}, partition: {msg.partition()}, "
                            f"offset: {msg.offset()} with key: {msg.key().decode('utf-8')}")

                file_name = msg.key().decode('utf-8') if msg.key() else none
                file_content = msg.value().decode('utf-8') if msg.value() else none

                success = false
                for attempt in range(max_retries):
                    try:
                        self.process_data_sanitization(file_content, file_name)
                        success = true
                        break  # exit retry loop on success
                    except exception as e:
                        logger.error(f"attempt {attempt + 1}/{max_retries} failed for {file_name}: {e}")
                        time.sleep(retry_backoff * (2 ** attempt))  # exponential backoff

                if not success:
                    logger.error(f"all {max_retries} retry attempts failed. sending {file_name} to dlq.")
                    send_to_dlq(file_name, file_content, "processing failed after retries.", logger)

                # commit kafka offset after successful processing
                try:
                    self.consumer.commit(asynchronous=false)
                except kafkaexception as e:
                    logger.error(f"error committing kafka offset: {e}")

    except exception as e:
        logger.error(f"an error occurred while consuming messages: {e}", exc_info=true)
        raise
    finally:
        # close the consumer to release resources
        self.consumer.close()
        logger.info("consumer closed successfully.")

```

#### **Sanitizes incoming JSON data before producing to Kafka.**

**Method**:  process_data_sanitization(self, file_content, file_name)

- This method processes tab-separated text files received from Kafka, validates them, and converts them into JSON format
  for further processing.

### Steps:

#### File Content Validation

- Checks if file_content is empty and sends it to the Dead Letter Queue (DLQ) if so.

#### DataFrame Conversion

- Converts JSON string input into a Pandas DataFrame using pd.read_json().
- Handles parsing errors and sends malformed JSON data to the DLQ.

#### Column Standardization & Duplicate rRemoval

- Converts all column names to uppercase for consistency.
- Drops duplicate rows using drop_duplicates().

#### Data Sanitization 

- Uses the DataSanitization for data sanitization.
- If sanitization fails, the file is sent to the DLQ.
- [View Sanitize Document](/home/dev1070/Hevin_1070/hevin.softvan@gmail.com/projects/Python_Workspace/ETL_workflow/documents/sanitize_process.md)

#### JSON Conversion & Storage

- Converts the sanitized DataFrame back to JSON using to_json(orient="records", lines=True).
- Stores the file name and JSON data for later Kafka production.

#### Parameters:

- `file_content`  The raw content of the file to be processed.
- `file_name` The name of the file being processed.

#### Return:

- `None` If an error occurs during processing or after validation, the method returns None.
- The data is stored in `self.sanitized_data_list`, which is a list of dictionaries with file name and corresponding
  JSON data for future Kafka production.

```python
def process_data_sanitization(self, file_content, file_name):
    """Sanitizes incoming JSON data and sends failed data to DLQ if necessary."""

    if not file_content:
        logger.error("Received empty file content.")
        send_to_dlq(file_name, file_content, "Empty file content received", logger)
        return

    success = False
    for attempt in range(max_retries):
        try:
            logger.info(f"Processing file: {file_name} from text to CSV. Attempt {attempt + 1}/{max_retries}")

            # Convert JSON string to DataFrame
            try:
                data_frame = pd.read_json(StringIO(file_content), lines=True)
            except ValueError as e:
                logger.error(f"Error parsing JSON data: {e}")
                send_to_dlq(file_name, file_content, f"JSON parsing error: {e}", logger)
                return

            if data_frame.empty:
                logger.warning(
                    f"Received an empty DataFrame after JSON parsing. Skipping processing for {file_name}.")
                send_to_dlq(file_name, file_content, "Empty DataFrame after parsing", logger)
                return

            # Convert headers to uppercase
            data_frame.columns = [col.upper() for col in data_frame.columns]

            # Remove duplicate rows
            data_frame.drop_duplicates(inplace=True)

            print(f"Data type : {data_frame.dtypes}")

            # Perform data sanitization
            data_sanitization = DataSanitization(data_frame, file_name)
            if not data_sanitization.sanitize_data():
                raise Exception(f"Data sanitization failed for file: {file_name}")

            print(f"DataFrame - transform script before:\n{data_frame}")

            # Convert DataFrame to JSON format
            json_data = data_frame.to_json(orient="records", lines=True)
            print(f"DataFrame - transform script after:\n{json_data}")

            # Append sanitized data to list
            self.sanitized_data_list.append({"file_name": file_name, "json_data": json_data})
            success = True
            break  # Exit retry loop if sanitization is successful

        except Exception as e:
            logger.error(f"Sanitization failed for {file_name} on attempt {attempt + 1}/{max_retries}: {e}")
            time.sleep(retry_backoff * (2 ** attempt))  # Exponential backoff before retrying

    if not success:
        logger.error(f"Sanitization failed for {file_name} after {max_retries} attempts. Sending to DLQ.")
        send_to_dlq(file_name, file_content, "Sanitization failed after retries", logger)

```

### **Publishing Sanitized Json to Kafka**

**Method**: produce_sanitized_data(self)

- This method is sending sanitize JSON data to a Kafka topic.
- It ensures that all records are successfully produced and logged while handling errors gracefully.

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

def produce_sanitized_data(self):
    """Produces validated JSON data to Kafka."""
    try:
        for record in self.sanitized_data_list:
            try:
                self.producer.produce(
                    self.sanitized_topic,
                    key=record["file_name"],
                    value=record["json_data"].encode('utf-8'),
                    callback=lambda err, msg: delivery_report(err, msg, logger)
                )
            except Exception as e:
                logger.error(f"Error producing record with key {record['file_name']}: {e}", exc_info=True)

        # Ensure all messages are sent
        self.producer.flush()
        logger.info("All sanitized records successfully produced to Kafka.")

    except Exception as e:
        logger.error(f"Critical error during message production: {e}", exc_info=True)

```

###  Main Execution

- Consuming Messages – consumer.consume_messages() likely reads messages from a Kafka topic.
- Producing Sanitized Data – consumer.produce_sanitized_data() likely sends processed/sanitized data to another topic.

```python
# Main function to run the consumer
if __name__ == "__main__":
    consumer = DataTransformationProcess()
    consumer.consume_messages()
    consumer.produce_sanitized_data()

```

## Example Execution

1. Consuming Messages
2. Run the script.
3. Check Kafka topic for messages.