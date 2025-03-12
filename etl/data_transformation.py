import time
from io import StringIO

import pandas as pd
from confluent_kafka import KafkaException, KafkaError

from base_files.logger_setup import setup_logger
from base_files.sanitize_process import DataSanitization
from kafka.kafka_constants import VALIDATED_TOPIC, SANITIZED_TOPIC, DLQ_TOPIC
from kafka.kafka_utility import create_kafka_producer, create_kafka_consumer, send_to_dlq, delivery_report

# Setup logger
logger = setup_logger("data_transformation")

max_retries = 3
retry_backoff = 2


class DataTransformationProcess:
    """Kafka Consumer to process file messages from Kafka, sanitize, and send to another topic."""

    def __init__(self):
        self.producer = create_kafka_producer(logger)  # Kafka producer instance
        self.consumer = create_kafka_consumer(
            "valid-consumer-group", logger
        )  # Kafka consumer instance
        self.valid_topic = VALIDATED_TOPIC  # Source topic for raw messages
        self.sanitized_topic = SANITIZED_TOPIC  # Target topic for sanitized data
        self.sanitized_data_list = []

    def consume_messages(self):
        """Consumes messages from Kafka and processes the file content indefinitely."""

        idle_count = 0
        idle_threshold = 10

        try:
            # Subscribe to the Kafka topic
            self.consumer.subscribe([self.valid_topic])

            logger.info(f"Consumer subscribed to topic: {self.valid_topic}")

            while True:
                # Poll for messages from Kafka
                msg = self.consumer.poll(timeout=1.0)  # Timeout in seconds

                if msg is None:
                    # Check no message received within the timeout period
                    idle_count += 1
                    logger.info(f"No message received within {idle_count} timeout.")
                    if idle_count >= idle_threshold:
                        logger.info("No new messages detected. Stopping the consumer.")
                        break
                    continue
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info(f"End of partition reached for {msg.topic()} "
                                    f"partition {msg.partition()} at offset {msg.offset()}")
                    else:
                        raise KafkaException(msg.error())
                else:
                    # Process the message
                    logger.info(f"Received message from topic: {msg.topic()}, partition: {msg.partition()}, "
                                f"offset: {msg.offset()} with key: {msg.key().decode('utf-8')}")

                    file_name = msg.key().decode('utf-8') if msg.key() else None
                    file_content = msg.value().decode('utf-8') if msg.value() else None

                    success = False
                    for attempt in range(max_retries):
                        try:
                            self.process_data_sanitization(file_content, file_name)
                            success = True
                            break  # Exit retry loop on success
                        except Exception as e:
                            logger.error(f"Attempt {attempt + 1}/{max_retries} failed for {file_name}: {e}")
                            time.sleep(retry_backoff * (2 ** attempt))  # Exponential backoff

                    if not success:
                        logger.error(f"All {max_retries} retry attempts failed. Sending {file_name} to DLQ.")
                        send_to_dlq(file_name, file_content, "Processing failed after retries.", logger)

                    # Commit Kafka offset after successful processing
                    try:
                        self.consumer.commit(asynchronous=False)
                    except KafkaException as e:
                        logger.error(f"Error committing Kafka offset: {e}")

        except Exception as e:
            logger.error(f"An error occurred while consuming messages: {e}", exc_info=True)
            raise
        finally:
            # Close the consumer to release resources
            self.consumer.close()
            logger.info("Consumer closed successfully.")

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


if __name__ == "__main__":
    consumer = DataTransformationProcess()
    consumer.consume_messages()
    consumer.produce_sanitized_data()
