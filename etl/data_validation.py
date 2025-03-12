import csv
import time
from io import StringIO

import pandas as pd
from confluent_kafka import KafkaException, KafkaError

from base_files.logger_setup import setup_logger
from base_files.validation import DataValidation
from kafka.kafka_constants import RAW_TOPIC, VALIDATED_TOPIC
from kafka.kafka_utility import create_kafka_producer, create_kafka_consumer, send_to_dlq, delivery_report

# Setup logger
logger = setup_logger("data_validation")


class FileValidationProcess:
    """Kafka Consumer to process file messages from Kafka, validate, and send to another topic."""

    def __init__(self, group_id):
        self.producer = create_kafka_producer(logger)  # Kafka producer instance
        self.consumer = create_kafka_consumer(group_id, logger)  # Kafka consumer instance for consumer 1
        self.topic = RAW_TOPIC  # Source topic for raw messages consume
        self.validated_topic = VALIDATED_TOPIC  # Target topic for validated data produce
        self.validated_data_list = []

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


# Main function to run the consumer
if __name__ == "__main__":
    consumer = FileValidationProcess('file-ingestion-consumer')
    consumer.consume_messages()
    consumer.produce_validated_data()
