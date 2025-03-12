import os
import time
from confluent_kafka import KafkaException

from base_files.logger_setup import setup_logger
from kafka.kafka_constants import RAW_TOPIC, RAW_DATA_DIR
from kafka.kafka_utility import create_kafka_producer, delivery_report

logger = setup_logger("data_ingestion")

class FileIngestionProcess:
    """Handles reading files from a directory and publishing their content to Kafka."""

    def __init__(self, input_dir):
        self.input_dir = input_dir  # Directory containing files
        self.producer = create_kafka_producer(logger)  # Kafka producer instance
        self.topic = RAW_TOPIC  # Kafka topic to publish messages

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


if __name__ == "__main__":
    try:
        if not os.path.isdir(RAW_DATA_DIR):
            raise ValueError(f"Input directory does not exist: {RAW_DATA_DIR}")

        file_ingestion_pipeline = FileIngestionProcess(RAW_DATA_DIR)
        file_ingestion_pipeline.publish_file_content_to_kafka()

    except Exception as ex:
        logger.critical(f"Fatal error in file ingestion pipeline: {ex}", exc_info=True)
        raise
