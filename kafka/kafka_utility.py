import json
import logging

from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from kafka.kafka_constants import KAFKA_SERVER, DLQ_TOPIC


def create_kafka_producer(logger):
    """
    Creates and returns a Kafka Producer instance.

    Args:
        logger (logging.Logger): Logger instance.

    Returns:
        Producer: Kafka producer instance.
    """
    try:
        producer = Producer({'bootstrap.servers': KAFKA_SERVER})
        logger.info("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        logging.error(f"Error creating Kafka Producer: {e}")
        raise


def create_kafka_consumer(group_id, logger, auto_offset_reset='earliest'):
    """
    Creates and returns a Kafka Consumer instance.

    Args:
        group_id (str): Consumer group ID.
        logger: Logger instance.
        auto_offset_reset (str, optional): Offset reset policy ('earliest' or 'latest'). Default is 'earliest'.

    Returns:
        Consumer: Kafka consumer instance.
    """

    try:
        consumer_config = {
            'bootstrap.servers': KAFKA_SERVER,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': False
        }

        consumer = Consumer(consumer_config)
        logger.info(f"Kafka Consumer created for group: {group_id}")
        return consumer

    except Exception as e:
        logger.error(f"Error creating Kafka consumer: {e}")
        raise


def delivery_report(err, msg, logger):
    """
    Callback function to track Kafka message delivery status.

    Args:
        err (KafkaError or None): Error if message delivery fails.
        msg (Message): Kafka message object.
        logger : Logger instance.
    """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} partition {msg.partition()}"
                    f"at offset {msg.offset()} with key {msg.key().decode('utf-8')}")


def create_kafka_topic(topic_name, num_partitions=1, replication_factor=1, logger=None):
    print("Num of Partition :  ", num_partitions)
    """
    Create a Kafka topic with the specified partitions and replication factor.

    :param topic_name: The name of the topic to create
    :param num_partitions: Number of partitions for the topic
    :param replication_factor: Replication factor for the topic
    :param logger: logger

    """
    admin_client = AdminClient({'bootstrap.servers': KAFKA_SERVER})
    topic_list = admin_client.list_topics(timeout=10).topics
    if topic_name not in topic_list:
        new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        admin_client.create_topics([new_topic])
        logger.info(f"Topic '{topic_name}' created successfully.")
    else:
        logger.info(f"Topic '{topic_name}' already exists.")


def send_to_dlq(file_name, file_content, error_message, logger):
    """Sends failed messages to Dead Letter Queue (DLQ)."""
    try:
        producer = Producer({'bootstrap.servers': KAFKA_SERVER})
        dlq_message = {
            "file_name": file_name,
            "file_content": file_content,
            "error_message": error_message
        }
        logger.info(f"Sending message to DLQ: {dlq_message}")

        producer.produce(
            DLQ_TOPIC,
            key=file_name,
            value=json.dumps(dlq_message).encode('utf-8'),
            callback=lambda err, msg: delivery_report(err, msg, logger)
        )
        producer.flush()
        logger.info(f"Message sent to DLQ for {file_name}: {error_message}")
    except Exception as e:
        logger.error(f"Failed to send {file_name} to DLQ: {e}", exc_info=True)
