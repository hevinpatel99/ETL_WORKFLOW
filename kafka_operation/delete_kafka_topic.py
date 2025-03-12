from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker
}

# Initialize AdminClient
admin_client = AdminClient(conf)

def delete_topic(topic_names):
    try:
        # Request topic deletion
        fs = admin_client.delete_topics(topic_names)

        # Wait for the operation completion and check the result
        for topic, future in fs.items():
            future.result()  # This will block until the deletion is complete
            print(f"Topic '{topic}' deleted successfully.")

    except KafkaException as e:
        print(f"Error deleting topic '{topic_names}': {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

# Example usage
topic_names =['raw_topic','validated_topic','dlq_topic','sanitized_topic'] # Replace with your topic name
delete_topic(topic_names)
