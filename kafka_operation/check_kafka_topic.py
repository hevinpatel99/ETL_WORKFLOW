from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

config = {
    'bootstrap.servers': 'localhost:9092'  # Replace with your Kafka broker's address
}
# Create Kafka AdminClient
admin_client = AdminClient(config)

# List existing topics
try:
    topics = admin_client.list_topics(timeout=10).topics
    print("Existing topics:", topics)
except KafkaException as e:
    print(f"Failed to list topics: {e}")
