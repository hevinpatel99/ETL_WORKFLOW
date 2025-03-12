import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

project_path = os.getenv("PROJECT_PATH")
# Debugging step
if project_path is None:
    raise ValueError("PROJECT_PATH is not set or .env file is missing!")

PROJECT_ROOT = Path(project_path)

RAW_DATA_DIR = PROJECT_ROOT / "raw_data"
print(RAW_DATA_DIR)

KAFKA_SERVER = 'localhost:9092'
DLQ_TOPIC = 'dlq_topic'
RAW_TOPIC = 'raw_topic'
VALIDATED_TOPIC = 'validated_topic'
SANITIZED_TOPIC = 'sanitized_topic'
