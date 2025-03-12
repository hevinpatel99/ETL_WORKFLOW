### Components:

1. **File Ingestion Pipeline**
    - **Description**: This component reads `.txt` files from the source directory and streams the raw content to the
      `raw_topic` Kafka topic.
    - **Kafka Topic**: `raw_topic`


2. **Data Validation Pipeline**
    - **Description**: Validates the schema of the incoming data, checks for required fields, and sends the validated
      data to the `validated-data` Kafka topic.
    - **Kafka Topic**: `validated-data`


3. **Data Transformation Pipeline**
    - **Description**: Consumes messages from the`validated-data` topic, to clean and structure the data,
      enrichment, and sends the processed data to the `sanitized-data` Kafka topic.
    - **Kafka Topic**: `sanitized-data`


4. **Data Loading Pipeline**
    - **Description**: Consumes messages from the `sanitized-data` topic and loads the data to target systems like
      cassandra.
    - **Target Systems**: cassandra

5. **Error Handling Pipeline**
    - **Description**: Listens to the `errors` Kafka topic and handles errors by logging them to a file or an error
      management system.
    - **Kafka Topic**: `dead-letter-queue`

---

## How It Works

The project is built on a series of interconnected pipelines that perform the following operations:

1. **File Ingestion**:
    - A source directory containing `.txt` files is monitored.
    - The files are read and streamed to the `raw_topic` Kafka topic.

2. **Data Validation**:
    - The sanitized data is validated for schema correctness and required fields, then forwarded to the `validated-data`
      Kafka topic.

3. **Data Transformation**:
    - The data from `validated-data` is consumed, cleaned, validated, and enriched before being sent to the
      `sanitized-data` Kafka topic.


4. **Data Loading**:
    - Validated data is loaded into target systems such as cassandra, based on predefined
      configurations.

5. **Error Handling**:
    - Any errors encountered during the pipelines are forwarded to the `dead-letter-queue(dlq_topic)` Kafka topic and
      are logged
      into the appropriate system for further investigation.








