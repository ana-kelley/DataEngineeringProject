import time
from kafka import KafkaConsumer
import json
from s3fs import S3FileSystem
from json import loads

COUNTER_FILE = "s3://ece5984-s3-anastasiia/traffic_data/counter.json"  # File to persist counter
DIR = "s3://ece5984-s3-anastasiia/traffic_data/raw/"  # S3 bucket location for output

def get_counter(s3):
    """Retrieve the current counter from S3, or initialize to 0 if the file doesn't exist."""
    try:
        with s3.open(COUNTER_FILE, 'r') as file:
            return json.load(file).get('counter', 0)
    except FileNotFoundError:
        return 0

def update_counter(s3, counter):
    """Update the counter in S3."""
    with s3.open(COUNTER_FILE, 'w') as file:
        json.dump({'counter': counter}, file)

def kafka_consumer():
    s3 = S3FileSystem()

    # Retrieve the current counter
    count = get_counter(s3)

    # Set up the Kafka consumer
    consumer = KafkaConsumer(
        'TrafficData',  # Kafka topic name
        bootstrap_servers=['34.227.190.251:9104'],  # Kafka server address
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        consumer_timeout_ms=5000  # Timeout after 5 seconds of no messages
    )

    # Define the duration to receive data
    t_end = time.time() + 60 * 1  # Duration in seconds

    while time.time() < t_end:
        for message in consumer:
            print(f"Received message: {message.value}")  # Debug output
            # Write each message to a JSON file in S3
            with s3.open(f"{DIR}traffic_data_{count}.json", 'w') as file:
                json.dump(message.value, file)
            count += 1
            if time.time() >= t_end:
                break  # Exit the loop if the time limit is exceeded

    # Update the counter in S3
    update_counter(s3, count)

    print("Done consuming")
    consumer.close()  # Ensure the consumer is properly closed
