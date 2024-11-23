import time
from kafka import KafkaConsumer
import json
from s3fs import S3FileSystem
from json import loads

# File to persist the counter in S3 for unique file naming
COUNTER_FILE = "s3://ece5984-s3-anastasiia/traffic_data/counter.json"
# Directory in S3 to store raw traffic data
DIR = "s3://ece5984-s3-anastasiia/traffic_data/raw/"

def get_counter(s3):
    """
    Retrieve the current counter from S3 to ensure unique file names.
    If the counter file doesn't exist, initialize the counter to 0.
    """
    try:
        with s3.open(COUNTER_FILE, 'r') as file:
            return json.load(file).get('counter', 0)  # Load the counter value from JSON
    except FileNotFoundError:
        return 0  # Return 0 if the counter file is missing

def update_counter(s3, counter):
    """
    Persist the updated counter to S3.
    This ensures the counter value is maintained across script executions.
    """
    with s3.open(COUNTER_FILE, 'w') as file:
        json.dump({'counter': counter}, file)  # Save the counter as JSON

def kafka_consumer():
    """
    Consume traffic data messages from Kafka, save them as JSON files in S3,
    and maintain a counter for unique file names.
    """
    s3 = S3FileSystem()

    # Retrieve the current counter value
    count = get_counter(s3)

    # Set up the Kafka consumer
    consumer = KafkaConsumer(
        'TrafficData',  # Kafka topic name
        bootstrap_servers=['34.227.190.251:9126'],  # Kafka server address (update here)
        value_deserializer=lambda x: loads(x.decode('utf-8')),  # Deserialize JSON messages
        consumer_timeout_ms=5000  # Timeout after 5 seconds of no messages
    )

    # Define the duration for receiving data
    t_end = time.time() + 60 * 1  # Run for 1 minute

    while time.time() < t_end:
        for message in consumer:
            print(f"Received message: {message.value}")  # Log received message
            # Save each message as a unique JSON file in S3
            with s3.open(f"{DIR}traffic_data_{count}.json", 'w') as file:
                json.dump(message.value, file)
            count += 1  # Increment the counter
            if time.time() >= t_end:
                break  # Exit the loop if the time limit is exceeded

    # Update the counter value in S3 after processing
    update_counter(s3, count)

    print("Done consuming")  # Log completion of the consumer
    consumer.close()  # Ensure the Kafka consumer is properly closed
