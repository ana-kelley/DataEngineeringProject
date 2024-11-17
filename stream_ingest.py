import time
from kafka import KafkaConsumer
import json
from s3fs import S3FileSystem
from json import loads

def kafka_consumer():
    s3 = S3FileSystem()
    DIR = "s3://ece5984-s3-anastasiia/traffic_data/raw/"  # S3 bucket location for output

    # Set up the Kafka consumer
    consumer = KafkaConsumer(
        'TrafficData',  # Kafka topic name
        bootstrap_servers=['34.227.190.251:9104'],  # Kafka server address
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        consumer_timeout_ms=5000  # Timeout after 5 seconds of no messages
    )

    # Define the duration to receive data
    t_end = time.time() + 60 * 1  # Duration in seconds
    count = 0  # Track message count

    while time.time() < t_end:
        for message in consumer:
            print(f"Received message: {message.value}")  # Debug output
            # Write each message to a JSON file in S3
            with s3.open(f"{DIR}traffic_data_{count}.json", 'w') as file:
                json.dump(message.value, file)
            count += 1
            if time.time() >= t_end:
                break  # Exit the loop if the time limit is exceeded

    print("Done consuming")
    consumer.close()  # Ensure the consumer is properly closed
