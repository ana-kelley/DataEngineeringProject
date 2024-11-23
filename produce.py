import requests
import json
import math
import time
from datetime import datetime
from kafka import KafkaProducer

# Constants
# TomTom API Key (should be retrieved from credentials.txt in the Project folder in S3)
TOMTOM_API_KEY = ''
# Tile size for dividing the bounding box (in kilometers)
TILE_SIZE = 100
# Kafka topic and server details
KAFKA_TOPIC = 'TrafficData'
KAFKA_SERVER = '34.227.190.251:9126'  # EC2 Kafka server IP and port (update here)

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize messages to JSON
)

def fetch_traffic_data_tile(min_lon, min_lat, max_lon, max_lat):
    """
    Fetch traffic data from the TomTom API for a specific tile.
    """
    url = 'https://api.tomtom.com/traffic/services/5/incidentDetails'
    params = {
        'key': TOMTOM_API_KEY,  # API Key for authentication
        'bbox': f"{min_lon},{min_lat},{max_lon},{max_lat}",  # Define the bounding box for the tile
        'fields': ('{incidents{'
                   'type,geometry{type,coordinates},'
                   'properties{'
                   'id,iconCategory,magnitudeOfDelay,startTime,endTime,from,to,length,delay,'
                   'roadNumbers,timeValidity,probabilityOfOccurrence,numberOfReports,lastReportTime,'
                   'events{description,code,iconCategory}'
                   '}}}'),  # Fields to be retrieved
        'language': 'en-US'  # Language for the response
    }
    response = requests.get(url, params=params)  # Make the API request
    if response.status_code == 200:
        return response.json()  # Return JSON data if successful
    else:
        # Log an error if the API request fails
        print(f"Failed to fetch data for tile ({min_lon}, {min_lat}, {max_lon}, {max_lat}). "
              f"Status: {response.status_code}")
        return None

def split_into_tiles(bbox):
    """
    Divide a large bounding box into smaller tiles, each meeting the 10,000 km² limit.
    """
    min_lon, min_lat, max_lon, max_lat = bbox
    lat_step = TILE_SIZE / 111  # Approx. 1 degree latitude equals 111 km
    lon_step = TILE_SIZE / (111 * math.cos(math.radians(min_lat)))  # Adjust longitude step by latitude

    tiles = []
    lat = min_lat
    while lat < max_lat:
        lon = min_lon
        while lon < max_lon:
            # Create a tile and append it to the list
            tiles.append((lon, lat, min(lon + lon_step, max_lon), min(lat + lat_step, max_lat)))
            lon += lon_step
        lat += lat_step
    return tiles

def send_data_to_kafka(data):
    """
    Send traffic data to the Kafka topic.
    """
    producer.send(KAFKA_TOPIC, data)  # Send data to Kafka
    print(f"Data sent to Kafka topic {KAFKA_TOPIC}")

def fetch_and_publish_all_tiles(bbox, duration):
    """
    Fetch traffic data for all tiles in the bounding box and publish it to Kafka.
    """
    t_end = time.time() + duration  # Calculate the end time
    tiles = split_into_tiles(bbox)  # Divide the bounding box into tiles
    while time.time() < t_end:
        for tile in tiles:
            # Fetch data for each tile
            tile_data = fetch_traffic_data_tile(*tile)
            if tile_data:
                # Send each incident to Kafka
                for incident in tile_data.get("incidents", []):
                    send_data_to_kafka(incident)

if __name__ == "__main__":
    # Define the bounding box for Virginia
    richmond_bbox = [-77.5205, 37.4030, -77.3025, 37.5680]
    # Fetch and publish traffic data for the bounding box
    fetch_and_publish_all_tiles(richmond_bbox, duration=60)  # Run for 60 seconds
    producer.flush()  # Ensure all messages are sent
