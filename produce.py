import requests
import json
import math
import time
from datetime import datetime
from kafka import KafkaProducer

# Constants
TOMTOM_API_KEY = ''
TILE_SIZE = 100  # km
KAFKA_TOPIC = 'TrafficData'
KAFKA_SERVER = '34.227.190.251:9104'  # EC2 Kafka server IP and port

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_traffic_data_tile(min_lon, min_lat, max_lon, max_lat):
    """Fetch traffic data from TomTom API for a specific tile."""
    url = 'https://api.tomtom.com/traffic/services/5/incidentDetails'
    params = {
        'key': TOMTOM_API_KEY,
        'bbox': f"{min_lon},{min_lat},{max_lon},{max_lat}",
        'fields': ('{incidents{'
                   'type,geometry{type,coordinates},'
                   'properties{'
                   'id,iconCategory,magnitudeOfDelay,startTime,endTime,from,to,length,delay,'
                   'roadNumbers,timeValidity,probabilityOfOccurrence,numberOfReports,lastReportTime,'
                   'events{description,code,iconCategory}'
                   '}}}'),
        'language': 'en-US'
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for tile ({min_lon}, {min_lat}, {max_lon}, {max_lat}). Status: {response.status_code}")
        return None

def split_into_tiles(bbox):
    """Divide a large bounding box into tiles each meeting the 10,000 km² limit."""
    min_lon, min_lat, max_lon, max_lat = bbox
    lat_step = TILE_SIZE / 111  # Approx 1 degree of latitude equals 111 km
    lon_step = TILE_SIZE / (111 * math.cos(math.radians(min_lat)))

    tiles = []
    lat = min_lat
    while lat < max_lat:
        lon = min_lon
        while lon < max_lon:
            tiles.append((lon, lat, min(lon + lon_step, max_lon), min(lat + lat_step, max_lat)))
            lon += lon_step
        lat += lat_step
    return tiles

def send_data_to_kafka(data):
    """Send data to Kafka."""
    producer.send(KAFKA_TOPIC, data)
    print(f"Data sent to Kafka topic {KAFKA_TOPIC}")

def fetch_and_publish_all_tiles(bbox, duration):
    """Fetch and publish traffic data for all tiles in the specified bounding box."""
    t_end = time.time() + duration
    tiles = split_into_tiles(bbox)
    while time.time() < t_end:
        for tile in tiles:
            tile_data = fetch_traffic_data_tile(*tile)
            if tile_data:
                for incident in tile_data.get("incidents", []):
                    send_data_to_kafka(incident)

if __name__ == "__main__":
    # For now bounding box for Virginia
    richmond_bbox = [-77.5205, 37.4030, -77.3025, 37.5680]
    fetch_and_publish_all_tiles(richmond_bbox, duration=3)  # Run the producer for 1 minute
    producer.flush()  # Ensure all messages are sent