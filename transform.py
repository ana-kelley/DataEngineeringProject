import pandas as pd
from s3fs.core import S3FileSystem
import json

def get_processed_files(s3, log_path):
    """
    Retrieve the list of processed files from S3.
    Reads the log file from S3 containing paths of already processed files.
    """
    try:
        with s3.open(log_path, 'r') as f:
            return set(f.read().splitlines())  # Return a set of processed file paths
    except FileNotFoundError:
        return set()  # Return an empty set if the log file doesn't exist

def log_processed_file(s3, log_path, file_path):
    """
    Log a processed file to S3.
    Appends the file path of a processed file to the log file in S3.
    """
    with s3.open(log_path, 'a') as f:
        f.write(file_path + '\n')  # Append the file path followed by a newline

def transform_data():
    """
    Main function to transform raw traffic data.
    Processes new JSON files from the raw S3 directory, transforms them into structured format,
    and saves them as .pkl files in the transformed S3 directory. Logs processed files to avoid reprocessing.
    """
    # Define S3 paths for raw data, transformed data, and processed log
    DIR_RAW = "s3://ece5984-s3-anastasiia/traffic_data/raw/"
    DIR_TRANSFORMED = "s3://ece5984-s3-anastasiia/traffic_data/transformed/"
    PROCESSED_LOG = "s3://ece5984-s3-anastasiia/traffic_data/processed_files.log"
    s3 = S3FileSystem()

    # Retrieve the list of already processed files from the log
    processed_files = get_processed_files(s3, PROCESSED_LOG)

    # List all raw files in the S3 directory
    raw_files = s3.ls(DIR_RAW)
    for file_path in raw_files:
        # Skip files that are not JSON or have already been processed
        if not file_path.endswith('.json') or file_path in processed_files:
            print(f"Skipping already processed file: {file_path}")
            continue

        print(f"Processing file: {file_path}")

        # Load the raw JSON file
        with s3.open(file_path, 'rb') as f:
            raw_json = json.load(f)

        # Create initial DataFrame from JSON
        try:
            df = pd.DataFrame([{
                "id": raw_json["properties"].get("id"),
                "icon_category": raw_json["properties"].get("iconCategory"),
                "magnitude_of_delay": raw_json["properties"].get("magnitudeOfDelay"),
                "start_time": raw_json["properties"].get("startTime"),
                "end_time": raw_json["properties"].get("endTime"),
                "from_location": raw_json["properties"].get("from"),
                "to_location": raw_json["properties"].get("to"),
                "length": raw_json["properties"].get("length"),
                "delay": raw_json["properties"].get("delay"),
                "road_numbers": ', '.join(raw_json["properties"].get("roadNumbers", [])),  # Join list into a string
                "time_validity": raw_json["properties"].get("timeValidity"),
                "probability_of_occurrence": raw_json["properties"].get("probabilityOfOccurrence"),
                "number_of_reports": raw_json["properties"].get("numberOfReports"),
                "last_report_time": raw_json["properties"].get("lastReportTime"),
                "events": raw_json["properties"].get("events"),
                "geometry_type": raw_json["geometry"].get("type"),
                "coordinates": raw_json["geometry"].get("coordinates")
            }])

            # Explode coordinates into separate rows for detailed geospatial analysis
            coords_df = pd.DataFrame(df['coordinates'].iloc[0], columns=['longitude', 'latitude'])
            df = df.drop(columns=['coordinates']).merge(coords_df, how='cross')

            # Explode events into separate rows for detailed event analysis
            df = df.explode('events').reset_index(drop=True)
            if not df['events'].isnull().all():
                event_details = pd.json_normalize(df['events'])
                df = pd.concat([df.drop(columns=['events']), event_details], axis=1)

            # Add time-based columns for easier temporal analysis
            df['start_date'] = pd.to_datetime(df['start_time']).dt.date
            df['start_hour'] = pd.to_datetime(df['start_time']).dt.hour
            df['end_date'] = pd.to_datetime(df['end_time']).dt.date
            df['end_hour'] = pd.to_datetime(df['end_time']).dt.hour

        except KeyError as e:
            print(f"Skipping file {file_path} due to missing key: {e}")
            continue

        print(f"DataFrame created from {file_path}:\n{df.head()}")

        # Save the transformed DataFrame as a .pkl file
        transformed_path = file_path.replace("raw/", "transformed/").replace(".json", ".pkl")
        with s3.open(transformed_path, 'wb') as f:
            df.to_pickle(f)
        print(f"Transformed file saved to {transformed_path}")

        # Log the processed file
        log_processed_file(s3, PROCESSED_LOG, file_path)
