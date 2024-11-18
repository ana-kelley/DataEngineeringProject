import pandas as pd
from s3fs.core import S3FileSystem
import json

def get_processed_files(s3, log_path):
    try:
        with s3.open(log_path, 'r') as f:
            return set(f.read().splitlines())
    except FileNotFoundError:
        return set()

def log_processed_file(s3, log_path, file_path):
    with s3.open(log_path, 'a') as f:
        f.write(file_path + '\n')

def transform_data():
    DIR_RAW = "s3://ece5984-s3-anastasiia/traffic_data/raw/"
    DIR_TRANSFORMED = "s3://ece5984-s3-anastasiia/traffic_data/transformed/"
    PROCESSED_LOG = "s3://ece5984-s3-anastasiia/traffic_data/processed_files.log"
    s3 = S3FileSystem()

    processed_files = get_processed_files(s3, PROCESSED_LOG)

    raw_files = s3.ls(DIR_RAW)
    for file_path in raw_files:
        if not file_path.endswith('.json') or file_path in processed_files:
            print(f"Skipping already processed file: {file_path}")
            continue

        print(f"Processing file: {file_path}")

        # Load the raw JSON file
        with s3.open(file_path, 'rb') as f:
            raw_json = json.load(f)

        # Create initial DataFrame
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
                "road_numbers": ', '.join(raw_json["properties"].get("roadNumbers", [])),
                "time_validity": raw_json["properties"].get("timeValidity"),
                "probability_of_occurrence": raw_json["properties"].get("probabilityOfOccurrence"),
                "number_of_reports": raw_json["properties"].get("numberOfReports"),
                "last_report_time": raw_json["properties"].get("lastReportTime"),
                "events": raw_json["properties"].get("events"),
                "geometry_type": raw_json["geometry"].get("type"),
                "coordinates": raw_json["geometry"].get("coordinates")
            }])

            # Explode coordinates into separate rows
            coords_df = pd.DataFrame(df['coordinates'].iloc[0], columns=['longitude', 'latitude'])
            df = df.drop(columns=['coordinates']).merge(coords_df, how='cross')

            # Explode events into separate rows
            df = df.explode('events').reset_index(drop=True)
            if not df['events'].isnull().all():
                event_details = pd.json_normalize(df['events'])
                df = pd.concat([df.drop(columns=['events']), event_details], axis=1)

            # Add time-based columns
            df['start_date'] = pd.to_datetime(df['start_time']).dt.date
            df['start_hour'] = pd.to_datetime(df['start_time']).dt.hour
            df['end_date'] = pd.to_datetime(df['end_time']).dt.date
            df['end_hour'] = pd.to_datetime(df['end_time']).dt.hour

        except KeyError as e:
            print(f"Skipping file {file_path} due to missing key: {e}")
            continue

        print(f"DataFrame created from {file_path}:\n{df.head()}")

        # Save the DataFrame as a .pkl file
        transformed_path = file_path.replace("raw/", "transformed/").replace(".json", ".pkl")
        with s3.open(transformed_path, 'wb') as f:
            df.to_pickle(f)
        print(f"Transformed file saved to {transformed_path}")

        # Log the processed file
        log_processed_file(s3, PROCESSED_LOG, file_path)
