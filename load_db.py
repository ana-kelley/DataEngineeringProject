from sqlalchemy import create_engine
import pandas as pd
from s3fs.core import S3FileSystem

# Path to the log file in S3 to keep track of processed transformed files
PROCESSED_LOG = "s3://ece5984-s3-anastasiia/traffic_data/processed_transformed_files.log"

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

def update_time_validity(engine, new_event_ids):
    """
    Update the time_validity field in the database for old events.
    Marks old events as 'inactive' if their IDs are not in the latest pull.
    """
    with engine.connect() as connection:
        new_event_ids_str = ', '.join([f"'{event_id}'" for event_id in new_event_ids])
        query = f"""
        UPDATE traffic_data
        SET time_validity = 'inactive'
        WHERE time_validity = 'present' AND id NOT IN ({new_event_ids_str});
        """
        connection.execute(query)
        print("Updated old events to 'inactive'")

def load_data():
    """
    Main function to load transformed data into the MySQL database.
    Processes only new files from the transformed directory and updates the database.
    """
    # Directory in S3 containing transformed files
    DIR_TRANSFORMED = "s3://ece5984-s3-anastasiia/traffic_data/transformed/"
    s3 = S3FileSystem()

    # Retrieve the list of already processed files from the log
    processed_files = get_processed_files(s3, PROCESSED_LOG)
    # List all files in the transformed directory
    transformed_files = s3.ls(DIR_TRANSFORMED)

    # MySQL credentials (Password should be retrieved from S3 under the Project folder in credentials.txt)
    user = "admin"
    pw = ""  # Replace with the password from credentials.txt in S3
    endpnt = "data-eng-db.cluster-cwgvgleixj0c.us-east-1.rds.amazonaws.com"
    db_name = "anastasiia"

    # Create a connection to the MySQL database
    engine = create_engine(f"mysql+pymysql://{user}:{pw}@{endpnt}/{db_name}")

    # List to store IDs of events processed in this batch
    processed_event_ids = []

    for file_path in transformed_files:
        # Skip files that are not .pkl
        if not file_path.endswith('.pkl'):
            continue

        # Skip already processed files
        if file_path in processed_files:
            print(f"Skipping already processed file: {file_path}")
            continue

        print(f"Loading file: {file_path}")

        # Load the transformed .pkl file
        with s3.open(file_path, 'rb') as f:
            df = pd.read_pickle(f)

        # Skip empty DataFrames
        if df.empty:
            print(f"Skipping empty file: {file_path}")
            continue

        # Collect unique event IDs for updating time validity
        processed_event_ids.extend(df['id'].unique())

        # Append the data to the MySQL table
        try:
            df.to_sql('traffic_data', con=engine, if_exists='append', chunksize=1000, index=False)
            print(f"Data from {file_path} appended to table traffic_data")
        except Exception as e:
            print(f"Failed to load data from {file_path}. Error: {e}")

        # Log the processed file
        log_processed_file(s3, PROCESSED_LOG, file_path)

    # Update the time_validity field for old events
    update_time_validity(engine, processed_event_ids)
