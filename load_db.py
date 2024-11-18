from sqlalchemy import create_engine
import pandas as pd
from s3fs.core import S3FileSystem

def update_time_validity(engine, new_event_ids):
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
    DIR_TRANSFORMED = "s3://ece5984-s3-anastasiia/traffic_data/transformed/"
    s3 = S3FileSystem()

    transformed_files = s3.ls(DIR_TRANSFORMED)

    # MySQL credentials
    user = "admin"
    pw = ""
    endpnt = "data-eng-db.cluster-cwgvgleixj0c.us-east-1.rds.amazonaws.com"
    db_name = "anastasiia"

    engine = create_engine(f"mysql+pymysql://{user}:{pw}@{endpnt}/{db_name}")

    processed_event_ids = []

    for file_path in transformed_files:
        if not file_path.endswith('.pkl'):
            continue

        print(f"Loading file: {file_path}")

        # Load transformed .pkl file
        with s3.open(file_path, 'rb') as f:
            df = pd.read_pickle(f)

        if df.empty:
            print(f"Skipping empty file: {file_path}")
            continue

        # Collect event IDs
        processed_event_ids.extend(df['id'].unique())

        # Append the data to MySQL
        try:
            df.to_sql('traffic_data', con=engine, if_exists='append', chunksize=1000, index=False)
            print(f"Data from {file_path} appended to table traffic_data")
        except Exception as e:
            print(f"Failed to load data from {file_path}. Error: {e}")

    # Update old events' time validity
    update_time_validity(engine, processed_event_ids)
