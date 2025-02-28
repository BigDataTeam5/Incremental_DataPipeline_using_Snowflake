import requests
import pandas as pd
import boto3
import io
import os
from snowflake.snowpark import Session
from dotenv import load_dotenv

# Load environment variables for Snowflake
load_dotenv()

# AWS S3 Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
PARENT_FOLDER = os.getenv("PARENT_FOLDER", "noaa-co2-data")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# Snowflake Connection
connection_name = os.getenv("SNOWFLAKE_CONNECTION", "dev")


def fetch_co2_data():
    """Fetches CO2 data from NOAA, processes it, and uploads to S3."""
    print("Fetching CO2 data from NOAA...")
    url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.txt"

    response = requests.get(url)
    data = response.text

    print("Processing CO2 data...")
    lines = data.split("\n")

    # Remove comment lines
    data_lines = [line for line in lines if not line.startswith("#") and line.strip()]

    # Split data into columns
    parsed_data = [line.split() for line in data_lines]

    # Define column names
    max_columns = max(len(row) for row in parsed_data)
    columns = ["Year", "Month", "Day", "Decimal Date", "CO2 (ppm)"]
    if max_columns == 6:
        columns.append("CO2 Daily Change")

    # Create DataFrame
    df = pd.DataFrame(parsed_data, columns=columns)

    # Convert columns to appropriate types
    df["Year"] = df["Year"].astype(int)
    df["Month"] = df["Month"].astype(int)
    df["Day"] = df["Day"].astype(int)
    df["Decimal Date"] = df["Decimal Date"].astype(float)
    df["CO2 (ppm)"] = pd.to_numeric(df["CO2 (ppm)"], errors='coerce').fillna(0)

    print("Uploading CO2 data to S3...")
    for year, year_df in df.groupby("Year"):
        csv_buffer = io.StringIO()
        year_df.to_csv(csv_buffer, index=False)
        s3_object_name = f"{PARENT_FOLDER}/{year}/co2_daily_mlo.csv"

        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_object_name,
            Body=csv_buffer.getvalue()
        )

        print(f"Uploaded: s3://{S3_BUCKET_NAME}/{s3_object_name}")

    print("All CO2 data uploaded successfully to S3.")
    return df


def create_snowflake_session():
    """Creates a Snowflake session using Snowpark."""
    print(f"Connecting to Snowflake using profile: {connection_name}")
    
    session = Session.builder.config("connection_name", connection_name).getOrCreate()
    
    print(f"Connected to Snowflake.")
    print(f"Database: {session.get_current_database()}")
    print(f"Schema: {session.get_current_schema()}")
    print(f"Warehouse: {session.get_current_warehouse()}")
    print(f"Role: {session.get_current_role()}")

    return session


def create_raw_co2_stream(session):
    """Creates a stream in Snowflake for incremental CO2 data processing."""
    print("Creating Snowflake stream on RAW_CO2.CO2_DATA...")
    
    session.use_schema('RAW_CO2')
    _ = session.sql('''
        CREATE OR REPLACE STREAM CO2_DATA_STREAM
        ON TABLE CO2_DATA
        APPEND_ONLY = FALSE
        SHOW_INITIAL_ROWS = TRUE
        COMMENT = 'Stream to capture changes to the CO2 data table'
    ''').collect()
    
    print("Stream CO2_DATA_STREAM created successfully.")


def test_raw_co2_stream(session):
    """Tests the newly created Snowflake stream."""
    print("Checking Snowflake stream metadata...")
    session.use_schema('RAW_CO2')

    stream_info = session.sql("DESCRIBE STREAM CO2_DATA_STREAM").collect()
    print("\nStream details:")
    for info in stream_info:
        print(f"  {info}")

    print("\nChecking stream contents...")
    stream_data = session.sql('''
        SELECT * FROM CO2_DATA_STREAM
        WHERE METADATA$ACTION = 'INSERT'
        ORDER BY METADATA$ROW_ID
        LIMIT 5
    ''').collect()

    print(f"\nSample data from stream (found {len(stream_data)} rows):")
    for row in stream_data:
        print(f"  {row}")


if __name__ == "__main__":
    print("Starting CO2 Data Pipeline...")

    # Fetch and upload CO2 data
    df = fetch_co2_data()

    # Connect to Snowflake
    session = create_snowflake_session()

    # Create Snowflake stream
    create_raw_co2_stream(session)

    # Test Snowflake stream
    test_raw_co2_stream(session)

    print("CO2 Data Pipeline Execution Completed!")
