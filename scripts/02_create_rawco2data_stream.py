#------------------------------------------------------------------------------
# CO2 Data Pipeline
# Script:       02_create_rawco2data_stream.py
#------------------------------------------------------------------------------

# SNOWFLAKE ADVANTAGE: Streams for incremental processing (CDC)
# SNOWFLAKE ADVANTAGE: Efficient data processing

from snowflake.snowpark import Session
from dotenv import load_dotenv
import os
import json
import sys

# Load environment variables from .env file
load_dotenv('.env')

# Use the specified environment or default to "dev"
env = os.getenv("ENV", "dev").lower()
print(f"Using environment: {env}")

# Ensure the templates directory path is correct
template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates", "environment.json")
print(f"Looking for configuration at: {template_path}")

try:
    # Load environment configuration from JSON file
    with open(template_path, "r") as json_file:
        env_config = json.load(json_file)
except FileNotFoundError:
    print(f"ERROR: Configuration file not found at {template_path}")
    print("Please ensure the templates directory exists and contains environment.json")
    sys.exit(1)

# Establish Snowflake connection using the environment-specific details
try:
    connection_name = env_config["environment"]
    print(f"Using Snowflake connection profile: {connection_name}")
except KeyError:
    print("ERROR: Invalid environment.json format. 'environment' key is missing.")
    sys.exit(1)

def create_raw_co2_stream(session):
    """Create a stream on the RAW_CO2.CO2_DATA table"""
    session.use_schema('RAW_CO2')
    
    # Create the stream on the raw CO2 data table
    _ = session.sql('''
        CREATE OR REPLACE STREAM CO2_DATA_STREAM 
        ON TABLE CO2_DATA
        APPEND_ONLY = false
        SHOW_INITIAL_ROWS = false
        COMMENT = 'Stream to capture changes to the CO2 data table'
    ''').collect()
    
    print("Created RAW_CO2.CO2_DATA_STREAM successfully")

def test_raw_co2_stream(session):
    """Test the newly created stream"""
    session.use_schema('RAW_CO2')
    
    # Check stream metadata
    stream_info = session.sql("DESCRIBE STREAM CO2_DATA_STREAM").collect()
    print("\nStream details:")
    for info in stream_info:
        print(f"  {info}")
    
    # Check stream contents
    stream_data = session.sql('''
        SELECT * FROM CO2_DATA_STREAM 
        WHERE METADATA$ACTION = 'INSERT' 
        ORDER BY METADATA$ROW_ID
        LIMIT 5
    ''').collect()
    
    print(f"\nSample data from stream (found {len(stream_data)} rows):")
    for row in stream_data:
        print(f"  {row}")

# For local debugging
if __name__ == "__main__":
    try:
        # Create a Snowpark session using the configured profile
        with Session.builder.config("connection_name", connection_name).getOrCreate() as session:
            print(f"Connected to Snowflake using {connection_name} profile")
            print(f"Current database: {session.get_current_database()}")
            print(f"Current schema: {session.get_current_schema()}")
            print(f"Current warehouse: {session.get_current_warehouse()}")
            print(f"Current role: {session.get_current_role()}")
            
            create_raw_co2_stream(session)
            test_raw_co2_stream(session)
    except Exception as e:
        print(f"Error: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()