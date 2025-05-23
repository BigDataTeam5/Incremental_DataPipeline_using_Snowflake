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

# Load environment variables from .env file (only for AWS credentials, not for env)
load_dotenv('.env')

# Ensure the templates directory path is correct
# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))
# Go up one level to reach the project root
project_root = os.path.dirname(os.path.dirname(current_dir))
# Build the path to environment.json
template_path = os.path.join(project_root, "templates", "environment.json")
print(f"Looking for configuration at: {template_path}")

try:
    # Load environment configuration from JSON file
    with open(template_path, "r") as json_file:
        env_config = json.load(json_file)
        env = env_config.get("environment", "").lower()
        print(f"Using environment from JSON: {env}")
except FileNotFoundError:
    print(f"ERROR: Configuration file not found at {template_path}")
    print("Please ensure the templates directory exists and contains environment.json")
    sys.exit(1)
except KeyError:
    print("ERROR: Invalid environment.json format. 'environment' key is missing.")
    sys.exit(1)

# Establish Snowflake connection using the environment-specific details
connection_name = env
print(f"Using Snowflake connection profile: {connection_name}")

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