#------------------------------------------------------------------------------
# CO2 Data Pipeline
# Script:       02_create_rawco2data_stream.py
#------------------------------------------------------------------------------

# SNOWFLAKE ADVANTAGE: Streams for incremental processing (CDC)
# SNOWFLAKE ADVANTAGE: Efficient data processing

from snowflake.snowpark import Session
from dotenv import load_dotenv
import os

def create_raw_co2_stream(session):
    """Create a stream on the RAW_CO2.CO2_DATA table"""
    session.use_schema('RAW_CO2')
    
    # Create the stream on the raw CO2 data table
    _ = session.sql('''
        CREATE OR REPLACE STREAM CO2_DATA_STREAM 
        ON TABLE CO2_DATA
        APPEND_ONLY = FALSE
        SHOW_INITIAL_ROWS = TRUE
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
    # Load environment variables
    load_dotenv()
    
    # Set up connection parameters
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE")
    }
    
    # Create a Snowpark session
    with Session.builder.configs(connection_parameters).create() as session:
        create_raw_co2_stream(session)
        test_raw_co2_stream(session)