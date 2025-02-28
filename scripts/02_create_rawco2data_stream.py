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
        APPEND_ONLY = true
        SHOW_INITIAL_ROWS = true
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
def check_stream_without_consuming(session):
    """Check if stream has data without advancing consumption point"""
    session.use_schema('RAW_CO2')
    
    # Check if stream has data
    has_data = session.sql("SELECT SYSTEM$STREAM_HAS_DATA('CO2_DATA_STREAM')").collect()[0][0]
    print(f"Stream has unconsumed data: {has_data}")
    
    # Count rows without consuming
    row_count = session.sql("""
        SELECT COUNT(*) 
        FROM TABLE(INFORMATION_SCHEMA.STREAM_DATA('RAW_CO2.CO2_DATA_STREAM'))
    """).collect()[0][0]
    print(f"Stream contains approximately {row_count} records")

def force_stream_refresh_after_copy(session, table_name="CO2_DATA"):
    """Force stream refresh by adding a dummy record and then removing it"""
    session.use_schema('RAW_CO2')
    
    try:
        # Add dummy record
        session.sql(f"""
            INSERT INTO {table_name} VALUES
            (9999, 12, 31, 9999.999, 999.99)
        """).collect()
        
        # Remove dummy record
        session.sql(f"""
            DELETE FROM {table_name} WHERE YEAR = 9999
        """).collect()
        
        print(f"Successfully refreshed stream for {table_name}")
        return True
    except Exception as e:
        print(f"Error refreshing stream: {str(e)}")
        return False

# For local debugging
if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Use connection name from environment or default to "dev"
    connection_name = os.getenv("SNOWFLAKE_CONNECTION", "dev")
    print(f"Using Snowflake connection profile: {connection_name}")
    
    # Create a Snowpark session
    with Session.builder.config("connection_name", connection_name).getOrCreate() as session:
        print(f"Connected to Snowflake using {connection_name} profile")
        print(f"Current database: {session.get_current_database()}")
        print(f"Current schema: {session.get_current_schema()}")
        print(f"Current warehouse: {session.get_current_warehouse()}")
        print(f"Current role: {session.get_current_role()}")
        
        create_raw_co2_stream(session)
        test_raw_co2_stream(session)
        # After your COPY operations complete:
        force_stream_refresh_after_copy(session)
        # Verify stream has data without consuming it
        check_stream_without_consuming(session) 