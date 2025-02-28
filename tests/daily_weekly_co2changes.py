import os
from snowflake.snowpark import Session
from dotenv import load_dotenv

def create_co2_changes_test_views(session: Session):
    """
    Creates two  views in the RAW_CO2 schema for testing the CO2_CHANGES UDF:
      - TEMP_CO2_CHANGES_DAILY aggregates all daily CO2_PPM values and applies CO2_CHANGES.
      - TEMP_CO2_CHANGES_WEEKLY groups the data by week and applies CO2_CHANGES on the weekly CO2_PPM values.
    """
    # Create  view for daily changes test.
    daily_view_sql = """
    CREATE OR REPLACE  VIEW RAW_CO2.TEMP_CO2_CHANGES_DAILY AS
    SELECT 
        ARRAY_AGG(CO2_PPM) WITHIN GROUP (ORDER BY DATE) AS CO2_VALUES,
        ANALYTICS_CO2.CO2_CHANGES(ARRAY_AGG(CO2_PPM) WITHIN GROUP (ORDER BY DATE)) AS DAILY_WEEKLY_CHANGES
    FROM (
        SELECT 
            TO_DATE(CONCAT(CAST(YEAR AS STRING), '-', CAST(MONTH AS STRING), '-', CAST(DAY AS STRING)), 'YYYY-MM-DD') AS DATE,
            CO2_PPM
        FROM RAW_CO2.CO2_DATA_STREAM
    ) AS t;
    """
    session.sql(daily_view_sql).collect()
    print(" view TEMP_CO2_CHANGES_DAILY created in RAW_CO2 schema for daily changes test.")

    # Create  view for weekly changes test.
    weekly_view_sql = """
    CREATE OR REPLACE  VIEW RAW_CO2.TEMP_CO2_CHANGES_WEEKLY AS
    WITH daily_data AS (
        SELECT 
            TO_DATE(CONCAT(CAST(YEAR AS STRING), '-', CAST(MONTH AS STRING), '-', CAST(DAY AS STRING)), 'YYYY-MM-DD') AS DATE,
            CO2_PPM
        FROM RAW_CO2.CO2_DATA_STREAM
    ),
    weekly_data AS (
        SELECT 
            DATE_TRUNC('WEEK', DATE) AS WEEK_START,
            ARRAY_AGG(CO2_PPM) WITHIN GROUP (ORDER BY DATE) AS CO2_VALUES
        FROM daily_data
        GROUP BY DATE_TRUNC('WEEK', DATE)
    )
    SELECT 
        WEEK_START,
        ANALYTICS_CO2.CO2_CHANGES(CO2_VALUES) AS WEEKLY_CHANGES
    FROM weekly_data;
    """
    session.sql(weekly_view_sql).collect()
    print(" view TEMP_CO2_CHANGES_WEEKLY created in RAW_CO2 schema for weekly changes test.")

if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    
    # Use connection name from environment or default to "dev"
    connection_name = os.getenv("SNOWFLAKE_CONNECTION", "dev")
    print(f"Using Snowflake connection profile: {connection_name}")
    
    # Create a Snowpark session using the connection profile
    with Session.builder.config("connection_name", connection_name).getOrCreate() as session:
        print(f"Connected to Snowflake using {connection_name} profile")
        print(f"Current database: {session.get_current_database()}")
        print(f"Current schema: {session.get_current_schema()}")
        print(f"Current warehouse: {session.get_current_warehouse()}")
        print(f"Current role: {session.get_current_role()}")
        
        # Run the stored procedure to create the test views
        create_co2_changes_test_views(session)