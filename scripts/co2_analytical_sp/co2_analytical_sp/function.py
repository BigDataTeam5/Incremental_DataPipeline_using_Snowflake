import os
from snowflake.snowpark import Session
from dotenv import load_dotenv

def create_analytics_tables(session: Session) -> str:
    """
    Creates two analytics tables in the ANALYTICS_CO2 schema:
      - DAILY_ANALYTICS: Contains daily analytics (normalized CO2, daily volatility, daily percent change, etc.)
      - WEEKLY_ANALYTICS: Contains weekly analytics (last weekly CO2, weekly volatility, weekly percent change, etc.)
      
    Uses dynamic warehouse scaling for improved performance.
    """
    # session.sql("USE SCHEMA CO2_DB_DEV.ANALYTICS_CO2").collect()

    # Scale up the warehouse to XLARGE for processing
    session.sql("ALTER WAREHOUSE CO2_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()

    # Create the daily analytics table
    daily_sql = """
    CREATE OR REPLACE TABLE ANALYTICS_CO2.DAILY_ANALYTICS AS
    WITH daily_data AS (
      SELECT
        DATE,
        YEAR,
        MONTH AS MONTH_NUM,
        DAY,
        ROUND(CO2_PPM, 3) AS CO2_PPM,
        DAYNAME(DATE) AS DAY_OF_WEEK,
        MONTHNAME(DATE) AS MONTH_NAME,
        ROUND(CO2_DB_DEV.ANALYTICS_CO2.NORMALIZE_CO2_UDF(
          CO2_PPM,
          MIN(CO2_PPM) OVER (),
          MAX(CO2_PPM) OVER ()
        ), 3) AS NORMALIZED_CO2,
        ROUND(CO2_DB_DEV.ANALYTICS_CO2.CALCULATE_CO2_VOLATILITY(
          CO2_PPM,
          LAG(CO2_PPM) OVER (ORDER BY DATE)
        ), 3) AS DAILY_VOLATILITY,
        ROUND(CO2_DB_DEV.ANALYTICS_CO2.CO2_DAILY_PERCENT_CHANGE(
          CO2_PPM,
          LAG(CO2_PPM) OVER (ORDER BY DATE)
        ), 3) AS DAILY_PERCENTAGE_CHANGE
      FROM HARMONIZED_CO2.HARMONIZED_CO2
    )
    SELECT * FROM daily_data;
    """
    session.sql(daily_sql).collect()

    # Create the weekly analytics table
    weekly_sql = """
    CREATE OR REPLACE TABLE ANALYTICS_CO2.WEEKLY_ANALYTICS AS
    WITH weekly_base AS (
      SELECT
        DATE_TRUNC('WEEK', DATE) AS WEEK_START,
        DATE,
        CO2_PPM,
        CO2_DB_DEV.ANALYTICS_CO2.CALCULATE_CO2_VOLATILITY(
          CO2_PPM,
          LAG(CO2_PPM) OVER (PARTITION BY DATE_TRUNC('WEEK', DATE) ORDER BY DATE)
        ) AS DAILY_VOLATILITY
      FROM HARMONIZED_CO2.HARMONIZED_CO2
    ),
    weekly_with_rn AS (
      SELECT
        WEEK_START,
        CO2_PPM,
        DAILY_VOLATILITY,
        ROW_NUMBER() OVER (PARTITION BY WEEK_START ORDER BY DATE DESC) AS rn
      FROM weekly_base
    ),
    weekly_rollup AS (
      SELECT
        WEEK_START,
        MAX(CASE WHEN rn = 1 THEN CO2_PPM END) AS LAST_CO2,
        AVG(DAILY_VOLATILITY) AS WEEKLY_VOLATILITY
      FROM weekly_with_rn
      GROUP BY WEEK_START
    )
    SELECT
      WEEK_START,
      ROUND(LAST_CO2, 3) AS WEEKLY_CO2,
      ROUND(WEEKLY_VOLATILITY, 3) AS WEEKLY_VOLATILITY,
      ROUND(CO2_DB_DEV.ANALYTICS_CO2.CO2_WEEKLY_PERCENT_CHANGE(
        LAST_CO2,
        LAG(LAST_CO2) OVER (ORDER BY WEEK_START)
      ), 3) AS WEEKLY_PERCENTAGE_CHANGE
    FROM weekly_rollup;
    """
    session.sql(weekly_sql).collect()

    # Scale warehouse back down to XSMALL after processing
    session.sql("ALTER WAREHOUSE CO2_WH SET WAREHOUSE_SIZE = XSMALL").collect()

    return "Analytics tables (DAILY_ANALYTICS and WEEKLY_ANALYTICS) created successfully."

if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()

    # Use the connection name from environment or default to "dev"
    connection_name = os.getenv("SNOWFLAKE_CONNECTION", "dev")
    print(f"Using Snowflake connection profile: {connection_name}")

    # Create a Snowpark session using the connection profile
    with Session.builder.config("connection_name", connection_name).getOrCreate() as session:
        print(f"Connected to Snowflake using {connection_name} profile")
        print(f"Current database: {session.get_current_database()}")
        print(f"Current schema: {session.get_current_schema()}")
        print(f"Current warehouse: {session.get_current_warehouse()}")
        print(f"Current role: {session.get_current_role()}")

        result = create_analytics_tables(session)
        print(result)
