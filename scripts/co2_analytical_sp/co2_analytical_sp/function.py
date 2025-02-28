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
    # Remove this line - USE statements aren't allowed in stored procedures
    # session.sql("USE SCHEMA CO2_DB_DEV.ANALYTICS_CO2").collect()

    # Scale up the warehouse to XLARGE for processing
    session.sql("ALTER WAREHOUSE CO2_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()

    # Create the daily analytics table with fully qualified names
    daily_sql = """
    CREATE OR REPLACE TABLE CO2_DB_DEV.ANALYTICS_CO2.DAILY_ANALYTICS AS
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
      FROM CO2_DB_DEV.HARMONIZED_CO2.HARMONIZED_CO2
    )
    SELECT * FROM daily_data;
    """
    session.sql(daily_sql).collect()

    # Rest of your code with fully qualified object names...
    # [...]
    
    # Scale down the warehouse when done
    session.sql("ALTER WAREHOUSE CO2_WH SET WAREHOUSE_SIZE = SMALL WAIT_FOR_COMPLETION = TRUE").collect()
    
    return "Analytics tables created successfully!"

def main(session: Session) -> str:
    return create_analytics_tables(session)