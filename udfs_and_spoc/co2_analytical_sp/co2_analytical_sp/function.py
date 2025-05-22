import os
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from snowflake.snowpark.window import Window
from dotenv import load_dotenv
import json
import sys


# Determine if we're running in Snowflake or locally
is_running_in_snowflake = 'SNOWFLAKE_PYTHON_INTERPRETER' in os.environ

# Environment setup - only do file operations when running locally
if not is_running_in_snowflake:
    # Get the project root directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "..", "..", ".."))

    # Load environment exclusively from JSON file
    try:
        env_json_path = os.path.join(project_root, "templates", "environment.json")
        print(f"Looking for environment.json at: {env_json_path}")
        
        if os.path.exists(env_json_path):
            with open(env_json_path, "r") as json_file:
                env_config = json.load(json_file)
            env = env_config.get("environment", "").lower()
            print(f"Loaded environment from file: {env}")
        else:
            # If JSON doesn't exist, use a default value
            env = ""
            print(f"Warning: environment.json not found. No environment configured.")
    except Exception as e:
        print(f"Error loading configuration: {e}")
        env = ""
        print(f"Using empty environment due to error.")
else:
    # When running in Snowflake, use the environment from the connection
    env = os.getenv("SNOWFLAKE_ENV", "").lower()
    print(f"Running in Snowflake with environment: {env}")

# Use the environment to establish Snowflake connection
connection_name = env
print(f"Using Snowflake connection profile: {connection_name}")


def table_exists(session, schema='', name=''):
    """Check if a table exists in the specified schema."""
    exists = session.sql(
        f"SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{name}') AS TABLE_EXISTS"
    ).collect()[0]['TABLE_EXISTS']
    return exists

def create_daily_stats_table(session):
    """Create the daily CO2 stats table with proper schema definition."""
    print("Creating DAILY_CO2_STATS table...")
    session.sql("""
    CREATE OR REPLACE TABLE ANALYTICS_CO2.DAILY_CO2_STATS (
        DATE DATE,
        CO2_PPM FLOAT,
        PREV_DAY_CO2 FLOAT,
        DAILY_CHANGE FLOAT,
        DAILY_VOLATILITY FLOAT,
        NORMALIZED_CO2 FLOAT,
        META_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """).collect()

def create_weekly_stats_table(session):
    """Create the weekly CO2 stats table with proper schema definition."""
    print("Creating WEEKLY_CO2_STATS table...")
    session.sql("""
    CREATE OR REPLACE TABLE ANALYTICS_CO2.WEEKLY_CO2_STATS (
        WEEK_START DATE,
        AVG_WEEKLY_CO2 FLOAT,
        WEEK_START_CO2 FLOAT,
        WEEK_END_CO2 FLOAT,
        WEEKLY_CHANGE FLOAT,
        WEEKLY_VOLATILITY FLOAT,
        NORMALIZED_WEEKLY_CO2 FLOAT,
        META_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """).collect()

def process_daily_metrics(session):
    """Process daily metrics using Snowpark DataFrame API instead of raw SQL."""
    current_warehouse = session.get_current_warehouse()
    try:
        # Scale warehouse up for better performance
        if current_warehouse:
            session.sql(f"ALTER WAREHOUSE {current_warehouse} SET WAREHOUSE_SIZE = LARGE WAIT_FOR_COMPLETION = TRUE").collect()
        
        print("Processing daily CO2 metrics...")
        
        # First, try to access the temporary _CO2_MINMAX table created by harmonized procedure
        try:
            minmax_df = session.sql("SELECT * FROM ANALYTICS_CO2._CO2_MINMAX").collect()
            min_co2 = minmax_df[0]["MIN_CO2"]
            max_co2 = minmax_df[0]["MAX_CO2"]
            print(f"Found _CO2_MINMAX table with MIN_CO2={min_co2}, MAX_CO2={max_co2}")
        except Exception as e:
            print(f"Could not access temporary _CO2_MINMAX table: {e}")
        
        # Get source data
        source_df = session.table("HARMONIZED_CO2.HARMONIZED_CO2")
        
        # Create a DataFrame with calculated metrics
        daily_df = source_df.select(
            F.col("DATE"),
            F.col("CO2_PPM"),
            F.lag(F.col("CO2_PPM")).over(Window.order_by(F.col("DATE"))).alias("PREV_DAY_CO2")
        )
        
        # Apply UDFs to create final DataFrame
        result_df = daily_df.select(
            F.col("DATE"),
            F.col("CO2_PPM"),
            F.col("PREV_DAY_CO2"),
            F.call_udf("ANALYTICS_CO2.CO2_DAILY_PERCENT_CHANGE", F.col("PREV_DAY_CO2"), F.col("CO2_PPM")).alias("DAILY_CHANGE"),
            F.call_udf("ANALYTICS_CO2.CALCULATE_CO2_VOLATILITY", F.col("CO2_PPM"), F.col("PREV_DAY_CO2")).alias("DAILY_VOLATILITY"),
            F.lit(min_co2).alias("MIN_CO2"),
            F.lit(max_co2).alias("MAX_CO2"),
            F.call_udf("ANALYTICS_CO2.NORMALIZE_CO2_UDF", F.col("CO2_PPM"), F.lit(min_co2), F.lit(max_co2)).alias("NORMALIZED_CO2"),
            F.current_timestamp().alias("META_UPDATED_AT")
        )
        
        # Merge the data into the target table
        target_df = session.table("ANALYTICS_CO2.DAILY_CO2_STATS")
        
        # Define update dictionary
        cols_to_update = {c: result_df[c] for c in result_df.schema.names if c != "MIN_CO2" and c != "MAX_CO2"}
        
        # Perform the merge operation
        target_df.merge(
            result_df,
            (target_df["DATE"] == result_df["DATE"]),
            [
                F.when_matched().update(cols_to_update),
                F.when_not_matched().insert(cols_to_update)
            ]
        )
        
        print("Daily CO2 metrics processed successfully")
        
    except Exception as e:
        print(f"Error processing daily metrics: {str(e)}")
        raise
    finally:
        # Scale warehouse back down
        if current_warehouse:
            session.sql(f"ALTER WAREHOUSE {current_warehouse} SET WAREHOUSE_SIZE = XSMALL").collect()

def process_weekly_metrics(session):
    """Process weekly metrics using Snowpark DataFrame API."""
    current_warehouse = session.get_current_warehouse()
    try:
        if current_warehouse:
            session.sql(f"ALTER WAREHOUSE {current_warehouse} SET WAREHOUSE_SIZE = LARGE WAIT_FOR_COMPLETION = TRUE").collect()
        
        print("Processing weekly CO2 metrics...")
        
        # First, try to access the min/max values (reuse the same code as in daily metrics)
        try:
            minmax_df = session.sql("SELECT * FROM _CO2_MINMAX").collect()
            min_co2 = minmax_df[0]["MIN_CO2"]
            max_co2 = minmax_df[0]["MAX_CO2"]
        except Exception as e:
            try:
                minmax_df = session.sql("SELECT * FROM HARMONIZED_CO2._CO2_MINMAX").collect()
                min_co2 = minmax_df[0]["MIN_CO2"]
                max_co2 = minmax_df[0]["MAX_CO2"]
            except Exception as e2:
                minmax_sql = session.sql("SELECT MIN(CO2_PPM) AS MIN_CO2, MAX(CO2_PPM) AS MAX_CO2 FROM HARMONIZED_CO2.HARMONIZED_CO2").collect()
                min_co2 = minmax_sql[0]["MIN_CO2"]
                max_co2 = minmax_sql[0]["MAX_CO2"]
                
        # Get source data
        source_df = session.table("HARMONIZED_CO2.HARMONIZED_CO2")
        
        # Create weekly aggregations
        weekly_df = source_df.group_by(
            F.date_trunc("WEEK", F.col("DATE")).alias("WEEK_START")
        ).agg(
            F.avg("CO2_PPM").alias("AVG_WEEKLY_CO2"),
            F.min("CO2_PPM").alias("WEEK_START_CO2"),
            F.max("CO2_PPM").alias("WEEK_END_CO2")
        )
        
        # Apply UDFs to create final DataFrame
        result_df = weekly_df.select(
            F.col("WEEK_START"),
            F.col("AVG_WEEKLY_CO2"),
            F.col("WEEK_START_CO2"),
            F.col("WEEK_END_CO2"),
            F.call_udf("ANALYTICS_CO2.CO2_DAILY_PERCENT_CHANGE", F.col("WEEK_START_CO2"), F.col("WEEK_END_CO2")).alias("WEEKLY_CHANGE"),
            F.call_udf("ANALYTICS_CO2.CALCULATE_CO2_VOLATILITY", F.col("WEEK_END_CO2"), F.col("WEEK_START_CO2")).alias("WEEKLY_VOLATILITY"),
            F.call_udf("ANALYTICS_CO2.NORMALIZE_CO2_UDF", F.col("AVG_WEEKLY_CO2"), F.lit(min_co2), F.lit(max_co2)).alias("NORMALIZED_WEEKLY_CO2"),
            F.current_timestamp().alias("META_UPDATED_AT")
        )
        
        # Merge the data into the target table
        target_df = session.table("ANALYTICS_CO2.WEEKLY_CO2_STATS")
        
        # Define update dictionary
        cols_to_update = {c: result_df[c] for c in result_df.schema.names}
        
        # Perform the merge operation
        target_df.merge(
            result_df,
            (target_df["WEEK_START"] == result_df["WEEK_START"]),
            [
                F.when_matched().update(cols_to_update),
                F.when_not_matched().insert(cols_to_update)
            ]
        )
        
        print("Weekly CO2 metrics processed successfully")
        
    except Exception as e:
        print(f"Error processing weekly metrics: {str(e)}")
        raise
    finally:
        # Scale warehouse back down
        if current_warehouse:
            session.sql(f"ALTER WAREHOUSE {current_warehouse} SET WAREHOUSE_SIZE = XSMALL").collect()

def create_analytics_tables(session: Session) -> str:
    """
    Creates analytics tables with essential metrics using Python and SQL UDFs.
    """
    try:
        # First check if tables already exist
        daily_exists = table_exists(session, schema='ANALYTICS_CO2', name='DAILY_CO2_STATS')
        weekly_exists = table_exists(session, schema='ANALYTICS_CO2', name='WEEKLY_CO2_STATS')
        
        # Create the tables if they don't exist
        if not daily_exists:
            create_daily_stats_table(session)
        if not weekly_exists:
            create_weekly_stats_table(session)
        
        # Process and merge the data
        process_daily_metrics(session)
        process_weekly_metrics(session)
        
        return "Analytics tables successfully processed"
    except Exception as e:
        print(f"Error creating analytics tables: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"Error: {str(e)}"
    
def main(session: Session) -> str:
    """Main function to be called by the stored procedure."""
    return create_analytics_tables(session)

if __name__ == "__main__":
    try:
        # Create a Snowpark session
        if is_running_in_snowflake:
            # In Snowflake, session is automatically available
            session = Session.get_active_session()
        else:
            # Locally, create a session using the connection profile
            session = Session.builder.config("connection_name", connection_name).getOrCreate()
        
        print(f"Connected to Snowflake using {connection_name} profile")
        print(f"Current database: {session.get_current_database()}")
        print(f"Current schema: {session.get_current_schema()}")
        print(f"Current warehouse: {session.get_current_warehouse()}")
        print(f"Current role: {session.get_current_role()}")

        result = main(session)
        print(result)
    except Exception as e:
        print(f"Error: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()
