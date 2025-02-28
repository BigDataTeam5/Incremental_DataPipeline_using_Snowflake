import os
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from dotenv import load_dotenv
import json
import sys

# Determine if we're running in Snowflake or locally
is_running_in_snowflake = 'SNOWFLAKE_PYTHON_INTERPRETER' in os.environ

# Environment setup - only do file operations when running locally
if not is_running_in_snowflake:
    # Get the project root directory (assuming the function.py is in udfs_and_spoc/co2_harmonized_sp/co2_harmonized_sp/)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "..", "..", ".."))

    # Load environment variables from .env file
    env_file = os.path.join(project_root, '.env')
    load_dotenv(env_file)

    # Use the specified environment or default to "dev"
    env = os.getenv("ENV", "dev").lower()
    print(f"Using environment: {env}")

    # Load environment configuration from JSON file
    try:
        env_json_path = os.path.join(project_root, "templates", "environment.json")
        print(f"Looking for environment.json at: {env_json_path}")
        
        if os.path.exists(env_json_path):
            with open(env_json_path, "r") as json_file:
                env_config = json.load(json_file)
            env = env_config.get("environment", env)
            print(f"Loaded environment from file: {env}")
    except Exception as e:
        print(f"Error loading environment.json: {e}")
        print("Continuing with default environment")
else:
    # When running in Snowflake, use the environment from the connection or default to dev
    env = os.getenv("SNOWFLAKE_ENV", "dev").lower()
    print(f"Running in Snowflake with environment: {env}")

# Use the environment to establish Snowflake connection
connection_name = env
print(f"Using Snowflake connection profile: {connection_name}")

def table_exists(session: Session, schema: str, name: str) -> bool:
    """
    Check if a table exists in the specified schema.
    """
    try:
        result = session.sql(f"SHOW TABLES LIKE '{name}' IN SCHEMA {schema}").collect()
        return len(result) > 0
    except Exception as e:
        print("Error checking table existence:", e)
        return False

def create_harmonized_table(session: Session):
    """
    Creates the target table (harmonized_co2) in the HARMONIZED_CO2 schema.
    The table includes a META_UPDATED_AT column to record merge timestamps.
    """
    create_table_sql = """
    CREATE TABLE HARMONIZED_CO2.harmonized_co2 (
        DATE DATE,
        YEAR NUMBER,
        MONTH NUMBER,
        DAY NUMBER,
        CO2_PPM FLOAT,
        META_UPDATED_AT TIMESTAMP_NTZ
    )
    """
    session.sql(create_table_sql).collect()
    print("Table harmonized_co2 created in schema HARMONIZED_CO2.")

def merge_raw_into_harmonized(session: Session) -> bool:
    """
    Merges new records from the existing stream (CO2_DATA_STREAM) into the harmonized_co2 table.
    This function scales the warehouse dynamically, computes a DATE column from YEAR, MONTH, and DAY,
    and adds the current timestamp to META_UPDATED_AT during the merge.
    """
    try:
        # Scale warehouse up for performance
        session.sql(f"ALTER WAREHOUSE co2_wh_{env} SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()

        # Load the source stream and compute the DATE column
        source_df = session.table("RAW_CO2.CO2_DATA_STREAM") \
            .with_column("DATE", F.to_date(F.concat_ws(F.lit("-"), F.col("YEAR").cast("string"),
                                                           F.col("MONTH").cast("string"),
                                                           F.col("DAY").cast("string")), "YYYY-MM-DD"))

        # Load the target harmonized table
        target_df = session.table("HARMONIZED_CO2.harmonized_co2")

        # Build the update dictionary: update columns and add META_UPDATED_AT timestamp
        updates = {
            "DATE": source_df["DATE"],
            "YEAR": source_df["YEAR"],
            "MONTH": source_df["MONTH"],
            "DAY": source_df["DAY"],
            "CO2_PPM": source_df["CO2_PPM"],
            "META_UPDATED_AT": F.current_timestamp()
        }

        # Perform the merge based on the computed DATE
        target_df.merge(
            source_df,
            target_df["DATE"] == source_df["DATE"],
            [
                F.when_matched().update(updates),
                F.when_not_matched().insert(updates)
            ]
        )

        print("Merge operation completed successfully.")
        return True
    except Exception as e:
        print("Error during merge operation:", e)
        return False
    finally:
        # Always scale warehouse back down, even if there's an error
        try:
            session.sql(f"ALTER WAREHOUSE co2_wh_{env} SET WAREHOUSE_SIZE = XSMALL").collect()
            print("Warehouse scaled back down to XSMALL")
        except Exception as scaling_error:
            print(f"Warning: Failed to scale down warehouse: {scaling_error}")

def main(session: Session) -> str:
    """Main function for the stored procedure: ensures the target table exists, then merges new data."""
    if not table_exists(session, schema='HARMONIZED_CO2', name='harmonized_co2'):
        create_harmonized_table(session)

    success = merge_raw_into_harmonized(session)
    if success:
        return "CO2_HARMONIZED_SP: Raw → Harmonized merge complete!"
    else:
        return "CO2_HARMONIZED_SP: Error during merge operation"

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

        # Run the merge process for harmonized data
        result = main(session)
        print(result)
    except Exception as e:
        print(f"Error: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()
