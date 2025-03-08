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
    current_warehouse = None
    try:
        # Get current warehouse from session for dynamic operations
        current_warehouse = session.get_current_warehouse()
        if not current_warehouse:
            print("Warning: No current warehouse available in session.")
            # Use environment-based warehouse as fallback if current isn't available
            if env:
                current_warehouse = f"co2_wh_{env}"
                print(f"Using fallback warehouse: {current_warehouse}")
            else:
                raise ValueError("No warehouse specified and environment variable not set")
        
        print(f"Using warehouse: {current_warehouse}")
        
        # Scale warehouse up for performance
        session.sql(f"ALTER WAREHOUSE {current_warehouse} SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()
        print(f"Warehouse {current_warehouse} scaled up to XLARGE")

        # Check stream data availability
        stream_count = session.sql("SELECT COUNT(*) FROM RAW_CO2.CO2_DATA_STREAM").collect()[0][0]
        print(f"Records in stream: {stream_count}")
        
        if stream_count == 0:
            print("Stream is empty - nothing to process")
            return True

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
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Always scale warehouse back down, even if there's an error
        try:
            if current_warehouse:
                session.sql(f"ALTER WAREHOUSE {current_warehouse} SET WAREHOUSE_SIZE = XSMALL").collect()
                print(f"Warehouse {current_warehouse} scaled down to XSMALL")
        except Exception as scaling_error:
            print(f"Warning: Failed to scale down warehouse: {scaling_error}")

def main(session: Session) -> str:
    """Main function for the stored procedure: ensures the target table exists, then merges new data."""
    try:
        print(f"Session info: DB={session.get_current_database()}, SCHEMA={session.get_current_schema()}, WAREHOUSE={session.get_current_warehouse()}, ROLE={session.get_current_role()}")
        
        if not table_exists(session, schema='HARMONIZED_CO2', name='harmonized_co2'):
            create_harmonized_table(session)

        success = merge_raw_into_harmonized(session)
        if success:
            return "CO2_HARMONIZED_SP: Raw → Harmonized merge complete!"
        else:
            return "CO2_HARMONIZED_SP: Error during merge operation"
    except Exception as e:
        import traceback
        tb_str = traceback.format_exc()
        print(f"Error in main: {str(e)}\n{tb_str}")
        return f"CO2_HARMONIZED_SP: Error in main function: {str(e)}"

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