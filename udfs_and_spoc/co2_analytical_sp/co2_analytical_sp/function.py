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

def create_analytics_tables(session: Session) -> str:
    """
    Creates a simplified analytics table in the ANALYTICS_CO2 schema.
    This is a simpler version to avoid Snowflake execution errors.
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
        session.sql(f"ALTER WAREHOUSE {current_warehouse} SET WAREHOUSE_SIZE = LARGE WAIT_FOR_COMPLETION = TRUE").collect()
        print(f"Warehouse {current_warehouse} scaled up to LARGE")

        # Create a simpler daily analytics table without UDF calls initially
        print("Creating simplified daily analytics table...")
        daily_sql = f"""
        CREATE OR REPLACE TABLE ANALYTICS_CO2.DAILY_ANALYTICS AS
        SELECT
            DATE,
            YEAR,
            MONTH AS MONTH_NUM,
            DAY,
            ROUND(CO2_PPM, 3) AS CO2_PPM,
            DAYNAME(DATE) AS DAY_OF_WEEK,
            MONTHNAME(DATE) AS MONTH_NAME
        FROM HARMONIZED_CO2.HARMONIZED_CO2
        """
        session.sql(daily_sql).collect()
        print("Created DAILY_ANALYTICS table")

        # Try adding a derived column with simple calculations (no UDFs)
        print("Adding derived columns...")
        session.sql(f"""
        CREATE OR REPLACE TABLE ANALYTICS_CO2.DAILY_CO2_STATS AS
        SELECT
            DATE,
            CO2_PPM,
            LAG(CO2_PPM) OVER (ORDER BY DATE) AS PREV_DAY_CO2,
            CASE
                WHEN LAG(CO2_PPM) OVER (ORDER BY DATE) > 0 THEN
                    ROUND(((CO2_PPM - LAG(CO2_PPM) OVER (ORDER BY DATE)) / LAG(CO2_PPM) OVER (ORDER BY DATE)) * 100, 3)
                ELSE NULL
            END AS PERCENT_CHANGE
        FROM HARMONIZED_CO2.HARMONIZED_CO2
        """).collect()
        print("Created DAILY_CO2_STATS table")

        # Create a simple weekly summary table
        print("Creating weekly summary table...")
        session.sql(f"""
        CREATE OR REPLACE TABLE ANALYTICS_CO2.WEEKLY_CO2_SUMMARY AS
        SELECT
            DATE_TRUNC('WEEK', DATE) AS WEEK_START,
            AVG(CO2_PPM) AS AVG_WEEKLY_CO2,
            MIN(CO2_PPM) AS MIN_WEEKLY_CO2,
            MAX(CO2_PPM) AS MAX_WEEKLY_CO2,
            COUNT(*) AS MEASUREMENTS_PER_WEEK
        FROM HARMONIZED_CO2.HARMONIZED_CO2
        GROUP BY WEEK_START
        ORDER BY WEEK_START
        """).collect()
        print("Created WEEKLY_CO2_SUMMARY table")

        return "Simplified analytics tables created successfully."
    except Exception as e:
        print(f"Error creating analytics tables: {str(e)}")
        return f"Error: {str(e)}"
    finally:
        # Always scale warehouse back down, even if there's an error
        try:
            if current_warehouse:
                session.sql(f"ALTER WAREHOUSE {current_warehouse} SET WAREHOUSE_SIZE = XSMALL").collect()
                print(f"Warehouse {current_warehouse} scaled down to XSMALL")
        except Exception as scaling_error:
            print(f"Warning: Failed to scale down warehouse: {str(scaling_error)}")
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
