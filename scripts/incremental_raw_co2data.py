import time
from snowflake.snowpark import Session
from dotenv import load_dotenv
import os
import json

# Load environment variables from .env file (for AWS credentials only, not for env)
load_dotenv('.env')

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))
# Go up one level to reach the project root
project_root = os.path.dirname(current_dir)
# Build the path to environment.json
env_file_path = os.path.join(project_root, "templates", "environment.json")

# Load environment configuration ONLY from JSON file
try:
    with open(env_file_path, "r") as json_file:
        env_config = json.load(json_file)
    # Use environment from JSON file
    connection_name = env_config.get("environment", "").lower()
    print(f"Loaded environment from file: {connection_name}")
except FileNotFoundError:
    print(f"ERROR: Configuration file not found at {env_file_path}")
    print("Please ensure the templates directory exists and contains environment.json")
    connection_name = ""
    print("Using empty connection name due to missing configuration")
except Exception as e:
    print(f"Error loading configuration: {e}")
    connection_name = ""
    print("Using empty connection name due to error")

# Print connection info for debugging
print(f"Using Snowflake connection profile: {connection_name}")

# Define constants
DEFAULT_SCHEMA = "RAW_CO2"
CO2_TABLES = ['co2_data']
TABLE_DICT = {
    "co2": {"tables": CO2_TABLES}
}

# SNOWFLAKE ADVANTAGE: Schema detection
# SNOWFLAKE ADVANTAGE: Data ingestion with COPY
# SNOWFLAKE ADVANTAGE: Snowflake Tables (not file-based)

def load_raw_table(session, tname=None, s3dir=None, year=None):
    """Load data from stage into raw tables"""
    session.use_schema(DEFAULT_SCHEMA)
    # Adjust path to match actual structure: noaa-co2-data/YYYY/co2_daily_mlo.csv
    if year is None:
        location = "@EXTERNAL.NOAA_CO2_STAGE/"
    else:
        print('\tLoading year {}'.format(year)) 
        location = "@EXTERNAL.NOAA_CO2_STAGE/{}/".format(year)
    
    # Create table if it doesn't exist
    if not session.sql(f"SHOW TABLES LIKE '{tname}' IN SCHEMA {DEFAULT_SCHEMA}").collect():
        print(f"Creating table {DEFAULT_SCHEMA}.{tname}")
        session.sql(f"""
        CREATE TABLE {DEFAULT_SCHEMA}.{tname} (
            YEAR NUMBER(4,0),
            MONTH NUMBER(2,0),
            DAY NUMBER(2,0),
            DECIMAL_DATE FLOAT,
            CO2_PPM FLOAT
        )
        """).collect()
    
    # Try loading with explicit schema and without compression
    try:
        # Define the schema explicitly
        print(f"Loading data from {location} into {DEFAULT_SCHEMA}.{tname}")
        
        # Try direct COPY command instead of DataFrame
        copy_sql = f"""
        COPY INTO {DEFAULT_SCHEMA}.{tname} (YEAR, MONTH, DAY, DECIMAL_DATE, CO2_PPM)
        FROM (
            SELECT 
                $1, $2, $3, $4, $5
            FROM {location}
        )
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )
        
        PATTERN = '.*co2_daily_mlo\\.csv'
        ON_ERROR = CONTINUE
        """
        
        result = session.sql(copy_sql).collect()
        print(f"Loaded data into {DEFAULT_SCHEMA}.{tname}: {result}")
        
    except Exception as e:
        print(f"Error loading data: {e}")
        raise
    
    # Add comment to table
    comment_text = '''{"origin":"sf_sit-is","name":"co2_data_pipeline","version":{"major":1, "minor":0}}'''
    sql_command = f"""COMMENT ON TABLE {tname} IS '{comment_text}';"""
    session.sql(sql_command).collect()

def load_all_raw_tables(session):
    """Load all raw tables from stage"""
    # Use the current warehouse from the session instead of constructing name
    current_warehouse = session.get_current_warehouse()
    if not current_warehouse:
        print("Error: No current warehouse set in session")
        return
        
    print(f"Using warehouse: {current_warehouse}")
    
    try:
        # Scale up the warehouse for better performance
        session.sql(f"ALTER WAREHOUSE {current_warehouse} SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()
        print(f"Warehouse {current_warehouse} scaled up to XLARGE")
        
        # Process all tables
        for s3dir, data in TABLE_DICT.items():
            tnames = data['tables']
            for tname in tnames:
                print(f"Loading {tname}")
                # Load data for specified years
                for year in range(2020, 2026):
                    try:
                        load_raw_table(session, tname=tname, s3dir=s3dir, year=year)
                    except Exception as e:
                        print(f"Error loading data for year {year}: {e}")
                        # Continue with next year instead of failing completely
    except Exception as e:
        print(f"Error during raw data loading: {e}")
        raise
    finally:
        # Always scale down the warehouse when done, even if there was an error
        try:
            session.sql(f"ALTER WAREHOUSE {current_warehouse} SET WAREHOUSE_SIZE = XSMALL").collect()
            print(f"Warehouse {current_warehouse} scaled down to XSMALL")
        except Exception as scaling_error:
            print(f"Warning: Failed to scale down warehouse: {scaling_error}")

def validate_raw_tables(session):
    """Validate loaded tables"""
    # Check column names from the inferred schema
    for tname in CO2_TABLES:
        print(f'{tname}: \n\t{session.table(f"{DEFAULT_SCHEMA}." + tname).columns}\n')
        # Display sample data
        print(f'Sample data:')
        print(session.sql(f"SELECT * FROM {DEFAULT_SCHEMA}.{tname} LIMIT 5").collect())

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
            
            # Set default schema
            session.use_schema(DEFAULT_SCHEMA)
            print(f"Set active schema to: {DEFAULT_SCHEMA}")
            
            load_all_raw_tables(session)
            validate_raw_tables(session)
    except Exception as e:
        print(f"Error: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()