import time
from snowflake.snowpark import Session
from dotenv import load_dotenv
import os

load_dotenv('.env')

connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE")
}

CO2_TABLES = ['co2_data']
TABLE_DICT = {
    "co2": {"schema": "RAW_CO2", "tables": CO2_TABLES}
}

# SNOWFLAKE ADVANTAGE: Schema detection
# SNOWFLAKE ADVANTAGE: Data ingestion with COPY
# SNOWFLAKE ADVANTAGE: Snowflake Tables (not file-based)

def load_raw_table(session, tname=None, s3dir=None, year=None, schema=None):
    """Load data from stage into raw tables"""
    session.use_schema(schema)
    
    # Adjust path to match actual structure: noaa-co2-data/YYYY/co2_daily_mlo.csv
    if year is None:
        location = "@EXTERNAL.NOAA_CO2_STAGE/"
    else:
        print('\tLoading year {}'.format(year)) 
        location = "@EXTERNAL.NOAA_CO2_STAGE/{}/".format(year)
    
    # Create table if it doesn't exist
    if not session.sql(f"SHOW TABLES LIKE '{tname}' IN SCHEMA {schema}").collect():
        print(f"Creating table {schema}.{tname}")
        session.sql(f"""
        CREATE TABLE {schema}.{tname} (
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
        print(f"Loading data from {location} into {schema}.{tname}")
        
        # Try direct COPY command instead of DataFrame
        copy_sql = f"""
        COPY INTO {schema}.{tname} (YEAR, MONTH, DAY, DECIMAL_DATE, CO2_PPM)
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
        print(f"Loaded data into {schema}.{tname}: {result}")
        
    except Exception as e:
        print(f"Error loading data: {e}")
        raise
    
    # Add comment to table
    comment_text = '''{"origin":"sf_sit-is","name":"co2_data_pipeline","version":{"major":1, "minor":0}}'''
    sql_command = f"""COMMENT ON TABLE {tname} IS '{comment_text}';"""
    session.sql(sql_command).collect()

def load_all_raw_tables(session):
    """Load all raw tables from stage"""
    wh_name = os.getenv("SNOWFLAKE_WAREHOUSE", "CO2_WH")
    _ = session.sql(f"ALTER WAREHOUSE {wh_name} SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()

    for s3dir, data in TABLE_DICT.items():
        tnames = data['tables']
        schema = data['schema']
        for tname in tnames:
            print(f"Loading {tname}")
            # Load data for all years from 1974 to 2019
            for year in range(1974, 2020):
                load_raw_table(session, tname=tname, s3dir=s3dir, year=year, schema=schema)

    _ = session.sql(f"ALTER WAREHOUSE {wh_name} SET WAREHOUSE_SIZE = XSMALL").collect()

def validate_raw_tables(session):
    """Validate loaded tables"""
    # Check column names from the inferred schema
    for tname in CO2_TABLES:
        print(f'{tname}: \n\t{session.table("RAW_CO2." + tname).columns}\n')
        # Display sample data
        print(f'Sample data:')
        print(session.sql(f"SELECT * FROM RAW_CO2.{tname} LIMIT 5").collect())

# For local debugging
if __name__ == "__main__":
    # Create a local Snowpark session
    with Session.builder.configs(connection_parameters).create() as session:
        try:
            load_all_raw_tables(session)
            validate_raw_tables(session)
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            print(f"Error type: {type(e)}")
            import traceback
            traceback.print_exc()