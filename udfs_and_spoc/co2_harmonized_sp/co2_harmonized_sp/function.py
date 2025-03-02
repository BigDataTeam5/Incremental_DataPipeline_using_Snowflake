import os
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from dotenv import load_dotenv

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
        session.sql("ALTER WAREHOUSE CO2_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()

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

        # Scale warehouse back down after merge
        session.sql("ALTER WAREHOUSE CO2_WH SET WAREHOUSE_SIZE = XSMALL").collect()
        print("Merge operation completed successfully.")
        return True
    except Exception as e:
        print("Error during merge operation:", e)
        return False

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
        
        # Run the merge process for harmonized data
        result = main(session)
        print(result)
