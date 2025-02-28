# ---------------------------------------------------------------------
# Co2 Harmonized Stored Procedure 
# Script: co2_harmonized_sp.py
# ---------------------------------------------------------------------

import snowflake.snowpark.functions as F
from snowflake.snowpark import Session
import datetime

def table_exists(session, schema='', name=''):
    """Checks whether a specific table exists in INFORMATION_SCHEMA."""
    exists = session.sql(
        f"SELECT EXISTS ("
        f"SELECT * FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{name}') AS TABLE_EXISTS"
    ).collect()[0]['TABLE_EXISTS']
    return exists

def create_harmonized_table(session):
    """Creates the HARMONIZED_CO2.CO2_HARMONIZED table if not existing."""
    session.sql("""
        CREATE TABLE HARMONIZED_CO2.CO2_HARMONIZED (
            CO2_DATE DATE,
            CO2_PPM FLOAT,
            CO2_SITE STRING DEFAULT 'MAUNA_LOA',
            YEAR NUMBER(4,0),
            MONTH NUMBER(2,0),
            DAY NUMBER(2,0),
            DECIMAL_DATE FLOAT,
            VALIDATION_STATUS STRING,
            META_UPDATED_AT TIMESTAMP,
            META_ROW_ID STRING
        )
    """).collect()
    
    print("Created HARMONIZED_CO2.CO2_HARMONIZED table")

def validate_timestamps(session, source):
    """Validates timestamp data in the source."""
    # Get current date
    today = datetime.date.today()
    
    # Filter out future dates
    valid_dates = source.filter(
        (F.col("YEAR") < today.year) | 
        ((F.col("YEAR") == today.year) & (F.col("MONTH") < today.month)) |
        ((F.col("YEAR") == today.year) & (F.col("MONTH") == today.month) & 
         (F.col("DAY") <= today.day))
    )
    
    # Filter out dates before 1950 (early CO2 measurements)
    valid_dates = valid_dates.filter(F.col("YEAR") >= 1950)
    
    # Add validation status
    return valid_dates.withColumn(
        "VALIDATION_STATUS", 
        F.when(
            ((F.col("YEAR") > 1950) & 
            (F.col("YEAR") < today.year)) |
            ((F.col("YEAR") == today.year) & 
             (F.col("MONTH") <= today.month) &
             (F.col("DAY") <= today.day)), 
            "VALID"
        ).otherwise("INVALID_DATE")
    )

def validate_data_consistency(session, source):
    """Validates CO2 measurements are within reasonable ranges."""
    # CO2 should be within reasonable range (pre-industrial ~280ppm, current ~420ppm)
    # Allow some margin for historical data and future increase
    return source.withColumn(
        "VALIDATION_STATUS",
        F.when((F.col("CO2_PPM").between(200, 500)) & 
               (F.col("VALIDATION_STATUS") == "VALID"), 
               "VALID")
        .when(F.col("VALIDATION_STATUS") == "INVALID_DATE", "INVALID_DATE")
        .otherwise("INVALID_CO2_VALUE")
    )

def check_duplicates(session, source):
    """Checks for and flags duplicate dates in the source data."""
    # Count occurrences of each date
    date_counts = source.groupBy(
        F.col("YEAR"), 
        F.col("MONTH"), 
        F.col("DAY")
    ).count()
    
    # Join back to flag duplicates
    source_with_counts = source.join(
        date_counts,
        on=["YEAR", "MONTH", "DAY"],
        how="left"
    )
    
    # Flag duplicates in validation status
    return source_with_counts.withColumn(
        "VALIDATION_STATUS",
        F.when((F.col("count") > 1) & 
               (F.col("VALIDATION_STATUS") == "VALID"), 
               "DUPLICATE_DATE")
        .otherwise(F.col("VALIDATION_STATUS"))
    ).drop("count")

def merge_raw_into_harmonized(session):
    """Merges new data from RAW_CO2.co2_data_stream into HARMONIZED_CO2.CO2_HARMONIZED."""
    try:
        # Get source data from stream
        source = session.table("RAW_CO2.CO2_DATA_STREAM")
        
        # First create a clean dataframe with just the columns we need
        source = source.select(
            "YEAR", "MONTH", "DAY", "DECIMAL_DATE", "CO2_PPM", 
            "METADATA$ACTION", "METADATA$ISUPDATE", "METADATA$ROW_ID"
        )
        
        # Filter for INSERT and UPDATE actions only
        source = source.filter(F.col("METADATA$ACTION").isin(["INSERT", "UPDATE"]))
        
        # Create CO2_DATE column
        source = source.withColumn(
            "CO2_DATE", 
            F.to_date(F.concat(
                F.lpad(F.col("YEAR").cast("string"), 4, '0'), 
                F.lit('-'),
                F.lpad(F.col("MONTH").cast("string"), 2, '0'),
                F.lit('-'),
                F.lpad(F.col("DAY").cast("string"), 2, '0')
            ))
        )
        
        # Apply validations in sequence (no intermediate filters)
        source = validate_timestamps(session, source)
        source = validate_data_consistency(session, source)
        source = check_duplicates(session, source)
        
        # Now we can safely filter for valid records
        valid_source = source.filter(F.col("VALIDATION_STATUS") == "VALID")
    
        # Create or get target table
        if not table_exists(session, schema='HARMONIZED_CO2', name='CO2_HARMONIZED'):
            create_harmonized_table(session)
        
        target = session.table("HARMONIZED_CO2.CO2_HARMONIZED")

        # Build dict of columns to update (excluding CO2_DATE if that is our match key)
        columns_to_update = {
            "CO2_PPM": valid_source["CO2_PPM"],
            "DECIMAL_DATE": valid_source["DECIMAL_DATE"],
            "CO2_SITE": F.lit("MAUNA_LOA"),
            "YEAR": valid_source["YEAR"],
            "MONTH": valid_source["MONTH"],
            "DAY": valid_source["DAY"],
            "VALIDATION_STATUS": valid_source["VALIDATION_STATUS"],
            "META_UPDATED_AT": F.current_timestamp(),
            "META_ROW_ID": valid_source["METADATA$ROW_ID"]
        }

        # Perform the merge
        merge_result = target.merge(
            source=valid_source,
            condition=target["CO2_DATE"] == valid_source["CO2_DATE"],
            clauses=[
                F.when_matched().update(columns_to_update),
                F.when_not_matched().insert({
                    "CO2_DATE": valid_source["CO2_DATE"],
                    **columns_to_update
                })
            ]
        )
        
        # Log invalid records for review (optional)
        invalid_records = source.filter(F.col("VALIDATION_STATUS") != "VALID")
        invalid_count = invalid_records.count()
        
        # Get stream consumption status (needed to advance stream pointer)
        session.sql("SELECT SYSTEM$STREAM_HAS_DATA('RAW_CO2.CO2_DATA_STREAM')").collect()
        
        # Get actual counts for reporting
        rows_processed = valid_source.count()
        rows_inserted = sum(1 for row in merge_result.collect() if row['ROWS_INSERTED'] > 0)
        rows_updated = sum(1 for row in merge_result.collect() if row['ROWS_UPDATED'] > 0)
        
        return {
            "valid_records_processed": rows_processed,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "invalid_records_skipped": invalid_count
        }
        
    except Exception as e:
        # Log the error
        print(f"Error in merge_raw_into_harmonized: {str(e)}")
        # You could log this to a Snowflake table for better tracking
        
        # Return error information
        return {
            "valid_records_processed": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "invalid_records_skipped": 0,
            "error": str(e)
        }
    
def main(session: Session) -> str:
    """Main function for the stored procedure: ensures table exists, merges new data."""
    # Process the data
    result = merge_raw_into_harmonized(session)
    
    return (f"CO2_HARMONIZED_SP: Raw → Harmonized merge complete! "
            f"Valid records: {result['valid_records_processed']} "
            f"(Inserted: {result['rows_inserted']}, Updated: {result['rows_updated']}), "
            f"Invalid/skipped: {result['invalid_records_skipped']}")

if __name__ == "__main__":
    # Local debugging with an ad-hoc Snowpark Session
    with Session.builder.getOrCreate() as session:
        print(main(session))  # type: ignore