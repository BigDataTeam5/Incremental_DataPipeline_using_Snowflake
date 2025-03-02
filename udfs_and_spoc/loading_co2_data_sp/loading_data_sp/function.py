import os
import io
import requests
import pandas as pd
import boto3
from snowflake.snowpark import Session
from dotenv import load_dotenv

def fetch_co2_data_incremental(session):
    """
    Main function to incrementally fetch new CO2 data from NOAA, upload to S3,
    and load into Snowflake RAW_CO2 schema.
    
    Returns a success message.
    """
    # Step 1: Get environment variables
    load_dotenv()
    
    # Get AWS credentials from environment
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
    PARENT_FOLDER = os.getenv("PARENT_FOLDER", "noaa-co2-data")
    AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
    
    print("Initializing CO2 data pipeline...")
    print(f"Target S3 bucket: {S3_BUCKET_NAME}/{PARENT_FOLDER}")
    
    # Step 2: Get the latest date from the RAW_CO2 schema
    print("Finding latest date in RAW_CO2.CO2_DATA...")
    
    try:
        latest_date_result = session.sql("""
            SELECT 
                MAX(TO_DATE(CONCAT(
                    LPAD(YEAR::STRING, 4, '0'), '-',
                    LPAD(MONTH::STRING, 2, '0'), '-',
                    LPAD(DAY::STRING, 2, '0')))
                ) AS MAX_DATE 
            FROM RAW_CO2.CO2_DATA
        """).collect()
        
        latest_date = latest_date_result[0]["MAX_DATE"] if latest_date_result[0]["MAX_DATE"] else None
        print(f"Latest date in RAW_CO2.CO2_DATA: {latest_date}")
        
    except Exception as e:
        print(f"Error getting latest date: {str(e)}")
        # Return early with descriptive error
        return f"ERROR: Failed to get latest date from RAW_CO2.CO2_DATA: {str(e)}"
    
    # Step 3: Fetch the NOAA CO2 data, focusing only on current year
    print("Fetching CO2 data from NOAA (current year only)...")
    url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.txt"

    try:
        # Get the current year
        import datetime
        current_year = datetime.datetime.now().year
        print(f"Focusing on data for current year: {current_year}")
        
        response = requests.get(url)
        if response.status_code != 200:
            return f"ERROR: Failed to fetch data from NOAA. Status code: {response.status_code}"
        
        # Parse NOAA data, skipping comment lines
        lines = [line for line in response.text.split("\n") 
                if not line.startswith("#") and line.strip()]
        
        # Parse into columns, only keeping current year data
        parsed_data = []
        for line in lines:
            parts = line.split()
            # Only take the columns we need and only for current year
            if len(parts) >= 5 and int(parts[0]) >= current_year:
                parsed_data.append(parts[:5])
        
        # Create DataFrame (now with only current year data)
        columns = ["Year", "Month", "Day", "Decimal_Date", "CO2_ppm"]
        df = pd.DataFrame(parsed_data, columns=columns)
        
        # Convert columns to appropriate types
        df["Year"] = df["Year"].astype(int)
        df["Month"] = df["Month"].astype(int)
        df["Day"] = df["Day"].astype(int)
        df["Decimal_Date"] = df["Decimal_Date"].astype(float)
        df["CO2_ppm"] = pd.to_numeric(df["CO2_ppm"], errors="coerce")
        
        # Create a date column for filtering
        df["Date"] = pd.to_datetime(df[["Year", "Month", "Day"]])
        
        print(f"Fetched {len(df)} records from NOAA for {current_year}")        
        # Step 4: Filter for new records only
        if latest_date:
            df_new = df[df["Date"] > pd.to_datetime(latest_date)]
            print(f"Found {len(df_new)} new records since {latest_date}")
        else:
            df_new = df
            print(f"No existing data found. Will load all {len(df_new)} records")
        
        # If no new data, return early
        if df_new.empty:
            return "No new CO2 data to load. Database is up to date."
            
        # Step 5: Upload new data to S3
        print(f"Uploading {len(df_new)} new records to S3...")
        
        # Initialize S3 client
        s3_client = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        
        # Upload by year partitions
        for year, year_df in df_new.groupby("Year"):
            # Drop the Date column as it's not in the original data
            upload_df = year_df.drop(columns=["Date"])
            
            csv_buffer = io.StringIO()
            upload_df.to_csv(csv_buffer, index=False)
            
            s3_object_name = f"{PARENT_FOLDER}/{year}/co2_daily_mlo.csv"
            
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_object_name,
                Body=csv_buffer.getvalue()
            )
            print(f"Uploaded s3://{S3_BUCKET_NAME}/{s3_object_name}")
        
        # Step 6: Use COPY to load from S3 into Snowflake
        print("Loading new data from S3 into Snowflake...")
        
        # Scale up the warehouse for better performance
        session.sql("ALTER WAREHOUSE CO2_WH SET WAREHOUSE_SIZE = LARGE WAIT_FOR_COMPLETION = TRUE").collect()
        
        # For each year, COPY from the corresponding partition
        years_loaded = []
        for year in df_new["Year"].unique():
            years_loaded.append(str(year))
            copy_sql = f"""
            COPY INTO RAW_CO2.CO2_DATA (YEAR, MONTH, DAY, DECIMAL_DATE, CO2_PPM)
            FROM (
                SELECT 
                    $1, $2, $3, $4, $5
                FROM @EXTERNAL.NOAA_CO2_STAGE/{PARENT_FOLDER}/{year}/
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
            print(f"Loaded data for year {year}: {result}")
            
        # Scale down the warehouse when done
        session.sql("ALTER WAREHOUSE CO2_WH SET WAREHOUSE_SIZE = XSMALL").collect()
        
        # Step 7: Verify the data was loaded and advance the stream
        count_sql = """
            SELECT COUNT(*) as NEW_ROWS FROM RAW_CO2.CO2_DATA_STREAM 
            WHERE METADATA$ACTION = 'INSERT'
        """
        count_result = session.sql(count_sql).collect()
        new_rows_count = count_result[0]["NEW_ROWS"] if count_result else 0
        
        # Move the stream consumption point forward (will be used by harmonized task)
        session.sql("SELECT SYSTEM$STREAM_HAS_DATA('RAW_CO2.CO2_DATA_STREAM')").collect()
        
        years_loaded_str = ", ".join(years_loaded)
        return f"Successfully loaded {new_rows_count} new CO2 records for years: {years_loaded_str}"
        
    except Exception as e:
        print(f"Error in CO2 data pipeline: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"ERROR: CO2 data pipeline failed: {str(e)}"

def main(session):
    """Entry point function for the stored procedure."""
    return fetch_co2_data_incremental(session)

# For local testing only
if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Use connection name from environment or default to "dev"
    connection_name = os.getenv("SNOWFLAKE_CONNECTION", "dev")
    print(f"Using Snowflake connection profile: {connection_name}")
    
    # Create a Snowpark session using the connection profile
    with Session.builder.config("connection_name", connection_name).getOrCreate() as session:
        print(f"Connected to Snowflake using {connection_name} profile")
        print(f"Database: {session.get_current_database()}")
        print(f"Schema: {session.get_current_schema()}")
        print(f"Warehouse: {session.get_current_warehouse()}")
        
        result = fetch_co2_data_incremental(session)
        print(f"Result: {result}")