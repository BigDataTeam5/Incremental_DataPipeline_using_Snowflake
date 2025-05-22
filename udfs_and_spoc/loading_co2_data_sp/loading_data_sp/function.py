import os
import io
import requests
import pandas as pd
from snowflake.snowpark import Session
import re

def fetch_co2_data_incremental(session, env):
    """
    Main function to incrementally fetch new CO2 data from NOAA,
    upload to S3 using Snowflake storage integration, and then
    load into Snowflake RAW_CO2 schema.
    
    Parameters:
    - session: Snowflake session
    - env: Environment name for warehouse sizing (e.g., 'dev', 'test', 'prod')
    
    Returns a success message.
    """
    # Using hardcoded values instead of environment variables
    S3_BUCKET_NAME = "noa-co2-datapipeline"
    PARENT_FOLDER = "noaa-co2-data"
    
    print("Initializing CO2 data pipeline...")
    print(f"Target S3 path: {PARENT_FOLDER}")
    print(f"Using environment: {env}")
    
    # Check table definition
    try:
        table_cols = session.sql("DESC TABLE RAW_CO2.CO2_DATA").collect()
        print("Table structure:")
        for col in table_cols[:5]:  # Just show first few columns
            print(f"  {col}")
    except Exception as e:
        print(f"Warning: Could not get table structure: {e}")
    
    # Step 1: Get the latest date from the RAW_CO2 schema
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
        return f"ERROR: Failed to get latest date from RAW_CO2.CO2_DATA: {str(e)}"
    
    # Step 2: Fetch the NOAA CO2 data, focusing only on current year
    print("Fetching CO2 data from NOAA (current year only)...")
    url = "https://oc62udov6d.execute-api.us-east-2.amazonaws.com/default/lambda_handler"

    try:
        # Get the current year
        import datetime
        current_year = datetime.datetime.now().year
        print(f"Focusing on data for current year: {current_year}")
        
        response = requests.get(url)
        if response.status_code != 200:
            return f"ERROR: Failed to fetch data from NOAA. Status code: {response.status_code}"
        
        print(f"Response type: {type(response.text)}")
        print(f"Response content preview: {response.text[:100]}")
        
        # Robust response parsing
        noaa_text = ""
        try:
            # Handle different response formats
            if "application/json" in response.headers.get('Content-Type', ''):
                print("Content-Type is JSON, parsing as JSON")
                try:
                    response_json = response.json()
                    # If body is a string, use it directly
                    if 'body' in response_json and isinstance(response_json['body'], str):
                        noaa_text = response_json['body']
                        print(f"Using body field from JSON (length: {len(noaa_text)})")
                    else:
                        # If we got JSON but no body field or not a string, use the whole response
                        noaa_text = str(response.text)
                        print("No body field in JSON or not a string, using raw response text")
                except Exception as json_error:
                    print(f"JSON parsing failed: {str(json_error)}, using raw text")
                    noaa_text = str(response.text)
            else:
                # If content type is not JSON, use the raw text
                print(f"Content-Type is not JSON: {response.headers.get('Content-Type', 'unknown')}")
                noaa_text = str(response.text)
                print("Using raw response text")
                
            print(f"Final noaa_text type: {type(noaa_text)}")
            print(f"Text preview: {noaa_text[:50]}...")
        except Exception as e:
            print(f"Fallback: Error parsing response: {str(e)}")
            # Ultimate fallback - use the raw text as a string
            noaa_text = str(response.text)
            print(f"Using raw text as fallback, length: {len(noaa_text)}")
        
        # Parse NOAA data, skipping comment lines
        lines = []
        try:
            if isinstance(noaa_text, str):
                lines = [line for line in noaa_text.split("\n") 
                        if line and not line.startswith("#") and line.strip()]
                print(f"Standard parsing found {len(lines)} data lines")
            else:
                print(f"Warning: noaa_text is not a string but {type(noaa_text)}")
                noaa_text = str(noaa_text)
                lines = [line for line in noaa_text.split("\n") 
                        if line and not line.startswith("#") and line.strip()]
        except Exception as parsing_error:
            print(f"Error in standard parsing: {parsing_error}")
        
        # FALLBACK: If standard parsing failed or returned too few lines, try regex
        if not lines or len(lines) < 10:
            print("Fallback to regex extraction of CO2 data")
            try:
                # Extract data lines with regex - matching lines with year, month, day pattern
                pattern = r"\s*(\d{4})\s+(\d{1,2})\s+(\d{1,2})\s+(\d{4}\.\d+)\s+(\d+\.\d+)"
                matches = re.findall(pattern, noaa_text)
                
                if matches:
                    print(f"Found {len(matches)} data lines using regex")
                    # Continue with the matched data
                    parsed_data = []
                    for match in matches:
                        year, month, day, decimal_date, co2_ppm = match
                        # Only keep data for current year and newer
                        if int(year) >= current_year:
                            parsed_data.append([year, month, day, decimal_date, co2_ppm])
                    print(f"After year filtering, have {len(parsed_data)} data points")
                else:
                    return "ERROR: Failed to extract CO2 data from response using regex"
            except Exception as regex_error:
                print(f"Error in regex parsing: {regex_error}")
                return f"ERROR: All parsing methods failed: {str(regex_error)}"
        else:
            # Parse into columns, only keeping current year data
            parsed_data = []
            for line in lines:
                try:
                    parts = line.split()
                    # Only take the columns we need and only for current year
                    if len(parts) >= 5 and int(parts[0]) >= current_year:
                        parsed_data.append(parts[:5])
                except Exception as line_error:
                    print(f"Error parsing line: {line_error} on line: {line[:50]}")
                    # Skip this line and continue
                    continue
            
            print(f"After parsing, found {len(parsed_data)} data points for current year+")
        
        # Create DataFrame (now with only current year data)
        columns = ["Year", "Month", "Day", "Decimal_Date", "CO2_ppm"]
        if not parsed_data:
            return "ERROR: No valid CO2 data found for the current year"
            
        df = pd.DataFrame(parsed_data, columns=columns)
        
        # Convert columns to appropriate types with error handling
        try:
            df["Year"] = df["Year"].astype(int)
            df["Month"] = df["Month"].astype(int)
            df["Day"] = df["Day"].astype(int)
            df["Decimal_Date"] = df["Decimal_Date"].astype(float)
            df["CO2_ppm"] = pd.to_numeric(df["CO2_ppm"], errors="coerce")
            
            # Create a date column for filtering
            df["Date"] = pd.to_datetime(df[["Year", "Month", "Day"]])
            
            print(f"Fetched {len(df)} records from NOAA for {current_year}")
        except Exception as conversion_error:
            print(f"Error converting data types: {str(conversion_error)}")
            print("DataFrame preview:")
            print(df.head())
            return f"ERROR: Failed to convert data types: {str(conversion_error)}"
            
        # Step 3: Filter for new records only
        if latest_date:
            df_new = df[df["Date"] > pd.to_datetime(latest_date)]
            print(f"Found {len(df_new)} new records since {latest_date}")
        else:
            df_new = df
            print(f"No existing data found. Will load all {len(df_new)} records")
        
        # If no new data, return early
        if df_new.empty:
            return "No new CO2 data to load. Database is up to date."

        # Scale up the warehouse for better performance if environment is provided
        if env:
            try:
                session.sql(f"ALTER WAREHOUSE co2_wh_{env} SET WAREHOUSE_SIZE = LARGE WAIT_FOR_COMPLETION = TRUE").collect()
                print(f"Scaled up warehouse co2_wh_{env} to LARGE")
            except Exception as e:
                print(f"Warning: Could not scale up warehouse: {e}")
        else:
            print("No environment specified, skipping warehouse scaling")
        
        try:
            # Step 4: Upload data to S3 using Snowflake storage integration
            print(f"Uploading {len(df_new)} new records to S3 using Snowflake stage...")
            
            # Create a user staging area if it doesn't exist
            try:
                session.sql("CREATE STAGE IF NOT EXISTS RAW_CO2.USER_TEMP_STAGE").collect()
                print("Created or confirmed user temporary stage")
            except Exception as e:
                print(f"Warning: Could not create temp stage: {e}")
            
            # Upload by year partitions
            years_loaded = []
            for year, year_df in df_new.groupby("Year"):
                # Drop the Date column as it's not in the original data
                upload_df = year_df.drop(columns=["Date"])
                
                # Convert to CSV
                csv_data = upload_df.to_csv(index=False)
                
                # File name for internal staging
                temp_file_name = f"co2_data_{year}.csv"
                
                # Stage path for external S3
                external_stage_path = f"{PARENT_FOLDER}/{year}/co2_daily_mlo.csv"
                
                try:
                    # Upload data to user stage first
                    session.file.put(csv_data, f"@RAW_CO2.USER_TEMP_STAGE/{temp_file_name}", overwrite=True)
                    print(f"Uploaded data to user stage: @RAW_CO2.USER_TEMP_STAGE/{temp_file_name}")
                    
                    # Copy from user stage to external S3 stage
                    copy_cmd = f"""
                    COPY INTO @EXTERNAL.NOAA_CO2_STAGE/{external_stage_path}
                    FROM @RAW_CO2.USER_TEMP_STAGE/{temp_file_name}
                    FILE_FORMAT = (TYPE = CSV)
                    OVERWRITE = TRUE
                    """
                    session.sql(copy_cmd).collect()
                    print(f"Copied to external S3 stage: {external_stage_path}")
                    
                    # Now COPY from S3 stage to RAW_CO2.CO2_DATA table
                    copy_to_table_cmd = f"""
                    COPY INTO RAW_CO2.CO2_DATA (YEAR, MONTH, DAY, DECIMAL_DATE, CO2_PPM)
                    FROM @EXTERNAL.NOAA_CO2_STAGE/{external_stage_path}
                    FILE_FORMAT = (
                        TYPE = CSV
                        FIELD_DELIMITER = ','
                        SKIP_HEADER = 1
                        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    )
                    ON_ERROR = CONTINUE
                    """
                    copy_result = session.sql(copy_to_table_cmd).collect()
                    print(f"Loaded data from S3 to Snowflake: {copy_result}")
                    years_loaded.append(str(year))
                    
                except Exception as stage_error:
                    print(f"Error in staging process: {stage_error}")
                    print("Falling back to direct DataFrame insertion...")
                    
                    # Get column names from the upload DataFrame
                    print(f"DataFrame columns: {upload_df.columns}")
                    
                    try:
                        # Convert to Snowpark DataFrame with explicit column mapping
                        # This ensures column names match exactly what Snowflake expects
                        snowpark_df = session.create_dataframe(upload_df)
                        
                        # Rename columns to match Snowflake's uppercase convention
                        snowpark_df = snowpark_df.to_df("YEAR", "MONTH", "DAY", "DECIMAL_DATE", "CO2_PPM")
                        
                        # Show the Snowpark DataFrame structure
                        print("Snowpark DataFrame schema:")
                        snowpark_df.printSchema()
                        
                        # Insert directly using SQL with VALUES
                        records = upload_df.to_records(index=False)
                        batch_size = 100  # Process in batches to avoid huge SQL statements
                        
                        for i in range(0, len(records), batch_size):
                            batch = records[i:i+batch_size]
                            values_str = []
                            
                            for row in batch:
                                # Format each value properly for SQL
                                year = int(row[0])
                                month = int(row[1])
                                day = int(row[2])
                                decimal_date = float(row[3])
                                co2_ppm = float(row[4])
                                
                                values_str.append(f"({year}, {month}, {day}, {decimal_date}, {co2_ppm})")
                            
                            # Join all value strings
                            all_values = ", ".join(values_str)
                            
                            # Execute the insert
                            insert_sql = f"""
                            INSERT INTO RAW_CO2.CO2_DATA (YEAR, MONTH, DAY, DECIMAL_DATE, CO2_PPM)
                            VALUES {all_values}
                            """
                            session.sql(insert_sql).collect()
                            print(f"Inserted batch of {len(batch)} rows directly")
                        
                        years_loaded.append(f"{year}(direct)")
                        
                    except Exception as insert_error:
                        print(f"Direct insert error: {insert_error}")
                        # Try even simpler approach - one row at a time
                        try:
                            print("Falling back to row-by-row insertion...")
                            inserted = 0
                            
                            for _, row in upload_df.iterrows():
                                try:
                                    # Format each value and handle potential type issues
                                    year = int(row['Year'])
                                    month = int(row['Month'])
                                    day = int(row['Day'])
                                    decimal_date = float(row['Decimal_Date'])
                                    co2_ppm = float(row['CO2_ppm'])
                                    
                                    insert_sql = f"""
                                    INSERT INTO RAW_CO2.CO2_DATA (YEAR, MONTH, DAY, DECIMAL_DATE, CO2_PPM)
                                    VALUES ({year}, {month}, {day}, {decimal_date}, {co2_ppm})
                                    """
                                    session.sql(insert_sql).collect()
                                    inserted += 1
                                except Exception as row_error:
                                    print(f"Error inserting row: {row_error}")
                            
                            print(f"Inserted {inserted} rows individually")
                            years_loaded.append(f"{year}(row-insert:{inserted})")
                            
                        except Exception as row_insert_error:
                            print(f"Row insertion error: {row_insert_error}")
                            return f"ERROR: Failed to load data using any method: {str(row_insert_error)}"            
            # Step 5: Verify the data was loaded and advance the stream
            count_sql = """
                SELECT COUNT(*) as NEW_ROWS FROM RAW_CO2.CO2_DATA_STREAM 
                WHERE METADATA$ACTION = 'INSERT'
            """
            count_result = session.sql(count_sql).collect()
            new_rows_count = count_result[0]["NEW_ROWS"] if count_result else 0
            
            # Move the stream consumption point forward (will be used by harmonized task)
            session.sql("SELECT SYSTEM$STREAM_HAS_DATA('RAW_CO2.CO2_DATA_STREAM')").collect()
            
            years_loaded_str = ", ".join(years_loaded)
            
            # Clean up temporary files
            try:
                session.sql("REMOVE @RAW_CO2.USER_TEMP_STAGE PATTERN='co2_data_.*'").collect()
                print("Cleaned up temporary files")
            except Exception as e:
                print(f"Warning: Could not clean up temp files: {e}")
            
            return f"Successfully loaded {new_rows_count} new CO2 records for years: {years_loaded_str}"
            
        except Exception as load_error:
            print(f"Error in data loading process: {load_error}")
            return f"ERROR: Failed to load data: {str(load_error)}"
        
        finally:
            # Always scale down the warehouse when done, even if there was an error
            if env:
                try:
                    session.sql(f"ALTER WAREHOUSE co2_wh_{env} SET WAREHOUSE_SIZE = XSMALL").collect()
                    print("Warehouse scaled back down to XSMALL")
                except Exception as scaling_error:
                    print(f"Warning: Failed to scale down warehouse: {scaling_error}")
            
    except Exception as e:
        print(f"Error in CO2 data pipeline: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"ERROR: CO2 data pipeline failed: {str(e)}"

def main(session):
    """Entry point function for the stored procedure."""    
    try:
        # Get from current warehouse name
        warehouse = session.get_current_warehouse()
        env = warehouse.lower().replace("co2_wh_", "")
        print(f"Detected environment from warehouse name: {env}")
    except Exception as e:
        print(f"Could not detect environment: {e}")
    print(f"Using Snowflake environment: {env}")
    return fetch_co2_data_incremental(session, env)

# This section only runs when testing locally, not in Snowflake
if __name__ == "__main__" and 'SNOWFLAKE_PYTHON_INTERPRETER' not in os.environ:
    print("This script is designed to run as a Snowflake stored procedure.")