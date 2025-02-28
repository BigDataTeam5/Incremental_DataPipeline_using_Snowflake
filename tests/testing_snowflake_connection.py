import pytest
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from a .env file (if used locally)

@pytest.fixture
def snowflake_connection():
    """Fixture to establish a connection to Snowflake."""
    conn = None
    try:
        # Print the connection parameters for debugging
        print("Connecting to Snowflake with the following parameters:")
        print(f"User: {os.getenv('SNOWFLAKE_USER')}")
        print(f"Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
        print(f"Warehouse: {os.getenv('SNOWFLAKE_WAREHOUSE')}")
        print(f"Database: {os.getenv('SNOWFLAKE_DATABASE')}")
        print(f"Schema: {os.getenv('SNOWFLAKE_SCHEMA')}")

        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        yield conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")  # Print error for debugging
        yield None  # Yield None if connection fails
    finally:
        if conn is not None:
            conn.close()  # Close the connection only if it was created