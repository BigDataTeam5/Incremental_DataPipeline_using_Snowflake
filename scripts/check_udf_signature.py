import os
import sys
import argparse

def check_udf_signature(udf_path):
    """Check the signature of a UDF function.py file."""
    function_file = os.path.join(udf_path, "function.py")
    
    if not os.path.exists(function_file):
        print(f"Error: Function file not found at: {function_file}")
        return False
    
    with open(function_file, 'r') as f:
        content = f.read()
    
    # Look for the main function definition
    if "def main(session, input_data" in content:
        print("✅ UDF uses Snowpark style with session and input_data parameters")
        print("Use this SQL:")
        print("""
        CREATE OR REPLACE FUNCTION UDF_NAME(input_data VARIANT)
        RETURNS VARIANT
        LANGUAGE PYTHON
        RUNTIME_VERSION=3.8
        PACKAGES = ('snowflake-snowpark-python')
        IMPORTS = ('@STAGE/path/to/zip')
        HANDLER = 'function.main'
        """)
        return True
        
    elif "def main(input_data" in content:
        print("✅ UDF uses basic style with just input_data parameter")
        print("Use this SQL:")
        print("""
        CREATE OR REPLACE FUNCTION UDF_NAME(input_data VARIANT)
        RETURNS VARIANT
        LANGUAGE PYTHON
        RUNTIME_VERSION=3.8
        PACKAGES = ('snowflake-snowpark-python')
        IMPORTS = ('@STAGE/path/to/zip')
        HANDLER = 'function.main'
        """)
        return True
    
    else:
        print("❌ Could not identify UDF signature pattern")
        print("Please check the function.py file manually")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Check UDF function signature')
    parser.add_argument('udf_path', help='Path to UDF directory')
    args = parser.parse_args()
    
    check_udf_signature(args.udf_path)
