import os
import sys
import argparse

def fix_udf_function(udf_path):
    """
    Check and fix the UDF function signature to work with Snowflake.
    This wraps a 2-parameter function to accept a single parameter from Snowflake.
    """
    function_file = os.path.join(udf_path, "function.py")
    
    if not os.path.exists(function_file):
        print(f"Error: Function file not found at: {function_file}")
        return False
    
    # Back up original file
    backup_file = function_file + ".bak"
    try:
        with open(function_file, 'r') as f:
            content = f.read()
        
        # Save backup
        with open(backup_file, 'w') as f:
            f.write(content)
            
        # Check if the function has a session parameter
        if "def main(session, input_data" in content:
            print("Found Snowpark UDF with session parameter")
            
            # Add wrapper function that gets session from snowflake.snowpark.functions
            modified_content = content.replace(
                "def main(session, input_data",
                """# Original function
def main_with_session(session, input_data

# Wrapper function that Snowflake calls directly - gets session and passes to original
def main(input_data"""
            )
            
            # Add the call to the original function at the end
            if "return" in modified_content:
                # If there's a return statement, add the wrapper call before the last return
                lines = modified_content.split("\n")
                for i in range(len(lines)-1, -1, -1):
                    if lines[i].strip().startswith("return"):
                        last_return_idx = i
                        break
                
                # Insert the wrapper call before the last return with proper indentation
                indent = lines[last_return_idx].split("return")[0]
                lines.insert(last_return_idx, f"{indent}# Get session from snowflake context")
                lines.insert(last_return_idx+1, f"{indent}from snowflake.snowpark.context import get_active_session")
                lines.insert(last_return_idx+2, f"{indent}session = get_active_session()")
                lines.insert(last_return_idx+3, f"{indent}# Call the original function with session")
                lines.insert(last_return_idx+4, f"{indent}return main_with_session(session, input_data)")
                
                modified_content = "\n".join(lines)
            else:
                # If no return, add it at the end
                modified_content += """
    # Get session from snowflake context
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
    # Call the original function with session
    return main_with_session(session, input_data)
"""
            
            # Write the modified content
            with open(function_file, 'w') as f:
                f.write(modified_content)
                
            print(f"✅ Updated {function_file} with wrapper function")
            print(f"Original file backed up to {backup_file}")
            return True
        else:
            print("No session parameter detected, no changes needed")
            return False
            
    except Exception as e:
        print(f"Error fixing UDF: {e}")
        # Try to restore backup if it exists
        if os.path.exists(backup_file):
            print("Restoring backup...")
            with open(backup_file, 'r') as f:
                original = f.read()
            with open(function_file, 'w') as f:
                f.write(original)
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fix UDF function for Snowflake compatibility')
    parser.add_argument('udf_path', help='Path to UDF directory containing function.py')
    
    args = parser.parse_args()
    fix_udf_function(args.udf_path)
