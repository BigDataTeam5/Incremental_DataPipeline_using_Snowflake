import os
import sys
import logging
import toml
import zipfile
import tempfile
import snowflake.connector
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def zip_directory(source_dir, zip_path):
    """Create a zip file from a directory."""
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, source_dir)
                zipf.write(file_path, arcname)

def get_connection_config(profile_name):
    """Get connection details from the connections.toml file."""
    config_path = os.path.expanduser("~/.snowflake/connections.toml")
    
    if not os.path.exists(config_path):
        logger.error(f"Connection file not found: {config_path}")
        return None
    
    try:
        with open(config_path, 'r') as f:
            config = toml.load(f)
        
        # Log available profiles to help with debugging
        logger.info(f"Available profiles in connections.toml: {list(config.keys())}")
        
        # Try multiple possible variations of the profile name
        possible_names = [
            profile_name,                  # Standard name: [dev] or [prod]
            f"connections.{profile_name}",  # With prefix: [connections.dev] or [connections.prod]
        ]
        
        # Also try without prefix if it has one
        if profile_name.startswith("connections."):
            possible_names.append(profile_name[11:])
        
        # Try each possible profile name
        for name in possible_names:
            if name in config:
                logger.info(f"Found profile '{name}' in config file")
                return config[name]
        
        # If we get here, no profile match was found
        logger.error(f"Profile '{profile_name}' not found in config file. Available profiles: {list(config.keys())}")
        return None
    
    except Exception as e:
        logger.error(f"Error reading connection config: {str(e)}")
        return None

def create_snowflake_connection(conn_config):
    """Create a Snowflake connection from configuration."""
    try:
        # Check if we're using key pair authentication
        if 'private_key_path' in conn_config:
            key_path = conn_config['private_key_path']
            logger.info(f"Using private key from: {key_path}")
            
            # Read the private key properly
            try:
                from cryptography.hazmat.backends import default_backend
                from cryptography.hazmat.primitives import serialization
                
                # Log file stats to debug issues
                if os.path.exists(key_path):
                    logger.info(f"Private key file exists: {os.path.getsize(key_path)} bytes")
                else:
                    logger.error(f"Private key file not found: {key_path}")
                    raise FileNotFoundError(f"Private key file not found: {key_path}")
                
                # Read key file
                with open(key_path, "rb") as key_file:
                    key_data = key_file.read()
                    
                # Try to load the private key
                try:
                    p_key = serialization.load_pem_private_key(
                        key_data,
                        password=None,
                        backend=default_backend()
                    )
                    
                    # Convert to DER format as required by Snowflake
                    pkb = p_key.private_bytes(
                        encoding=serialization.Encoding.DER,
                        format=serialization.PrivateFormat.PKCS8,
                        encryption_algorithm=serialization.NoEncryption()
                    )
                    
                    logger.info("Private key loaded and converted successfully")
                    
                    # Connect with private key
                    return snowflake.connector.connect(
                        account=conn_config.get('account'),
                        user=conn_config.get('user'),
                        private_key=pkb,
                        warehouse=conn_config.get('warehouse'),
                        database=conn_config.get('database'),
                        schema=conn_config.get('schema'),
                        role=conn_config.get('role')
                    )
                except Exception as e:
                    logger.error(f"Error loading private key: {str(e)}")
                    
                    # Fall back to password if available
                    if 'password' in conn_config:
                        logger.info("Falling back to password authentication")
                        return snowflake.connector.connect(
                            account=conn_config.get('account'),
                            user=conn_config.get('user'),
                            password=conn_config.get('password'),
                            warehouse=conn_config.get('warehouse'),
                            database=conn_config.get('database'),
                            schema=conn_config.get('schema'),
                            role=conn_config.get('role'),
                            authenticator=conn_config.get('authenticator', 'snowflake')
                        )
                    else:
                        raise
            except Exception as e:
                logger.error(f"Error processing private key: {str(e)}")
                raise
        else:
            # Connect with password
            return snowflake.connector.connect(
                account=conn_config.get('account'),
                user=conn_config.get('user'),
                password=conn_config.get('password'),
                warehouse=conn_config.get('warehouse'),
                database=conn_config.get('database'),
                schema=conn_config.get('schema'),
                role=conn_config.get('role'),
                authenticator=conn_config.get('authenticator', 'snowflake')
            )
    except Exception as e:
        logger.error(f"Failed to create Snowflake connection: {str(e)}")
        raise

def deploy_component(profile_name, component_path, component_name, component_type):
    """Deploy a component to Snowflake."""
    logger.info(f"Deploying component: {component_name} ({component_type})")
    
    # Get connection config
    conn_config = get_connection_config(profile_name)
    if conn_config is None:
        return False
    
    conn = None
    try:
        # Connect to Snowflake using the enhanced connection function
        conn = create_snowflake_connection(conn_config)
        
        cursor = conn.cursor()
        
        # Create temporary stage if it doesn't exist
        stage_name = f"{conn_config.get('database')}.{conn_config.get('schema')}.DEPLOYMENT_STAGE"
        logger.info(f"Using stage: {stage_name}")
        cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")
        
        # Package the code
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = os.path.join(temp_dir, f"{component_name}.zip")
            code_dir = os.path.join(component_path, component_name.lower().replace(" ", "_"))
            
            # Find the actual code directory if it doesn't match the expected pattern
            if not os.path.exists(code_dir):
                subdirs = [d for d in os.listdir(component_path) if os.path.isdir(os.path.join(component_path, d))]
                if subdirs:
                    code_dir = os.path.join(component_path, subdirs[0])
                    logger.info(f"Using actual code directory: {code_dir}")
            
            # Check and fix UDF function signature if necessary
            if component_type.lower() == "udf":
                logger.info(f"Checking and fixing UDF function signature for {component_name}")
                os.system(f"python scripts/check_and_fix_udf.py {code_dir}")
            
            # Log directory contents
            logger.info(f"Component directory structure:")
            for root, dirs, files in os.walk(component_path):
                level = root.replace(component_path, '').count(os.sep)
                indent = ' ' * 4 * level
                logger.info(f"{indent}{os.path.basename(root)}/")
                sub_indent = ' ' * 4 * (level + 1)
                for f in files:
                    logger.info(f"{sub_indent}{f}")
            
            # Zip the directory
            zip_directory(code_dir, zip_path)
            logger.info(f"Created zip file: {zip_path}")
            
            # Upload to stage
            upload_query = f"PUT file://{zip_path} @{stage_name}/{component_name.replace(' ', '_')}/ OVERWRITE=TRUE"
            logger.info(f"Uploading with query: {upload_query}")
            cursor.execute(upload_query)
            
            # Create UDF or procedure
            zip_filename = os.path.basename(zip_path)
            import_path = f"@{stage_name}/{component_name.replace(' ', '_')}/{zip_filename}"
            
            if component_type.lower() == "udf":
                # For Snowpark UDFs - different SQL based on parameter signature
                sql = f"""
                CREATE OR REPLACE FUNCTION {component_name.replace(' ', '_')}(input_data VARIANT)
                RETURNS VARIANT
                LANGUAGE PYTHON
                RUNTIME_VERSION=3.8
                PACKAGES = ('snowflake-snowpark-python')
                HANDLER = 'function.main'
                IMPORTS = ('{import_path}')
                """
            else:  # procedure
                sql = f"""
                CREATE OR REPLACE PROCEDURE {component_name.replace(' ', '_')}()
                RETURNS VARIANT
                LANGUAGE PYTHON
                RUNTIME_VERSION=3.8
                PACKAGES = ('snowflake-snowpark-python')
                IMPORTS = ('{import_path}')
                HANDLER = 'function.main'
                """
            
            logger.info(f"Creating with SQL: {sql}")
            cursor.execute(sql)
            
            logger.info(f"Successfully deployed {component_name}")
            return True
            
    except Exception as e:
        logger.error(f"Error deploying {component_name}: {str(e)}")
        return False
    
    finally:
        if conn:
            conn.close()

def execute_sql_file(profile_name, sql_file):
    """Execute SQL from a file."""
    logger.info(f"Executing SQL file: {sql_file}")
    
    # Get connection config
    conn_config = get_connection_config(profile_name)
    if conn_config is None:
        return False
    
    conn = None
    try:
        # Connect to Snowflake using the enhanced connection function
        conn = create_snowflake_connection(conn_config)
        
        # Read SQL file
        with open(sql_file, 'r') as f:
            sql_commands = f.read()
        
        # Execute each SQL command
        cursor = conn.cursor()
        for sql in sql_commands.split(';'):
            sql = sql.strip()
            if sql:
                logger.info(f"Executing SQL: {sql[:80]}...")
                cursor.execute(sql)
                
        logger.info(f"SQL file execution complete: {sql_file}")
        return True
        
    except Exception as e:
        logger.error(f"Error executing SQL file: {str(e)}")
        return False
    
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Deploy components or execute SQL')
    subparsers = parser.add_subparsers(dest='command')
    
    # Deploy component subcommand
    deploy_parser = subparsers.add_parser('deploy')
    deploy_parser.add_argument('--profile', required=True, help='Connection profile')
    deploy_parser.add_argument('--path', required=True, help='Component path')
    deploy_parser.add_argument('--name', required=True, help='Component name')
    deploy_parser.add_argument('--type', required=True, help='Component type (udf or procedure)')
    
    # Execute SQL subcommand
    sql_parser = subparsers.add_parser('sql')
    sql_parser.add_argument('--profile', required=True, help='Connection profile')
    sql_parser.add_argument('--file', required=True, help='SQL file path')
    
    args = parser.parse_args()
    
    if args.command == 'deploy':
        success = deploy_component(args.profile, args.path, args.name, args.type)
        sys.exit(0 if success else 1)
    
    elif args.command == 'sql':
        success = execute_sql_file(args.profile, args.file)
        sys.exit(0 if success else 1)
    
    else:
        parser.print_help()
        sys.exit(1)