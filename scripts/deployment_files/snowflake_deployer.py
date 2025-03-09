import os
import sys
import logging
import toml
import yaml
import snowflake.connector
from pathlib import Path
import subprocess

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define constants
IGNORE_FOLDERS = ['.git', '__pycache__', '.ipynb_checkpoints']
SNOWFLAKE_PROJECT_CONFIG_FILENAME = 'snowflake.yml'

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
                    # Add debug output to diagnose key content issues
                    logger.info(f"Key content preview (first 50 chars): {key_data[:50]}")
                    
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
                    
                    # Add debug flag to disable MFA prompt
                    params = {
                        'account': conn_config.get('account'),
                        'user': conn_config.get('user'),
                        'private_key': pkb,
                        'warehouse': conn_config.get('warehouse'),
                        'database': conn_config.get('database'),
                        'schema': conn_config.get('schema'),
                        'role': conn_config.get('role'),
                        # Explicitly disable MFA/2FA challenge for key-based auth
                        'client_request_mfa_token': False,
                        'authenticator': 'snowflake'
                    }
                    
                    # Fix the f-string syntax by using a helper function
                    param_strs = []
                    for k, v in params.items():
                        if k == 'private_key':
                            param_strs.append(f"{k}=[REDACTED]")
                        else:
                            param_strs.append(f"{k}={v}")
                    
                    logger.info(f"Connecting with params: {', '.join(param_strs)}")
                    
                    # Connect with private key
                    return snowflake.connector.connect(**params)
                    
                except Exception as e:
                    logger.error(f"Error loading private key: {str(e)}")
                    
                    # Do NOT fall back to password auth - this might be triggering MFA
                    logger.error("Key authentication failed - not falling back to password auth")
                    raise
                    
            except Exception as e:
                logger.error(f"Error processing private key: {str(e)}")
                raise
        else:
            # Connect with password - only if no key is configured
            logger.warning("No private key configured - using password authentication")
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

def check_for_changes(directory_path, git_ref='HEAD~1'):
    """Check if files in the directory have changed compared to a git reference."""
    try:
        # Get the base directory for the repository
        repo_dir = subprocess.check_output("git rev-parse --show-toplevel", shell=True).decode().strip()
        
        # Get the relative path from the repo root
        rel_path = os.path.relpath(directory_path, repo_dir)
        
        # Use git to check for changes in the directory
        cmd = f"git diff --name-only {git_ref} HEAD -- {rel_path}"
        logger.info(f"Running git command: {cmd}")
        
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        changed_files = result.stdout.strip().split('\n')
        
        # Filter out empty strings
        changed_files = [f for f in changed_files if f]
        
        has_changes = len(changed_files) > 0
        logger.info(f"Changes detected in {directory_path}: {has_changes}")
        if has_changes:
            logger.info(f"Changed files: {', '.join(changed_files)}")
        
        return has_changes
    except Exception as e:
        logger.warning(f"Error checking for changes: {str(e)}. Assuming changes exist.")
        return True  # If we can't determine changes, assume there are changes

def deploy_snowpark_projects(root_directory, profile_name, check_git_changes=False, git_ref='HEAD~1'):
    """Deploy all Snowpark projects found in the root directory using Snow CLI."""
    logger.info(f"Deploying all Snowpark apps in root directory {root_directory}")
    
    # Get connection config for environment variables
    conn_config = get_connection_config(profile_name)
    if conn_config is None:
        return False
    
    # Set environment variables for Snow CLI
    os.environ["SNOWFLAKE_ACCOUNT"] = conn_config.get('account', '')
    os.environ["SNOWFLAKE_USER"] = conn_config.get('user', '')
    os.environ["SNOWFLAKE_ROLE"] = conn_config.get('role', '')
    os.environ["SNOWFLAKE_WAREHOUSE"] = conn_config.get('warehouse', '')
    os.environ["SNOWFLAKE_DATABASE"] = conn_config.get('database', '')
    
    # Handle password or key-based authentication
    if 'password' in conn_config:
        os.environ["SNOWFLAKE_PASSWORD"] = conn_config.get('password', '')
    elif 'private_key_path' in conn_config:
        os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"] = conn_config.get('private_key_path', '')
    
    # Check if Snow CLI is available
    snow_cli_check = os.system("snow --version > /dev/null 2>&1")
    if snow_cli_check != 0:
        logger.error("Snow CLI not found or not working properly. Make sure it's installed and in PATH.")
        return False
    
    # Stats for summary
    projects_found = 0
    projects_deployed = 0
    projects_skipped = 0
    
    # Walk the entire directory structure recursively
    success = True
    for (directory_path, directory_names, file_names) in os.walk(root_directory):
        # Get just the last/final folder name in the directory path
        base_name = os.path.basename(directory_path)

        # Skip any folders we want to ignore
        if base_name in IGNORE_FOLDERS:
            continue

        # A snowflake.yml file in the folder is our indication that this folder contains
        # a Snow CLI project
        if not SNOWFLAKE_PROJECT_CONFIG_FILENAME in file_names:
            continue
            
        projects_found += 1
        logger.info(f"Found Snowflake project in folder {directory_path}")

        # Check if project has changed since last commit
        should_deploy = True
        if check_git_changes:
            has_changes = check_for_changes(directory_path, git_ref)
            if not has_changes:
                logger.info(f"No changes detected in {directory_path}. Skipping deployment.")
                projects_skipped += 1
                should_deploy = False
        
        if should_deploy:
            # Read the project config
            project_settings = {}
            try:
                with open(f"{directory_path}/{SNOWFLAKE_PROJECT_CONFIG_FILENAME}", "r") as yamlfile:
                    project_settings = yaml.load(yamlfile, Loader=yaml.FullLoader)

                # Confirm that this is a Snowpark project
                if 'snowpark' not in project_settings:
                    logger.info(f"Skipping non Snowpark project in folder {base_name}")
                    continue

                # Deploy the Snowpark project with the snowcli tool
                project_name = project_settings['snowpark'].get('project_name', 'Unknown')
                logger.info(f"Found Snowflake Snowpark project '{project_name}' in folder {base_name}")
                logger.info(f"Calling snowcli to deploy the project")
                
                # Save current directory
                current_dir = os.getcwd()
                
                try:
                    # Change to project directory
                    os.chdir(f"{directory_path}")
                    
                    # Build the project
                    build_cmd = f"snow snowpark build --temporary-connection --account $SNOWFLAKE_ACCOUNT --user $SNOWFLAKE_USER --role $SNOWFLAKE_ROLE --warehouse $SNOWFLAKE_WAREHOUSE --database $SNOWFLAKE_DATABASE"
                    logger.info(f"Executing: {build_cmd}")
                    build_result = os.system(build_cmd)
                    
                    # Deploy the project
                    deploy_cmd = f"snow snowpark deploy --replace --temporary-connection --account $SNOWFLAKE_ACCOUNT --user $SNOWFLAKE_USER --role $SNOWFLAKE_ROLE --warehouse $SNOWFLAKE_WAREHOUSE --database $SNOWFLAKE_DATABASE"
                    logger.info(f"Executing: {deploy_cmd}")
                    deploy_result = os.system(deploy_cmd)
                    
                    if build_result != 0 or deploy_result != 0:
                        logger.error(f"Failed to deploy project in {directory_path}")
                        success = False
                    else:
                        logger.info(f"Successfully deployed project in {directory_path}")
                        projects_deployed += 1
                    
                finally:
                    # Restore original directory
                    os.chdir(current_dir)
                    
            except Exception as e:
                logger.error(f"Error processing project in {directory_path}: {str(e)}")
                success = False
    
    # Log summary
    logger.info(f"Deployment summary: Found {projects_found} projects, deployed {projects_deployed}, skipped {projects_skipped}")
    
    return success

def deploy_component(profile_name, component_path, component_name, component_type, check_git_changes=False, git_ref='HEAD~1'):
    """Deploy a single component, checking for changes if requested."""
    logger.info(f"Processing component: {component_name} ({component_type})")
    
    # Check if component has changed
    should_deploy = True
    if check_git_changes:
        has_changes = check_for_changes(component_path, git_ref)
        if not has_changes:
            logger.info(f"No changes detected in {component_path}. Skipping deployment.")
            return True  # Return success, just skipped
    
    # Deploy the component
    if should_deploy:
        # Check if component is a Snow CLI project (has snowflake.yml)
        if os.path.exists(os.path.join(component_path, SNOWFLAKE_PROJECT_CONFIG_FILENAME)):
            logger.info(f"Component {component_name} is a Snow CLI project, deploying with Snow CLI")
            return deploy_snowpark_projects(component_path, profile_name, False)  # Don't check changes again
        else:
            logger.error(f"Component {component_name} doesn't appear to be a Snow CLI project")
            return False
    
    return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Deploy Snowflake components or execute SQL')
    subparsers = parser.add_subparsers(dest='command')
    
    # Deploy all projects subcommand
    deploy_all_parser = subparsers.add_parser('deploy-all')
    deploy_all_parser.add_argument('--profile', required=True, help='Connection profile')
    deploy_all_parser.add_argument('--path', required=True, help='Root directory path')
    deploy_all_parser.add_argument('--check-changes', action='store_true', help='Only deploy projects with changes')
    deploy_all_parser.add_argument('--git-ref', default='HEAD~1', help='Git reference to compare against (default: HEAD~1)')
    
    # Deploy single component subcommand
    deploy_parser = subparsers.add_parser('deploy')
    deploy_parser.add_argument('--profile', required=True, help='Connection profile')
    deploy_parser.add_argument('--path', required=True, help='Component path')
    deploy_parser.add_argument('--name', required=True, help='Component name')
    deploy_parser.add_argument('--type', required=True, help='Component type (udf or procedure)')
    deploy_parser.add_argument('--check-changes', action='store_true', help='Only deploy if component has changes')
    deploy_parser.add_argument('--git-ref', default='HEAD~1', help='Git reference to compare against (default: HEAD~1)')
    
    # Execute SQL subcommand
    sql_parser = subparsers.add_parser('sql')
    sql_parser.add_argument('--profile', required=True, help='Connection profile')
    sql_parser.add_argument('--file', required=True, help='SQL file path')
    
    args = parser.parse_args()
    
    if args.command == 'deploy-all':
        success = deploy_snowpark_projects(args.path, args.profile, args.check_changes, args.git_ref)
        sys.exit(0 if success else 1)
    
    elif args.command == 'deploy':
        success = deploy_component(args.profile, args.path, args.name, args.type, args.check_changes, args.git_ref)
        sys.exit(0 if success else 1)
        
    elif args.command == 'sql':
        success = execute_sql_file(args.profile, args.file)
        sys.exit(0 if success else 1)
    
    else:
        parser.print_help()
        sys.exit(1)