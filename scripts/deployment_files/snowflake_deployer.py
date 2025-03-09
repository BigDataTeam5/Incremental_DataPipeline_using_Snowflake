import os
import sys
import logging
import toml
import yaml
import snowflake.connector
from pathlib import Path
import subprocess
import zipfile
import tempfile

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

def analyze_function_signature(function_file):
    """Analyze the function signature to determine parameter structure."""
    try:
        import re
        with open(function_file, 'r') as f:
            content = f.read()
        
        # Look for the main function definition
        signature_match = re.search(r'def\s+main\s*\((.*?)\)', content)
        if signature_match:
            params = signature_match.group(1).strip()
            logger.info(f"Function signature parameters: '{params}'")
            
            # Count parameters (excluding session if present)
            param_list = [p.strip() for p in params.split(',') if p.strip()]
            
            # Check if session is a parameter
            has_session = any(p.strip().startswith('session') for p in param_list)
            param_count = len(param_list)
            
            return {
                'has_session': has_session,
                'param_count': param_count,
                'param_list': param_list
            }
    except Exception as e:
        logger.error(f"Error analyzing function signature: {e}")
    
    # Default fallback
    return {
        'has_session': False,
        'param_count': 1,
        'param_list': ['input_data']
    }

def zip_directory(source_dir, zip_path):
    """Create a zip file from a directory."""
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, source_dir)
                zipf.write(file_path, arcname)

def fallback_deploy_udf(conn_config, component_path, component_name, project_config=None):
    """Deploy UDF directly using Snowflake connector when Snow CLI fails."""
    logger.info(f"Attempting fallback deployment for {component_name}")
    
    conn = None
    try:
        # Connect to Snowflake
        conn = create_snowflake_connection(conn_config)
        cursor = conn.cursor()
        
        # Find code directory
        if os.path.isdir(os.path.join(component_path, component_name.lower().replace(" ", "_"))):
            code_dir = os.path.join(component_path, component_name.lower().replace(" ", "_"))
        else:
            # Look for first directory that might contain the code
            for item in os.listdir(component_path):
                if os.path.isdir(os.path.join(component_path, item)):
                    code_dir = os.path.join(component_path, item)
                    break
            else:
                logger.error(f"Could not find code directory in {component_path}")
                return False

        # Package the code
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = os.path.join(temp_dir, f"{component_name}.zip")
            
            # Check if there's a snowflake.yml to use
            if project_config:
                src_dir = os.path.join(component_path, project_config['snowpark'].get('src', ''))
                if os.path.exists(src_dir) and os.path.isdir(src_dir):
                    code_dir = src_dir
                    logger.info(f"Using source directory from config: {code_dir}")
                
            # Check and fix UDF function signature if necessary
            function_file = os.path.join(code_dir, "function.py")
            signature_info = {'has_session': False, 'param_count': 1, 'param_list': ['input_data']}
            
            if os.path.exists(function_file):
                logger.info(f"Analyzing UDF function signature for {component_name}")
                signature_info = analyze_function_signature(function_file)
                logger.info(f"Function analysis: Session={signature_info['has_session']}, "
                          f"Param Count={signature_info['param_count']}")
            
            # Log directory contents
            logger.info(f"Component directory structure:")
            for root, dirs, files in os.walk(code_dir):
                level = root.replace(code_dir, '').count(os.sep)
                indent = ' ' * 4 * level
                logger.info(f"{indent}{os.path.basename(root)}/")
                sub_indent = ' ' * 4 * (level + 1)
                for f in files:
                    logger.info(f"{sub_indent}{f}")
            
            # Zip the directory
            zip_directory(code_dir, zip_path)
            logger.info(f"Created zip file: {zip_path}")
            
            # Create temporary stage if it doesn't exist
            stage_name = f"{conn_config.get('database')}.{conn_config.get('schema')}.DEPLOYMENT_STAGE"
            logger.info(f"Using stage: {stage_name}")
            cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")
            
            # Upload to stage
            upload_query = f"PUT file://{zip_path} @{stage_name}/{component_name.replace(' ', '_')}/ OVERWRITE=TRUE"
            logger.info(f"Uploading with query: {upload_query}")
            cursor.execute(upload_query)
            
            # Create UDF
            zip_filename = os.path.basename(zip_path)
            import_path = f"@{stage_name}/{component_name.replace(' ', '_')}/{zip_filename}"
            
            # Determine function signature based on project config or function analysis
            if project_config and 'functions' in project_config.get('snowpark', {}):
                # Use signature from project config
                function_config = project_config['snowpark']['functions'][0]
                
                # Build parameters from signature
                params = []
                for param in function_config.get('signature', []):
                    params.append(f"{param['name']} {param['type']}")
                
                param_str = ", ".join(params)
                return_type = function_config.get('returns', 'VARIANT')
                
                sql = f"""
                CREATE OR REPLACE FUNCTION {component_name.replace(' ', '_')}({param_str})
                RETURNS {return_type}
                LANGUAGE PYTHON
                RUNTIME_VERSION=3.8
                PACKAGES = ('snowflake-snowpark-python')
                IMPORTS = ('{import_path}')
                HANDLER = 'function.main'
                """
            else:
                # Use signature from function analysis
                if signature_info['param_count'] == 1:
                    sql = f"""
                    CREATE OR REPLACE FUNCTION {component_name.replace(' ', '_')}(input_data VARIANT)
                    RETURNS VARIANT
                    LANGUAGE PYTHON
                    RUNTIME_VERSION=3.8
                    PACKAGES = ('snowflake-snowpark-python')
                    IMPORTS = ('{import_path}')
                    HANDLER = 'function.main'
                    """
                elif signature_info['param_count'] == 2:
                    sql = f"""
                    CREATE OR REPLACE FUNCTION {component_name.replace(' ', '_')}(previous_value FLOAT, current_value FLOAT)
                    RETURNS FLOAT
                    LANGUAGE PYTHON
                    RUNTIME_VERSION=3.8
                    PACKAGES = ('snowflake-snowpark-python')
                    IMPORTS = ('{import_path}')
                    HANDLER = 'function.main'
                    """
                else:
                    sql = f"""
                    CREATE OR REPLACE FUNCTION {component_name.replace(' ', '_')}(input_data VARIANT)
                    RETURNS VARIANT
                    LANGUAGE PYTHON
                    RUNTIME_VERSION=3.8
                    PACKAGES = ('snowflake-snowpark-python')
                    IMPORTS = ('{import_path}')
                    HANDLER = 'function.main'
                    """
            
            logger.info(f"Creating with SQL: {sql}")
            cursor.execute(sql)
            
            logger.info(f"Successfully deployed {component_name} using fallback method")
            return True
            
    except Exception as e:
        logger.error(f"Error in fallback deployment for {component_name}: {str(e)}")
        return False
    
    finally:
        if conn:
            conn.close()

def verify_snow_cli_installation():
    """Verify Snow CLI is installed and available, install if missing."""
    try:
        # Try to check if snow CLI is installed
        result = subprocess.run(["snow", "--version"], capture_output=True, text=True)
        version_output = result.stdout.strip()
        logger.info(f"Snow CLI is installed: {version_output}")
        
        # Parse the version number to determine command availability
        version_str = version_output.split(": ")[-1] if ": " in version_output else version_output
        version_parts = version_str.split('.')
        
        try:
            major = int(version_parts[0])
            minor = int(version_parts[1]) if len(version_parts) > 1 else 0
            
            logger.info(f"Snow CLI version parsed: {major}.{minor}")
            
            # Use appropriate commands based on version
            if major == 0 and minor < 3:
                # For older versions like 0.2.9, check if we can list objects
                check_cmd = ["snow", "object", "list", "--help"]
                logger.info("Using Snow CLI legacy commands (v0.2.x)")
            else:
                # For newer versions, we expect snowpark commands
                check_cmd = ["snow", "--help"]
                logger.info("Using Snow CLI modern commands (v0.3+)")
                
            # Check if basic commands work
            help_result = subprocess.run(check_cmd, capture_output=True, text=True)
            if help_result.returncode != 0:
                logger.warning(f"Snow CLI command check failed: {help_result.stderr}")
            else:
                logger.info("Snow CLI command check passed")
        except (ValueError, IndexError):
            logger.warning(f"Could not parse Snow CLI version: {version_str}")
            
        return True
        
    except (subprocess.SubprocessError, FileNotFoundError):
        logger.warning("Snow CLI not found. Attempting to install...")
        
        try:
            # Install Snow CLI using pip
            subprocess.run(["pip", "install", "--upgrade", "snowflake-cli==0.3.0", "snowflake-cli-labs==0.2.0"], check=True)
            logger.info("Successfully installed Snow CLI via pip")
            
            # Verify installation
            result = subprocess.run(["snow", "--version"], capture_output=True, text=True)
            logger.info(f"Verified Snow CLI installation: {result.stdout.strip()}")
            
            return True
        except subprocess.SubprocessError as e:
            logger.error(f"Failed to install Snow CLI: {str(e)}")
            return False

def deploy_snowpark_projects(root_directory, profile_name, check_git_changes=False, git_ref='HEAD~1'):
    """Deploy all Snowpark projects found in the root directory using Snow CLI."""
    logger.info(f"Deploying all Snowpark apps in root directory {root_directory}")
    
    # Ensure Snow CLI is installed
    if not verify_snow_cli_installation():
        logger.error("Snow CLI is not available. Cannot proceed with deployment.")
        return False
    
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
        
    # Try to get the Snow CLI version to determine command format
    try:
        version_result = subprocess.run(["snow", "--version"], capture_output=True, text=True)
        version_output = version_result.stdout.strip()
        version_str = version_output.split(": ")[-1] if ": " in version_output else version_output
        version_parts = version_str.split('.')
        major = int(version_parts[0])
        minor = int(version_parts[1]) if len(version_parts) > 1 else 0
        
        # Set command format based on version
        if major == 0 and minor < 3:
            # Old Snow CLI (0.2.x) uses different commands
            use_legacy_commands = True
            logger.info("Using legacy Snow CLI commands (v0.2.x)")
        else:
            use_legacy_commands = False
            logger.info("Using modern Snow CLI commands (v0.3+)")
    except Exception as e:
        logger.warning(f"Could not determine Snow CLI version: {str(e)}. Will use fallback.")
        use_legacy_commands = True
    
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
        
        # Display project file content for debugging
        try:
            with open(os.path.join(directory_path, SNOWFLAKE_PROJECT_CONFIG_FILENAME), 'r') as f:
                logger.info(f"Project config content:\n{f.read()}")
        except Exception as e:
            logger.warning(f"Could not read project config: {str(e)}")

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
                
                # Decide deployment method based on CLI version
                deployment_successful = False
                
                try:
                    # Change to project directory
                    os.chdir(f"{directory_path}")
                    
                    # List directory contents for debugging
                    logger.info(f"Directory contents of {directory_path}:")
                    for item in os.listdir('.'):
                        logger.info(f"  - {item}")
                    
                    if use_legacy_commands:
                        # Legacy Snow CLI (0.2.x) - use native Python UDF deployment
                        logger.info("Using fallback deployment method for Snow CLI 0.2.x")
                        
                        # Get function information from project config
                        if 'functions' in project_settings.get('snowpark', {}) and project_settings['snowpark']['functions']:
                            function_config = project_settings['snowpark']['functions'][0]
                            function_name = function_config.get('name', component_name)
                            
                            # Use fallback deployment
                            if fallback_deploy_udf(conn_config, directory_path, function_name, project_settings):
                                logger.info(f"Successfully deployed {project_name} using fallback method")
                                deployment_successful = True
                                projects_deployed += 1
                        else:
                            logger.error("No function definition found in project config")
                    else:
                        # Modern Snow CLI (0.3+) - use snowpark commands
                        # Build the project
                        logger.info("Building project with Snow CLI...")
                        build_cmd = ["snow", "snowpark", "build", "--temporary-connection", 
                                    "--account", os.environ["SNOWFLAKE_ACCOUNT"], 
                                    "--user", os.environ["SNOWFLAKE_USER"], 
                                    "--role", os.environ["SNOWFLAKE_ROLE"], 
                                    "--warehouse", os.environ["SNOWFLAKE_WAREHOUSE"], 
                                    "--database", os.environ["SNOWFLAKE_DATABASE"]]
                        
                        logger.info(f"Executing: {' '.join(build_cmd)}")
                        build_result = subprocess.run(build_cmd, capture_output=True, text=True)
                        logger.info(f"Build STDOUT: {build_result.stdout}")
                        logger.info(f"Build STDERR: {build_result.stderr}")
                        
                        # Deploy the project
                        logger.info("Deploying project with Snow CLI...")
                        deploy_cmd = ["snow", "snowpark", "deploy", "--replace", "--temporary-connection", 
                                    "--account", os.environ["SNOWFLAKE_ACCOUNT"], 
                                    "--user", os.environ["SNOWFLAKE_USER"], 
                                    "--role", os.environ["SNOWFLAKE_ROLE"], 
                                    "--warehouse", os.environ["SNOWFLAKE_WAREHOUSE"], 
                                    "--database", os.environ["SNOWFLAKE_DATABASE"]]
                        
                        logger.info(f"Executing: {' '.join(deploy_cmd)}")
                        deploy_result = subprocess.run(deploy_cmd, capture_output=True, text=True)
                        logger.info(f"Deploy STDOUT: {deploy_result.stdout}")
                        logger.info(f"Deploy STDERR: {deploy_result.stderr}")
                        
                        if build_result.returncode != 0 or deploy_result.returncode != 0:
                            logger.error(f"Failed to deploy project in {directory_path}")
                            # Try fallback method
                            logger.info(f"Attempting fallback deployment for {project_name}")
                            function_name = project_name
                            if 'functions' in project_settings.get('snowpark', {}) and project_settings['snowpark']['functions']:
                                function_name = project_settings['snowpark']['functions'][0].get('name', project_name)
                                
                            if fallback_deploy_udf(conn_config, directory_path, function_name, project_settings):
                                logger.info(f"Fallback deployment successful for {project_name}")
                                deployment_successful = True
                                projects_deployed += 1
                        else:
                            logger.info(f"Successfully deployed project in {directory_path}")
                            deployment_successful = True
                            projects_deployed += 1
                            
                except Exception as e:
                    logger.error(f"Error processing project in {directory_path}: {str(e)}")
                    success = False
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
    
    # Get connection config for fallback deployment if needed
    conn_config = get_connection_config(profile_name)
    if conn_config is None:
        return False
    
    # Deploy the component
    if should_deploy:
        # Check if component is a Snow CLI project (has snowflake.yml)
        config_file = os.path.join(component_path, SNOWFLAKE_PROJECT_CONFIG_FILENAME)
        if os.path.exists(config_file):
            logger.info(f"Component {component_name} is a Snow CLI project, deploying with Snow CLI")
            
            # Load project config for potential fallback
            project_config = None
            try:
                with open(config_file, 'r') as yamlfile:
                    project_config = yaml.load(yamlfile, Loader=yaml.FullLoader)
            except Exception as e:
                logger.warning(f"Could not load project config: {str(e)}")
            
            # Try deploying with Snow CLI first
            result = deploy_snowpark_projects(component_path, profile_name, False)
            
            # If Snow CLI failed, try fallback for UDFs
            if not result and component_type.lower() == "udf":
                logger.info(f"Trying fallback deployment for {component_name}")
                return fallback_deploy_udf(conn_config, component_path, component_name, project_config)
            
            return result
        else:
            logger.warning(f"Component {component_name} doesn't have snowflake.yml, trying fallback deployment")
            if component_type.lower() == "udf":
                return fallback_deploy_udf(conn_config, component_path, component_name)
            else:
                logger.error(f"Cannot deploy {component_type} without Snow CLI or snowflake.yml")
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