#!/usr/bin/env python3
"""
Test key-based authentication with Snowflake.
This script attempts to connect to Snowflake using a private key
and reports detailed diagnostics about the authentication process.
"""

import os
import sys
import logging
import argparse
import toml
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_key_auth(profile_name, verbose=False):
    """Test key authentication with the given profile."""
    # Load the connection profile
    config_path = os.path.expanduser("~/.snowflake/connections.toml")
    
    if not os.path.exists(config_path):
        logger.error(f"Connection file not found: {config_path}")
        return False
    
    try:
        # Print file diagnostics
        if verbose:
            logger.info(f"Config file path: {config_path}")
            logger.info(f"File exists: {os.path.exists(config_path)}")
            logger.info(f"File size: {os.path.getsize(config_path)}")
            logger.info(f"Permissions: {oct(os.stat(config_path).st_mode)[-3:]}")
        
        # Read and parse the file
        with open(config_path, 'r', encoding='utf-8') as f:
            if verbose:
                # Preview file content for debugging
                preview = f.read(100)
                logger.info(f"File preview: {preview}...")
                f.seek(0)  # Reset file pointer after preview
            
            # Parse TOML
            config = toml.load(f)
            
            logger.info(f"Available profiles: {list(config.keys())}")
            if verbose:
                for k in config.keys():
                    logger.info(f"Profile '{k}' has keys: {list(config[k].keys())}")
        
        if profile_name not in config:
            logger.error(f"Profile '{profile_name}' not found in config file")
            logger.info("Available profiles: " + ", ".join(config.keys()))
            return False
        
        profile = config[profile_name]
        
        # Check if key authentication is configured
        if 'private_key_path' not in profile:
            logger.error(f"Profile '{profile_name}' does not have 'private_key_path' configured")
            logger.info(f"Profile contains these keys: {list(profile.keys())}")
            return False
        
        key_path = profile['private_key_path']
        logger.info(f"Using private key from: {key_path}")
        
        # Check if key file exists
        if not os.path.exists(key_path):
            logger.error(f"Private key file not found: {key_path}")
            return False
        
        # Read and validate key file
        try:
            with open(key_path, "rb") as key_file:
                key_data = key_file.read()
                logger.info(f"Private key file size: {len(key_data)} bytes")
                
                if verbose:
                    # Show more details about the key content (safely)
                    logger.info(f"Key starts with: {key_data[:50]}")
                    logger.info(f"Key ends with: {key_data[-50:] if len(key_data) > 50 else key_data}")
                
                # Check for PEM header/footer
                if b"-----BEGIN PRIVATE KEY-----" not in key_data:
                    logger.warning("Key file missing PEM header - may not be a valid PEM file")
                
                # Try to load the key
                p_key = serialization.load_pem_private_key(
                    key_data,
                    password=None,
                    backend=default_backend()
                )
                logger.info("Private key loaded successfully")
                
                # Convert to DER format for Snowflake
                pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
                logger.info("Private key converted to DER format")
                
                # Attempt connection
                logger.info(f"Connecting to Snowflake as user '{profile.get('user')}' on account '{profile.get('account')}'")
                
                conn = snowflake.connector.connect(
                    account=profile.get('account'),
                    user=profile.get('user'),
                    private_key=pkb,
                    warehouse=profile.get('warehouse'),
                    database=profile.get('database'),
                    schema=profile.get('schema'),
                    role=profile.get('role'),
                    client_request_mfa_token=False,  # Explicitly disable MFA
                    authenticator='snowflake'  # Ensure using Snowflake authenticator
                )
                
                # Test connectivity by running a simple query
                logger.info("Connection established, testing query execution...")
                cursor = conn.cursor()
                cursor.execute("SELECT current_version(), current_user(), current_role()")
                result = cursor.fetchone()
                logger.info(f"Connected successfully: Version={result[0]}, User={result[1]}, Role={result[2]}")
                
                conn.close()
                logger.info("Test completed successfully!")
                return True
                
        except Exception as e:
            logger.error(f"Error during key authentication test: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
            
    except Exception as e:
        logger.error(f"Error loading connection profile: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test key-based authentication with Snowflake')
    parser.add_argument('--profile', required=True, help='Connection profile name (e.g., dev or prod)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show more detailed output')
    
    args = parser.parse_args()
    
    success = test_key_auth(args.profile, args.verbose)
    sys.exit(0 if success else 1)
