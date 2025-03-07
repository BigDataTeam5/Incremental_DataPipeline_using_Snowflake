import pytest
import snowflake.connector
import os
import logging
import toml
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Path to connections.toml
CONNECTIONS_FILE = os.path.expanduser("~/.snowflake/connections.toml")

def load_connection_profiles():
    """Load all connection profiles from the TOML file."""
    try:
        if os.path.exists(CONNECTIONS_FILE):
            with open(CONNECTIONS_FILE, 'r') as f:
                profiles = toml.load(f)
                logger.info(f"Loaded {len(profiles)} connection profiles")
                return profiles
        else:
            logger.error(f"Connections file not found: {CONNECTIONS_FILE}")
            return {}
    except Exception as e:
        logger.error(f"Error loading connection profiles: {str(e)}")
        return {}

def _test_connection_profile(profile_name, connection_params):
    """Test a specific connection profile."""
    conn = None
    try:
        # Print connection details (excluding password)
        safe_params = {k: v for k, v in connection_params.items() if k != 'password'}
        logger.info(f"Testing profile '{profile_name}':")
        for key, value in safe_params.items():
            logger.info(f"  {key}: {value}")
            
        # Connect to Snowflake
        conn = snowflake.connector.connect(**connection_params)
        
        # Test connection is working
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_WAREHOUSE(), CURRENT_VERSION()")
        result = cursor.fetchone()
        cursor.close()
        
        # Print connection info
        logger.info(f"✅ Connection successful for '{profile_name}'")
        logger.info(f"  Current user: {result[0]}")
        logger.info(f"  Current role: {result[1]}")
        logger.info(f"  Current database: {result[2]}")
        logger.info(f"  Current warehouse: {result[3]}")
        logger.info(f"  Snowflake version: {result[4]}")
        
        return True, result
    except Exception as e:
        logger.error(f"❌ Connection failed for '{profile_name}': {str(e)}")
        return False, None
    finally:
        if conn is not None:
            conn.close()

# Main test function
def test_all_profiles():
    """Test all connection profiles."""
    profiles = load_connection_profiles()
    results = {}
    
    if not profiles:
        pytest.fail("No connection profiles found")
    
    for profile_name, params in profiles.items():
        success, details = _test_connection_profile(profile_name, params)
        results[profile_name] = {
            'success': success,
            'details': details,
            'params': {k: v for k, v in params.items() if k != 'password'}
        }
    
    # Final results summary
    logger.info("\n--- CONNECTION RESULTS SUMMARY ---")
    for profile, result in results.items():
        status = "✅ SUCCESS" if result['success'] else "❌ FAILED"
        logger.info(f"{profile}: {status}")
    
    # Make sure at least one connection worked
    assert any(r['success'] for r in results.values()), "All connection attempts failed"
    
    return results

# Individual test functions for pytest
def test_default_connection():
    """Test the default connection."""
    profiles = load_connection_profiles()
    if 'default' not in profiles:
        pytest.skip("No 'default' connection profile found")
    success, _ = _test_connection_profile('default', profiles['default'])
    assert success, "Default connection failed"

def test_dev_connection():
    """Test the dev connection."""
    profiles = load_connection_profiles()
    if 'dev' not in profiles:
        pytest.skip("No 'dev' connection profile found")
    success, _ = _test_connection_profile('dev', profiles['dev'])
    assert success, "Dev connection failed"

def test_prod_connection():
    """Test the prod connection."""
    profiles = load_connection_profiles()
    if 'prod' not in profiles:
        pytest.skip("No 'prod' connection profile found")
    success, _ = _test_connection_profile('prod', profiles['prod'])
    assert success, "Prod connection failed"

# Run tests directly when executed as script
if __name__ == "__main__":
    test_all_profiles()