import os
import sys
import pytest
from unittest.mock import MagicMock, patch, call

# Add the parent directory to path so we can import the function module
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                           "udfs_and_spoc", "co2_analytical_sp", "co2_analytical_sp"))

# Import the module but patch the environment before importing the specific function
with patch.dict('os.environ', {"ENV": "dev"}):
    import function as analytical_function
    from function import create_analytics_tables

@pytest.fixture
def mock_session():
    """Create a mock Snowflake session."""
    session = MagicMock()
    
    # Mock SQL execution results
    mock_collect = MagicMock()
    session.sql.return_value.collect = mock_collect
    
    # Mock current session state methods
    session.get_current_database.return_value = "CO2_DB_DEV"
    session.get_current_schema.return_value = "ANALYTICS_CO2"
    session.get_current_warehouse.return_value = "CO2_WH_DEV"
    session.get_current_role.return_value = "CO2_ROLE_DEV"
    
    return session

def test_create_analytics_tables_success(mock_session):
    """Test successful execution of create_analytics_tables."""
    # Set the environment variable
    with patch.object(analytical_function, 'env', 'dev'):
        # Call the function
        result = create_analytics_tables(mock_session)
        
        # Verify result
        assert "successfully" in result.lower()
        
        # Verify SQL calls
        sql_calls = [call_args[0][0] for call_args in mock_session.sql.call_args_list]
        
        # Verify warehouse scaling up - now using LARGE instead of XLARGE
        assert any("ALTER WAREHOUSE co2_wh_dev SET WAREHOUSE_SIZE = LARGE" in call for call in sql_calls), \
            "Did not find command to scale up warehouse to LARGE"
        
        # Verify table creation calls
        assert any("CREATE OR REPLACE TABLE ANALYTICS_CO2.DAILY_ANALYTICS" in call for call in sql_calls), \
            "Did not find DAILY_ANALYTICS table creation"
        assert any("CREATE OR REPLACE TABLE ANALYTICS_CO2.DAILY_CO2_STATS" in call for call in sql_calls), \
            "Did not find DAILY_CO2_STATS table creation"
        assert any("CREATE OR REPLACE TABLE ANALYTICS_CO2.WEEKLY_CO2_SUMMARY" in call for call in sql_calls), \
            "Did not find WEEKLY_CO2_SUMMARY table creation"
        
        # Verify warehouse scaling down
        assert any("ALTER WAREHOUSE co2_wh_dev SET WAREHOUSE_SIZE = XSMALL" in call for call in sql_calls), \
            "Did not find command to scale down warehouse"

def test_create_analytics_tables_sql_error(mock_session):
    """Test error handling when SQL execution fails."""
    # Set the environment variable
    with patch.object(analytical_function, 'env', 'dev'):
        # Make SQL execution fail
        mock_session.sql.return_value.collect.side_effect = Exception("SQL execution error")
        
        # Call the function
        result = create_analytics_tables(mock_session)
        
        # Verify error in result
        assert "error" in result.lower()
        
        # Verify warehouse scaling down attempted even after error
        sql_calls = [call_args[0][0] for call_args in mock_session.sql.call_args_list]
        assert any("ALTER WAREHOUSE co2_wh_dev SET WAREHOUSE_SIZE = XSMALL" in call for call in sql_calls)

def test_warehouse_scaling_error(mock_session):
    """Test handling of warehouse scaling errors."""
    # Set the environment variable
    with patch.object(analytical_function, 'env', 'dev'):
        # Make first SQL call work (scale up) but fail on subsequent calls
        call_count = [0]
        
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call (scale up) works fine
                return MagicMock()
            else:
                # Subsequent calls fail
                raise Exception("SQL error")
        
        mock_session.sql.return_value.collect.side_effect = side_effect
        
        # Patch print function to check for warnings
        with patch("builtins.print") as mock_print:
            # Call the function
            result = create_analytics_tables(mock_session)
            
            # Verify we attempted to scale down
            mock_session.sql.assert_any_call("ALTER WAREHOUSE co2_wh_dev SET WAREHOUSE_SIZE = XSMALL")
            
            # Verify warning messages
            any_warning = False
            for call_args in mock_print.call_args_list:
                args = call_args[0]
                if any(isinstance(arg, str) and "warning" in arg.lower() for arg in args):
                    any_warning = True
                    break
            assert any_warning, "Expected a warning message about scaling"

def test_environment_specific_execution(mock_session):
    """Test that the function uses the correct environment variables."""
    # Set the environment variable directly in the module
    with patch.object(analytical_function, 'env', 'prod'):
        # Call the function
        create_analytics_tables(mock_session)
        
        # Verify environment-specific SQL
        sql_calls = [call_args[0][0] for call_args in mock_session.sql.call_args_list]
        
        # Check for prod environment warehouse names - now using LARGE
        assert any("ALTER WAREHOUSE co2_wh_prod SET WAREHOUSE_SIZE = LARGE" in call for call in sql_calls), \
            f"No call to scale up prod warehouse found in: {sql_calls}"
        assert any("ALTER WAREHOUSE co2_wh_prod SET WAREHOUSE_SIZE = XSMALL" in call for call in sql_calls), \
            f"No call to scale down prod warehouse found in: {sql_calls}"

if __name__ == "__main__":
    pytest.main(["-v", "testing_co2_analytical_sp.py"])
