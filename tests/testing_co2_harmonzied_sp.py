import pytest
import sys
import os
from unittest.mock import patch, MagicMock

# Add the parent directory to path so we can import the function module
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                           "udfs_and_spoc", "co2_harmonized_sp", "co2_harmonized_sp"))

# Import the module but patch the environment before importing the specific functions
with patch.dict('os.environ', {"ENV": "dev"}):
    import function as harmonized_function
    from function import main, table_exists, create_harmonized_table, merge_raw_into_harmonized

@pytest.fixture
def mock_session():
    """Create a mock Snowflake session."""
    mock = MagicMock()
    # Mock collect method
    mock.sql().collect.return_value = []
    return mock

@pytest.fixture
def mock_snowpark_functions():
    """Create mock for snowflake.snowpark.functions."""
    with patch("snowflake.snowpark.functions") as mock_functions:
        mock_functions.lit.return_value = MagicMock()
        mock_functions.col.return_value = MagicMock()
        mock_functions.concat_ws.return_value = MagicMock()
        mock_functions.to_date.return_value = MagicMock()
        mock_functions.current_timestamp.return_value = MagicMock()
        mock_functions.when_matched.return_value.update.return_value = MagicMock()
        mock_functions.when_not_matched.return_value.insert.return_value = MagicMock()
        yield mock_functions

def test_table_exists_when_table_present(mock_session):
    """Test table_exists when table is present."""
    mock_session.sql().collect.return_value = [{'name': 'harmonized_co2'}]
    assert table_exists(mock_session, 'HARMONIZED_CO2', 'harmonized_co2') is True

def test_table_exists_when_table_absent(mock_session):
    """Test table_exists when table is not present."""
    mock_session.sql().collect.return_value = []
    assert table_exists(mock_session, 'HARMONIZED_CO2', 'harmonized_co2') is False

def test_table_exists_handle_exception(mock_session):
    """Test table_exists error handling."""
    mock_session.sql.side_effect = Exception("Query error")
    assert table_exists(mock_session, 'HARMONIZED_CO2', 'harmonized_co2') is False

def test_create_harmonized_table(mock_session):
    """Test create_harmonized_table function."""
    create_harmonized_table(mock_session)
    
    # Instead of checking call count, check that SQL was called with the right statement
    mock_session.sql.assert_any_call("""
    CREATE TABLE HARMONIZED_CO2.harmonized_co2 (
        DATE DATE,
        YEAR NUMBER,
        MONTH NUMBER,
        DAY NUMBER,
        CO2_PPM FLOAT,
        META_UPDATED_AT TIMESTAMP_NTZ
    )
    """)
    
    # Check that collect() was called after sql()
    mock_session.sql().collect.assert_called()

def test_merge_raw_into_harmonized_success(mock_session, mock_snowpark_functions):
    """Test successful merge operation."""
    # Mock the table method to return DataFrames
    mock_source_df = MagicMock()
    mock_target_df = MagicMock()
    mock_session.table.side_effect = [mock_source_df, mock_target_df]
    
    # Configure source_df.with_column to return itself
    mock_source_df.with_column.return_value = mock_source_df
    
    # Set the environment variable
    with patch.object(harmonized_function, 'env', 'dev'):
        assert merge_raw_into_harmonized(mock_session) is True
        
        # Check that the SQL calls include warehouse scaling operations
        warehouse_scale_up_called = False
        warehouse_scale_down_called = False
        
        for call_args in mock_session.sql.call_args_list:
            if len(call_args[0]) > 0:  # Make sure there are arguments
                sql_cmd = call_args[0][0]
                if "ALTER WAREHOUSE co2_wh_dev SET WAREHOUSE_SIZE = XLARGE" in sql_cmd:
                    warehouse_scale_up_called = True
                if "ALTER WAREHOUSE co2_wh_dev SET WAREHOUSE_SIZE = XSMALL" in sql_cmd:
                    warehouse_scale_down_called = True
        
        assert warehouse_scale_up_called, "Warehouse scaling up was not called"
        assert warehouse_scale_down_called, "Warehouse scaling down was not called"
        
        # Check merge was called
        mock_target_df.merge.assert_called_once()

def test_merge_raw_into_harmonized_failure(mock_session):
    """Test merge operation failure."""
    # Make the table method raise an exception
    mock_session.table.side_effect = Exception("Database error")
    
    # Set the environment variable
    with patch.object(harmonized_function, 'env', 'dev'):
        assert merge_raw_into_harmonized(mock_session) is False

def test_main_table_exists(mock_session):
    """Test main function when table exists."""
    # Mock table_exists to return True
    with patch.object(harmonized_function, "table_exists", return_value=True), \
         patch.object(harmonized_function, "merge_raw_into_harmonized", return_value=True):
        result = main(mock_session)
    
    assert "merge complete" in result
    
def test_main_table_doesnt_exist(mock_session):
    """Test main function when table doesn't exist."""
    # Mock table_exists to return False
    with patch.object(harmonized_function, "table_exists", return_value=False), \
         patch.object(harmonized_function, "create_harmonized_table") as mock_create, \
         patch.object(harmonized_function, "merge_raw_into_harmonized", return_value=True):
        result = main(mock_session)
    
    # Check that create_harmonized_table was called
    mock_create.assert_called_once_with(mock_session)
    assert "merge complete" in result

def test_main_merge_failure(mock_session):
    """Test main function when merge fails."""
    with patch.object(harmonized_function, "table_exists", return_value=True), \
         patch.object(harmonized_function, "merge_raw_into_harmonized", return_value=False):
        result = main(mock_session)
    
    assert "Error during merge" in result

if __name__ == "__main__":
    pytest.main(["-v", "testing_co2_harmonzied_sp.py"])