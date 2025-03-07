import pytest
import pandas as pd
import io
import sys
import os
from datetime import datetime
from unittest.mock import patch, MagicMock, Mock
# Add the parent directory to path so we can import the function module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from udfs_and_spoc.loading_data_sp.function import fetch_co2_data_incremental

@pytest.fixture
def mock_session():
    """Create a mock Snowflake session."""
    mock = MagicMock()
    # Mock collect method to return data
    mock_result = MagicMock()
    mock.sql().collect.return_value = [mock_result]
    return mock

@pytest.fixture
def mock_requests_get():
    """Create a mock for requests.get that returns sample CO2 data."""
    mock = MagicMock()
    mock.return_value.status_code = 200
    # Sample CO2 data with header comments
    current_year = datetime.now().year
    mock.return_value.text = f"""# CO2 data from Mauna Loa Observatory
# Some header info
{current_year} 1 1 {current_year}.000 418.50
{current_year} 1 2 {current_year}.003 418.65
{current_year} 1 3 {current_year}.005 418.75
"""
    return mock

@pytest.fixture
def mock_boto3_client():
    """Create a mock for boto3.client."""
    mock = MagicMock()
    mock.return_value.put_object.return_value = {}
    return mock

@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set mock environment variables."""
    monkeypatch.setenv("AWS_ACCESS_KEY", "mock_access_key")
    monkeypatch.setenv("AWS_SECRET_KEY", "mock_secret_key")
    monkeypatch.setenv("S3_BUCKET_NAME", "mock-bucket")

@pytest.mark.parametrize(
    "latest_date,expected_outcome",
    [
        (None, "Successfully loaded"),  # No data exists yet
        (f"{datetime.now().year}-01-01", "Successfully loaded"),  # Some new data to load
        (f"{datetime.now().year}-12-31", "No new CO2 data to load")  # No new data
    ]
)
def test_fetch_co2_data_incremental(mock_session, mock_requests_get, mock_boto3_client, 
                                   mock_env_vars, latest_date, expected_outcome):
    """Test fetching CO2 data with different latest dates."""
    # Configure the mock_session to return the latest date
    mock_result = mock_session.sql().collect.return_value[0]
    mock_result.__getitem__.return_value = pd.to_datetime(latest_date) if latest_date else None
    
    # Apply patches
    with patch("requests.get", mock_requests_get), \
         patch("boto3.client", mock_boto3_client), \
         patch("pandas.to_datetime") as mock_to_datetime:
        
        # Configure mock_to_datetime to handle both string and DataFrame column inputs
        def side_effect(arg, *args, **kwargs):
            if isinstance(arg, str):
                return pd.to_datetime(arg)
            else:
                # For DataFrame column
                if latest_date and pd.to_datetime(latest_date) >= datetime(datetime.now().year, 1, 3):
                    return pd.Series([]) # Empty series for "no new data" case
                else:
                    return pd.Series([datetime(datetime.now().year, 1, 2), 
                                     datetime(datetime.now().year, 1, 3)])
        mock_to_datetime.side_effect = side_effect
        
        # Run the function
        result = fetch_co2_data_incremental(mock_session)
        
        # Check if result contains expected outcome
        assert expected_outcome in result

def test_error_handling_api_failure(mock_session, mock_env_vars):
    """Test error handling when API request fails."""
    # Create a failing mock for requests.get
    mock_requests_fail = MagicMock()
    mock_requests_fail.return_value.status_code = 404
    
    with patch("requests.get", mock_requests_fail):
        result = fetch_co2_data_incremental(mock_session)
        assert "ERROR" in result
        assert "Status code: 404" in result

def test_error_handling_db_query_failure(mock_session, mock_env_vars):
    """Test error handling when the database query fails."""
    # Make the session query throw an exception
    mock_session.sql.side_effect = Exception("Database error")
    
    result = fetch_co2_data_incremental(mock_session)
    assert "ERROR" in result
    assert "Failed to get latest date" in result

if __name__ == "__main__":
    pytest.main()