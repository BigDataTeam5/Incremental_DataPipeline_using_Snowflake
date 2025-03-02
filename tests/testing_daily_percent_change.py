import pytest
import sys
import os

# Add the parent directory to path so we can import the function module
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "udfs_and_spoc", "daily_co2_changes", "daily_changes"))
from function import co2_percent_change

def test_normal_calculation():
    """Test normal percentage change calculation."""
    assert co2_percent_change(100, 110) == 10.0
    assert co2_percent_change(110, 100) == -9.090909090909092
    assert co2_percent_change(418.5, 420.23) == pytest.approx(0.413858065374222, abs=0.002) 

def test_none_values():
    """Test handling of None values."""
    assert co2_percent_change(None, 100) == 0.0
    assert co2_percent_change(100, None) == 0.0
    assert co2_percent_change(None, None) == 0.0

def test_zero_values():
    """Test handling of zero values."""
    assert co2_percent_change(0, 100) == 0.0
    assert co2_percent_change(100, 0) == -100.0

def test_type_conversion():
    """Test handling of string inputs that can be converted."""
    assert co2_percent_change("100", "110") == 10.0

def test_invalid_inputs():
    """Test handling of invalid inputs."""
    assert co2_percent_change("abc", 100) == 0.0
    assert co2_percent_change(100, "xyz") == 0.0
    assert co2_percent_change({}, []) == 0.0