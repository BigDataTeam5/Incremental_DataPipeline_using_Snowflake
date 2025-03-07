import pytest
import sys
import os

# Add the path to the function module
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'udfs_and_spoc', 'python_udf', 'co2_volatility'))
from function import calculate_co2_volatility

# Test normal cases
@pytest.mark.parametrize("current, previous, expected", [
    (410.0, 400.0, 2.4691),     # ~2.5% volatility
    (400.0, 410.0, 2.4691),     # Same volatility (direction doesn't matter)
    (350.0, 350.0, 0.0),        # No change
    (410.5, 405.2, 1.2983),     # Small change
    (500.0, 300.0, 50.0),       # Large change
])

# This is a more robust solution for floating-point comparisons
def test_calculate_volatility_normal(current, previous, expected):
    result = calculate_co2_volatility(current, previous)
    assert result == pytest.approx(expected, abs=0.002)  # Allow small differences

# Test edge cases
@pytest.mark.parametrize("current, previous, expected", [
    (0, 400.0, None),           # Zero current value
    (400.0, 0, None),           # Zero previous value
    (-10, 400.0, None),         # Negative current value
    (400.0, -10, None),         # Negative previous value
    (None, 400.0, None),        # None current value
    (400.0, None, None),        # None previous value
])
def test_calculate_volatility_edge_cases(current, previous, expected):
    result = calculate_co2_volatility(current, previous)
    assert result == expected
if __name__ == "__main__":
    pytest.main(args=[__file__])