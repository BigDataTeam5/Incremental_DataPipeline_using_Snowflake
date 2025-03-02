#------------------------------------------------------------------------------
# Script:       co2_volatility_udf.py
# Author:       [Your Name]
# Last Updated: [Date]
#------------------------------------------------------------------------------
 
import sys
 
def calculate_co2_volatility(current_value: float, previous_value: float) -> float:
    """
    Calculate the volatility between two CO2 measurements.
    Formula: |current - previous| / ((current + previous) / 2) * 100
 
    Args:
        current_value (float): Current CO2 measurement in ppm.
        previous_value (float): Previous CO2 measurement in ppm.
 
    Returns:
        float: Volatility as a percentage (rounded to 4 decimals), or 0.0 for invalid inputs.
    """
    try:
        if current_value is None or previous_value is None:
            return 0.0  # Return 0 instead of None for invalid inputs
        if current_value <= 0 or previous_value <= 0:
            return 0.0  # Return 0 for non-positive values
 
        # Calculate the average of the two values
        average = (current_value + previous_value) / 2.0
        if average == 0:
            return 0.0  # Avoid division by zero
 
        # Calculate volatility percentage
        volatility = abs(current_value - previous_value) / average * 100.0
        return round(volatility, 4)
    except Exception:
        return 0.0  # Return 0 in case of any unexpected errors
 
def main(current_value: float, previous_value: float) -> float:
    """
    Main function to handle CO2 volatility calculation.
 
    Args:
        current_value (float): Current CO2 measurement.
        previous_value (float): Previous CO2 measurement.
 
    Returns:
        float: Volatility percentage.
    """
    return calculate_co2_volatility(current_value, previous_value)
 
# For local debugging: Handle argument parsing
if __name__ == '__main__':
    if len(sys.argv) > 1:
        # Convert input arguments to floats
        current_value = float(sys.argv[1])
        previous_value = float(sys.argv[2])
        print(main(current_value, previous_value))