#------------------------------------------------------------------------------
# Script:       co2_volatility_udf.py
# Purpose:      Calculate volatility between CO2 measurements
# Last Updated: [Current Date]
#------------------------------------------------------------------------------
 

import sys
 
def calculate_co2_volatility(current_value: float, previous_value: float):
    """
    Calculate the volatility between two CO2 measurements.
    Formula: |current - previous| / ((current + previous) / 2) * 100
 
    Args:
        current_value (float): Current CO2 measurement in ppm.
        previous_value (float): Previous CO2 measurement in ppm.
 
    Returns:
        float or None: Volatility as a percentage (rounded to 4 decimals), or None for invalid inputs.
    """
    try:
        if current_value is None or previous_value is None:
            return None  # Return None instead of 0.0 for invalid inputs
        if current_value <= 0 or previous_value <= 0:
            return None  # Return None for non-positive values
 
        # Calculate the average of the two values
        average = (current_value + previous_value) / 2.0
        if average == 0:
            return None  # Avoid division by zero
 
        # Calculate volatility percentage
        volatility = abs(current_value - previous_value) / average * 100.0
        return round(volatility, 4)
    except Exception:
        return None  # Return None in case of any unexpected errors
 
def main(current_value: float, previous_value: float):
    """
    Main function to handle CO2 volatility calculation.
 
    Args:
        current_value (float): Current CO2 measurement.
        previous_value (float): Previous CO2 measurement.
 
    Returns:
        float or None: Volatility percentage or None for invalid inputs.
    """
    return calculate_co2_volatility(current_value, previous_value)
 
# For local debugging: Handle argument parsing
if __name__ == '__main__':
    if len(sys.argv) == 3:
        try:
            current_value = float(sys.argv[1]) if sys.argv[1].lower() != 'none' else None
            previous_value = float(sys.argv[2]) if sys.argv[2].lower() != 'none' else None
            result = main(current_value, previous_value)
            if result is not None:
                print(f"CO₂ Volatility: {result:.4f}%")
            else:
                print("CO₂ Volatility: None (Invalid input values)")
        except ValueError:
            print("Error: Arguments must be numbers or 'None'")
            print("Usage: python co2_volatility_udf.py <current_value> <previous_value>")
    else:
        print("Usage: python co2_volatility_udf.py <current_value> <previous_value>")