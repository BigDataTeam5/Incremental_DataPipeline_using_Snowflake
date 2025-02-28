#------------------------------------------------------------------------------
# Script:       co2_changes_udf.py
# Author:       [Your Name]
# Last Updated: [Date]
#------------------------------------------------------------------------------
 
# Import necessary libraries
import sys
import pandas as pd
 
def co2_changes(co2_values):
    """
    Compute the daily and weekly changes in CO2 ppm values.
 
    :param co2_values: List of CO2 ppm values.
    :return: Tuple of two Pandas Series - daily changes and weekly changes.
    """
    # Convert the input to a Pandas Series and remove NaN values
    co2_series = pd.Series(co2_values).dropna()
 
    # Calculate daily change (difference from the previous day)
    daily_change = co2_series.diff().fillna(0)
 
    # Calculate weekly change (difference from 7 days ago)
    weekly_change = co2_series.diff(periods=7).fillna(0)
 
    return daily_change.tolist(), weekly_change.tolist()
 
def main(co2_values):
    """
    The main function to handle the CO2 changes calculation.
    :param co2_values: List of CO2 ppm values passed as arguments.
    :return: Tuple with daily changes and weekly changes.
    """
    daily, weekly = co2_changes(co2_values)
    return daily, weekly
 
# For local debugging: Handle argument parsing
if __name__ == '__main__':
    if len(sys.argv) > 1:
        # Convert the input arguments to a list of floats (CO2 values)
        co2_values = list(map(float, sys.argv[1:]))
        daily, weekly = main(co2_values)
        print("Daily Changes:", daily)
        print("Weekly Changes:", weekly)
